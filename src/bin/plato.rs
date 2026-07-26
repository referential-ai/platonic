use clap::{Parser, Subcommand};
use plato_agent::{
    AppError, ApprovalMode, IssuePrepOptions, IssuePrepOutcome, RunLedger, RunOptions, RunOutcome,
    RunOverrides, RunSession,
    daemon::{
        client::{DaemonClient, DaemonConnectionConfig},
        lock::ensure_workspace_unlocked,
        server::DaemonServer,
        wake_listener,
    },
    ledger::{latest_default_sqlite_session_id, latest_sqlite_session_id},
    new_session_id,
    paths::default_sqlite,
    replay_default_sqlite, replay_file, replay_sqlite, run_issue_prep, run_question,
    tui::{TuiOptions, run_tui},
};
use platonic_core::RunId;
use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const EMBEDDED_DAEMON_TIMEOUT: Duration = Duration::from_secs(3);
const EMBEDDED_DAEMON_POLL: Duration = Duration::from_millis(50);
const EMBEDDED_DAEMON_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Debug, Parser)]
#[command(name = "plato")]
#[command(about = "Plato Agent CLI")]
struct Cli {
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    events: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        value_name = "PATH",
        num_args = 0..=1,
        require_equals = true,
        help = "Use SQLite ledger; bare --db uses the platform user-state path"
    )]
    db: Option<Option<PathBuf>>,

    #[arg(
        long,
        global = true,
        help = "Auto-approve enabled tool calls that would otherwise prompt"
    )]
    yolo: bool,

    #[arg(
        short = 'c',
        long = "continue",
        help = "Continue the latest SQLite workspace session"
    )]
    continue_session: bool,

    #[arg(long, global = true, help = "Start the interactive terminal UI")]
    tui: bool,

    #[command(subcommand)]
    command: Option<Command>,

    #[arg(value_name = "QUESTION")]
    question: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Replay {
        #[arg(long, value_name = "RUN_ID")]
        run: Option<String>,

        #[arg(value_name = "FILE")]
        file: Option<PathBuf>,
    },
    /// Run the fixed, file-backed issue preparation pipeline.
    IssuePrep {
        #[command(subcommand)]
        command: IssuePrepCommand,
    },
}

#[derive(Debug, Subcommand)]
enum IssuePrepCommand {
    /// Create a run from Markdown on stdin and process it.
    Start {
        #[arg(value_name = "RUN_DIR", help = "New artifact directory")]
        run_dir: PathBuf,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> plato_agent::AppResult<()> {
    let cli = Cli::parse();
    let workspace_root = std::env::current_dir()?;
    if cli.tui {
        return run_tui_mode(cli, workspace_root);
    }
    if matches!(&cli.command, Some(Command::IssuePrep { .. })) {
        validate_issue_prep_cli(&cli)?;
    }
    match cli.command {
        Some(Command::Replay { run, file }) => {
            let ledger = replay_ledger(cli.db, file, &workspace_root)?;
            write_replay_output(&mut io::stdout(), ledger, run.as_deref(), &workspace_root)
        }
        Some(Command::IssuePrep { command }) => {
            run_issue_prep_cli(command, cli.config, workspace_root)
        }
        None => {
            let question = cli.question.join(" ");
            let ledger = run_ledger(cli.events, cli.db, &workspace_root)?;
            let session = run_session(cli.continue_session, &ledger)?;
            let outcome = run_question(RunOptions {
                question,
                config_path: cli.config,
                overrides: RunOverrides::default(),
                ledger: ledger.clone(),
                workspace_root,
                approval_mode: ApprovalMode::from_yolo(cli.yolo),
                run_id: None,
                session,
                event_sender: None,
                stream_to_stderr: true,
                cancel: None,
            })?;
            write_run_success_output(&mut io::stdout(), &mut io::stderr(), &outcome, &ledger)
        }
    }
}

fn validate_issue_prep_cli(cli: &Cli) -> plato_agent::AppResult<()> {
    if cli.events.is_some() || cli.db.is_some() || cli.yolo || cli.continue_session {
        return Err(AppError::Config(
            "plato issue-prep cannot be combined with --events, --db, --yolo, or -c".into(),
        ));
    }
    if !cli.question.is_empty() {
        return Err(AppError::Config(
            "plato issue-prep cannot be combined with a question".into(),
        ));
    }
    Ok(())
}

fn run_issue_prep_cli(
    command: IssuePrepCommand,
    config_path: Option<PathBuf>,
    workspace_root: PathBuf,
) -> plato_agent::AppResult<()> {
    let run_dir = match command {
        IssuePrepCommand::Start { run_dir } => resolve_cli_path(run_dir, &workspace_root),
    };
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let outcome = run_issue_prep(IssuePrepOptions {
        workspace_root,
        config_path,
        run_dir: run_dir.clone(),
        input,
    })?;
    write_issue_prep_output(&mut io::stdout(), &mut io::stderr(), outcome, &run_dir)
}

fn write_issue_prep_output(
    stdout: &mut impl Write,
    stderr: &mut impl Write,
    outcome: IssuePrepOutcome,
    run_dir: &Path,
) -> plato_agent::AppResult<()> {
    match outcome {
        IssuePrepOutcome::Candidate { markdown } => {
            stdout.write_all(markdown.as_bytes())?;
            writeln!(stderr, "run_dir: {}", run_dir.display())?;
            Ok(())
        }
        IssuePrepOutcome::Blocked { stage, reasons } => Err(AppError::IssuePrepBlocked {
            stage,
            reasons: reasons.join("; "),
            run_dir: run_dir.to_path_buf(),
        }),
    }
}

fn run_tui_mode(cli: Cli, workspace_root: PathBuf) -> plato_agent::AppResult<()> {
    let options = tui_options_from_cli(&cli, &workspace_root)?;
    let _embedded_daemon = ensure_tui_daemon(&workspace_root)?;
    run_tui(options)
}

fn tui_options_from_cli(cli: &Cli, workspace_root: &Path) -> plato_agent::AppResult<TuiOptions> {
    validate_tui_cli(cli)?;
    let mut options = TuiOptions::new(workspace_root.to_path_buf());
    options.config = cli.config.clone();
    Ok(options)
}

fn validate_tui_cli(cli: &Cli) -> plato_agent::AppResult<()> {
    if cli.command.is_some() {
        return Err(AppError::Config(
            "plato --tui cannot be combined with subcommands".into(),
        ));
    }
    if !cli.question.is_empty() {
        return Err(AppError::Config(
            "plato --tui cannot be combined with a question".into(),
        ));
    }
    if cli.events.is_some() || cli.db.is_some() || cli.yolo || cli.continue_session {
        return Err(AppError::Config(
            "plato --tui cannot be combined with --events, --db, --yolo, or -c".into(),
        ));
    }
    Ok(())
}

fn ensure_tui_daemon(workspace_root: &Path) -> plato_agent::AppResult<Option<EmbeddedDaemon>> {
    let config = DaemonConnectionConfig::resolve(workspace_root, None)?;
    if daemon_accepts_hello(&config) {
        return Ok(None);
    }
    start_embedded_daemon(workspace_root, &config).map(Some)
}

fn start_embedded_daemon(
    workspace_root: &Path,
    config: &DaemonConnectionConfig,
) -> plato_agent::AppResult<EmbeddedDaemon> {
    let server = DaemonServer::bind(workspace_root, None)?;
    let socket_path = server.paths().socket_path.clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let thread_shutdown = shutdown.clone();
    let handle = thread::spawn(move || server.serve_forever(thread_shutdown));
    let mut daemon = EmbeddedDaemon {
        shutdown,
        socket_path,
        handle: Some(handle),
    };
    wait_for_embedded_daemon(config, &mut daemon)?;
    Ok(daemon)
}

fn wait_for_embedded_daemon(
    config: &DaemonConnectionConfig,
    daemon: &mut EmbeddedDaemon,
) -> plato_agent::AppResult<()> {
    let deadline = Instant::now() + EMBEDDED_DAEMON_TIMEOUT;
    loop {
        if daemon_accepts_hello(config) {
            return Ok(());
        }
        if daemon.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return daemon_finished_before_ready(daemon);
        }
        if Instant::now() >= deadline {
            return Err(AppError::Config(format!(
                "timed out waiting for embedded plato-agentd at {}",
                config.socket_path.display()
            )));
        }
        thread::sleep(EMBEDDED_DAEMON_POLL);
    }
}

fn daemon_accepts_hello(config: &DaemonConnectionConfig) -> bool {
    let Ok(mut client) = DaemonClient::connect(&config.socket_path) else {
        return false;
    };
    client.hello(&config.workspace_root).is_ok()
}

fn daemon_finished_before_ready(daemon: &mut EmbeddedDaemon) -> plato_agent::AppResult<()> {
    let Some(handle) = daemon.handle.take() else {
        return Err(AppError::Config(
            "embedded plato-agentd stopped before accepting connections".into(),
        ));
    };
    match handle.join() {
        Ok(Ok(())) => Err(AppError::Config(
            "embedded plato-agentd exited before accepting connections".into(),
        )),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(AppError::Config(
            "embedded plato-agentd panicked before accepting connections".into(),
        )),
    }
}

struct EmbeddedDaemon {
    shutdown: Arc<AtomicBool>,
    socket_path: PathBuf,
    handle: Option<JoinHandle<plato_agent::AppResult<()>>>,
}

impl Drop for EmbeddedDaemon {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        wake_listener(&self.socket_path);
        if let Some(handle) = self.handle.take() {
            let deadline = Instant::now() + EMBEDDED_DAEMON_SHUTDOWN_TIMEOUT;
            while !handle.is_finished() && Instant::now() < deadline {
                thread::sleep(EMBEDDED_DAEMON_POLL);
            }
            if handle.is_finished() {
                let _ = handle.join();
            }
        }
    }
}

fn run_ledger(
    events: Option<PathBuf>,
    db: Option<Option<PathBuf>>,
    workspace_root: &Path,
) -> plato_agent::AppResult<RunLedger> {
    if db.is_some() && events.is_some() {
        return Err(AppError::Config(
            "--events and --db are mutually exclusive".into(),
        ));
    }
    match events {
        Some(path) => Ok(RunLedger::Jsonl(path)),
        None => {
            let ledger = sqlite_ledger(db, workspace_root)?;
            ensure_workspace_unlocked(workspace_root)?;
            Ok(ledger)
        }
    }
}

fn sqlite_ledger(
    db: Option<Option<PathBuf>>,
    workspace_root: &Path,
) -> plato_agent::AppResult<RunLedger> {
    match db {
        None | Some(None) => default_sqlite(workspace_root).map(RunLedger::DefaultSqlite),
        Some(Some(path)) => Ok(RunLedger::Sqlite(resolve_cli_path(path, workspace_root))),
    }
}

fn run_session(
    continue_session: bool,
    ledger: &RunLedger,
) -> plato_agent::AppResult<Option<RunSession>> {
    match ledger {
        RunLedger::Jsonl(_) if continue_session => Err(AppError::Config(
            "plato -c requires the SQLite ledger; remove --events".into(),
        )),
        RunLedger::Jsonl(_) => Ok(None),
        RunLedger::Sqlite(path) if continue_session => {
            let session_id = latest_sqlite_session_id(path).map_err(|error| match error {
                AppError::NoSqliteSessions | AppError::NoSqliteRuns => AppError::Config(
                    "plato -c found no previous SQLite session; run plato \"...\" first".into(),
                ),
                error => error,
            })?;
            Ok(Some(RunSession::Continue { session_id }))
        }
        RunLedger::Sqlite(_) => Ok(Some(RunSession::Fresh {
            session_id: new_session_id(),
        })),
        RunLedger::DefaultSqlite(path) if continue_session => {
            let session_id =
                latest_default_sqlite_session_id(path).map_err(|error| match error {
                    AppError::NoSqliteSessions | AppError::NoSqliteRuns => AppError::Config(
                        "plato -c found no previous SQLite session; run plato \"...\" first".into(),
                    ),
                    error => error,
                })?;
            Ok(Some(RunSession::Continue { session_id }))
        }
        RunLedger::DefaultSqlite(_) => Ok(Some(RunSession::Fresh {
            session_id: new_session_id(),
        })),
    }
}

fn replay_ledger(
    db: Option<Option<PathBuf>>,
    file: Option<PathBuf>,
    workspace_root: &Path,
) -> plato_agent::AppResult<RunLedger> {
    match (db, file) {
        (Some(_), Some(_)) => Err(AppError::Config(
            "replay accepts either --db or a JSONL file, not both".into(),
        )),
        (None, Some(file)) => Ok(RunLedger::Jsonl(file)),
        (db, None) => sqlite_ledger(db, workspace_root),
    }
}

fn resolve_cli_path(path: PathBuf, workspace_root: &Path) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    }
}

fn write_run_success_output(
    stdout: &mut impl Write,
    stderr: &mut impl Write,
    outcome: &RunOutcome,
    ledger: &RunLedger,
) -> plato_agent::AppResult<()> {
    writeln!(stdout, "{}", outcome.final_answer)?;
    if let RunLedger::Sqlite(path) = ledger {
        write_sqlite_replay_hint(stderr, &outcome.run_id, path)?;
    }
    if let RunLedger::DefaultSqlite(path) = ledger {
        write_sqlite_replay_hint(stderr, &outcome.run_id, path.as_path())?;
    }
    Ok(())
}

fn write_replay_output(
    stdout: &mut impl Write,
    ledger: RunLedger,
    run: Option<&str>,
    workspace_root: &Path,
) -> plato_agent::AppResult<()> {
    match ledger {
        RunLedger::Sqlite(path) => {
            ensure_workspace_unlocked(workspace_root)?;
            writeln!(stdout, "{}", replay_sqlite(&path, run)?)?;
        }
        RunLedger::DefaultSqlite(path) => {
            ensure_workspace_unlocked(workspace_root)?;
            writeln!(stdout, "{}", replay_default_sqlite(&path, run)?)?;
        }
        RunLedger::Jsonl(file) => {
            if run.is_some() {
                return Err(AppError::Config("replay --run requires --db".into()));
            }
            writeln!(stdout, "{}", replay_file(&file)?)?;
        }
    }
    Ok(())
}

fn write_sqlite_replay_hint(
    stderr: &mut impl Write,
    run_id: &RunId,
    path: &Path,
) -> plato_agent::AppResult<()> {
    let path = path.to_string_lossy();
    writeln!(stderr, "run_id: {run_id}")?;
    writeln!(stderr, "ledger_path: {path}")?;
    writeln!(
        stderr,
        "replay: plato replay --db={} --run {run_id}",
        shell_quote(&path)
    )?;
    Ok(())
}

#[cfg(unix)]
fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "_./:-".contains(character))
    {
        value.into()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

#[cfg(windows)]
fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || r"_./:\-".contains(character))
    {
        value.into()
    } else {
        format!("\"{}\"", value.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_success_hint_goes_to_stderr_without_changing_stdout() {
        let outcome = RunOutcome {
            run_id: RunId::new("run_1").unwrap(),
            final_answer: "done".into(),
        };
        let ledger = RunLedger::Sqlite(PathBuf::from("/tmp/plato proof/agent.db"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        write_run_success_output(&mut stdout, &mut stderr, &outcome, &ledger).unwrap();

        assert_eq!(String::from_utf8(stdout).unwrap(), "done\n");
        let stderr = String::from_utf8(stderr).unwrap();
        #[cfg(unix)]
        assert_eq!(
            stderr,
            "run_id: run_1\nledger_path: /tmp/plato proof/agent.db\nreplay: plato replay --db='/tmp/plato proof/agent.db' --run run_1\n"
        );
        #[cfg(windows)]
        assert_eq!(
            stderr,
            "run_id: run_1\nledger_path: /tmp/plato proof/agent.db\nreplay: plato replay --db=\"/tmp/plato proof/agent.db\" --run run_1\n"
        );
    }

    #[test]
    fn jsonl_success_does_not_print_replay_hint() {
        let outcome = RunOutcome {
            run_id: RunId::new("run_1").unwrap(),
            final_answer: "done".into(),
        };
        let ledger = RunLedger::Jsonl(PathBuf::from("events.jsonl"));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        write_run_success_output(&mut stdout, &mut stderr, &outcome, &ledger).unwrap();

        assert_eq!(String::from_utf8(stdout).unwrap(), "done\n");
        assert!(stderr.is_empty());
    }

    #[test]
    fn issue_prep_exposes_only_a_fresh_start_command() {
        let cli = Cli::try_parse_from(["plato", "issue-prep", "start", "runs/123"]).unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::IssuePrep {
                command: IssuePrepCommand::Start { run_dir },
            }) if run_dir == Path::new("runs/123")
        ));
        assert!(Cli::try_parse_from(["plato", "issue-prep", "resume", "runs/123"]).is_err());
        assert!(
            Cli::try_parse_from([
                "plato",
                "issue-prep",
                "start",
                "runs/123",
                "--issue",
                "referential-ai/plato-agent#123",
            ])
            .is_err()
        );
    }

    #[test]
    fn issue_prep_rejects_one_shot_run_options() {
        let cli =
            Cli::try_parse_from(["plato", "--db", "issue-prep", "start", "runs/123"]).unwrap();

        let error = validate_issue_prep_cli(&cli).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message)
                if message
                    == "plato issue-prep cannot be combined with --events, --db, --yolo, or -c"
        ));
    }

    #[test]
    fn issue_prep_candidate_uses_stdout_and_reports_its_run_directory() {
        let run_dir = Path::new("/tmp/issue-prep-123");
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        write_issue_prep_output(
            &mut stdout,
            &mut stderr,
            IssuePrepOutcome::Candidate {
                markdown: "# Candidate\n".into(),
            },
            run_dir,
        )
        .unwrap();

        assert_eq!(String::from_utf8(stdout).unwrap(), "# Candidate\n");
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "run_dir: /tmp/issue-prep-123\n"
        );
    }

    #[test]
    fn issue_prep_block_is_a_typed_error_without_stdout() {
        let run_dir = Path::new("/tmp/issue-prep-123");
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let error = write_issue_prep_output(
            &mut stdout,
            &mut stderr,
            IssuePrepOutcome::Blocked {
                stage: "review".into(),
                reasons: vec!["proof is incomplete".into()],
            },
            run_dir,
        )
        .unwrap_err();

        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        assert!(matches!(
            error,
            AppError::IssuePrepBlocked { stage, reasons, run_dir }
                if stage == "review"
                    && reasons == "proof is incomplete"
                    && run_dir == Path::new("/tmp/issue-prep-123")
        ));
    }

    #[test]
    fn tui_flag_builds_tui_options_with_config() {
        let dir = tempfile::tempdir().unwrap();
        let cli = Cli::try_parse_from(["plato", "--tui", "--config", "custom.toml"]).unwrap();

        let options = tui_options_from_cli(&cli, dir.path()).unwrap();

        assert_eq!(options.workspace, dir.path());
        assert_eq!(options.config.as_deref(), Some(Path::new("custom.toml")));
        assert_eq!(options.socket, None);
        assert_eq!(options.run, None);
        assert!(!options.snapshot);
    }

    #[test]
    fn tui_flag_rejects_one_shot_only_options() {
        let dir = tempfile::tempdir().unwrap();
        let cli = Cli::try_parse_from(["plato", "--tui", "--yolo"]).unwrap();

        let error = tui_options_from_cli(&cli, dir.path()).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message)
                if message == "plato --tui cannot be combined with --events, --db, --yolo, or -c"
        ));
    }

    #[test]
    fn tui_flag_rejects_questions() {
        let dir = tempfile::tempdir().unwrap();
        let cli = Cli::try_parse_from(["plato", "--tui", "hello"]).unwrap();

        let error = tui_options_from_cli(&cli, dir.path()).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message) if message == "plato --tui cannot be combined with a question"
        ));
    }

    #[test]
    fn embedded_daemon_drop_is_bounded_when_wake_connect_fails() {
        let workspace = tempfile::tempdir().unwrap();
        let release = Arc::new(AtomicBool::new(false));
        let worker_release = release.clone();
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (finished_sender, finished_receiver) = std::sync::mpsc::channel();
        let handle = thread::spawn(move || -> plato_agent::AppResult<()> {
            started_sender.send(()).unwrap();
            while !worker_release.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
            finished_sender.send(()).unwrap();
            Ok(())
        });
        started_receiver.recv().unwrap();
        let daemon = EmbeddedDaemon {
            shutdown: Arc::new(AtomicBool::new(false)),
            socket_path: workspace.path().join("missing.sock"),
            handle: Some(handle),
        };

        let started = Instant::now();
        drop(daemon);
        let elapsed = started.elapsed();
        release.store(true, Ordering::SeqCst);
        finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        assert!(
            elapsed < Duration::from_secs(1),
            "embedded daemon drop took {elapsed:?}"
        );
    }

    #[test]
    fn explicit_sqlite_path_is_resolved_against_workspace_root() {
        let dir = tempfile::tempdir().unwrap();

        let ledger = sqlite_ledger(Some(Some(PathBuf::from("agent.db"))), dir.path()).unwrap();

        assert_eq!(ledger, RunLedger::Sqlite(dir.path().join("agent.db")));
    }

    #[test]
    fn default_run_uses_default_sqlite_path() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let ledger = run_ledger(None, None, workspace.path()).unwrap();

            assert_eq!(
                ledger,
                RunLedger::DefaultSqlite(default_sqlite(workspace.path()).unwrap())
            );
        });
    }

    #[test]
    fn default_sqlite_run_fails_closed_when_daemon_lock_exists() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let socket = workspace.path().join("agent.sock");
            let _lock = plato_agent::daemon::lock::WorkspaceLock::acquire_for_workspace(
                workspace.path(),
                &socket,
            )
            .unwrap();

            let error = run_ledger(None, None, workspace.path()).unwrap_err();

            assert!(matches!(error, AppError::DaemonLockHeld { .. }));
        });
    }

    #[test]
    fn jsonl_run_does_not_check_daemon_lock() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let socket = workspace.path().join("agent.sock");
            let _lock = plato_agent::daemon::lock::WorkspaceLock::acquire_for_workspace(
                workspace.path(),
                &socket,
            )
            .unwrap();

            let ledger =
                run_ledger(Some(PathBuf::from("events.jsonl")), None, workspace.path()).unwrap();

            assert_eq!(ledger, RunLedger::Jsonl(PathBuf::from("events.jsonl")));
        });
    }

    #[test]
    fn default_sqlite_run_starts_fresh_session() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let ledger = RunLedger::DefaultSqlite(default_sqlite(workspace.path()).unwrap());

            let session = run_session(false, &ledger).unwrap().unwrap();

            assert!(matches!(session, RunSession::Fresh { .. }));
        });
    }

    #[test]
    fn continue_rejects_jsonl_ledger() {
        let ledger = RunLedger::Jsonl(PathBuf::from("events.jsonl"));

        let error = run_session(true, &ledger).unwrap_err();

        assert!(matches!(
            error,
            AppError::Config(message)
                if message == "plato -c requires the SQLite ledger; remove --events"
        ));
    }

    #[test]
    fn continue_uses_latest_sqlite_session() {
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("agent.db");
        let mut ledger = plato_agent::ledger::SqliteLedger::open_or_create(&path).unwrap();
        let run_id = RunId::new("run_1").unwrap();
        ledger
            .begin_session_run("session_1", &run_id, "hello", true)
            .unwrap();
        ledger.finish_session_run(&run_id, "hi").unwrap();

        let session = run_session(true, &RunLedger::Sqlite(path))
            .unwrap()
            .unwrap();

        assert_eq!(
            session,
            RunSession::Continue {
                session_id: "session_1".into()
            }
        );
    }

    #[test]
    fn bare_replay_uses_default_sqlite_path() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let ledger = replay_ledger(None, None, workspace.path()).unwrap();

            assert_eq!(
                ledger,
                RunLedger::DefaultSqlite(default_sqlite(workspace.path()).unwrap())
            );
        });
    }

    #[test]
    fn replay_file_stays_explicit_jsonl() {
        let workspace = tempfile::tempdir().unwrap();

        let ledger =
            replay_ledger(None, Some(PathBuf::from("events.jsonl")), workspace.path()).unwrap();

        assert_eq!(ledger, RunLedger::Jsonl(PathBuf::from("events.jsonl")));
    }

    #[test]
    fn default_sqlite_replay_fails_closed_when_daemon_lock_exists() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let socket = workspace.path().join("agent.sock");
            let _lock = plato_agent::daemon::lock::WorkspaceLock::acquire_for_workspace(
                workspace.path(),
                &socket,
            )
            .unwrap();
            let ledger = RunLedger::DefaultSqlite(default_sqlite(workspace.path()).unwrap());
            let mut stdout = Vec::new();

            let error =
                write_replay_output(&mut stdout, ledger, None, workspace.path()).unwrap_err();

            assert!(matches!(error, AppError::DaemonLockHeld { .. }));
            assert!(stdout.is_empty());
        });
    }

    fn with_test_xdg<T>(root: &Path, run: impl FnOnce() -> T) -> T {
        #[cfg(unix)]
        {
            let state_home = root.join("xdg-state");
            let runtime_home = root.join("xdg-runtime");
            temp_env::with_vars(
                [
                    ("XDG_STATE_HOME", Some(state_home.as_os_str())),
                    ("XDG_RUNTIME_DIR", Some(runtime_home.as_os_str())),
                ],
                run,
            )
        }
        #[cfg(windows)]
        {
            let local_app_data = root.join("local-app-data");
            temp_env::with_var("LOCALAPPDATA", Some(local_app_data.as_os_str()), run)
        }
    }
}
