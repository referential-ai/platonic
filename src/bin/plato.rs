use clap::{Parser, Subcommand};
use plato_agent::{
    AppError, ApprovalMode, IssuePrepOptions, IssuePrepOutcome, RunLedger, RunOptions, RunOutcome,
    RunOverrides, RunSession,
    config::Config,
    daemon::{
        client::{DaemonClient, DaemonConnectionConfig},
        lock::WorkspaceLock,
        protocol::{HelloResult, PendingApprovalSnapshot, RunStateName, StreamEvent},
    },
    discord_gateway::preflight_discord_gateway_daemon,
    ledger::{latest_default_sqlite_session_id, latest_sqlite_session_id},
    new_session_id,
    paths::{self, default_sqlite},
    replay_default_sqlite, replay_file, replay_sqlite, run_issue_prep, run_question,
    tui::{TuiOptions, run_tui},
};
use platonic_core::{HarnessEvent, RunId};
use std::{
    io::{self, BufRead, IsTerminal, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, Stdio},
    thread,
    time::{Duration, Instant},
};

const DAEMON_CLIENT_TIMEOUT: Duration = Duration::from_secs(3);
const DAEMON_POLL: Duration = Duration::from_millis(50);
const DAEMON_EVENT_PAGE: usize = 128;

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
    /// Run the workspace daemon in the foreground.
    Daemon {
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
    },
    /// Run an explicit gateway connector.
    Gateway {
        #[command(subcommand)]
        command: GatewayCommand,
    },
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
enum GatewayCommand {
    /// Run the Discord gateway for this workspace.
    Discord {
        #[arg(long, value_name = "PATH")]
        socket: Option<PathBuf>,
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
    let mut workspace_lock = None;
    if let Err(error) = run(&mut workspace_lock) {
        eprintln!("error: {error}");
        drop(workspace_lock);
        std::process::exit(1);
    }
}

fn run(workspace_lock: &mut Option<WorkspaceLock>) -> plato_agent::AppResult<()> {
    let cli = Cli::parse();
    let workspace_root = std::env::current_dir()?;
    let implicit_tui =
        implicit_tui_requested(&cli, io::stdin().is_terminal(), io::stdout().is_terminal());
    if cli.tui || implicit_tui {
        return run_tui_mode(cli, workspace_root);
    }
    if matches!(&cli.command, Some(Command::IssuePrep { .. })) {
        validate_issue_prep_cli(&cli)?;
    }
    if matches!(&cli.command, Some(Command::Daemon { .. })) {
        validate_daemon_cli(&cli)?;
    }
    if matches!(&cli.command, Some(Command::Gateway { .. })) {
        validate_gateway_cli(&cli)?;
    }
    match cli.command {
        Some(Command::Daemon { socket }) => run_daemon_service(workspace_root, socket),
        Some(Command::Gateway { command }) => match command {
            GatewayCommand::Discord { socket } => {
                run_discord_gateway_service(workspace_root, socket, cli.config)
            }
        },
        Some(Command::Replay { run, file }) => {
            let ledger = replay_ledger(cli.db, file, &workspace_root)?;
            *workspace_lock = acquire_sqlite_cli_lock(&ledger, &workspace_root)?;
            write_replay_output(&mut io::stdout(), ledger, run.as_deref())
        }
        Some(Command::IssuePrep { command }) => {
            run_issue_prep_cli(command, cli.config, workspace_root)
        }
        None => run_prompt(cli, workspace_root, workspace_lock),
    }
}

fn implicit_tui_requested(cli: &Cli, stdin_is_terminal: bool, stdout_is_terminal: bool) -> bool {
    cli.command.is_none() && cli.question.is_empty() && stdin_is_terminal && stdout_is_terminal
}

fn validate_daemon_cli(cli: &Cli) -> plato_agent::AppResult<()> {
    if cli.config.is_some()
        || cli.events.is_some()
        || cli.db.is_some()
        || cli.yolo
        || cli.continue_session
        || !cli.question.is_empty()
    {
        return Err(AppError::Config(
            "plato daemon cannot be combined with --config, --events, --db, --yolo, -c, or a question"
                .into(),
        ));
    }
    Ok(())
}

fn validate_gateway_cli(cli: &Cli) -> plato_agent::AppResult<()> {
    if cli.events.is_some()
        || cli.db.is_some()
        || cli.yolo
        || cli.continue_session
        || !cli.question.is_empty()
    {
        return Err(AppError::Config(
            "plato gateway cannot be combined with --events, --db, --yolo, -c, or a question"
                .into(),
        ));
    }
    Ok(())
}

fn run_daemon_service(
    workspace_root: PathBuf,
    socket: Option<PathBuf>,
) -> plato_agent::AppResult<()> {
    let mut command = ProcessCommand::new(sibling_binary("plato-agentd")?);
    command.arg("--workspace").arg(&workspace_root);
    if let Some(socket) = socket {
        command.arg("--socket").arg(socket);
    }
    command.current_dir(workspace_root);
    handoff(command)
}

fn run_discord_gateway_service(
    workspace_root: PathBuf,
    socket: Option<PathBuf>,
    config: Option<PathBuf>,
) -> plato_agent::AppResult<()> {
    Config::load(&workspace_root, config.as_deref())?;
    let daemon = DaemonConnectionConfig::resolve(&workspace_root, socket.clone())?;
    preflight_discord_gateway_daemon(&daemon, DAEMON_CLIENT_TIMEOUT).map_err(|error| {
        let hint = match socket.as_deref() {
            Some(socket) => format!(
                "plato daemon --socket {}",
                shell_quote(&socket.to_string_lossy())
            ),
            None => "plato daemon".into(),
        };
        AppError::Config(format!(
            "workspace daemon is unavailable or incompatible at {}: {error}; start it with `{hint}`",
            daemon.socket_path.display()
        ))
    })?;

    let mut command = ProcessCommand::new(sibling_binary("plato-gateway-discord")?);
    command
        .arg("--workspace")
        .arg(&daemon.workspace_root)
        .current_dir(&daemon.workspace_root);
    if let Some(socket) = socket {
        command.arg("--socket").arg(socket);
    }
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    handoff(command)
}

#[cfg(unix)]
fn handoff(mut command: ProcessCommand) -> plato_agent::AppResult<()> {
    use std::os::unix::process::CommandExt;

    Err(command.exec().into())
}

#[cfg(windows)]
fn handoff(mut command: ProcessCommand) -> plato_agent::AppResult<()> {
    let status = command.status()?;
    std::process::exit(status.code().unwrap_or(1));
}

fn run_prompt(
    cli: Cli,
    workspace_root: PathBuf,
    workspace_lock: &mut Option<WorkspaceLock>,
) -> plato_agent::AppResult<()> {
    let question = cli.question.join(" ");
    if question.trim().is_empty() {
        return Err(AppError::EmptyQuestion);
    }
    if daemon_prompt_eligible(&cli) {
        let config = DaemonConnectionConfig::resolve(&workspace_root, None)?;
        if let Some(mut client) = connect_serving_daemon(&config) {
            let stdin = io::stdin();
            return run_daemon_prompt(
                &mut client,
                question,
                cli.continue_session,
                cli.config.as_deref(),
                &mut stdin.lock(),
                &mut io::stdout(),
                &mut io::stderr(),
            );
        }
    }

    let ledger = run_ledger(cli.events, cli.db, &workspace_root)?;
    *workspace_lock = acquire_sqlite_cli_lock(&ledger, &workspace_root)?;
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

fn daemon_prompt_eligible(cli: &Cli) -> bool {
    cli.events.is_none() && !matches!(cli.db, Some(Some(_))) && !cli.yolo
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
    ensure_tui_daemon(&workspace_root)?;
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

fn ensure_tui_daemon(workspace_root: &Path) -> plato_agent::AppResult<()> {
    let config = DaemonConnectionConfig::resolve(workspace_root, None)?;
    if connect_serving_daemon(&config).is_some() {
        return Ok(());
    }
    let mut daemon = spawn_detached_daemon(workspace_root)?;
    wait_for_persistent_daemon(&config, &mut daemon)
}

fn connect_serving_daemon(config: &DaemonConnectionConfig) -> Option<DaemonClient> {
    connect_serving_daemon_with_timeout(config, DAEMON_CLIENT_TIMEOUT)
}

fn connect_serving_daemon_with_timeout(
    config: &DaemonConnectionConfig,
    timeout: Duration,
) -> Option<DaemonClient> {
    connect_workspace_daemon_with_timeout(config, timeout)
        .ok()
        .map(|(client, _hello)| client)
}

fn connect_workspace_daemon_with_timeout(
    config: &DaemonConnectionConfig,
    timeout: Duration,
) -> plato_agent::AppResult<(DaemonClient, HelloResult)> {
    let mut client = DaemonClient::connect_with_timeout(&config.socket_path, timeout)?;
    let hello = client.hello(&config.workspace_root)?;
    Ok((client, hello))
}

fn spawn_detached_daemon(workspace_root: &Path) -> plato_agent::AppResult<Child> {
    let binary = sibling_binary("plato-agentd")?;
    let mut command = ProcessCommand::new(&binary);
    command
        .arg("--workspace")
        .arg(workspace_root)
        .current_dir(workspace_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    detach_command(&mut command);
    command.spawn().map_err(|error| {
        AppError::Config(format!(
            "failed to start persistent {}: {error}",
            binary.display()
        ))
    })
}

fn sibling_binary(name: &str) -> plato_agent::AppResult<PathBuf> {
    let mut binary = std::env::current_exe()?;
    binary.set_file_name(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    Ok(binary)
}

#[cfg(unix)]
fn detach_command(command: &mut ProcessCommand) {
    use std::os::unix::process::CommandExt;

    // `setsid` is async-signal-safe and detaches the daemon from the TUI's terminal session.
    unsafe {
        command.pre_exec(|| rustix::process::setsid().map(|_| ()).map_err(Into::into));
    }
}

#[cfg(windows)]
fn detach_command(command: &mut ProcessCommand) {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS};

    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

fn wait_for_persistent_daemon(
    config: &DaemonConnectionConfig,
    daemon: &mut Child,
) -> plato_agent::AppResult<()> {
    let deadline = Instant::now() + DAEMON_CLIENT_TIMEOUT;
    loop {
        if connect_serving_daemon(config).is_some() {
            return Ok(());
        }
        if let Some(status) = daemon.try_wait()? {
            return Err(AppError::Config(format!(
                "persistent plato-agentd exited before accepting connections: {status}"
            )));
        }
        if Instant::now() >= deadline {
            let _ = daemon.kill();
            let _ = daemon.wait();
            return Err(AppError::Config(format!(
                "timed out waiting for persistent plato-agentd at {}",
                config.socket_path.display()
            )));
        }
        thread::sleep(DAEMON_POLL);
    }
}

fn run_daemon_prompt(
    client: &mut DaemonClient,
    question: String,
    continue_session: bool,
    config_path: Option<&Path>,
    stdin: &mut impl BufRead,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> plato_agent::AppResult<()> {
    let config_path = config_path.map(|path| path.to_string_lossy().into_owned());
    let started = if continue_session {
        client.message_append(question, config_path, false)?
    } else {
        client.run_start(question, config_path, false)?
    };
    let run_id = RunId::new(started.run_id.clone())?;
    let mut from_offset = Some(0);
    let mut terminal_failure = None;
    let mut wrote_stderr_delta = false;

    loop {
        let page = client.events_stream(&started.run_id, from_offset, DAEMON_EVENT_PAGE)?;
        let event_count = page.events.len();
        from_offset = Some(page.next_offset);

        for buffered in page.events {
            match buffered.event {
                StreamEvent::AssistantDelta { text, .. } => {
                    stderr.write_all(text.as_bytes())?;
                    stderr.flush()?;
                    wrote_stderr_delta = true;
                }
                StreamEvent::ApprovalRequested {
                    run_id,
                    tool_call_id,
                    tool_name,
                    effect,
                    reason,
                    approval_preview,
                    diff_preview,
                } => {
                    let pending = client
                        .transcript_read(&run_id)?
                        .pending_approval
                        .filter(|pending| pending.tool_call_id == tool_call_id)
                        .unwrap_or(PendingApprovalSnapshot {
                            run_id: run_id.clone(),
                            tool_call_id: tool_call_id.clone(),
                            tool_name,
                            effect,
                            reason: Some(reason),
                            input_preview: None,
                            approval_preview,
                            diff_preview,
                        });
                    if prompt_daemon_approval(stdin, stderr, &pending)? {
                        client.approval_grant(&run_id, &tool_call_id)?;
                    } else {
                        client.approval_deny(
                            &run_id,
                            &tool_call_id,
                            "approval denied by stdin".into(),
                        )?;
                    }
                }
                StreamEvent::Ledger { record } => match record.event {
                    HarnessEvent::ModelResponded { .. } if wrote_stderr_delta => {
                        writeln!(stderr)?;
                        wrote_stderr_delta = false;
                    }
                    HarnessEvent::RunFailed { reason, .. } => {
                        terminal_failure = Some(reason);
                    }
                    _ => {}
                },
                StreamEvent::Canceled { .. } | StreamEvent::Unknown(_) => {}
            }
        }

        if event_count == DAEMON_EVENT_PAGE {
            continue;
        }
        match page.status {
            RunStateName::Running | RunStateName::CancelRequested => {
                thread::sleep(DAEMON_POLL);
            }
            RunStateName::Finished => {
                if wrote_stderr_delta {
                    writeln!(stderr)?;
                }
                let transcript = client.transcript_read(&started.run_id)?;
                let final_answer = transcript.final_answer.ok_or_else(|| {
                    AppError::DaemonProtocol(format!(
                        "finished daemon run {} omitted its final answer",
                        started.run_id
                    ))
                })?;
                writeln!(stdout, "{final_answer}")?;
                write_sqlite_replay_hint(stderr, &run_id, Path::new(&started.ledger_path))?;
                return Ok(());
            }
            RunStateName::Canceled => return Err(AppError::RunCanceled),
            RunStateName::Failed | RunStateName::Interrupted => {
                return Err(AppError::RunFailed(terminal_failure.unwrap_or_else(|| {
                    format!(
                        "daemon run {} ended as {}",
                        started.run_id,
                        page.status.as_str()
                    )
                })));
            }
        }
    }
}

fn prompt_daemon_approval(
    stdin: &mut impl BufRead,
    stderr: &mut impl Write,
    pending: &PendingApprovalSnapshot,
) -> plato_agent::AppResult<bool> {
    if let Some(preview) = pending.approval_preview.as_deref() {
        write!(stderr, "Approve {}?\n{preview}\n[y/N] ", pending.tool_name)?;
    } else if let Some(preview) = pending.input_preview.as_deref() {
        write!(stderr, "Approve {} {preview}? [y/N] ", pending.tool_name)?;
    } else {
        write!(stderr, "Approve {}? [y/N] ", pending.tool_name)?;
    }
    stderr.flush()?;

    let mut line = String::new();
    stdin.read_line(&mut line)?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
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
        None => sqlite_ledger(db, workspace_root),
    }
}

fn acquire_sqlite_cli_lock(
    ledger: &RunLedger,
    workspace_root: &Path,
) -> plato_agent::AppResult<Option<WorkspaceLock>> {
    match ledger {
        RunLedger::Jsonl(_) => Ok(None),
        RunLedger::Sqlite(_) | RunLedger::DefaultSqlite(_) => {
            let socket_path = paths::default_socket_path(workspace_root)?;
            WorkspaceLock::acquire_for_workspace(workspace_root, &socket_path).map(Some)
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
) -> plato_agent::AppResult<()> {
    match ledger {
        RunLedger::Sqlite(path) => {
            writeln!(stdout, "{}", replay_sqlite(&path, run)?)?;
        }
        RunLedger::DefaultSqlite(path) => {
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
    use plato_agent::daemon::{server::DaemonServer, wake_listener};
    use serde_json::{Value, json};
    use std::{
        net::{TcpListener, TcpStream},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

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
    fn service_commands_parse_only_explicit_entries() {
        let daemon =
            Cli::try_parse_from(["plato", "daemon", "--socket", "runtime/agent.sock"]).unwrap();
        assert!(matches!(
            daemon.command,
            Some(Command::Daemon { socket })
                if socket.as_deref() == Some(Path::new("runtime/agent.sock"))
        ));

        let gateway = Cli::try_parse_from([
            "plato",
            "gateway",
            "discord",
            "--socket",
            "runtime/agent.sock",
            "--config",
            "gateway.toml",
        ])
        .unwrap();
        assert_eq!(gateway.config.as_deref(), Some(Path::new("gateway.toml")));
        assert!(matches!(
            gateway.command,
            Some(Command::Gateway {
                command: GatewayCommand::Discord { socket },
            }) if socket.as_deref() == Some(Path::new("runtime/agent.sock"))
        ));

        assert!(Cli::try_parse_from(["plato", "gateway"]).is_err());
        assert!(Cli::try_parse_from(["plato", "gateway", "telegram"]).is_err());
    }

    #[test]
    fn service_commands_reject_unrelated_run_options() {
        let daemon = Cli::try_parse_from(["plato", "--config", "plato.toml", "daemon"]).unwrap();
        assert!(matches!(
            validate_daemon_cli(&daemon),
            Err(AppError::Config(message))
                if message.starts_with("plato daemon cannot be combined")
        ));

        let gateway = Cli::try_parse_from(["plato", "--db", "gateway", "discord"]).unwrap();
        assert!(matches!(
            validate_gateway_cli(&gateway),
            Err(AppError::Config(message))
                if message.starts_with("plato gateway cannot be combined")
        ));
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
    fn implicit_tui_requires_both_terminals_and_no_explicit_entry() {
        let bare = Cli::try_parse_from(["plato"]).unwrap();
        let mut workspace_lock = None;
        assert!(implicit_tui_requested(&bare, true, true));
        assert!(!implicit_tui_requested(&bare, false, true));
        assert!(!implicit_tui_requested(&bare, true, false));
        assert!(matches!(
            run_prompt(bare, PathBuf::from("."), &mut workspace_lock),
            Err(AppError::EmptyQuestion)
        ));

        let prompt = Cli::try_parse_from(["plato", "hello"]).unwrap();
        assert!(!implicit_tui_requested(&prompt, true, true));
        let empty_prompt = Cli::try_parse_from(["plato", ""]).unwrap();
        assert!(!implicit_tui_requested(&empty_prompt, true, true));
        assert!(matches!(
            run_prompt(empty_prompt, PathBuf::from("."), &mut workspace_lock),
            Err(AppError::EmptyQuestion)
        ));
        let replay = Cli::try_parse_from(["plato", "replay"]).unwrap();
        assert!(!implicit_tui_requested(&replay, true, true));
    }

    #[test]
    fn daemon_prompt_eligibility_keeps_explicit_run_modes_direct() {
        for arguments in [
            vec!["plato", "hello"],
            vec!["plato", "--db", "hello"],
            vec!["plato", "--config", "custom.toml", "hello"],
        ] {
            let cli = Cli::try_parse_from(arguments).unwrap();
            assert!(daemon_prompt_eligible(&cli));
        }
        for arguments in [
            vec!["plato", "--events", "events.jsonl", "hello"],
            vec!["plato", "--db=events.db", "hello"],
            vec!["plato", "--yolo", "hello"],
        ] {
            let cli = Cli::try_parse_from(arguments).unwrap();
            assert!(!daemon_prompt_eligible(&cli));
        }
    }

    #[test]
    fn delegated_approval_keeps_stdin_default_no() {
        let pending = PendingApprovalSnapshot {
            run_id: "run_1".into(),
            tool_call_id: "call_1".into(),
            tool_name: "file.write".into(),
            effect: platonic_core::EffectClass::WorkspaceWrite,
            reason: Some("approval required".into()),
            input_preview: Some(r#"{"path":"out.txt"}"#.into()),
            approval_preview: None,
            diff_preview: None,
        };
        let mut stderr = Vec::new();

        let granted = prompt_daemon_approval(&mut "\n".as_bytes(), &mut stderr, &pending).unwrap();

        assert!(!granted);
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            r#"Approve file.write {"path":"out.txt"}? [y/N] "#
        );
    }

    #[cfg(unix)]
    #[test]
    fn daemon_probe_bounds_a_stalled_hello() {
        use std::os::unix::net::UnixListener;

        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let config = DaemonConnectionConfig::resolve(workspace.path(), Some(socket_path)).unwrap();
        let server = thread::spawn(move || {
            let _stream = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(150));
        });

        let started = Instant::now();
        assert!(connect_serving_daemon_with_timeout(&config, Duration::from_millis(50)).is_none());
        let elapsed = started.elapsed();
        server.join().unwrap();

        assert!(elapsed < Duration::from_secs(1), "probe took {elapsed:?}");
        assert_eq!(DAEMON_CLIENT_TIMEOUT, Duration::from_secs(3));
    }

    #[test]
    fn serving_daemon_handles_fresh_and_latest_continuation() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let provider = FakeProvider::start(vec![
                text_stream(&["first ", "answer"]),
                text_stream(&["second answer"]),
            ]);
            let config_path = workspace.path().join("custom.toml");
            write_test_config(&config_path, &provider.base_url, "file.read");
            let daemon = TestDaemon::start(workspace.path());
            let mut client = daemon.client();
            let mut first_stdout = Vec::new();
            let mut first_stderr = Vec::new();

            run_daemon_prompt(
                &mut client,
                "first question".into(),
                false,
                Some(&config_path),
                &mut "".as_bytes(),
                &mut first_stdout,
                &mut first_stderr,
            )
            .unwrap();

            let mut second_stdout = Vec::new();
            let mut second_stderr = Vec::new();
            run_daemon_prompt(
                &mut client,
                "follow up".into(),
                true,
                Some(&config_path),
                &mut "".as_bytes(),
                &mut second_stdout,
                &mut second_stderr,
            )
            .unwrap();
            daemon.stop();
            let requests = provider.join();

            assert_eq!(String::from_utf8(first_stdout).unwrap(), "first answer\n");
            let first_stderr = String::from_utf8(first_stderr).unwrap();
            assert!(first_stderr.starts_with("first answer\nrun_id: run_"));
            assert!(first_stderr.contains("\nledger_path: "));
            assert!(first_stderr.contains("\nreplay: plato replay --db="));
            assert_eq!(String::from_utf8(second_stdout).unwrap(), "second answer\n");
            assert!(
                String::from_utf8(second_stderr)
                    .unwrap()
                    .starts_with("second answer\nrun_id: run_")
            );
            assert_eq!(requests[0]["model"], "test-model");
            let continued_messages = requests[1]["messages"].to_string();
            assert!(continued_messages.contains("first question"));
            assert!(continued_messages.contains("first answer"));
            assert!(continued_messages.contains("follow up"));
        });
    }

    #[test]
    fn delegated_prompt_tolerates_context_compaction_ledger_event() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let old_answer = "old answer ".repeat(800);
            let provider = FakeProvider::start(vec![
                text_stream(&[&old_answer]),
                text_stream(&["current answer"]),
            ]);
            let config_path = workspace.path().join("custom.toml");
            write_test_config_with_budget(&config_path, &provider.base_url, "file.read", 1_000);
            let daemon = TestDaemon::start(workspace.path());
            let mut client = daemon.client();

            run_daemon_prompt(
                &mut client,
                "old question".into(),
                false,
                Some(&config_path),
                &mut "".as_bytes(),
                &mut Vec::new(),
                &mut Vec::new(),
            )
            .unwrap();
            let mut stdout = Vec::new();
            run_daemon_prompt(
                &mut client,
                "current question".into(),
                true,
                Some(&config_path),
                &mut "".as_bytes(),
                &mut stdout,
                &mut Vec::new(),
            )
            .unwrap();
            daemon.stop();
            let requests = provider.join();

            assert_eq!(String::from_utf8(stdout).unwrap(), "current answer\n");
            let continued_messages = requests[1]["messages"].to_string();
            assert!(
                continued_messages
                    .contains("[older session turns omitted to fit the context budget]")
            );
            assert!(!continued_messages.contains("old question"));
            let ledger_path = default_sqlite(workspace.path()).unwrap();
            let ledger =
                plato_agent::ledger::SqliteLedger::open_default_readonly(&ledger_path).unwrap();
            let session = ledger.read_latest_session().unwrap();
            let continued_run = session.runs.last().unwrap();
            assert_eq!(
                continued_run
                    .records
                    .iter()
                    .filter(|record| matches!(record.event, HarnessEvent::ContextCompacted { .. }))
                    .count(),
                1
            );
        });
    }

    #[test]
    fn delegated_prompt_bridges_stdin_grant_and_denial() {
        for (stdin, final_answer, file_exists) in
            [("y\n", "granted", true), ("\n", "denied", false)]
        {
            let workspace = tempfile::tempdir().unwrap();
            with_test_xdg(workspace.path(), || {
                let provider =
                    FakeProvider::start(vec![tool_call_stream(), text_stream(&[final_answer])]);
                let config_path = workspace.path().join("plato.toml");
                write_test_config(&config_path, &provider.base_url, "file.write");
                let daemon = TestDaemon::start(workspace.path());
                let mut client = daemon.client();
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();

                run_daemon_prompt(
                    &mut client,
                    "write the file".into(),
                    false,
                    Some(&config_path),
                    &mut stdin.as_bytes(),
                    &mut stdout,
                    &mut stderr,
                )
                .unwrap();
                daemon.stop();
                let requests = provider.join();

                assert_eq!(
                    String::from_utf8(stdout).unwrap(),
                    format!("{final_answer}\n")
                );
                assert!(String::from_utf8(stderr).unwrap().contains(
                    r#"Approve file.write {"content":"hello","path":"out.txt"}? [y/N] "#
                ));
                assert_eq!(workspace.path().join("out.txt").exists(), file_exists);
                let continuation = requests[1]["messages"].to_string();
                if file_exists {
                    assert!(continuation.contains(r#"\"path\":\"out.txt\""#));
                    assert!(continuation.contains(r#"\"bytes\":5"#));
                } else {
                    assert!(continuation.contains("approval denied by stdin"));
                }
            });
        }
    }

    #[test]
    fn delegated_prompt_returns_terminal_daemon_failure() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let provider = FakeProvider::start(vec!["data: not-json\n\n".into()]);
            let config_path = workspace.path().join("plato.toml");
            write_test_config(&config_path, &provider.base_url, "file.read");
            let daemon = TestDaemon::start(workspace.path());
            let mut client = daemon.client();
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();

            let error = run_daemon_prompt(
                &mut client,
                "fail".into(),
                false,
                Some(&config_path),
                &mut "".as_bytes(),
                &mut stdout,
                &mut stderr,
            )
            .unwrap_err();
            daemon.stop();
            provider.join();

            assert!(stdout.is_empty());
            assert!(matches!(
                error,
                AppError::RunFailed(reason)
                    if reason.contains("provider returned invalid SSE JSON")
            ));
        });
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

            let ledger = run_ledger(None, None, workspace.path()).unwrap();
            let error = acquire_sqlite_cli_lock(&ledger, workspace.path()).unwrap_err();

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
            assert!(
                acquire_sqlite_cli_lock(&ledger, workspace.path())
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn direct_sqlite_error_returns_with_lock_held_for_final_stderr() {
        let workspace = tempfile::tempdir().unwrap();
        with_test_xdg(workspace.path(), || {
            let cli = Cli::try_parse_from([
                "plato",
                "--db=agent.db",
                "--config",
                "missing.toml",
                "question",
            ])
            .unwrap();
            let mut workspace_lock = None;

            run_prompt(cli, workspace.path().to_path_buf(), &mut workspace_lock).unwrap_err();

            assert!(workspace_lock.is_some());
            let socket_path = paths::default_socket_path(workspace.path()).unwrap();
            assert!(matches!(
                WorkspaceLock::acquire_for_workspace(workspace.path(), &socket_path),
                Err(AppError::DaemonLockHeld { .. })
            ));
            drop(workspace_lock.take());
            drop(WorkspaceLock::acquire_for_workspace(workspace.path(), &socket_path).unwrap());
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
        let turn_id = platonic_core::TurnId::new("turn_1").unwrap();
        let events = [
            HarnessEvent::RunStarted {
                run_id: run_id.clone(),
                agent_id: platonic_core::AgentId::new("plato").unwrap(),
            },
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: platonic_core::ContextPack {
                    token_budget: 4_000,
                    fragments: vec![],
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: platonic_core::ModelName::new("test-model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id,
                step: 0,
                output: platonic_core::Message {
                    role: platonic_core::MessageRole::Assistant,
                    content: "hi".into(),
                },
                proposed_calls: vec![],
                usage: None,
            },
        ];
        for (seq, event) in events.into_iter().enumerate() {
            ledger
                .append(
                    run_id.as_str(),
                    &platonic_core::RecordedEvent {
                        seq: seq as u64,
                        occurred_at_ms: seq as u64,
                        event,
                    },
                )
                .unwrap();
        }
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

    struct TestDaemon {
        config: DaemonConnectionConfig,
        shutdown: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<plato_agent::AppResult<()>>>,
    }

    impl TestDaemon {
        fn start(workspace: &Path) -> Self {
            let server = DaemonServer::bind(workspace, None).unwrap();
            let config = DaemonConnectionConfig::resolve(workspace, None).unwrap();
            let shutdown = Arc::new(AtomicBool::new(false));
            let server_shutdown = Arc::clone(&shutdown);
            let handle = thread::spawn(move || server.serve_forever(server_shutdown));
            Self {
                config,
                shutdown,
                handle: Some(handle),
            }
        }

        fn client(&self) -> DaemonClient {
            connect_serving_daemon(&self.config).unwrap()
        }

        fn stop(mut self) {
            self.client().shutdown_if_idle().unwrap();
            self.handle.take().unwrap().join().unwrap().unwrap();
        }
    }

    impl Drop for TestDaemon {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                self.shutdown.store(true, Ordering::SeqCst);
                wake_listener(&self.config.socket_path);
                handle.join().unwrap().unwrap();
            }
        }
    }

    struct FakeProvider {
        base_url: String,
        handle: thread::JoinHandle<Vec<Value>>,
    }

    impl FakeProvider {
        fn start(responses: Vec<String>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let handle = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut requests = Vec::new();
                for body in responses {
                    let mut stream = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                assert!(
                                    Instant::now() < deadline,
                                    "timed out waiting for provider request"
                                );
                                thread::sleep(Duration::from_millis(10));
                            }
                            Err(error) => panic!("provider accept failed: {error}"),
                        }
                    };
                    let request = read_provider_request(&mut stream);
                    requests.push(serde_json::from_str(&request).unwrap());
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .unwrap();
                }
                requests
            });
            Self { base_url, handle }
        }

        fn join(self) -> Vec<Value> {
            self.handle.join().unwrap()
        }
    }

    fn write_test_config(path: &Path, base_url: &str, enabled_tool: &str) {
        write_test_config_with_budget(path, base_url, enabled_tool, 4000);
    }

    fn write_test_config_with_budget(
        path: &Path,
        base_url: &str,
        enabled_tool: &str,
        token_budget: u32,
    ) {
        std::fs::write(
            path,
            format!(
                r#"
[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "PATH"
base_url = "{base_url}"
timeout_ms = 2000

[limits]
token_budget = {token_budget}
max_output_tokens = 32
max_turns = 2

[tools]
enabled = ["{enabled_tool}"]
"#
            ),
        )
        .unwrap();
    }

    fn read_provider_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "provider request ended before headers");
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap();
        while bytes.len() < header_end + content_length {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "provider request ended before body");
            bytes.extend_from_slice(&buffer[..count]);
        }
        String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap()
    }

    fn text_stream(chunks: &[&str]) -> String {
        let mut body = String::new();
        for chunk in chunks {
            body.push_str(&format!(
                "data: {}\n\n",
                json!({
                    "choices": [{
                        "index": 0,
                        "delta": {"content": chunk},
                        "finish_reason": null
                    }]
                })
            ));
        }
        body.push_str(
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        );
        body.push_str("data: [DONE]\n\n");
        body
    }

    fn tool_call_stream() -> String {
        concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"file_write\",\"arguments\":\"{\\\"path\\\":\\\"out.txt\\\",\\\"content\\\":\\\"hello\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        )
        .into()
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

            let error = acquire_sqlite_cli_lock(&ledger, workspace.path()).unwrap_err();

            assert!(matches!(error, AppError::DaemonLockHeld { .. }));
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
