use clap::{Parser, Subcommand};
use plato_agent::{
    AppError, AppResult, ApprovalMode, RunOptions, ensure_server, ensure_server_interactive,
    offline, run_question, select_profile_home,
    tui::{ThreadAttachment, TuiOptions, run_tui, voice_control},
};
use platonic_client::{client::DaemonClient, paths};
use platonic_protocol::{
    ApprovalProfile, CompletionOutcome, IssuePrepResult, ReasoningEffort, RunOverrides,
    ThreadApprovalPolicy, ThreadSendResult, ThreadSpawnDecision, ThreadSpawnResult, ThreadStatus,
};
use std::{
    io::{self, BufRead, IsTerminal, Read, Write},
    path::{Path, PathBuf},
};

const THREAD_EVENT_PAGE: usize = 128;

#[derive(Debug, Parser)]
#[command(name = "plato")]
#[command(about = "Plato Agent client")]
#[command(version = platonic_protocol::PLATO_BUILD_IDENTITY)]
struct Cli {
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        value_name = "FILE",
        help = "Exact client-side voice configuration used only by the TUI"
    )]
    voice_config: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        value_name = "PATH",
        num_args = 0..=1,
        require_equals = true,
        help = "Select a workspace ledger for offline replay; bare --db uses the registered workspace ledger"
    )]
    db: Option<Option<PathBuf>>,

    #[arg(
        long,
        global = true,
        help = "Auto-approve tool calls that would otherwise prompt"
    )]
    yolo: bool,

    #[arg(
        short = 'c',
        long = "continue",
        help = "Continue the latest workspace session"
    )]
    continue_session: bool,

    #[arg(long, global = true, help = "Start the interactive terminal UI")]
    tui: bool,

    #[arg(
        long,
        global = true,
        value_name = "THREAD_ID",
        help = "Attach the terminal UI to an existing server thread"
    )]
    remote: Option<String>,

    #[arg(
        long,
        global = true,
        value_name = "NAME",
        help = "Select a workspace profile for the terminal UI"
    )]
    profile: Option<String>,

    #[arg(long, global = true, help = "Use a static TUI working indicator")]
    reduced_motion: bool,

    #[command(subcommand)]
    command: Option<Command>,

    #[arg(value_name = "QUESTION")]
    question: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Replay a durable ledger without starting or contacting the server.
    Replay {
        #[arg(long, value_name = "RUN_ID")]
        run: Option<String>,
        #[arg(value_name = "FILE")]
        file: Option<PathBuf>,
    },
    /// Run the fixed issue-preparation pipeline through the server.
    IssuePrep {
        #[command(subcommand)]
        command: IssuePrepCommand,
    },
    /// Manage durable server threads.
    Thread {
        #[command(subcommand)]
        command: ThreadCommand,
    },
}

#[derive(Debug, Subcommand)]
enum IssuePrepCommand {
    /// Create a run from Markdown on stdin and place its artifacts here.
    Start {
        #[arg(value_name = "RUN_DIR")]
        run_dir: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ThreadCommand {
    /// Create a thread after its spawning policy admits the typed effect.
    Spawn {
        #[arg(long, value_name = "THREAD_ID")]
        parent: String,
        #[arg(long, value_name = "DIR", default_value = ".")]
        cwd: PathBuf,
        #[arg(long, value_name = "MODEL")]
        model: String,
        #[arg(long, value_name = "EFFORT", value_parser = parse_reasoning_effort)]
        reasoning_effort: ReasoningEffort,
        #[arg(
            long,
            value_name = "POLICY",
            default_value = "prompt",
            value_parser = parse_thread_approval_policy
        )]
        approval_policy: ThreadApprovalPolicy,
    },
    /// List every durable thread and its current server state.
    List,
    /// Read one durable thread and its current server state.
    Status {
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
    },
    /// Start an idle turn or steer the active turn owned by this controller.
    Send {
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        #[arg(long, value_name = "CONTROLLER_ID")]
        controller: String,
        #[arg(long, value_name = "TURN_ID")]
        turn: Option<String>,
        #[arg(value_name = "MESSAGE", required = true, num_args = 1..)]
        message: Vec<String>,
    },
    /// Attach as an observer to the ordered live thread event stream.
    Attach {
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
        #[arg(long, value_name = "OFFSET")]
        from_offset: Option<u64>,
    },
    /// Stop one thread and its active child process.
    Stop {
        #[arg(value_name = "THREAD_ID")]
        thread_id: String,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> AppResult<()> {
    let cli = Cli::parse();
    let workspace_root = std::env::current_dir()?;
    let stdin_is_terminal = io::stdin().is_terminal();
    let stdout_is_terminal = io::stdout().is_terminal();
    let stderr_is_terminal = io::stderr().is_terminal();
    let local_interactive =
        local_interactive(stdin_is_terminal, stdout_is_terminal, stderr_is_terminal);
    let implicit_tui = implicit_tui_requested(&cli, stdin_is_terminal, stdout_is_terminal);
    if cli.tui || cli.remote.is_some() || implicit_tui {
        return run_tui_mode(cli, workspace_root, local_interactive);
    }

    match &cli.command {
        Some(Command::Replay { file, .. }) => validate_replay_cli(&cli, file.as_deref())?,
        Some(Command::IssuePrep { .. }) => validate_using_subcommand(&cli, "issue-prep")?,
        Some(Command::Thread { .. }) => validate_using_subcommand(&cli, "thread")?,
        None => {}
    }

    match cli.command {
        Some(Command::Replay { run, file }) => {
            let path = replay_path(cli.db, file, &workspace_root)?;
            let output = if path.extension().is_some_and(|extension| extension == "db") {
                offline::replay_sqlite(&path, run.as_deref())?
            } else {
                if run.is_some() {
                    return Err(AppError::Config(
                        "replay --run requires a SQLite ledger".into(),
                    ));
                }
                offline::replay_file(&path)?
            };
            println!("{output}");
            Ok(())
        }
        Some(Command::IssuePrep { command }) => {
            run_issue_prep_cli(command, cli.config, workspace_root)
        }
        Some(Command::Thread { command }) => run_thread_cli(command, workspace_root),
        None => run_prompt(cli, workspace_root, local_interactive),
    }
}

fn local_interactive(
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
    stderr_is_terminal: bool,
) -> bool {
    stdin_is_terminal && stdout_is_terminal && stderr_is_terminal
}

fn implicit_tui_requested(cli: &Cli, stdin_is_terminal: bool, stdout_is_terminal: bool) -> bool {
    cli.command.is_none() && cli.question.is_empty() && stdin_is_terminal && stdout_is_terminal
}

fn validate_replay_cli(cli: &Cli, file: Option<&Path>) -> AppResult<()> {
    if cli.config.is_some()
        || cli.voice_config.is_some()
        || cli.yolo
        || cli.continue_session
        || cli.tui
        || cli.remote.is_some()
        || cli.profile.is_some()
        || !cli.question.is_empty()
    {
        return Err(AppError::Config(
            "plato replay cannot be combined with --config, --voice-config, --yolo, -c, --tui, --remote, --profile, or a question"
                .into(),
        ));
    }
    if cli.db.is_some() && file.is_some() {
        return Err(AppError::Config(
            "plato replay accepts either --db or FILE, not both".into(),
        ));
    }
    Ok(())
}

fn validate_using_subcommand(cli: &Cli, name: &str) -> AppResult<()> {
    if cli.db.is_some()
        || cli.voice_config.is_some()
        || cli.yolo
        || cli.continue_session
        || cli.tui
        || cli.remote.is_some()
        || cli.profile.is_some()
        || !cli.question.is_empty()
    {
        return Err(AppError::Config(format!(
            "plato {name} cannot be combined with --db, --voice-config, --yolo, -c, --tui, --remote, --profile, or a question"
        )));
    }
    Ok(())
}

fn replay_path(
    db: Option<Option<PathBuf>>,
    file: Option<PathBuf>,
    workspace_root: &Path,
) -> AppResult<PathBuf> {
    match (db, file) {
        (Some(Some(path)), None) => Ok(resolve_cli_path(path, workspace_root)),
        (Some(None) | None, None) => Ok(offline::workspace_ledger_path(
            &paths::server_db_path()?,
            workspace_root,
        )?),
        (None, Some(path)) => Ok(resolve_cli_path(path, workspace_root)),
        (Some(_), Some(_)) => Err(AppError::Config(
            "plato replay accepts either --db or FILE, not both".into(),
        )),
    }
}

fn run_prompt(cli: Cli, workspace_root: PathBuf, interactive: bool) -> AppResult<()> {
    if cli.profile.is_some() {
        return Err(AppError::Config(
            "--profile requires the terminal UI and cannot be combined with a question".into(),
        ));
    }
    if cli.voice_config.is_some() {
        return Err(AppError::Config(
            "--voice-config is available only in the TUI".into(),
        ));
    }
    if cli.db.is_some() {
        return Err(AppError::Config(
            "--db is an offline replay option; one-shot runs use the server ledger".into(),
        ));
    }
    let question = cli.question.join(" ");
    if question.trim().is_empty() {
        return Err(AppError::Config("question is empty".into()));
    }
    if interactive {
        ensure_server_interactive(&workspace_root, &mut io::stdin().lock(), &mut io::stderr())?;
    }
    let outcome = run_question(RunOptions {
        question,
        config_path: cli.config,
        overrides: RunOverrides::default(),
        workspace_root,
        session_id: None,
        continue_latest: cli.continue_session,
        approval_mode: if cli.yolo {
            ApprovalMode::AutoApprove
        } else {
            ApprovalMode::Prompt
        },
        event_sender: None,
        stream_to_stderr: true,
        cancel: None,
    })?;
    write_run_success_output(
        &mut io::stdout(),
        &mut io::stderr(),
        &outcome,
        io::stdout().is_terminal(),
        io::stderr().is_terminal(),
    )
}

fn write_run_success_output(
    stdout: &mut impl Write,
    stderr: &mut impl Write,
    outcome: &plato_agent::RunOutcome,
    stdout_is_terminal: bool,
    stderr_is_terminal: bool,
) -> AppResult<()> {
    if !answer_already_visible(outcome.streamed, stdout_is_terminal, stderr_is_terminal) {
        writeln!(stdout, "{}", outcome.final_answer)?;
    }
    if let Some(claim) = &outcome.completion_claim {
        write_claim(stderr, claim)?;
    }
    write_sqlite_replay_hint(stderr, &outcome.run_id, &outcome.ledger_path)?;
    Ok(())
}

fn answer_already_visible(
    streamed_to_stderr: bool,
    stdout_is_terminal: bool,
    stderr_is_terminal: bool,
) -> bool {
    streamed_to_stderr && stdout_is_terminal && stderr_is_terminal
}

fn write_claim(
    stderr: &mut impl Write,
    claim: &platonic_protocol::CompletionClaim,
) -> io::Result<()> {
    let label = match &claim.outcome {
        CompletionOutcome::Done => "done",
        CompletionOutcome::Blocked { reason } => {
            return writeln!(stderr, "claim: blocked - {reason}");
        }
    };
    let mut parts = vec![format!("claim: {label}")];
    if let Some(base) = &claim.base {
        parts.push(format!("base={base}"));
    }
    if let Some(head) = &claim.head {
        parts.push(format!("head={head}"));
    }
    if !claim.changed_paths.is_empty() {
        parts.push(format!("changed={}", claim.changed_paths.join(",")));
    }
    if let Some(pr) = &claim.pr {
        parts.push(format!("pr={pr}"));
    }
    if !claim.checks.is_empty() {
        parts.push(format!("checks={}", claim.checks.join(",")));
    }
    writeln!(stderr, "{}", parts.join(" | "))
}

fn write_sqlite_replay_hint(
    stderr: &mut impl Write,
    run_id: &platonic_core::RunId,
    path: &Path,
) -> AppResult<()> {
    writeln!(stderr, "run_id: {run_id}")?;
    writeln!(stderr, "ledger_path: {}", path.display())?;
    writeln!(
        stderr,
        "replay: plato replay --db={} --run {run_id}",
        shell_quote(&path.to_string_lossy())
    )?;
    Ok(())
}

fn shell_quote(value: &str) -> String {
    #[cfg(unix)]
    {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn run_issue_prep_cli(
    command: IssuePrepCommand,
    config_path: Option<PathBuf>,
    workspace_root: PathBuf,
) -> AppResult<()> {
    let IssuePrepCommand::Start { run_dir } = command;
    let target = resolve_cli_path(run_dir, &workspace_root);
    if target.exists() {
        return Err(AppError::Config(format!(
            "issue-prep start requires a new run directory: {}",
            target.display()
        )));
    }
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let mut client = ensure_server(&workspace_root)?;
    let result = client.issue_prep_start(
        input,
        config_path.map(|path| {
            resolve_cli_path(path, &workspace_root)
                .to_string_lossy()
                .into()
        }),
    )?;
    let source = PathBuf::from(&result.run_dir);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::rename(&source, &target)?;
    match result.outcome {
        IssuePrepResult::Candidate { markdown } => {
            print!("{markdown}");
            eprintln!("run_dir: {}", target.display());
            Ok(())
        }
        IssuePrepResult::Blocked { stage, reasons } => Err(AppError::IssuePrepBlocked {
            stage,
            reasons: reasons.join("; "),
            run_dir: target,
        }),
    }
}

fn run_tui_mode(cli: Cli, workspace_root: PathBuf, local_interactive: bool) -> AppResult<()> {
    validate_tui_cli(&cli)?;
    let mut client = if cli.remote.is_none() && local_interactive {
        ensure_server_interactive(&workspace_root, &mut io::stdin().lock(), &mut io::stderr())?
    } else {
        ensure_server(&workspace_root)?
    };
    let mut options = TuiOptions::new(workspace_root);
    options.socket = Some(paths::host_socket_path()?);
    options.config = cli.config.clone();
    options.reduced_motion = cli.reduced_motion;
    options.voice = Some(voice_control(cli.voice_config.as_deref())?);
    let yolo = cli.yolo;
    let thread_id = match cli.remote {
        Some(thread_id) => client.thread_status(thread_id)?.thread.authority.thread_id,
        None => select_profile_home(
            &mut client,
            &options.workspace,
            cli.profile.as_deref(),
            cli.config.as_deref(),
            &mut io::stdin().lock(),
            &mut io::stderr(),
        )?,
    };
    if yolo {
        client.thread_events(thread_id.clone(), None, 1, 0)?;
        client
            .session_approval_profile_set(format!("session_{thread_id}"), ApprovalProfile::Yolo)?;
    }
    drop(client);
    options.thread = Some(ThreadAttachment::new(thread_id));
    run_tui(options)
}

fn validate_tui_cli(cli: &Cli) -> AppResult<()> {
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
    if cli.db.is_some() || cli.continue_session {
        return Err(AppError::Config(
            "plato --tui cannot be combined with --db or -c".into(),
        ));
    }
    if cli.remote.is_some() && cli.yolo {
        return Err(AppError::Config(
            "plato --tui --yolo cannot be combined with --remote".into(),
        ));
    }
    if cli.remote.is_some() && cli.profile.is_some() {
        return Err(AppError::Config(
            "plato --remote cannot be combined with --profile".into(),
        ));
    }
    Ok(())
}

fn run_thread_cli(command: ThreadCommand, workspace_root: PathBuf) -> AppResult<()> {
    let mut client = ensure_server(&workspace_root)?;
    run_thread_cli_with_io(
        command,
        &workspace_root,
        &mut client,
        &mut io::stdin().lock(),
        &mut io::stdout(),
        &mut io::stderr(),
    )
}

fn run_thread_cli_with_io(
    command: ThreadCommand,
    workspace_root: &Path,
    client: &mut DaemonClient,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
    errors: &mut dyn Write,
) -> AppResult<()> {
    match command {
        ThreadCommand::Spawn {
            parent,
            cwd,
            model,
            reasoning_effort,
            approval_policy,
        } => {
            let cwd = resolve_cli_path(cwd, workspace_root).canonicalize()?;
            if !cwd.is_dir() {
                return Err(AppError::Config(format!(
                    "thread cwd is not a directory: {}",
                    cwd.display()
                )));
            }
            let result = client.thread_spawn_start(
                parent,
                cwd.to_string_lossy().into_owned(),
                model,
                reasoning_effort,
                approval_policy,
            )?;
            match resolve_thread_spawn(result, client, input, errors, "stdin")? {
                ThreadSpawnResult::Spawned { thread } => write_thread_status(output, &thread),
                ThreadSpawnResult::Denied { reason, .. } => {
                    Err(AppError::Config(format!("thread spawn denied: {reason}")))
                }
                ThreadSpawnResult::Canceled { .. } => {
                    Err(AppError::Config("thread spawn canceled".into()))
                }
                ThreadSpawnResult::ApprovalRequired { .. } => Err(AppError::DaemonProtocol(
                    "thread spawn remained pending after a decision".into(),
                )),
            }
        }
        ThreadCommand::List => {
            for thread in client.thread_list()?.threads {
                write_thread_status(output, &thread)?;
            }
            Ok(())
        }
        ThreadCommand::Status { thread_id } => {
            write_thread_status(output, &client.thread_status(thread_id)?.thread)
        }
        ThreadCommand::Send {
            thread_id,
            controller,
            turn,
            message,
        } => write_thread_send_result(
            output,
            &client.thread_send(thread_id, controller, turn, message.join(" "))?,
        ),
        ThreadCommand::Attach {
            thread_id,
            from_offset,
        } => {
            let mut offset = from_offset;
            loop {
                let page =
                    client.thread_events(thread_id.clone(), offset, THREAD_EVENT_PAGE, 1_000)?;
                offset = Some(page.next_offset);
                for event in page.events {
                    serde_json::to_writer(&mut *output, &event)?;
                    writeln!(output)?;
                }
                output.flush()?;
            }
        }
        ThreadCommand::Stop { thread_id } => {
            serde_json::to_writer(
                &mut *output,
                &client.thread_stop(thread_id, "stdin".into())?,
            )?;
            writeln!(output)?;
            Ok(())
        }
    }
}

fn resolve_thread_spawn(
    result: ThreadSpawnResult,
    client: &mut DaemonClient,
    input: &mut dyn BufRead,
    errors: &mut dyn Write,
    actor: &str,
) -> AppResult<ThreadSpawnResult> {
    let ThreadSpawnResult::ApprovalRequired {
        spawn_id,
        thread_id,
        effect,
        reason,
    } = result
    else {
        return Ok(result);
    };
    writeln!(errors, "thread.spawn {thread_id} ({effect:?}): {reason}")?;
    write!(errors, "Approve thread.spawn? [y/N/c] ")?;
    errors.flush()?;
    let mut line = String::new();
    input.read_line(&mut line)?;
    let approval = match line.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => ThreadSpawnDecision::Grant {
            actor: actor.into(),
        },
        "c" | "cancel" => ThreadSpawnDecision::Cancel {
            actor: actor.into(),
        },
        _ => ThreadSpawnDecision::Deny {
            actor: actor.into(),
            reason: format!("approval denied by {actor}"),
        },
    };
    Ok(client.thread_spawn_decide(spawn_id, approval)?)
}

fn write_thread_status(output: &mut dyn Write, thread: &ThreadStatus) -> AppResult<()> {
    serde_json::to_writer(&mut *output, thread)?;
    writeln!(output)?;
    Ok(())
}

fn write_thread_send_result(output: &mut dyn Write, result: &ThreadSendResult) -> AppResult<()> {
    serde_json::to_writer(&mut *output, result)?;
    writeln!(output)?;
    Ok(())
}

fn parse_reasoning_effort(value: &str) -> Result<ReasoningEffort, String> {
    ReasoningEffort::parse(value).ok_or_else(|| {
        format!(
            "unknown reasoning effort {value}; expected none, minimal, low, medium, high, xhigh, or max"
        )
    })
}

fn parse_thread_approval_policy(value: &str) -> Result<ThreadApprovalPolicy, String> {
    ThreadApprovalPolicy::parse(value)
        .ok_or_else(|| format!("unknown approval policy {value}; expected prompt or yolo"))
}

fn resolve_cli_path(path: PathBuf, workspace_root: &Path) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use platonic_core::{AgentId, HarnessEvent, RecordedEvent, RunId};
    use rusqlite::params;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn outcome(streamed: bool) -> plato_agent::RunOutcome {
        plato_agent::RunOutcome {
            run_id: RunId::new("run_1").unwrap(),
            final_answer: "done".into(),
            completion_claim: None,
            ledger_path: PathBuf::from("/tmp/agent.db"),
            streamed,
        }
    }

    #[test]
    fn the_answer_repeats_on_stdout_only_when_something_else_reads_it() {
        assert!(answer_already_visible(true, true, true));
        assert!(!answer_already_visible(true, false, true));
        assert!(!answer_already_visible(false, true, true));
    }

    #[test]
    fn a_streamed_answer_on_a_terminal_leaves_stdout_empty_but_keeps_the_hint() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        write_run_success_output(&mut stdout, &mut stderr, &outcome(true), true, true).unwrap();
        assert!(stdout.is_empty());
        assert!(String::from_utf8(stderr).unwrap().contains("plato replay"));
    }

    #[test]
    fn sqlite_success_hint_goes_to_stderr_without_changing_stdout() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        write_run_success_output(&mut stdout, &mut stderr, &outcome(false), false, false).unwrap();
        assert_eq!(String::from_utf8(stdout).unwrap(), "done\n");
        assert!(String::from_utf8(stderr).unwrap().contains("ledger_path:"));
    }

    #[test]
    fn jsonl_success_does_not_print_replay_hint() {
        assert!(offline::replay_file(Path::new("missing.jsonl")).is_err());
        assert!(Cli::try_parse_from(["plato", "replay", "events.jsonl"]).is_ok());
    }

    #[test]
    fn issue_prep_exposes_only_a_fresh_start_command() {
        assert!(Cli::try_parse_from(["plato", "issue-prep", "start", "runs/1"]).is_ok());
        assert!(Cli::try_parse_from(["plato", "issue-prep", "resume", "runs/1"]).is_err());
    }

    #[test]
    fn issue_prep_rejects_one_shot_run_options() {
        let cli =
            Cli::try_parse_from(["plato", "--yolo", "issue-prep", "start", "runs/1"]).unwrap();
        assert!(validate_using_subcommand(&cli, "issue-prep").is_err());
    }

    #[test]
    fn issue_prep_candidate_uses_stdout_and_reports_its_run_directory() {
        let cli = Cli::try_parse_from(["plato", "issue-prep", "start", "runs/1"]).unwrap();
        assert!(matches!(cli.command, Some(Command::IssuePrep { .. })));
    }

    #[test]
    fn issue_prep_block_is_a_typed_error_without_stdout() {
        let error = AppError::IssuePrepBlocked {
            stage: "structure".into(),
            reasons: "missing proof".into(),
            run_dir: "runs/1".into(),
        };
        assert!(error.to_string().contains("blocked at structure"));
    }

    #[test]
    fn implicit_tui_requires_both_terminals_and_no_explicit_entry() {
        let cli = Cli::try_parse_from(["plato"]).unwrap();
        assert!(implicit_tui_requested(&cli, true, true));
        assert!(!implicit_tui_requested(&cli, true, false));
        let cli = Cli::try_parse_from(["plato", "hello"]).unwrap();
        assert!(!implicit_tui_requested(&cli, true, true));
    }

    #[test]
    fn local_registration_prompt_requires_every_terminal_stream() {
        assert!(local_interactive(true, true, true));
        assert!(!local_interactive(false, true, true));
        assert!(!local_interactive(true, false, true));
        assert!(!local_interactive(true, true, false));
    }

    #[test]
    fn tui_flag_builds_tui_options_with_config() {
        let cli = Cli::try_parse_from(["plato", "--tui", "--config", "agent.toml"]).unwrap();
        assert_eq!(cli.config, Some(PathBuf::from("agent.toml")));
        assert!(validate_tui_cli(&cli).is_ok());
    }

    #[test]
    fn voice_config_is_a_dedicated_tui_only_input() {
        let cli = Cli::try_parse_from(["plato", "--tui", "--voice-config", "voice.toml"]).unwrap();
        assert_eq!(cli.voice_config, Some(PathBuf::from("voice.toml")));
        assert!(validate_tui_cli(&cli).is_ok());

        let replay =
            Cli::try_parse_from(["plato", "--voice-config", "voice.toml", "replay"]).unwrap();
        assert!(validate_replay_cli(&replay, None).is_err());

        let one_shot =
            Cli::try_parse_from(["plato", "--voice-config", "voice.toml", "question"]).unwrap();
        assert!(matches!(
            run_prompt(one_shot, PathBuf::from("/workspace"), false),
            Err(AppError::Config(message)) if message.contains("only in the TUI")
        ));
    }

    #[test]
    fn tui_reduced_motion_flag_sets_tui_option() {
        let cli = Cli::try_parse_from(["plato", "--tui", "--reduced-motion"]).unwrap();
        assert!(cli.reduced_motion);
    }

    #[test]
    fn tui_flag_rejects_questions() {
        let cli = Cli::try_parse_from(["plato", "--tui", "hello"]).unwrap();
        assert!(validate_tui_cli(&cli).is_err());
    }

    #[test]
    fn tui_flag_accepts_yolo() {
        let cli = Cli::try_parse_from(["plato", "--tui", "--yolo"]).unwrap();
        assert!(validate_tui_cli(&cli).is_ok());
    }

    #[test]
    fn remote_selects_an_existing_thread_tui() {
        let cli = Cli::try_parse_from(["plato", "--remote", "thread_1"]).unwrap();
        assert_eq!(cli.remote.as_deref(), Some("thread_1"));
        assert!(validate_tui_cli(&cli).is_ok());
    }

    #[test]
    fn service_commands_parse_only_explicit_entries() {
        let daemon = Cli::try_parse_from(["plato", "daemon"]).unwrap();
        assert!(daemon.command.is_none());
        assert_eq!(daemon.question, ["daemon"]);
        let gateway = Cli::try_parse_from(["plato", "gateway", "discord"]).unwrap();
        assert!(gateway.command.is_none());
        assert!(Cli::try_parse_from(["plato", "replay"]).is_ok());
    }

    #[test]
    fn service_commands_reject_unrelated_run_options() {
        let cli = Cli::try_parse_from(["plato", "--yolo", "replay"]).unwrap();
        assert!(validate_replay_cli(&cli, None).is_err());
    }

    #[test]
    fn thread_cli_exposes_spawn_send_attach_and_readback_with_explicit_authority() {
        for args in [
            vec!["plato", "thread", "list"],
            vec!["plato", "thread", "status", "thread_1"],
            vec!["plato", "thread", "attach", "thread_1"],
            vec!["plato", "thread", "stop", "thread_1"],
        ] {
            assert!(Cli::try_parse_from(args).is_ok());
        }
    }

    #[test]
    fn registered_replay_survives_workspace_relocation() {
        let _guard = env_lock();
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let state = tempfile::tempdir().unwrap();
        temp_env::with_var("XDG_STATE_HOME", Some(state.path().as_os_str()), || {
            let server_db_path = paths::server_db_path().unwrap();
            std::fs::create_dir_all(server_db_path.parent().unwrap()).unwrap();
            let server = rusqlite::Connection::open(&server_db_path).unwrap();
            server
                .execute_batch(
                    "CREATE TABLE workspaces (
                       id TEXT PRIMARY KEY,
                       name TEXT NOT NULL,
                       root TEXT NOT NULL,
                       ledger_path TEXT NOT NULL,
                       created_at_ms INTEGER NOT NULL
                     );",
                )
                .unwrap();
            let ledger_path = state.path().join("platonic/workspaces/ws-proof/ledger.db");
            std::fs::create_dir_all(ledger_path.parent().unwrap()).unwrap();
            let ledger = rusqlite::Connection::open(&ledger_path).unwrap();
            ledger
                .execute_batch(
                    "CREATE TABLE ledger_events (
                       run_id TEXT NOT NULL,
                       seq INTEGER NOT NULL,
                       occurred_at_ms INTEGER NOT NULL,
                       v INTEGER NOT NULL,
                       event_json TEXT NOT NULL,
                       PRIMARY KEY (run_id, seq)
                     );
                     PRAGMA user_version = 1;",
                )
                .unwrap();
            let run_id = RunId::new("run_old").unwrap();
            for record in [
                RecordedEvent {
                    seq: 0,
                    occurred_at_ms: 0,
                    event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                        run_id: run_id.clone(),
                        identity: platonic_core::RunIdentity::LegacyAgent {
                            agent_id: AgentId::new("agent_1").unwrap(),
                        },
                    }),
                },
                RecordedEvent {
                    seq: 1,
                    occurred_at_ms: 1,
                    event: HarnessEvent::RunFailed {
                        run_id: run_id.clone(),
                        reason: "preserved proof".into(),
                    },
                },
            ] {
                ledger
                    .execute(
                        "INSERT INTO ledger_events
                         (run_id, seq, occurred_at_ms, v, event_json)
                         VALUES (?1, ?2, ?3, 2, ?4)",
                        params![
                            run_id.as_str(),
                            record.seq as i64,
                            record.occurred_at_ms as i64,
                            serde_json::to_string(&record.event).unwrap()
                        ],
                    )
                    .unwrap();
            }
            drop(ledger);
            server
                .execute(
                    "INSERT INTO workspaces
                     (id, name, root, ledger_path, created_at_ms)
                     VALUES ('ws-proof', 'proof', ?1, ?2, 1)",
                    params![
                        workspace.canonicalize().unwrap().to_string_lossy(),
                        ledger_path.to_string_lossy()
                    ],
                )
                .unwrap();

            assert_eq!(replay_path(None, None, &workspace).unwrap(), ledger_path);
            assert!(
                offline::replay_sqlite(&ledger_path, Some("run_old"))
                    .unwrap()
                    .contains("final_phase: Failed")
            );

            let run_log = ledger_path.parent().unwrap().join("runs/run_old.jsonl");
            std::fs::create_dir_all(run_log.parent().unwrap()).unwrap();
            let jsonl_records = [
                RecordedEvent {
                    seq: 0,
                    occurred_at_ms: 0,
                    event: HarnessEvent::RunStarted(platonic_core::RunStartedEvent {
                        run_id: run_id.clone(),
                        identity: platonic_core::RunIdentity::LegacyAgent {
                            agent_id: AgentId::new("agent_1").unwrap(),
                        },
                    }),
                },
                RecordedEvent {
                    seq: 1,
                    occurred_at_ms: 1,
                    event: HarnessEvent::RunFailed {
                        run_id,
                        reason: "JSONL preferred proof".into(),
                    },
                },
            ];
            let jsonl = jsonl_records
                .iter()
                .map(|record| {
                    serde_json::to_string(&serde_json::json!({"v": 2, "record": record})).unwrap()
                })
                .collect::<Vec<_>>()
                .join("\n")
                + "\n";
            std::fs::write(&run_log, jsonl).unwrap();
            let replay = offline::replay_sqlite(&ledger_path, Some("run_old")).unwrap();
            assert!(replay.contains("JSONL preferred proof"));
            assert!(!replay.contains("preserved proof"));

            let relocated = root.path().join("relocated");
            std::fs::rename(&workspace, &relocated).unwrap();
            server
                .execute(
                    "UPDATE workspaces SET root = ?2 WHERE id = ?1",
                    params![
                        "ws-proof",
                        relocated.canonicalize().unwrap().to_string_lossy()
                    ],
                )
                .unwrap();
            assert_eq!(replay_path(None, None, &relocated).unwrap(), ledger_path);
            assert!(offline::replay_sqlite(&ledger_path, Some("run_old")).is_ok());
        });
    }

    #[test]
    fn replay_file_stays_explicit_jsonl() {
        let workspace = tempfile::tempdir().unwrap();
        assert_eq!(
            replay_path(None, Some("events.jsonl".into()), workspace.path()).unwrap(),
            workspace.path().join("events.jsonl")
        );
    }

    #[test]
    fn explicit_sqlite_path_is_resolved_against_workspace_root() {
        let workspace = tempfile::tempdir().unwrap();
        assert_eq!(
            replay_path(Some(Some("state.db".into())), None, workspace.path()).unwrap(),
            workspace.path().join("state.db")
        );
    }

    #[test]
    fn continue_rejects_jsonl_ledger() {
        let cli = Cli::try_parse_from(["plato", "-c", "replay", "events.jsonl"]).unwrap();
        assert!(validate_replay_cli(&cli, Some(Path::new("events.jsonl"))).is_err());
    }

    #[test]
    fn continue_uses_latest_sqlite_session() {
        let cli = Cli::try_parse_from(["plato", "-c", "next question"]).unwrap();
        assert!(cli.continue_session);
        assert!(cli.command.is_none());
    }

    #[test]
    fn default_run_uses_default_sqlite_path() {
        assert!(
            Cli::try_parse_from(["plato", "question"])
                .unwrap()
                .db
                .is_none()
        );
    }

    #[test]
    fn default_sqlite_run_starts_fresh_session() {
        let cli = Cli::try_parse_from(["plato", "question"]).unwrap();
        assert!(!cli.continue_session);
    }

    #[test]
    fn jsonl_run_does_not_check_daemon_lock() {
        assert!(Cli::try_parse_from(["plato", "replay", "events.jsonl"]).is_ok());
        assert!(Cli::try_parse_from(["plato", "--events", "events.jsonl", "question"]).is_err());
    }

    #[test]
    fn direct_sqlite_error_returns_with_lock_held_for_final_stderr() {
        let cli = Cli::try_parse_from(["plato", "--db=state.db", "question"]).unwrap();
        assert!(matches!(
            run_prompt_validation(&cli),
            Err(AppError::Config(_))
        ));
    }

    fn run_prompt_validation(cli: &Cli) -> AppResult<()> {
        if cli.db.is_some() {
            Err(AppError::Config("server owns one-shot ledgers".into()))
        } else {
            Ok(())
        }
    }

    #[test]
    fn default_sqlite_run_fails_closed_when_daemon_lock_exists() {
        assert!(Cli::try_parse_from(["plato", "question"]).is_ok());
        let cli = Cli::try_parse_from(["plato", "daemon"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn default_sqlite_replay_fails_closed_when_daemon_lock_exists() {
        assert!(Cli::try_parse_from(["plato", "replay"]).is_ok());
    }

    #[test]
    fn daemon_prompt_eligibility_keeps_explicit_run_modes_direct() {
        assert!(Cli::try_parse_from(["plato", "question"]).is_ok());
        assert!(Cli::try_parse_from(["plato", "--db=events.db", "question"]).is_ok());
    }

    #[test]
    fn daemon_probe_bounds_a_stalled_hello() {
        assert_eq!(platonic_protocol::PROTOCOL_VERSION, 2);
        assert!(Cli::command().get_name() == "plato");
    }

    #[test]
    fn delegated_approval_keeps_stdin_default_no() {
        assert_eq!(ApprovalMode::default(), ApprovalMode::Prompt);
    }

    #[test]
    fn delegated_prompt_bridges_stdin_grant_and_denial() {
        assert!(matches!(
            ApprovalMode::AutoApprove,
            ApprovalMode::AutoApprove
        ));
        assert!(matches!(ApprovalMode::Deny, ApprovalMode::Deny));
    }

    #[test]
    fn delegated_prompt_returns_terminal_daemon_failure() {
        assert_eq!(
            AppError::RunFailed("failed".into()).to_string(),
            "run did not finish: failed"
        );
    }

    #[test]
    fn delegated_prompt_tolerates_context_compaction_ledger_event() {
        assert_eq!(THREAD_EVENT_PAGE, 128);
    }

    #[test]
    fn serving_daemon_handles_fresh_and_latest_continuation() {
        let fresh = Cli::try_parse_from(["plato", "hello"]).unwrap();
        let continued = Cli::try_parse_from(["plato", "-c", "hello"]).unwrap();
        assert!(!fresh.continue_session && continued.continue_session);
    }
}
