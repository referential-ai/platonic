use plato_agent::{
    daemon::{
        client::DaemonClient,
        protocol::{RunStateName, ShutdownIfIdleResultName, StreamEvent},
    },
    ledger::SqliteLedger,
    paths,
};
use platonic_core::{
    ActorId, AgentId, ContextPack, EffectClass, HarnessEvent, Message, MessageRole, ModelName,
    ModelUsage, PolicyDecision, ReadbackEntry, RecordedEvent, ResultVisibility, RunId, RunPhase,
    RunReadback, ToolCall, ToolCallId, ToolName, ToolProposal, ToolResult, TurnId,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

const API_KEY_ENV: &str = "PLATO_SEMANTIC_CONFORMANCE_TEST_KEY";
const DENIAL_REASON: &str = "approval denied by stdin";
const FIXTURE_INITIAL: &str = "fixture baseline\n";
const FIXTURE_WRITTEN: &str = "fixture changed\n";
const SERVED_MODEL: &str = "provider/test-model-2026-08-01";
const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const REQUESTS_PER_LEG: usize = 2;
static SCENARIO_SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn read_only_success_is_semantically_conformant() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    run_scenario(Scenario::ReadOnly);
}

#[test]
fn approved_workspace_write_is_semantically_conformant() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    run_scenario(Scenario::ApprovedWrite);
}

#[test]
fn denied_workspace_write_is_semantically_conformant() {
    let _serial = SCENARIO_SERIAL.lock().unwrap();
    run_scenario(Scenario::DeniedWrite);
}

#[test]
fn normalizer_accepts_only_host_variance_and_keeps_ids_bijective() {
    let direct = sample_evidence(SampleApproval::Granted);
    let mut daemon = direct.clone();
    for (index, record) in daemon.records.iter_mut().enumerate() {
        record.occurred_at_ms = 9_000 + index as u64;
    }
    mutate_host_ids(&mut daemon.records, |kind, id| {
        *id = format!("daemon_{}_{}", kind.label(), id);
    });
    set_approval_actor(&mut daemon.records, "daemon");

    assert_semantically_equivalent(&direct, &daemon).unwrap();

    let mut collapsed = daemon.clone();
    mutate_host_ids(&mut collapsed.records, |kind, id| {
        if kind == IdKind::Turn && id.ends_with("turn_2") {
            *id = "daemon_turn_turn_1".into();
        }
    });
    assert_difference(&direct, &collapsed, "persisted event semantics");

    let mut non_transport_actor = daemon;
    set_approval_actor(&mut non_transport_actor.records, "automation");
    assert_difference(&direct, &non_transport_actor, "persisted event semantics");
}

#[test]
fn normalizer_rejects_changed_policy_outcome_and_event_order() {
    let direct = sample_evidence(SampleApproval::Granted);
    let mut policy_changed = direct.clone();
    let decision = policy_changed
        .records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            HarnessEvent::PolicyEvaluated { decision, .. } => Some(decision),
            _ => None,
        })
        .unwrap();
    *decision = PolicyDecision::Deny {
        reason: "changed policy outcome".into(),
    };
    assert_difference(&direct, &policy_changed, "persisted event semantics");

    let mut reordered = direct.clone();
    reordered.records.swap(1, 2);
    assert_difference(&direct, &reordered, "persisted event semantics");
}

#[test]
fn semantic_comparison_rejects_changed_final_phase_and_usage_known_state() {
    let direct = sample_evidence(SampleApproval::Granted);
    let mut phase_changed = direct.clone();
    phase_changed.final_phase = RunPhase::Failed {
        reason: "changed final phase".into(),
    };
    assert_difference(&direct, &phase_changed, "final phase");

    let mut usage_changed = direct.clone();
    usage_changed.usage_known[0] = false;
    assert_difference(&direct, &usage_changed, "usage-known state");
}

#[test]
fn normalizer_rejects_changed_approval_outcome_and_denial_reason() {
    let granted = sample_evidence(SampleApproval::Granted);
    let mut outcome_changed = granted.clone();
    let approval_index = outcome_changed
        .records
        .iter()
        .position(|record| matches!(record.event, HarnessEvent::ApprovalGranted { .. }))
        .unwrap();
    let replacement = match &outcome_changed.records[approval_index].event {
        HarnessEvent::ApprovalGranted {
            run_id,
            call_id,
            actor_id,
        } => HarnessEvent::ApprovalDenied {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            actor_id: actor_id.clone(),
            reason: DENIAL_REASON.into(),
        },
        _ => unreachable!(),
    };
    outcome_changed.records[approval_index].event = replacement;
    assert_difference(&granted, &outcome_changed, "persisted event semantics");

    let denied = sample_evidence(SampleApproval::Denied);
    let mut reason_changed = denied.clone();
    let reason = reason_changed
        .records
        .iter_mut()
        .find_map(|record| match &mut record.event {
            HarnessEvent::ApprovalDenied { reason, .. } => Some(reason),
            _ => None,
        })
        .unwrap();
    *reason = "different denial reason".into();
    assert_difference(&denied, &reason_changed, "persisted event semantics");
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    ReadOnly,
    ApprovedWrite,
    DeniedWrite,
}

impl Scenario {
    fn question(self) -> &'static str {
        match self {
            Self::ReadOnly => "read the fixture",
            Self::ApprovedWrite => "write the fixture with approval",
            Self::DeniedWrite => "try to write the fixture without approval",
        }
    }

    fn answer(self) -> &'static str {
        match self {
            Self::ReadOnly => "The fixture contains its baseline text.",
            Self::ApprovedWrite => "The fixture was changed.",
            Self::DeniedWrite => "The fixture was not changed.",
        }
    }

    fn provider_tool(self) -> (&'static str, Value) {
        match self {
            Self::ReadOnly => ("file_read", json!({"path": "fixture.txt"})),
            Self::ApprovedWrite | Self::DeniedWrite => (
                "file_write",
                json!({"path": "fixture.txt", "content": FIXTURE_WRITTEN}),
            ),
        }
    }

    fn approval(self) -> ScenarioApproval {
        match self {
            Self::ReadOnly => ScenarioApproval::None,
            Self::ApprovedWrite => ScenarioApproval::Grant,
            Self::DeniedWrite => ScenarioApproval::Deny,
        }
    }

    fn usage(self) -> (UsageFixture, UsageFixture) {
        match self {
            Self::ReadOnly => (UsageFixture::Known(13, 3), UsageFixture::Known(21, 7)),
            Self::ApprovedWrite => (UsageFixture::Unknown, UsageFixture::Unknown),
            Self::DeniedWrite => (UsageFixture::Known(0, 0), UsageFixture::Known(0, 0)),
        }
    }

    fn expected_usage_known(self) -> Vec<bool> {
        let (first, second) = self.usage();
        vec![first.is_known(), second.is_known()]
    }

    fn expected_fixture(self) -> &'static str {
        match self {
            Self::ApprovedWrite => FIXTURE_WRITTEN,
            Self::ReadOnly | Self::DeniedWrite => FIXTURE_INITIAL,
        }
    }

    fn provider_replies(self) -> Vec<ProviderReply> {
        let (tool_name, input) = self.provider_tool();
        let (tool_usage, answer_usage) = self.usage();
        let tool = ProviderReply::tool_call(tool_name, input, tool_usage);
        let answer = ProviderReply::answer(self.answer(), answer_usage);
        vec![tool.clone(), answer.clone(), tool, answer]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScenarioApproval {
    None,
    Grant,
    Deny,
}

fn run_scenario(scenario: Scenario) {
    let proof = ProofContext::new();
    proof.restore_fixture();
    let provider = ScriptedProvider::start(scenario.provider_replies());
    write_provider_config(&proof.config_path, &provider.base_url);

    let direct = run_direct_leg(&proof, scenario);
    assert_eq!(
        direct.fixture,
        scenario.expected_fixture().as_bytes(),
        "{scenario:?} direct fixture effect"
    );

    proof.restore_fixture();
    assert_eq!(
        fs::read(&proof.fixture_path).unwrap(),
        FIXTURE_INITIAL.as_bytes()
    );

    let daemon = ProofDaemon::start(&proof);
    let mut client = daemon.connect();
    let hello = client.hello(&proof.workspace).unwrap();
    assert_eq!(Path::new(&hello.ledger_path), proof.ledger_path);
    let daemon_leg = run_daemon_leg(&proof, scenario, &mut client);
    daemon.stop(client);

    let requests = provider.join();
    assert_eq!(
        requests.len(),
        REQUESTS_PER_LEG * 2,
        "{scenario:?} provider request count"
    );
    let direct = direct.with_provider_requests(requests[..REQUESTS_PER_LEG].to_vec());
    let daemon = daemon_leg.with_provider_requests(requests[REQUESTS_PER_LEG..].to_vec());

    assert_eq!(
        direct.usage_known,
        scenario.expected_usage_known(),
        "{scenario:?} direct usage-known state"
    );
    assert_eq!(
        daemon.usage_known,
        scenario.expected_usage_known(),
        "{scenario:?} daemon usage-known state"
    );
    assert_eq!(served_models(&direct.records), vec![SERVED_MODEL; 2]);
    assert_eq!(served_models(&daemon.records), vec![SERVED_MODEL; 2]);
    assert_approval_transport(scenario, &direct.records, &daemon.records);
    assert_policy_and_effect(scenario, &direct.records);
    assert_policy_and_effect(scenario, &daemon.records);
    assert_semantically_equivalent(&direct, &daemon)
        .unwrap_or_else(|error| panic!("{scenario:?} semantic divergence: {error}"));
}

fn run_direct_leg(proof: &ProofContext, scenario: Scenario) -> RunEvidence {
    proof.assert_no_serving_daemon("before direct CLI run");
    let mut command = proof.plato_command();
    command
        .arg("--config")
        .arg("plato.toml")
        .arg(scenario.question())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    if let Some(mut stdin) = child.stdin.take() {
        match scenario.approval() {
            ScenarioApproval::Grant => stdin.write_all(b"y\n").unwrap(),
            ScenarioApproval::Deny => stdin.write_all(b"n\n").unwrap(),
            ScenarioApproval::None => {}
        }
    }
    let output = child.wait_with_output().unwrap();
    assert_success("direct CLI", &output);
    proof.assert_no_serving_daemon("after direct CLI run");

    let stderr = String::from_utf8(output.stderr).unwrap();
    let run_id = output_field(&stderr, "run_id: ");
    assert_eq!(
        Path::new(&output_field(&stderr, "ledger_path: ")),
        proof.ledger_path
    );
    let answer = String::from_utf8(output.stdout)
        .unwrap()
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    assert_eq!(answer, scenario.answer());

    let records = SqliteLedger::open_readonly(&proof.ledger_path)
        .unwrap()
        .read_run(&run_id)
        .unwrap();
    assert!(
        records
            .iter()
            .all(|record| record.event.run_id().as_str() == run_id)
    );
    RunEvidence::from_leg(records, answer, fs::read(&proof.fixture_path).unwrap())
}

fn run_daemon_leg(
    proof: &ProofContext,
    scenario: Scenario,
    client: &mut DaemonClient,
) -> RunEvidence {
    let started = client
        .run_start(scenario.question().into(), Some("plato.toml".into()), false)
        .unwrap();
    assert_eq!(Path::new(&started.ledger_path), proof.ledger_path);
    let deadline = Instant::now() + PROOF_TIMEOUT;
    let mut approval_decided = false;
    let mut from_offset = Some(0);

    loop {
        let page = client
            .events_stream(&started.run_id, from_offset, 128)
            .unwrap();
        from_offset = Some(page.next_offset);
        for buffered in &page.events {
            if let StreamEvent::ApprovalRequested { tool_call_id, .. } = &buffered.event {
                assert!(!approval_decided, "received duplicate approval request");
                match scenario.approval() {
                    ScenarioApproval::Grant => {
                        client
                            .approval_grant(&started.run_id, tool_call_id)
                            .unwrap();
                    }
                    ScenarioApproval::Deny => {
                        client
                            .approval_deny(&started.run_id, tool_call_id, DENIAL_REASON.into())
                            .unwrap();
                    }
                    ScenarioApproval::None => {
                        panic!("read-only scenario unexpectedly requested approval");
                    }
                }
                approval_decided = true;
            }
        }

        match page.status {
            RunStateName::Finished => break,
            RunStateName::Running | RunStateName::CancelRequested => {}
            status => panic!("daemon run ended as {status}"),
        }
        assert!(
            Instant::now() < deadline,
            "daemon run did not finish for {scenario:?}"
        );
        thread::sleep(POLL_INTERVAL);
    }

    assert_eq!(
        approval_decided,
        scenario.approval() != ScenarioApproval::None
    );
    let transcript = client.transcript_read(&started.run_id).unwrap();
    assert_eq!(transcript.status, RunStateName::Finished);
    let answer = transcript.final_answer.unwrap();
    assert_eq!(answer, scenario.answer());
    let records = SqliteLedger::open_readonly(&proof.ledger_path)
        .unwrap()
        .read_run(&started.run_id)
        .unwrap();
    RunEvidence::from_leg(records, answer, fs::read(&proof.fixture_path).unwrap())
}

#[derive(Clone, Debug)]
struct RunEvidence {
    provider_requests: Vec<Value>,
    records: Vec<RecordedEvent>,
    final_phase: RunPhase,
    final_answer: String,
    usage_known: Vec<bool>,
    fixture: Vec<u8>,
}

impl RunEvidence {
    fn from_leg(records: Vec<RecordedEvent>, reported_answer: String, fixture: Vec<u8>) -> Self {
        let readback = RunReadback::from_events(&records).unwrap();
        let final_answer = readback_answer(&readback);
        assert_eq!(reported_answer, final_answer);
        Self {
            provider_requests: Vec::new(),
            usage_known: usage_known(&records),
            records,
            final_phase: readback.final_phase,
            final_answer,
            fixture,
        }
    }

    fn with_provider_requests(mut self, provider_requests: Vec<Value>) -> Self {
        self.provider_requests = provider_requests;
        self
    }
}

fn readback_answer(readback: &RunReadback) -> String {
    readback
        .entries
        .iter()
        .rev()
        .find_map(|entry| match entry {
            ReadbackEntry::ModelMessage { message, .. }
                if message.role == MessageRole::Assistant =>
            {
                Some(message.content.clone())
            }
            _ => None,
        })
        .expect("finished run has an assistant answer")
}

fn usage_known(records: &[RecordedEvent]) -> Vec<bool> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            HarnessEvent::ModelResponded { usage, .. } => Some(usage.is_some()),
            _ => None,
        })
        .collect()
}

fn served_models(records: &[RecordedEvent]) -> Vec<&str> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            HarnessEvent::ModelResponded {
                served_model: Some(model),
                ..
            } => Some(model.as_str()),
            _ => None,
        })
        .collect()
}

fn assert_semantically_equivalent(
    direct: &RunEvidence,
    daemon: &RunEvidence,
) -> Result<(), String> {
    if direct.provider_requests != daemon.provider_requests {
        return Err("provider requests differ".into());
    }
    if normalize_records(&direct.records) != normalize_records(&daemon.records) {
        return Err("persisted event semantics differ".into());
    }
    if direct.final_phase != daemon.final_phase {
        return Err("final phase differs".into());
    }
    if direct.final_answer != daemon.final_answer {
        return Err("final answer differs".into());
    }
    if direct.usage_known != daemon.usage_known {
        return Err("usage-known state differs".into());
    }
    if direct.fixture != daemon.fixture {
        return Err("workspace fixture effect differs".into());
    }
    Ok(())
}

fn assert_difference(direct: &RunEvidence, changed: &RunEvidence, expected: &str) {
    let error = assert_semantically_equivalent(direct, changed).unwrap_err();
    assert!(
        error.contains(expected),
        "expected {expected:?} difference, got {error:?}"
    );
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum IdKind {
    Run,
    Turn,
    Call,
    Artifact,
}

impl IdKind {
    fn label(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Turn => "turn",
            Self::Call => "call",
            Self::Artifact => "artifact",
        }
    }
}

#[derive(Default)]
struct IdNormalizer {
    canonical: HashMap<(IdKind, String), String>,
    next: HashMap<IdKind, u64>,
}

impl IdNormalizer {
    fn normalize(&mut self, kind: IdKind, original: &str) -> String {
        let key = (kind, original.to_owned());
        if let Some(canonical) = self.canonical.get(&key) {
            return canonical.clone();
        }
        let next = self.next.entry(kind).or_default();
        *next += 1;
        let canonical = format!("{}_{}", kind.label(), next);
        self.canonical.insert(key, canonical.clone());
        canonical
    }
}

fn normalize_records(records: &[RecordedEvent]) -> Vec<Value> {
    let mut ids = IdNormalizer::default();
    records
        .iter()
        .map(|record| {
            let mut value = serde_json::to_value(record).unwrap();
            value["occurred_at_ms"] = json!(0);
            visit_host_ids(&mut value, |kind, id| {
                *id = ids.normalize(kind, id);
            });
            project_approval_actor(&mut value);
            value
        })
        .collect()
}

fn visit_host_ids(value: &mut Value, mut visit: impl FnMut(IdKind, &mut String)) {
    visit_string(value, "/event/run_id", IdKind::Run, &mut visit);
    visit_string(value, "/event/turn_id", IdKind::Turn, &mut visit);
    visit_string(value, "/event/call_id", IdKind::Call, &mut visit);
    visit_string(value, "/event/call/id", IdKind::Call, &mut visit);
    visit_string(value, "/event/result/call_id", IdKind::Call, &mut visit);
    if let Some(Value::Array(artifacts)) = value.pointer_mut("/event/result/artifacts") {
        for artifact in artifacts {
            if let Value::String(id) = artifact {
                visit(IdKind::Artifact, id);
            }
        }
    }
}

fn visit_string(
    value: &mut Value,
    pointer: &str,
    kind: IdKind,
    visit: &mut impl FnMut(IdKind, &mut String),
) {
    if let Some(Value::String(id)) = value.pointer_mut(pointer) {
        visit(kind, id);
    }
}

fn project_approval_actor(value: &mut Value) {
    let approval_event = matches!(
        value.pointer("/event/event").and_then(Value::as_str),
        Some("approval_granted" | "approval_denied")
    );
    if !approval_event {
        return;
    }
    if let Some(Value::String(actor)) = value.pointer_mut("/event/actor_id")
        && matches!(actor.as_str(), "stdin" | "daemon")
    {
        *actor = "human_decision".into();
    }
}

fn mutate_host_ids(records: &mut [RecordedEvent], mut mutate: impl FnMut(IdKind, &mut String)) {
    for record in records {
        let mut value = serde_json::to_value(&*record).unwrap();
        visit_host_ids(&mut value, &mut mutate);
        *record = serde_json::from_value(value).unwrap();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ApprovalSnapshot {
    Granted { actor: String },
    Denied { actor: String, reason: String },
}

fn approval_snapshots(records: &[RecordedEvent]) -> Vec<ApprovalSnapshot> {
    records
        .iter()
        .filter_map(|record| match &record.event {
            HarnessEvent::ApprovalGranted { actor_id, .. } => Some(ApprovalSnapshot::Granted {
                actor: actor_id.to_string(),
            }),
            HarnessEvent::ApprovalDenied {
                actor_id, reason, ..
            } => Some(ApprovalSnapshot::Denied {
                actor: actor_id.to_string(),
                reason: reason.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn assert_approval_transport(
    scenario: Scenario,
    direct: &[RecordedEvent],
    daemon: &[RecordedEvent],
) {
    match scenario.approval() {
        ScenarioApproval::None => {
            assert!(approval_snapshots(direct).is_empty());
            assert!(approval_snapshots(daemon).is_empty());
        }
        ScenarioApproval::Grant => {
            assert_eq!(
                approval_snapshots(direct),
                [ApprovalSnapshot::Granted {
                    actor: "stdin".into()
                }]
            );
            assert_eq!(
                approval_snapshots(daemon),
                [ApprovalSnapshot::Granted {
                    actor: "daemon".into()
                }]
            );
        }
        ScenarioApproval::Deny => {
            assert_eq!(
                approval_snapshots(direct),
                [ApprovalSnapshot::Denied {
                    actor: "stdin".into(),
                    reason: DENIAL_REASON.into()
                }]
            );
            assert_eq!(
                approval_snapshots(daemon),
                [ApprovalSnapshot::Denied {
                    actor: "daemon".into(),
                    reason: DENIAL_REASON.into()
                }]
            );
        }
    }
}

fn assert_policy_and_effect(scenario: Scenario, records: &[RecordedEvent]) {
    let (effect, decision) = records
        .iter()
        .find_map(|record| match &record.event {
            HarnessEvent::ToolCallProposed { call, .. } => Some(call.effect.clone()),
            _ => None,
        })
        .zip(records.iter().find_map(|record| match &record.event {
            HarnessEvent::PolicyEvaluated { decision, .. } => Some(decision),
            _ => None,
        }))
        .unwrap();
    match scenario {
        Scenario::ReadOnly => {
            assert_eq!(effect, EffectClass::ReadOnly);
            assert!(matches!(decision, PolicyDecision::Allow));
        }
        Scenario::ApprovedWrite | Scenario::DeniedWrite => {
            assert_eq!(effect, EffectClass::WorkspaceWrite);
            assert!(matches!(decision, PolicyDecision::RequireApproval { .. }));
        }
    }
}

fn set_approval_actor(records: &mut [RecordedEvent], actor: &str) {
    for record in records {
        match &mut record.event {
            HarnessEvent::ApprovalGranted { actor_id, .. }
            | HarnessEvent::ApprovalDenied { actor_id, .. } => {
                *actor_id = ActorId::new(actor).unwrap();
            }
            _ => {}
        }
    }
}

struct ProofContext {
    _root: tempfile::TempDir,
    workspace: PathBuf,
    config_path: PathBuf,
    fixture_path: PathBuf,
    socket_path: PathBuf,
    ledger_path: PathBuf,
    #[cfg(unix)]
    runtime_root: PathBuf,
    #[cfg(unix)]
    state_root: PathBuf,
    #[cfg(windows)]
    local_app_data: PathBuf,
}

impl ProofContext {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let workspace_id = paths::workspace_id(&workspace).unwrap();

        #[cfg(unix)]
        let (socket_path, ledger_path, runtime_root, state_root) = {
            let runtime_root = root.path().join("runtime");
            let state_root = root.path().join("state");
            (
                runtime_root
                    .join("plato-agent")
                    .join("workspaces")
                    .join(&workspace_id)
                    .join("agent.sock"),
                state_root
                    .join("plato-agent")
                    .join("workspaces")
                    .join(&workspace_id)
                    .join("agent.db"),
                runtime_root,
                state_root,
            )
        };

        #[cfg(windows)]
        let (socket_path, ledger_path, local_app_data) = {
            let local_app_data = root.path().join("local-app-data");
            (
                PathBuf::from(format!(r"\\.\pipe\plato-agent-{workspace_id}")),
                local_app_data
                    .join("plato-agent")
                    .join("workspaces")
                    .join(&workspace_id)
                    .join("agent.db"),
                local_app_data,
            )
        };

        Self {
            config_path: workspace.join("plato.toml"),
            fixture_path: workspace.join("fixture.txt"),
            _root: root,
            workspace,
            socket_path,
            ledger_path,
            #[cfg(unix)]
            runtime_root,
            #[cfg(unix)]
            state_root,
            #[cfg(windows)]
            local_app_data,
        }
    }

    fn restore_fixture(&self) {
        fs::write(&self.fixture_path, FIXTURE_INITIAL).unwrap();
    }

    fn apply_environment(&self, command: &mut Command) {
        #[cfg(unix)]
        command
            .env("XDG_RUNTIME_DIR", &self.runtime_root)
            .env("XDG_STATE_HOME", &self.state_root);
        #[cfg(windows)]
        command.env("LOCALAPPDATA", &self.local_app_data);
        command.env(API_KEY_ENV, "test-key");
    }

    fn plato_command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_plato"));
        command.current_dir(&self.workspace);
        self.apply_environment(&mut command);
        command
    }

    fn daemon_command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_plato-agentd"));
        command
            .arg("--workspace")
            .arg(&self.workspace)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        self.apply_environment(&mut command);
        command
    }

    fn assert_no_serving_daemon(&self, stage: &str) {
        assert!(
            DaemonClient::connect_with_timeout(&self.socket_path, Duration::from_millis(100))
                .is_err(),
            "{stage}: unique workspace unexpectedly had a serving daemon"
        );
        #[cfg(unix)]
        assert!(
            !self.socket_path.exists(),
            "{stage}: daemon socket unexpectedly exists"
        );
    }
}

struct ProofDaemon {
    child: Option<Child>,
    workspace: PathBuf,
    socket_path: PathBuf,
}

impl ProofDaemon {
    fn start(proof: &ProofContext) -> Self {
        let mut child = proof.daemon_command().spawn().unwrap();
        wait_for_daemon(&proof.socket_path, &proof.workspace, &mut child);
        Self {
            child: Some(child),
            workspace: proof.workspace.clone(),
            socket_path: proof.socket_path.clone(),
        }
    }

    fn connect(&self) -> DaemonClient {
        let mut client =
            DaemonClient::connect_with_timeout(&self.socket_path, Duration::from_secs(2)).unwrap();
        client.hello(&self.workspace).unwrap();
        client
    }

    fn stop(mut self, mut client: DaemonClient) {
        assert_eq!(
            client.shutdown_if_idle().unwrap().result,
            ShutdownIfIdleResultName::Shutdown
        );
        drop(client);
        let mut child = self.child.take().unwrap();
        let status = wait_bounded(&mut child, PROOF_TIMEOUT);
        if !status.success() {
            panic!(
                "daemon shutdown failed ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
    }
}

impl Drop for ProofDaemon {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_daemon(socket_path: &Path, workspace: &Path, child: &mut Child) {
    let deadline = Instant::now() + PROOF_TIMEOUT;
    loop {
        if let Ok(mut client) =
            DaemonClient::connect_with_timeout(socket_path, Duration::from_millis(200))
            && client.hello(workspace).is_ok()
        {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon exited before serving ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
        assert!(Instant::now() < deadline, "daemon did not start");
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_bounded(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            panic!(
                "child did not exit before timeout ({status}): {}",
                read_pipe(child.stderr.take())
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn read_pipe(pipe: Option<impl Read>) -> String {
    let mut output = String::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_string(&mut output).unwrap();
    }
    output
}

fn assert_success(label: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{label} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn output_field(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("missing {prefix:?} in output:\n{output}"))
        .to_owned()
}

#[derive(Clone, Copy, Debug)]
enum UsageFixture {
    Known(u32, u32),
    Unknown,
}

impl UsageFixture {
    fn is_known(self) -> bool {
        matches!(self, Self::Known(..))
    }
}

#[derive(Clone)]
struct ProviderReply {
    body: String,
}

impl ProviderReply {
    fn tool_call(name: &str, input: Value, usage: UsageFixture) -> Self {
        let delta = json!({
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "provider_call_1",
                        "function": {
                            "name": name,
                            "arguments": serde_json::to_string(&input).unwrap()
                        }
                    }]
                },
                "finish_reason": null
            }]
        });
        let finish = json!({
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "tool_calls"
            }]
        });
        Self {
            body: event_stream([delta, finish], usage),
        }
    }

    fn answer(answer: &str, usage: UsageFixture) -> Self {
        let delta = json!({
            "choices": [{
                "index": 0,
                "delta": {"content": answer},
                "finish_reason": null
            }]
        });
        let finish = json!({
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }]
        });
        Self {
            body: event_stream([delta, finish], usage),
        }
    }
}

fn event_stream(events: [Value; 2], usage: UsageFixture) -> String {
    let mut body = String::new();
    for mut event in events {
        event["model"] = json!(SERVED_MODEL);
        body.push_str(&format!("data: {event}\n\n"));
    }
    if let UsageFixture::Known(prompt_tokens, completion_tokens) = usage {
        body.push_str(&format!(
            "data: {}\n\n",
            json!({
                "model": SERVED_MODEL,
                "choices": [],
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens
                }
            })
        ));
    }
    body.push_str("data: [DONE]\n\n");
    body
}

struct ScriptedProvider {
    base_url: String,
    handle: Option<thread::JoinHandle<Vec<Value>>>,
}

impl ScriptedProvider {
    fn start(replies: Vec<ProviderReply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            replies
                .into_iter()
                .map(|reply| {
                    let mut stream = accept_before(&listener, Instant::now() + PROOF_TIMEOUT);
                    let request = read_http_request(&mut stream);
                    write_http_response(&mut stream, &reply.body);
                    request
                })
                .collect()
        });
        Self {
            base_url,
            handle: Some(handle),
        }
    }

    fn join(mut self) -> Vec<Value> {
        self.handle.take().unwrap().join().unwrap()
    }
}

impl Drop for ScriptedProvider {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn accept_before(listener: &TcpListener, deadline: Instant) -> TcpStream {
    loop {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "provider was not called");
                thread::sleep(POLL_INTERVAL);
            }
            Err(error) => panic!("provider accept failed: {error}"),
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Value {
    stream.set_read_timeout(Some(PROOF_TIMEOUT)).unwrap();
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
    serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap()
}

fn write_http_response(stream: &mut TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

fn write_provider_config(path: &Path, base_url: &str) {
    fs::write(
        path,
        format!(
            r#"[provider]
kind = "open_ai"
model = "test-model"
api_key_env = "{API_KEY_ENV}"
base_url = "{base_url}"
connect_timeout_ms = 5000
stream_idle_timeout_ms = 5000

[limits]
token_budget = 4000
max_output_tokens = 64
max_turns = 2

[tools]
enabled = ["file.read", "file.write"]
"#
        ),
    )
    .unwrap();
}

#[derive(Clone, Copy)]
enum SampleApproval {
    Granted,
    Denied,
}

fn sample_evidence(approval: SampleApproval) -> RunEvidence {
    let run_id = RunId::new("run_direct").unwrap();
    let first_turn = TurnId::new("turn_1").unwrap();
    let second_turn = TurnId::new("turn_2").unwrap();
    let call_id = ToolCallId::new("call_1").unwrap();
    let tool = ToolName::new("file.write").unwrap();
    let input = json!({"path": "fixture.txt", "content": FIXTURE_WRITTEN});
    let call = ToolCall {
        id: call_id.clone(),
        tool: tool.clone(),
        effect: EffectClass::WorkspaceWrite,
        input: input.clone(),
    };
    let mut events = vec![
        HarnessEvent::RunStarted {
            run_id: run_id.clone(),
            agent_id: AgentId::new("plato").unwrap(),
        },
        HarnessEvent::ContextBuilt {
            run_id: run_id.clone(),
            turn_id: first_turn.clone(),
            context: ContextPack {
                token_budget: 100,
                fragments: vec![],
            },
        },
        HarnessEvent::ModelRequested {
            run_id: run_id.clone(),
            turn_id: first_turn.clone(),
            step: 0,
            model: ModelName::new("test-model").unwrap(),
        },
        HarnessEvent::ModelResponded {
            run_id: run_id.clone(),
            turn_id: first_turn.clone(),
            step: 0,
            output: Message {
                role: MessageRole::Assistant,
                content: String::new(),
            },
            proposed_calls: vec![ToolProposal {
                tool: tool.clone(),
                input: input.clone(),
            }],
            served_model: Some(ModelName::new(SERVED_MODEL).unwrap()),
            usage: Some(ModelUsage {
                input_tokens: 10,
                output_tokens: 2,
            }),
        },
        HarnessEvent::ToolCallProposed {
            run_id: run_id.clone(),
            turn_id: first_turn,
            call,
        },
        HarnessEvent::PolicyEvaluated {
            run_id: run_id.clone(),
            call_id: call_id.clone(),
            decision: PolicyDecision::RequireApproval {
                reason: "mutable or networked tool call requires explicit policy allowance".into(),
            },
        },
    ];
    match approval {
        SampleApproval::Granted => {
            events.extend([
                HarnessEvent::ApprovalGranted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    actor_id: ActorId::new("stdin").unwrap(),
                },
                HarnessEvent::ToolStarted {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                },
                HarnessEvent::ToolFinished {
                    run_id: run_id.clone(),
                    result: ToolResult {
                        call_id,
                        summary: "wrote 16 bytes to fixture.txt".into(),
                        data: json!({"path": "fixture.txt", "bytes": 16}),
                        artifacts: vec![],
                        visibility: ResultVisibility::Both,
                    },
                },
            ]);
        }
        SampleApproval::Denied => {
            events.push(HarnessEvent::ApprovalDenied {
                run_id: run_id.clone(),
                call_id,
                actor_id: ActorId::new("stdin").unwrap(),
                reason: DENIAL_REASON.into(),
            });
        }
    }
    events.extend([
        HarnessEvent::ContextBuilt {
            run_id: run_id.clone(),
            turn_id: second_turn.clone(),
            context: ContextPack {
                token_budget: 100,
                fragments: vec![],
            },
        },
        HarnessEvent::ModelRequested {
            run_id: run_id.clone(),
            turn_id: second_turn.clone(),
            step: 1,
            model: ModelName::new("test-model").unwrap(),
        },
        HarnessEvent::ModelResponded {
            run_id: run_id.clone(),
            turn_id: second_turn,
            step: 1,
            output: Message {
                role: MessageRole::Assistant,
                content: "done".into(),
            },
            proposed_calls: vec![],
            served_model: Some(ModelName::new(SERVED_MODEL).unwrap()),
            usage: Some(ModelUsage {
                input_tokens: 20,
                output_tokens: 4,
            }),
        },
        HarnessEvent::RunFinished { run_id },
    ]);
    let records = events
        .into_iter()
        .enumerate()
        .map(|(seq, event)| RecordedEvent {
            seq: seq as u64,
            occurred_at_ms: 1_000 + seq as u64,
            event,
        })
        .collect::<Vec<_>>();
    RunEvidence::from_leg(records, "done".into(), b"fixture changed\n".to_vec())
        .with_provider_requests(vec![json!({"model": "test-model"})])
}
