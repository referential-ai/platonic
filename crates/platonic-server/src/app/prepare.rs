use super::{
    RunSession, new_run_id,
    session::{
        SessionHydration, hydrated_messages, load_platonic_memory, provider_system_context,
        provider_system_context_with_interruption,
    },
    types::{RunLedger, RunOptions},
};
use crate::{
    AppError, AppResult,
    config::{ComputerConfig, Config, LimitsConfig, ProviderConfig, ProviderKind, ToolsConfig},
    ledger::{EventRecorder, SqliteLedger, run_jsonl_path},
    model::{ModelMessage, RunOverrides},
    paths::DefaultSqlitePath,
    provider::openai_compat::{OpenAiCompatibleClient, TokenLimitField},
    tool_catalog::{ToolSpec, tool_specs},
};
use platonic_core::{AgentId, RunId};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedRun {
    pub(super) question: String,
    pub(super) overrides: RunOverrides,
    pub(super) workspace_root: PathBuf,
    pub(super) voice_interruption_context: Option<String>,
    pub(super) config: RunConfigSnapshot,
    pub(super) agent_id: AgentId,
    pub(super) run_id: RunId,
    pub(super) session_hydration: Option<SessionHydration>,
    pub(super) messages: Vec<ModelMessage>,
    pub(super) platonic_memory: Option<String>,
    pub(super) system_context: String,
    pub(super) first_system_context: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApprovalMode, RunLedger, model::RunOverrides, tool_catalog::THREAD_SPAWN};

    #[test]
    fn thread_preparation_projects_immutable_agent_and_toolset() {
        let workspace = tempfile::tempdir().unwrap();
        let config_path = workspace.path().join("authorized.toml");
        fs::write(
            &config_path,
            r#"[provider]
api_key_env = "PATH"

[tools]
enabled = ["file.read", "file.write"]
"#,
        )
        .unwrap();
        let options = RunOptions {
            question: "coordinate one worker".into(),
            config_path: Some(config_path),
            overrides: RunOverrides::default(),
            ledger: RunLedger::Jsonl(workspace.path().join("ledger.jsonl")),
            workspace_root: workspace.path().to_path_buf(),
            approval_mode: ApprovalMode::Deny { actor: "test" },
            run_id: Some(RunId::new("run_thread_projection").unwrap()),
            session: None,
            event_sender: None,
            stream_to_stderr: false,
            cancel: None,
            voice_interruption_context: None,
        };
        let agent_id = AgentId::new("coordinator").unwrap();
        let resolved_toolset = vec!["file.read".into(), THREAD_SPAWN.into()];

        let (prepared, _) =
            prepare_run_for_thread(&options, Some(agent_id.clone()), Some(&resolved_toolset))
                .unwrap();

        assert_eq!(prepared.agent_id, agent_id);
        assert!(prepared.has_tool(THREAD_SPAWN));
        assert_eq!(prepared.config.tools.enabled, resolved_toolset);
        assert_eq!(
            tool_specs(&prepared.config.tools.enabled)
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            ["file_read", "thread_spawn"]
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunConfigSnapshot {
    pub(super) provider: ProviderConfig,
    pub(super) limits: LimitsConfig,
    pub(super) tools: ToolsConfig,
    pub(super) computer: ComputerConfig,
}

impl PreparedRun {
    pub(crate) fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub(crate) fn provider_api_key_env(&self) -> &str {
        &self.config.provider.api_key_env
    }

    pub(crate) fn has_tool(&self, name: &str) -> bool {
        self.config.tools.enabled.iter().any(|tool| tool == name)
    }
}

pub(super) fn begin_session_recorder(
    mut ledger: SqliteLedger,
    session: &RunSession,
    run_id: &RunId,
    question: &str,
    config: &Config,
    tools: &[ToolSpec],
    system_context: &str,
) -> AppResult<(EventRecorder, SessionHydration)> {
    let turns = ledger.begin_session_run(
        session.session_id(),
        run_id,
        question,
        session.create_session(),
    )?;
    let hydration = hydrated_messages(&turns, question, config, tools, system_context)?;
    Ok((
        EventRecorder::from_session_sqlite(ledger, run_id),
        hydration,
    ))
}

fn begin_default_jsonl_session_recorder(
    location: &DefaultSqlitePath,
    session: &RunSession,
    run_id: &RunId,
    question: &str,
    config: &Config,
    tools: &[ToolSpec],
    system_context: &str,
) -> AppResult<(EventRecorder, SessionHydration)> {
    let mut ledger = SqliteLedger::open_or_create_default(location)?;
    let log_path = run_jsonl_path(location.as_path(), run_id.as_str())?;
    let recorder = EventRecorder::create_default_jsonl(location, run_id)?;
    let turns = match ledger.begin_session_run(
        session.session_id(),
        run_id,
        question,
        session.create_session(),
    ) {
        Ok(turns) => turns,
        Err(error) => {
            drop(recorder);
            if let Err(cleanup) = fs::remove_file(&log_path) {
                return Err(AppError::Config(format!(
                    "{error}; failed to remove uncommitted run log {}: {cleanup}",
                    log_path.display()
                )));
            }
            return Err(error);
        }
    };
    let recorder = recorder.with_session_jsonl_creation(ledger, run_id, session.create_session());
    match hydrated_messages(&turns, question, config, tools, system_context) {
        Ok(hydration) => Ok((recorder, hydration)),
        Err(error) => match recorder.discard_empty_session_admission() {
            Ok(()) => Err(error),
            Err(cleanup) => Err(AppError::Config(format!(
                "{error}; failed to discard uncommitted run admission: {cleanup}"
            ))),
        },
    }
}
pub(crate) fn prepare_run(options: &RunOptions) -> AppResult<(PreparedRun, EventRecorder)> {
    prepare_run_for_thread(options, None, None)
}

pub(crate) fn prepare_run_for_thread(
    options: &RunOptions,
    agent_id: Option<AgentId>,
    toolset: Option<&[String]>,
) -> AppResult<(PreparedRun, EventRecorder)> {
    if options.question.trim().is_empty() {
        return Err(AppError::EmptyQuestion);
    }

    let platonic_memory = load_platonic_memory(&options.workspace_root)?;
    let system_context = provider_system_context(platonic_memory.as_deref());
    let first_system_context = provider_system_context_with_interruption(
        platonic_memory.as_deref(),
        options.voice_interruption_context.as_deref(),
    );
    let mut config = Config::load(&options.workspace_root, options.config_path.as_deref())?;
    if let Some(model) = &options.overrides.model {
        config.provider.model = model.clone();
    }
    if let Some(toolset) = toolset {
        config.tools.enabled = toolset.to_vec();
    }
    let run_id = match options.run_id.clone() {
        Some(run_id) => run_id,
        None => new_run_id()?,
    };
    let _provider_preflight = OpenAiCompatibleClient::from_config(
        &config.provider.api_key_env,
        config.provider.base_url.clone(),
        config.provider.connect_timeout_ms,
        config.provider.stream_idle_timeout_ms,
        config.provider.http_referer.clone(),
        config.provider.app_title.clone(),
        token_limit_field(&config.provider.kind),
    )?;
    let tools = tool_specs(&config.tools.enabled);
    let (recorder, mut session_hydration) = match (&options.ledger, &options.session) {
        (RunLedger::Sqlite(path), Some(session)) => {
            let (recorder, hydration) = begin_session_recorder(
                SqliteLedger::open_or_create(path)?,
                session,
                &run_id,
                &options.question,
                &config,
                &tools,
                &first_system_context,
            )?;
            (recorder, Some(hydration))
        }
        (RunLedger::DefaultSqlite(path), Some(session)) => {
            let (recorder, hydration) = begin_default_jsonl_session_recorder(
                path,
                session,
                &run_id,
                &options.question,
                &config,
                &tools,
                &first_system_context,
            )?;
            (recorder, Some(hydration))
        }
        (RunLedger::Jsonl(_), Some(_)) => {
            return Err(AppError::Config("sessions require a SQLite ledger".into()));
        }
        (RunLedger::Jsonl(path), None) => (EventRecorder::create_jsonl(path)?, None),
        (RunLedger::Sqlite(path), None) => (EventRecorder::create_sqlite(path, &run_id)?, None),
        (RunLedger::DefaultSqlite(path), None) => {
            (EventRecorder::create_default_jsonl(path, &run_id)?, None)
        }
    };
    let messages = session_hydration
        .as_mut()
        .map(|hydration| std::mem::take(&mut hydration.retained_messages))
        .unwrap_or_else(|| vec![ModelMessage::user_text(options.question.clone())]);
    Ok((
        PreparedRun {
            question: options.question.clone(),
            overrides: options.overrides.clone(),
            workspace_root: options.workspace_root.clone(),
            voice_interruption_context: options.voice_interruption_context.clone(),
            config: RunConfigSnapshot {
                provider: config.provider,
                limits: config.limits,
                tools: config.tools,
                computer: config.computer,
            },
            agent_id: agent_id.unwrap_or(AgentId::new("plato")?),
            run_id,
            session_hydration,
            messages,
            platonic_memory,
            system_context,
            first_system_context,
        },
        recorder,
    ))
}
pub(super) fn token_limit_field(kind: &ProviderKind) -> TokenLimitField {
    match kind {
        ProviderKind::OpenAi => TokenLimitField::MaxCompletionTokens,
        ProviderKind::OpenRouter => TokenLimitField::MaxTokens,
    }
}
