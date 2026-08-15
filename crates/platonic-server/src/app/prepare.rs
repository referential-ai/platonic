use super::{
    RunSession, new_run_id,
    session::{
        SessionHydration, hydrated_messages, load_platonic_memory, provider_system_context,
        provider_system_context_with_interruption, provider_system_context_with_profile,
        provider_system_context_with_profile_and_interruption,
    },
    types::{RunLedger, RunOptions},
};
use crate::{
    AppError, AppResult,
    config::{ComputerConfig, Config, LimitsConfig, ProviderConfig, ProviderKind, ToolsConfig},
    ledger::{EventRecorder, SqliteLedger, run_jsonl_path},
    model::{ModelBlock, ModelMessage, RunOverrides},
    paths::DefaultSqlitePath,
    provider::openai_compat::{OpenAiCompatibleClient, TokenLimitField},
    server_store::ProfileRevisionRecord,
    tool_catalog::{ToolSpec, tool_specs},
};
use platonic_core::{AgentId, ProfileId, RunId, RunIdentity};
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
    pub(super) identity: RunIdentity,
    pub(super) run_id: RunId,
    pub(super) session_hydration: Option<SessionHydration>,
    pub(super) messages: Vec<ModelMessage>,
    pub(super) platonic_memory: Option<String>,
    pub(super) profile_context: Option<PreparedProfileContext>,
    pub(super) system_context: String,
    pub(super) first_system_context: String,
}

pub(super) const PROFILE_CONTEXT_TOKEN_BUDGET: u32 = 8_192;
pub(crate) const SPAWN_EDGE_CONTEXT_TOKEN_BUDGET: u32 = 4_096;
const PROFILE_CONTEXT_CONTENT_TOKEN_BUDGET: u32 = PROFILE_CONTEXT_TOKEN_BUDGET - 2;
const PROFILE_CONTEXT_TRUNCATION_MARKER: &str =
    "\n\n[profile context truncated to the 8192-token lane]";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedProfileContext {
    pub(super) profile_id: ProfileId,
    pub(super) revision: u64,
    pub(super) content_hash: String,
    pub(super) content: String,
    pub(super) truncated: bool,
}

impl PreparedProfileContext {
    pub(super) fn source(&self) -> String {
        format!(
            "profile:{}@{}#sha256:{}",
            self.profile_id, self.revision, self.content_hash
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ApprovalMode, RunLedger,
        model::{ModelRequest, RunOverrides},
        server_store::ProfileRevisionContent,
        tool_catalog::THREAD_SPAWN,
    };
    use platonic_core::{
        ContextLane, HarnessEvent, RecordedEvent, RunReadback, RunStartedEvent, TurnId,
    };

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
        let identity = RunIdentity::LegacyAgent {
            agent_id: agent_id.clone(),
        };
        let resolved_toolset = vec!["file.read".into(), THREAD_SPAWN.into()];

        let (prepared, _) = prepare_run_for_thread(
            &options,
            Some(identity.clone()),
            Some(&resolved_toolset),
            None,
        )
        .unwrap();

        assert_eq!(prepared.identity, identity);
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

    #[test]
    fn profile_context_records_exact_verified_revision_hash_and_replays_embedded_content() {
        let profile_id = ProfileId::new("profile-context-test").unwrap();
        let content = ProfileRevisionContent {
            instructions_markdown: "Follow the profile instruction.".into(),
            memory_markdown: "Remember the durable fact.".into(),
            skill_refs: vec!["skill:review@sha256:abc".into()],
        };
        let revision = ProfileRevisionRecord {
            profile_id: profile_id.clone(),
            revision: 7,
            parent_revision: Some(6),
            actor: "operator".into(),
            created_at_ms: 70,
            content_hash: content.content_hash().unwrap(),
            content,
        };
        let identity = RunIdentity::Profile {
            profile_id: profile_id.clone(),
            profile_revision: 7,
        };
        let selected = prepare_profile_context(Some(&identity), Some(&revision))
            .unwrap()
            .unwrap();
        assert_eq!(
            selected.source(),
            format!("profile:{profile_id}@7#sha256:{}", revision.content_hash)
        );

        let request = ModelRequest {
            model: crate::config::Config::default().provider.model,
            system: provider_system_context_with_profile(Some(&selected.content), None),
            max_output_tokens: 1,
            reasoning_effort: None,
            messages: vec![],
            tools: vec![],
        };
        let context = super::super::context::context_pack_with_profile_and_interruption(
            &request,
            u32::MAX,
            Some(&selected),
            None,
            None,
        )
        .unwrap();
        let run_id = RunId::new("run-profile-context").unwrap();
        let records = [
            RecordedEvent {
                seq: 0,
                occurred_at_ms: 1,
                event: HarnessEvent::RunStarted(RunStartedEvent {
                    run_id: run_id.clone(),
                    identity,
                }),
            },
            RecordedEvent {
                seq: 1,
                occurred_at_ms: 2,
                event: HarnessEvent::ContextBuilt {
                    run_id,
                    turn_id: TurnId::new("turn-profile-context").unwrap(),
                    context,
                },
            },
        ];
        let replay = RunReadback::from_events(&records).unwrap();
        let profile_fragment = replay
            .entries
            .iter()
            .find_map(|entry| match entry {
                platonic_core::ReadbackEntry::ContextFragment { fragment, .. }
                    if fragment.lane == ContextLane::RetrievedContext
                        && fragment.source.starts_with("profile:") =>
                {
                    Some(fragment)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(profile_fragment.source, selected.source());
        assert_eq!(profile_fragment.content, selected.content);

        let mut tampered = revision;
        tampered.content.memory_markdown.push_str(" changed");
        assert!(
            prepare_profile_context(
                Some(&RunIdentity::Profile {
                    profile_id,
                    profile_revision: 7,
                }),
                Some(&tampered),
            )
            .unwrap_err()
            .to_string()
            .contains("content hash mismatch")
        );
    }

    #[test]
    fn profile_context_truncation_is_deterministic_and_inside_its_lane() {
        let profile_id = ProfileId::new("profile-context-bound").unwrap();
        let content = ProfileRevisionContent {
            instructions_markdown: "instruction ".repeat(10_000),
            memory_markdown: "memory ".repeat(16_000),
            skill_refs: vec![],
        };
        content.validate().unwrap();
        let revision = ProfileRevisionRecord {
            profile_id: profile_id.clone(),
            revision: 1,
            parent_revision: None,
            actor: "operator".into(),
            created_at_ms: 1,
            content_hash: content.content_hash().unwrap(),
            content,
        };
        let identity = RunIdentity::Profile {
            profile_id,
            profile_revision: 1,
        };
        let first = prepare_profile_context(Some(&identity), Some(&revision))
            .unwrap()
            .unwrap();
        let second = prepare_profile_context(Some(&identity), Some(&revision))
            .unwrap()
            .unwrap();
        assert_eq!(first, second);
        assert!(first.truncated);
        assert!(first.content.ends_with(PROFILE_CONTEXT_TRUNCATION_MARKER));
        assert!(
            super::super::context::estimate_tokens(&first.content)
                <= PROFILE_CONTEXT_CONTENT_TOKEN_BUDGET
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

    pub(crate) fn has_logical_read_tool(&self) -> bool {
        self.config
            .tools
            .enabled
            .iter()
            .any(|tool| crate::tool_catalog::is_logical_read_tool(tool))
    }

    pub(crate) fn add_spawn_edge_context(&mut self, content: String) -> AppResult<()> {
        let before = super::context::estimate_tokens(&serde_json::to_string(&self.messages)?);
        let message = ModelMessage::assistant_blocks(vec![ModelBlock::Text { text: content }]);
        let insert_at = self.messages.len().saturating_sub(1);
        self.messages.insert(insert_at, message);
        let after = super::context::estimate_tokens(&serde_json::to_string(&self.messages)?);
        let added = after.saturating_sub(before);
        if added > SPAWN_EDGE_CONTEXT_TOKEN_BUDGET {
            return Err(AppError::Config(format!(
                "spawn-edge context uses {added} estimated tokens, maximum is {SPAWN_EDGE_CONTEXT_TOKEN_BUDGET}"
            )));
        }
        self.config.limits.token_budget = self
            .config
            .limits
            .token_budget
            .checked_add(added)
            .ok_or_else(|| {
                AppError::Config("spawn-edge context token budget overflowed u32".into())
            })?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn messages(&self) -> &[ModelMessage] {
        &self.messages
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
    prepare_run_for_thread(options, None, None, None)
}

pub(crate) fn prepare_run_for_thread(
    options: &RunOptions,
    identity: Option<RunIdentity>,
    toolset: Option<&[String]>,
    profile_revision: Option<&ProfileRevisionRecord>,
) -> AppResult<(PreparedRun, EventRecorder)> {
    if options.question.trim().is_empty() {
        return Err(AppError::EmptyQuestion);
    }

    let profile_context = prepare_profile_context(identity.as_ref(), profile_revision)?;
    let platonic_memory = load_platonic_memory(&options.workspace_root)?;
    let base_system_context = provider_system_context(platonic_memory.as_deref());
    let base_first_system_context = provider_system_context_with_interruption(
        platonic_memory.as_deref(),
        options.voice_interruption_context.as_deref(),
    );
    let system_context = provider_system_context_with_profile(
        profile_context
            .as_ref()
            .map(|context| context.content.as_str()),
        platonic_memory.as_deref(),
    );
    let first_system_context = provider_system_context_with_profile_and_interruption(
        profile_context
            .as_ref()
            .map(|context| context.content.as_str()),
        platonic_memory.as_deref(),
        options.voice_interruption_context.as_deref(),
    );
    let mut config = Config::load(&options.workspace_root, options.config_path.as_deref())?;
    let profile_tokens = super::context::estimate_tokens(&system_context)
        .saturating_sub(super::context::estimate_tokens(&base_system_context))
        .max(
            super::context::estimate_tokens(&first_system_context)
                .saturating_sub(super::context::estimate_tokens(&base_first_system_context)),
        );
    if profile_tokens > PROFILE_CONTEXT_TOKEN_BUDGET {
        return Err(AppError::Config(format!(
            "profile context uses {profile_tokens} estimated tokens, maximum is {PROFILE_CONTEXT_TOKEN_BUDGET}"
        )));
    }
    config.limits.token_budget = config
        .limits
        .token_budget
        .checked_add(profile_tokens)
        .ok_or_else(|| AppError::Config("profile context token budget overflowed u32".into()))?;
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
            identity: identity.unwrap_or(RunIdentity::LegacyAgent {
                agent_id: AgentId::new("plato")?,
            }),
            run_id,
            session_hydration,
            messages,
            platonic_memory,
            profile_context,
            system_context,
            first_system_context,
        },
        recorder,
    ))
}

fn prepare_profile_context(
    identity: Option<&RunIdentity>,
    revision: Option<&ProfileRevisionRecord>,
) -> AppResult<Option<PreparedProfileContext>> {
    match (identity, revision) {
        (
            Some(RunIdentity::Profile {
                profile_id,
                profile_revision,
            }),
            Some(revision),
        ) if profile_id == &revision.profile_id && profile_revision == &revision.revision => {}
        (Some(RunIdentity::Profile { .. }), Some(_)) => {
            return Err(AppError::Config(
                "selected profile revision does not match run identity".into(),
            ));
        }
        (Some(RunIdentity::Profile { .. }), None) => {
            return Err(AppError::Config(
                "profile run is missing its selected content revision".into(),
            ));
        }
        (Some(RunIdentity::LegacyAgent { .. }) | None, Some(_)) => {
            return Err(AppError::Config(
                "legacy run cannot select profile context".into(),
            ));
        }
        (Some(RunIdentity::LegacyAgent { .. }) | None, None) => return Ok(None),
    }
    let revision = revision.expect("matched profile revision above");
    revision
        .content
        .validate()
        .map_err(|error| AppError::Config(error.to_string()))?;
    let verified_hash = revision
        .content
        .content_hash()
        .map_err(|error| AppError::Config(error.to_string()))?;
    if revision.content_hash != verified_hash {
        return Err(AppError::Config(
            "selected profile revision content hash mismatch".into(),
        ));
    }
    let skill_refs = serde_json::to_string(&revision.content.skill_refs)?;
    let rendered = format!(
        "Profile instructions:\n{}\n\nProfile memory:\n{}\n\nProfile skill references (read-only context; no tool authority):\n{}",
        revision.content.instructions_markdown, revision.content.memory_markdown, skill_refs
    );
    let (content, truncated) = truncate_profile_context(&rendered);
    Ok(Some(PreparedProfileContext {
        profile_id: revision.profile_id.clone(),
        revision: revision.revision,
        content_hash: revision.content_hash.clone(),
        content,
        truncated,
    }))
}

fn truncate_profile_context(content: &str) -> (String, bool) {
    if super::context::estimate_tokens(content) <= PROFILE_CONTEXT_CONTENT_TOKEN_BUDGET {
        return (content.into(), false);
    }
    let marker_chars = PROFILE_CONTEXT_TRUNCATION_MARKER.chars().count();
    let max_chars = usize::try_from(PROFILE_CONTEXT_CONTENT_TOKEN_BUDGET)
        .unwrap_or(usize::MAX)
        .saturating_mul(4)
        .saturating_sub(1);
    let retained = max_chars.saturating_sub(marker_chars);
    let mut truncated = content.chars().take(retained).collect::<String>();
    truncated.push_str(PROFILE_CONTEXT_TRUNCATION_MARKER);
    debug_assert!(
        super::context::estimate_tokens(&truncated) <= PROFILE_CONTEXT_CONTENT_TOKEN_BUDGET
    );
    (truncated, true)
}
pub(super) fn token_limit_field(kind: &ProviderKind) -> TokenLimitField {
    match kind {
        ProviderKind::OpenAi => TokenLimitField::MaxCompletionTokens,
        ProviderKind::OpenRouter => TokenLimitField::MaxTokens,
    }
}
