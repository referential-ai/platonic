mod artifacts;
mod contract;
mod prompts;

#[cfg(test)]
mod tests;

use self::{
    artifacts::{
        CANDIDATE_FILE, IssuePrepRun, PREPARE_FILES, REFINE_FILES, REVIEW_FILES, StageFiles, sha256,
    },
    contract::{render_candidate, validate_prepared, validate_refined, validate_review},
    prompts::{SYSTEM_PROMPT, issue_source_json, prepare_prompt, refine_prompt, review_prompt},
};
use crate::{
    AppError, AppResult,
    config::{Config, ProviderConfig, ProviderKind},
    model::{ModelMessage, ModelRequest, ModelResponse, ModelStop},
    provider::openai_compat::{OpenAiCompatibleClient, TokenLimitField},
};
use platonic_core::ModelUsage;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::PathBuf;

const PIPELINE_VERSION: u32 = 1;
const MAX_INPUT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct IssuePrepOptions {
    pub workspace_root: PathBuf,
    pub config_path: Option<PathBuf>,
    pub run_dir: PathBuf,
    pub input: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IssuePrepOutcome {
    Candidate { markdown: String },
    Blocked { stage: String, reasons: Vec<String> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PipelineConfig {
    provider_kind: String,
    provider_base_url: String,
    model: String,
    max_output_tokens: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunManifest {
    pipeline_version: u32,
    provider_kind: String,
    provider_base_url: String,
    model: String,
    max_output_tokens: u32,
    system_prompt: String,
    input_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StageResult {
    stop: ModelStop,
    text: String,
    tool_call_count: usize,
    usage: ModelUsage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidationRecord {
    stage: String,
    validation_kind: ValidationKind,
    status: ValidationStatus,
    errors: Vec<String>,
    prompt_sha256: String,
    result_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ValidationStatus {
    Passed,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ValidationKind {
    Structural,
    ModelReview,
}

enum StageOutcome<T> {
    Passed(T),
    Blocked(Vec<String>),
}

pub fn run_issue_prep(options: IssuePrepOptions) -> AppResult<IssuePrepOutcome> {
    let config = Config::load(&options.workspace_root, options.config_path.as_deref())?;
    let pipeline_config = PipelineConfig::from_config(&config);
    let provider = config.provider;
    let mut client = None;

    run_issue_prep_with(options, pipeline_config, |request| {
        if client.is_none() {
            client = Some(client_from_provider(&provider)?);
        }
        client
            .as_ref()
            .expect("client was initialized")
            .send(request)
    })
}

fn run_issue_prep_with(
    options: IssuePrepOptions,
    config: PipelineConfig,
    mut send: impl FnMut(&ModelRequest) -> AppResult<ModelResponse>,
) -> AppResult<IssuePrepOutcome> {
    validate_input(&options.input)?;
    let manifest = RunManifest::new(&config, options.input.as_bytes());
    let run = IssuePrepRun::start(&options.run_dir, &manifest, &options.input)?;
    let source = issue_source_json(&options.input)?;

    let prepared = match run_stage(
        &run,
        &manifest,
        PREPARE_FILES,
        ValidationKind::Structural,
        prepare_prompt(&source),
        validate_prepared,
        &mut send,
    )? {
        StageOutcome::Passed(value) => value,
        StageOutcome::Blocked(reasons) => {
            return Ok(IssuePrepOutcome::Blocked {
                stage: PREPARE_FILES.name.into(),
                reasons,
            });
        }
    };

    let refined = match run_stage(
        &run,
        &manifest,
        REFINE_FILES,
        ValidationKind::Structural,
        refine_prompt(&source, &prepared)?,
        validate_refined,
        &mut send,
    )? {
        StageOutcome::Passed(value) => value,
        StageOutcome::Blocked(reasons) => {
            return Ok(IssuePrepOutcome::Blocked {
                stage: REFINE_FILES.name.into(),
                reasons,
            });
        }
    };

    match run_stage(
        &run,
        &manifest,
        REVIEW_FILES,
        ValidationKind::ModelReview,
        review_prompt(&source, &refined)?,
        validate_review,
        &mut send,
    )? {
        StageOutcome::Passed(_) => {}
        StageOutcome::Blocked(reasons) => {
            return Ok(IssuePrepOutcome::Blocked {
                stage: REVIEW_FILES.name.into(),
                reasons,
            });
        }
    }

    let candidate = render_candidate(&refined);
    run.write_artifact(CANDIDATE_FILE, candidate.as_bytes())?;
    Ok(IssuePrepOutcome::Candidate {
        markdown: run.read_text(CANDIDATE_FILE)?,
    })
}

fn run_stage<T>(
    run: &IssuePrepRun,
    manifest: &RunManifest,
    files: StageFiles,
    validation_kind: ValidationKind,
    prompt: String,
    validate: fn(&T) -> Vec<String>,
    send: &mut impl FnMut(&ModelRequest) -> AppResult<ModelResponse>,
) -> AppResult<StageOutcome<T>>
where
    T: DeserializeOwned,
{
    run.write_artifact(files.prompt, prompt.as_bytes())?;
    let prompt = run.read_text(files.prompt)?;
    let response = send(&ModelRequest {
        model: manifest.model.clone(),
        system: manifest.system_prompt.clone(),
        max_output_tokens: manifest.max_output_tokens,
        reasoning_effort: None,
        messages: vec![ModelMessage::user_text(&prompt)],
        tools: vec![],
    })?;
    run.write_json(
        files.result,
        &StageResult {
            text: response.text(),
            tool_call_count: response.tool_uses().len(),
            stop: response.stop,
            usage: response.usage,
        },
    )?;

    let result: StageResult = run.read_json(files.result)?;
    let mut errors = result_contract_errors(&result);
    let parsed = if errors.is_empty() {
        match serde_json::from_str::<T>(&result.text) {
            Ok(value) => Some(value),
            Err(error) => {
                errors.push(format!("result is not valid stage JSON: {error}"));
                None
            }
        }
    } else {
        None
    };
    if let Some(value) = &parsed {
        errors.extend(validate(value));
    }

    let validation = ValidationRecord {
        stage: files.name.into(),
        validation_kind,
        status: if errors.is_empty() {
            ValidationStatus::Passed
        } else {
            ValidationStatus::Blocked
        },
        errors: errors.clone(),
        prompt_sha256: sha256(&run.read(files.prompt)?),
        result_sha256: sha256(&run.read(files.result)?),
    };
    run.write_json(files.validation, &validation)?;
    let recorded: ValidationRecord = run.read_json(files.validation)?;
    if recorded != validation {
        return Err(AppError::IssuePrepArtifactConflict(
            run.path(files.validation),
        ));
    }

    if errors.is_empty() {
        Ok(StageOutcome::Passed(
            parsed.expect("successful validation has a parsed result"),
        ))
    } else {
        Ok(StageOutcome::Blocked(errors))
    }
}

fn result_contract_errors(result: &StageResult) -> Vec<String> {
    let mut errors = Vec::new();
    if result.stop != ModelStop::EndTurn {
        errors.push(format!(
            "provider stop must be end_turn, got {:?}",
            result.stop
        ));
    }
    if result.tool_call_count != 0 {
        errors.push("stage response must not contain tool calls".into());
    }
    if result.text.trim().is_empty() {
        errors.push("stage response must not be empty".into());
    }
    errors
}

fn validate_input(input: &str) -> AppResult<()> {
    if input.trim().is_empty() {
        return Err(AppError::Config(
            "issue-prep input must not be empty".into(),
        ));
    }
    if input.len() > MAX_INPUT_BYTES {
        return Err(AppError::Config(format!(
            "issue-prep input exceeds {MAX_INPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

impl PipelineConfig {
    fn from_config(config: &Config) -> Self {
        Self {
            provider_kind: provider_kind_name(&config.provider.kind).into(),
            provider_base_url: config.provider.base_url.clone(),
            model: config.provider.model.clone(),
            max_output_tokens: config.limits.max_output_tokens,
        }
    }
}

impl RunManifest {
    fn new(config: &PipelineConfig, input: &[u8]) -> Self {
        Self {
            pipeline_version: PIPELINE_VERSION,
            provider_kind: config.provider_kind.clone(),
            provider_base_url: config.provider_base_url.clone(),
            model: config.model.clone(),
            max_output_tokens: config.max_output_tokens,
            system_prompt: SYSTEM_PROMPT.into(),
            input_sha256: sha256(input),
        }
    }
}

fn client_from_provider(config: &ProviderConfig) -> AppResult<OpenAiCompatibleClient> {
    OpenAiCompatibleClient::from_config(
        &config.api_key_env,
        config.base_url.clone(),
        config.connect_timeout_ms,
        config.stream_idle_timeout_ms,
        config.http_referer.clone(),
        config.app_title.clone(),
        match config.kind {
            ProviderKind::OpenAi => TokenLimitField::MaxCompletionTokens,
            ProviderKind::OpenRouter => TokenLimitField::MaxTokens,
        },
    )
}

fn provider_kind_name(kind: &ProviderKind) -> &'static str {
    match kind {
        ProviderKind::OpenAi => "open_ai",
        ProviderKind::OpenRouter => "open_router",
    }
}
