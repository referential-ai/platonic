use plato_agent::{
    AppError, ReasoningEffort as RootReasoningEffort, RunOverrides as RootRunOverrides,
    daemon::protocol::*,
    model::{ReasoningEffort as ModelReasoningEffort, RunOverrides as ModelRunOverrides},
};

#[test]
fn historical_protocol_and_model_paths_share_the_extracted_types() {
    let effort: ModelReasoningEffort = RootReasoningEffort::High;
    let protocol_effort: ReasoningEffort = effort;
    assert_eq!(protocol_effort, ReasoningEffort::High);

    let overrides: ModelRunOverrides = RootRunOverrides {
        model: Some("openai/gpt-5".into()),
        reasoning_effort: Some(effort),
    };
    let params = RunStartParams {
        question: "hello".into(),
        config_path: None,
        overrides,
        wait: Some(false),
    };
    assert_eq!(
        params.overrides.reasoning_effort,
        Some(ReasoningEffort::High)
    );

    let error = AppError::DaemonResponse(ProtocolError {
        code: ERROR_RUN_FAILED.into(),
        message: "synthetic failure".into(),
    });
    assert_eq!(
        error.to_string(),
        "daemon protocol error run_failed: synthetic failure"
    );
}
