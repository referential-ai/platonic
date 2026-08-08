use plato_agent::{
    AppError, ReasoningEffort as ClientReasoningEffort, RunOverrides as ClientRunOverrides,
};
use platonic_protocol::{
    ERROR_RUN_FAILED, ProtocolError, ReasoningEffort, RunOverrides, RunStartParams,
};

#[test]
fn historical_protocol_and_model_paths_share_the_extracted_types() {
    let effort: ReasoningEffort = ClientReasoningEffort::High;
    assert_eq!(effort, ReasoningEffort::High);

    let overrides: RunOverrides = ClientRunOverrides {
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
