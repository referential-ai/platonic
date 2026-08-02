use plato_agent::tui::{
    ActiveRunView, ApprovalModalView, ConnectionState, LiveEventKind, LiveEventLine,
    SessionPickerView, TranscriptState, TranscriptView, TuiOptions, TuiState, approval_from_event,
    live_event_line, model_from_event, render, render_snapshot, run_tui,
    tool_input_preview_from_event,
};
#[test]
fn historical_tui_imports_resolve_to_the_extracted_owner() {
    let state = TuiState::disconnected("/workspace".into(), "/socket".into(), "offline".into());
    let extracted: &plato_tui::TuiState = &state;
    assert_eq!(
        render_snapshot(&state, 60, 12).unwrap(),
        plato_tui::render_snapshot(extracted, 60, 12).unwrap()
    );

    let _: fn(TuiOptions) -> plato_agent::AppResult<()> = run_tui;
    let _ = (
        std::mem::size_of::<ActiveRunView>(),
        std::mem::size_of::<ApprovalModalView>(),
        std::mem::size_of::<ConnectionState>(),
        std::mem::size_of::<LiveEventKind>(),
        std::mem::size_of::<LiveEventLine>(),
        std::mem::size_of::<SessionPickerView>(),
        std::mem::size_of::<TranscriptState>(),
        std::mem::size_of::<TranscriptView>(),
        approval_from_event,
        live_event_line,
        model_from_event,
        render,
        tool_input_preview_from_event,
    );
}

#[test]
fn compatibility_wrapper_preserves_client_error_display() {
    let parent = tempfile::tempdir().unwrap();
    let options = TuiOptions {
        workspace: parent.path().join("missing"),
        socket: None,
        run: None,
        config: None,
        snapshot: true,
        reduced_motion: false,
    };

    let direct = plato_tui::run_tui(options.clone()).unwrap_err();
    let compatible = run_tui(options).unwrap_err();
    assert_eq!(compatible.to_string(), direct.to_string());
    assert!(direct.to_string().starts_with("io error: "));
}
