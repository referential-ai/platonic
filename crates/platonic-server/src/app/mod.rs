mod context;
mod prepare;
mod run_loop;
mod session;
mod tool_exec;
mod types;

pub use session::RunSession;
pub use types::{
    ApprovalHandler, ApprovalMode, ApprovalRequest, AssistantDeltaEvent, RunEvent, RunLedger,
    RunOptions, RunOutcome,
};

pub(crate) use prepare::{PreparedRun, prepare_run, prepare_run_for_thread};
pub(crate) use run_loop::run_prepared_question;
pub(crate) use types::ExternalApprovalOutcome;

use crate::AppResult;
use platonic_core::RunId;
use std::sync::atomic::{AtomicU64, Ordering};

static ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn run_question(options: RunOptions) -> AppResult<RunOutcome> {
    let (prepared, mut recorder) = prepare_run(&options)?;
    run_prepared_question(
        prepared,
        &mut recorder,
        options.approval_mode,
        options.event_sender,
        options.stream_to_stderr,
        options.cancel,
        crate::tools::RunToolHandlers::default(),
    )
}

pub fn new_run_id() -> AppResult<RunId> {
    Ok(RunId::new(generated_id("run"))?)
}

pub fn new_session_id() -> String {
    generated_id("session")
}

fn generated_id(prefix: &str) -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!(
        "{}_{}_{}_{}",
        prefix,
        millis,
        std::process::id(),
        ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::run_loop::print_fallback_assistant_text;
    use super::*;

    pub(super) const FALLBACK_ASSISTANT_TEXT: &str = "fallback assistant text";

    #[test]
    fn generated_run_and_session_ids_are_unique() {
        let first_run = new_run_id().unwrap();
        let second_run = new_run_id().unwrap();
        let first_session = new_session_id();
        let second_session = new_session_id();

        assert_ne!(first_run, second_run);
        assert_ne!(first_session, second_session);
    }

    #[test]
    #[ignore = "subprocess helper for deterministic stderr capture"]
    fn fallback_assistant_text_capture_child() {
        let stream_to_stderr = std::env::var("PLATO_FALLBACK_CAPTURE_STREAM").unwrap() == "true";
        print_fallback_assistant_text(stream_to_stderr, 0, FALLBACK_ASSISTANT_TEXT);
    }
}
