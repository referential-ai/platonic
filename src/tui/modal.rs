use crate::daemon::protocol::{BufferedStreamEvent, StreamEvent};
use platonic_core::HarnessEvent;
use serde_json::Value;

use super::LiveEventLine;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalModalView {
    pub run_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub effect: String,
    pub reason: String,
    pub input_preview: String,
    pub approval_preview: Option<String>,
    pub diff_preview: Option<String>,
}

pub fn approval_from_event(
    event: &StreamEvent,
    input_preview: Option<String>,
) -> Option<ApprovalModalView> {
    let StreamEvent::ApprovalRequested {
        run_id,
        tool_call_id,
        tool_name,
        effect,
        reason,
        approval_preview,
        diff_preview,
    } = event
    else {
        return None;
    };
    Some(ApprovalModalView {
        run_id: run_id.clone(),
        tool_call_id: tool_call_id.clone(),
        tool_name: tool_name.clone(),
        effect: serde_json::to_value(effect)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "unknown effect".into()),
        reason: reason.clone(),
        input_preview: input_preview.unwrap_or_else(|| "input preview unavailable".into()),
        approval_preview: approval_preview
            .as_deref()
            .filter(|preview| !preview.is_empty())
            .map(str::to_owned),
        diff_preview: diff_preview
            .as_deref()
            .filter(|diff| !diff.is_empty())
            .map(str::to_owned),
    })
}

pub fn tool_input_preview_from_event(event: &StreamEvent) -> Option<(String, String)> {
    let StreamEvent::Ledger { record } = event else {
        return None;
    };
    let HarnessEvent::ToolCallProposed { call, .. } = &record.event else {
        return None;
    };
    let call_id = call.id.to_string();
    let preview = serde_json::to_string_pretty(&call.input)
        .unwrap_or_else(|_| "input preview unavailable".into());
    Some((call_id, truncate_preview(preview, 1200)))
}

pub fn live_event_line(buffered: &BufferedStreamEvent) -> LiveEventLine {
    let offset = Some(buffered.offset);
    match &buffered.event {
        StreamEvent::Ledger { record } => ledger_event_line(offset, &record.event),
        StreamEvent::ApprovalRequested {
            tool_name, effect, ..
        } => {
            let effect = serde_json::to_value(effect)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown effect".into());
            LiveEventLine::warning(offset, format!("approval pending {tool_name} ({effect})"))
        }
        StreamEvent::AssistantDelta { text, .. } => LiveEventLine::assistant_delta(offset, text),
        StreamEvent::Canceled { .. } => LiveEventLine::status(offset, "canceled"),
        StreamEvent::Unknown(event) => match event.get("kind").and_then(Value::as_str) {
            Some(kind) => LiveEventLine::status(offset, kind),
            None => LiveEventLine::status(
                offset,
                serde_json::to_string(event).unwrap_or_else(|_| "unrenderable event".into()),
            ),
        },
    }
}

pub fn model_from_event(event: &StreamEvent) -> Option<String> {
    let StreamEvent::Ledger { record } = event else {
        return None;
    };
    match &record.event {
        HarnessEvent::ModelRequested { model, .. } => Some(model.to_string()),
        _ => None,
    }
}

fn ledger_event_line(offset: Option<u64>, event: &HarnessEvent) -> LiveEventLine {
    match event {
        HarnessEvent::ModelRequested { model, .. } => {
            LiveEventLine::status(offset, format!("model {model}"))
        }
        HarnessEvent::ModelResponded { output, .. } => {
            if output.content.is_empty() {
                LiveEventLine::status(offset, "assistant response")
            } else {
                LiveEventLine::assistant(offset, &output.content)
            }
        }
        HarnessEvent::ToolCallProposed { call, .. } => {
            LiveEventLine::tool(offset, format!("{} proposed", call.tool))
        }
        HarnessEvent::ToolStarted { call_id, .. } => {
            LiveEventLine::tool(offset, format!("{call_id} running"))
        }
        HarnessEvent::ToolFinished { result, .. } => LiveEventLine::tool(offset, &result.summary),
        HarnessEvent::ToolFailed { reason, .. } => {
            LiveEventLine::warning(offset, format!("tool failed: {reason}"))
        }
        HarnessEvent::RunFinished { .. } => LiveEventLine::status(offset, "run finished"),
        HarnessEvent::RunFailed { reason, .. } => LiveEventLine::warning(offset, reason),
        other => LiveEventLine::status(offset, other.name().replace('_', " ")),
    }
}

fn truncate_preview(mut preview: String, max_chars: usize) -> String {
    if preview.chars().count() <= max_chars {
        return preview;
    }
    preview = preview.chars().take(max_chars).collect();
    preview.push_str("\n... truncated");
    preview
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffered(offset: u64, event: Value) -> BufferedStreamEvent {
        serde_json::from_value(serde_json::json!({"offset": offset, "event": event})).unwrap()
    }

    fn ledger(offset: u64, event: Value) -> BufferedStreamEvent {
        buffered(
            offset,
            serde_json::json!({
                "kind": "ledger",
                "record": {
                    "seq": offset,
                    "occurred_at_ms": offset,
                    "event": event
                }
            }),
        )
    }

    #[test]
    fn formats_daemon_event_lines() {
        let approval = live_event_line(&buffered(
            4,
            serde_json::json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.write",
                "effect": "workspace_write",
                "reason": "approval required"
            }),
        ));
        let ledger = live_event_line(&ledger(
            5,
            serde_json::json!({
                "event": "tool_call_proposed",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "call": {
                    "id": "call_1",
                    "tool": "file.read",
                    "effect": "read_only",
                    "input": {}
                }
            }),
        ));
        let delta = live_event_line(&buffered(
            6,
            serde_json::json!({
                "kind": "assistant_delta",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "step": 0,
                "delta_index": 0,
                "text": "hello"
            }),
        ));

        assert_eq!(
            approval,
            LiveEventLine::warning(Some(4), "approval pending file.write (workspace_write)")
        );
        assert_eq!(ledger, LiveEventLine::tool(Some(5), "file.read proposed"));
        assert_eq!(delta, LiveEventLine::assistant_delta(Some(6), "hello"));
    }

    #[test]
    fn extracts_tool_input_preview_and_approval_modal_from_events() {
        let proposed = ledger(
            3,
            serde_json::json!({
                "event": "tool_call_proposed",
                "run_id": "run_1",
                "turn_id": "turn_1",
                "call": {
                    "id": "call_1",
                    "tool": "file.write",
                    "effect": "workspace_write",
                    "input": {
                        "path": "scratch.txt",
                        "content": "hello"
                    }
                }
            }),
        );
        let approval = buffered(
            4,
            serde_json::json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.write",
                "effect": "workspace_write",
                "reason": "file.write requires approval"
            }),
        );
        let (call_id, input_preview) = tool_input_preview_from_event(&proposed.event).unwrap();
        let modal = approval_from_event(&approval.event, Some(input_preview)).unwrap();

        assert_eq!(call_id, "call_1");
        assert_eq!(modal.run_id, "run_1");
        assert!(modal.input_preview.contains("scratch.txt"));
        assert!(modal.input_preview.contains("hello"));
        assert_eq!(modal.approval_preview, None);
        assert_eq!(modal.diff_preview, None);
    }

    #[test]
    fn approval_modal_prefers_diff_preview_when_present() {
        let approval = buffered(
            4,
            serde_json::json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.edit",
                "effect": "workspace_write",
                "reason": "file.edit requires approval",
                "diff_preview": "--- a/note.txt\n+++ b/note.txt\n@@ -1,1 +1,1 @@\n-old\n+new\n"
            }),
        );
        let modal =
            approval_from_event(&approval.event, Some(r#"{"path":"note.txt"}"#.into())).unwrap();

        assert!(modal.input_preview.contains("note.txt"));
        assert_eq!(modal.approval_preview, None);
        assert!(modal.diff_preview.as_ref().unwrap().contains("-old"));
    }

    #[test]
    fn approval_modal_ignores_empty_diff_preview() {
        let approval = buffered(
            4,
            serde_json::json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "file.edit",
                "effect": "workspace_write",
                "reason": "file.edit requires approval",
                "diff_preview": ""
            }),
        );
        let modal =
            approval_from_event(&approval.event, Some(r#"{"path":"note.txt"}"#.into())).unwrap();

        assert!(modal.input_preview.contains("note.txt"));
        assert_eq!(modal.approval_preview, None);
        assert_eq!(modal.diff_preview, None);
    }

    #[test]
    fn approval_modal_extracts_shell_approval_preview() {
        let approval = buffered(
            4,
            serde_json::json!({
                "kind": "approval_requested",
                "run_id": "run_1",
                "tool_call_id": "call_1",
                "tool_name": "shell.exec",
                "effect": "external_side_effect",
                "reason": "shell.exec requires approval",
                "approval_preview": "command: cargo test\ncwd: /tmp/work"
            }),
        );
        let modal =
            approval_from_event(&approval.event, Some(r#"{"command":"cargo test"}"#.into()))
                .unwrap();

        assert_eq!(
            modal.approval_preview.as_deref(),
            Some("command: cargo test\ncwd: /tmp/work")
        );
        assert_eq!(modal.diff_preview, None);
    }
}
