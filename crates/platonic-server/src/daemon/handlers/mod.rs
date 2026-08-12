mod control;
mod registry;
mod runs;
mod sessions;
mod threads;
mod types;

pub(super) use runs::reconcile_one_shot_run_roots;

use crate::daemon::{
    protocol::{Envelope, ProtocolRequest, decode_request},
    runtime::DaemonRuntime,
};

pub(super) fn handle_line(runtime: &DaemonRuntime, line: &str) -> Envelope {
    match decode_request(line) {
        Ok(request) => handle_request(runtime, request),
        Err(error) => *error,
    }
}

pub(super) fn handle_request(runtime: &DaemonRuntime, request: Envelope) -> Envelope {
    match request
        .params
        .clone()
        .expect("decoded request carries typed params")
    {
        ProtocolRequest::Hello(params) => control::handle_hello(runtime, request, params),
        ProtocolRequest::RunStart(params) => runs::handle_run_start(runtime, request, params),
        ProtocolRequest::MessageAppend(params) => {
            runs::handle_message_append(runtime, request, params)
        }
        ProtocolRequest::IssuePrepStart(params) => {
            runs::handle_issue_prep_start(runtime, request, params)
        }
        ProtocolRequest::EventsStream(params) => {
            runs::handle_events_stream(runtime, request, params)
        }
        ProtocolRequest::ApprovalDecide(params) => {
            runs::handle_approval_decide(runtime, request, params)
        }
        ProtocolRequest::RunCancel(params) => runs::handle_run_cancel(runtime, request, params),
        ProtocolRequest::SessionsList => sessions::handle_sessions_list(runtime, request),
        ProtocolRequest::TranscriptRead(params) => {
            sessions::handle_transcript_read(runtime, request, params)
        }
        ProtocolRequest::DaemonStatus(params) => {
            control::handle_daemon_status(runtime, request, params)
        }
        ProtocolRequest::SessionApprovalProfileSet(params) => {
            sessions::handle_session_approval_profile_set(runtime, request, params)
        }
        ProtocolRequest::DaemonShutdownIfIdle => control::handle_shutdown_if_idle(runtime, request),
        ProtocolRequest::ThreadSpawn(params) => {
            threads::handle_thread_spawn(runtime, request, params)
        }
        ProtocolRequest::ThreadList => threads::handle_thread_list(runtime, request),
        ProtocolRequest::ThreadStatus(params) => {
            threads::handle_thread_status(runtime, request, params)
        }
        ProtocolRequest::ThreadAuthority(params) => {
            threads::handle_thread_authority(runtime, request, params)
        }
        ProtocolRequest::ThreadSend(params) => {
            threads::handle_thread_send(runtime, request, params)
        }
        ProtocolRequest::ThreadEvents(params) => {
            threads::handle_thread_events(runtime, request, params)
        }
        ProtocolRequest::ThreadStop(params) => {
            threads::handle_thread_stop(runtime, request, params)
        }
        ProtocolRequest::WorkspaceCreate(params) => {
            registry::handle_workspace_create(runtime, request, params)
        }
        ProtocolRequest::WorkspaceList(params) => {
            registry::handle_workspace_list(runtime, request, params)
        }
        ProtocolRequest::WorkspaceStatus(params) => {
            registry::handle_workspace_status(runtime, request, params)
        }
        ProtocolRequest::AgentCreate(params) => {
            registry::handle_agent_create(runtime, request, params)
        }
        ProtocolRequest::AgentList(params) => registry::handle_agent_list(runtime, request, params),
        ProtocolRequest::AgentStatus(params) => {
            registry::handle_agent_status(runtime, request, params)
        }
    }
}
