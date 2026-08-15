use super::{
    Gateway, HttpRequest, HttpResponse, error_response,
    idempotency::{Reservation, StoredResponse},
    native_transport_error, now_ms, target_error, wire,
};
use platonic_client::ClientError;
use platonic_protocol::{BufferedStreamEvent, BufferedThreadEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io::ErrorKind,
    net::TcpStream,
    time::{Duration, Instant},
};

const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_APPROVAL_REASON_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const EVENT_LIMIT: usize = 128;
const THREAD_POLL_MS: u64 = 1_000;
const STREAM_LIFETIME: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug)]
enum Route<'a> {
    Status,
    Workspaces,
    Profiles {
        workspace_id: &'a str,
    },
    Profile {
        workspace_id: &'a str,
        profile_id: &'a str,
    },
    Threads {
        workspace_id: &'a str,
        profile_id: &'a str,
    },
    Thread {
        workspace_id: &'a str,
        profile_id: &'a str,
        thread_id: &'a str,
    },
    ThreadAuthority {
        workspace_id: &'a str,
        profile_id: &'a str,
        thread_id: &'a str,
    },
    Messages {
        workspace_id: &'a str,
        profile_id: &'a str,
        thread_id: &'a str,
    },
    ThreadEvents {
        workspace_id: &'a str,
        profile_id: &'a str,
        thread_id: &'a str,
    },
    Stop {
        workspace_id: &'a str,
        profile_id: &'a str,
        thread_id: &'a str,
    },
    Transcript {
        workspace_id: &'a str,
        profile_id: &'a str,
        run_id: &'a str,
    },
    RunEvents {
        workspace_id: &'a str,
        profile_id: &'a str,
        run_id: &'a str,
    },
    Cancel {
        workspace_id: &'a str,
        profile_id: &'a str,
        run_id: &'a str,
    },
    Approval {
        workspace_id: &'a str,
        profile_id: &'a str,
        run_id: &'a str,
        tool_call_id: &'a str,
    },
}

#[derive(Clone, Copy)]
enum MutationTarget<'a> {
    Thread {
        workspace_id: &'a str,
        profile_id: &'a str,
        thread_id: &'a str,
    },
    Run {
        workspace_id: &'a str,
        profile_id: &'a str,
        run_id: &'a str,
    },
}

impl<'a> MutationTarget<'a> {
    fn workspace_id(self) -> &'a str {
        match self {
            Self::Thread { workspace_id, .. } | Self::Run { workspace_id, .. } => workspace_id,
        }
    }

    fn profile_id(self) -> &'a str {
        match self {
            Self::Thread { profile_id, .. } | Self::Run { profile_id, .. } => profile_id,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MessageBody {
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prior_interrupted_run_id: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApprovalBody {
    decision: HttpApprovalDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum HttpApprovalDecision {
    Grant,
    Deny,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyBody {}

pub(super) fn handle(
    gateway: &Gateway,
    request: HttpRequest,
    stream: &mut TcpStream,
) -> Option<HttpResponse> {
    let principal = match gateway.authenticate(&request) {
        Ok(principal) => principal,
        Err(response) => return Some(response),
    };
    if request.target.contains('?') {
        return Some(error_response(
            400,
            "malformed_request",
            "query parameters are not supported",
        ));
    }
    let route = match parse_route(&request.target) {
        Some(route) => route,
        None => {
            return Some(error_response(
                404,
                "not_found",
                "the HTTP route does not exist",
            ));
        }
    };
    let expected_method = match route {
        Route::Messages { .. }
        | Route::Stop { .. }
        | Route::Cancel { .. }
        | Route::Approval { .. } => "POST",
        _ => "GET",
    };
    if request.method != expected_method {
        return Some(
            error_response(
                405,
                "method_not_allowed",
                "the HTTP method is not admitted for this route",
            )
            .with_header("Allow", expected_method),
        );
    }
    let _principal_guard = match gateway.limits.acquire_principal(&principal.name) {
        Ok(guard) => guard,
        Err(response) => return Some(response),
    };

    match route {
        Route::Status => read_status(gateway, &principal, &request),
        Route::Workspaces => read_workspaces(gateway, &principal, &request),
        Route::Profiles { workspace_id } => {
            read_profiles(gateway, &principal, workspace_id, &request)
        }
        Route::Profile {
            workspace_id,
            profile_id,
        } => read_profile(gateway, &principal, workspace_id, profile_id, &request),
        Route::Threads {
            workspace_id,
            profile_id,
        } => read_threads(gateway, &principal, workspace_id, profile_id, &request),
        Route::Thread {
            workspace_id,
            profile_id,
            thread_id,
        } => read_thread(
            gateway,
            &principal,
            workspace_id,
            profile_id,
            thread_id,
            &request,
        ),
        Route::ThreadAuthority {
            workspace_id,
            profile_id,
            thread_id,
        } => read_authority(
            gateway,
            &principal,
            workspace_id,
            profile_id,
            thread_id,
            &request,
        ),
        Route::Transcript {
            workspace_id,
            profile_id,
            run_id,
        } => read_transcript(
            gateway,
            &principal,
            workspace_id,
            profile_id,
            run_id,
            &request,
        ),
        Route::Messages {
            workspace_id,
            profile_id,
            thread_id,
        } => mutation(
            gateway,
            &principal,
            &request,
            ("thread.send", &[workspace_id, profile_id, thread_id]),
            MutationTarget::Thread {
                workspace_id,
                profile_id,
                thread_id,
            },
            parse_message,
            |client, body| {
                client.thread_send_with_prior_interruption(
                    thread_id.into(),
                    principal.name.clone(),
                    body.turn_id,
                    body.message,
                    body.prior_interrupted_run_id,
                )
            },
        ),
        Route::Stop {
            workspace_id,
            profile_id,
            thread_id,
        } => mutation(
            gateway,
            &principal,
            &request,
            ("thread.stop", &[workspace_id, profile_id, thread_id]),
            MutationTarget::Thread {
                workspace_id,
                profile_id,
                thread_id,
            },
            parse_empty,
            |client, _| {
                #[cfg(unix)]
                client.set_timeout(STOP_TIMEOUT)?;
                client.thread_stop(thread_id.into(), principal.name.clone())
            },
        ),
        Route::Cancel {
            workspace_id,
            profile_id,
            run_id,
        } => mutation(
            gateway,
            &principal,
            &request,
            ("run.cancel", &[workspace_id, profile_id, run_id]),
            MutationTarget::Run {
                workspace_id,
                profile_id,
                run_id,
            },
            parse_empty,
            |client, _| client.run_cancel_as(run_id, principal.name.clone()),
        ),
        Route::Approval {
            workspace_id,
            profile_id,
            run_id,
            tool_call_id,
        } => mutation(
            gateway,
            &principal,
            &request,
            (
                "approval.decide",
                &[workspace_id, profile_id, run_id, tool_call_id],
            ),
            MutationTarget::Run {
                workspace_id,
                profile_id,
                run_id,
            },
            parse_approval,
            |client, body| match body.decision {
                HttpApprovalDecision::Grant => {
                    client.approval_grant_as(run_id, tool_call_id, principal.name.clone())
                }
                HttpApprovalDecision::Deny => client.approval_deny_as(
                    run_id,
                    tool_call_id,
                    principal.name.clone(),
                    body.reason.unwrap_or_default(),
                ),
            },
        ),
        Route::ThreadEvents {
            workspace_id,
            profile_id,
            thread_id,
        } => {
            stream_thread_events(
                gateway,
                &principal,
                workspace_id,
                profile_id,
                thread_id,
                &request,
                stream,
            );
            None
        }
        Route::RunEvents {
            workspace_id,
            profile_id,
            run_id,
        } => {
            stream_run_events(
                gateway,
                &principal,
                workspace_id,
                profile_id,
                run_id,
                &request,
                stream,
            );
            None
        }
    }
}

fn parse_route(target: &str) -> Option<Route<'_>> {
    let segments = target.strip_prefix('/')?.split('/').collect::<Vec<_>>();
    if segments.iter().any(|segment| segment.is_empty()) {
        return None;
    }
    match segments.as_slice() {
        ["v2", "status"] => Some(Route::Status),
        ["v2", "workspaces"] => Some(Route::Workspaces),
        ["v2", "workspaces", workspace_id, "profiles"] => Some(Route::Profiles { workspace_id }),
        ["v2", "workspaces", workspace_id, "profiles", profile_id] => Some(Route::Profile {
            workspace_id,
            profile_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "threads",
        ] => Some(Route::Threads {
            workspace_id,
            profile_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "threads",
            thread_id,
        ] => Some(Route::Thread {
            workspace_id,
            profile_id,
            thread_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "threads",
            thread_id,
            "authority",
        ] => Some(Route::ThreadAuthority {
            workspace_id,
            profile_id,
            thread_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "threads",
            thread_id,
            "messages",
        ] => Some(Route::Messages {
            workspace_id,
            profile_id,
            thread_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "threads",
            thread_id,
            "events",
        ] => Some(Route::ThreadEvents {
            workspace_id,
            profile_id,
            thread_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "threads",
            thread_id,
            "stop",
        ] => Some(Route::Stop {
            workspace_id,
            profile_id,
            thread_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "runs",
            run_id,
            "transcript",
        ] => Some(Route::Transcript {
            workspace_id,
            profile_id,
            run_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "runs",
            run_id,
            "events",
        ] => Some(Route::RunEvents {
            workspace_id,
            profile_id,
            run_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "runs",
            run_id,
            "cancel",
        ] => Some(Route::Cancel {
            workspace_id,
            profile_id,
            run_id,
        }),
        [
            "v2",
            "workspaces",
            workspace_id,
            "profiles",
            profile_id,
            "runs",
            run_id,
            "approvals",
            tool_call_id,
        ] => Some(Route::Approval {
            workspace_id,
            profile_id,
            run_id,
            tool_call_id,
        }),
        _ => None,
    }
}

fn read_status(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    let workspace_id = match principal.workspace_ids.first() {
        Some(workspace_id) => workspace_id,
        None => {
            return Some(error_response(
                403,
                "forbidden_scope",
                "no workspace is admitted",
            ));
        }
    };
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => return Some(response),
    };
    match client.daemon_status(None, None) {
        Ok(status) => read_json_response(
            200,
            &serde_json::json!({
                "protocol_version": platonic_protocol::PROTOCOL_VERSION,
                "daemon": status.daemon,
                "model": status.model,
            }),
        ),
        Err(error) => Some(native_error(&error)),
    }
}

fn read_workspaces(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    let mut client = match gateway.control_client() {
        Ok(client) => client,
        Err(error) => return Some(native_transport_error(&error)),
    };
    let result = match client.workspace_list() {
        Ok(result) => result,
        Err(error) => return Some(native_error(&error)),
    };
    let workspaces = result
        .workspaces
        .into_iter()
        .filter(|workspace| principal.workspace_ids.contains(&workspace.id))
        .collect::<Vec<_>>();
    read_json_response(200, &serde_json::json!({ "workspaces": workspaces }))
}

fn read_profiles(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    if let Err(response) = gateway.authorize_workspace(principal, workspace_id) {
        return Some(response);
    }
    let mut client = match gateway.control_client() {
        Ok(client) => client,
        Err(error) => return Some(native_transport_error(&error)),
    };
    let result = match client.profile_list(Some(workspace_id.into()), Some(100)) {
        Ok(result) => result,
        Err(error) => return Some(native_error(&error)),
    };
    let profiles = result
        .profiles
        .into_iter()
        .filter(|profile| {
            principal.profile_ids.is_empty()
                || principal
                    .profile_ids
                    .iter()
                    .any(|id| id == profile.id.as_str())
        })
        .collect::<Vec<_>>();
    read_json_response(
        200,
        &serde_json::json!({ "profiles": profiles, "truncated": result.truncated }),
    )
}

fn read_profile(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    match gateway.authorize_profile(principal, workspace_id, profile_id) {
        Ok(status) => read_json_response(200, &status),
        Err(response) => Some(response),
    }
}

fn read_threads(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    if let Err(response) = gateway.authorize_profile(principal, workspace_id, profile_id) {
        return Some(response);
    }
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => return Some(response),
    };
    let result = match client.thread_list() {
        Ok(result) => result,
        Err(error) => return Some(native_error(&error)),
    };
    let threads = result
        .threads
        .into_iter()
        .filter(|thread| {
            thread
                .authority
                .profile_id
                .as_ref()
                .is_some_and(|id| id.as_str() == profile_id)
        })
        .collect::<Vec<_>>();
    read_json_response(200, &serde_json::json!({ "threads": threads }))
}

fn read_thread(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    thread_id: &str,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    if let Err(response) = gateway.authorize_profile_scope(principal, workspace_id, profile_id) {
        return Some(response);
    }
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => return Some(response),
    };
    if let Err(response) =
        gateway.authorize_thread(principal, &mut client, workspace_id, profile_id, thread_id)
    {
        return Some(response);
    }
    match client.thread_status(thread_id.into()) {
        Ok(result) => read_json_response(200, &result),
        Err(error) => Some(target_error(&error)),
    }
}

fn read_authority(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    thread_id: &str,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    if let Err(response) = gateway.authorize_profile_scope(principal, workspace_id, profile_id) {
        return Some(response);
    }
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => return Some(response),
    };
    match gateway.authorize_thread(principal, &mut client, workspace_id, profile_id, thread_id) {
        Ok(result) => read_json_response(200, &result),
        Err(response) => Some(response),
    }
}

fn read_transcript(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    run_id: &str,
    request: &HttpRequest,
) -> Option<HttpResponse> {
    if let Err(response) = require_empty_body(request) {
        return Some(response);
    }
    if let Err(response) = gateway.authorize_profile_scope(principal, workspace_id, profile_id) {
        return Some(response);
    }
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => return Some(response),
    };
    if let Err(response) =
        gateway.authorize_run(principal, &mut client, workspace_id, profile_id, run_id)
    {
        return Some(response);
    }
    match client.transcript_read(run_id) {
        Ok(result) => read_json_response(200, &result),
        Err(error) => Some(target_error(&error)),
    }
}

fn mutation<T, R>(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    request: &HttpRequest,
    operation: (&str, &[&str]),
    target: MutationTarget<'_>,
    parse_body: impl Fn(&HttpRequest) -> Result<T, HttpResponse>,
    dispatch: impl FnOnce(&mut platonic_client::client::DaemonClient, T) -> Result<R, ClientError>,
) -> Option<HttpResponse>
where
    R: Serialize,
{
    let body = match parse_body(request) {
        Ok(body) => body,
        Err(response) => return Some(response),
    };
    let key = match idempotency_key(request) {
        Ok(key) => key,
        Err(response) => return Some(response),
    };
    if let Err(response) = gateway.authorize_workspace(principal, target.workspace_id()) {
        return Some(response);
    }
    if let Err(response) =
        gateway.authorize_profile_scope(principal, target.workspace_id(), target.profile_id())
    {
        return Some(response);
    }
    let canonical = match canonical_request_body(request) {
        Ok(canonical) => canonical,
        Err(response) => return Some(response),
    };
    let key_hash: [u8; 32] = Sha256::digest(key.as_bytes()).into();
    let fingerprint = fingerprint(&principal.name, operation.0, operation.1, &canonical);
    let reservation = match gateway
        .idempotency
        .lock()
        .expect("HTTP idempotency lock poisoned")
        .reserve(&principal.name, &key_hash, &fingerprint, now_ms())
    {
        Ok(reservation) => reservation,
        Err(_) => {
            return Some(error_response(
                503,
                "idempotency_unavailable",
                "the idempotency store is unavailable",
            ));
        }
    };
    match reservation {
        Reservation::InProgress => {
            return Some(
                error_response(
                    409,
                    "idempotency_in_progress",
                    "the idempotent request is still in progress",
                )
                .with_header("Retry-After", "1"),
            );
        }
        Reservation::Replay(response) => {
            return Some(
                HttpResponse::json(response.status, response.body)
                    .with_header("Idempotency-Replayed", "true"),
            );
        }
        Reservation::Conflict => {
            return Some(error_response(
                409,
                "idempotency_key_conflict",
                "the idempotency key was used for a different request",
            ));
        }
        Reservation::Ambiguous => {
            return Some(error_response(
                409,
                "idempotency_outcome_unknown",
                "the earlier request outcome is unknown; inspect state and use a new key",
            ));
        }
        Reservation::Fresh => {}
    }

    if !gateway
        .limits
        .admit_mutation(&principal.name, Instant::now())
    {
        let response = error_response(
            429,
            "rate_limited",
            "the principal mutation rate limit is reached",
        );
        let stored = StoredResponse {
            status: response.status,
            body: response.body.clone(),
        };
        if gateway
            .idempotency
            .lock()
            .expect("HTTP idempotency lock poisoned")
            .complete(
                &principal.name,
                &key_hash,
                &fingerprint,
                &stored,
                true,
                now_ms(),
            )
            .is_err()
        {
            return Some(error_response(
                503,
                "idempotency_unavailable",
                "the rate-limit outcome could not be recorded",
            ));
        }
        return Some(response);
    }

    let mut client = match gateway.workspace_client(principal, target.workspace_id()) {
        Ok(client) => client,
        Err(response) => {
            if store_response(gateway, principal, &key_hash, &fingerprint, &response, true).is_err()
            {
                return Some(idempotency_record_error());
            }
            return Some(response);
        }
    };
    let scope = match target {
        MutationTarget::Thread {
            workspace_id,
            profile_id,
            thread_id,
        } => gateway
            .authorize_thread(principal, &mut client, workspace_id, profile_id, thread_id)
            .map(|_| ()),
        MutationTarget::Run {
            workspace_id,
            profile_id,
            run_id,
        } => gateway.authorize_run(principal, &mut client, workspace_id, profile_id, run_id),
    };
    if let Err(response) = scope {
        if store_response(gateway, principal, &key_hash, &fingerprint, &response, true).is_err() {
            return Some(idempotency_record_error());
        }
        return Some(response);
    }
    let (response, known_error) = match dispatch(&mut client, body) {
        Ok(result) => match json_response(200, &result) {
            Some(response) => (response, false),
            None => {
                if mark_ambiguous(gateway, principal, &key_hash, &fingerprint).is_err() {
                    return Some(idempotency_record_error());
                }
                return Some(error_response(
                    502,
                    "native_response_too_large",
                    "the native response exceeds the gateway limit",
                ));
            }
        },
        Err(ClientError::DaemonResponse(error)) => (native_protocol_error(&error), true),
        Err(error) => {
            if mark_ambiguous(gateway, principal, &key_hash, &fingerprint).is_err() {
                return Some(idempotency_record_error());
            }
            return Some(native_transport_error(&error));
        }
    };
    if store_response(
        gateway,
        principal,
        &key_hash,
        &fingerprint,
        &response,
        known_error,
    )
    .is_err()
    {
        return Some(idempotency_record_error());
    }
    Some(response)
}

fn store_response(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    key_hash: &[u8; 32],
    fingerprint: &[u8; 32],
    response: &HttpResponse,
    known_error: bool,
) -> Result<(), ()> {
    gateway
        .idempotency
        .lock()
        .expect("HTTP idempotency lock poisoned")
        .complete(
            &principal.name,
            key_hash,
            fingerprint,
            &StoredResponse {
                status: response.status,
                body: response.body.clone(),
            },
            known_error,
            now_ms(),
        )
        .map_err(|_| ())
}

fn idempotency_record_error() -> HttpResponse {
    error_response(
        500,
        "idempotency_unavailable",
        "the native outcome could not be recorded",
    )
}

fn stream_thread_events(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    thread_id: &str,
    request: &HttpRequest,
    stream: &mut TcpStream,
) {
    if require_empty_body(request).is_err() {
        let _ = wire::write_response(
            stream,
            &error_response(400, "malformed_request", "GET requests have no body"),
        );
        return;
    }
    if let Err(response) = gateway.authorize_profile_scope(principal, workspace_id, profile_id) {
        let _ = wire::write_response(stream, &response);
        return;
    }
    let _stream_guard = match gateway.limits.acquire_stream(&principal.name) {
        Ok(guard) => guard,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    let cursor = match thread_event_cursor(request) {
        Ok(cursor) => cursor,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    if let Err(response) =
        gateway.authorize_thread(principal, &mut client, workspace_id, profile_id, thread_id)
    {
        let _ = wire::write_response(stream, &response);
        return;
    }
    let (requested_epoch, mut offset) = cursor
        .map(|(epoch, offset)| (Some(epoch), Some(offset)))
        .unwrap_or((None, None));
    let first = match client.thread_events_in_epoch(
        thread_id.into(),
        requested_epoch,
        offset,
        EVENT_LIMIT,
        THREAD_POLL_MS,
    ) {
        Ok(page) => page,
        Err(error) => {
            let _ = wire::write_response(stream, &event_error(&error));
            return;
        }
    };
    if first.reset.is_some() {
        let _ = wire::write_response(stream, &super::cursor_unavailable());
        return;
    }
    let live_epoch_id = first.live_epoch_id.clone();
    if wire::write_sse_headers(stream).is_err() {
        return;
    }
    offset = Some(first.next_offset);
    if write_thread_page(stream, &live_epoch_id, first.events).is_err() {
        return;
    }
    let started = Instant::now();
    while started.elapsed() < STREAM_LIFETIME {
        match client.thread_events_in_epoch(
            thread_id.into(),
            Some(live_epoch_id.clone()),
            offset,
            EVENT_LIMIT,
            THREAD_POLL_MS,
        ) {
            Ok(page) => {
                if page.reset.is_some() || page.live_epoch_id != live_epoch_id {
                    let _ = wire::write_sse_error(stream, &super::cursor_unavailable().body);
                    return;
                }
                offset = Some(page.next_offset);
                if page.events.is_empty() {
                    if wire::write_sse_keepalive(stream).is_err() {
                        return;
                    }
                } else if write_thread_page(stream, &live_epoch_id, page.events).is_err() {
                    return;
                }
            }
            Err(error) => {
                let body = event_error(&error).body;
                let _ = wire::write_sse_error(stream, &body);
                return;
            }
        }
    }
}

fn stream_run_events(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    workspace_id: &str,
    profile_id: &str,
    run_id: &str,
    request: &HttpRequest,
    stream: &mut TcpStream,
) {
    if require_empty_body(request).is_err() {
        let _ = wire::write_response(
            stream,
            &error_response(400, "malformed_request", "GET requests have no body"),
        );
        return;
    }
    if let Err(response) = gateway.authorize_profile_scope(principal, workspace_id, profile_id) {
        let _ = wire::write_response(stream, &response);
        return;
    }
    let _stream_guard = match gateway.limits.acquire_stream(&principal.name) {
        Ok(guard) => guard,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    let mut offset = match event_offset(request) {
        Ok(offset) => offset,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    let target = format!("run\0{workspace_id}\0{profile_id}\0{run_id}");
    let generation = match gateway.daemon_generation() {
        Ok(generation) => generation,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    if let Err(response) = gateway.admit_stream_cursor(&target, generation, offset) {
        let _ = wire::write_response(stream, &response);
        return;
    }
    let mut client = match gateway.workspace_client(principal, workspace_id) {
        Ok(client) => client,
        Err(response) => {
            let _ = wire::write_response(stream, &response);
            return;
        }
    };
    if let Err(response) =
        gateway.authorize_run(principal, &mut client, workspace_id, profile_id, run_id)
    {
        let _ = wire::write_response(stream, &response);
        return;
    }
    if let Some(requested) = offset
        && gateway.known_stream_tip(&target, generation).is_none()
    {
        let tip = match client.events_stream(run_id, None, 1) {
            Ok(page) => page.next_offset,
            Err(error) => {
                let _ = wire::write_response(stream, &event_error(&error));
                return;
            }
        };
        if requested > tip {
            let _ = wire::write_response(stream, &super::cursor_unavailable());
            return;
        }
    }
    let first = match client.events_stream(run_id, offset, EVENT_LIMIT) {
        Ok(page) => page,
        Err(error) => {
            let _ = wire::write_response(stream, &event_error(&error));
            return;
        }
    };
    if gateway.daemon_generation().ok() != Some(generation) {
        let _ = wire::write_response(stream, &super::cursor_unavailable());
        return;
    }
    if let Err(response) =
        gateway.record_stream_cursor(target.clone(), generation, first.next_offset)
    {
        let _ = wire::write_response(stream, &response);
        return;
    }
    if wire::write_sse_headers(stream).is_err() {
        return;
    }
    offset = Some(first.next_offset);
    if write_run_page(stream, first.events).is_err() {
        return;
    }
    let started = Instant::now();
    while started.elapsed() < STREAM_LIFETIME {
        std::thread::sleep(Duration::from_secs(1));
        if gateway.daemon_generation().ok() != Some(generation) {
            let _ = wire::write_sse_error(stream, &super::cursor_unavailable().body);
            return;
        }
        match client.events_stream(run_id, offset, EVENT_LIMIT) {
            Ok(page) => {
                if let Err(response) =
                    gateway.record_stream_cursor(target.clone(), generation, page.next_offset)
                {
                    let _ = wire::write_sse_error(stream, &response.body);
                    return;
                }
                offset = Some(page.next_offset);
                if page.events.is_empty() {
                    if wire::write_sse_keepalive(stream).is_err() {
                        return;
                    }
                } else if write_run_page(stream, page.events).is_err() {
                    return;
                }
            }
            Err(error) => {
                let body = event_error(&error).body;
                let _ = wire::write_sse_error(stream, &body);
                return;
            }
        }
    }
}

fn write_thread_page(
    stream: &mut TcpStream,
    live_epoch_id: &str,
    events: Vec<BufferedThreadEvent>,
) -> std::io::Result<()> {
    for buffered in events {
        let next_offset = event_cursor(buffered.offset)?;
        let data = serde_json::to_vec(&serde_json::json!({
            "live_epoch_id": live_epoch_id,
            "offset": buffered.offset,
            "turn_id": buffered.turn_id,
            "event": buffered.event,
        }))
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
        wire::write_sse_event(stream, format!("{live_epoch_id}:{next_offset}"), &data)?;
    }
    Ok(())
}

fn write_run_page(stream: &mut TcpStream, events: Vec<BufferedStreamEvent>) -> std::io::Result<()> {
    for buffered in events {
        let data = serde_json::to_vec(&buffered)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
        wire::write_sse_event(stream, event_cursor(buffered.offset)?, &data)?;
    }
    Ok(())
}

fn event_cursor(offset: u64) -> std::io::Result<u64> {
    offset
        .checked_add(1)
        .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, "event offset overflow"))
}

fn event_offset(request: &HttpRequest) -> Result<Option<u64>, HttpResponse> {
    let values = request.header_values("last-event-id").collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Some)
            .ok_or_else(|| {
                error_response(
                    400,
                    "invalid_event_cursor",
                    "Last-Event-ID must be an exact unsigned native offset",
                )
            }),
        _ => Err(error_response(
            400,
            "invalid_event_cursor",
            "Last-Event-ID must occur at most once",
        )),
    }
}

fn thread_event_cursor(request: &HttpRequest) -> Result<Option<(String, u64)>, HttpResponse> {
    let values = request.header_values("last-event-id").collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.split_once(':'))
            .and_then(|(epoch, offset)| {
                (!epoch.is_empty())
                    .then(|| {
                        offset
                            .parse::<u64>()
                            .ok()
                            .map(|offset| (epoch.into(), offset))
                    })
                    .flatten()
            })
            .map(Some)
            .ok_or_else(|| {
                error_response(
                    400,
                    "invalid_event_cursor",
                    "Last-Event-ID must be <live_epoch_id>:<native_offset>",
                )
            }),
        _ => Err(error_response(
            400,
            "invalid_event_cursor",
            "Last-Event-ID must occur at most once",
        )),
    }
}

fn event_error(error: &ClientError) -> HttpResponse {
    match error {
        ClientError::DaemonResponse(protocol) if protocol.code.as_str() == "lagged" => {
            error_response(
                409,
                "event_cursor_unavailable",
                "the native event cursor is unavailable; inspect status or transcript",
            )
        }
        ClientError::DaemonResponse(_) => target_error(error),
        _ => native_transport_error(error),
    }
}

fn parse_message(request: &HttpRequest) -> Result<MessageBody, HttpResponse> {
    let body: MessageBody = parse_json(request)?;
    if body.message.is_empty() || body.message.len() > MAX_MESSAGE_BYTES {
        return Err(error_response(
            400,
            "invalid_message",
            "message must contain 1 through 262144 UTF-8 bytes",
        ));
    }
    Ok(body)
}

fn parse_approval(request: &HttpRequest) -> Result<ApprovalBody, HttpResponse> {
    let body: ApprovalBody = parse_json(request)?;
    if body
        .reason
        .as_ref()
        .is_some_and(|reason| reason.len() > MAX_APPROVAL_REASON_BYTES)
    {
        return Err(error_response(
            400,
            "invalid_approval_reason",
            "approval reason exceeds 16384 UTF-8 bytes",
        ));
    }
    if matches!(body.decision, HttpApprovalDecision::Grant) && body.reason.is_some() {
        return Err(error_response(
            400,
            "malformed_request",
            "grant decisions do not admit a reason",
        ));
    }
    Ok(body)
}

fn parse_empty(request: &HttpRequest) -> Result<EmptyBody, HttpResponse> {
    if request.body.is_empty() {
        return Ok(EmptyBody {});
    }
    parse_json(request)
}

fn parse_json<T: for<'de> Deserialize<'de>>(request: &HttpRequest) -> Result<T, HttpResponse> {
    let content_types = request.header_values("content-type").collect::<Vec<_>>();
    if content_types.len() != 1
        || !std::str::from_utf8(content_types[0])
            .ok()
            .is_some_and(|value| value.eq_ignore_ascii_case("application/json"))
    {
        return Err(error_response(
            415,
            "unsupported_media_type",
            "POST bodies require Content-Type: application/json",
        ));
    }
    serde_json::from_slice(&request.body).map_err(|_| {
        error_response(
            400,
            "malformed_request",
            "the JSON body does not match the route contract",
        )
    })
}

fn require_empty_body(request: &HttpRequest) -> Result<(), HttpResponse> {
    if request.body.is_empty() {
        Ok(())
    } else {
        Err(error_response(
            400,
            "malformed_request",
            "GET requests have no body",
        ))
    }
}

fn idempotency_key(request: &HttpRequest) -> Result<&str, HttpResponse> {
    let values = request.header_values("idempotency-key").collect::<Vec<_>>();
    let value = match values.as_slice() {
        [] => {
            return Err(error_response(
                400,
                "missing_idempotency_key",
                "Idempotency-Key is required",
            ));
        }
        [value] => *value,
        _ => {
            return Err(error_response(
                400,
                "invalid_idempotency_key",
                "Idempotency-Key must occur exactly once",
            ));
        }
    };
    let value = std::str::from_utf8(value).map_err(|_| {
        error_response(
            400,
            "invalid_idempotency_key",
            "Idempotency-Key must be UTF-8",
        )
    })?;
    if value.is_empty()
        || value.len() > 128
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(error_response(
            400,
            "invalid_idempotency_key",
            "Idempotency-Key must be 1 through 128 non-whitespace UTF-8 bytes",
        ));
    }
    Ok(value)
}

fn fingerprint(
    principal: &str,
    operation: &str,
    targets: &[&str],
    canonical_body: &[u8],
) -> [u8; 32] {
    let body_hash = Sha256::digest(canonical_body);
    let mut hasher = Sha256::new();
    for value in std::iter::once(principal)
        .chain(std::iter::once("v2"))
        .chain(std::iter::once(operation))
        .chain(targets.iter().copied())
    {
        hasher.update(value.len().to_be_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(body_hash);
    hasher.finalize().into()
}

fn canonical_request_body(request: &HttpRequest) -> Result<Vec<u8>, HttpResponse> {
    let value = if request.body.is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(&request.body).map_err(|_| {
            error_response(
                400,
                "malformed_request",
                "the JSON body does not match the route contract",
            )
        })?
    };
    let mut output = Vec::new();
    write_canonical_value(&value, &mut output)?;
    Ok(output)
}

#[cfg(test)]
fn canonical_json(value: &Value) -> Result<Vec<u8>, HttpResponse> {
    let value = serde_json::to_value(value).map_err(|_| {
        error_response(
            500,
            "internal_error",
            "the request fingerprint could not be computed",
        )
    })?;
    let mut output = Vec::new();
    write_canonical_value(&value, &mut output)?;
    Ok(output)
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> Result<(), HttpResponse> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(number) => {
            if number.is_f64() {
                return Err(error_response(
                    500,
                    "internal_error",
                    "floating-point JSON is not admitted in mutation bodies",
                ));
            }
            output.extend_from_slice(number.to_string().as_bytes());
        }
        Value::String(string) => output.extend_from_slice(
            serde_json::to_string(string)
                .expect("JSON strings are serializable")
                .as_bytes(),
        ),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_by(|left, right| utf16_cmp(left, right));
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .expect("JSON object keys are serializable")
                        .as_bytes(),
                );
                output.push(b':');
                write_canonical_value(&values[key], output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn utf16_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn mark_ambiguous(
    gateway: &Gateway,
    principal: &super::HttpGatewayPrincipal,
    key_hash: &[u8; 32],
    fingerprint: &[u8; 32],
) -> Result<(), ()> {
    gateway
        .idempotency
        .lock()
        .expect("HTTP idempotency lock poisoned")
        .mark_ambiguous(&principal.name, key_hash, fingerprint, now_ms())
        .map_err(|_| ())
}

fn native_error(error: &ClientError) -> HttpResponse {
    match error {
        ClientError::DaemonResponse(error) => native_protocol_error(error),
        _ => native_transport_error(error),
    }
}

fn native_protocol_error(error: &platonic_protocol::ProtocolError) -> HttpResponse {
    native_protocol_error_with_message(error, &error.message)
}

pub(super) fn native_protocol_error_with_message(
    error: &platonic_protocol::ProtocolError,
    message: &str,
) -> HttpResponse {
    let status = match error.code.as_str() {
        "malformed_request" => 400,
        "not_found" | "workspace_unregistered" | "workspace_broken" => 404,
        "lagged" => 409,
        "overload" | "daemon_shutting_down" => 503,
        _ => 502,
    };
    let body = serde_json::json!({
        "error": {
            "code": error.code.as_str(),
            "message": message,
        }
    });
    json_response(status, &body).unwrap_or_else(|| {
        error_response(
            502,
            "native_response_too_large",
            "the native response exceeds the gateway limit",
        )
    })
}

fn json_response(status: u16, value: &impl Serialize) -> Option<HttpResponse> {
    let body = serde_json::to_vec(value).ok()?;
    (body.len() <= MAX_RESPONSE_BYTES).then(|| HttpResponse::json(status, body))
}

fn read_json_response(status: u16, value: &impl Serialize) -> Option<HttpResponse> {
    Some(json_response(status, value).unwrap_or_else(|| {
        error_response(
            502,
            "native_response_too_large",
            "the native response exceeds the gateway limit",
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(body: &[u8], headers: &[(&str, &str)]) -> HttpRequest {
        HttpRequest {
            method: "POST".into(),
            target: "/v2/workspaces/ws/profiles/profile/threads/thread/messages".into(),
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).into(), value.as_bytes().to_vec()))
                .collect(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn route_table_admits_only_the_fourteen_v2_targets() {
        for path in [
            "/v2/status",
            "/v2/workspaces",
            "/v2/workspaces/ws/profiles",
            "/v2/workspaces/ws/profiles/p",
            "/v2/workspaces/ws/profiles/p/threads",
            "/v2/workspaces/ws/profiles/p/threads/t",
            "/v2/workspaces/ws/profiles/p/threads/t/authority",
            "/v2/workspaces/ws/profiles/p/threads/t/messages",
            "/v2/workspaces/ws/profiles/p/threads/t/events",
            "/v2/workspaces/ws/profiles/p/threads/t/stop",
            "/v2/workspaces/ws/profiles/p/runs/r/transcript",
            "/v2/workspaces/ws/profiles/p/runs/r/events",
            "/v2/workspaces/ws/profiles/p/runs/r/cancel",
            "/v2/workspaces/ws/profiles/p/runs/r/approvals/c",
        ] {
            assert!(parse_route(path).is_some(), "missing route {path}");
        }
        assert!(parse_route("/v2/workspaces/ws/agents").is_none());
        assert!(parse_route("/v2/workspaces/ws/threads").is_none());
        assert!(parse_route("/v2/status/").is_none());
    }

    #[test]
    fn request_bodies_reject_forged_identity_and_unknown_fields() {
        for body in [
            br#"{"message":"hello","actor":"forged"}"#.as_slice(),
            br#"{"message":"hello","controller_id":"forged"}"#.as_slice(),
            br#"{"message":"hello","future":true}"#.as_slice(),
        ] {
            assert!(
                parse_message(&request(body, &[("Content-Type", "application/json")])).is_err()
            );
        }
        assert!(
            parse_empty(&request(
                br#"{"actor":"forged"}"#,
                &[("Content-Type", "application/json")],
            ))
            .is_err()
        );
        assert!(
            parse_approval(&request(
                br#"{"decision":"grant","actor":"forged"}"#,
                &[("Content-Type", "application/json")],
            ))
            .is_err()
        );
    }

    #[test]
    fn idempotency_keys_are_strict_and_canonical_json_orders_utf16_keys() {
        assert_eq!(
            idempotency_key(&request(b"{}", &[("Idempotency-Key", "key-1")])).unwrap(),
            "key-1"
        );
        for key in ["", "has space", "line\nfeed"] {
            assert!(idempotency_key(&request(b"{}", &[("Idempotency-Key", key)])).is_err());
        }
        assert_eq!(
            canonical_json(&serde_json::json!({"z": 1, "a": 2})).unwrap(),
            br#"{"a":2,"z":1}"#
        );
    }

    #[test]
    fn last_event_id_uses_native_run_offsets_and_thread_epochs() {
        let valid = request(b"", &[("Last-Event-ID", "42")]);
        assert_eq!(event_offset(&valid).unwrap(), Some(42));
        assert_eq!(event_cursor(41).unwrap(), 42);
        assert!(event_offset(&request(b"", &[("Last-Event-ID", "42x")])).is_err());
        assert_eq!(
            thread_event_cursor(&request(b"", &[("Last-Event-ID", "epoch_1:42")])).unwrap(),
            Some(("epoch_1".into(), 42))
        );
        assert!(thread_event_cursor(&valid).is_err());
    }

    #[test]
    fn mutation_and_response_size_limits_are_exact() {
        let message = serde_json::to_vec(&serde_json::json!({
            "message": "x".repeat(MAX_MESSAGE_BYTES + 1)
        }))
        .unwrap();
        assert!(
            parse_message(&request(&message, &[("Content-Type", "application/json")])).is_err()
        );

        let approval = serde_json::to_vec(&serde_json::json!({
            "decision": "deny",
            "reason": "x".repeat(MAX_APPROVAL_REASON_BYTES + 1)
        }))
        .unwrap();
        assert!(
            parse_approval(&request(&approval, &[("Content-Type", "application/json")])).is_err()
        );
        assert!(json_response(200, &"x".repeat(MAX_RESPONSE_BYTES)).is_none());

        assert_eq!(EVENT_LIMIT, 128);
        assert_eq!(THREAD_POLL_MS, 1_000);
        assert_eq!(STOP_TIMEOUT, Duration::from_secs(15));
        assert_eq!(wire::MAX_BODY_BYTES, 768 * 1024);
        assert_eq!(wire::MAX_HEADER_BYTES, 32 * 1024);
        assert_eq!(wire::MAX_HEADER_COUNT, 64);
    }
}
