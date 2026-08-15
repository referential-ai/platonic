use serde_json::{Map, Value, json};

#[derive(Clone, Copy)]
enum RequestBody {
    Message,
    Approval,
    Empty,
}

#[derive(Clone, Copy)]
enum EventCursor {
    Thread,
    Run,
}

#[derive(Clone, Copy)]
struct Operation {
    method: &'static str,
    path: &'static str,
    operation_id: &'static str,
    summary: &'static str,
    path_parameters: &'static [&'static str],
    body: Option<RequestBody>,
    sse: Option<EventCursor>,
}

const OPERATIONS: &[Operation] = &[
    Operation {
        method: "get",
        path: "/v2/status",
        operation_id: "getStatus",
        summary: "Read authoritative daemon status",
        path_parameters: &[],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces",
        operation_id: "listWorkspaces",
        summary: "List workspaces admitted for the principal",
        path_parameters: &[],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles",
        operation_id: "listProfiles",
        summary: "List profiles admitted for the principal",
        path_parameters: &["workspace_id"],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}",
        operation_id: "getProfile",
        summary: "Read an admitted profile",
        path_parameters: &["workspace_id", "profile_id"],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/threads",
        operation_id: "listThreads",
        summary: "List existing threads",
        path_parameters: &["workspace_id", "profile_id"],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/threads/{thread_id}",
        operation_id: "getThread",
        summary: "Read an existing thread",
        path_parameters: &["workspace_id", "profile_id", "thread_id"],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/threads/{thread_id}/authority",
        operation_id: "getThreadAuthority",
        summary: "Read immutable thread authority",
        path_parameters: &["workspace_id", "profile_id", "thread_id"],
        body: None,
        sse: None,
    },
    Operation {
        method: "post",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/threads/{thread_id}/messages",
        operation_id: "sendThreadMessage",
        summary: "Start or steer a turn on an existing thread",
        path_parameters: &["workspace_id", "profile_id", "thread_id"],
        body: Some(RequestBody::Message),
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/threads/{thread_id}/events",
        operation_id: "streamThreadEvents",
        summary: "Stream live thread events",
        path_parameters: &["workspace_id", "profile_id", "thread_id"],
        body: None,
        sse: Some(EventCursor::Thread),
    },
    Operation {
        method: "post",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/threads/{thread_id}/stop",
        operation_id: "stopThread",
        summary: "Stop an existing thread",
        path_parameters: &["workspace_id", "profile_id", "thread_id"],
        body: Some(RequestBody::Empty),
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/runs/{run_id}/transcript",
        operation_id: "getRunTranscript",
        summary: "Read a durable run transcript",
        path_parameters: &["workspace_id", "profile_id", "run_id"],
        body: None,
        sse: None,
    },
    Operation {
        method: "get",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/runs/{run_id}/events",
        operation_id: "streamRunEvents",
        summary: "Stream run events",
        path_parameters: &["workspace_id", "profile_id", "run_id"],
        body: None,
        sse: Some(EventCursor::Run),
    },
    Operation {
        method: "post",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/runs/{run_id}/cancel",
        operation_id: "cancelRun",
        summary: "Cancel an active run with principal attribution",
        path_parameters: &["workspace_id", "profile_id", "run_id"],
        body: Some(RequestBody::Empty),
        sse: None,
    },
    Operation {
        method: "post",
        path: "/v2/workspaces/{workspace_id}/profiles/{profile_id}/runs/{run_id}/approvals/{tool_call_id}",
        operation_id: "decideApproval",
        summary: "Grant or deny one pending approval",
        path_parameters: &["workspace_id", "profile_id", "run_id", "tool_call_id"],
        body: Some(RequestBody::Approval),
        sse: None,
    },
];

/// Generates the deterministic OpenAPI 3.1 document for the HTTP gateway.
pub fn gateway_openapi() -> String {
    let mut paths = Map::new();
    for spec in OPERATIONS {
        let path = paths
            .entry(spec.path)
            .or_insert_with(|| Value::Object(Map::new()));
        path.as_object_mut()
            .expect("generated OpenAPI path is an object")
            .insert(spec.method.into(), operation(spec));
    }

    let document = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Platonic authenticated HTTP gateway",
            "version": "2.0.0",
            "description": "A translating adapter over the native typed NDJSON v2 protocol. It is plaintext, has no CORS, cookies, browser session, proxy-header identity, or in-process TLS, and is intended to sit behind an operator-owned TLS proxy. Actor and controller identity always come from the authenticated principal."
        },
        "servers": [{"url": "http://127.0.0.1:8787"}],
        "security": [{"bearerAuth": []}],
        "paths": paths,
        "components": components(),
        "x-profile-scoping": "Every path workspace, profile, and target is checked against the authenticated principal's fixed workspace allowlist and optional profile allowlist. Profile scope may narrow its workspace ceiling and never widen it. Crossed or unavailable targets fail closed.",
        "x-idempotency": "Message, stop, cancel, and approval mutations provide at-most-once gateway submission per principal and Idempotency-Key. Completed outcomes replay exactly; crash-ambiguous outcomes require readback and a new key.",
        "x-sse-cursors": "Thread Last-Event-ID is <live_epoch_id>:<next_native_offset>; run Last-Event-ID is the next native offset. Resume is promised only while the referenced native buffer remains available. Epoch change, restart, expiry, lag, or an unknown cursor returns event_cursor_unavailable and requires status or transcript readback.",
        "x-limits": {
            "json_body_bytes": 786432,
            "message_bytes": 262144,
            "approval_reason_bytes": 16384,
            "response_and_replay_bytes": 1048576,
            "native_ndjson_line_bytes": 1048576,
            "active_operations_total": 64,
            "active_requests_per_principal": 8,
            "sse_streams_total": 16,
            "sse_streams_per_principal": 4,
            "mutations_per_second_per_principal": 10,
            "mutation_burst": 20,
            "event_page": 128,
            "native_long_poll_seconds": 1,
            "stop_deadline_seconds": 15,
            "request_header_bytes": 32768,
            "request_header_count": 64,
            "request_header_deadline_seconds": 5,
            "request_body_deadline_seconds": 10,
            "response_write_deadline_seconds": 15,
            "sse_connection_deadline_seconds": 60
        },
        "x-gateway-error-codes": [
            "unauthorized", "forbidden_scope", "malformed_request", "not_found",
            "method_not_allowed", "missing_idempotency_key", "invalid_idempotency_key",
            "invalid_event_cursor", "invalid_message", "invalid_approval_reason",
            "idempotency_in_progress", "idempotency_key_conflict",
            "idempotency_outcome_unknown", "idempotency_unavailable", "request_too_large",
            "request_headers_too_large", "rate_limited", "overloaded",
            "event_cursor_unavailable", "native_version_skew", "native_unavailable",
            "native_response_too_large", "unsupported_media_type", "internal_error"
        ]
    });
    format!(
        "{}\n",
        serde_json::to_string_pretty(&document).expect("generated OpenAPI is serializable")
    )
}

fn operation(spec: &Operation) -> Value {
    let mut parameters = spec
        .path_parameters
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string", "minLength": 1}
            })
        })
        .collect::<Vec<_>>();
    if spec.body.is_some() {
        parameters.push(json!({"$ref": "#/components/parameters/IdempotencyKey"}));
    }
    if let Some(cursor) = spec.sse {
        parameters.push(json!({
            "$ref": match cursor {
                EventCursor::Thread => "#/components/parameters/ThreadLastEventId",
                EventCursor::Run => "#/components/parameters/RunLastEventId",
            }
        }));
    }

    let mut value = json!({
        "operationId": spec.operation_id,
        "summary": spec.summary,
        "parameters": parameters,
        "responses": responses(spec),
    });
    if let Some(body) = spec.body {
        value.as_object_mut().unwrap().insert(
            "requestBody".into(),
            json!({
                "required": !matches!(body, RequestBody::Empty),
                "content": {
                    "application/json": {
                        "schema": {"$ref": request_schema(body)}
                    }
                }
            }),
        );
    }
    value
}

fn responses(spec: &Operation) -> Value {
    let success = if let Some(cursor) = spec.sse {
        json!({
            "description": "SSE events with stable cursor IDs and JSON native event data",
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                    "example": match cursor {
                        EventCursor::Thread => "id: epoch_1:8\\ndata: {\"live_epoch_id\":\"epoch_1\",\"offset\":7,\"event\":{\"kind\":\"test\"}}\\n\\n",
                        EventCursor::Run => "id: 8\\ndata: {\"offset\":7,\"event\":{\"kind\":\"test\"}}\\n\\n",
                    }
                }
            }
        })
    } else {
        let mut success = json!({
            "description": "Native typed outcome",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/NativeOutcome"}
                }
            }
        });
        if spec.body.is_some() {
            success.as_object_mut().unwrap().insert(
                "headers".into(),
                json!({
                    "Idempotency-Replayed": {
                        "description": "true only when this exact stored outcome was replayed without native dispatch",
                        "schema": {"type": "string", "const": "true"}
                    }
                }),
            );
        }
        success
    };
    json!({
        "200": success,
        "400": error_response("Malformed input or missing/invalid idempotency key"),
        "401": error_response("Bearer authentication failed"),
        "403": error_response("The principal is outside the workspace scope"),
        "404": error_response("The route or scoped target is unavailable"),
        "405": error_response("The method is not admitted for this route"),
        "409": {
            "description": "Idempotency conflict/ambiguity or unavailable event cursor",
            "headers": {
                "Retry-After": {
                    "description": "One second for idempotency_in_progress",
                    "schema": {"type": "integer", "const": 1}
                }
            },
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
        },
        "413": error_response("Request body or headers exceed a fixed ceiling"),
        "415": error_response("A JSON mutation has an unsupported content type"),
        "429": error_response("The principal request or mutation-rate ceiling is reached"),
        "500": error_response("The gateway could not serialize or persist a deterministic outcome"),
        "502": error_response("The native daemon returned an invalid or oversized outcome"),
        "503": error_response("The gateway is overloaded, version-skewed, or native state is unavailable")
    })
}

fn error_response(description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
            }
        }
    })
}

fn request_schema(body: RequestBody) -> &'static str {
    match body {
        RequestBody::Message => "#/components/schemas/MessageRequest",
        RequestBody::Approval => "#/components/schemas/ApprovalRequest",
        RequestBody::Empty => "#/components/schemas/EmptyRequest",
    }
}

fn components() -> Value {
    json!({
        "securitySchemes": {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "32 random bytes encoded as base64url without padding",
                "description": "The gateway hashes bearer material with SHA-256 and compares configured canonical-home hashes in constant time. Proxy identity headers are ignored."
            }
        },
        "parameters": {
            "IdempotencyKey": {
                "name": "Idempotency-Key",
                "in": "header",
                "required": true,
                "description": "1-128 UTF-8 bytes with no whitespace or control characters. The durable namespace is the stable principal, API major, operation, exact path IDs, and RFC 8785 request-body hash.",
                "schema": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^\\S+$"},
                "example": "01J5Z9Y7Z8Y4VQG9G6H3M2K1PT"
            },
            "ThreadLastEventId": {
                "name": "Last-Event-ID",
                "in": "header",
                "required": false,
                "description": "Exact live epoch and next native event offset emitted by a prior thread SSE event.",
                "schema": {"type": "string", "pattern": "^[^:]+:[0-9]+$"},
                "example": "epoch_1:8"
            },
            "RunLastEventId": {
                "name": "Last-Event-ID",
                "in": "header",
                "required": false,
                "description": "Exact next native event offset emitted by a prior run SSE event in the same available daemon-lifetime cursor.",
                "schema": {"type": "integer", "format": "uint64", "minimum": 0},
                "example": 8
            }
        },
        "schemas": {
            "MessageRequest": {
                "type": "object",
                "additionalProperties": false,
                "required": ["message"],
                "properties": {
                    "message": {"type": "string", "minLength": 1, "maxLength": 262144},
                    "turn_id": {"type": ["string", "null"]},
                    "prior_interrupted_run_id": {"type": ["string", "null"]}
                },
                "example": {"message": "Continue with the bounded change", "turn_id": "turn_42"}
            },
            "ApprovalRequest": {
                "type": "object",
                "additionalProperties": false,
                "required": ["decision"],
                "properties": {
                    "decision": {"type": "string", "enum": ["grant", "deny"]},
                    "reason": {"type": ["string", "null"], "maxLength": 16384}
                },
                "examples": [
                    {"decision": "grant"},
                    {"decision": "deny", "reason": "Outside the approved scope"}
                ]
            },
            "EmptyRequest": {
                "type": "object",
                "additionalProperties": false,
                "maxProperties": 0,
                "example": {}
            },
            "NativeOutcome": {
                "type": "object",
                "description": "The exact typed native v2 result for the mapped operation.",
                "additionalProperties": true
            },
            "Error": {
                "type": "object",
                "additionalProperties": false,
                "required": ["error"],
                "properties": {
                    "error": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["code", "message"],
                        "properties": {
                            "code": {"type": "string", "description": "Stable gateway code or preserved stable native error code"},
                            "message": {"type": "string"}
                        }
                    }
                },
                "examples": [
                    {"error": {"code": "forbidden_scope", "message": "the requested workspace is not admitted"}},
                    {"error": {"code": "idempotency_outcome_unknown", "message": "inspect state and use a new key"}},
                    {"error": {"code": "event_cursor_unavailable", "message": "inspect status or transcript"}}
                ]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_openapi_is_valid_v31_complete_and_in_sync() {
        let generated = gateway_openapi();
        assert_eq!(gateway_openapi(), generated);
        let parsed: Value = serde_json::from_str(&generated).unwrap();
        assert_eq!(parsed["openapi"], "3.1.0");
        assert_eq!(parsed["info"]["version"], "2.0.0");
        assert_eq!(parsed["paths"].as_object().unwrap().len(), OPERATIONS.len());
        for spec in OPERATIONS {
            let operation = &parsed["paths"][spec.path][spec.method];
            assert!(!operation.is_null());
            let parameters = operation["parameters"].as_array().unwrap();
            if spec.body.is_some() {
                assert!(parameters.iter().any(|parameter| {
                    parameter["$ref"] == "#/components/parameters/IdempotencyKey"
                }));
            }
            if let Some(cursor) = spec.sse {
                let reference = match cursor {
                    EventCursor::Thread => "#/components/parameters/ThreadLastEventId",
                    EventCursor::Run => "#/components/parameters/RunLastEventId",
                };
                assert!(
                    parameters
                        .iter()
                        .any(|parameter| parameter["$ref"] == reference)
                );
            }
        }
        assert_eq!(parsed["security"][0]["bearerAuth"], json!([]));
        assert!(parsed["x-profile-scoping"].is_string());
        assert!(parsed["x-limits"].is_object());
        assert!(
            parsed["x-gateway-error-codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == "event_cursor_unavailable")
        );

        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../openapi/gateway-v2.yaml");
        if std::env::var_os("PLATONIC_UPDATE_OPENAPI").is_some() {
            std::fs::write(&path, &generated).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path).unwrap(), generated);
    }
}
