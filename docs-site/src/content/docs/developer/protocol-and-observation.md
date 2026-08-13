---
title: Protocol and observation
description: Understand protocol v1 methods, advertised capabilities, thread control, observers, gateways, and transport gates.
sidebar:
  order: 3
---

<p class="section-kicker developer">Developer docs</p>

> The wire contract lives in [`platonic-protocol`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs) and its exact-byte tests. This page groups that contract for navigation; it is not an API reference.

## Local typed handshake

Protocol v1 is a closed, versioned, newline-delimited JSON protocol over local IPC. Each request and response is a typed `Envelope`; unknown methods, fields, and incompatible versions fail at the envelope boundary. The source owns serialization and validation but performs no I/O or policy. See the crate [`README`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/README.md), [`Envelope`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs), and [`every_current_method_keeps_exact_v1_request_and_response_bytes`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs).

On a host connection, workspace and agent registry calls plus `daemon.shutdown_if_idle` can be dispatched without selecting a workspace. Other calls must begin with `hello`. The server resolves the registered workspace and returns its stable identity, ledger path, daemon scope, build identity, and advertised capabilities. The server-side gate is [`handle_host_line`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/server/host.rs); the transport call is [`DaemonClient::hello`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-client/src/client.rs).

Capabilities report what this peer implements. They do not grant workspace, thread, tool, or approval authority; the server still enforces those checks in its handlers.

## Callable methods and capabilities

At this source head, `ProtocolMethod` contains **27 callable methods**:

- Handshake: `hello`
- Run and event flow: `run.start`, `message.append`, `issue-prep.start`, `events.stream`, `run.cancel`
- Voice facts: `voice.events.commit`, `voice.events.read`
- Approval and session readback: `approval.decide`, `sessions.list`, `transcript.read`, `session.approval_profile.set`
- Host control: `daemon.status`, `daemon.shutdown_if_idle`
- Threads: `thread.spawn`, `thread.list`, `thread.status`, `thread.authority`, `thread.send`, `thread.events`, `thread.stop`
- Workspace registry: `workspace.create`, `workspace.list`, `workspace.status`
- Agent profiles: `agent.create`, `agent.list`, `agent.status`

The `hello` result advertises **29 capabilities**. Twenty-seven correspond to callable methods. The two additional flags, `transcript.read.typed` and `transcript.read.pending_approval`, describe additive fields returned by the callable `transcript.read`; they are not methods. The exhaustive inventory is the typed [`Capability`, `CAPABILITIES`, and `ProtocolMethod` source](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs), locked by [`capability_names_and_error_codes_keep_exact_v1_literals`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs).

Internal modules such as `handlers/runs.rs` or `runtime/thread.rs` are implementations behind those methods, not extra protocol surface. Add or change wire behavior in the typed protocol first, then route it through the server handler that owns the semantics.

## One controller, many observers

`thread.send` establishes one controller for an active turn. That controller may queue a bounded steer only with the matching turn id. A different controller receives `controller_owned`; after the turn becomes idle, another controller may start the next turn. The rule and rejection shapes live in [`runtime/thread.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/runtime/thread.rs) and are proven end to end by [`thread_send_and_three_observers_are_semantically_conformant_on_host_daemon`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-agent/tests/semantic_conformance.rs).

`thread.events` is independently observable by multiple clients. Each observer advances its own offset through the same ordered, bounded live buffer. Omitting an offset tails from the current position; requesting an evicted offset returns the typed `lagged` error. Durable transcript readback, not a private observer cache, is the recovery path after lag or reconnect. See [`LiveThread::events`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/runtime/thread.rs) and [`TranscriptReadResult`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs).

## Gateways are peer clients

A gateway is a server-owned connector module, but it reaches run authority through the same client and protocol boundary as any other peer. The Discord bridge connects, completes `hello`, checks required capabilities, calls `thread.status`, `thread.send`, `thread.events`, `approval.decide`, and `transcript.read`, and falls back to durable readback after lag or reconnect. It does not acquire sessions, policy, approvals, fallback, or run semantics. Read [`gateway/discord/daemon_bridge.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/gateway/discord/daemon_bridge.rs) and the [gateway boundary](https://github.com/referential-ai/platonic/blob/develop/README.md#discord-gateway).

## Local is not remote

The daemon endpoint is a user-owned Unix socket, with private parent directories and a `0600` socket. It is not a network listener. The implementation is in [`server/socket.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/server/socket.rs), while the accepted no-network-listener boundary is [decision P017](https://github.com/referential-ai/platonic-workspace/issues/83).

An outbound gateway connection does not turn the daemon protocol into a remote API. A network transport, JSON-RPC adapter, ACP adapter, authentication model, or remote authority projection needs its own accepted design and gate; it must not be smuggled into protocol v1. That boundary is locked by the accepted [typed protocol decision](https://github.com/referential-ai/platonic/issues/438). Operational socket and gateway setup belongs to [#548](https://github.com/referential-ai/platonic/issues/548).
