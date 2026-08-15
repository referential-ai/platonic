---
title: Crates and change routing
description: Find the crate and selected boundary module that owns a developer change without crossing prohibited dependencies.
sidebar:
  order: 5
---

<p class="section-kicker developer">Developer docs</p>

> Crate ownership is normative in the [repository agent guide](https://github.com/referential-ai/platonic/blob/develop/AGENTS.md#repo-boundary) and [decision P030](https://github.com/referential-ai/platonic-workspace/issues/83). This map is a route into those sources, not permission to split or move ownership.

## Active crates

| Crate | Responsibility |
| --- | --- |
| [`platonic-core`](https://github.com/referential-ai/platonic/tree/develop/crates/platonic-core) | Pure typed harness primitives: identifiers, events, run state, effect requests, and deterministic readback. Generated public API detail belongs on [docs.rs](https://docs.rs/platonic-core/0.3.1/platonic_core/). |
| [`platonic-protocol`](https://github.com/referential-ai/platonic/tree/develop/crates/platonic-protocol) | Closed protocol v2 types, serialization, and validation. It has no transport or server policy. |
| [`platonic-client`](https://github.com/referential-ai/platonic/tree/develop/crates/platonic-client) | Synchronous local connection, framing, deadlines, and typed request helpers. It does not own daemon run semantics. |
| [`platonic-server`](https://github.com/referential-ai/platonic/tree/develop/crates/platonic-server) | Technical implementation crate for the Platonic server: registry, thread and run authority, policy, approvals, providers, tools, ledger, daemon, and gateways. |
| [`platonic`](https://github.com/referential-ai/platonic/tree/develop/crates/platonic) | Thin crate for the Platonic product command, `platonic`, over `platonic-server`. |
| [`plato-agent`](https://github.com/referential-ai/platonic/tree/develop/crates/plato-agent) | Plato Agent client distribution: `plato`, `plato-tui`, client orchestration, and offline replay. It never links `platonic-server`. |
| [`plato-tui`](https://github.com/referential-ai/platonic/tree/develop/crates/plato-tui) | Client-side terminal UI components and protocol presentation. |
| [`plato-audio`](https://github.com/referential-ai/platonic/tree/develop/crates/plato-audio) | Client-side voice capture, playback, and audio support. |

Only `platonic-core` is the published implementation crate with a generated docs.rs surface at this head. The other workspace crates set `publish = false`; use their repository source and tests rather than treating reservation packages as API documentation. The publication rule is [decision P029](https://github.com/referential-ai/platonic-workspace/issues/83), reflected in the crate [`Cargo.toml` files](https://github.com/referential-ai/platonic/tree/develop/crates).

## Dependency direction

The useful compile-time spine is:

```text
plato-agent -> platonic-client -> platonic-protocol -> platonic-core

platonic -> platonic-server
                 -> platonic-client -> platonic-protocol -> platonic-core
```

The second client edge lets server-owned gateways behave as protocol peers; it does not transfer semantics into `platonic-client`. Compile-time direction and semantic ownership are different questions.

The prohibited directions are explicit:

- `platonic-core` cannot acquire I/O, provider, tool implementation, storage, daemon, connector, or UI code.
- `platonic-protocol` cannot acquire transport, policy, or server behavior.
- `plato-*` crates may depend on client, protocol, core, and client-side leaves, but never on `platonic-server` or the `platonic` command crate.
- `platonic-server` cannot depend on `plato-*`; gateways remain modules inside the server crate and use the client boundary.
- `platonic` stays a thin command over `platonic-server`.
- Provider, tool, store, and replay code stay in their current owning crates until a second consumer and an accepted design justify a split.

These directions are binding in [AGENTS.md](https://github.com/referential-ai/platonic/blob/develop/AGENTS.md) and partially compiler-checked by [`workspace_architecture_invariants_hold`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic/tests/architecture_invariants.rs).

## Selected boundary modules

Use this list to locate a crossing, then read callers and tests around it. It intentionally stops before becoming a module catalog.

| Change | Start here |
| --- | --- |
| One-shot attachment and ordered consumption | [`plato-agent/src/run.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-agent/src/run.rs) |
| Local request mechanics | [`platonic-client/src/client.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-client/src/client.rs) and [`transport.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-client/src/transport.rs) |
| Wire types and capability inventory | [`platonic-protocol/src/lib.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs) |
| Host handshake and typed dispatch | [`daemon/server/host.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/server/host.rs) and [`daemon/handlers/mod.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/handlers/mod.rs) |
| Thread authority, controller, and observer behavior | [`thread_authority.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/thread_authority.rs) and [`daemon/runtime/thread.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/runtime/thread.rs) |
| Shared run driving and supervised effects | [`app/run_loop.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/app/run_loop.rs) and [`daemon/run_child/supervisor.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/run_child/supervisor.rs) |
| Pure event validation and effect requests | [`platonic-core/src/run.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-core/src/run.rs) and the [`run` API](https://docs.rs/platonic-core/0.3.1/platonic_core/run/) |
| Provider and tool host effects | [`provider/openai_compat/client.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/provider/openai_compat/client.rs) and [`app/tool_exec.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/app/tool_exec.rs) |
| Ledger acknowledgement and recovery | [`ledger/recorder.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/recorder.rs), [`jsonl.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/jsonl.rs), and [`sqlite.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/sqlite.rs) |
| Offline replay | [`plato-agent/src/offline.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-agent/src/offline.rs) |
| Gateway as protocol peer | [`gateway/discord/daemon_bridge.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/gateway/discord/daemon_bridge.rs) |

For item-level types and methods in the pure kernel, continue to [docs.rs](https://docs.rs/platonic-core/0.3.1/platonic_core/). For runtime behavior, follow the linked server source and executable tests instead of copying an internal API into this site.
