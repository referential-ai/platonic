---
title: Request lifecycle
description: Trace one request through client transport, server authority, pure run state, effects, persistence, and observation.
sidebar:
  order: 2
---

<p class="section-kicker developer">Developer docs</p>

> This is a reading path through the implementation, not a second run specification. The [repository agent guide](https://github.com/referential-ai/platonic/blob/develop/AGENTS.md#runtime-topology) and [decision map](https://github.com/referential-ai/platonic-workspace/issues/83) own the runtime rules.

## One request, end to end

<ol class="system-flow" aria-label="Request path from the client through server authority, pure kernel validation, and host effects to the durable ledger">
  <li><small aria-hidden="true">01</small><strong>Client</strong></li>
  <li><small aria-hidden="true">02</small><strong>Server authority</strong></li>
  <li><small aria-hidden="true">03</small><strong>Pure kernel</strong></li>
  <li><small aria-hidden="true">04</small><strong>Host effects</strong></li>
  <li><small aria-hidden="true">05</small><strong>Durable ledger</strong></li>
</ol>

1. **A client attaches.** For a one-shot question, Plato Agent ensures the host daemon, opens a local connection, completes `hello`, and calls `run.start` or `message.append`. The client then polls `events.stream`; it does not drive the run itself. Read [`plato-agent/src/run.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-agent/src/run.rs).

2. **The request crosses local transport.** `DaemonClient` serializes a typed `Envelope`, appends one newline, writes it to the Unix stream, and validates the response version, id, method, and kind. `platonic-client` owns these connection mechanics, not server semantics. Read [`client.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-client/src/client.rs) and [`transport.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-client/src/transport.rs).

3. **The server establishes scope and authority.** The host connection rejects workspace-scoped calls until `hello` resolves a registered workspace. Typed dispatch then reaches the run or thread handler, which admits one active run, binds any thread turn, and projects the durable agent, toolset, approval, repository, and confinement authority into the run. Read [`server/host.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/server/host.rs), [`handlers/mod.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/handlers/mod.rs), and [`handlers/runs.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/handlers/runs.rs).

4. **The pure boundary checks ordered state.** `platonic-core::RunState` accepts only the next valid `RecordedEvent` and derives any pending `RunCommand`. It performs no I/O. The server recorder constructs the next sequence number and applies the event to `RunState` before attempting a durable write. Read the [`RunState` API](https://docs.rs/platonic-core/0.3.1/platonic_core/run/struct.RunState.html), [`RunCommand` API](https://docs.rs/platonic-core/0.3.1/platonic_core/run/enum.RunCommand.html), and the server [`ledger/recorder.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/recorder.rs).

5. **The server performs effects.** The shared run loop builds bounded context, records model intent, calls the configured provider, evaluates tool policy, obtains any approval, executes allowed tools, and records results. Provider clients and tool implementations stay in `platonic-server`; the kernel receives their typed facts only through events. Read [`app/run_loop.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/app/run_loop.rs), [`app/tool_exec.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/app/tool_exec.rs), and [`provider/openai_compat/client.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/provider/openai_compat/client.rs).

6. **The parent commits before publishing.** In production the server supervises a confined run child. The child reports record operations; the parent validates and commits them, then publishes the resulting ledger record to observers. Terminal intent is also committed by the parent. Read [`run_child/supervisor.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/run_child/supervisor.rs) and [`ledger/jsonl.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/jsonl.rs).

7. **Clients observe, they do not become writers.** `events.stream` returns a cursor-ordered page. Durable `ledger` entries contain committed `RecordedEvent` values; assistant deltas and other live notifications are transient. When a run is no longer live, the handler reads durable history. The one-shot client advances `next_offset` until it sees a terminal status, then reads the transcript. Read [`handle_events_stream`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/daemon/handlers/runs.rs) and the typed [`StreamEvent`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-protocol/src/lib.rs).

## Where authority changes shape

A workspace identifies the durable home. An immutable agent profile supplies defaults. A thread freezes the authority allowed for an ongoing conversation. Each admitted run executes within that projection and produces one ordered event history. This chain is server-owned; clients select and observe it through protocol methods rather than recreating it. The exact ownership rule is in the [repository guide](https://github.com/referential-ai/platonic/blob/develop/AGENTS.md#repo-boundary), and the complete thread record is proven by the [`thread_authority_persists_all_twelve_fields_and_is_immutable_after_restart` test](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/server_store/queries.rs).

An approval pauses one proposed effect, not the whole authority model. The server writes the request before announcing it, accepts one decision, records the corresponding harness event, and only then permits or denies execution. See the [approval persistence source and test](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/server_store/queries.rs).

## What is not durable yet

Provider response bytes, a running child process, assistant deltas, live event buffers, and active controller ownership are in-flight state. A crash cannot claim they committed merely because a client saw activity. Only acknowledged ledger or SQLite facts survive the boundary; startup converts an incomplete active run into a durable interruption. Continue with [durability and replay](../durability-and-replay/#restart-repair).

## Confinement boundary

On supported Linux hosts, the server applies Landlock to the run child using the thread's writable authority and scratch path. If confinement is unavailable, the durable fact is `none`; a server configured to require confinement rejects the run instead. This is an execution boundary, not a client promise. Read the repository [runtime topology](https://github.com/referential-ai/platonic/blob/develop/AGENTS.md#runtime-topology) and [`confinement.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/confinement.rs). Setup and operational policy belong to the [User operations guide](../../user/operations/).
