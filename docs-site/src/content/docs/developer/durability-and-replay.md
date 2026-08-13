---
title: Durability and replay
description: Locate durable facts, understand acknowledgement and restart repair, and know what offline replay can reconstruct.
sidebar:
  order: 4
---

<p class="section-kicker developer">Developer docs</p>

> The storage contract is owned by [decision P032](https://github.com/referential-ai/platonic-workspace/issues/83), the server ledger source, and its crash tests. This page explains their boundaries without defining a second schema.

## Two durable tiers

The server is the only ledger writer. New run events are append-only JSONL; SQLite holds indexed and mutable state. The current layout is implemented by [`ledger/jsonl.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/jsonl.rs) and [`ledger/sqlite.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/sqlite.rs).

| Store | Durable responsibility |
| --- | --- |
| `server.db` | Registered workspaces, immutable agent profiles, immutable thread authority, pending tool approvals and their one-time decisions, branch claims, and other host-wide state. See [`server_store`](https://github.com/referential-ai/platonic/tree/develop/crates/platonic-server/src/server_store). |
| `workspaces/<id>/ledger.db` | Workspace session and run indexes, mutable outcomes, and legacy event and voice compatibility. See [`SqliteLedger`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/sqlite.rs). |
| `workspaces/<id>/runs/<run-id>.jsonl` | The ordered, versioned event history and companion voice records for each new run. See [`run_jsonl_path`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/jsonl.rs). |

The split is deliberate: a per-run log remains inspectable and append-only, while relational state supports queries and atomic mutable facts. Legacy SQLite event rows remain readable; they are not the write path for new run histories.

## The acknowledgement boundary

For JSONL, the server first asks `RunState` to validate the next contiguous event. It serializes the versioned record with a trailing newline, writes and flushes it, calls `sync_data`, and only then returns the record as acknowledged. A write failure poisons that recorder so later work cannot pretend the failed append succeeded. The exact boundary and its SIGKILL proof are in [`JsonlEventRecorder::record_event`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/jsonl.rs) and [`sigkill_torn_tail_recovery_retains_every_acknowledged_event`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/jsonl.rs).

Bytes after the last newline are an uncommitted tail. Readers ignore them; reopening for append truncates them, syncs the repaired file, and reconstructs `RunState` from committed records. A client-visible assistant delta, provider byte, or process state is therefore not evidence of a committed event.

Both `server.db` and `ledger.db` use SQLite WAL mode with `synchronous=FULL`; state changes become authoritative at transaction commit. For a JSONL-backed run, the terminal event is synced before the SQLite session outcome is updated. Startup repair handles the crash window between those two commits. See the [SQLite configuration and terminal transaction](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/sqlite.rs) and [server-store configuration](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/server_store/queries.rs).

## Restart repair

When the restarted host first loads a workspace runtime, that workspace ledger is checked for session runs still marked `running`. The repair path in [`recover_running_session_runs`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/sqlite.rs) applies these rules:

1. Open a JSONL run at its last committed newline and replay its records through `RunState`.
2. If the JSONL already contains a terminal event, reconcile the SQLite outcome to that event.
3. If the history is nonterminal, append a durable `RunFailed` event and mark the SQLite run `interrupted`.
4. For a legacy SQLite event stream, append the interruption and update its outcome in one transaction.

The server does not resume a killed child, provider request, active controller, or partially observed delta stream. It records the loss as interruption. The exact terminal crash windows are exercised by [`sqlite_recovery_reconciles_existing_terminal_events_after_reopen`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/ledger/sqlite.rs) and neighboring recovery tests.

Pending approvals have a different lifetime. The server writes the immutable request to `server.db` before announcing it. After restart it remains readable and can be decided exactly once, even though its former active run is interrupted. See [`persist_tool_call_approval`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/server_store/queries.rs) and [`pending_approval_survives_restart_and_is_decided_exactly_once`](https://github.com/referential-ai/platonic/blob/develop/crates/platonic-server/src/server_store/queries.rs).

## Replay is readback, not re-execution

`plato replay` is deliberately offline and does not require the server binary. The client distribution opens a selected per-run JSONL log, or the legacy SQLite event rows when no JSONL exists, validates versions, run ids, ordering, and voice keys, then reconstructs typed readback from recorded events. Read [`plato-agent/src/offline.rs`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-agent/src/offline.rs) and the pure [`RunReadback` API](https://docs.rs/platonic-core/0.3.1/platonic_core/projection/struct.RunReadback.html).

Replay does not call a provider, execute a tool, ask for approval, continue an interrupted run, or recreate transient assistant deltas. It can prove only what the committed event history contains. The boundary is exercised by [`jsonl_reader_ignores_a_valid_unterminated_tail`](https://github.com/referential-ai/platonic/blob/develop/crates/plato-agent/src/offline.rs).
