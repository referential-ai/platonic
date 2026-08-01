# Plato Agent Architecture

This document copies the topology decision from the Platonic workspace issue that bootstrapped `referential-ai/plato-agent`.

## Runtime Topology

- One repo: `referential-ai/plato-agent`.
- End-state binaries:
  - `plato`: one-shot execution and offline replay.
  - `plato-agentd`: runtime daemon for sessions, event store, providers, tool execution, approvals, and connector-facing API.
  - `plato-tui`: terminal client for rendering and keyboard UX only.
- Bootstrap rule: create only `plato.rs` at repo creation. No stub binaries.
- Permanent invariant: `plato` one-shot and `plato replay` work without a daemon.
- Host-loop rule: `plato` and `plato-agentd` share one run-driving implementation. Binaries do not fork model/tool/policy event choreography.
- Fallback rule: provider fallback is per-run ledger evidence. The process that computes it is mechanics; unrecorded fallback is forbidden.
- Daemon noun: `plato-agentd` is the persistent runtime. Gateways are future ingress adapters and never own agent semantics.
- TUI decision: `plato-tui` is a separate binary in this crate once it exists.
- Daemon ownership: `plato-agentd` owns the local endpoint, event database, and process lock. Unix uses a private UDS plus XDG runtime/state paths; Windows uses a current-user named pipe plus `LocalAppData` runtime/state paths. Durable paths are keyed by `workspace-id` = sanitized root basename + first 16 SHA-256 hex chars of the canonical root path; the Windows pipe name uses the full bounded workspace id.
- Single-writer invariant: one live writer owns a workspace store. Before daemon and CLI coexist on SQLite, decide whether CLI writes directly, delegates to the daemon, or refuses while the daemon is active.
- Daemon API sketch: start run, append message, stream events, approve/deny, cancel, list sessions, read transcript. `run.start` and `message.append` default to async `wait: false`; explicit `wait: true` blocks until terminal result.
- Live assistant text deltas are transient daemon/app events; final `model_responded` ledger messages remain the replay source of truth.
- Connector rule: connectors and gateways never own sessions, policy, approvals, provider fallback, or run semantics. Process placement is host mechanics; the semantic boundary is binding.

## Boundary Ladder

Issue #3 and its [boundary addendum](https://github.com/referential-ai/plato-agent/issues/3#issuecomment-4883961697) are the evidence source for this ladder.
Default to a clear module with a narrow surface.
Promote to a Cargo feature only when a real build wants exclusion.
Promote to a crate only on a trigger: second consumer, independent process/deployable, or compile/dependency isolation.
`sqlite` as a feature is a later discussion candidate only; the current SQLite path stays concrete.
The Discord runtime is the `plato-gateway-discord` leaf crate; root retains
configuration admission, startup composition, and the installed compatibility
binary. Other connectors remain unadmitted.
The store becomes a crate only with out-of-crate consumers; scheduler, cron, and memory are daemon-era modules/features if they ever become real.
Crate-per-function upfront is rejected.

## Sequence

1. Build one-shot JSONL CLI.
2. Use it for real before spending on daemon/TUI.
3. Add SQLite as a concrete second persistence path inside the CLI.
4. Introduce a store trait only when the daemon creates a second caller or consumer that needs the abstraction.
5. Build daemon.
6. Build TUI.
7. Build connectors.

## Ledger Versioning

The CLI writes a `plato-agent` JSONL envelope around `platonic-core::RecordedEvent`:

```json
{ "v": 1, "record": { "seq": 0, "occurred_at_ms": 0, "event": { "event": "run_started" } } }
```

Bare `RecordedEvent` lines are not persisted by this app shell.
