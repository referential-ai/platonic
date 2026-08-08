# Plato Agent Architecture

Status: **PARTLY SUPERSEDED, 2026-08-07.** This document recorded the topology
that bootstrapped `referential-ai/plato-agent`, before the platform decisions of
2026-08-06/07.

> ### Read this before using the document below
>
> The [platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
> supersedes parts of this document. Where they disagree, the map wins.
>
> - **Platonic is the server and the product; Plato Agent is the client
>   distribution built on it** (P016, P018). The server command is `platonic`;
>   the client command is `plato`.
> - **The crate split has happened** (P015, P024). The Boundary Ladder below
>   describes the pre-extraction rule that a crate needs a trigger; the
>   architecture is now `platonic-core`, `platonic-protocol`, `platonic-client`,
>   `platonic-server`, plus the `plato-*` distribution.
> - **Windows is withdrawn** (P026). Every Windows named-pipe and `LocalAppData`
>   statement below is historical.
> - **A workspace is a first-class registered entity with a server-minted id**
>   (P006, P021). Nothing derives from a mutable path.
> - **The Sequence section is history**, not a plan.
>
> Sections not contradicted above still hold: the single-writer rule, the
> run-child boundary, the connector rule, and ledger versioning.

## Runtime Topology

- One repo: `referential-ai/plato-agent`.
- End-state binaries:
  - `plato`: daemon-backed one-shot execution and offline replay.
  - `platonic`: server runtime and protocol-backed operator commands.
  - `plato-tui`: terminal client for rendering and keyboard UX only.
- Bootstrap rule: create only `plato.rs` at repo creation. No stub binaries.
- Permanent invariant: `plato` one-shot auto-ensures the host server; `plato replay` remains offline and works without the server binary.
- Host-loop rule: `platonic serve` owns the one run-driving implementation. Clients do not fork model/tool/policy event choreography.
- Fallback rule: provider fallback is per-run ledger evidence. The process that computes it is mechanics; unrecorded fallback is forbidden.
- Server noun: `platonic serve` is the persistent runtime. Gateways are ingress adapters and never own agent semantics.
- TUI decision: `plato-tui` is a separate binary in this crate once it exists.
- Server ownership: `platonic serve` owns the host endpoint, workspace event databases, and process lock. Unix uses a private UDS plus XDG runtime/state paths; Windows uses a current-user named pipe plus `LocalAppData` runtime/state paths.
- Single-writer invariant: one live server owns a workspace store. A run child never opens the ledger or receives its path; it streams typed record operations to `platonic serve`, which remains the sole SQLite writer.
- Run-child boundary: every run executes in its own supervised child while the server remains authoritative. The parent supplies an explicit deadline and cancellation token, supervises the complete descendant tree, applies bounded grace and kill phases, drains child output with a bound, and asserts zero residual processes after every terminal path. One-shot clients use this same server-owned implementation.
- Daemon API sketch: start run, append message, stream events, approve/deny, cancel, list sessions, read transcript. `run.start` and `message.append` default to async `wait: false`; explicit `wait: true` blocks until terminal result.
- Live assistant text deltas are transient daemon/app events; final `model_responded` ledger messages remain the replay source of truth.
- Connector rule: connectors and gateways never own sessions, policy, approvals, provider fallback, or run semantics. Process placement is host mechanics; the semantic boundary is binding.

## Boundary Ladder

Issue #3 and its [boundary addendum](https://github.com/referential-ai/plato-agent/issues/3#issuecomment-4883961697) are the evidence source for this ladder.
Default to a clear module with a narrow surface.
Promote to a Cargo feature only when a real build wants exclusion.
Promote to a crate only on a trigger: second consumer, independent process/deployable, or compile/dependency isolation.
`sqlite` as a feature is a later discussion candidate only; the current SQLite path stays concrete.
The Discord runtime is a gateway module inside `platonic-server`; the
`platonic gateway discord` command supplies its thin product entry point. Other
connectors remain unadmitted.
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

The server writes a `plato-agent` ledger envelope around
`platonic-core::RecordedEvent`:

```json
{ "v": 1, "record": { "seq": 0, "occurred_at_ms": 0, "event": { "event": "run_started" } } }
```

Bare `RecordedEvent` lines are not persisted by this app shell.
