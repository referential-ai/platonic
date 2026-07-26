# Platonic Core Architecture

Platonic Core is the harness kernel for bounded agent execution.

## Kernel stance

Sans-IO: core is the loop, the ledger, and the boundaries. Nothing in core performs IO.

- The run loop is a pure state machine: feed it an event, get the next state plus typed commands (`request_model`, `execute_tool`, `await_approval`, ...).
- Hosts (CLI, daemon, gateway app) perform IO and feed results back as events.
- Interruption and approval are states, not callbacks.
- Replay is re-feeding the recorded event stream: zero model calls.
- No silent fallback is structural: every model call crosses the typed command/event boundary and is fully recorded — recording is never optional. Provider-selection policy activates at the second provider.
- No async runtime coupling. Trait seams appear only at the second concrete implementation.
- Decision boundary: a host choice that changes security, approval posture, context contents, provider/tool choice, replay, or audit semantics becomes a typed, recorded policy/event/command in core. Mechanical detail (retry timing, pooling, supervision, transport) stays in the host.

## Concurrency model

Core is single-threaded and synchronous. Concurrency is a host concern.

- Each run is an independent state machine value: no shared state, nothing to lock.
- Hosts (the `plato-agent` daemon) schedule many runs: one lightweight task per run, IO awaited host-side, results fed back as events. Multi-agent means many runs, not a threaded kernel.
- Event ordering matters per run only; one task per run gives it for free.
- Independent machines parallelize across runs automatically if CPU-bound work ever appears; core is untouched either way.
- Cross-run coordination (provider rate limits, global budgets/WIP caps, approval queues, append idempotency) is host/store concern — never locks in core. When such a choice changes a run's outcome, it enters that run's ledger as policy/event input.

## Clean room boundary

Core owns semantics; apps own mechanics. `platonic-core` (plus typed adapter crates, once forced into existence) is the clean room: sans-IO contracts, exhaustive proof. `plato-agent` and other app shells are pragmatic application code.

- Apps may be pragmatic about how, never about what: anything semantic crosses the boundary as typed commands, events, and policy — or it did not happen.
- Dependencies point inward only; the clean room never depends on app code.
- Promotion is by proof, not appetite: app code moves inward only when the decision-boundary rule catches it carrying semantics. Core does not grow to feel bigger.
- Proof gradient: clean room requires exact event/replay assertions; app zone gets smoke tests and manual verification.

## Boundary

In scope:

- run and turn identifiers
- typed context fragments and context packs
- model-facing messages
- tool-call proposals
- effect classes
- policy decisions
- structured tool results
- durable harness events

Out of scope for core:

- Discord/Slack/SMS gateways
- cron scheduler
- desktop UI
- model-provider implementations
- memory backends
- skill packages
- MCP server/client implementations
- filesystem/process/browser tools

Those belong in outer crates or apps.

## Module layout

The core crate is split by harness boundary, not by implementation convenience:

```text
src/lib.rs       public module surface and re-exports only
src/error.rs     shared error types
src/ids.rs       string-backed identifier newtypes
src/message.rs   model-facing message primitives
src/context.rs   lane-labeled context primitives with budget validation
src/policy.rs    effect classes and policy decisions
src/tool.rs      tool-call and tool-result boundary types
src/event.rs     durable harness event ledger
src/run.rs        pure run/turn state machine
src/projection.rs pure readback projection over recorded events
```

Keep modules narrow. If a module starts pulling in IO, provider clients, runtime
executors, platform adapters, or storage dependencies, the feature probably
belongs in an outer crate.

## Core concepts

### Agent unit

A bounded agent unit (`AgentId`): not a personality blob, but an execution identity with policy, tools, and state scope. Deep-vocabulary name: henad — docs only, never identifiers.

### Run

A durable execution instance. A run is event-log-first. Transcripts, metrics, replay, and audit views are derived from events. The run loop lives in `run` as a pure state machine; hosts drive it. `RunCommand` (desired effects) is a separate type from `HarnessEvent` (recorded facts); replay applies events only, so it can never re-emit IO.

`RunReadback` lives in `projection` as a small pure view over a recorded ledger. It validates the ledger by replaying each `RecordedEvent` through `RunState`, then projects chronological context fragments, model messages, tool calls, tool results, policy denials, approval grants and denials, and tool failures. It does not store events, render output, execute tools, call providers, or read clocks.

### ContextPack

A bounded context bundle of lane-labeled fragments:

- `system_contract`
- `current_task`
- `tool_schemas`
- `recent_turns`
- `retrieved_context`
- `artifact_summary`
- `policy`

The assembler must validate budget before a model call. No hidden prompt growth. One total token budget per pack today; per-lane budgets arrive with the context assembler.

### ToolCall

The model authors only a `ToolProposal`: registered tool name plus JSON input. The host/registry validates the input and attaches the `EffectClass` to form the `ToolCall` with a stable call id. Effect is never model-declared. `ToolCall.input` is the proposed input verbatim after validation; normalization or default expansion happens during execution, not in the recorded call. The registry itself lives outside this crate; core tracks the boundary.

### EffectClass

Current default posture:

- `read_only` → allow
- `workspace_write` / `network` → require approval unless policy grants it
- `external_side_effect` / `secret_access` → deny by default

### ToolResult

A structured result with:

- short summary
- JSON data
- artifact refs
- model/user/both visibility

Large raw output should be stored as an artifact and summarized for the model.

### HarnessEvent

The durable ledger. Initial events include:

- `run_started`
- `context_compacted`
- `context_built`
- `model_requested`
- `model_responded`
- `tool_call_proposed`
- `policy_evaluated`
- `approval_granted`
- `approval_denied`
- `tool_started`
- `tool_finished`
- `tool_failed`
- `run_finished`
- `run_failed`

Events are recorded wrapped in `RecordedEvent { seq, occurred_at_ms, event }`; hosts supply both fields — core never reads clocks. `seq` is per-run and contiguous from 0; the run machine rejects gaps and regressions. Store append is idempotent on `(run_id, seq)`; ordering across runs is a store concern. Correlation: tool events carry `call_id`; model request/response pairs carry a per-run `step` counter. Ledger variants are never cargo-feature-gated: one schema, always readable.

`context_compacted` records a non-empty prior-turn range and must be followed by `context_built` for the same turn, or by `run_failed`. It emits no command and leaves the public run phase unchanged.

Future storage backends can persist these events to SQLite, Postgres, files, or an append-only log.

## Possible vocabulary, not committed modules

Hermes-shaped needs not every embedding wants. None exists until a run forces it; each enters as events/types only — engines stay outside core.

- delegation — parent/child run linkage with budget inheritance (plausible early).
- memory — retrieval query/hit types entering via the `retrieved_context` lane; never a store (wait).
- scheduling — job/trigger/triage vocabulary so unattended runs route findings to triage, not user spam (wait).

Cargo features, if ever, gate helpers only — when a second embedding needs a different subset (one-shot CLI runner vs persistent daemon).

## First proof

The contract is sharp because scripted fake-host tests drive these through `run` with exact state, command, and event assertions:

- happy path: propose → approval required → granted → executed → finished;
- denial path: denied → no `execute_tool` command ever emitted;
- failure path: tool fails → typed failure recorded, run ends failed;
- ordering: out-of-order events rejected; no commands after a terminal event; context budget validated before any model request;
- replay: re-applying the log reproduces final state with zero model calls and zero tool executions.

The multi-turn proof extends that contract without adding IO or runtime machinery:

- a successful tool result can conclude one turn and feed a later host-built context/model turn;
- policy denial, approval denial, and tool failure can also conclude a turn and continue through later host-built context;
- each turn consumes at most one host-validated tool call, while `model_responded` records all proposals as ledger facts;
- immediate `turn_id` reuse after a concluded turn is rejected;
- model `step` sequencing continues across turns;
- replay over a two-turn ledger still emits zero model calls and zero tool executions.
- readback projection is derived from replay-validated ledgers and rejects invalid event streams instead of producing partial transcripts.

No new crates for this; a fake host in tests is enough.

## Planned crate split

Possible workspace shape:

```text
platonic-core       typed kernel primitives
platonic-provider   model-provider adapter traits
platonic-tools      tool registry and schema validation
platonic-store      event store implementations
platonic-replay     replay/caching engine
plato-agent         Plato Agent application/reference runtime
```

Do not create these crates until the core contract needs them.
