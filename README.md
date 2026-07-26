# Platonic Core

Core Rust harness primitives for disciplined, replayable agent execution.

Platonic Core is **not** a chatbot, personal assistant, workflow SaaS, or “Agent OS.” It is the small kernel underneath those things: typed context assembly, tool-call boundaries, policy decisions, event logs, and replay/audit surfaces.

Part of the Referential AI stack.

## Naming

The workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms. Platonic Core is the framework's sans-IO
kernel.

## Design stance

1. Every side effect is typed.
2. Every context byte has a lane and a budget.
3. Every run is an event log first; transcript is a derived view.
4. Provider fallback is a policy event, not a silent retry.
5. Tool output is structured data plus a short summary; raw logs become artifacts.
6. Memory is scoped, typed, source-backed, and budgeted — not a junk drawer.
7. The core stays small. Gateways, dashboards, cron, skills, voice, and platform adapters belong outside the kernel.

## Current crate contents

The crate is intentionally modular inside the kernel:

```text
ids      identifier newtypes
message  model-facing message primitives
context  lane-labeled context packs with budget validation
policy   effect classes and policy decisions
tool     tool-call and tool-result boundaries
event    durable harness event ledger
run        pure run state machine
projection pure run readback projection
error      shared error types
```

The public contract is documented in the [crate rustdoc](https://docs.rs/platonic-core).

## Minimal host

Run the public kernel through one approval-gated tool turn and replay the resulting ledger:

```bash
cargo run --example minimal_host
```

The example simulates model and tool IO. It performs no file write or network call.

## Verify

```bash
cargo test
```

## Status

Seed kernel. The run contract is implemented as a pure multi-turn state machine with replay-validated readback projections; outer apps still own IO, providers, tools, stores, gateways, renderers, and schedulers.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE) ([official text](https://www.apache.org/licenses/LICENSE-2.0))
- [MIT License](LICENSE-MIT) ([official text](https://opensource.org/licenses/MIT))

at your option.
