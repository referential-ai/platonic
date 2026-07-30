# Changelog

## 0.2.0 — 2026-07-30

- Added typed, replay-validated readback paths for context compaction, whole-batch tool-proposal rejection, and non-terminal model failure. A model failure preserves the pending request so the host can retry the same turn and step.
- Changed `ModelResponded::usage` to `Option<ModelUsage>`. Known usage, including reported zero, keeps the 0.1 object representation; provider-omitted usage is `null`.
- Added exhaustive literal event fixtures and a dependency-free `minimal_host` example covering an approval-gated tool turn and offline replay.

This release is source-breaking for Rust consumers: public event, phase, and readback enums gained variants, and consumers must now handle optional model usage.

## 0.1.0 — 2026-07-15

First release. Sans-IO harness kernel: identifier newtypes, model-facing message primitives, lane-budgeted context packs, typed effect classes and policy decisions, tool-call/result boundaries, durable event ledger, pure multi-turn run state machine, replay-validated readback projections, shared error types. Providers, tools, stores, and interfaces live in outer crates.

## 0.1.0-alpha.1 — 2026-07-15

Publication rehearsal of the same kernel surface.
