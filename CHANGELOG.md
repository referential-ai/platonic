# Changelog

## 0.3.1 — 2026-08-01

- Added optional `ModelResponded.served_model` and matching readback data to retain the validated provider-reported model identity while preserving pre-field 0.3.0 event compatibility.

## 0.3.0 — 2026-07-31

- Enforced run-wide uniqueness for accepted `TurnId` and `ToolCallId` values during transitions and replay, including the public `ToolCallReused` error.
- Documented `RunReadback` as an all-visibility audit projection that retains each tool result's `ResultVisibility`, with focused coverage for tool-call mismatches and default approval policy.
- Declared Rust 1.85 as the minimum supported compiler, added an exact Rust 1.85 locked CI check, and removed the redundant development-only `serde_json` declaration.
- Made `HarnessEvent` and `PolicyDecision` reject unknown JSON fields while retaining literal 0.2.0 compatibility fixtures, and restricted the crate package to its public source, example, README, changelog, and license files.
- Clarified that security fixes support the latest published release on `main`.

This release is source- and wire-behavior-breaking for Rust consumers: `Error` gained `ToolCallReused`, earlier turn and tool-call IDs cannot be reused within a run, and unknown fields in the two durable tagged enums now fail deserialization.

## 0.2.0 — 2026-07-30

- Added typed, replay-validated readback paths for context compaction, whole-batch tool-proposal rejection, and non-terminal model failure. A model failure preserves the pending request so the host can retry the same turn and step.
- Changed `ModelResponded::usage` to `Option<ModelUsage>`. Known usage, including reported zero, keeps the 0.1 object representation; provider-omitted usage is `null`.
- Added exhaustive literal event fixtures and a dependency-free `minimal_host` example covering an approval-gated tool turn and offline replay.

This release is source-breaking for Rust consumers: public event, phase, and readback enums gained variants, and consumers must now handle optional model usage.

## 0.1.0 — 2026-07-15

First release. Sans-IO harness kernel: identifier newtypes, model-facing message primitives, lane-budgeted context packs, typed effect classes and policy decisions, tool-call/result boundaries, durable event ledger, pure multi-turn run state machine, replay-validated readback projections, shared error types. Providers, tools, stores, and interfaces live in outer crates.

## 0.1.0-alpha.1 — 2026-07-15

Publication rehearsal of the same kernel surface.
