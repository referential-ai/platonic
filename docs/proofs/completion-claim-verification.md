# Completion Claim Verification Checklist

Issue: [#406](https://github.com/referential-ai/plato-agent/issues/406)
Spec: [threaded server §5](https://github.com/referential-ai/platonic-workspace/blob/develop/product/reviews/2026-08-03-threaded-server-spec.md#5-completion-and-stalls-d007)

## Protocol

- [x] `CompletionClaim` round-trips typed with literal fixtures (full, minimal, blocked)
- [x] `CompletionClaimed` stream event round-trips in known-variants test
- [x] Absent `completion_claim` field stays compatible: legacy `RunStartResult` wire decodes with `completion_claim: None`
- [x] Absent `completion_claim` field stays compatible: legacy `TranscriptReadResult` wire decodes with `completion_claim: None`
- [x] Malformed `completion_claimed` stream event (missing `claim`) fails decode

## Daemon

- [x] `RunStatus` carries `completion_claim` through `finish_run`
- [x] `run_start_response` includes `completion_claim` when present
- [x] `TranscriptReadResult` from cold (SQLite) reads returns `completion_claim: None`

## Rendering

- [x] TUI `modal.rs` renders `completion_claimed` events as status lines ("claim done" / "claim blocked")
- [x] CLI `write_run_success_output` prints claim details when present
- [x] Claim rendering is visually distinct from verified state: labeled as "claim", never confused with final answer

## Gates

- [x] `cargo fmt --check` passes
- [x] `cargo test --locked` passes (498 passed, 0 failed, 2 ignored)
- [x] `cargo clippy --locked -p plato-tui --all-targets -- -D warnings` passes
