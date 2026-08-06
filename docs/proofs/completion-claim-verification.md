# Completion Claim Verification Checklist

Issue: [#406](https://github.com/referential-ai/plato-agent/issues/406)
Spec: [threaded server §5](https://github.com/referential-ai/platonic-workspace/blob/develop/product/reviews/2026-08-03-threaded-server-spec.md#5-completion-and-stalls-d007)

## Protocol

- [ ] `CompletionClaim` round-trips typed with literal fixtures (full, minimal, blocked)
- [ ] `CompletionClaimed` stream event round-trips in known-variants test
- [ ] Absent `completion_claim` field stays compatible: legacy `RunStartResult` wire decodes with `completion_claim: None`
- [ ] Absent `completion_claim` field stays compatible: legacy `TranscriptReadResult` wire decodes with `completion_claim: None`
- [ ] Malformed `completion_claimed` stream event (missing `claim`) fails decode

## Daemon

- [ ] `RunStatus` carries `completion_claim` through `finish_run`
- [ ] `run_start_response` includes `completion_claim` when present
- [ ] `TranscriptReadResult` from cold (SQLite) reads returns `completion_claim: None`

## Rendering

- [ ] TUI `modal.rs` renders `completion_claimed` events as status lines ("claim done" / "claim blocked")
- [ ] CLI `write_run_success_output` prints claim details when present
- [ ] Claim rendering is visually distinct from verified state: labeled as "claim", never confused with final answer

## Gates

- [ ] `cargo fmt --check` passes
- [ ] `cargo test --locked` passes
- [ ] `cargo clippy --locked --all-targets -- -D warnings` passes
