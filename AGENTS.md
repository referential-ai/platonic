# Agent Instructions for platonic-core

This repo is the small Rust harness kernel for the Platonic framework. The
workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms.

## Rules

- Keep the core small. Do not add gateways, cron, UI, provider clients, memory stores, or tool implementations to this crate.
- Event log first. If behavior matters, model it as a typed event before building a view around it.
- Every side effect must cross a typed `ToolCall` + `PolicyDecision` boundary.
- Context must be lane-budgeted. Do not add unbounded prompt/memory injection.
- No silent provider fallback. Fallback is policy plus event log entry.
- No `unsafe` in the core crate.
- Prefer explicit types over stringly runtime behavior.
- If a feature exceeds the kernel boundary, reject it from this repo; do not create an outer crate or placeholder doc without separately admitted work.
- Merge authority follows the workspace-root `AGENTS.md`; CI must be green and every issue- or PR-specific review and proof gate must be satisfied.
- The workspace-root [Simplicity Directive](https://github.com/referential-ai/platonic-workspace/blob/main/AGENTS.md#simplicity-directive) is binding: every changed line must serve named acceptance; stop before scope widens.
- Candidate split repos are not active implementation targets until the workspace approves the split through a `Ready for dev` issue/design.

## Verification

Run before pushing changes:

```bash
cargo fmt --check
cargo test
```

For structural changes, update `docs/ARCHITECTURE.md` and `docs/TECHNICAL-LESSONS.md` if assumptions change.

## GitHub-Native Workflow

<!-- BEGIN GITHUB WORKSPACE OPS -->
# Agent Operating Rules

- GitHub Project #1 (`Platonic`) is the visible active-work board/WIP readback surface for this workspace.
- GitHub Issue is the scope contract: problem, expected behavior, scope, non-goals, acceptance criteria, and verification/proof.
- GitHub PR is the implementation, proof, review, and merge surface.
- Do not start implementation unless the issue is `Ready for dev` or the human explicitly authorizes exploration.
- `Ready for dev` means the issue/design/plan is clear enough for one bounded worker. `Needs refine` means refine/reconcile before coding.
- If scope is unclear, refine/comment on the issue before coding.
- Link every PR to its issue.
- Post proof in the PR: tests, commands, screenshots, or manual verification.
- Do not silently change scope. If scope changes, comment with proposed revised acceptance criteria.
- Use plandocs only for complex/risky work: cross-repo, auth/security, schema/data migration, deployment/infra, multi-agent, more than one PR, or unclear architecture.
- Wiki, plandoc, Discord/Slack, tmux, and local notes must not mirror active board/ticket state. Important decisions must be copied to the issue, PR, `AGENTS.md`, or approved design/plandoc.
<!-- END GITHUB WORKSPACE OPS -->
