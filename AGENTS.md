# Plato Agent Guide

The workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms. This repository hosts Plato Agent; its
technical identities remain unchanged.

## Repo Boundary

- This repo owns app IO: CLI, config, provider calls, local tools, approvals, persistence, daemon, TUI, and connectors.
- `platonic-core` owns pure typed harness primitives only.
- Do not move provider clients, tool implementations, stores, daemon code, TUI code, or connector code into `platonic-core`.
- Do not split provider, tool, store, or replay code into candidate repos until a second concrete use and a `Ready for dev` issue/design justify the split.

## Workflow

- GitHub Issues are the scope and acceptance contract.
- GitHub PRs are the implementation and proof surface.
- Link every PR to its issue and include verification commands or manual proof.
- A PR changing user-visible behavior must update `README.md` or `docs/QUICKSTART.md` in the same PR.
- Merge authority follows the workspace-root `AGENTS.md`; CI must be green and every issue- or PR-specific review and proof gate must be satisfied.
- The workspace-root [Simplicity Directive](https://github.com/referential-ai/platonic-workspace/blob/main/AGENTS.md#simplicity-directive) is binding: every changed line must serve named acceptance; stop before scope widens.
- Do not use local TODOs, wiki pages, tmux pane names, or chat history as active-work authority.
- Do not start implementation unless a GitHub issue or direct human task has clear scope, non-goals, acceptance, target surface, and proof.

## Runtime Topology

- `plato` one-shot execution and `plato replay` must work without a daemon permanently.
- One-shot, daemon, TUI, gateway, and desktop surfaces share one run-driving implementation. Do not duplicate model/tool/policy event choreography.
- Provider fallback changes run outcome and must be recorded in the run ledger. Unrecorded fallback is forbidden.
- `plato-agentd` owns the persistent workspace runtime; clients attach through the daemon protocol and do not own run semantics.
- Connectors must not own sessions, policy, approvals, provider fallback, or run semantics.

## Verification

- Unix external-daemon proofs use a pre-absent, issue-named short `/tmp/p<issue>` root with mode `0700`; derive and print the final socket path before spawn and require its byte length to be below 100; readiness uses `-S` followed by the existing bounded client/status/hello readback, never `-s`; preserve the original timeouts and assertions; clean up only the exact owned tmux session, process/group, socket, state, and root, never a broad `/tmp` scan.

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

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
