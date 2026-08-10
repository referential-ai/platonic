# Plato Agent Guide

The workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms.

This repository hosts **two** products, split across crates since #449:

- **Platonic** — the agent server, and the product. Workspaces, agents,
  threads, tools, providers, ledger, policy, approvals, protocol, gateways.
  Crate `platonic-server`, command `platonic`.
- **Plato Agent** — the client distribution built on it. Clients, curated
  agent configurations, skills. Crate `plato-agent`, command `plato`.

Architecture is indexed on the
[decision map](https://github.com/referential-ai/platonic-workspace/issues/83).

## Repo Boundary

- `crates/platonic-server` owns workspaces, agents, threads, sessions, policy, approvals, provider calls, the ledger, the protocol, and gateways. Clients, distributions, and connectors do not acquire those semantics.
- `crates/plato-agent` owns the client distribution: the `plato` and `plato-tui` binaries, the TUI, and the voice subsystem. It depends only on the client, protocol, core, and client-side leaf crates; it never links `platonic-server`.
- `crates/platonic` owns the thin `platonic` product command over `platonic-server`. The Discord connector is a server module under `crates/platonic-server/src/gateway/discord`.
- `platonic-core` owns pure typed harness primitives only. The server instantiates it once per thread and performs every effect it asks for; the kernel never does. That separation is enforced by the crate boundary, not by convention.
- An **agent** is data — a configured profile bound to one workspace, providing a default toolset, operating many threads. Not a process the server launches, and not a linked plugin.
- Do not move provider clients, tool implementations, stores, daemon code, TUI code, or connector code into `platonic-core`.
- Do not split provider, tool, store, or replay code into further crates until a second concrete use and a `Ready for dev` issue/design justify it. `platonic-server` stays one crate until something else consumes its pieces.

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

- `plato` one-shot execution auto-ensures the host server. `plato replay` remains fully offline and must work without the server binary.
- One-shot, daemon, TUI, gateway, and desktop surfaces share one run-driving implementation. Do not duplicate model/tool/policy event choreography.
- Provider fallback changes run outcome and must be recorded in the run ledger. Unrecorded fallback is forbidden.
- `platonic serve` owns the persistent **server** runtime — one daemon per host, serving many workspaces. Clients attach through the server protocol and do not own run semantics.
- A **workspace** is a named, registered directory owning one ledger and possibly several repositories. It is not derived from a path: the registry maps a stable server-minted id to the workspace's current directory and ledger location.
- Connectors must not own sessions, policy, approvals, provider fallback, or run semantics.

## Verification

- Unix external-daemon proofs use a pre-absent, issue-named short `/tmp/p<issue>` root with mode `0700`; derive and print the final socket path before spawn and require its byte length to be below 100; readiness uses `-S` followed by the existing bounded client/status/hello readback, never `-s`; preserve the original timeouts and assertions; clean up only the exact owned tmux session, process/group, socket, state, and root, never a broad `/tmp` scan.

```bash
cargo fmt --check
cargo test --workspace --locked
cargo clippy --workspace --locked --all-targets -- -D warnings
cargo test --locked --manifest-path desktop/src-tauri/Cargo.toml
```

`cargo test` takes `--workspace` because the repository root is a virtual
workspace; package-scoped tests cover only one member and report a fraction of
the suite.

`desktop/src-tauri` is excluded from the Cargo workspace because it needs GTK
and webkit system libraries. Even `--workspace` therefore does **not** cover
it, and the fourth command is required: CI proves the desktop crate in the
`Linux shell` job, so a change that builds locally can still break CI without
it. Clippy takes `--workspace` to match CI exactly.

**Run the battery on the pinned toolchain, or its verdict is not CI's.** CI
pins `1.88.0`; a newer toolchain reports lints CI does not, and an older one
misses lints CI enforces. Both `rust-toolchain.toml` and `.mise.toml` pin
`1.88.0`, so rustup and mise users each get the pinned toolchain
automatically — but neither helps if `cargo` resolves to a distribution
package instead. Check before trusting a green battery:

```bash
cargo clippy --version   # must report 0.1.88
```

A distribution `cargo` on `PATH` is not a rustup proxy and silently ignores
`rust-toolchain.toml`. That is the configuration under which a clean local
clippy run and a red CI run can both be honest.

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
