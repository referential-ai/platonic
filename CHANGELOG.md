# Changelog

## Platonic Unreleased

- Add a selectable Responses provider protocol beside Chat Completions.
- Add a local inference comparison proxy that Platonic, Codex, and Hermes
  clients can use as an OpenAI-compatible base URL.
- Add one-call shell credential grants.
- Hide shell credentials that are unavailable on the host from the model.
- Preserve TUI drafts across a daemon restart.
- Show the active run ID in the TUI working status.
- Correct the public 0.2.2 install and proof journey.
- Upgrade `rtrb` to 0.3.5 for RUSTSEC-2026-0274, a double free in
  `ReadChunk::commit` when an element's `Drop` panics.

## Platonic 0.2.2 - 2026-08-16

- Keep restart-synthesized failure returns from replacing a recovered child's
  real typed return.
- Preserve a committed parent follow-up when an interrupted child resumes in a
  fresh run.

## Platonic 0.2.1 - 2026-08-15

- Preserve profile yolo auto-approval for eligible tool calls made by
  supervised run children.

## Platonic 0.2.0 - 2026-08-15

- Complete the five-phase profile train: durable profile registry and revisions,
  one resumable home thread per profile, bounded profile-scoped context reads,
  typed child returns, and the protocol v2 cutover across the server, clients,
  CLI, TUI, HTTP gateway, and documentation.

## Platonic 0.1.0 - 2026-08-10

- Establish the product release identity independently of workspace crate
  versions and embed the exact source commit and UTC build date in server
  diagnostics.
- Add the two-target command-bundle path with deterministic inventories and
  SHA-256 manifests.
- Run one host-only Platonic server across every registered workspace and attach
  Plato Agent one-shots, TUI clients, and gateways to it; replay remains
  offline.
- Support Linux x86-64 and macOS Apple silicon command bundles at launch;
  Windows server and client support is withdrawn.
- Store new run events in one append-only JSONL file per run while retaining
  read-only replay of legacy SQLite event rows.

## Plato Agent 0.2.0 - Unreleased

- Align current copy with the workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md),
  preserving all technical identifiers.
- Raise the Plato Agent root and desktop minimum supported Rust version to
  1.88; `platonic-core` remains on Rust 1.85.

## Plato Agent 0.1.0 - 2026-07-15

First release. Local CLI, daemon, TUI, desktop shell, and Discord gateway over
the replayable `platonic-core` ledger, with explicit tool policy and local
approvals.

Known limitation: `shell.exec` is bounded and approval-gated but does not yet
run in an OS or container sandbox ([#81](https://github.com/referential-ai/platonic/issues/81)).
