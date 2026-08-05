# Changelog

## 0.2.0 - 2026-08-04

- Align current copy with the workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md),
  preserving all technical identifiers.
- Raise the Plato Agent root and desktop minimum supported Rust version to
  1.88; Platonic Core remains on Rust 1.85.

## 0.1.0 - 2026-07-15

First release. Local CLI, daemon, TUI, desktop shell, and Discord gateway over
the replayable `platonic-core` ledger, with explicit tool policy and local
approvals.

Known limitation: `shell.exec` is bounded and approval-gated but does not yet
run in an OS or container sandbox ([#81](https://github.com/referential-ai/plato-agent/issues/81)).
