# plato-tui

Terminal client library for Plato Agent. It owns terminal state, daemon-event reduction, rendering, keyboard handling, thread selection, and client-local voice integration.

The standalone `plato-tui` binary attaches to an existing `platonic serve` host endpoint. It never starts, supervises, restarts, or stops the server, and it does not call providers, execute tools, decide server policy, or write the workspace ledger directly. The `plato` distribution binary owns the ordinary auto-ensuring TUI entrypoint.

User documentation:

- [Daily TUI and CLI workflows](../../docs-site/src/content/docs/user/operations/tui-and-cli.md)
- [TUI controls](../../docs-site/src/content/docs/reference/operations/tui.md)
- [Approvals](../../docs-site/src/content/docs/user/operations/approvals.md)
- [Replay, reconnect, and recovery](../../docs-site/src/content/docs/user/operations/history-and-recovery.md)

The command and key tables are source-checked against `crates/plato-agent/src/bin/plato-tui.rs`, this crate's command registry and input handlers, and focused tests.
