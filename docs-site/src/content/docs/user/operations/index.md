---
title: Daily operation
description: Choose a Plato Agent client, resume durable work, and inspect the server-owned state behind it.
sidebar:
  label: Daily operation
  order: 10
---

<p class="section-kicker user">User docs</p>

> **Unreleased 0.2.0 documentation:** These pages describe the current `develop`
> behavior. The public release is still 0.1.0; no released 0.2.0 bundle or
> bundle-install proof exists. The first journey was verified with binaries
> built from its exact source commit. Do not treat or publish this guide as
> release-current until `platonic-v0.2.0` exists.

Complete the [first productive journey](../first-run/) before using this section as the daily reference.

Plato Agent has several local client surfaces, but they all use one Platonic server on the same host. The server owns threads, provider calls, tools, approvals, and the durable ledger.

## Choose a surface

| Need | Use | What happens |
| --- | --- | --- |
| Work interactively | `plato` | Ensures the host server, admits a durable root thread, and opens the TUI |
| Ask once | `plato "QUESTION"` | Ensures the host server, runs one turn, and prints a replay hint |
| Continue the latest workspace session | `plato -c "QUESTION"` | Appends one turn through the one-shot client |
| Return to a durable thread | `plato --remote THREAD_ID` | Opens a TUI on that existing thread through the same-host socket |
| Inspect committed history | `plato replay` | Reads the registered workspace ledger offline |
| Attach a terminal-only client | `plato-tui` | Connects to an existing server; it never starts or supervises one |

`--remote` does not mean a remote host or HTTP gateway. It attaches another local terminal client to an existing thread through the host socket.

## Pick up work

A **session** groups conversational runs in one workspace. A **thread** adds durable, immutable authority such as its model, approval policy, working tree, and network grant.

- Use `plato -c "QUESTION"` to continue the latest workspace session through the one-shot client.
- Use `/threads` in the TUI or `plato thread list` to find durable threads.
- Use `plato --remote THREAD_ID` or select a thread in the picker to attach without creating another one.
- Use `v` for the complete audit view and `plato replay` for the committed offline record.

Closing a client does not stop the host server or delete a thread. Stop a thread only when you intend to finish its server-owned repository lifecycle.

## Inspect status

Use `/status` in the TUI for the selected session. It reads the effective provider and model, server identity, ledger, usage, approval counts, and session-scoped approval state without calling a model or changing the session.

For a shell-readable server result:

```bash
platonic status --workspace "$PWD"
```

Use `plato thread status THREAD_ID` for a durable thread's model, approval policy, working directory, and current loaded or active state.

## Continue by task

- [Work in the TUI or one-shot CLI](./tui-and-cli/)
- [Review approvals and yolo behavior](./approvals/)
- [Replay, reconnect, and recover](./history-and-recovery/)
- [Configure a provider and runtime limits](../../reference/configuration/)
- [Look up commands](../../reference/operations/commands/) or [TUI controls](../../reference/operations/tui/)
- Enter the optional [voice](./voice/) or [Discord](./discord/) path
