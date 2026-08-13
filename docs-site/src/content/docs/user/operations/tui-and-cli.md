---
title: TUI and CLI workflows
description: Start local work, continue sessions, switch durable threads, and inspect live state.
sidebar:
  order: 1
---

<p class="section-kicker user">User docs</p>

This page is part of the [unreleased 0.2.0 operating guide](../).

## Start an interactive thread

Run Plato Agent from a registered Git workspace:

```bash
plato
```

Bare `plato` in a terminal ensures the host server, asks before admitting a durable root thread, and attaches the TUI to it. The thread keeps its server-minted ID and authority after the TUI exits.

Write in the composer and press `Enter` to submit. Use `Shift+Enter`, `Alt+Enter`, `Ctrl+J`, or `Ctrl+M` for a newline. Bracketed paste is inserted literally as one undoable edit.

The default conversation view keeps the human and assistant messages concise. Press `v` with an empty composer to switch to the complete ordered audit view. Press `?` for the controls implemented by the running client.

## Find or resume a thread

Open the picker with `/threads`. `/sessions` is a compatibility alias for the same picker.

1. Type any case-insensitive subsequence of a thread ID or its `active`, `loaded`, or `unloaded` state.
2. Move with `Up` and `Down` or `Ctrl+P` and `Ctrl+N`.
3. Press `Enter` to attach the selected thread, or `Esc` to close the picker.

From another terminal on the same host, attach directly:

```bash
plato --remote THREAD_ID
```

Attachment does not mint a thread or change its model, toolset, paths, network grant, or approval policy. Multiple clients can observe a thread; the server still enforces active-turn controller rules.

The ordinary `plato` TUI is attached to a durable thread, so `/new` is unavailable there. In an unattached standalone TUI session, `/new` clears the selected session so the next message starts fresh.

## Use one-shot sessions

Run one question without opening the TUI:

```bash
plato "summarize the current Git status"
```

The client ensures the same host server and streams the answer. On success it prints the run ID, ledger path, and an offline replay command.

<figure class="expected-output">
  <figcaption>
    Stable one-shot completion shape
    <span>Underlined values vary by run and registered workspace.</span>
  </figcaption>
  <pre tabindex="0"><samp>run_id: <var>&lt;run-id&gt;</var>
ledger_path: <var>&lt;ledger-path&gt;</var>
replay: plato replay --db='<var>&lt;ledger-path&gt;</var>' --run <var>&lt;run-id&gt;</var></samp></pre>
</figure>

Continue the latest workspace session with:

```bash
plato -c "explain the most important change"
```

Use one-shot mode for a bounded request and the TUI when you want live audit, thread switching, status, or approval controls. Both paths use the server's run implementation and ledger.

## Read live state

In the TUI, `/status` opens a read-only snapshot for the selected session. It distinguishes the configured model or alias from the provider-reported served model; a provider that omits that identity appears as `served unknown`.

The audit view includes the exact ordered events already available to the client. It does not replace the offline durable record. Use [replay](../history-and-recovery/#read-durable-history) when the client or server is not running.

## Finish or leave

`q` with an empty composer, `Esc` from the idle main view, `/quit`, and `/exit` close the TUI. Closing it leaves the server and durable thread available.

`plato thread stop THREAD_ID` is different: it cancels any active child, runs the server-owned repository integration and cleanup path, and writes a durable stop record. Do not use it merely to close a client.

For exact syntax and context-sensitive controls, use the [command reference](../../../reference/operations/commands/) and [TUI reference](../../../reference/operations/tui/).
