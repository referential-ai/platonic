---
title: Operations commands
description: Source-checked command forms for local Plato Agent and Platonic server operation.
sidebar:
  order: 1
---

<p class="section-kicker reference">Reference</p>

This page is part of the [Platonic 0.2.0 operating guide](../../../user/operations/).

These tables cover the core operating surface. Run a command with `--help` for its final-head parser output. Paths passed to client commands are resolved from the workspace unless the help says otherwise.

## Plato Agent entrypoints

| Command | Purpose |
| --- | --- |
| `plato` | Start the TUI when standard input and output are terminals |
| `plato --tui` | Explicitly start the TUI |
| `plato [--config FILE] [--yolo] "QUESTION"` | Run one question through the host server |
| `plato -c "QUESTION"` | Continue the latest workspace session |
| `plato --remote THREAD_ID` | Attach a TUI to an existing thread on the same host |
| `plato --profile NAME` | Select a workspace profile and attach its durable home |
| `plato --reduced-motion` | Start the TUI with a static working indicator |
| `plato --voice-config FILE` | Start the TUI with exact client-only voice configuration |

`--remote` cannot be combined with `--yolo` or `--profile`. `--profile` is TUI-only. TUI mode cannot be combined with a question, `--db`, `-c`, or a subcommand. `--voice-config` is TUI-only.

## Offline replay

| Command | Purpose |
| --- | --- |
| `plato replay` | Replay the latest registered workspace session from its SQLite ledger |
| `plato replay --run RUN_ID` | Replay one run from the registered workspace ledger |
| `plato replay FILE` | Replay one JSONL file directly |
| `plato replay --db` | Explicitly select the registered workspace ledger |
| `plato replay --db=PATH [--run RUN_ID]` | Replay an explicit SQLite ledger; a path value requires `=` |

Replay cannot be combined with `--config`, `--voice-config`, `--yolo`, `-c`, `--tui`, `--remote`, or a question. `--run` requires a SQLite ledger; `FILE` and `--db` are mutually exclusive.

## Durable threads

| Command | Purpose |
| --- | --- |
| `plato thread spawn --parent THREAD_ID --model MODEL --reasoning-effort EFFORT [--cwd DIR] [--approval-policy prompt\|yolo]` | Propose a same-profile child, then answer its `[y/N/c]` admission prompt |
| `plato thread list` | Print every durable thread in the selected workspace joined with current server state as JSON, one per line |
| `plato thread status THREAD_ID` | Print one durable thread and current state as JSON |
| `plato thread send THREAD_ID --controller CONTROLLER_ID [--turn TURN_ID] MESSAGE...` | Start an idle turn or steer the active turn owned by that controller |
| `plato thread attach THREAD_ID [--from-offset OFFSET]` | Stream ordered thread events as JSON lines until interrupted |
| `plato thread stop THREAD_ID` | Stop the thread and active child through the server lifecycle |

Reasoning effort accepts `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`. Approval policy accepts `prompt` or `yolo`; the default is `prompt`. `--cwd` defaults to the workspace directory. `--parent` is required, and the server rejects cross-profile or widening children. Home threads reject `thread.stop`.

## Profiles

| Command | Purpose |
| --- | --- |
| `platonic profile create NAME WORKSPACE_ID [--config FILE] [--model MODEL] [--reasoning-effort EFFORT] [--approval-policy prompt\|yolo] [--tool TOOL]... [--instructions FILE] [--memory FILE] [--skill REF]...` | Create a profile after resolving provider and tool defaults and proving its provider key is available |
| `platonic profile list [--workspace WORKSPACE_ID] [--limit COUNT]` | List profiles, optionally within one workspace |
| `platonic profile status PROFILE_ID` | Read current defaults, content revision, and home relation |
| `platonic profile update PROFILE_ID [--config FILE] [--model MODEL] [--reasoning-effort EFFORT] [--approval-policy prompt\|yolo] [--tool TOOL]... [--instructions FILE] [--memory FILE] [--skill REF]... [--clear-skills]` | Change future defaults and append one content revision |
| `platonic profile open PROFILE_ID [--repo PATH]... [--working-repository PATH] [--working-subdir PATH] [--idempotency-key KEY] [--approve]` | Resolve the durable home or propose it; `--approve` grants a new pending proposal |

Profile names are workspace-local; profile ids are server-minted. `create` uses
the selected config only to resolve defaults and provider readiness. `update`
preserves unspecified values, and an explicitly repeated `--tool` replaces the
toolset. Content files are read as Markdown. `open` defaults each path to `.`;
without `--approve`, a new home remains an explicit approval result.

## Host server and workspaces

| Command | Purpose |
| --- | --- |
| `platonic serve` | Run the one-per-host server in the foreground |
| `platonic status [--workspace DIR] [--session SESSION_ID] [--config FILE]` | Read authoritative JSON status; workspace defaults to `.` |
| `platonic shutdown [--workspace DIR]` | Shut down the host server only when it is idle |
| `platonic workspace create NAME DIR` | Register a named canonical workspace directory |
| `platonic workspace list` | List registered workspaces |
| `platonic workspace status WORKSPACE_ID` | Read one workspace by its server-minted ID |

`platonic status`, `platonic shutdown`, and workspace commands also accept a local `--socket PATH` where their help lists it. That is an explicit same-host endpoint, not a cross-host transport.

## Standalone TUI

| Command | Purpose |
| --- | --- |
| `plato-tui [--workspace DIR]` | Attach a terminal client to the existing host server |
| `plato-tui --run RUN_ID` | Initially display a specific run transcript |
| `plato-tui --socket PATH` | Attach to an explicit local endpoint |
| `plato-tui --config PATH` | Pass a config path to daemon-started unattached runs |
| `plato-tui --profile NAME` | Select a workspace profile and attach its home |
| `plato-tui --voice-config FILE` | Load exact client-only voice configuration |
| `plato-tui --reduced-motion` | Use a static working indicator |

Unlike `plato`, standalone `plato-tui` never starts, supervises, restarts, or stops `platonic serve`.

Continue with the [daily workflows](../../../user/operations/tui-and-cli/) or [TUI controls](../tui/).
