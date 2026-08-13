---
title: First run
description: Build exact-head commands, finish one read-only TUI task, and prove its durable record.
sidebar:
  order: 2
---

<p class="section-kicker user">User docs</p>

This is one linear OpenRouter journey through the unreleased 0.2.0 behavior on
`develop`. It uses the built-in configuration and `OPENROUTER_API_KEY`; do not
add a `plato.toml` for this path.

> **Release gate:** The public release is 0.1.0. No released 0.2.0 bundle or
> bundle-install proof exists until the real `platonic-v0.2.0` release. This
> journey therefore uses locally built binaries from one exact source commit.
> Do not substitute or rename a 0.1.0 bundle, and do not publish this page as
> release-current.

## 1. Build the exact-head commands

From the repository root at the commit under review, build all three commands
with that source identity embedded:

```bash
source_commit=$(git rev-parse --verify HEAD)
build_date=$(date -u +%F)
PLATONIC_BUILD_COMMIT="$source_commit" \
PLATONIC_BUILD_DATE="$build_date" \
PLATO_BUILD_IDENTITY="0.2.0 $source_commit $build_date" \
  cargo build --locked --release \
  --package plato-agent --package platonic \
  --bin plato --bin plato-tui --bin platonic
export PATH="$PWD/target/release:$PATH"

platonic --version
plato --version
plato-tui --version
```

**Checkpoint:** each line contains the exact 40-character commit and UTC build
date. `plato` and `plato-tui` report 0.2.0. `platonic` reports product version
0.1.0 because the unreleased tree deliberately retains the current release
contract; that output is not evidence of a 0.2.0 bundle. Keep this terminal's
`PATH` for the rest of the journey.

<figure class="expected-output">
  <figcaption>
    Stable version shape
    <span>Underlined provenance values come from the exact source commit and UTC build date.</span>
  </figcaption>
  <pre tabindex="0"><samp>platonic 0.1.0 (<var>&lt;source-commit&gt;</var>, <var>&lt;YYYY-MM-DD&gt;</var>)
plato 0.2.0 <var>&lt;source-commit&gt;</var> <var>&lt;YYYY-MM-DD&gt;</var>
plato-tui 0.2.0 <var>&lt;source-commit&gt;</var> <var>&lt;YYYY-MM-DD&gt;</var></samp></pre>
</figure>

## 2. Give the server its OpenRouter credential

Create an [OpenRouter API key](https://openrouter.ai/settings/keys) and save
only that key in a private file outside the workspace. In the terminal that
will run the server, read it into the documented environment boundary without
printing it:

```bash
chmod 0600 "$HOME/.config/platonic/openrouter-key"
IFS= read -r OPENROUTER_API_KEY < "$HOME/.config/platonic/openrouter-key"
export OPENROUTER_API_KEY
test -n "$OPENROUTER_API_KEY" && printf '%s\n' 'OpenRouter key: available (value not shown)'
```

**Checkpoint:** the last command prints only:

<figure class="expected-output">
  <figcaption>
    Literal stable output
    <span>No credential value appears.</span>
  </figcaption>
  <pre tabindex="0"><samp>OpenRouter key: available (value not shown)</samp></pre>
</figure>

The secret belongs in the `platonic serve` environment. Do not put it in the
workspace, a command argument, a transcript, or a log. Server-run shell tools
receive a scrubbed environment and do not inherit provider credentials.

## 3. Start Platonic

Keep the credential terminal open and start the one host server in the
foreground:

```bash
platonic serve
```

**Checkpoint:** before waiting for clients, the server prints these two lines;
the socket prefix varies by host:

<figure class="expected-output">
  <figcaption>
    Stable startup shape
    <span>The underlined runtime directory varies by host.</span>
  </figcaption>
  <pre tabindex="0"><samp>daemon_scope: host
socket_path: <var>&lt;runtime-directory&gt;</var>/platonic/host/agent.sock</samp></pre>
</figure>

Leave this process running. The server, not the TUI, owns provider calls,
tools, approvals, and ledger writes.

## 4. Create and register a workspace

In a second terminal, make a harmless Git repository with one committed file:

```bash
mkdir -p "$HOME/platonic-first-run"
cd "$HOME/platonic-first-run"
git init
printf '%s\n' \
  '# First-run workspace' \
  '' \
  'This repository is a harmless, read-only target for the Plato Agent first run.' \
  > README.md
git add README.md
git -c user.name='Platonic First Run' \
  -c user.email='first-run@invalid' commit -m 'Initial workspace'

platonic workspace create first-run "$PWD"
platonic status --workspace "$PWD"
```

**Checkpoint:** `workspace create` returns one JSON object whose `workspace`
contains a stable `id`, the canonical `root`, a `ledger_path`, and
`"health":"present"`. In the following status JSON, check the `model` object
without exposing the key:

<figure class="expected-output">
  <figcaption>
    Stable status excerpt
    <span>The model object is reformatted; surrounding status fields are omitted.</span>
  </figcaption>
  <pre tabindex="0"><samp>{
  "requested_alias": "~openai/gpt-latest",
  "served_model": null,
  "provider_kind": "open_router",
  "key_present": true
}</samp></pre>
</figure>

`served_model` is `null` because this clean workspace has not completed a
provider response yet.

## 5. Open the TUI and approve the thread

From the registered workspace, run the client with no question or subcommand:

```bash
plato
```

Before the TUI opens, Plato Agent proposes a root thread with workspace-write
authority. The generated thread id varies:

<figure class="expected-output">
  <figcaption>
    Stable approval prompt
    <span>The underlined thread id is minted by the server.</span>
  </figcaption>
  <pre tabindex="0"><samp>thread.spawn <var>&lt;thread-id&gt;</var> (WorkspaceWrite): thread.spawn requires approval before authority is created
Approve thread.spawn? [y/N/c]</samp></pre>
</figure>

Type `y` and press Enter. This approves creation of the durable thread; it does
not approve every future tool call.

**Checkpoint:** the Plato Agent TUI opens with an empty transcript and the
composer ready for input.

## 6. Complete one read-only task

Submit this exact task in the composer:

```text
Use only file.read to read README.md. Reply with its Markdown heading and the purpose sentence below it. Do not change files.
```

**Checkpoint:** the transcript shows your task, a successful `file.read` tool
step, and a final answer containing both of these facts:

<figure class="expected-output">
  <figcaption>
    Required answer facts
    <span>The provider may format or add text; these two facts must appear.</span>
  </figcaption>
  <pre tabindex="0"><samp>First-run workspace
This repository is a harmless, read-only target for the Plato Agent first run.</samp></pre>
</figure>

`file.read` is read-only and auto-allowed, so no tool-approval panel appears.
After the answer finishes, press `q` with an empty composer to leave the TUI,
then verify that the repository is unchanged:

```bash
git status --short
```

**Checkpoint:** `git status --short` prints nothing.

## 7. Inspect status, transcript, and replay

List the durable threads and copy the `authority.thread_id` value from the JSON
line for the thread you just used:

```bash
plato thread list
thread_id=<paste-thread-id>
plato thread status "$thread_id"
```

**Checkpoint:** status returns that same id and working directory under
`authority`. After the completed task, `live.loaded` is `true` and
`live.current_turn_id` is `null`.

Reattach to the same live thread:

```bash
plato --remote "$thread_id"
```

**Checkpoint:** the TUI reloads the task, `file.read` step, and final answer.
Press `q` again after inspecting it.

Now read the workspace ledger:

```bash
plato replay
```

**Checkpoint:** replay begins with a `session_id` and `run_id`, includes
`final_phase: Finished`, the user message, `tool_call file.read`, its tool
result, and the final assistant message. Replay is read-only: it performs no
provider request and runs no tool.

## 8. Prove the restart boundary

Stop the idle server, then replay with the credential removed from this child
command:

```bash
platonic shutdown --workspace "$PWD"
env -u OPENROUTER_API_KEY plato replay
```

**Checkpoint:** shutdown prints `{"result":"shutdown"}`, and offline replay
still prints the same finished run.

Start `platonic serve` again from the credential terminal. In the workspace
terminal, inspect the same records:

```bash
platonic status --workspace "$PWD"
plato thread list
plato replay
```

**Checkpoint:** the workspace and completed replay survive. The old thread id
also remains, but `live.loaded` is now `false` because live execution state
belongs to one server process. Start bare `plato` and approve a new root thread
for future work rather than trying to continue the unloaded authority record.

## Recover by symptom

| Symptom | Recovery |
| --- | --- |
| Looking for a 0.2.0 archive returns HTTP 404 | The release gate is still closed. Do not substitute or rename 0.1.0; build the exact-head commands in step 1 or wait for `platonic-v0.2.0`. |
| `platonic serve` reports a missing provider key, or status shows `"key_present":false` | Stop the idle server. Load `OPENROUTER_API_KEY` in the terminal that will own `platonic serve`, verify only that it is nonempty, and restart the server. |
| `plato` reports `workspace_unregistered` | Return to the intended Git repository and run `platonic workspace create first-run "$PWD"` once. If the name is already used, inspect `platonic workspace list` instead of creating a competing record. |
| Thread creation reports that the directory is not a Git repository or has no usable commit | Complete the `git init`, `git add`, and initial commit commands above, then rerun bare `plato`. |
| `thread spawn denied` appears | Rerun bare `plato`, review the proposed root authority, and type `y` only if it matches this workspace. |
| The task fails before a final answer | Check `platonic status --workspace "$PWD"`. Confirm `provider_kind` is `open_router` and `key_present` is `true`; then retry in a new TUI thread. The failed attempt remains in the ledger. |
| `plato replay` reports no sessions | Finish one task from this registered workspace first. Replay selects that workspace's latest recorded session. |

For daily operation, approvals, and provider choices, follow
[#548](https://github.com/referential-ai/platonic/issues/548). For server,
protocol, and ledger internals, follow
[#547](https://github.com/referential-ai/platonic/issues/547).
