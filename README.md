# Platonic

*by Referential.ai*

Platonic is a self-hosted agent server. One host server runs many registered
workspaces, agents, and durable threads while owning provider calls, tools,
policy, approvals, and ledgers. Plato Agent is the client distribution built
on Platonic.

The public site is [referential.ai](https://referential.ai).

The workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms.

**New here? Start with [docs/QUICKSTART.md](docs/QUICKSTART.md) to install the
command bundle and run the server and client.**

Platonic 0.1.0 launches with command bundles for Linux x86-64 and macOS Apple
silicon. Windows server and client support is withdrawn. Additional targets
are post-launch and proof-first. Downloadable bundles are the only Platonic
product distribution; see [the release contract](docs/RELEASE.md).

The bootstrap surface is intentionally small:

- `platonic serve` runs the one host server in the foreground.
- `platonic workspace create <name> <directory>` registers a workspace explicitly.
- Bare `plato` in a terminal ensures the host server, asks once before registering an unknown directory, creates an approved durable thread, and opens the TUI on it.
- `plato --remote <thread-id>` opens another TUI on the same host socket and existing thread.
- `plato "question"` ensures the host server and runs as a short-lived client.
- `plato -c "follow-up"` continues the latest workspace session from the workspace ledger.
- `plato replay <file>` validates and prints a deterministic JSONL readback without network calls or tool execution.
- `plato replay [--run <id>]` replays the default workspace ledger; omitted `--run` selects the latest session.
- `plato replay --db[=<path>] [--run <id>]` selects an explicit workspace state database and replays its run log.
- `plato issue-prep start <run-dir>` runs the fixed issue preparation pipeline from Markdown on stdin.
- `plato thread spawn|list|status|send|attach|stop` manages and observes durable threads on a serving host server.
- `platonic status|shutdown` inspects or stops the host server.
- `platonic workspace create|list|status` manages registered workspaces.
- `platonic agent create|list|status` manages configured agent profiles.
- `platonic gateway discord` runs the server-owned Discord connector.

## Configuration

Config resolution order:

1. `--config <path>`
2. `$PLATO_CONFIG`
3. `./plato.toml`
4. `~/.config/plato/config.toml`
5. built-in defaults

Auto-discovered `./plato.toml` cannot set `provider.api_key_env`,
`provider.base_url`, or any `[gateway]` table. Use `--config`, `$PLATO_CONFIG`,
or the user config for provider credentials, custom endpoints, and gateway
trust settings.

Leading `~` expands in explicit config paths. Relative explicit paths resolve
against the workspace root. Built-in defaults use OpenRouter:

```toml
[provider]
kind = "open_router"
model = "~openai/gpt-latest"
api_key_env = "OPENROUTER_API_KEY"
connect_timeout_ms = 30000
stream_idle_timeout_ms = 120000

[limits]
token_budget = 4000
max_output_tokens = 1024
max_turns = 8

[tools]
enabled = ["file.read", "file.list", "file.write", "file.edit", "shell.exec", "web.fetch"]
```

`thread.spawn` is available but not enabled by default. Add it only to a
coordinator's resolved toolset. The server-owned `limits.max_spawn_depth`
defaults to `1` and must be positive. At `platonic serve` startup, the bound is
resolved once from the user config (`~/.config/plato/config.toml`), then the
built-in default. Per-run
`--config`, `PLATO_CONFIG`, and workspace `plato.toml` do not configure the
host bound, and later file changes take effect only after a server restart.

OpenAI-compatible direct OpenAI config remains available:

```toml
[provider]
kind = "open_ai"
model = "gpt-5.5"
api_key_env = "OPENAI_API_KEY"
```

`connect_timeout_ms` bounds each socket connection and request write.
`stream_idle_timeout_ms` bounds each response read, so continued response
progress receives a fresh idle window. The legacy `timeout_ms` name remains an
alias for `stream_idle_timeout_ms`; setting both names is an error. Cancelable
runs poll for cancellation during stalled response-body reads on a fixed
25-millisecond interval without shortening that configured idle window.

A completion POST rejected with HTTP 429 before any response body or streaming
delta retries once and records the failed attempt plus repeated request at the
same turn and step. A finite, nonnegative numeric `Retry-After` of at most 30
seconds is honored; a missing or invalid value waits one second, and a value
over 30 seconds is not retried. Cancellation before the 429 is handled, and
cancellation during the retry wait prevents the second request event and POST.
Once the second request boundary is crossed, cancellation does not retract that
POST and is observed at the existing response boundary. Other provider failures
are not retried.

`file.read` and `file.list` are auto-allowed. `file.write`, `file.edit`, and
`shell.exec` require stdin approval and default to no. `web.fetch` also requires
explicit local approval for every URL and is never auto-approved by `--yolo`.
Its approval preview shows the normalized public HTTP(S) URL, origin, and
validated destination addresses. Each approved fetch is GET-only, disables
environment proxies and automatic redirects, revalidates and pins public DNS
answers for every same-origin hop, accepts supported UTF-8 text up to 1 MiB,
and returns at most 48 KiB. HTML is converted to plain text; response bodies
from errors are never returned. `shell.exec` runs from the workspace root with
a scrubbed child environment, no provider credentials, bounded stdout/stderr,
and a timeout. It uses `sh -c`; timeout or cancellation terminates the full
process tree.
In the TUI, a pending `shell.exec` can be allowed once or allowed for the
selected session until the daemon process exits. Later shell calls in that
session retain their approval policy and ledger facts but do not prompt again;
other sessions and restarted daemons prompt normally.
Use `--yolo` to auto-approve enabled workspace-write tools and exact
`shell.exec` calls that would otherwise prompt. Yolo mode does not enable
disabled or unknown tools, approve network or secret-access tools, permit any
other external side effect, auto-approve direct changes to root `PLATONIC.md`,
or bypass workspace path checks.

## Workspace Memory

If `<workspace-root>/PLATONIC.md` exists, Plato Agent snapshots its exact
contents once at run start and includes that snapshot in every model turn for
the run. Only that exact filename at the workspace root is recognized; aliases
such as `PLATO.md` and files below the root are ignored. A missing file leaves
provider requests unchanged.

`PLATONIC.md` must be a regular file containing valid UTF-8 and no more than
8,192 bytes. The final path component is opened without following symlinks,
and content is never trimmed. Workspace memory counts against the context
budget; older session turns may be dropped to make room, but the memory
snapshot is not shortened. Changes made after a run starts apply to the next
run, not later turns in the current run.

Direct `file.write` and `file.edit` calls to this reserved file always require
approval, including under `--yolo`. The complete proposed content is checked
against the same UTF-8 and 8,192-byte limits before either tool writes; failed
validation leaves an existing or absent target unchanged.

## Issue Preparation

Issue preparation is one fixed pipeline: prepare, refine, then model review.
Each stage writes its prompt, reads that file for the model request, records the
result, and writes a hash-bound validation record. Rust enforces response shape
and structural invariants; the final review is model-authored and is not
independent semantic proof. There are no tools, retries, loops, or configurable
nodes.

```bash
cat issue.md | cargo run --bin plato -- issue-prep start \
  .plato/issue-prep/issue-123
```

From the TUI, submit `/issue-prep <rough issue>`. The daemon runs the same
pipeline and returns the candidate or blocked reasons in the transcript.
An animated elapsed indicator remains visible while it runs. Artifacts are
written under `.plato/issue-prep/<run_id>/`.
The TUI keeps connection setup and `hello` on its three-second deadline, then
waits for the synchronous pipeline to finish under its existing provider bounds.

`start` requires a new run directory. A failed run remains unchanged; retry
with a different directory. A successful candidate is written to stdout and
`40-candidate.md`. A structurally blocked stage or model review with findings
exits nonzero and leaves its reasons in the stage validation file.

Every run uses this exact convention:

```text
00-manifest.json
01-input.md
10-prepare.prompt.md
11-prepare.result.json
12-prepare.validation.json
20-refine.prompt.md
21-refine.result.json
22-refine.validation.json
30-review.prompt.md
31-review.result.json
32-review.validation.json
40-candidate.md
```

## Workspace Ledgers

Every prompt executes through the host server, which is the only ledger writer.
New run events are append-only `RecordedEvent` envelopes in
`<workspace-ledger-dir>/runs/<run-id>.jsonl`; SQLite retains the session index
and other stateful tables. Default ledger directories are mode `0700`, and run
logs, the database, and SQLite sidecars are mode `0600`.

Replay is read-only and fully offline. It never starts or contacts the server,
makes a provider request, or executes a tool. Readback prefers a run JSONL file
when present and otherwise reads legacy `ledger_events` and `voice_events`
rows, so runs recorded before the JSONL transition remain available. Replay
shows durable final assistant messages rather than transient streaming deltas.

```bash
plato replay
plato replay --run run_123
plato replay events.jsonl
plato replay --db
plato replay --db=/tmp/platonic.db --run run_123
```

A one-shot prints live assistant text, its run id, ledger path, and replay hint
to stderr; stdout contains only the final answer. Server startup repairs an
unterminated JSONL tail and closes a run left active by interruption.

## Server

`platonic serve` is the local runtime for `plato`, `plato-tui`, the desktop
shell, and gateways. The
[platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
owns the architecture; this section covers operator behavior.

Start the one host server in the foreground:

```bash
platonic serve
```

From another terminal, register and inspect workspaces through that server:

```bash
platonic workspace create example /path/to/workspace
platonic workspace list
platonic workspace status <workspace-id>
platonic status --workspace /path/to/workspace
```

The server prints:

```text
daemon_scope: host
socket_path: <daemon-endpoint>
```

On supported platforms the endpoint is
`${XDG_RUNTIME_DIR:-<system-temp>/plato-agent-<uid>}/platonic/host/agent.sock`.
Every connection selects a registered workspace during `hello`. There is no
workspace-owned server or workspace-derived endpoint.

No directory becomes a workspace silently. An interactive local `plato`
one-shot or TUI asks once before creating an unknown workspace. Piped or
headless one-shots, `plato --remote`, gateways, desktop clients, explicit
`--socket` attachments, and `plato-tui --snapshot` fail with
`workspace_unregistered` and name `platonic workspace create` as the action.
Bare `plato`, `plato "question"`, and `plato --tui` ensure the installed
sibling server; standalone `plato-tui` and gateways never start it.

Agent profiles are immutable data bound to one workspace. Provider keys are
neither sent nor stored when a profile is created:

```bash
platonic agent create builder <workspace-id> --reasoning-effort high
platonic agent create reviewer <workspace-id> --model gpt-5.6-sol --tool file.read
platonic agent list
platonic agent status builder
```

Every admitted thread stores immutable authority before it becomes available.
A child may narrow its parent's paths, repositories, toolset, model, reasoning
effort, and approval policy, but cannot widen them. Each thread works in a
server-owned private-ref copy of at least one Git repository; its branch claim
lasts until stop, and the server never changes or deletes the user's repository.
The decision map owns the complete authority contract.

On Linux with Landlock support, thread children are write-confined to their
private repositories and scratch directory. macOS and Linux hosts without
Landlock record `confinement: "none"`. Set `[confinement] require = true` in
the user config and restart the server to refuse unconfined spawns. The typed
`thread.authority` protocol readback returns the complete immutable authority
and durable confinement fact. `plato thread status` intentionally retains the
protocol-v1 authority projection plus transient live state.

```bash
# Create a root thread after the stdin approval.
plato thread spawn --model gpt-5.6-sol --reasoning-effort xhigh

plato thread list
plato thread status <thread-id>
plato --remote <thread-id>
plato thread send <thread-id> --controller terminal-2 "inspect the workspace"
plato thread attach <thread-id> --from-offset 0
plato thread stop <thread-id>
```

`plato --remote` opens another TUI on the same durable thread. The lower-level
`thread attach` command prints ordered JSON events until interrupted. Any
number of clients may observe a thread, but exactly one controller owns an
active turn. Stopping a thread unloads it and releases its branch claim; it does
not delete its durable authority.

Server-wide state is under
`${XDG_STATE_HOME:-$HOME/.local/state}/platonic/server.db`. Each workspace
ledger is under
`${XDG_STATE_HOME:-$HOME/.local/state}/platonic/workspaces/<workspace-id>/ledger.db`.
The host endpoint and lock are current-user-only. Do not delete a live lock;
after shutdown the Unix lock file may remain, but it has no kernel lock owner.

Stop the host server only when it has no active work:

```bash
platonic shutdown --workspace /path/to/workspace
```

The command returns `refused_active` without changing an active server, or
`shutdown` before the endpoint closes.

## Local Dogfood Deployment

From a clean `develop` checkout, refresh the current-user binaries with:

```bash
./scripts/deploy-local.sh
```

The command fetches `origin/develop`, requires local `develop` to equal it,
builds the three locked release binaries, and installs them at
`~/.local/lib/plato-agent/{plato,platonic,plato-tui}-real`. Existing wrapper
scripts are not changed. It prints before/after checksums, gracefully retires
only an owner-validated idle installed daemon, and verifies a new isolated
daemon hello plus TUI snapshot before completing the atomic set replacement.

Dirty, detached, non-`develop`, ahead, behind, failed-build, incomplete-stage,
invalid-daemon, active-daemon, and failed-readback cases fail closed without
changing the installed set. The immediately previous complete set is retained
at `~/.local/lib/plato-agent.rollback`; restore all three binaries together
with:

```bash
./scripts/deploy-local.sh --rollback
```

Deployed hello and TUI identity is `version commit UTC-date`. Builds made
outside the deploy command report `unknown` provenance explicitly.

## Desktop (Development)

This Cargo workspace and the desktop package require Rust 1.88. `platonic-core`
remains on Rust 1.85. Desktop packages are development artifacts and are not
part of the Platonic 0.1.0 command-bundle release.

The desktop shell renders full typed session history, streams the selected run,
and supports new or continued messages, approval decisions, and cancel.
Provider credentials remain with the server. The desktop attaches to a
manually started host server.

![Plato Agent desktop showing an exact-run transcript](docs/images/desktop-plato-agent.png)

```bash
# Terminal 1, from the repository root
cargo run --bin platonic -- serve

# Terminal 2
cd desktop
npm ci
npm run tauri:dev
```

On first launch, choose the server workspace. The shell remembers its canonical
path as the next-launch seed and returns to the picker if that directory
disappears; each running shell keeps its own selected root. **New chat** clears
the selected session; otherwise the composer continues it. Switching chats does
not cancel their active runs. Closing the shell never stops the server or an
active run. Linux development requires the
[Tauri system dependencies](https://v2.tauri.app/start/prerequisites/#linux).

### macOS DMG (Protected Build)

The macOS desktop targets Apple Silicon and macOS 14 or newer. Its DMG contains
the same-revision, target-suffixed `platonic` sidecar and uses the same
attach-first, login-shell `PATH`, and detach-on-close lifecycle as the Linux
package.

Pull requests run credential-free native arm64 packaging validation and never
upload the unsigned DMG. The signed artifact path is only the protected/manual
or reusable `macOS Desktop DMG` workflow with `signed: true`. The protected
`macos-release` environment supplies the encrypted Developer ID identity and
notarization credentials. The job imports the identity into a disposable
keychain and removes all signing material on exit. Missing inputs fail before
the build; there is no unsigned distribution fallback. The workflow signs and
notarizes with Tauri, verifies the nested sidecar and app signatures, validates
the stapled DMG with Gatekeeper, and uploads the DMG plus its SHA-256 file as a
short-lived Actions artifact. It never creates a tag or GitHub Release. This is
a direct-download,
drag-to-Applications DMG only: it does not add a Mac App Store path, App Store
sandboxing or receipts, or a `.pkg` installer.

The protected job runs only on the `wikus` Apple Silicon proof host. Its signed
DMG must be installed there before the native workspace-selection, WKWebView,
bundled-daemon cold-spawn, deterministic-run, PATH-only `shell.exec`, and
shell-close-survival acceptance is recorded. The standard `macos-14` job is a
minimum-version compatibility gate and does not replace that release proof.

## Discord Gateway

The Discord gateway in `platonic-server` receives messages over an outbound WebSocket
and sends replies through Discord's REST API. Gateway routing maps Discord
channels to existing durable threads:

```toml
[gateway.discord]
api_key_env = "DISCORD_BOT_TOKEN"

[gateway.discord.channel_threads]
"111111111111111111" = "thread_123"
```

The entire `[gateway]` table is accepted only from `--config`, `PLATO_CONFIG`,
or the user config, not auto-discovered workspace `plato.toml`.
`channel_threads` must contain at least one positive numeric channel ID. It
selects context only; it does not authorize a Discord identity. Every mapped
thread must already exist in the selected workspace, and its immutable
authority remains daemon-owned.

Discord identity authority comes only from the canonical user config
`~/.config/plato/config.toml`:

```toml
[principals.discord."123456789"]
name = "jerome"
# remote_ceiling = "yolo" # optional, high-trust explicit grant
```

An omitted `remote_ceiling` is `prompt`. A prompting principal cannot control a
`yolo` thread; remote yolo is possible only with the explicit home-owned grant,
and child authority remains capped by the existing parent subset gate. A
workspace `plato.toml` cannot define `[principals]`. Neither `--config` nor
`PLATO_CONFIG` supplies principal authority, even when it selects gateway
routing. Unknown identities are denied before channel lookup, content scanning,
daemon access, session lookup, Discord response work, or effects. An admitted
identity in an unmapped channel is ignored at the separate context gate.

With the host daemon already running, start the gateway from the private
environment that loaded the bot token from
`$HOME/.config/plato/discord-bot-token` outside terminal or pane input. The
token literal never belongs in `argv`, pane text, logs, GitHub, or chat:

```bash
unset OPENAI_API_KEY OPENROUTER_API_KEY
platonic gateway discord --config ~/.config/plato/gateway.toml
```

With no `--socket`, the gateway attaches to the host endpoint. An explicit
socket remains a test/operator override.

The server-owned gateway completes a bounded daemon `hello`, requires the exact
workspace ID plus `hello`, `thread.authority`, `thread.status`, `thread.send`,
`thread.events`, and `approval.decide`, and validates every mapped thread before
beginning Discord REST and WebSocket work. Missing or empty principal maps and
bot tokens fail at startup with an actionable diagnostic. Stale bot credentials
fail the first authenticated Discord lookup, while mismatched application IDs
and missing interaction tokens fail closed at ingress. A failed probe starts no
gateway; the gateway never starts a server with its Discord environment.

At startup, the gateway replaces the Discord application's global command
registry with the commands this binary supports. The current registry contains
`/status`, `/approve`, and `/deny`. For an admitted principal, `/status` reports
the mapped durable thread and its authority. `/approve` and `/deny` resolve only
that channel's exact pending run and tool-call identifiers. The named principal
is recorded as attribution in the existing tool and coordinator-spawn approval
chain; the actor field is not an independent grant of authority.

Enable the bot's Message Content intent. Grant View Channel, Send Messages, Add
Reactions, and Read Message History; also grant Send Messages in Threads when
using threads. Messages from unknown identities are ignored. For admitted messages,
the gateway adds 👀, refreshes Discord's typing indicator while the run is
active, then replaces 👀 with ✅ or ❌. Canceled and interrupted runs remove 👀
without a terminal reaction. Each channel or DM drives its mapped durable
thread; final answers are recovered from typed thread events after daemon reconnects.
Approval-required runs post one bounded notification with the tool, effect, and
preview; `/approve` or `/deny` resolves that exact operation. Failed runs post
`Run failed. Inspect it locally with: plato replay`; canceled and interrupted
runs stay silent.
A Discord response-delivery failure is contained to that message, and the
gateway continues processing subsequent messages. A definitely rejected HTTP
429 with a valid `Retry-After` of at most 30 seconds waits the full delay and
retries that message chunk once; transport failures and HTTP 5xx responses are
not retried.

Admitted-principal messages over 4,096 UTF-8 bytes or matching the fixed unsafe-input
markers are rejected before daemon access with `Message rejected: unsafe or
oversized Discord input.` Authentication and authorization do not make message
text trusted: accepted messages are still untrusted content and are forwarded
unchanged only after the existing ingress scan.

## TUI

Bare `plato` in a terminal is the interactive local entrypoint; `plato --tui`
is its explicit equivalent. It ensures the host daemon, obtains the root thread
spawn decision, and attaches to that durable thread. `plato --remote
<thread-id>` attaches a second TUI without creating another thread. Exiting a
TUI leaves the host daemon and authority record available. Standalone
`plato-tui` attaches to the same host endpoint but never starts it; an explicit
`--socket` remains available for focused endpoint proofs.
It renders a conversation-first transcript with distinct `You` and `Plato`
messages, at most one subtle trace summary per run, one status row, a composer,
session picker, and a bounded approval pane above the composer. Press `v` from
an empty composer to toggle
the complete ordered audit view without reloading the session.
Assistant messages render headings, emphasis, lists, quotes, inline code,
fenced code, and unified diffs in conversation view. User messages remain
literal, while audit view retains the exact stored transcript source.
The multiline composer uses the terminal cursor without adding a caret glyph to
its text. Bracketed paste inserts literal text as one undoable edit.
Slash-command suggestions use case-insensitive subsequence matching while
retaining the five-row popup.
Committed conversation rows use the terminal's native scrollback, so wheel
scrolling, text selection, search, and transcript retention after exit work as
they do for ordinary terminal output. Audit, session, help, status, and approval
overlays temporarily use the alternate screen and restore the conversation when
they close. On resize, the terminal reflows rows already in scrollback while the
TUI redraws only the live tail, composer, footer, and active overlay; committed
rows are not duplicated at the new width.
A nonempty `NO_COLOR` suppresses colors while retaining emphasis and layout.
Otherwise, the TUI detects true color or xterm-256 support and makes one
best-effort 100-millisecond OSC 11 background query at startup. User-message
tint, accents, and semantic colors adapt to light or dark terminal backgrounds;
16-color and unknown terminals keep default colors with dim chrome.

```bash
cargo run --bin plato
```

`plato-tui` remains a terminal client for a manually started `platonic serve`. It
does not spawn, supervise, restart, or stop the daemon, and it does not call
providers, execute tools, or write the workspace ledger directly.
Assistant text appears live through daemon `events.stream`; replay remains
based on final ledger messages.
Live Markdown drains at a pressure-adaptive cadence, flushes quiet partial text
promptly, holds incomplete tables, and consolidates to the exact raw assistant
source before transcript reload and resize.
Session picker statuses are `running`, `finished`, `failed`, `canceled`, or
`interrupted`; `interrupted` means a daemon restart closed a previously running
session so it can be resumed. Picker rows show that status, a compact relative
age, and a bounded preview of the session's first question. Raw session IDs stay
hidden in normal rows while remaining available for exact resume and recovery.
On attach, the TUI selects the latest session by default; submitted messages
continue that session until `/new` clears the selection.
Live rows, model status, warnings, and approvals remain bound to that selected
session and run across reloads. A pending approval is restored from daemon
readback. An accepted grant or deny immediately resolves its conversation trace
to approval, while a failed decision remains available to retry.
While a provider response is pending, the status row labels the selected model
or alias. After the response is durable, it labels the provider-reported served
model, or `served unknown` when the provider omits that identity.
Use `/status` for one authoritative daemon readback of the effective model,
daemon identity, selected session, reported token usage, persisted approval
facts, and the selected session's live shell grant and approval profile. The
read-only modal does not invoke a model or change the session.

`plato --tui --yolo` starts the local TUI thread in yolo mode. Within the TUI,
`/yolo on` and `/yolo off` change only the selected session until the daemon
exits. Before a session exists, the command selects the profile for the next
fresh run. A persistent footer warning distinguishes current-session yolo from
next-session yolo. Auto-grants retain the normal require-approval policy fact
and record actor `tui_yolo`.

```bash
cargo run --bin platonic -- serve
cargo run --bin plato-tui -- --workspace "$PWD"
```

Use `--socket <path>` when connecting to a non-default socket, `--config <path>`
to pass a config file to daemon-started runs, and `--run <run_id>` to open a
specific transcript. `--voice-config <path>` selects a separate client-only
file with one strict `[voice]` table; the server never receives or parses it.
The table requires explicit `kokoro_model`, `whisper_model`, and `silero_model`
paths and optionally accepts exact `capture_device` and `playback_device` IDs.
No model or device discovery or download occurs.

Keys:

- `Enter`: submit the composer to the daemon. A session can have only one
  active run.
- `Tab`: queue the composer while a run is active.
- `Shift-Enter`, `Alt-Enter`, `Ctrl-J`, or `Ctrl-M`: insert a newline.
- `Shift` plus an arrow, `Home`, or `End`: select text; typing replaces the
  selection. `Alt-B`/`Alt-F` and `Ctrl-Left`/`Ctrl-Right` move by word.
- `Ctrl-Z` / `Ctrl-R`: undo / redo composer edits. `Ctrl-K`, `Ctrl-U`,
  `Ctrl-W`, and `Ctrl-Y` retain the existing kill/yank bindings.
- `v` (with an empty composer): toggle conversation and audit views. A `v`
  typed into a nonempty composer remains input.
- `/sessions`: open the session picker. Type a case-insensitive subsequence of a
  first-question label or a raw session ID for recovery (`q` is text);
  `Backspace` edits; `Up`/`Down`
  and `Ctrl-P`/`Ctrl-N` wrap through matches; `Enter` resumes the focused match;
  `Esc` closes. With no matches, `Enter` keeps the picker open.
- `/status`: request one authoritative runtime readback; `Esc` closes the
  read-only modal.
- `/yolo on|off`: change the selected session's daemon-lifetime approval
  profile, or the next fresh run's profile when no session is selected.
- `/voice on|off`: grant or revoke local audio for this client session. Voice
  starts off, invalid configuration fails closed, and off drains and closes the
  current devices idempotently.
- `/new`: turn voice off and clear the selected session so the next submitted
  message starts fresh.
- `/issue-prep <rough issue>`: prepare and review an implementation issue.
  It is unavailable while another run or issue-prep command is active, and the
  TUI waits for it before exiting.
- `g` / `d`: allow once or deny the focused approval request.
- `s`: allow the focused `shell.exec` request and later shell calls in the
  selected session until the daemon exits. This action is hidden for other tools.
- `Up` / `Down` and `PageUp` / `PageDown`: scroll the focused audit or approval
  overlay.
- `Ctrl-C`: request `run.cancel` for the active run; a second `Ctrl-C` exits the
  TUI. Exiting the TUI does not stop the daemon.
- `r`: reconnect and reload daemon state.
- `q` (with an empty composer) or `Esc`: exit the TUI from the main view; the
  session picker uses them as described above.

## Commands

```bash
cargo run --bin plato
cargo run --bin plato -- "read README.md and summarize it"
cargo run --bin plato -- -c "what did you just summarize?"
cargo run --bin plato -- --yolo "write local-proof.txt with hello from Plato Agent"
cargo run --bin plato -- "run cargo test --locked and summarize the result"
cargo run --bin platonic -- serve
cargo run --bin platonic -- status
cargo run --bin platonic -- workspace create example /path/to/workspace
cargo run --bin platonic -- workspace list
cargo run --bin platonic -- workspace status workspace_123
cargo run --bin platonic -- shutdown
cargo run --bin plato -- thread spawn --model gpt-5.6-sol --reasoning-effort xhigh
cargo run --bin plato -- thread list
cargo run --bin plato -- thread status thread_123
cargo run --bin plato -- thread send thread_123 --controller terminal_a "inspect the workspace"
cargo run --bin plato -- thread attach thread_123 --from-offset 0
cargo run --bin platonic -- gateway discord --config ~/.config/plato/gateway.toml
cargo run --bin plato -- replay
cargo run --bin plato -- replay events.jsonl
cargo run --bin plato -- replay --db
cargo run --bin plato -- replay --db=/tmp/plato-agent.db --run run_123
cargo run --bin plato -- --tui --config plato.toml
cargo run --bin plato-tui -- --workspace "$PWD"
```

## Dogfood Recipe

```bash
tmp=$(mktemp -d)
cat > "$tmp/plato.toml" <<'TOML'
[provider]
kind = "open_router"
model = "~openai/gpt-latest"
api_key_env = "OPENROUTER_API_KEY"
http_referer = "https://referential.ai"
app_title = "Plato Agent"

[limits]
token_budget = 4000
max_output_tokens = 512
max_turns = 8

[tools]
enabled = ["file.read", "file.list", "file.write", "file.edit"]
TOML

OPENROUTER_API_KEY="$(cat /path/to/your/openrouter-key)" \
  cargo run --bin plato -- --config "$tmp/plato.toml" \
  "list the files in this workspace and summarize what you see"
```

## Boundary

`platonic-core` remains pure. Platonic owns provider calls, tools, approvals,
ledgers, policy, and gateways. Plato Agent owns the client commands and TUI.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE) ([official text](https://www.apache.org/licenses/LICENSE-2.0))
- [MIT License](LICENSE-MIT) ([official text](https://opensource.org/licenses/MIT))

at your option.
