# Plato Agent

The reference agent runtime for the Platonic framework.

**Platonic**

*by Referential.ai*

Plato Agent is the named application built on the Platonic framework. It shows
its work: every step is recorded, replayable, and auditable.

The workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms.

**New here? Start with [docs/QUICKSTART.md](docs/QUICKSTART.md) — build, run, and test in five minutes.**

The bootstrap surface is intentionally small:

- Bare `plato` in a terminal ensures the host daemon, asks once before registering an unknown directory, creates an approved durable thread, and opens the TUI on it.
- `plato --remote <thread-id>` opens another TUI on the same host socket and existing thread.
- `plato "question"` ensures the host server and runs as a short-lived client.
- `plato -c "follow-up"` continues the latest workspace session from the workspace ledger.
- `plato replay <file>` validates and prints a deterministic JSONL readback without network calls or tool execution.
- `plato replay [--run <id>]` replays the default workspace ledger; omitted `--run` selects the latest session.
- `plato replay --db[=<path>] [--run <id>]` selects an explicit workspace state database and replays its run log.
- `plato issue-prep start <run-dir>` runs the fixed issue preparation pipeline from Markdown on stdin.
- `plato thread spawn|list|status|send|attach|stop` manages and observes durable threads on a serving host daemon.
- `platonic serve|status|shutdown` runs and operates the server.
- `platonic workspace create|list|status` manages registered workspaces.
- `platonic agent create|list|status` manages configured agent profiles.
- `platonic gateway discord` runs the server-owned Discord connector.

## Configuration

Config resolution order:

1. `--config <path>`
2. `$PLATO_CONFIG`
3. `./plato.toml`
4. `~/.config/plato/config.toml` on Unix or `%APPDATA%\plato\config.toml` on Windows
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
resolved once from the user config (`~/.config/plato/config.toml` on Unix or
`%APPDATA%\plato\config.toml` on Windows), then the built-in default. Per-run
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
and a timeout. It uses `sh -c` on Unix and
`cmd.exe /C` on Windows; timeout or cancellation terminates the full process tree.
In the TUI, a pending `shell.exec` can be allowed once or allowed for the
selected session until the daemon process exits. Later shell calls in that
session retain their approval policy and ledger facts but do not prompt again;
other sessions and restarted daemons prompt normally.
Use `--yolo` to auto-approve enabled workspace-write tools that would otherwise
prompt. Yolo mode does not enable disabled or unknown tools, approve network
tools, permit deny-class effects such as external side effects or secret access,
approve `shell.exec`, auto-approve direct changes to root `PLATONIC.md`, or
bypass workspace path checks.

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

- Bare `plato "..."` writes to the default platform user-state path.
- `plato -c "..."` continues the latest session from that store.
- `--db[=<path>]` belongs to offline replay; one-shot runs always use the server-owned workspace ledger.
- New run events are append-only `RecordedEvent` envelopes in
  `<workspace-ledger-dir>/runs/<run-id>.jsonl`. SQLite retains the session
  index and other queryable state.
- On Unix, default ledger directories are `0700`; run logs, the state database,
  and SQLite sidecars are `0600`.
- Live assistant text, `run_id`, `ledger_path`, and replay hints print to stderr. Stdout remains only the final answer.
- Replay shows final assistant messages, not partial streaming deltas.
- Replay renders dropped oldest session turns as `[<turn_id>] context_compacted estimated_tokens=<before>-><after> dropped_turns=<start>..<end>`; the zero-based range has an exclusive end and the token values are host estimates of the complete context before and after compaction.
- Ledger, approval, replay, and typed-transcript tool call ids are host-minted per run; provider ids remain provider-facing.
- Streamed runs request provider usage chunks. Usage is recorded only when the
  provider reports both token counts; reported zeros remain known, while
  omitted or partial usage is recorded as unknown.
- `plato replay` without arguments replays the latest session from the default workspace ledger.
- `plato replay --run <id>` replays a single run.
- A JSONL record is acknowledged only after the complete serialized envelope,
  its newline commit marker, a flush, and `sync_data`. Write-open validates the
  committed prefix and truncates an unterminated tail before appending. Readers
  never expose a malformed tail. An already-open `tail -f` sees each record
  after it commits.
- Voice companion envelopes append as one synced batch in the same per-run
  JSONL. New runs do not write `ledger_events` or `voice_events` rows.
- Read-only SQLite replay reads `user_version` first: schema v1 uses only
  `ledger_events`, v2 adds sessions, v3 adds voice companions, v4 adds
  immutable thread authority, and v5 adds immutable thread-stop records. Newer
  schemas fail without migration. Write-open
  remains the sole migration path to the current schema.
- Every prompt runs through the host server. Replay opens JSONL or SQLite
  read-only and never starts or contacts the server.
- Runs recorded before the JSONL transition remain readable from
  `ledger_events` and `voice_events`. Readback prefers a run JSONL when present
  and otherwise uses those legacy rows.
- The server syncs a JSONL terminal event before updating its SQLite session
  outcome. Daemon startup repairs a torn tail, reconciles an already committed
  terminal event, or appends one interruption failure before closing a run
  still marked running.

Replay forms:

```bash
cargo run --bin plato -- replay
cargo run --bin plato -- replay --db
cargo run --bin plato -- replay --db=/tmp/plato-agent.db
cargo run --bin plato -- replay --db=/tmp/plato-agent.db --run run_123
```

## Server

`platonic serve` is the local runtime for clients such as `plato`,
`plato-tui`, and the desktop shell. The runtime topology and verb set are defined in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#runtime-topology) and issue
[#11](https://github.com/referential-ai/plato-agent/issues/11).

Start the host server in the foreground:

```bash
platonic serve
```

Attaching never registers a directory implicitly. A local `plato` one-shot or
TUI with terminal stdin, stdout, and stderr asks once for a workspace name,
defaulting to the directory basename; pressing Enter creates it. Declining or
EOF leaves it unregistered. Piped or otherwise headless one-shots,
`plato --remote`, gateways, and desktop clients never ask or create: they fail
with `workspace_unregistered` and name `platonic workspace create` as the
operator action. Standalone `plato-tui` asks only on its default local endpoint;
an explicit `--socket` attachment and `--snapshot` never ask or create.

For scripts and services, register deliberately before attaching:

```bash
platonic workspace create example /path/to/workspace
```

The explicit legacy workspace mode remains available for standalone clients
and focused proofs, but it has the same create-before-use gate. Start it, then
use the printed socket path for the control request before attaching:

```bash
platonic serve --workspace "$PWD"
platonic workspace create example "$PWD" --socket <printed-socket-path>
```

Host mode uses `${XDG_RUNTIME_DIR:-<system-temp>/plato-agent-<uid>}/platonic/host/agent.sock`
on Unix or `\\.\pipe\plato-agent-host` on Windows. Each connection selects its
workspace through the existing `hello` request; the response adds
`"daemon_scope":"host"` while retaining the existing build-provenance
`daemon_version`. Bare `plato`, `plato "question"`, and `plato --tui` ensure
this server and attach as clients. `platonic status`, `platonic shutdown`, and
the `platonic workspace` commands operate it through the existing protocol.

Agent profiles are immutable data hard-bound to one registered, present
workspace. Creation resolves model and tool defaults through the normal config
order, with explicit CLI overrides, and refuses a missing configured provider
key environment variable with the exact env/config action. The record contains
only the agent id, workspace id, model, reasoning effort, approval policy,
validated internal tool names, and creation time; provider keys are never sent
or stored.

```bash
platonic agent create builder <workspace-id> --reasoning-effort high
platonic agent create reviewer <workspace-id> --model gpt-5.6-sol --tool file.read
platonic agent list
platonic agent status builder
```

The `plato thread` commands connect only to this host endpoint. Spawn requires
an explicit cwd, model, reasoning effort, and approval policy; cwd defaults to
the current directory and policy defaults to `prompt`. A root spawn prompts on
stdin. A child spawn is evaluated by its loaded parent's immutable policy:
`prompt` asks for a decision and `yolo` auto-grants the workspace-write spawn
effect. A child cwd must remain within its parent's granted paths, its toolset
must be a subset of the parent's, and its policy can never be more permissive.
Every final approval decision and actor is stored in the server store.

A coordinator with `thread.spawn` in its immutable resolved toolset can ask the
model to dispatch a configured agent by id, with optional narrowing overrides
for model, reasoning effort, approval policy, and toolset. The proposal has the
`WorkspaceWrite` effect and traverses the normal tool policy and approval gate.
After approval, the server reuses the same durable spawn admission path as the
typed client command; the tool result reports the durable worker thread id and
is wrapped as untrusted provider input. Target-agent defaults and the parent's
toolset, policy, cwd/path grants, network grant, and server spawn-depth bound
are all ceilings. An attempted expansion returns a typed
`thread_authority_exceeded` tool result and creates no child authority.

A grant atomically stores the twelve immutable authority fields before the
thread becomes loaded. Every admitted spawn works in one or more private-ref
repositories. A spawn can name workspace-relative Git repositories and
existing branches; an empty list must infer the repository that contains
`cwd`, or the spawn is rejected before admission. The server claims each
`(workspace, repository, branch)` before creating a private-ref repository
beneath
`$XDG_STATE_HOME/platonic/worktrees/<thread-id>/`. Each private repository has
its own refs and index, reads objects through an alternate to the server-owned
shared Git store, and disables automatic GC. Omitting a branch creates the
fresh `thread/<thread-id>` branch from the source HEAD. A second live claim for
the same branch fails with `thread_branch_claim_conflict`.

On hosts with Landlock support, every newly admitted thread child is
write-confined to its private repositories and scratch directory. Unsupported
hosts record `confinement: "none"`; set `[confinement] require = true` in the
user config to refuse those spawns. `thread.authority` reports the immutable
confinement fact alongside the repository, branch, and path authority. On
stop, or during startup reconciliation after a crash, the server fetches the
claimed branch into shared storage and removes only its owned private
repository and claim. It never changes or deletes the user's repository.

New rows leave the legacy `cwd` column null. Migrated eight-field rows remain
enumerable without backfill: agent, toolset, and worktrees default absent or
empty, the recorded cwd becomes one writable granted path, and network
defaults denied. Denial, cancellation, or persistence failure creates no
thread authority.

Protocol-v1 `thread.list` and `thread.status` keep their original eight-field
authority projection, including `cwd`, so compiled v1 clients continue to
decode those responses byte-for-byte. New rows derive that compatibility cwd
from the first recorded worktree or granted-path root; migrated rows retain
their stored cwd. Daemons advertise `thread.authority` for a separate typed
readback of the complete twelve-field immutable record.

```bash
# Terminal 1
plato
# approve the root thread.spawn prompt, then use the TUI normally

# Terminal 2
plato thread list
plato thread status <thread-id>

# Terminal 3: attach another interactive observer/controller
plato --remote <thread-id>

# The lower-level typed client remains available for scripting and proof
plato thread send <thread-id> --controller terminal-2 "inspect the workspace"
plato thread send <thread-id> --controller terminal-2 \
  --turn <thread-turn-id> "also summarize the findings"
plato thread attach <thread-id>
plato thread stop <thread-id>
```

Spawn and status print one typed JSON object. List prints one object per durable
thread. Each joins its immutable `authority` record with transient `live`
fields (`loaded` and `current_turn_id`); liveness is never persisted. Restarted,
clientless, and orphaned threads therefore remain enumerable with
`loaded: false`.

Send also prints a typed JSON receipt. An idle thread returns `started` with a
daemon-minted turn id. The same controller can supply that exact id with
`--turn` to queue a continuation and receive `steered`; another controller is
typed-rejected for the entire turn. Accepted continuations keep the same turn
id until their queue drains and the final run is terminal. Controller ownership
is daemon-live state and is never added to the immutable authority record.
Each TUI uses a distinct controller identity. A remote TUI observes the same
live events, can steer a turn it owns, receives the typed controller-owned
refusal while another client owns that turn, and can take the next idle turn.
Attaching never creates another authority record or registry.

Attach prints one JSON event per line until interrupted. Any number of attach
clients can read the same ordered thread-local offsets without becoming
controllers. Omit `--from-offset` to start at the retained tip, or use
`--from-offset 0` for a late attach that should replay everything still in the
bounded live buffer. A lagged offset fails explicitly rather than skipping
events; retained events and observer subscriptions are not persisted.

The transient `live` fields also include monotone `last_activity_at_ms` while
the thread is loaded; live activity is never copied into immutable authority
storage. `thread stop` is the only management mutation: it records the
requesting actor after the supervised child process tree reaches zero
residuals, then unloads the thread. There is no pause or live approval-policy
edit; changing authority requires stopping and spawning a new thread through
the approval gate.

On startup it prints:

```text
workspace_id: <workspace-id>
socket_path: <daemon-endpoint>
ledger_path: <state-path>/ledger.db
```

Default ledger paths are keyed by the server-minted workspace id stored in the
registry. Moving a workspace updates its registry root without changing that
id or its history:

- Unix ledger: `${XDG_STATE_HOME:-$HOME/.local/state}/platonic/workspaces/<workspace-id>/ledger.db`
- Windows ledger: `%LOCALAPPDATA%\platonic\workspaces\<workspace-id>\ledger.db`

On first attach, a registered workspace that still points at the legacy
path-derived `agent.db` is moved to this layout and its registry row is updated.

Explicit legacy daemon endpoints remain keyed by the path-derived workspace id
during migration:

- Unix socket: `${XDG_RUNTIME_DIR:-<system-temp>/plato-agent-<uid>}/platonic/workspaces/<workspace-id>/agent.sock`
- Unix lock: `${XDG_RUNTIME_DIR:-<system-temp>/plato-agent-<uid>}/platonic/workspaces/<workspace-id>/agent.lock`
- Windows pipe: `\\.\pipe\plato-agent-<workspace-id>`
- Windows lock: `%LOCALAPPDATA%\platonic\workspaces\<workspace-id>\agent.lock`

Interactive `plato` uses the host endpoint above.
One-shot `plato "question"` auto-ensures the host server and always uses its
server-owned workspace ledger.

Runtime directories are restricted to `0700` and the daemon socket to `0600`.
A custom Unix `--socket` parent is restricted to `0700` at startup. Windows
pipe and lock ACLs grant access only to the current user and reject remote pipe
clients. Windows clients limit server impersonation to identity inspection,
authenticate the server's user before sending protocol bytes, and bound
busy-pipe connection waits.

The daemon holds the workspace lock while it is active. On Unix, the lock is a
persistent current-user `0600` regular file guarded by a nonblocking exclusive
kernel advisory lock. Startup validates the file without following symlinks,
then rewrites its diagnostic metadata only after acquiring the kernel lock.
Normal and abrupt exits release the kernel lock but leave the file in place, so
lock probes use kernel ownership rather than path existence. SIGINT and SIGTERM
on Unix, and Ctrl-C or Ctrl-Break on Windows, trigger a graceful shutdown: the
daemon stops accepting new connections, then removes its endpoint before
exiting. On Windows the daemon creates the exact lock file with delete-on-close
and holds its handle for the daemon lifetime, so normal or abrupt exit removes
the path. Do not remove a lock for a live daemon. Ordinary Windows daemon
startup may wait up to five seconds for a valid installer or update gate
owner to release or abandon the gate. Invalid ownership or a timeout causes
startup to fail closed before the daemon creates its endpoint or workspace lock.
Live assistant deltas are transient `events.stream` events and are not written
to the ledger. After a `lagged` response, omitting `from_offset` resumes at the
current tip; `transcript.read` returns ledger-backed status and final answer.
The collector drains every queued run event into the contiguous-offset buffer
before `finished`, `failed`, or `canceled` can be observed.
An accepted `run.cancel` stores `cancel_requested` before replying. Repeated
requests return that state without another cancellation event, while terminal
runs reject cancellation.
Each daemon run executes in a supervised child process while `platonic serve`
stays alive and authoritative. The daemon is the only workspace-ledger writer;
the child receives a prepared run snapshot without a ledger path and returns
typed ledger operations, live deltas, approval requests, and its result over
private stdio.
Every child has an explicit 30-minute deadline. Cancellation or deadline expiry
first sends the child cancellation token, then terminates its complete process
tree after a bounded grace period, escalates to a kill when necessary, drains
output for a bounded interval, and verifies that no child processes remain.
The daemon retains event buffers for the newest 32 terminal runs in completion
order. After eviction, `events.stream` replays durable ledger records from the
run JSONL or legacy SQLite rows; transient deltas and approval notifications
remain live-only. `transcript.read` and `sessions.list` remain ledger-backed.
`hello` advertises `transcript.read.typed`. Successful `transcript.read`
responses preserve the legacy `transcript` string and add ordered `typed.runs`
with structured chat, tool, policy, and approval entries.
`hello` also advertises `transcript.read.pending_approval`; while a run is
paused, its transcript response includes the complete pending approval and
omits it immediately after a decision or cancellation.
`hello` advertises `daemon.shutdown_if_idle` for graceful control. The request
omits `params` (an empty object is also accepted). It returns `refused_active`
without changing the daemon while a run or approval is active; otherwise it
closes run admission, returns `shutdown`, then exits and removes its socket and
lock. Duplicate shutdown and run-admission requests dispatched before teardown
fail with `daemon_shutting_down`; after the `shutdown` response, connection
close is expected and lock removal confirms success.

The product command exposes the protocol-backed operator surface directly:

```bash
platonic status --workspace "$PWD"
platonic workspace create example /path/to/workspace
platonic workspace list
platonic workspace status <workspace-id>
platonic shutdown --workspace "$PWD"
```

The commands emit one typed JSON result. Shutdown reports `refused_active`
without changing an active server, or `shutdown` before graceful process exit.

Minimal NDJSON-over-Unix-socket check, using the `workspace_id` and
`socket_path` printed by the daemon:

```bash
WORKSPACE_ROOT="$PWD" \
WORKSPACE_ID="<workspace-id>" \
SOCKET_PATH="<socket-path>" \
python3 - <<'PY'
import json
import os
import socket

def send(file, request):
    file.write(json.dumps(request) + "\n")
    file.flush()
    print(file.readline(), end="")

with socket.socket(socket.AF_UNIX) as sock:
    sock.connect(os.environ["SOCKET_PATH"])
    file = sock.makefile("rw")
    send(file, {
        "v": 1,
        "id": "hello_1",
        "kind": "request",
        "method": "hello",
        "params": {
            "workspace_root": os.environ["WORKSPACE_ROOT"],
            "workspace_id": os.environ["WORKSPACE_ID"],
        },
    })
    send(file, {
        "v": 1,
        "id": "sessions_1",
        "kind": "request",
        "method": "sessions.list",
    })
PY
```

NDJSON `run.start` and `message.append` default to `wait: false`, returning a
`running` response immediately. Send `"wait": true` only when the connection can
block until the run finishes.
The TUI, desktop shell, one-shot CLI client, and Discord gateway bound
daemon connects and each complete request to three seconds. The desktop uses a
fresh budget for hello and every normal read or mutation.

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

The Plato Agent workspace and desktop package require Rust 1.88. Platonic Core
remains on Rust 1.85.

The desktop shell renders full typed session history, streams the selected run,
and supports new or continued messages, approval decisions, and cancel.
Provider credentials remain with the daemon. Linux development attaches to a
manually started daemon. On Windows, the shell first attaches to a valid daemon
for the selected workspace; when none is listening, it starts the absolute
sibling `platonic.exe` sidecar and retries for a bounded interval.

![Plato Agent desktop showing an exact-run transcript](docs/images/desktop-plato-agent.png)

```bash
# Terminal 1, from the repository root
cargo run --bin platonic -- serve

# Terminal 2
cd desktop
npm ci
npm run tauri:dev
```

On first launch, choose the daemon workspace. The shell remembers its canonical
path as the next-launch seed and returns to the picker if that directory
disappears; each running shell keeps its own selected root. **New chat** clears
the selected session; otherwise the composer continues it. Switching chats does
not cancel their active runs. Closing the Windows shell never stops a daemon or
run. The ready shell checks daemon health without restarting it; a child crash
shows the disconnected screen, and only **Reconnect** attempts one new start.
The shell never removes a daemon lock, and reports the endpoint and lock paths
when startup remains blocked. Only one Windows shell may own a workspace at a
time. Linux development requires the
[Tauri system dependencies](https://v2.tauri.app/start/prerequisites/#linux).

### Windows Installer (Unsigned Development)

Phase A targets x64 Windows 10 22H2 or newer. Build the per-user NSIS installer
on Windows:

```powershell
cd desktop
npm ci
npm run tauri:bundle:windows
```

The installer retains its technical `Plato` identity under `%LOCALAPPDATA%`,
bundles the same-revision `platonic.exe`, and downloads the WebView2
Evergreen bootstrapper when the runtime is absent. Upgrade and uninstall first
close the desktop, block new installed-sidecar starts, and make one bounded
`platonic shutdown` invocation. An active server aborts before installed
binaries or user files change; idle servers exit cleanly. These unsigned artifacts are for
development proof only and are not distributed.

### Linux AppImage (Private Release)

Linux releases target x86-64 Ubuntu 24.04 on the WebKitGTK 4.1 ABI. The
AppImage contains the same-revision `platonic` sidecar. It first attaches
to a valid workspace daemon; if none is available, it restores only the user's
login-shell `PATH`, starts the bundled sidecar, and retries for a bounded
interval. Closing Plato Agent detaches without stopping the daemon or active runs.
Startup failures report the sidecar, socket, and lock paths and never delete a
lock or fall back to a system daemon.

Authenticated private-release download and integrity check:

```bash
gh auth status
gh release download plato-desktop-v0.1.0 \
  --repo referential-ai/platonic \
  --pattern 'Plato-*-x86_64.AppImage*'
sha256sum --check Plato-*-x86_64.AppImage.sha256
chmod +x Plato-*-x86_64.AppImage
./Plato-*-x86_64.AppImage
```

Ubuntu 24.04 needs its WebKitGTK 4.1 runtime and `libfuse2t64`; Rust, Node,
and development packages are not runtime dependencies. These private artifacts
are not a public community launch. Build the AppImage on Ubuntu 24.04 with
`npm run tauri:bundle:linux -- --ci -- --locked` from `desktop/`.

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

With the host daemon already running, start the gateway in an environment
that contains the bot token but no provider credentials:

```bash
unset OPENAI_API_KEY OPENROUTER_API_KEY
export DISCORD_BOT_TOKEN="$(cat /path/to/discord-bot-token)"
platonic gateway discord --config ~/.config/plato/gateway.toml
```

With no `--socket`, the gateway attaches to the host endpoint. An explicit
socket remains a test/operator override during the endpoint migration.

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
TUI leaves the host daemon and authority record available. The standalone
`plato-tui` binary remains the explicit legacy workspace-daemon client during
this migration stage.
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
facts, and the selected session's live shell grant. The read-only modal does
not invoke a model or change the session.

```bash
cargo run --bin platonic -- serve --workspace "$PWD"
cargo run --bin plato-tui -- --workspace "$PWD"
```

Use `--socket <path>` when connecting to a non-default socket, `--config <path>`
to pass a config file to daemon-started runs, and `--run <run_id>` to open a
specific transcript.

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
- `/new`: clear the selected session so the next submitted message starts fresh.
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
http_referer = "https://example.invalid"
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

`platonic-core` remains pure. Provider calls, local tools, approval prompts, ledger files, SQLite, daemon runtime, TUI, and connectors belong in this repo.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE) ([official text](https://www.apache.org/licenses/LICENSE-2.0))
- [MIT License](LICENSE-MIT) ([official text](https://opensource.org/licenses/MIT))

at your option.
