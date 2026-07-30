# Quickstart — run and test Plato Agent

Everything below is copy-pasteable. Companion docs: [`../README.md`](../README.md) (full reference), [`ARCHITECTURE.md`](ARCHITECTURE.md) (topology and law).

## 0. One-time setup

```bash
cd ~/projects/platonic-workspace/plato-agent
cargo build --locked                      # builds all binaries
export OPENROUTER_API_KEY="$(cat /path/to/your/openrouter-key)"
export PATH="$PWD/target/debug:$PATH"     # so the binaries just work in this shell
```

`plato` works without a local config when `OPENROUTER_API_KEY` is exported.
Config is discovered in this order: `--config`, `PLATO_CONFIG`, `./plato.toml`,
`~/.config/plato/config.toml`, built-in defaults. Optional local config:

```toml
[provider]
kind = "open_router"
model = "~openai/gpt-latest"
api_key_env = "OPENROUTER_API_KEY"

[limits]
token_budget = 4000
max_output_tokens = 1024
max_turns = 8

[tools]
enabled = ["file.read", "file.list", "file.write", "file.edit", "shell.exec"]
```

## 1. First run (60-second smoke test)

```bash
plato "list the files here and summarize what this project is"
plato -c "name the most important file from that summary"
plato replay        # audit the latest default SQLite session
```

Live assistant text prints to stderr; the final answer prints to stdout. The
complete run ledger lands in the default platform SQLite store for the workspace.
`-c` continues the latest workspace session. Use `--events <file>` when you
want JSONL.

## 2. Test the approval boundary

```bash
plato --events w1.jsonl "write hello.txt containing: hi from plato"
# -> Approve file.write {...}? [y/N]   press Enter -> denied (default no)

plato --yolo --events w2.jsonl "write hello.txt containing: hi from plato"
# -> auto-approved; the ledger records actor "yolo"

plato --events w3.jsonl "run cargo test --locked and summarize the result"
# -> Approve shell.exec?   press y to run the command
```

Reads and listings never prompt. Workspace writes prompt unless `--yolo`.
Yolo does not approve network tools or `shell.exec`. `shell.exec` always
prompts and runs with a scrubbed environment that does not inherit provider
credentials.
Nothing escapes the workspace: `../`, absolute paths, and symlinks out are refused.

## 3. Durable runs (SQLite)

```bash
plato "read Cargo.toml and name the package"
# stderr prints: run_id / ledger_path / the exact replay command
plato -c "what did I ask you to inspect?"
plato replay                # replays the latest session
```

Explicit SQLite paths need the equals form: `--db=/tmp/run.db`. If the
workspace daemon is live, default-ledger prompts delegate to it. Replay,
explicit `--db=<path>`, and direct `--yolo` SQLite paths remain direct and fail
closed if they conflict with the daemon-owned store. Replay shows final
assistant messages, not partial live deltas.

## 4. The full experience: TUI

One terminal, same workspace:

```bash
plato
```

This attaches to the workspace daemon if one is already running. Otherwise it
starts the sibling `plato-agentd` detached. Quitting the TUI leaves that daemon
running. `plato --tui --config plato.toml` is the explicit form when selecting
a config.
The screen is a chat-first transcript with a bottom status rule and composer.

Manual two-terminal mode still works:

```bash
plato daemon                                          # terminal A
plato-tui --workspace "$PWD" --config plato.toml      # terminal B
```

`plato daemon` stays in the foreground and delegates to the supported sibling
`plato-agentd`. Ctrl-C shuts it down cleanly (socket and lock removed).
Quitting either TUI form never stops the daemon.

Start the optional Discord connector from a separate environment:

```bash
unset OPENAI_API_KEY OPENROUTER_API_KEY
export DISCORD_BOT_TOKEN="$(cat /path/to/discord-bot-token)"
plato gateway discord --config ~/.config/plato/gateway.toml
```

The gateway entry requires a successful workspace daemon `hello`. Probe
failures start no connector and point to `plato daemon`; the gateway never
starts a daemon with its Discord environment. The direct
`plato-gateway-discord --workspace "$PWD"` technical command remains supported.

| Key | Does |
| --- | --- |
| type + Enter | start a run when idle |
| `g` / `d` | grant / deny in the approval modal |
| Ctrl-C | first press cancels the active run; second quits the TUI |
| `r` | reconnect (only when the screen shows daemon unavailable) |
| `q` / Esc | quit (`q` only with an empty composer, so it is typeable in words) |
| Ctrl-U | clear the composer |

When `plato-gateway-discord` reaches an approval-required tool, Discord gets one
bounded notification with the tool, effect, and preview. Grant or deny it
locally in `plato-tui`; the gateway never sends approval decisions. Failed runs
post `Run failed. Inspect it locally with: plato replay`. Canceled and
interrupted runs do not post terminal messages. Allowed messages show 👀 and a
typing indicator while active, then ✅ or ❌; canceled and interrupted runs
remove 👀 without a terminal reaction. The bot needs Add Reactions and Read
Message History, plus Send Messages in Threads when threads are used.

## 5. Run the test suite (no API key needed)

```bash
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
```

## 6. Troubleshooting

| Symptom | Fix |
| --- | --- |
| daemon lock held | on Unix, a live kernel lock owner or failed lock-file safety validation blocks startup; inspect the reported details. Crashed owners recover automatically. Never delete a live lock |
| `--db /path` ignored | use the equals form: `--db=/path` |
| provider api key env is not set | re-export `OPENROUTER_API_KEY` in this shell |
| ledger already exists | JSONL ledgers never overwrite — pass a fresh `--events` name |
| run stops after 8 turns | runs are bounded by `limits.max_turns`; ask tighter or configure a different limit |
| `plato -c` says no previous session | run `plato "..."` once in this workspace first |
