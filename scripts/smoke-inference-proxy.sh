#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
root=/tmp/p641
runtime="$root/runtime"
state="$root/state"
capture="$root/capture"
workspace="$root/workspace"
outputs="$root/outputs"
control_socket="$runtime/platonic/inference-proxy/control.sock"
host_socket="$runtime/platonic/host/agent.sock"
model=openai/gpt-5.6-luna
marker="P641_COMPARE_$(date +%s)_$$"
task="Reply with exactly: $marker"

[[ -n ${OPENROUTER_API_KEY:-} ]] || {
  echo "OPENROUTER_API_KEY is required" >&2
  exit 1
}
[[ ! -e $root ]] || {
  echo "$root must be absent before the smoke" >&2
  exit 1
}
((${#control_socket} < 100)) || {
  echo "control socket path is too long: $control_socket" >&2
  exit 1
}
command -v codex >/dev/null
command -v hermes >/dev/null
command -v python3 >/dev/null

mkdir -m 0700 "$root" "$runtime" "$state" "$workspace" "$outputs" "$root/codex" "$root/hermes"
echo "control_socket: $control_socket"
echo "host_socket: $host_socket"
((${#host_socket} < 100)) || {
  echo "host socket path is too long: $host_socket" >&2
  exit 1
}

target_dir=${CARGO_TARGET_DIR:-$repo/target}
[[ $target_dir = /* ]] || target_dir="$repo/$target_dir"
platonic_bin="$target_dir/debug/platonic"
plato_bin="$target_dir/debug/plato"
proxy_pid=
host_pid=

process_alive() {
  local state
  [[ -n $1 ]] || return 1
  state=$(awk '/^State:/ { print $2 }' "/proc/$1/status" 2>/dev/null) || return 1
  [[ $state != Z ]] && kill -0 "$1" 2>/dev/null
}

stop_owned_process() {
  local pid=$1
  process_alive "$pid" || return 0
  kill "$pid" 2>/dev/null || true
  for _ in {1..100}; do
    process_alive "$pid" || return 0
    sleep 0.05
  done
  kill -KILL "$pid" 2>/dev/null || true
}

print_failure_logs() {
  local log
  for log in "$outputs"/*.stderr; do
    [[ -s $log ]] || continue
    echo "failure log: ${log##*/}" >&2
    python3 - "$log" <<'PY' >&2
import os
import sys
data = open(sys.argv[1], "rb").read()
secret = os.environ["OPENROUTER_API_KEY"].encode()
sys.stderr.write(data.replace(secret, b"[REDACTED]").decode("utf-8", "replace")[-4000:])
PY
  done
}

cleanup() {
  result=$?
  trap - EXIT INT TERM
  set +e

  if ((result != 0)); then
    print_failure_logs
  fi

  if [[ -x $platonic_bin ]]; then
    host_lock="$runtime/platonic/host/agent.lock"
    if [[ -f $host_lock ]]; then
      host_pid=$(python3 - "$host_lock" <<'PY'
import json
import sys
try:
    print(json.load(open(sys.argv[1], encoding="utf-8")).get("pid", ""))
except Exception:
    print("")
PY
)
    fi
    XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" \
      "$platonic_bin" shutdown --workspace "$workspace" >/dev/null 2>&1 || true
    for _ in {1..100}; do
      process_alive "$host_pid" || break
      sleep 0.05
    done
    XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" \
      "$platonic_bin" inference-proxy down >/dev/null 2>&1 || true
    for _ in {1..100}; do
      process_alive "$proxy_pid" || break
      sleep 0.05
    done
    stop_owned_process "$host_pid"
    stop_owned_process "$proxy_pid"
  fi

  if process_alive "$host_pid" || process_alive "$proxy_pid" || [[ -S $control_socket || -S $host_socket ]]; then
    echo "owned process cleanup failed" >&2
    result=1
  fi
  rm -rf -- "$root"
  [[ ! -e $root ]] || result=1
  echo "cleanup: removed $root; owned processes stopped"
  exit "$result"
}
trap cleanup EXIT INT TERM

cd "$repo"
cargo build --locked -p platonic -p plato-agent --bin platonic --bin plato

XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" \
  "$platonic_bin" inference-proxy up --capture-dir "$capture" >"$outputs/up.json"
proxy_pid=$(python3 - "$outputs/up.json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["pid"])
PY
)
base_url=$(python3 - "$outputs/up.json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["base_url"])
PY
)

git -C "$workspace" init -q
(
  cd "$workspace"
  exec env XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" "$platonic_bin" serve
) >"$outputs/server.stdout" 2>"$outputs/server.stderr" &
host_pid=$!
for _ in {1..200}; do
  [[ -S $host_socket ]] && break
  process_alive "$host_pid" || {
    echo "platonic serve exited before readiness" >&2
    exit 1
  }
  sleep 0.05
done
[[ -S $host_socket ]] || {
  echo "platonic serve did not create its socket" >&2
  exit 1
}
XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" \
  timeout 5 "$platonic_bin" workspace create p641-smoke "$workspace" \
  >"$outputs/workspace.json"
XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" \
  timeout 5 "$platonic_bin" status --workspace "$workspace" >"$outputs/status.json"

cat >"$root/codex/config.toml" <<EOF
model = "$model"
model_provider = "openrouter_proxy"
model_reasoning_effort = "medium"

[model_providers.openrouter_proxy]
name = "OpenRouter through Platonic"
base_url = "$base_url"
env_key = "OPENROUTER_API_KEY"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
EOF
chmod 0600 "$root/codex/config.toml"

cat >"$root/plato.toml" <<EOF
[provider]
kind = "open_router"
model = "$model"
api_key_env = "OPENROUTER_API_KEY"
base_url = "$base_url"
connect_timeout_ms = 30000
stream_idle_timeout_ms = 300000

[limits]
token_budget = 8192
max_output_tokens = 512
max_turns = 2
EOF
chmod 0600 "$root/plato.toml"

CODEX_HOME="$root/codex" codex exec \
  --ephemeral --ignore-rules --skip-git-repo-check --sandbox read-only \
  -C "$workspace" --json "$task" >"$outputs/codex.jsonl" 2>"$outputs/codex.stderr"
grep -Fq "$marker" "$outputs/codex.jsonl"
echo "client complete: Codex Responses"

HERMES_HOME="$root/hermes" HERMES_NO_TMUX=1 OPENROUTER_BASE_URL="$base_url" \
  hermes --ignore-user-config --ignore-rules --safe-mode \
  --provider openrouter --model "$model" --reasoning medium --in "$workspace" \
  -z "$task" >"$outputs/hermes.txt" 2>"$outputs/hermes.stderr"
grep -Fq "$marker" "$outputs/hermes.txt"
echo "client complete: Hermes Chat Completions"

(
  cd "$workspace"
  XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" PLATONIC_BIN="$platonic_bin" \
    "$plato_bin" --config "$root/plato.toml" --yolo "$task"
) >"$outputs/plato.txt" 2>"$outputs/plato.stderr"
grep -Fq "$marker" "$outputs/plato.txt"
echo "client complete: Platonic Chat Completions"

XDG_RUNTIME_DIR="$runtime" XDG_STATE_HOME="$state" \
  "$platonic_bin" inference-proxy compare "$capture" >"$outputs/comparison.json"
python3 - "$outputs/comparison.json" <<'PY'
import json
import sys
flows = json.load(open(sys.argv[1], encoding="utf-8"))["flows"]
protocols = [flow["protocol"] for flow in flows]
if protocols.count("responses") < 1 or protocols.count("chat_completions") < 2:
    raise SystemExit(f"expected one Responses and two Chat Completions flows, got {protocols}")
PY

python3 - "$root" <<'PY'
import os
import stat
import sys
secret = os.environ["OPENROUTER_API_KEY"].encode()
for directory, _, files in os.walk(sys.argv[1]):
    for name in files:
        path = os.path.join(directory, name)
        if not stat.S_ISREG(os.lstat(path).st_mode):
            continue
        with open(path, "rb") as handle:
            if secret in handle.read():
                raise SystemExit("OpenRouter credential was persisted in the disposable root")
PY

echo "capture_dir: $capture"
echo "capture_file: $capture/traffic.jsonl"
echo "comparison_file: $outputs/comparison.json"
cat "$outputs/comparison.json"
