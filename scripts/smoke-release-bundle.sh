#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'release bundle smoke: %s\n' "$*" >&2
  exit 1
}

[[ "$#" -eq 2 ]] || die 'usage: smoke-release-bundle.sh ARCHIVE TARGET'

archive_dir=$(cd -- "$(dirname -- "$1")" && pwd -P)
archive="$archive_dir/$(basename -- "$1")"
target=$2
case "$target" in
  linux-x86_64 | macos-arm64) ;;
  *) die "unsupported target: $target" ;;
esac
[[ -f "$archive" && ! -L "$archive" ]] || die "archive is not a regular file: $archive"

archive_name=${archive##*/}
bundle=${archive_name%.tar.gz}
[[ "$bundle" != "$archive_name" && "$bundle" == platonic-*-$target ]] \
  || die "archive name does not match target $target: $archive_name"

umask 077
proof_root=$(mktemp -d /tmp/p14l4.XXXXXX)
proof_root=$(cd -- "$proof_root" && pwd -P)
case "$proof_root" in
  /tmp/p14l4.* | /private/tmp/p14l4.*) ;;
  *) die "unexpected proof root: $proof_root" ;;
esac
chmod 0700 "$proof_root"

server_pid=
provider_pid=
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  set +e
  for pid in "$provider_pid" "$server_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null
      sleep 1
      kill -KILL "$pid" 2>/dev/null
    fi
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null
    fi
  done
  if [[ "$status" -ne 0 ]]; then
    for log in provider.log server.stderr plato.stderr replay.stderr shutdown.stderr; do
      if [[ -s "$proof_root/$log" ]]; then
        printf '\n[%s]\n' "$log" >&2
        sed -n '1,200p' "$proof_root/$log" >&2
      fi
    done
  fi
  rm -rf -- "$proof_root"
  exit "$status"
}
trap cleanup EXIT HUP INT TERM

wait_bounded() {
  pid=$1
  label=$2
  (
    sleep 15
    if kill -0 "$pid" 2>/dev/null; then
      printf 'release bundle smoke: %s exceeded 15 seconds\n' "$label" >&2
      kill -TERM "$pid" 2>/dev/null
    fi
  ) &
  timer_pid=$!

  set +e
  wait "$pid"
  status=$?
  kill -TERM "$timer_pid" 2>/dev/null
  wait "$timer_pid" 2>/dev/null
  set -e

  [[ "$status" -eq 0 ]] || die "$label failed with status $status"
}

extract="$proof_root/extract"
home="$proof_root/home"
runtime="$proof_root/runtime"
state="$proof_root/state"
config_home="$proof_root/config"
temp="$proof_root/temp"
workspace="$proof_root/workspace"
mkdir -p -- "$extract" "$home" "$runtime" "$state" "$config_home" "$temp" "$workspace"
chmod 0700 "$extract" "$home" "$runtime" "$state" "$config_home" "$temp" "$workspace"

archive_sha_before=$(shasum -a 256 "$archive" | awk '{print $1}')
tar -xzf "$archive" -C "$extract"
bin="$extract/$bundle/bin"
platonic="$bin/platonic"
plato="$bin/plato"
[[ -x "$platonic" && -f "$platonic" && ! -L "$platonic" ]] \
  || die 'extracted platonic is not a regular executable'
[[ -x "$plato" && -f "$plato" && ! -L "$plato" ]] \
  || die 'extracted plato is not a regular executable'

socket="$runtime/platonic/host/agent.sock"
printf 'release bundle smoke socket: %s\n' "$socket"
[[ "${#socket}" -lt 100 ]] || die "socket path is too long: ${#socket} bytes"
[[ ! -e "$socket" ]] || die "socket path was not absent: $socket"

clean_path=/usr/bin:/bin:/usr/sbin:/sbin
product_env=(
  "HOME=$home"
  "PATH=$clean_path"
  'LANG=C'
  'LC_ALL=C'
  "TMPDIR=$temp"
  "XDG_CONFIG_HOME=$config_home"
  "XDG_RUNTIME_DIR=$runtime"
  "XDG_STATE_HOME=$state"
  'PLATONIC_RELEASE_SMOKE_KEY=loopback-only-fixture'
  "PLATONIC_BIN=$platonic"
)

env -i HOME="$home" PATH="$clean_path" LANG=C LC_ALL=C TMPDIR="$temp" \
  /usr/bin/git -C "$workspace" init -q
env -i HOME="$home" PATH="$clean_path" LANG=C LC_ALL=C TMPDIR="$temp" \
  /usr/bin/git -C "$workspace" \
    -c user.name='Platonic Release Smoke' \
    -c user.email='release-smoke@invalid' \
    commit --allow-empty --no-gpg-sign -q -m 'Initial workspace'

(
  cd -- "$workspace"
  exec env -i "${product_env[@]}" "$platonic" serve
) >"$proof_root/server.stdout" 2>"$proof_root/server.stderr" &
server_pid=$!

attempt=0
while [[ ! -S "$socket" ]]; do
  kill -0 "$server_pid" 2>/dev/null || die 'platonic serve exited before creating its socket'
  attempt=$((attempt + 1))
  [[ "$attempt" -lt 200 ]] || die 'platonic serve did not create its socket within 10 seconds'
  sleep 0.05
done

env -i "${product_env[@]}" "$platonic" workspace create release-smoke "$workspace" \
  >"$proof_root/workspace.json"
env -i "${product_env[@]}" "$platonic" status --workspace "$workspace" \
  >"$proof_root/status.json"
grep -F '"name":"release-smoke"' "$proof_root/workspace.json" >/dev/null \
  || die 'workspace registration readback did not name release-smoke'
grep -F '"daemon"' "$proof_root/status.json" >/dev/null \
  || die 'daemon status readback was missing'

python_bin=$(command -v python3) || die 'python3 is required for the loopback fixture'
port_file="$proof_root/provider.port"
request_file="$proof_root/provider.request"
answer='release bundle smoke completed'
question='Return exactly: release bundle smoke completed'
"$python_bin" - "$port_file" "$request_file" "$question" "$answer" \
  >"$proof_root/provider.log" 2>&1 <<'PY' &
import json
import os
import socket
import sys

port_file, request_file, question, answer = sys.argv[1:]
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(1)
listener.settimeout(15)

port = str(listener.getsockname()[1]).encode("ascii")
fd = os.open(port_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "wb") as stream:
    stream.write(port)
    stream.flush()
    os.fsync(stream.fileno())

connection, peer = listener.accept()
if peer[0] != "127.0.0.1":
    raise RuntimeError(f"non-loopback provider peer: {peer[0]}")
connection.settimeout(15)

request = bytearray()
while b"\r\n\r\n" not in request:
    chunk = connection.recv(4096)
    if not chunk:
        raise RuntimeError("provider request ended before headers")
    request.extend(chunk)
    if len(request) > 1024 * 1024:
        raise RuntimeError("provider headers exceeded limit")

header_bytes, body = request.split(b"\r\n\r\n", 1)
header_lines = header_bytes.decode("iso-8859-1").split("\r\n")
if header_lines[0] != "POST /chat/completions HTTP/1.1":
    raise RuntimeError(f"unexpected provider request line: {header_lines[0]}")
headers = {}
for line in header_lines[1:]:
    name, value = line.split(":", 1)
    headers[name.lower()] = value.strip()
content_length = int(headers["content-length"])
while len(body) < content_length:
    chunk = connection.recv(min(4096, content_length - len(body)))
    if not chunk:
        raise RuntimeError("provider request ended before body")
    body.extend(chunk)
body = bytes(body[:content_length])
payload = json.loads(body)
if payload.get("stream") is not True:
    raise RuntimeError("provider request was not streaming")
if question not in json.dumps(payload.get("messages", [])):
    raise RuntimeError("provider request omitted the smoke question")

fd = os.open(request_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "wb") as stream:
    stream.write(body)

events = [
    json.dumps({"choices": [{"index": 0, "delta": {"content": answer}, "finish_reason": None}]}),
    json.dumps({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
    "[DONE]",
]
response_body = "".join(f"data: {event}\n\n" for event in events).encode("utf-8")
response = (
    "HTTP/1.1 200 OK\r\n"
    "content-type: text/event-stream\r\n"
    f"content-length: {len(response_body)}\r\n"
    "connection: close\r\n\r\n"
).encode("ascii") + response_body
connection.sendall(response)
connection.close()
listener.close()
print("loopback provider served one deterministic response")
PY
provider_pid=$!

attempt=0
while [[ ! -f "$port_file" ]]; do
  kill -0 "$provider_pid" 2>/dev/null || die 'loopback provider exited before reporting its port'
  attempt=$((attempt + 1))
  [[ "$attempt" -lt 200 ]] || die 'loopback provider did not report its port within 10 seconds'
  sleep 0.05
done
port=$(cat -- "$port_file")
[[ "$port" =~ ^[0-9]+$ ]] || die "loopback provider returned an invalid port: $port"

config="$proof_root/plato.toml"
cat >"$config" <<EOF
[provider]
kind = "open_ai"
model = "release-smoke-model"
api_key_env = "PLATONIC_RELEASE_SMOKE_KEY"
base_url = "http://127.0.0.1:$port"
connect_timeout_ms = 5000
stream_idle_timeout_ms = 5000

[limits]
token_budget = 4000
max_output_tokens = 32
max_turns = 2

[tools]
enabled = ["file.read"]
EOF

(
  cd -- "$workspace"
  env -i "${product_env[@]}" "$plato" --config "$config" "$question"
) >"$proof_root/answer.txt" 2>"$proof_root/plato.stderr"
printf '%s\n' "$answer" >"$proof_root/expected-answer.txt"
diff -u "$proof_root/expected-answer.txt" "$proof_root/answer.txt"
wait_bounded "$provider_pid" 'loopback provider'
provider_pid=
grep -F "$question" "$request_file" >/dev/null \
  || die 'loopback provider did not record the smoke question'

(
  cd -- "$workspace"
  env -i "${product_env[@]}" "$plato" replay
) >"$proof_root/replay.txt" 2>"$proof_root/replay.stderr"
grep -F "$answer" "$proof_root/replay.txt" >/dev/null \
  || die 'offline replay did not contain the completed answer'

env -i "${product_env[@]}" "$platonic" shutdown --workspace "$workspace" \
  >"$proof_root/shutdown.json" 2>"$proof_root/shutdown.stderr"
grep -F '"result":"shutdown"' "$proof_root/shutdown.json" >/dev/null \
  || die 'platonic shutdown was not acknowledged'
wait_bounded "$server_pid" 'platonic serve shutdown'
server_pid=
[[ ! -S "$socket" ]] || die 'platonic serve socket remained after shutdown'

archive_sha_after=$(shasum -a 256 "$archive" | awk '{print $1}')
[[ "$archive_sha_after" == "$archive_sha_before" ]] \
  || die 'release archive changed during smoke proof'

printf 'release bundle smoke: %s clean extract/run lifecycle passed\n' "$target"
