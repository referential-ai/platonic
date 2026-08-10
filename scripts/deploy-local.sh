#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

readonly PROGRAM="${0##*/}"
readonly -a BUILD_BINARIES=(plato platonic plato-tui)
readonly -a INSTALL_BINARIES=(plato-real platonic-real plato-tui-real)

stage_dir=""
proof_root=""
proof_pid=""
proof_socket=""
transaction="none"

usage() {
  printf 'usage: %s [--rollback]\n' "$PROGRAM"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

path_uid() {
  stat -c '%u' -- "$1"
}

validate_owned_directory() {
  local path="$1"
  [[ ! -L "$path" ]] || die "directory must not be a symlink: $path"
  [[ -d "$path" ]] || die "expected directory: $path"
  [[ "$(path_uid "$path")" == "$(id -u)" ]] || die "directory is not owned by the current user: $path"
}

prepare_install_parent() {
  local home_path="$HOME"
  [[ -n "$home_path" && "$home_path" == /* ]] || die 'HOME must be an absolute path'
  [[ -d "$home_path" ]] || die "HOME does not exist: $home_path"
  validate_owned_directory "$home_path"

  local local_dir="$home_path/.local"
  local lib_dir="$local_dir/lib"
  [[ -e "$local_dir" ]] || mkdir -- "$local_dir"
  validate_owned_directory "$local_dir"
  [[ -e "$lib_dir" ]] || mkdir -- "$lib_dir"
  validate_owned_directory "$lib_dir"
}

validate_binary_file() {
  local path="$1"
  [[ ! -L "$path" ]] || die "binary must not be a symlink: $path"
  [[ -f "$path" ]] || die "binary is not a regular file: $path"
  [[ -x "$path" ]] || die "binary is not executable: $path"
  [[ "$(path_uid "$path")" == "$(id -u)" ]] || die "binary is not owned by the current user: $path"
}

validate_set() {
  local path="$1"
  local allow_missing="$2"
  if [[ ! -e "$path" ]]; then
    [[ "$allow_missing" == "yes" ]] || die "binary set is missing: $path"
    return 1
  fi
  validate_owned_directory "$path"

  local LC_ALL=C
  local -a entries=()
  local -a unexpected=()
  shopt -s dotglob nullglob
  entries=("$path"/*)
  shopt -u dotglob nullglob
  if ((${#entries[@]} == 0)) && [[ "$allow_missing" == "yes" ]]; then
    return 1
  fi

  local entry expected name owned
  for entry in "${entries[@]}"; do
    name="${entry##*/}"
    owned="no"
    for expected in "${INSTALL_BINARIES[@]}"; do
      if [[ "$name" == "$expected" ]]; then
        owned="yes"
        break
      fi
    done
    [[ "$owned" == "yes" ]] || unexpected+=("$name")
  done
  if ((${#unexpected[@]} > 0)); then
    for name in "${unexpected[@]}"; do
      printf 'error: unexpected binary-set basename %q in %s; move it outside the managed directory before retrying\n' \
        "$name" "$path" >&2
    done
    exit 1
  fi

  for name in "${INSTALL_BINARIES[@]}"; do
    validate_binary_file "$path/$name"
  done
  return 0
}

sha256_file() {
  sha256sum -- "$1" | awk '{print $1}'
}

record_checksums() {
  local label="$1"
  local path="$2"
  local name
  if [[ ! -d "$path" ]]; then
    for name in "${INSTALL_BINARIES[@]}"; do
      printf 'checksum %s missing %s\n' "$label" "$name"
    done
    return
  fi
  for name in "${INSTALL_BINARIES[@]}"; do
    printf 'checksum %s %s %s\n' "$label" "$(sha256_file "$path/$name")" "$name"
  done
}

set_digests() {
  local path="$1"
  local name
  for name in "${INSTALL_BINARIES[@]}"; do
    sha256_file "$path/$name"
  done
}

assert_digests() {
  local path="$1"
  shift
  local -a expected=("$@")
  local index
  for index in "${!INSTALL_BINARIES[@]}"; do
    [[ "$(sha256_file "$path/${INSTALL_BINARIES[$index]}")" == "${expected[$index]}" ]] \
      || die "binary set checksum mismatch: $path/${INSTALL_BINARIES[$index]}"
  done
}

fetch_develop() {
  git -C "$repo_root" fetch --quiet --no-tags origin \
    refs/heads/develop:refs/remotes/origin/develop
}

require_clean_develop() {
  local branch head remote
  [[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal)" ]] \
    || die 'checkout is dirty; no files were changed or discarded'
  branch="$(git -C "$repo_root" symbolic-ref --quiet --short HEAD)" \
    || die 'checkout is detached; develop is required'
  [[ "$branch" == "develop" ]] || die "checkout branch is $branch; develop is required"
  head="$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')"
  remote="$(git -C "$repo_root" rev-parse --verify 'refs/remotes/origin/develop^{commit}')" \
    || die 'origin/develop is unavailable after fetch'
  [[ "$head" == "$remote" ]] \
    || die "develop does not equal origin/develop (develop=$head origin/develop=$remote)"
}

atomic_exchange() {
  python3 - "$1" "$2" <<'PY'
import ctypes
import os
import sys

left, right = map(os.fsencode, sys.argv[1:])
libc = ctypes.CDLL(None, use_errno=True)
renameat2 = getattr(libc, "renameat2", None)
if renameat2 is None:
    raise SystemExit("renameat2 is unavailable; refusing a non-atomic set replacement")
renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
renameat2.restype = ctypes.c_int
if renameat2(-100, left, -100, right, 2) != 0:
    error = ctypes.get_errno()
    raise OSError(error, os.strerror(error), (os.fsdecode(left), os.fsdecode(right)))
PY
}

daemon_rpc() {
  local socket_path="$1"
  local workspace_root="$2"
  local expected_identity="$3"
  local expected_pid="$4"
  local action="$5"
  python3 - "$socket_path" "$workspace_root" "$expected_identity" "$expected_pid" "$action" <<'PY'
import json
import os
import socket
import struct
import sys

socket_path, workspace_root, expected_identity, expected_pid, action = sys.argv[1:]
workspace_root = os.path.realpath(workspace_root)
expected_pid = int(expected_pid)

def request(stream, request_id, method, params=None):
    payload = {"v": 1, "id": request_id, "kind": "request", "method": method}
    if params is not None:
        payload["params"] = params
    stream.sendall(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
    data = bytearray()
    while not data.endswith(b"\n"):
        chunk = stream.recv(4096)
        if not chunk:
            raise RuntimeError(f"daemon closed before {method} response")
        data.extend(chunk)
        if len(data) > 65536:
            raise RuntimeError(f"daemon {method} response exceeds 65536 bytes")
    response = json.loads(data)
    if response.get("v") != 1 or response.get("id") != request_id:
        raise RuntimeError(f"invalid {method} response envelope")
    if response.get("kind") != "response" or response.get("method") != method:
        raise RuntimeError(f"daemon rejected {method}: {response}")
    return response.get("result")

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
    stream.settimeout(3)
    stream.connect(socket_path)
    peer_pid, peer_uid, _ = struct.unpack(
        "3i", stream.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
    )
    if peer_pid != expected_pid or peer_uid != os.getuid():
        raise RuntimeError(f"daemon peer identity mismatch: pid={peer_pid} uid={peer_uid}")

    if action == "shutdown":
        result = request(stream, "deploy_shutdown", "daemon.shutdown_if_idle")
        if not isinstance(result, dict) or result.get("result") != "shutdown":
            raise RuntimeError(f"daemon refused idle shutdown: {result}")
        print("shutdown")
        raise SystemExit(0)
    if action != "hello":
        raise RuntimeError(f"unsupported action: {action}")

    listed = request(stream, "deploy_workspace_list", "workspace.list", {})
    workspaces = listed.get("workspaces", []) if isinstance(listed, dict) else []
    workspace = next(
        (entry for entry in workspaces if os.path.realpath(entry.get("root", "")) == workspace_root),
        None,
    )
    if workspace is None:
        created = request(stream, "deploy_workspace_create", "workspace.create", {
            "name": "deploy-readback",
            "root": workspace_root,
        })
        workspace = created.get("workspace") if isinstance(created, dict) else None
    if not isinstance(workspace, dict) or not isinstance(workspace.get("id"), str):
        raise RuntimeError(f"daemon did not return a registered workspace: {workspace}")

    hello = request(stream, "deploy_hello", "hello", {
        "workspace_root": workspace_root,
        "workspace_id": workspace["id"],
    })
    if not isinstance(hello, dict) or hello.get("workspace_id") != workspace["id"]:
        raise RuntimeError(f"daemon hello workspace mismatch: {hello}")
    if hello.get("daemon_scope") != "host":
        raise RuntimeError(f"daemon hello scope mismatch: {hello}")
    if hello.get("daemon_version") != expected_identity:
        raise RuntimeError(
            f"daemon provenance mismatch: expected {expected_identity!r}, got {hello.get('daemon_version')!r}"
        )
    print(json.dumps(hello, sort_keys=True))
PY
}

retire_installed_daemons() {
  local scan_runtime_root="$1"
  local scan_installed_daemon="$2"
  python3 - "$scan_runtime_root" "$scan_installed_daemon" <<'PY'
import fcntl
import json
import os
import socket
import stat
import struct
import sys
import time

runtime_root, installed_daemon = sys.argv[1:]
uid = os.getuid()
installed_daemon = os.path.abspath(installed_daemon)
host_directory = os.path.join(runtime_root, "platonic", "host")
host_lock = os.path.join(host_directory, "agent.lock")
host_endpoint = os.path.join(host_directory, "agent.sock")
maximum_lock_bytes = 16 * 1024

def fail(message):
    raise RuntimeError(message)

def process_identity(pid):
    try:
        status_text = open(f"/proc/{pid}/status", encoding="utf-8").read()
        stat_text = open(f"/proc/{pid}/stat", encoding="utf-8").read()
        executable_link = os.readlink(f"/proc/{pid}/exe").removesuffix(" (deleted)")
        executable_stat = os.stat(f"/proc/{pid}/exe")
    except (FileNotFoundError, ProcessLookupError):
        fail(f"daemon lock has stale pid {pid}")
    uid_line = next((line for line in status_text.splitlines() if line.startswith("Uid:")), None)
    if uid_line is None:
        fail(f"daemon pid {pid} omitted process uid")
    process_uids = [int(value) for value in uid_line.split()[1:]]
    if not process_uids or any(value != uid for value in process_uids):
        fail(f"daemon pid {pid} is not owned exclusively by uid {uid}")
    close = stat_text.rfind(")")
    fields = stat_text[close + 2:].split()
    if close < 0 or len(fields) <= 19:
        fail(f"daemon pid {pid} has unreadable process identity")
    return fields[19], executable_link, executable_stat

def request(stream, request_id, method, params=None):
    payload = {"v": 1, "id": request_id, "kind": "request", "method": method}
    if params is not None:
        payload["params"] = params
    stream.sendall(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
    data = bytearray()
    while not data.endswith(b"\n"):
        chunk = stream.recv(4096)
        if not chunk:
            fail(f"daemon closed before {method} response")
        data.extend(chunk)
        if len(data) > 65536:
            fail(f"daemon {method} response exceeds 65536 bytes")
    response = json.loads(data)
    if response.get("v") != 1 or response.get("id") != request_id:
        fail(f"invalid {method} response envelope")
    if response.get("kind") != "response" or response.get("method") != method:
        fail(f"daemon rejected {method}: {response}")
    return response.get("result")

if not os.path.exists(host_lock):
    print("old daemon not running")
    raise SystemExit(0)
host_stat = os.lstat(host_directory)
if not stat.S_ISDIR(host_stat.st_mode) or host_stat.st_uid != uid or stat.S_IMODE(host_stat.st_mode) != 0o700:
    fail(f"daemon host namespace has invalid type, ownership, or mode: {host_directory}")
lock_stat = os.lstat(host_lock)
if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_uid != uid or stat.S_IMODE(lock_stat.st_mode) != 0o600:
    fail(f"daemon lock has invalid type, owner, or mode: {host_lock}")
flags = os.O_RDWR | os.O_CLOEXEC
if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
descriptor = os.open(host_lock, flags)
opened_stat = os.fstat(descriptor)
if (opened_stat.st_dev, opened_stat.st_ino) != (lock_stat.st_dev, lock_stat.st_ino):
    os.close(descriptor)
    fail(f"daemon lock changed while opening: {host_lock}")
try:
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    pass
else:
    fcntl.flock(descriptor, fcntl.LOCK_UN)
    os.close(descriptor)
    print("old daemon not running")
    raise SystemExit(0)

stream = None
try:
    raw = os.pread(descriptor, maximum_lock_bytes + 1, 0)
    if len(raw) > maximum_lock_bytes:
        fail(f"daemon lock exceeds {maximum_lock_bytes} bytes: {host_lock}")
    metadata = json.loads(raw)
    v1_keys = {"v", "pid", "executable", "workspace_root", "workspace_id", "socket_path"}
    v2_keys = {"v", "pid", "executable", "endpoint"}
    if not isinstance(metadata, dict):
        fail(f"daemon lock metadata is invalid: {host_lock}")
    if metadata.get("v") == 1 and set(metadata) == v1_keys:
        if metadata.get("workspace_root") != "host" or metadata.get("workspace_id") != "host":
            fail(f"daemon host identity mismatch: {host_lock}")
        endpoint = metadata.get("socket_path")
    elif metadata.get("v") == 2 and set(metadata) == v2_keys:
        endpoint = metadata.get("endpoint")
    else:
        fail(f"daemon lock metadata is invalid: {host_lock}")
    if endpoint != host_endpoint:
        fail(f"installed daemon endpoint is not the host endpoint: {endpoint}")

    pid = metadata.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        fail(f"daemon lock pid is invalid: {host_lock}")
    executable = metadata.get("executable")
    if not isinstance(executable, str) or not os.path.isabs(executable):
        fail(f"daemon executable identity is missing: {host_lock}")
    start_time, process_executable, process_stat = process_identity(pid)
    metadata_executable = os.path.realpath(executable)
    if metadata_executable != installed_daemon and process_executable != installed_daemon:
        fail(f"host daemon pid {pid} is not the installed daemon executable")
    if os.path.exists(executable):
        executable_stat = os.stat(executable)
        if (executable_stat.st_dev, executable_stat.st_ino) != (process_stat.st_dev, process_stat.st_ino):
            fail(f"daemon lock executable does not match pid {pid}")
    elif process_executable != metadata_executable:
        fail(f"daemon executable disappeared during validation: {executable}")

    endpoint_stat = os.lstat(endpoint)
    if not stat.S_ISSOCK(endpoint_stat.st_mode) or endpoint_stat.st_uid != uid or stat.S_IMODE(endpoint_stat.st_mode) != 0o600:
        fail(f"installed daemon endpoint has invalid type, owner, or mode: {endpoint}")
    stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stream.settimeout(3)
    stream.connect(endpoint)
    peer_pid, peer_uid, _ = struct.unpack(
        "3i", stream.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
    )
    if peer_pid != pid or peer_uid != uid:
        fail(f"installed daemon peer identity mismatch: pid={peer_pid} uid={peer_uid}")

    listed = request(stream, "deploy_workspace_list", "workspace.list", {})
    workspaces = listed.get("workspaces", []) if isinstance(listed, dict) else []
    workspace = workspaces[0] if workspaces else None
    if workspace is None:
        control_root = os.path.realpath(os.path.dirname(installed_daemon))
        created = request(stream, "deploy_workspace_create", "workspace.create", {
            "name": "deploy-replacement",
            "root": control_root,
        })
        workspace = created.get("workspace") if isinstance(created, dict) else None
    if not isinstance(workspace, dict):
        fail(f"installed daemon did not return a workspace: pid={pid}")
    hello = request(stream, "deploy_hello", "hello", {
        "workspace_root": workspace.get("root"),
        "workspace_id": workspace.get("id"),
    })
    if not isinstance(hello, dict) or hello.get("daemon_scope") != "host":
        fail(f"installed daemon hello scope mismatch: pid={pid}")
    if "daemon.shutdown_if_idle" not in hello.get("capabilities", []):
        fail(f"installed daemon lacks daemon.shutdown_if_idle: pid={pid}")
    if os.pread(descriptor, maximum_lock_bytes + 1, 0) != raw:
        fail(f"installed daemon lock changed during validation: {host_lock}")
    current_start, current_executable, _ = process_identity(pid)
    if current_start != start_time or current_executable != process_executable:
        fail(f"installed daemon process changed during validation: pid={pid}")

    print(f"old daemon pid={pid} provenance={hello.get('daemon_version')}")
    result = request(stream, "deploy_shutdown", "daemon.shutdown_if_idle")
    if not isinstance(result, dict) or result.get("result") != "shutdown":
        fail(f"installed daemon pid {pid} refused shutdown because it is active")
    stream.close()
    stream = None

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        process_gone = not os.path.exists(f"/proc/{pid}/stat")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            unlocked = False
        else:
            unlocked = True
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        if process_gone and unlocked and not os.path.exists(endpoint):
            print(f"old daemon pid={pid} shutdown=acknowledged")
            break
        time.sleep(0.05)
    else:
        fail(f"installed daemon pid {pid} did not exit after shutdown acknowledgement")
finally:
    if stream is not None:
        stream.close()
    os.close(descriptor)
PY
}

wait_for_process_exit() {
  local pid="$1"
  local attempts=100
  while ((attempts > 0)); do
    if [[ ! -e "/proc/$pid/stat" ]]; then
      wait "$pid" 2>/dev/null || true
      return 0
    fi
    sleep 0.05
    attempts=$((attempts - 1))
  done
  return 1
}

stop_proof_daemon() {
  if [[ -n "$proof_pid" && -e "/proc/$proof_pid/stat" && -S "$proof_socket" ]]; then
    daemon_rpc "$proof_socket" "$proof_root/workspace" "$build_identity" "$proof_pid" shutdown \
      >/dev/null 2>&1 || return 1
    wait_for_process_exit "$proof_pid" || return 1
  fi
  proof_pid=""
}

restore_transaction() {
  case "$transaction" in
    swapped-stage)
      atomic_exchange "$install_dir" "$stage_dir"
      ;;
    staged-previous-rollback)
      atomic_exchange "$stage_dir" "$rollback_dir"
      atomic_exchange "$install_dir" "$stage_dir"
      ;;
    created-rollback)
      atomic_exchange "$install_dir" "$rollback_dir"
      stage_dir="$rollback_dir"
      ;;
    first-installed)
      [[ ! -e "$stage_dir" ]] || return 1
      mv -- "$install_dir" "$stage_dir"
      ;;
    rollback-swapped)
      atomic_exchange "$install_dir" "$rollback_dir"
      ;;
    none | complete)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
  transaction="none"
}

cleanup() {
  local status=$?
  local proof_stopped="yes"
  trap - EXIT INT TERM
  set +e
  if [[ -n "$proof_pid" ]]; then
    if ! stop_proof_daemon; then
      printf 'error: proof daemon did not acknowledge bounded idle shutdown\n' >&2
      status=1
      proof_stopped="no"
    fi
  fi
  if ((status != 0)) && [[ "$transaction" != "none" && "$transaction" != "complete" ]]; then
    if restore_transaction; then
      printf 'rollback: restored the pre-command installed set\n' >&2
    else
      printf 'error: automatic installed-set rollback failed\n' >&2
    fi
  fi
  if [[ -n "$stage_dir" && -d "$stage_dir" ]]; then
    rm -rf -- "$stage_dir"
  fi
  if [[ "$proof_stopped" == "yes" && -n "$proof_root" && -d "$proof_root" ]]; then
    rm -rf -- "$proof_root"
  elif [[ -n "$proof_root" ]]; then
    printf 'error: preserved live proof workspace at %s\n' "$proof_root" >&2
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT TERM

mode="deploy"
case "${1:-}" in
  "")
    ;;
  --rollback)
    mode="rollback"
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    die "unknown argument: $1"
    ;;
esac
(($# == 0)) || die 'too many arguments'

[[ "$(uname -s)" == "Linux" ]] || die 'local deployment currently requires Linux'
for command in awk id mkdir mv python3 sha256sum stat uname; do
  require_command "$command"
done

readonly install_parent="$HOME/.local/lib"
readonly install_dir="$install_parent/plato-agent"
readonly rollback_dir="$install_parent/plato-agent.rollback"
readonly installed_daemon="$install_dir/platonic-real"
readonly runtime_root="${XDG_RUNTIME_DIR:-/tmp/plato-agent-$(id -u)}"

if [[ "$mode" == "rollback" ]]; then
  prepare_install_parent
  validate_set "$install_dir" no
  validate_set "$rollback_dir" no
  [[ "$(stat -c '%d' -- "$install_dir")" == "$(stat -c '%d' -- "$rollback_dir")" ]] \
    || die 'installed and rollback sets are not on the same filesystem'
  mapfile -t rollback_digests < <(set_digests "$rollback_dir")
  record_checksums before "$install_dir"
  retire_installed_daemons "$runtime_root" "$installed_daemon"
  atomic_exchange "$install_dir" "$rollback_dir"
  transaction="rollback-swapped"
  validate_set "$install_dir" no
  assert_digests "$install_dir" "${rollback_digests[@]}"
  record_checksums after "$install_dir"
  transaction="complete"
  printf 'rollback: restored all three binaries from %s\n' "$rollback_dir"
  exit 0
fi

for command in cargo chmod cp date dirname env git grep jq mktemp rm sed sleep; do
  require_command "$command"
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
readonly repo_root
[[ "$script_dir" == "$repo_root/scripts" ]] || die 'deploy script is not inside the repository scripts directory'

require_clean_develop
fetch_develop
require_clean_develop
source_commit="$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')"
readonly source_commit
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || die "source commit is not a full lowercase Git object id: $source_commit"
build_date="$(date -u +%Y-%m-%d)"
readonly build_date
[[ "$build_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "invalid UTC build date: $build_date"

prepare_install_parent

metadata="$(cargo metadata --manifest-path "$repo_root/Cargo.toml" --locked --no-deps --format-version 1)"
package_version="$(jq -er '.packages[] | select(.name == "plato-agent") | .version' <<<"$metadata")"
readonly package_version
target_dir="$(jq -er '.target_directory' <<<"$metadata")"
readonly target_dir
[[ "$package_version" != *[[:space:]]* ]] || die "package version contains whitespace: $package_version"
readonly build_identity="$package_version $source_commit $build_date"

if validate_set "$install_dir" yes; then
  installed_before="yes"
else
  installed_before="no"
fi
if [[ -e "$rollback_dir" ]]; then
  validate_set "$rollback_dir" no
  [[ "$installed_before" == "yes" ]] || die 'rollback set exists while the installed set is absent'
fi
install_device="$(stat -c '%d' -- "$install_parent")"
readonly install_device
if [[ "$installed_before" == "yes" ]]; then
  [[ "$(stat -c '%d' -- "$install_dir")" == "$install_device" ]] \
    || die 'installed set is not on the install-parent filesystem'
fi
if [[ -e "$rollback_dir" ]]; then
  [[ "$(stat -c '%d' -- "$rollback_dir")" == "$install_device" ]] \
    || die 'rollback set is not on the install-parent filesystem'
fi
record_checksums before "$install_dir"

printf 'build: commit=%s date=%s\n' "$source_commit" "$build_date"
PLATO_BUILD_IDENTITY="$build_identity" cargo build \
  --manifest-path "$repo_root/Cargo.toml" \
  --package plato-agent \
  --package platonic \
  --locked \
  --release \
  --bin plato \
  --bin platonic \
  --bin plato-tui

fetch_develop
require_clean_develop
[[ "$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')" == "$source_commit" ]] \
  || die 'source commit changed during the build'

for index in "${!BUILD_BINARIES[@]}"; do
  binary="$target_dir/release/${BUILD_BINARIES[$index]}"
  validate_binary_file "$binary"
  version_output="$($binary --version)"
  [[ "$version_output" == "${BUILD_BINARIES[$index]} $build_identity" ]] \
    || die "built binary provenance mismatch: $binary reported $version_output"
done

stage_dir="$(mktemp -d "$install_parent/.plato-agent.stage.XXXXXX")"
if [[ "$installed_before" == "yes" ]]; then
  chmod --reference="$install_dir" "$stage_dir"
else
  chmod 0755 "$stage_dir"
fi
for index in "${!BUILD_BINARIES[@]}"; do
  source_binary="$target_dir/release/${BUILD_BINARIES[$index]}"
  staged_binary="$stage_dir/${INSTALL_BINARIES[$index]}"
  cp -- "$source_binary" "$staged_binary"
  if [[ "$installed_before" == "yes" ]]; then
    chmod --reference="$install_dir/${INSTALL_BINARIES[$index]}" "$staged_binary"
  else
    chmod 0755 "$staged_binary"
  fi
  validate_binary_file "$staged_binary"
  [[ "$(sha256_file "$staged_binary")" == "$(sha256_file "$source_binary")" ]] \
    || die "staged binary checksum mismatch: $staged_binary"
  version_output="$($staged_binary --version)"
  [[ "$version_output" == "${BUILD_BINARIES[$index]} $build_identity" ]] \
    || die "staged binary provenance mismatch: $staged_binary reported $version_output"
done
validate_set "$stage_dir" no
[[ "$(stat -c '%d' -- "$stage_dir")" == "$install_device" ]] \
  || die 'staged and installed sets are not on the same filesystem'

retire_installed_daemons "$runtime_root" "$installed_daemon"

if [[ "$installed_before" == "yes" ]]; then
  atomic_exchange "$stage_dir" "$install_dir"
  transaction="swapped-stage"
else
  mv -- "$stage_dir" "$install_dir"
  transaction="first-installed"
fi

validate_set "$install_dir" no
for index in "${!BUILD_BINARIES[@]}"; do
  installed_binary="$install_dir/${INSTALL_BINARIES[$index]}"
  version_output="$($installed_binary --version)"
  [[ "$version_output" == "${BUILD_BINARIES[$index]} $build_identity" ]] \
    || die "installed binary provenance mismatch: $installed_binary reported $version_output"
done

proof_root="$(mktemp -d /tmp/p408.XXXXXX)"
chmod 0700 "$proof_root"
mkdir -- "$proof_root/home" "$proof_root/runtime" "$proof_root/state" "$proof_root/workspace"
chmod 0700 "$proof_root/home" "$proof_root/runtime" "$proof_root/state" "$proof_root/workspace"
proof_socket="$proof_root/runtime/platonic/host/agent.sock"
printf 'proof endpoint: %s (%s bytes)\n' "$proof_socket" "${#proof_socket}"
((${#proof_socket} < 100)) || die 'new readback daemon endpoint exceeds 99 bytes'
(
  cd -- "$proof_root/workspace"
  exec env -i \
    HOME="$proof_root/home" \
    PATH="$PATH" \
    XDG_RUNTIME_DIR="$proof_root/runtime" \
    XDG_STATE_HOME="$proof_root/state" \
    "$installed_daemon" serve
) >"$proof_root/daemon.stdout" 2>"$proof_root/daemon.stderr" &
proof_pid=$!

attempts=100
while [[ ! -S "$proof_socket" ]] && ((attempts > 0)); do
  if ! kill -0 "$proof_pid" 2>/dev/null; then
    sed -n '1,120p' "$proof_root/daemon.stderr" >&2
    die 'new readback daemon exited before creating its socket'
  fi
  sleep 0.05
  attempts=$((attempts - 1))
done
[[ -S "$proof_socket" ]] || die 'new readback daemon did not create its socket within five seconds'
[[ "$(stat -Lc '%d:%i' -- "/proc/$proof_pid/exe")" == "$(stat -Lc '%d:%i' -- "$installed_daemon")" ]] \
  || die 'new readback process is not running the installed daemon executable'
proof_lock="$proof_root/runtime/platonic/host/agent.lock"
jq -e \
  --arg endpoint "$proof_socket" \
  --argjson pid "$proof_pid" \
  'keys == ["endpoint", "executable", "pid", "v"] and .v == 2 and .pid == $pid and .endpoint == $endpoint' \
  "$proof_lock" >/dev/null \
  || die 'new readback daemon did not publish exact host lock metadata'

hello_json="$(daemon_rpc "$proof_socket" "$proof_root/workspace" "$build_identity" "$proof_pid" hello)"
[[ "$(jq -er '.daemon_version' <<<"$hello_json")" == "$build_identity" ]] \
  || die 'new daemon hello did not report exact provenance'

env -i \
  HOME="$proof_root/home" \
  PATH="$PATH" \
  XDG_RUNTIME_DIR="$proof_root/runtime" \
  XDG_STATE_HOME="$proof_root/state" \
  "$install_dir/plato-tui-real" \
  --workspace "$proof_root/workspace" \
  --snapshot >"$proof_root/tui.snapshot"
readonly short_commit="${source_commit:0:7}"
grep -F -- "$package_version $short_commit $build_date" "$proof_root/tui.snapshot" >/dev/null \
  || die 'TUI snapshot did not render package version, short commit, and UTC build date'

daemon_rpc "$proof_socket" "$proof_root/workspace" "$build_identity" "$proof_pid" shutdown >/dev/null
wait_for_process_exit "$proof_pid" || die 'new readback daemon did not exit after shutdown acknowledgement'
printf 'readback: new_pid=%s hello=%s tui=%s %s %s\n' \
  "$proof_pid" "$build_identity" "$package_version" "$short_commit" "$build_date"
proof_pid=""

if [[ "$installed_before" == "yes" ]]; then
  if [[ -e "$rollback_dir" ]]; then
    atomic_exchange "$stage_dir" "$rollback_dir"
    transaction="staged-previous-rollback"
  else
    mv -- "$stage_dir" "$rollback_dir"
    stage_dir=""
    transaction="created-rollback"
  fi
  validate_set "$rollback_dir" no
fi

record_checksums after "$install_dir"
record_checksums rollback "$rollback_dir"
transaction="complete"
printf 'deployed: %s\n' "$build_identity"
printf 'installed: %s\n' "$install_dir"
if [[ "$installed_before" == "yes" ]]; then
  printf 'rollback: %s --rollback\n' "$repo_root/scripts/deploy-local.sh"
fi
