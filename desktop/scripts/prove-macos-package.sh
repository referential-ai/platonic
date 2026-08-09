#!/bin/bash

set -euo pipefail

test "$(uname -s)" = Darwin
test "$(uname -m)" = arm64
test "$(sw_vers -productVersion | cut -d. -f1)" -ge 14

desktop=$(cd "$(dirname "$0")/.." && pwd)
cd "$desktop"

app=${PLATONIC_MACOS_APP:?PLATONIC_MACOS_APP must name the packaged Plato.app}
app=$(cd "$(dirname "$app")" && pwd)/$(basename "$app")
main="$app/Contents/MacOS/plato-desktop"
sidecar="$app/Contents/MacOS/platonic"
test -x "$main"
test -x "$sidecar"

proof_root=/tmp/p167
smoke_root=/tmp/p167w
scratch_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/p167-package-proof"
for path in "$proof_root" "$smoke_root" "$scratch_root"; do
  test ! -e "$path"
done
mkdir -m 700 "$proof_root" "$smoke_root" "$scratch_root"
proof_root=$(cd "$proof_root" && pwd -P)
smoke_root=$(cd "$smoke_root" && pwd -P)
scratch_root=$(cd "$scratch_root" && pwd -P)

app_pid=
daemon_lock="$proof_root/runtime/platonic/host/agent.lock"
stop_owned_daemon() {
  test -f "$daemon_lock" || return 0
  daemon_pid=$(plutil -extract pid raw -o - "$daemon_lock" 2>/dev/null || true)
  case "$daemon_pid" in
    ''|*[!0-9]*) return 0 ;;
  esac
  command=$(ps -ww -p "$daemon_pid" -o command= 2>/dev/null || true)
  test "$command" = "$sidecar serve" || return 0
  kill "$daemon_pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    kill -0 "$daemon_pid" 2>/dev/null || return 0
    sleep 1
  done
  kill -KILL "$daemon_pid" 2>/dev/null || true
}
cleanup() {
  if test -n "$app_pid"; then
    kill "$app_pid" 2>/dev/null || true
    wait "$app_pid" 2>/dev/null || true
  fi
  stop_owned_daemon
  rm -rf "$proof_root" "$smoke_root" "$scratch_root"
}
trap cleanup EXIT

mkdir -m 700 "$proof_root/runtime" "$proof_root/state"
export XDG_RUNTIME_DIR="$proof_root/runtime"
export XDG_STATE_HOME="$proof_root/state"
socket="$XDG_RUNTIME_DIR/platonic/host/agent.sock"
printf 'socket=%s\nbytes=%s\n' "$socket" "${#socket}"
test "${#socket}" -lt 100

proof_bin="$scratch_root/path-bin"
proof_home="$scratch_root/home"
mkdir -m 700 "$proof_bin" "$proof_home"
cat > "$proof_bin/path-only-proof" <<'PROOF'
#!/bin/sh
if [ "${PLATO_APPIMAGE_PROOF_KEY+x}" = x ]; then
  echo 'scoped credential reached shell.exec' >&2
  exit 97
fi
printf PATH_ONLY_OK
PROOF
chmod 700 "$proof_bin/path-only-proof"
printf 'export PATH="%s:/usr/bin:/bin"\n' "$proof_bin" > "$proof_home/.zprofile"
chmod 600 "$proof_home/.zprofile"

cargo_home=${CARGO_HOME:-$HOME/.cargo}
rustup_home=${RUSTUP_HOME:-$HOME/.rustup}
clean_path="$cargo_home/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
HOME="$proof_home" \
  CARGO_HOME="$cargo_home" \
  RUSTUP_HOME="$rustup_home" \
  PATH="$clean_path" \
  SHELL=/bin/zsh \
  PLATO_APPIMAGE_PROOF_KEY=appimage-proof-dummy \
  PLATO_APPIMAGE_TEST_DAEMON="$sidecar" \
  cargo test --manifest-path src-tauri/Cargo.toml --locked \
    unix_proof::provisioned_unix_sidecar_lifecycle -- \
    --ignored --exact --nocapture --test-threads=1
HOME="$proof_home" \
  CARGO_HOME="$cargo_home" \
  RUSTUP_HOME="$rustup_home" \
  PATH="$clean_path" \
  SHELL=/bin/zsh \
  PLATO_APPIMAGE_PROOF_KEY=appimage-proof-dummy \
  PLATO_APPIMAGE_TEST_DAEMON="$sidecar" \
  PLATO_PACKAGED_PATH_COMMAND=path-only-proof \
  PLATO_PACKAGED_PATH_OUTPUT=PATH_ONLY_OK \
  cargo test --manifest-path src-tauri/Cargo.toml --locked \
    unix_proof::provisioned_unix_path_only_shell_exec -- \
    --ignored --exact --nocapture --test-threads=1

rm -rf "$proof_root"

mkdir -m 700 "$smoke_root/home" "$smoke_root/runtime" "$smoke_root/state"
HOME="$smoke_root/home" \
  XDG_RUNTIME_DIR="$smoke_root/runtime" \
  XDG_STATE_HOME="$smoke_root/state" \
  "$main" > "$scratch_root/wkwebview.log" 2>&1 &
app_pid=$!

cat > "$scratch_root/window.swift" <<'SWIFT'
import CoreGraphics
import Foundation

let pid = Int32(CommandLine.arguments[1])!
let deadline = Date().addingTimeInterval(20)
while Date() < deadline {
    let windows = CGWindowListCopyWindowInfo(
        [.optionOnScreenOnly, .excludeDesktopElements],
        kCGNullWindowID
    ) as! [[String: Any]]
    if let window = windows.first(where: { entry in
        let owner = (entry[kCGWindowOwnerPID as String] as? NSNumber)?.int32Value
        let layer = (entry[kCGWindowLayer as String] as? NSNumber)?.intValue
        return owner == pid && layer == 0
    }),
       let number = (window[kCGWindowNumber as String] as? NSNumber)?.uint32Value,
       let bounds = window[kCGWindowBounds as String] as? [String: Any],
       let width = (bounds["Width"] as? NSNumber)?.intValue,
       let height = (bounds["Height"] as? NSNumber)?.intValue {
        print("\(number) \(width) \(height)")
        exit(0)
    }
    Thread.sleep(forTimeInterval: 0.25)
}
fputs("no visible application window\n", stderr)
exit(1)
SWIFT

window_record=$(swift "$scratch_root/window.swift" "$app_pid")
read -r window_id window_width window_height <<< "$window_record"
for value in "$window_id" "$window_width" "$window_height"; do
  case "$value" in
    ''|*[!0-9]*) exit 1 ;;
  esac
done
test "$window_width" -ge 600
test "$window_height" -ge 400
kill -0 "$app_pid"
artifact_dir=${PLATONIC_MACOS_PROOF_DIR:-$desktop/artifacts/macos-wkwebview}
mkdir -p "$artifact_dir"
screenshot="$artifact_dir/workspace-selection.png"
workspace_count=$(find "$smoke_root/home" -type f -name workspace.json | wc -l | tr -d ' ')
test "$workspace_count" = 0
vmmap "$app_pid" > "$scratch_root/vmmap.txt"
webkit_image=$(awk '$NF ~ /\/WebKit\.framework\/Versions\/A\/WebKit$/ { print $NF; exit }' \
  "$scratch_root/vmmap.txt")
test -n "$webkit_image"

screenshot_status=unavailable
if screencapture -x -l "$window_id" "$screenshot" 2> "$scratch_root/screencapture.log" \
  && test -s "$screenshot" \
  && test "$(stat -f %z "$screenshot")" -gt 10000; then
  sips -g pixelWidth -g pixelHeight "$screenshot"
  screenshot_status=captured
else
  rm -f "$screenshot"
fi

cat > "$artifact_dir/native-window.txt" <<EOF
cold_launch=passed
window_visible=passed
window_id=$window_id
window_width=$window_width
window_height=$window_height
webkit_image=$webkit_image
fresh_workspace_state=absent
screenshot=$screenshot_status
EOF
