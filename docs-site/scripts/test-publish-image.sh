#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'docs publisher test: %s\n' "$*" >&2
  exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
readonly repo_root
workflow="$repo_root/.github/workflows/docs-site.yml"
readonly workflow

[[ "$(grep -Fxc '  contents: read' "$workflow")" -eq 1 ]] \
  || die 'docs workflow must retain one read-only permission declaration'
if grep -Eq '\$\{\{[[:space:]]*secrets\.|DIGITALOCEAN|docker[[:space:]]+(login|push)|doctl' "$workflow"; then
  die 'docs workflow must not contain publication credentials or registry-write commands'
fi

umask 077
proof_root="$(mktemp -d "${TMPDIR:-/tmp}/p559-publisher.XXXXXX")"
chmod 0700 "$proof_root"
readonly proof_root
trap 'rm -rf -- "$proof_root"' EXIT

fixture="$proof_root/repository"
fake_bin="$proof_root/bin"
call_log="$proof_root/calls"
mkdir -p "$fixture/docs-site/scripts" "$fake_bin"
cp "$script_dir/publish-image.sh" "$fixture/docs-site/scripts/publish-image.sh"

cat >"$fixture/docs-site/scripts/smoke-container.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'smoke|%s\n' "$1" >>"$CALL_LOG"
if [[ "${FAIL_SMOKE:-0}" -eq 1 ]]; then
  printf 'fixture smoke failed\n' >&2
  exit 1
fi
EOF
chmod 0755 "$fixture/docs-site/scripts/"*.sh

cat >"$fake_bin/doctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'doctl|%s\n' "$*" >>"$CALL_LOG"
[[ "$*" == 'registry docker-config --read-write --expiry-seconds 900' ]]
[[ "${DIGITALOCEAN_ACCESS_TOKEN:-}" == 'do-not-print-fixture-value' ]]
printf '{"auths":{"registry.digitalocean.com":{"auth":"invalid-fixture-value"}}}\n'
EOF

cat >"$fake_bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker|%s|%s\n' "$*" "${DOCKER_CONFIG:-}" >>"$CALL_LOG"

case "${1:-} ${2:-}" in
  'image rm')
    exit 0
    ;;
  'image inspect')
    printf '%s\n' "$EXPECTED_DIGEST_REF"
    ;;
  'push '*)
    [[ -n "${DOCKER_CONFIG:-}" && -s "$DOCKER_CONFIG/config.json" ]]
    [[ -z "${DIGITALOCEAN_ACCESS_TOKEN+x}" ]]
    if [[ "${TEST_MODE:-}" == malformed-digest ]]; then
      printf '%s\n' 'pushed: digest: sha256:not-a-digest size: 1'
    else
      printf 'pushed: digest: %s size: 1\n' "${EXPECTED_DIGEST_REF##*@}"
    fi
    ;;
  'pull '*)
    [[ -n "${DOCKER_CONFIG:-}" && -s "$DOCKER_CONFIG/config.json" ]]
    [[ -z "${DIGITALOCEAN_ACCESS_TOKEN+x}" ]]
    ;;
  'logout registry.digitalocean.com')
    [[ -n "${DOCKER_CONFIG:-}" && -s "$DOCKER_CONFIG/config.json" ]]
    [[ "${TEST_MODE:-}" != cleanup-failure ]]
    ;;
  *)
    printf 'unexpected fake docker command: %s\n' "$*" >&2
    exit 2
    ;;
esac
EOF
chmod 0755 "$fake_bin/doctl" "$fake_bin/docker"

git -c init.defaultBranch=fixture -C "$fixture" init -q
git -C "$fixture" add docs-site/scripts
git -C "$fixture" \
  -c user.name='Docs Publisher Test' \
  -c user.email='docs-publisher-test@invalid' \
  commit --no-gpg-sign -q -m fixture

source_sha="$(git -C "$fixture" rev-parse HEAD)"
readonly source_sha
repository='registry.digitalocean.com/kb-sf-repo-1/referential-docs'
readonly repository
image_ref="$repository:$source_sha"
readonly image_ref
digest='sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
readonly digest
digest_ref="$repository@$digest"
readonly digest_ref
publisher="$fixture/docs-site/scripts/publish-image.sh"
readonly publisher

output=
status=0
run_publisher() {
  local mode="$1"
  shift
  : >"$call_log"
  set +e
  output="$(
    cd -- "$fixture"
    env \
      PATH="$fake_bin:$PATH" \
      CALL_LOG="$call_log" \
      DIGITALOCEAN_ACCESS_TOKEN='do-not-print-fixture-value' \
      EXPECTED_DIGEST_REF="$digest_ref" \
      TEST_MODE="$mode" \
      "$publisher" "$@" 2>&1
  )"
  status=$?
  set -e
  [[ "$output" != *invalid-fixture-value* && "$output" != *do-not-print-fixture-value* ]] \
    || die 'publisher printed credential material'
}

expect_failure() {
  local name="$1"
  local diagnostic="$2"
  local mode="$3"
  shift 3
  run_publisher "$mode" "$@"
  [[ "$status" -ne 0 ]] || die "$name unexpectedly passed"
  grep -F "$diagnostic" <<<"$output" >/dev/null \
    || die "$name did not report '$diagnostic': $output"
  printf 'publisher negative passed: %s\n' "$name"
}

expect_no_registry_command() {
  if grep -Eq '^(doctl|docker)\|' "$call_log"; then
    die "$1 reached a registry-capable command"
  fi
}

assert_configs_removed() {
  local config
  while IFS= read -r config; do
    [[ -z "$config" || ! -e "$config" ]] || die "temporary Docker config remains: $config"
  done < <(awk -F'|' '$1 == "docker" && $3 != "" { print $3 }' "$call_log" | sort -u)
}

expect_failure 'non-40-character SHA' 'source SHA must be exactly 40' normal \
  short "$repository:short" none
expect_no_registry_command 'non-40-character SHA'

wrong_sha='0000000000000000000000000000000000000000'
expect_failure 'wrong source commit' 'checkout does not match requested source SHA' normal \
  "$wrong_sha" "$repository:$wrong_sha" none
expect_no_registry_command 'wrong source commit'

expect_failure 'wrong repository' 'image reference must be exactly' normal \
  "$source_sha" "registry.digitalocean.com/kb-sf-repo-1/wrong:$source_sha" none
expect_no_registry_command 'wrong repository'

expect_failure 'mutable tag' 'image reference must be exactly' normal \
  "$source_sha" "$repository:latest" none
expect_no_registry_command 'mutable tag'

expect_failure 'wrong rollback repository' 'rollback predecessor must be none' normal \
  "$source_sha" "$image_ref" \
  'registry.digitalocean.com/kb-sf-repo-1/wrong@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
expect_no_registry_command 'wrong rollback repository'

touch "$fixture/untracked"
expect_failure 'dirty checkout' 'source checkout is dirty' normal \
  "$source_sha" "$image_ref" none
rm -- "$fixture/untracked"
expect_no_registry_command 'dirty checkout'

FAIL_SMOKE=1
export FAIL_SMOKE
expect_failure 'failed verification' 'fixture smoke failed' normal \
  "$source_sha" "$image_ref" none
unset FAIL_SMOKE
expect_no_registry_command 'failed verification'

expect_failure 'malformed digest output' 'malformed manifest digest' malformed-digest \
  "$source_sha" "$image_ref" none
grep -F 'docker|logout registry.digitalocean.com|' "$call_log" >/dev/null \
  || die 'malformed digest failure did not log out'
if grep -F 'docker|pull ' "$call_log" >/dev/null; then
  die 'malformed digest failure reached registry readback'
fi
assert_configs_removed

expect_failure 'credential cleanup failure' 'credential cleanup failed' cleanup-failure \
  "$source_sha" "$image_ref" none
[[ "$output" != *'Documentation image handoff'* ]] \
  || die 'cleanup failure emitted a successful handoff'
assert_configs_removed

run_publisher normal "$source_sha" "$image_ref" none
[[ "$status" -eq 0 ]] || die "simulated publication failed: $output"
grep -F "Source commit: $source_sha" <<<"$output" >/dev/null
grep -F "Full-SHA tag: $image_ref" <<<"$output" >/dev/null
grep -F "Immutable image: $digest_ref" <<<"$output" >/dev/null
grep -F 'Build/smoke proof: passed' <<<"$output" >/dev/null
grep -E '^Publication timestamp: [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' \
  <<<"$output" >/dev/null
grep -F 'Rollback predecessor: none' <<<"$output" >/dev/null
[[ "$(grep -Ec '^docker\|pull ' "$call_log")" -eq 2 ]] \
  || die 'simulated publication did not perform tag and digest readback'
grep -F 'docker|logout registry.digitalocean.com|' "$call_log" >/dev/null \
  || die 'simulated publication did not log out'
assert_configs_removed

printf 'docs publisher contract passed: CI is read-only; validation, verification, digest, readback, handoff, secret redaction, and cleanup paths are fail-closed\n'
