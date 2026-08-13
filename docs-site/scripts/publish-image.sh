#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'docs image publisher: %s\n' "$*" >&2
  exit 1
}

readonly registry='registry.digitalocean.com'
readonly repository='registry.digitalocean.com/kb-sf-repo-1/referential-docs'

[[ "$#" -eq 3 ]] \
  || die 'usage: publish-image.sh SOURCE_SHA IMAGE_REF ROLLBACK_PREDECESSOR_OR_NONE'
readonly source_sha="$1"
readonly image_ref="$2"
readonly rollback_predecessor="$3"

[[ "$source_sha" =~ ^[0-9a-f]{40}$ ]] \
  || die 'source SHA must be exactly 40 lowercase hexadecimal characters'
readonly expected_ref="$repository:$source_sha"
[[ "$image_ref" == "$expected_ref" ]] \
  || die "image reference must be exactly $expected_ref"
if [[ "$rollback_predecessor" != none ]]; then
  [[ "$rollback_predecessor" =~ ^registry\.digitalocean\.com/kb-sf-repo-1/referential-docs@sha256:[0-9a-f]{64}$ ]] \
    || die "rollback predecessor must be none or $repository@sha256:<64 lowercase hex characters>"
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
readonly repo_root
head_sha="$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')"
[[ "$head_sha" == "$source_sha" ]] \
  || die "checkout does not match requested source SHA (HEAD=$head_sha requested=$source_sha)"
[[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal)" ]] \
  || die 'source checkout is dirty'

for command in chmod date docker doctl git grep mktemp rm; do
  command -v "$command" >/dev/null 2>&1 || die "required command is unavailable: $command"
done

"$script_dir/smoke-container.sh" "$image_ref"

umask 077
docker_config="$(mktemp -d "${TMPDIR:-/tmp}/p559-docker.XXXXXX")"
chmod 0700 "$docker_config"
credentials_ready=0

cleanup_credentials() {
  local path="$docker_config"
  local failed=0
  unset DIGITALOCEAN_ACCESS_TOKEN
  if [[ -z "$path" ]]; then
    return 0
  fi
  if [[ "$credentials_ready" -eq 1 ]]; then
    DOCKER_CONFIG="$path" docker logout "$registry" >/dev/null 2>&1 || failed=1
  fi
  rm -rf -- "$path" || failed=1
  [[ ! -e "$path" ]] || failed=1
  docker_config=
  credentials_ready=0
  return "$failed"
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  if ! cleanup_credentials; then
    printf 'docs image publisher: credential cleanup failed\n' >&2
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if ! doctl registry docker-config --read-write --expiry-seconds 900 \
  >"$docker_config/config.json" 2>/dev/null; then
  die 'could not create short-lived registry credentials'
fi
[[ -s "$docker_config/config.json" ]] || die 'registry credential configuration is empty'
chmod 0600 "$docker_config/config.json"
credentials_ready=1
unset DIGITALOCEAN_ACCESS_TOKEN

push_log="$docker_config/push.log"
if ! DOCKER_CONFIG="$docker_config" docker push "$image_ref" >"$push_log" 2>&1; then
  die 'registry push failed'
fi
mapfile -t digest_lines < <(grep -E 'digest:' "$push_log" || true)
[[ "${#digest_lines[@]}" -eq 1 ]] \
  || die 'registry push did not return exactly one manifest digest'
[[ "${digest_lines[0]}" =~ digest:[[:space:]](sha256:[0-9a-f]{64})([[:space:]]|$) ]] \
  || die 'registry push returned a malformed manifest digest'
readonly digest="${BASH_REMATCH[1]}"
readonly digest_ref="$repository@$digest"

docker image rm "$image_ref" >/dev/null \
  || die 'could not remove the locally built tag before registry readback'
DOCKER_CONFIG="$docker_config" docker pull "$image_ref" >/dev/null \
  || die 'authenticated tag pull failed'
tag_digests="$(docker image inspect "$image_ref" --format '{{range .RepoDigests}}{{println .}}{{end}}')"
grep -Fx "$digest_ref" <<<"$tag_digests" >/dev/null \
  || die 'authenticated tag pull did not resolve to the pushed manifest digest'

docker image rm "$image_ref" >/dev/null \
  || die 'could not remove the tag readback before digest readback'
DOCKER_CONFIG="$docker_config" docker pull "$digest_ref" >/dev/null \
  || die 'authenticated digest pull failed'
digest_digests="$(docker image inspect "$digest_ref" --format '{{range .RepoDigests}}{{println .}}{{end}}')"
grep -Fx "$digest_ref" <<<"$digest_digests" >/dev/null \
  || die 'authenticated digest pull did not preserve the pushed manifest digest'

if ! cleanup_credentials; then
  trap - EXIT INT TERM
  die 'credential cleanup failed'
fi
trap - EXIT INT TERM

published_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '%s\n' \
  'Documentation image handoff' \
  "Source commit: $source_sha" \
  "Full-SHA tag: $image_ref" \
  "Immutable image: $digest_ref" \
  'Build/smoke proof: passed (docs-site/scripts/smoke-container.sh)' \
  "Publication timestamp: $published_at" \
  "Rollback predecessor: $rollback_predecessor"
