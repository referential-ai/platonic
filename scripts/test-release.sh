#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
readonly repo_root

"$script_dir/check-release-contract.sh" metadata
product_version="$("$script_dir/check-release-contract.sh" product-version)"
readonly product_version
metadata="$(cargo metadata --manifest-path "$repo_root/Cargo.toml" --locked --no-deps --format-version 1)"
release_tag="$(jq -er '.metadata["platonic-release"]["product-tag"]' <<<"$metadata")"
client_version="$(jq -er '.packages[] | select(.name == "plato-agent") | .version' <<<"$metadata")"
source_commit="$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')"
readonly release_tag client_version source_commit
readonly build_date="2026-08-09"

workflow="$repo_root/.github/workflows/platonic-release.yml"
readonly workflow
action_count=0
while IFS= read -r action; do
  [[ "$action" =~ ^[[:space:]]*uses:[[:space:]][^@[:space:]]+@[0-9a-f]{40}([[:space:]]+#[[:space:]].*)?$ ]] \
    || die "release workflow action is not full-SHA pinned: $action"
  action_count=$((action_count + 1))
done < <(grep -E '^[[:space:]]+uses:' "$workflow")
[[ "$action_count" -gt 0 ]] || die 'release workflow has no actions to audit'

attestation_action='        uses: actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8 # v4.2.2'
publish_job="$(sed -n '/^  publish:$/,$p' "$workflow")"
readonly attestation_action publish_job
[[ "$(grep -Fxc "$attestation_action" "$workflow")" -eq 1 ]] \
  || die 'release workflow must use the approved artifact-attestation action exactly once'
[[ "$(grep -Fxc "$attestation_action" <<<"$publish_job")" -eq 1 ]] \
  || die 'artifact attestation must remain inside the publish job'
[[ "$(grep -Fxc '  contents: read' "$workflow")" -eq 1 ]] \
  || die 'release workflow must retain its read-only default permission'
[[ "$(grep -Fxc '      contents: write' "$workflow")" -eq 1 ]] \
  || die 'release contents write permission must exist only on the publish job'
[[ "$(grep -Fxc '      contents: write' <<<"$publish_job")" -eq 1 ]] \
  || die 'release contents write permission must remain on the publish job'
[[ "$(grep -Fxc '      attestations: write' "$workflow")" -eq 1 ]] \
  || die 'attestations write permission must exist only on the publish job'
[[ "$(grep -Fxc '      attestations: write' <<<"$publish_job")" -eq 1 ]] \
  || die 'attestations write permission must remain on the publish job'
[[ "$(grep -Fxc '      id-token: write' "$workflow")" -eq 1 ]] \
  || die 'OIDC write permission must exist only on the publish job'
[[ "$(grep -Fxc '      id-token: write' <<<"$publish_job")" -eq 1 ]] \
  || die 'OIDC write permission must remain on the publish job'
[[ "$(grep -Fxc "    if: \${{ github.event_name == 'workflow_dispatch' && needs.validate.outputs.release_tag != '' }}" <<<"$publish_job")" -eq 1 ]] \
  || die 'publish permissions must remain unavailable to pull-request code'
[[ "$(grep -Fc 'test "$GITHUB_SHA" = "$SOURCE_COMMIT"' "$workflow")" -eq 2 ]] \
  || die 'tagged publication must bind the workflow run to the exact source commit'

mapfile -t attested_payloads < <(
  sed -n '/actions\/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8/,$p' "$workflow" \
    | sed -n 's/^[[:space:]]*\(incoming\/platonic-.*\)$/\1/p'
)
expected_attested_payloads=(
  'incoming/platonic-${{ needs.validate.outputs.product_version }}-linux-x86_64.files'
  'incoming/platonic-${{ needs.validate.outputs.product_version }}-linux-x86_64.sha256'
  'incoming/platonic-${{ needs.validate.outputs.product_version }}-linux-x86_64.tar.gz'
  'incoming/platonic-${{ needs.validate.outputs.product_version }}-macos-arm64.files'
  'incoming/platonic-${{ needs.validate.outputs.product_version }}-macos-arm64.sha256'
  'incoming/platonic-${{ needs.validate.outputs.product_version }}-macos-arm64.tar.gz'
)
diff -u \
  <(printf '%s\n' "${expected_attested_payloads[@]}") \
  <(printf '%s\n' "${attested_payloads[@]}") \
  || die 'release workflow must attest exactly the six locked payloads'

if output="$("$script_dir/check-release-contract.sh" tag "$release_tag" "$source_commit" 0000000000000000000000000000000000000000 2>&1)"; then
  die 'non-main release tag passed validation'
fi
grep -F 'release tag is not the exact main commit' <<<"$output" >/dev/null \
  || die "non-main tag rejection was not explicit: $output"

if output="$("$script_dir/check-release-contract.sh" tag platonic-v9.9.9 "$source_commit" "$source_commit" 2>&1)"; then
  die 'mismatched product version passed validation'
fi
grep -F 'release tag does not match Cargo product metadata' <<<"$output" >/dev/null \
  || die "product-version rejection was not explicit: $output"

proof_root="$(mktemp -d /tmp/p14-release.XXXXXX)"
trap 'rm -rf -- "$proof_root"' EXIT
mkdir -- "$proof_root/bin"
for binary in platonic plato plato-tui; do
  if [[ "$binary" == "platonic" ]]; then
    identity="$binary $product_version ($source_commit, $build_date)"
  else
    identity="$binary $client_version $source_commit $build_date"
  fi
  printf '#!/usr/bin/env bash\nprintf '\''%%s\\n'\'' '\''%s'\''\n' "$identity" >"$proof_root/bin/$binary"
  chmod 0755 "$proof_root/bin/$binary"
done

expected_files="$proof_root/expected.files"
printf '%s\n' \
  CHANGELOG.md \
  LICENSE-APACHE \
  LICENSE-MIT \
  bin/plato \
  bin/plato-tui \
  bin/platonic >"$expected_files"

for target in linux-x86_64 macos-arm64; do
  first="$proof_root/$target-first"
  second="$proof_root/$target-second"
  for output_dir in "$first" "$second"; do
    "$script_dir/package-release.py" \
      --target "$target" \
      --source-commit "$source_commit" \
      --build-date "$build_date" \
      --binary-dir "$proof_root/bin" \
      --output-dir "$output_dir" >/dev/null
  done

  bundle="platonic-$product_version-$target"
  diff -u "$expected_files" "$first/$bundle.files"
  cmp "$first/$bundle.files" "$second/$bundle.files"
  cmp "$first/$bundle.tar.gz" "$second/$bundle.tar.gz"
  cmp "$first/$bundle.sha256" "$second/$bundle.sha256"
  (cd -- "$first" && sha256sum -c "$bundle.sha256")

  expected_archive="$proof_root/$target.archive"
  {
    printf '%s/\n' "$bundle"
    printf '%s/bin/\n' "$bundle"
    sed "s|^|$bundle/|" "$expected_files"
  } >"$expected_archive"
  tar -tzf "$first/$bundle.tar.gz" >"$proof_root/$target.actual"
  diff -u "$expected_archive" "$proof_root/$target.actual"
done

printf 'release contract tests: workflow permissions/attestations, two target shapes, deterministic lists/manifests, and rejection paths passed\n'
