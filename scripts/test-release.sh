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

printf 'release contract tests: two target shapes, deterministic lists/manifests, and rejection paths passed\n'
