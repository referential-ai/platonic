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

metadata() {
  cargo metadata \
    --manifest-path "$repo_root/Cargo.toml" \
    --locked \
    --no-deps \
    --format-version 1
}

check_metadata() {
  local cargo_metadata
  cargo_metadata="$(metadata)"
  jq -e '
    .metadata["platonic-release"] as $release
    | ($release["product-version"] == "0.2.0")
      and ($release["product-tag"] == "platonic-v0.2.0")
      and ($release["product-channel"] == "bundles")
      and ($release["public-code-crates"] == ["platonic-core"])
      and ($release["bundle-targets"] == ["linux-x86_64", "macos-arm64"])
      and ($release["bundle-binaries"] == ["platonic", "plato", "plato-tui"])
      and (
        [.packages[] | select(.publish != []) | .name] | sort
        == ($release["public-code-crates"] | sort)
      )
      and (
        [.packages[] | select(.name == "platonic-core") | .publish]
        == [["crates-io"]]
      )
  ' <<<"$cargo_metadata" >/dev/null \
    || die 'Cargo metadata violates the Platonic release or P029 publication contract'
  printf 'release metadata: product=0.2.0 tag=platonic-v0.2.0 channel=bundles public-code=platonic-core\n'
}

product_version() {
  metadata | jq -er '.metadata["platonic-release"]["product-version"]'
}

validate_commit() {
  local requested_commit="$1"
  [[ "$requested_commit" =~ ^[0-9a-f]{40}$ ]] \
    || die "source commit must be a full lowercase Git object id: $requested_commit"
  local head
  head="$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')"
  [[ "$head" == "$requested_commit" ]] \
    || die "checkout does not match requested source commit (HEAD=$head requested=$requested_commit)"
  [[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal)" ]] \
    || die 'exact-commit release checkout is dirty'
  printf 'source commit: %s\n' "$requested_commit"
}

validate_tag() {
  local release_tag="$1"
  local tagged_commit="$2"
  local main_commit="$3"
  local expected_tag
  expected_tag="$(metadata | jq -er '.metadata["platonic-release"]["product-tag"]')"
  [[ "$release_tag" == "$expected_tag" ]] \
    || die "release tag does not match Cargo product metadata (expected=$expected_tag actual=$release_tag)"
  [[ "$tagged_commit" =~ ^[0-9a-f]{40}$ ]] \
    || die "tagged commit must be a full lowercase Git object id: $tagged_commit"
  [[ "$main_commit" =~ ^[0-9a-f]{40}$ ]] \
    || die "main commit must be a full lowercase Git object id: $main_commit"
  [[ "$tagged_commit" == "$main_commit" ]] \
    || die "release tag is not the exact main commit (tag=$tagged_commit main=$main_commit)"
  printf 'release tag: %s commit=%s\n' "$release_tag" "$tagged_commit"
}

case "${1:-}" in
  metadata)
    [[ "$#" -eq 1 ]] || die 'usage: check-release-contract.sh metadata'
    check_metadata
    ;;
  product-version)
    [[ "$#" -eq 1 ]] || die 'usage: check-release-contract.sh product-version'
    product_version
    ;;
  commit)
    [[ "$#" -eq 2 ]] || die 'usage: check-release-contract.sh commit COMMIT'
    validate_commit "$2"
    ;;
  tag)
    [[ "$#" -eq 4 ]] || die 'usage: check-release-contract.sh tag TAG TAG_COMMIT MAIN_COMMIT'
    validate_tag "$2" "$3" "$4"
    ;;
  *)
    die 'usage: check-release-contract.sh {metadata|product-version|commit COMMIT|tag TAG TAG_COMMIT MAIN_COMMIT}'
    ;;
esac
