#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: quality.sh [--help] [STAGE ...]

Canonical deterministic quality battery for humans, agents, and CI parity.
Each stage runs the exact command lines the CI jobs run on the pinned 1.88.0
toolchain. With no stages, all run in order; any failure fails the battery.

Stages:
  rust          format, test, clippy, docs, MSRV, release contract, package
  desktop       desktop/src-tauri format, test, clippy (needs GTK/webkit)
  web           knip, svelte-check, tests, build (desktop/), site check
  duplication   JSCPD token duplication gate (.jscpd.json)
  security      cargo audit on root and desktop lockfiles
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
readonly repo_root

need() {
  command -v "$1" >/dev/null 2>&1 || die "$1 not found; $2"
}

run() {
  printf '\n== %s\n' "$*"
  "$@"
}

stage_rust() {
  need cargo 'install the pinned 1.88.0 toolchain: rustup toolchain install 1.88.0 --profile minimal --component rustfmt,clippy'
  need rustup 'install rustup and the pinned toolchains'
  [[ "$(cargo clippy --version)" == *0.1.88* ]] \
    || die 'pinned toolchain mismatch: cargo clippy --version must report 0.1.88; a distribution cargo ignores rust-toolchain.toml'
  rustup toolchain list | grep -q '^1\.85\.0' \
    || die 'toolchain 1.85.0 not installed; rustup toolchain install 1.85.0 --profile minimal'
  (
    cd "$repo_root"
    run cargo fmt --all -- --check
    run cargo test --workspace --locked
    run cargo clippy --workspace --locked --all-targets -- -D warnings
    run env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --locked --no-deps
    run cargo +1.85.0 check --locked -p platonic-core
    run "$script_dir/test-release.sh"
    run cargo package --locked -p platonic-core
  )
}

stage_desktop() {
  need pkg-config 'install the desktop system libraries (see .github/workflows/desktop.yml)'
  pkg-config --exists gtk+-3.0 webkit2gtk-4.1 \
    || die 'desktop battery needs gtk+-3.0 and webkit2gtk-4.1 system libraries (see .github/workflows/desktop.yml)'
  (
    cd "$repo_root/desktop"
    run cargo fmt --manifest-path src-tauri/Cargo.toml --check
    run cargo test --manifest-path src-tauri/Cargo.toml --locked
    run cargo clippy --manifest-path src-tauri/Cargo.toml --locked --all-targets -- -D warnings
  )
}

stage_web() {
  need npm 'install Node >= 24'
  (
    cd "$repo_root/desktop"
    [[ -d node_modules ]] || run npm ci
    run npx --yes knip@6.32.1 --config knip.json
    run npm run check
    run npm test
    run npm run build
  )
  (
    cd "$repo_root/site"
    [[ -d node_modules ]] || run npm ci
    run npx --yes knip@6.32.1 --config knip.json
    run npm run check
  )
}

stage_duplication() {
  need npx 'install Node >= 24'
  need jq 'install jq'
  (
    cd "$repo_root"
    out="$(mktemp -d /tmp/quality-jscpd.XXXXXX)"
    trap 'rm -rf -- "$out"' EXIT
    run npx --yes jscpd@5.0.14 --config .jscpd.json --reporters console,json --output "$out" --no-colors
    threshold="$(jq -er '.threshold' .jscpd.json)"
    jq -e --argjson threshold "$threshold" \
      '.statistics.total.percentageTokens <= $threshold' \
      "$out/jscpd-report.json" >/dev/null \
      || die "duplication exceeds ${threshold}%"
    printf '\nduplication: %.2f%% <= %.2f%%\n' \
      "$(jq -r '.statistics.total.percentageTokens' "$out/jscpd-report.json")" \
      "$threshold"
  )
}

stage_security() {
  need cargo-audit 'install it pinned: cargo install cargo-audit --locked --version 0.22.0'
  (
    cd "$repo_root"
    run cargo audit --file Cargo.lock
    run cargo audit --file desktop/src-tauri/Cargo.lock
  )
}

case "${1:-}" in
  --help | -h)
    usage
    exit 0
    ;;
esac

stages=("$@")
[[ ${#stages[@]} -gt 0 ]] || stages=(rust desktop web duplication security)
for stage in "${stages[@]}"; do
  case "$stage" in
    rust) stage_rust ;;
    desktop) stage_desktop ;;
    web) stage_web ;;
    duplication) stage_duplication ;;
    security) stage_security ;;
    *) die "unknown stage: $stage (see quality.sh --help)" ;;
  esac
done

printf '\nquality battery passed: %s\n' "${stages[*]}"
