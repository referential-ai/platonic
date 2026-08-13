#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse --show-toplevel)
capture_sources=(
  Cargo.toml
  Cargo.lock
  rust-toolchain.toml
  crates/plato-tui
  crates/plato-agent/Cargo.toml
  crates/plato-agent/src/bin/plato-tui.rs
  crates/plato-agent/src/tui
  crates/platonic-core
  crates/platonic-protocol
  scripts/capture-tui-docs.sh
)

cd "$repo_root"
if [[ -n $(git status --porcelain -- "${capture_sources[@]}") ]]; then
  printf '%s\n' 'capture inputs must be committed before recording provenance' >&2
  exit 1
fi

source_commit=$(git log -1 --format=%H -- "${capture_sources[@]}")
target_dir=${CARGO_TARGET_DIR:-"$repo_root/target"}
if [[ $target_dir != /* ]]; then
  target_dir="$repo_root/$target_dir"
fi

cargo build --locked --offline --package plato-agent --bin plato-tui
PLATO_TUI_DOC_OUTPUT_DIR="$repo_root/docs-site/src/assets/tui" \
PLATO_TUI_DOC_SOURCE_COMMIT="$source_commit" \
PLATO_TUI_DOC_BINARY="$target_dir/debug/plato-tui" \
  cargo test --locked --offline --package plato-tui --lib \
    render::doc_capture::write_documentation_assets -- \
    --exact --ignored --nocapture
