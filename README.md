# Platonic

*by Referential.ai*

Platonic is a self-hosted agent server. One host server runs many registered
workspaces, agent profiles, and durable threads while owning provider calls,
tools, policy, approvals, and ledgers. Plato Agent is the client distribution
built on Platonic.

The public site is [referential.ai](https://referential.ai). The workspace
[naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the product hierarchy and exact forms.

## Start here

- **Current public release (0.1.0):** install the supported command bundle from
  the concise [quickstart](docs/QUICKSTART.md), then use the
  [matching release documentation](https://github.com/referential-ai/platonic/blob/platonic-v0.1.0/docs/QUICKSTART.md).
- **Unreleased `develop` / 0.2.0:** read the Starlight
  [user overview](docs-site/src/content/docs/user/index.mdx) and complete the
  [first productive journey](docs-site/src/content/docs/user/first-run.md).
  No released 0.2.0 bundle or bundle-install proof exists; the journey uses
  exact-head local binaries and must not be published as current documentation
  until the `platonic-v0.2.0` release exists.
- **Release artifacts and verification:** use the
  [release contract](docs/RELEASE.md).

Daily operation, approvals, and provider guidance belongs to
[#548](https://github.com/referential-ai/platonic/issues/548). Architecture,
protocol, and ledger internals belong to
[#547](https://github.com/referential-ai/platonic/issues/547).

## Development

The repository is a Rust workspace pinned to Rust 1.88.0. Run the repository
battery from the root:

```bash
cargo fmt --check
cargo test --workspace --locked
cargo clippy --workspace --locked --all-targets -- -D warnings
cargo test --locked --manifest-path desktop/src-tauri/Cargo.toml
```

Build and check the Starlight documentation from `docs-site/`:

```bash
npm ci
npm run build
npm run crawl
```

## Boundary

`platonic-core` is an independent harness contract, not the Platonic product
server. See [AGENTS.md](AGENTS.md) for repository ownership rules.

## License

Apache-2.0. See [LICENSE](LICENSE).
