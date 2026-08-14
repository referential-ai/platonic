# Platonic

*by Referential.ai*

Platonic is a self-hosted agent server. One host server runs many registered
workspaces, agent profiles, and durable threads while owning provider calls,
tools, policy, approvals, and ledgers. Plato Agent is the client distribution
built on Platonic.

The public site is [referential.ai](https://referential.ai), and the canonical
documentation is [docs.referential.ai](https://docs.referential.ai/). The
workspace
[naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the product hierarchy and exact forms.

## Start here

- **Current public release (0.1.0):** install the supported command bundle from
  the concise [quickstart](docs/QUICKSTART.md), then use the
  [matching release documentation](https://github.com/referential-ai/platonic/blob/platonic-v0.1.0/docs/QUICKSTART.md).
- **Unreleased `develop` / 0.2.0:** read the Starlight
  [user overview](https://docs.referential.ai/user/) and complete the
  [first productive journey](https://docs.referential.ai/user/first-run/).
  No released 0.2.0 bundle or bundle-install proof exists; the journey uses
  exact-head local binaries and must not be published as current documentation
  until the `platonic-v0.2.0` release exists.
- **Release artifacts and verification:** use the
  [release contract](https://github.com/referential-ai/platonic/blob/develop/docs/RELEASE.md).

Daily operation, approvals, and provider guidance is in the
[User operations guide](https://docs.referential.ai/user/operations/).
Architecture, protocol, and ledger internals are in the
[Developer guide](https://docs.referential.ai/developer/).

<a id="configuration"></a>
<a id="discord-gateway"></a>
<a id="workspace-ledgers"></a>
<a id="server"></a>

## Reference routes

- [Configuration](https://docs.referential.ai/reference/configuration/)
- [Discord gateway](https://docs.referential.ai/user/operations/discord/)
- [Workspace ledgers](https://docs.referential.ai/developer/durability-and-replay/)
- [Server request lifecycle](https://docs.referential.ai/developer/runtime-boundaries/)

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

### Unreleased HTTP gateway

`platonic gateway http` exposes the bounded authenticated `/v1` HTTP/SSE
adapter on `127.0.0.1:8787` by default. It is plaintext and intended only for
a loopback hop behind an operator-owned TLS proxy. Generate a bearer token and
its configuration hash without persisting either value:

```bash
platonic gateway http --generate-token
```

Store only the emitted hash and fixed workspace scope in the canonical user
configuration at `$HOME/.config/plato/config.toml`:

```toml
[gateway.http]
bind = "127.0.0.1:8787"

[principals.http.remote_laptop]
name = "remote_laptop"
token_sha256 = ["<emitted lowercase SHA-256 hash>"]
workspace_ids = ["<server workspace id>"]
```

Then run `platonic gateway http`. A non-loopback bind additionally requires
`allow_non_loopback = true` or `--allow-non-loopback` and still requires
external TLS. The generated OpenAPI 3.1 contract is
[`openapi/gateway-v1.yaml`](openapi/gateway-v1.yaml).

### Unreleased Linux desktop observation

Platonic can expose screenshot-free, read-only X11/XWayland observation through
an operator-supplied `cua-driver 0.19.3` executable. The server neither installs
nor updates the driver. Enable both tools only from an explicit,
`PLATO_CONFIG`, or canonical user configuration:

```toml
[computer]
executable = "/absolute/path/to/cua-driver"

[tools]
enabled = [
  "file.read",
  "file.list",
  "computer.windows",
  "computer.observe",
]
```

Omit `computer.executable` to resolve `cua-driver` from the server process
`PATH`. Both tools remain disabled by default and require local approval;
native Wayland, macOS, Windows, screenshots, and desktop mutation are outside
this slice. The direct child uses Cua's `standard` permission mode; Platonic's
fixed method allowlist and approvals remain the authority boundary. Workspace
`plato.toml` files cannot configure or enable the tools.

## Boundary

`platonic-core` is an independent harness contract, not the Platonic product
server. See the repository
[agent guide](https://github.com/referential-ai/platonic/blob/develop/AGENTS.md)
for ownership rules.

## License

MIT OR Apache-2.0. See
[LICENSE-MIT](https://github.com/referential-ai/platonic/blob/develop/LICENSE-MIT)
and
[LICENSE-APACHE](https://github.com/referential-ai/platonic/blob/develop/LICENSE-APACHE).
