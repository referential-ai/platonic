# Platonic releases

Downloadable command bundles are the sole Platonic product distribution
channel. Crate versions are independent implementation metadata: the product
release version is `0.1.0`, while `platonic-core` keeps its own public semver.
Cargo's structured `workspace.metadata.platonic-release` and each package's
`publish` value enforce [P029](https://github.com/referential-ai/platonic-workspace/issues/83):
`platonic-core` is the only publishable product-code crate. Internal and client
crates are not product publication channels.

## Identity

Release builds embed the exact source commit and the workflow's UTC build date.
The server command and daemon hello report:

```text
platonic 0.1.0 (<40-character commit>, <YYYY-MM-DD>)
```

The `plato` and `plato-tui` commands continue to report the independently
truthful Plato Agent package version with the same commit and date.

## Branch and tag

Normal work integrates through `develop`. For an admitted release, cut
`release/0.1.0` from the accepted `develop` commit and limit it to release
proof, versioning, packaging, and release-blocking fixes. Promote that branch
to `main` only through a human-authorized pull request, return any release-only
fix to `develop`, and do not retain a permanent release branch.

Tag the exact accepted `main` merge as `platonic-v0.1.0`. The historical
Plato Agent tag `v0.1.0` is never moved or deleted. The release workflow checks
that the tag, product metadata, requested source commit, and current `main`
commit are identical before it can create a draft release.

## Bundles

The locked artifact set is:

| Target | Rust target | Bundle |
| --- | --- | --- |
| Linux x86-64 | `x86_64-unknown-linux-gnu` | `platonic-0.1.0-linux-x86_64.tar.gz` |
| macOS Apple silicon | `aarch64-apple-darwin` | `platonic-0.1.0-macos-arm64.tar.gz` |

Each archive has one same-named root directory and exactly these ordered file
list entries:

```text
CHANGELOG.md
LICENSE-APACHE
LICENSE-MIT
bin/plato
bin/plato-tui
bin/platonic
```

Each bundle is accompanied by a same-stem `.files` inventory and `.sha256`
manifest. The manifest contains one GNU-compatible SHA-256 line for the
archive. Archive paths, modes, owners, timestamps, gzip headers, and ordering
are normalized by `scripts/package-release.py`.

## Workflow

`Platonic release` is manual so dry runs, tags, and artifact publication stay
human-gated. Dispatch it with an exact 40-character commit and an empty
`release_tag` for the two-target dry run. After the release branch is promoted
and the approved tag exists, dispatch the same exact commit with
`release_tag=platonic-v0.1.0`; the workflow validates the tag against `main`,
builds both bundles, verifies their manifests, and creates a draft GitHub
release for final human review.

The bare `platonic` crate belongs to an unrelated dormant CAD project. A polite
transfer request may be made after launch, but ownership of that crate name is
never a launch gate. The separately approved L4 reservation stubs and stale
`plato-agent` yanks are administration actions, not part of this release path.
