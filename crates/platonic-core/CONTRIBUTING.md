# Contributing

## Before coding

- Open or select a GitHub issue. The issue is the authority for scope, non-goals, acceptance, and proof.
- Implementation starts only after the issue is `Ready for dev` and admitted by a maintainer. Active workspace WIP is capped at three items.
- Do not change scope silently. Propose changed acceptance criteria on the issue first.

## Pull requests

- Link the issue and keep the change focused.
- Post focused proof for the acceptance criteria.
- Run the required checks:

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

- Required CI and an independent review must be green.
- Only maintainers merge, and only after explicit human approval.
