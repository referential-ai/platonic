# Contributing

## Before You Start

Open or select a GitHub issue first. The issue owns the problem, expected behavior, target surface, scope, non-goals, acceptance criteria, and proof.

Implementation starts only after the issue is **Ready for dev** and a maintainer admits it under the workspace-wide WIP cap of three, or a maintainer authorizes an equivalent bounded task. Do not change scope silently; propose revised acceptance criteria on the issue before coding.

## Pull Requests

- Keep the change focused and link its issue (`Closes #123` when appropriate).
- Post the exact test commands and any manual proof in the PR.
- Required CI must be green.
- Independent review is required.
- Only maintainers merge. Green CI and review report readiness; a human maintainer still decides whether to merge.

## Verify

Run the canonical battery described in `AGENTS.md`:

```bash
./scripts/quality.sh        # full battery: rust, desktop, web, duplication, security
./scripts/quality.sh rust   # single stage (see --help)
```
