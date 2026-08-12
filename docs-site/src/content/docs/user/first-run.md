---
title: First run
description: The representative path from a workspace to a recorded Plato Agent run.
sidebar:
  order: 2
---

<p class="section-kicker user">User docs</p>

This page is a shell for the complete installation and onboarding guide. It keeps the stable product path visible while release-specific commands remain in the canonical quickstart.

## Before you start

- Install the matching Platonic command bundle.
- Export the provider credential named by your selected configuration.
- Open a Git repository you intend to register as a workspace.

## Run and inspect

```bash
platonic workspace create example "$PWD"
plato "describe this workspace"
plato replay
```

`plato replay` is read-only and offline. It validates and reads the recorded ledger without calling a model or tool.

## Next step

Review [configuration precedence](../../reference/configuration/#resolution-order) before adding a workspace-local `plato.toml`.
