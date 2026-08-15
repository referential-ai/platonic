---
title: Reference
description: A compact lookup shell for Platonic commands, configuration, and terminology.
sidebar:
  label: Reference overview
  order: 1
---

<p class="section-kicker reference">Reference</p>

Use this section for exact names and interface shapes. Narrative operating guidance belongs in User docs; architecture and extension guidance belongs in Developer docs.

## Command families

| Command | Purpose |
| --- | --- |
| `platonic serve` | Run the persistent host server |
| `platonic workspace ...` | Register and inspect workspaces |
| `platonic profile ...` | Create, update, inspect, and open workspace profiles |
| `plato "..."` | Run a one-shot prompt |
| `plato replay` | Read and validate recorded work offline |

## Configuration

The [configuration shell](./configuration/#resolution-order) records precedence and a representative provider shape.

## Stable terms

- **Workspace:** a named, registered directory with one ledger.
- **Profile:** configured, revisioned defaults bound to one workspace.
- **Thread:** durable authority that carries runs.
- **Run:** one bounded execution recorded in the ledger.
