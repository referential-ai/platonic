# Naming

## Authority

The workspace [naming authority](https://github.com/referential-ai/platonic-workspace/blob/main/product/branding.md)
owns the hierarchy and exact forms. This document only records core-specific
technical vocabulary.

Preferred GitHub layout:

```text
referential-ai/platonic-core
referential-ai/plato-agent
```

## Deeper internal vocabulary

These names are available for architecture docs and module concepts, not public marketing unless they prove useful.

- **Henad** — one isolated agent unit.
- **Telesterion** — execution chamber/runtime context.
- **Anaktoron** — secure core/state vault.
- **Epopteia** — trace/replay/audit view; the act of seeing what happened.
- **Legomena** — model messages/prompts; things said.
- **Dromena** — tool actions/side effects; things done.
- **Deiknumena** — artifacts/observations/evidence; things shown.
- **Aporrheta** — secrets and policy-denied material; things not to disclose.
- **Prohodos** — forward execution/procession.
- **Epistrophe** — replay/return/correction loop.

Technical identities remain unchanged. All code stays generic (`AgentId`, not
`HenadId`; `Error`, not `PlatonicError`). Deep terms are documentation color
only — never type names, fields, or serialized values.
