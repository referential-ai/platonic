---
title: Runtime boundaries
description: The ownership rules that keep Platonic run behavior in one server path.
sidebar:
  order: 2
---

<p class="section-kicker developer">Developer docs</p>

## One run-driving path

One-shot, daemon, TUI, gateway, and desktop surfaces share the server's run-driving implementation. A client must not fork model, tool, policy, approval, or ledger choreography.

## Durable authority

A workspace has a server-minted identity and one ledger. An agent is configured data bound to that workspace. A thread holds durable authority and can carry many runs.

```text
client request
  -> server authority
  -> harness request
  -> server effect
  -> ledger event
```

## Connector rule

Connectors translate ingress and egress. They do not own sessions, policy, approvals, provider fallback, or run outcomes.

Return to the [developer overview](../#the-boundary) or use the [reference shell](../../reference/) for exact interfaces.
