---
title: Optional Discord
description: Route Discord ingress to a durable Platonic thread without duplicating the gateway guide.
sidebar:
  order: 5
---

<p class="section-kicker user">User docs</p>

This page is part of the [unreleased 0.2.0 operating guide](../).

Discord is an optional server-owned ingress connector. It maps configured channels and principals to existing durable threads; it does not own sessions, approvals, provider calls, or run semantics.

## Prerequisite

Create and inspect the target workspace and thread locally first. Then complete the principal, channel, credential-environment, service, and proof steps in the [canonical Discord gateway guide](https://github.com/referential-ai/platonic/blob/develop/docs/GATEWAY.md).

## Start the connector

After that configuration is proven, the entry command is:

```bash
platonic gateway discord --workspace "$PWD" --config /path/to/gateway.toml
```

This connector is not a cross-host HTTP gateway and does not make `plato --remote` a network attachment command.

## Route failures

Use [core recovery](../history-and-recovery/) for the host server, workspace, provider, thread, or ledger. Use the gateway guide for Discord authentication, principal ceilings, channel mapping, ingress ordering, replies, and service management. Do not copy gateway credentials into core diagnostics.
