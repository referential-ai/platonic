---
title: Optional Discord
description: Route Discord ingress to a durable Platonic thread without duplicating the gateway guide.
sidebar:
  order: 5
---

<span id="platonic-discord-gateway-first-reply-and-replay"></span>

<p class="section-kicker user">User docs</p>

This page is part of the [Platonic 0.2.2 operating guide](../).

Discord is an optional server-owned ingress connector. It maps configured channels and principals to existing durable threads; it does not own sessions, approvals, provider calls, or run semantics.

<span id="1-create-and-install-the-discord-bot"></span>
<span id="2-store-the-token-at-the-approved-path"></span>
<span id="3-add-the-principal-and-channel-context-map"></span>

## Prerequisite

Create and inspect the target workspace and thread locally first. Then complete the principal, channel, credential-environment, service, and proof steps in the [canonical Discord gateway guide](https://github.com/referential-ai/platonic/blob/develop/docs/GATEWAY.md).

<span id="4-start-the-server-and-gateway"></span>
<span id="5-receive-the-first-reply"></span>
<span id="gateway-approvals"></span>
<span id="6-replay-the-reply"></span>

## Start the connector

After that configuration is proven, the entry command is:

```bash
platonic gateway discord --workspace "$PWD" --config /path/to/gateway.toml
```

This connector is not a cross-host HTTP gateway and does not make `plato --remote` a network attachment command.

<span id="troubleshooting"></span>
<span id="bot-ignores-the-principal-wrong-user-id"></span>
<span id="bot-ignores-the-principal-channel-is-not-mapped"></span>
<span id="gateway-closes-with-code-4014-message-content-intent-is-disabled"></span>

## Route failures

Use [core recovery](../history-and-recovery/) for the host server, workspace, provider, thread, or ledger. Use the gateway guide for Discord authentication, principal ceilings, channel mapping, ingress ordering, replies, and service management. Do not copy gateway credentials into core diagnostics.
