---
title: Configuration
description: Platonic configuration precedence and a representative Plato Agent provider block.
sidebar:
  order: 2
---

<p class="section-kicker reference">Reference</p>

## Resolution order

Plato Agent resolves configuration in this order:

1. `--config <path>`
2. `PLATO_CONFIG`
3. `./plato.toml`
4. `~/.config/plato/config.toml`
5. Built-in defaults

## Provider shape

```toml
[provider]
kind = "open_router"
model = "~openai/gpt-latest"
api_key_env = "OPENROUTER_API_KEY"
```

Keep credentials in the environment. The configuration names the environment variable; it does not contain the secret.

## Where to continue

Return to the [User docs](../../user/#start-a-durable-thread) for the operating path or [Developer docs](../../developer/#the-boundary) for runtime ownership.
