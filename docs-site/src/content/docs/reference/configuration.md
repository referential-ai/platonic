---
title: Configuration
description: Plato Agent configuration precedence, provider shapes, limits, tools, and host confinement controls.
sidebar:
  order: 2
---

<p class="section-kicker reference">Reference</p>

This page is part of the [unreleased 0.2.0 operating guide](../../user/operations/).

Configuration names environment variables that contain credentials. It never contains a credential value.

## Resolution order

For a workspace run, Plato Agent uses the first available source:

1. An explicit `--config FILE`
2. The path named by `PLATO_CONFIG`
3. `<workspace>/plato.toml`
4. `$HOME/.config/plato/config.toml`
5. Built-in defaults

An explicit or `PLATO_CONFIG` path may start with `~`; a relative path resolves from the workspace root. Sources are selected, not merged, and unknown fields are rejected. Environment-based resolution uses the environment of the server process.

A client-supplied `--config` path is carried by one-shot and unattached session runs. With `plato --tui --config FILE`, that file supplies the proposed root thread's model, but an attached durable-thread turn does not carry it in `thread.send`. Use `PLATO_CONFIG` in the server environment or user config for trusted provider settings that must govern attached turns. The thread's admitted model and toolset remain immutable.

The auto-discovered workspace `plato.toml` is untrusted. It cannot contain `provider.api_key_env`, `provider.base_url`, `[gateway]`, `[principals]`, `limits.max_spawn_depth`, or `[confinement]`. Put trusted provider fields in an explicitly selected config, the `PLATO_CONFIG` file, or user config. The [Discord operations page](../../user/operations/discord/) routes gateway and principal configuration to its detailed owner.

Server startup reads `limits.max_spawn_depth` and `confinement.require` only from `$HOME/.config/plato/config.toml`, then falls back to defaults. Restart an idle server after changing either field.

## Provider shapes

OpenRouter is the shipped default:

```toml
[provider]
kind = "open_router"
model = "~openai/gpt-latest"
api_key_env = "OPENROUTER_API_KEY"
```

The only other provider kind is the generic OpenAI-compatible shape. Its defaults call OpenAI directly:

```toml
[provider]
kind = "open_ai"
model = "gpt-5.5"
api_key_env = "OPENAI_API_KEY"
base_url = "https://api.openai.com/v1"
```

An explicitly trusted config can point that same `open_ai` shape at a custom or local OpenAI-compatible `/v1` endpoint:

```toml
[provider]
kind = "open_ai"
model = "local-model-name"
api_key_env = "LOCAL_OPENAI_API_KEY"
base_url = "http://127.0.0.1:8000/v1"
```

The server's provider client trims trailing slashes from `base_url` and posts completions to `<base_url>/chat/completions`.

The server reads the named variable from its own environment and sends it as the bearer credential. Set it outside TOML and restart the idle server when its environment changes. Do not print the variable during diagnosis.

`provider.model` is the requested model or alias. A newly admitted durable thread records that value immutably; changing config does not rewrite an existing thread. Status reports the requested value separately from the provider-reported served model, which may be unknown.

### Provider fields

| Field | Default | Meaning |
| --- | --- | --- |
| `provider.kind` | `open_router` | `open_router` or generic `open_ai` |
| `provider.model` | `~openai/gpt-latest` for OpenRouter; `gpt-5.5` for `open_ai` | Requested provider model or alias |
| `provider.api_key_env` | `OPENROUTER_API_KEY` or `OPENAI_API_KEY` | Name of the server-environment variable; forbidden in workspace config |
| `provider.base_url` | `https://openrouter.ai/api/v1` or `https://api.openai.com/v1` | API root; forbidden in workspace config |
| `provider.connect_timeout_ms` | `30000` | Positive connection and request-write timeout in milliseconds |
| `provider.stream_idle_timeout_ms` | `120000` | Positive timeout for each period without streaming progress |
| `provider.timeout_ms` | None | Legacy alias for `stream_idle_timeout_ms`; setting both is an error |
| `provider.http_referer` | None | Optional `HTTP-Referer` metadata header, normally used with OpenRouter |
| `provider.app_title` | None | Optional `X-OpenRouter-Title` metadata header, normally used with OpenRouter |

## Run limits

Every value must be positive.

| Field | Default | Meaning |
| --- | --- | --- |
| `limits.token_budget` | `4000` | Estimated context budget for one provider request |
| `limits.max_output_tokens` | `1024` | Requested response-token ceiling |
| `limits.max_turns` | `8` | Maximum model/tool steps in one run |
| `limits.max_spawn_depth` | `1` | Server ceiling for durable child-thread depth; user config and server restart only |

Example run limits:

```toml
[limits]
token_budget = 4000
max_output_tokens = 1024
max_turns = 8
```

## Enabled tools

The default toolset is:

```toml
[tools]
enabled = [
  "file.read",
  "file.list",
  "file.write",
  "file.edit",
  "shell.exec",
  "web.fetch",
]
```

The `tools.enabled` list must be nonempty and every name must be known. `thread.spawn` is implemented but is not enabled by default.

| Tool | Effect and operational control |
| --- | --- |
| `file.read`, `file.list` | Read-only and allowed when enabled |
| `file.write`, `file.edit` | Workspace write; prompts unless narrowly auto-granted by yolo |
| `shell.exec` | External side effect; exact shipped tool always enters local approval policy |
| `web.fetch` | Network; exact shipped tool always requires explicit local approval and never receives a yolo auto-grant |
| `thread.spawn` | Workspace write; also bounded by immutable parent authority and `max_spawn_depth` |

The root thread's resolved toolset determines whether it needs a writable path and whether it receives network authority. Removing `web.fetch` from the toolset prevents that tool and removes the network-effect source for newly admitted root threads. Existing thread authority does not change when TOML changes, and child threads cannot widen a parent's tools, paths, repositories, network, or approval policy.

## Worktrees and confinement

For a Git workspace, thread admission resolves a named repository and the server creates and owns its private worktree and branch. The recorded thread authority, not a free-form config path, controls its working directory and writable grants. Use `plato thread stop THREAD_ID` for the server-owned integration and cleanup path; do not move or delete private worktrees yourself.

On Linux, available Landlock support confines thread writes to the server-granted worktrees and paths. Other hosts record no filesystem confinement. `confinement.require` defaults to `false`. To reject new thread spawns when confinement is unavailable, set this only in user config and restart the server:

```toml
[confinement]
require = true
```

Confinement is an additional host boundary. It does not replace tool policy or approval decisions.

## Related guides

- [Daily operation](../../user/operations/)
- [Approvals](../../user/operations/approvals/)
- [History and recovery](../../user/operations/history-and-recovery/)
- [Optional Discord](../../user/operations/discord/) for the separate specialist gateway configuration
