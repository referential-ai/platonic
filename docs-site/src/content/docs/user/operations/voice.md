---
title: Optional voice
description: Enter the local voice path without making audio a prerequisite for Plato Agent.
sidebar:
  order: 4
---

<p class="section-kicker user">User docs</p>

This page is part of the [unreleased 0.2.0 operating guide](../).

Voice is an optional, local Plato Agent TUI feature. It captures and plays audio in the client; the Platonic server does not receive or parse the separate voice configuration.

## Prerequisite

Complete the local model, device, and interruption proof in the [canonical voice quickstart](https://github.com/referential-ai/platonic/blob/develop/docs/QUICKSTART.md#5-local-voice-activation-and-device-proof). Voice requires explicit local model paths and may require exact capture and playback device IDs. Nothing is discovered or downloaded for you.

## Enter voice mode

Start the TUI with the proven client-only configuration:

```bash
plato --voice-config /path/to/voice.toml
```

Voice starts off. Use `/voice on` to grant local audio for the current client session and `/voice off` to drain and close it.

## Route failures

An invalid configuration fails closed. If activation, a model, a device, or interruption behavior fails, turn voice off and return to the linked quickstart's focused proof. Core text TUI, server, thread, and replay troubleshooting remains in [history and recovery](../history-and-recovery/).
