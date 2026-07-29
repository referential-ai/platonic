---
title: Gateway Readiness
issue: https://github.com/referential-ai/plato-agent/issues/93
---

# Gateway Readiness (Spine Slice 6)

## Authority
- Human direction 2026-07-09: proceed with the gateway design track.
- Human direction 2026-07-29: gateways are optional, explicitly started
  connector processes managed outside the daemon; issue #259 owns the service
  entry implementation.
- Issue #93 is the scope contract.
- Spine (`hermes-light-product-spine.md`): gateways are thin daemon clients, after the local spine; remote channels may notify or deny approvals, never grant.
- MVP decisions (`mvp-decisions.md`): remote channels must not become a grant surface.
- `docs/ARCHITECTURE.md`: connectors never own sessions, policy, approvals, provider fallback, or run semantics.

## Source Grounding At Adoption

Historical 2026-07-09 snapshot; linked code and issues own current behavior.

Checked at adoption:
- `src/daemon/protocol.rs`: envelope `v: 1` with strict equality reject; `deny_unknown_fields` on envelope and all param structs; typed error codes including `lagged`, `unsupported_method`, `unsupported_version`; `hello` returns `daemon_version` + `capabilities` (method list); `events.stream` is cursor polling (`from_offset` → `next_offset`); `approval.decide` and `transcript.read` exist; `message.append` takes `session_id`.
- `src/daemon/handlers.rs`: full method surface is `hello`, `run.start`, `message.append`, `events.stream`, `approval.decide`, `run.cancel`, `sessions.list`, `transcript.read`.
- `src/daemon/runtime.rs` (`approval_handler`): a pending approval waits indefinitely on a condvar; it resolves only by `approval.decide` or run cancel. No timeout exists.
- `src/daemon/server.rs` (`bind`): `create_dir_all` + `UnixListener::bind` with no explicit permissions; the non-systemd runtime fallback lives under `/tmp/plato-agent/$USER`.
- `src/tools.rs` / daemon flow: approval requests carry tool name, effect, and diff preview as transient events.

## Desired Outcome
The owner can message Plato from one remote channel and get answers from runs executed by the local workspace daemon, with the same ledger, policy, and approval semantics as the TUI — and no new grant surface.

## Definition
Gateway v1 is one new binary in this repo (boundary ladder: module/binary first, crate only on a second consumer). It is a daemon-protocol client exactly like `plato-tui`: it connects to the Unix socket, speaks NDJSON v1, and owns zero run semantics. It additionally connects outbound to one messaging platform.

## Decisions

### D1. Socket security
The daemon never listens on any network transport. Remote reach happens only via the gateway's outbound connection to the platform (long-poll/websocket). Trust model: any same-user local process may connect — unchanged from today.

Hardening (implementation slice, evidence above): create the socket parent dir `0700`, set the socket `0600` at bind, and refuse to start if either ends up wider. `SO_PEERCRED` uid assertion: deferred until a real second-user threat exists.

### D2. Protocol evolution
- Strictness stays: `deny_unknown_fields` fails closed and is a feature.
- Methods are additive; clients discover them via `hello.capabilities` before use. Unknown method → existing `unsupported_method` error.
- Adding a param field is a daemon-first upgrade: daemon must be upgraded before or with clients. Same host, same user, usually same build — document the order, do not engineer skew tolerance.
- `v` bumps only on envelope-shape breaks; strict-equality reject stays.
- Error codes are contract; new codes are additive (#89 adds the `sessions.list` code).

### D3. Pairing and identity
Single-operator v1. The gateway config holds one platform bot/app token (via env var, `api_key_env` pattern — never a config value) and an allowlist of owner platform-user ids. Messages from anyone else are silently ignored. The daemon keeps no principal model: every local socket client is the owner.

Recorded trigger: any multi-user or shared-channel ambition makes ingress identity semantic — per the core decision-boundary rule it must become a typed, recorded event, which requires a new design before implementation.

### D4. Event-stream recovery
The ledger is truth; deltas are ephemera (existing law). The gateway polls `events.stream` from its last `next_offset`.

Verified gaps (PR #94 review): `lagged` reports `first_offset` only inside the error message and omitted `from_offset` means 0, not tip (handlers.rs:307-318); a restarted daemon has an empty run map, so pre-restart run streams answer `not_found` (runtime.rs:24-29); `transcript.read` is store-backed but returns only a rendered string; `DaemonClient` flattens typed error codes into one string (client.rs:227-234).

Recovery therefore requires a minimal typed surface — three additive in-repo changes, daemon-first per D2 (result structs carry no `deny_unknown_fields`, so result-field additions are client-safe):
- `transcript.read` result gains `status` and nullable `final_answer` — already ledger-backed, so it answers for post-restart sessions.
- `events.stream` with `from_offset` omitted means tail-from-current-tip; `lagged` recovery is one re-call without an offset. Existing omit-callers must be audited before the default changes.
- `DaemonClient` preserves `ProtocolError { code, message }` as a typed error instead of flattening.

Contract: on reconnect or `lagged`, resume from tip and backfill nothing; on daemon restart, resync via `sessions.list` + typed `transcript.read` and reply from final ledger state. No server-side persistent cursors, no delivery guarantee for deltas; final answers are recoverable without parsing display text.

Session mapping: one remote channel/thread ↔ one daemon session, via the existing `message.append` `session_id`. `wait:false` (the #82 default) + polling; the gateway never blocks a connection on a run.

### D5. Remote approval posture
The gateway never sends an approve decision — structurally: the approve branch does not exist in gateway code. On an `approval_requested` event it notifies the channel (tool, effect, short preview) and states that granting happens locally in the TUI/CLI. The pending approval keeps waiting (verified: indefinite condvar wait); `run.cancel` remains the escape hatch. No approval timeout in v1 — silent auto-deny changes unattended run outcomes and would need its own recorded-policy design.

Optional, config-off by default: `remote_deny = true` lets allowlisted owners reply deny, relayed as `approval.decide` deny with reason `remote deny via <platform>`. Deny-only is safe: it can never cause a side effect.

### D6. Connector lifecycle
Gateways are optional per-channel connector processes. The operator starts each
connector explicitly (`plato gateway discord` for Discord); there is no generic
unqualified gateway entry. A gateway must verify the workspace daemon with
`hello` and fail closed with a daemon-start hint when it is unavailable. It
never starts, embeds, or supervises the daemon, and the daemon never embeds or
supervises a gateway. The existing `plato-agentd` and
`plato-gateway-discord` binary identities remain supported.

Long-lived connectors belong under the OS service manager. The two systemd user
unit templates below use a systemd-escaped absolute workspace path as the
instance and keep provider credentials out of the gateway environment:

```ini
# ~/.config/systemd/user/plato-daemon@.service
[Unit]
Description=Plato Agent daemon for %f

[Service]
Type=simple
WorkingDirectory=%f
EnvironmentFile=-%h/.config/plato/daemon.env
ExecStart=/absolute/path/to/plato daemon
Restart=on-failure

[Install]
WantedBy=default.target
```

```ini
# ~/.config/systemd/user/plato-gateway-discord@.service
[Unit]
Description=Plato Agent Discord gateway for %f
Wants=plato-daemon@%i.service
After=plato-daemon@%i.service

[Service]
Type=simple
WorkingDirectory=%f
EnvironmentFile=%h/.config/plato/discord.env
ExecStart=/absolute/path/to/plato gateway discord
Restart=on-failure

[Install]
WantedBy=default.target
```

`daemon.env` holds provider credentials; `discord.env` holds only the Discord
token. Start the connector with:

```bash
instance="$(systemd-escape --path "$PWD")"
systemctl --user enable --now "plato-gateway-discord@${instance}.service"
```

`Wants` and `After` start and order the daemon unit; they do not replace the
gateway's required `hello` readiness check. This process boundary keeps the
remote transport and platform credential outside the provider/policy authority
process. The pinned
[reference security study](https://github.com/referential-ai/platonic-core/blob/main/docs/TECHNICAL-LESSONS.md#security-study-codex-ironclaw-openfang-goose-2026-07-12)
validates the thin remote-adapter boundary and records two counterexamples where
remote surfaces inherited or bypassed approval authority.

## Human Decision Slots

### Q1. Platform target
One platform for v1. Recommended default: Telegram — single bot token, outbound long-poll, numeric-uid allowlist; smallest pairing surface (verify at implementation). Alternatives: Discord, Slack, SMS.

Jerome: default (Telegram). Accepted 2026-07-09 by ratifying the architecture-lane recommendation; recorded on issue #93.
Superseded 2026-07-10 by direct human choice: **Discord** — outbound websocket receive + REST send, still no inbound listener (D1 unchanged). Telegram skeleton PR #105 closed unmerged; Discord implementation is #106.

### Q2. Remote deny relay
Ship v1 notify-only, or include the config-off `remote_deny` relay? Recommended default: notify-only.

Jerome: default (notify-only). Accepted 2026-07-09 by ratifying the architecture-lane recommendation; recorded on issue #93.

## Constraints
- `platonic-core` unchanged; no new event variants for gateway v1.
- Gateway process runs without provider credentials — the daemon holds those. Its environment needs only the platform token.
- One gateway instance ↔ one workspace daemon.
- All existing daemon invariants hold: single writer, one active run per session, transient deltas.

## Non-Goals
- No remote approval grants, ever, in this design.
- No TCP/remote socket, no reverse tunnels, no inbound listeners.
- No multi-workspace routing, no multi-user support, no shared channels.
- No message-history sync into context beyond existing session hydration.
- No new core crates; no connector crate split.

## Forbidden Operations
- Gateway must not write SQLite, spawn runs outside the daemon protocol, hold sessions/policy/approvals/fallback/run semantics, or receive provider credentials.
- No platform tokens in config values or the ledger.
- Remote channels must never grant approval-required effects.

## Original Slice Boundaries

Linked issues and PRs own implementation and status.
1. Socket hardening: `0700` dir / `0600` socket enforced at bind, fail-closed test. (Independent of platform choice.)
2. Typed recovery surface + contract test: the three D4 changes, plus a daemon integration test proving the lag path and the restart path with typed assertions.
3. Gateway binary skeleton for the chosen platform: hello/capabilities check, allowlist filter, session map, `message.append` + polling, final-answer reply.
4. Approval notify relay (plus `remote_deny` only if Q2 accepts it).

No implementation starts from this document; each slice needs its own `Ready for dev` issue.

## Acceptance Criteria
- Both `Jerome:` slots answered; slice issues cut accordingly.
- D1–D6 hold as written or are amended here before coding.

## Verifiable End Condition
From the remote channel, the owner: sends a message, gets the final answer; triggers an approval-required effect, sees the notification, grants it locally in the TUI, sees the completed result remotely; a non-allowlisted sender gets nothing. `plato replay` shows one coherent session; the ledger shows no gateway-originated grant.

## Proof Expectations
- Slice tests per issue (`cargo test --locked`; daemon integration tests for recovery and notify paths).
- One scratch-workspace product proof with the real platform: message, answer, notify, local grant, ignore-stranger.

## Risks
- Platform SDK sprawl: keep the platform client thin; no framework adoption for one bot.
- Notification spam on busy runs: notify on approval and terminal states only, not per-event.
- The `/tmp` runtime fallback stays same-user-writable-parent even after hardening; slice 1 must verify the full path chain, not just the leaf.
