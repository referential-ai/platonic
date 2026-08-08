---
title: shell.exec Safety Boundary
issue: https://github.com/referential-ai/plato-agent/issues/51
---

> **PARTLY SUPERSEDED, 2026-08-07.** This design predates the platform
> decisions of 2026-08-06/07. The
> [platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
> supersedes it where they disagree, and the map wins.
>
> Confinement is now owned by plato-agent#437 (D008, Landlock-first with
> Seatbelt on macOS) and shaped by P020, which records intent in the authority
> record and the applied backend as a ledger event. P026 removes Windows from
> scope entirely.

# shell.exec Safety Boundary

## Authority
- Issue #51 owns this design task.
- Issue #44 Q5/Q6 accepted: every command requires approval, cwd is the workspace root, child env is scrubbed, provider credentials are not inherited, output is capped, `--yolo` does not cover `shell.exec`, no dedicated network tool.
- Issue #6 remains the yolo/network guardrail.
- `.plandoc/designs/hermes-light-product-spine.md` names `shell.exec` as the first non-file tool candidate.

## Source Grounding At Adoption

Historical snapshot; linked code and issues own current behavior.

- `src/app.rs`: `--yolo` currently auto-grants generic `RequireApproval`; `ApprovalRequest` already carries tool, effect, reason, and diff preview.
- `src/tool_catalog.rs`: only file tools are registered; unknown tools fail closed as `ExternalSideEffect`.
- `src/tools.rs`: tool execution is app-owned and already records bounded structured results.
- `platonic-core`: `ExternalSideEffect` defaults to `Deny`, but app policy may record `RequireApproval`.

## Desired Outcome
`shell.exec` lets Plato run one approved local command from the workspace root, with no provider credentials in the child environment and enough ledger evidence to replay what happened.

## Scope / Anchor Boundary
- Scope: `plato-agent` tool catalog, tool execution, approval preview, replay/readback, and docs for one-shot CLI and daemon/TUI approval flows.
- Anchor workflow: a user asks Plato to run one local build/test command in the current workspace, grants or denies approval, and can replay the result.
- Boundary: `platonic-core` remains unchanged; the app shell owns command execution and safety policy.

## Constraints
- Preserve the existing file-tool behavior.
- Keep command approval local; remote clients may deny or notify, not grant.
- Keep provider credentials indirect through config `api_key_env` and absent from child env.
- Keep JSONL/SQLite replay derived from existing harness events.

## Contract
- Effect class: `shell.exec` is recorded as `ExternalSideEffect`; enabled `shell.exec` gets an app-local `RequireApproval` policy reason instead of the core default deny.
- Yolo boundary: `--yolo` must never auto-grant `shell.exec`; the auto-grant path must inspect the tool name or an equivalent app-local approval class.
- Input shape: `{ "command": string, "timeout_seconds"?: integer }`; `timeout_seconds` defaults to 120 and is capped at 600.
- Cwd: the child process always runs from `workspace_root`; no per-call cwd is accepted in MVP.
- Approval preview: show command, cwd, timeout, effect, and env posture before execution.
- Environment: start from an empty env and copy only `PATH`, `HOME`, `USER`, `LOGNAME`, `SHELL`, `TERM`, `COLORTERM`, `NO_COLOR`, `LANG`, `LC_ALL`, `TMPDIR`, `TEMP`, `TMP`, `CARGO_HOME`, and `RUSTUP_HOME` when present.
- Credential scrub: drop any variable whose name contains `KEY`, `TOKEN`, `SECRET`, `PASSWORD`, `CREDENTIAL`, or `AUTH`, even if it appears in the allowlist.
- Provider credentials: configured provider `api_key_env` names are explicitly removed from the child env.
- Network posture: there is no network tool. Network access can happen only because the approved command performs it; hidden network behavior inside scripts remains part of the approved `shell.exec` risk.
- Output cap: keep stdout and stderr separately, capped at 32 KiB each with visible truncation markers.
- Result shape: record `command`, `cwd`, `exit_code`, `timed_out`, `duration_ms`, `stdout`, `stderr`, and truncation booleans in `ToolResult.data`.
- Nonzero exit: record `ToolFinished` with the structured result and send the model an error tool message.
- Timeout or cancel: terminate the child process, record `ToolFailed` with a clear reason, and leave the run terminal if cancellation ends the run.
- Replay/readback: existing tool-call, approval, tool-result, and tool-failed lines are sufficient; no new core event is needed.

## Non-Goals
- No implementation in this design PR.
- No dedicated network tool.
- No remote approval grants.
- No shell session or streaming process UI.
- No OS/container sandboxing requirement.
- No `platonic-core` changes.

## Forbidden Operations
- Do not classify `shell.exec` as `WorkspaceWrite` or `Network` if that lets `--yolo` inherit approval.
- Do not pass all parent process env vars to the child process.
- Do not pass provider credential variables to the child process.
- Do not add durable token/process streaming events for command output.

## Acceptance Criteria
- Every `shell.exec` call requires local approval, including under `--yolo`.
- Approval preview exposes command, cwd, timeout, effect, and env posture.
- Child processes cannot inherit provider credential env vars.
- stdout and stderr are capped with explicit truncation markers.
- Success, nonzero exit, timeout/cancel, grant, and denial are replayable.
- Network access is only via approved command text.

## Tests Required For Issue #52
- Catalog registers `shell.exec` with provider name `shell_exec`, effect `ExternalSideEffect`, and strict input schema.
- Enabled `shell.exec` evaluates to `RequireApproval`; disabled `shell.exec` denies.
- `--yolo` does not auto-grant `shell.exec`.
- Approval preview includes command, cwd, timeout, effect, and scrubbed-env posture.
- Child env contains only allowlisted non-credential names.
- Provider `api_key_env` variables are absent from child env.
- Command cwd is the workspace root.
- stdout and stderr caps add truncation markers independently.
- Exit code 0 records `ToolFinished` and returns structured stdout/stderr.
- Nonzero exit records `ToolFinished` and returns an error tool message.
- Timeout records `ToolFailed` and does not hang the run.
- Approval denial records `ApprovalDenied` and does not execute the command.
- Replay shows the tool call, approval decision, and final tool result or failure.

## Ownership Map
- Tool registration and provider name: `src/tool_catalog.rs`.
- Policy/yolo boundary and approval request: `src/app.rs`.
- Process execution, env scrub, output caps, timeout: `src/tools.rs`.
- Daemon approval events: `src/daemon/runtime.rs` and `src/daemon/handlers.rs`.
- TUI approval display: `src/tui/modal.rs`, `src/tui/render.rs`, `src/tui/app.rs`.
- Replay/readback: `src/replay.rs` plus `platonic-core` projections.

## Rollout / Security / Privacy
- Roll out in one #52 PR after this design is accepted.
- Prove with unit tests plus one scratch-workspace harmless command.
- Treat env scrub and yolo exclusion as security gates, not polish.
- Do not document provider key values; only provider key variable names may appear in tests or proofs.

## Assumptions
- MVP runs on the same local Unix-like host as the current CLI/daemon.
- Command execution uses a shell command string because the public tool is `shell.exec`.
- No sandbox is available for #52; approval, cwd, env scrub, timeout, and output caps are the MVP containment.
- The current `--yolo` behavior for file writes remains unchanged.

## Risks
- Hidden network behavior inside approved scripts cannot be detected without sandboxing.
- Long-running child processes can outlive the parent if cancellation is not implemented carefully.
- An allowlist that is too small can break build tools; an allowlist that is too large can leak secrets.

## Drift Rules
- If #52 changes the effect class, yolo boundary, env allowlist, output caps, or timeout shape, update this design or quote the new decision on #52 before coding.
- If a network-class tool is added later, link issue #6 again; this design does not decide general network-tool yolo semantics.

## Questions Recorded at Adoption
- None for #52 MVP implementation.

## Contract Neighborhood
- source artifacts: issues #6, #44, #51, #52; this design; product spine; MVP decisions.
- planned touch surface for #52: `src/tool_catalog.rs`, `src/tools.rs`, `src/app.rs`, daemon/TUI approval preview, replay tests, README/quickstart.
- upstream producers: provider tool proposal names, config `tools.enabled`, CLI/TUI approval modes.
- downstream consumers: ledger replay, daemon `approval_requested` events, TUI approval modal, final MVP proof #53.
- neighboring authorities: `plato-agent/AGENTS.md`, `docs/ARCHITECTURE.md`, `platonic-core` policy/tool/event contracts.
- proof surfaces: focused unit tests, replay tests, scratch-workspace command proof, `cargo fmt --check`, `cargo test --locked`, `cargo clippy --locked --all-targets -- -D warnings`.

Findings:
- N1 [pass]: no core event is needed because current tool/approval events already express the contract.
- N2 [pass]: `ExternalSideEffect` plus app-local approval avoids the #6 `Network`/`--yolo` inheritance risk.
- N3 [pass]: network remains visible only as approved command text; no hidden network grant surface is added.

Contradictions:
- C1 [resolved]: core defaults deny `ExternalSideEffect`; #52 must add an app-local enabled-tool override for `shell.exec`.

Boundary Decision:
- ready: #52 can implement this in `plato-agent` without touching `platonic-core`.

## Verifiable End Condition
A reviewer can implement #52 from this document without adding new safety decisions.

## Proof Expectations
- Docs-only PR linked to #51.
- `git diff --check`.
- Contradiction check against product spine, MVP decisions, #6, #44, #51, and #52.
