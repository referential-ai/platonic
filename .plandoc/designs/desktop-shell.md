---
title: Desktop Shell
issue: https://github.com/referential-ai/plato-agent/issues/139
---

> **PARTLY SUPERSEDED, 2026-08-07.** This design predates the platform
> decisions of 2026-08-06/07. The
> [platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
> supersedes it where they disagree, and the map wins.
>
> The Tauri client sits on Platonic, not on `plato-agentd` as the product
> (P016, P018). P026 withdraws Windows, so the signed-Windows path in this
> design is out of scope; macOS distribution remains (#167).

# Plato Desktop Shell (Tauri + Web UI over plato-agentd)

Revision 6 — **Active**; adopted 2026-07-13 by explicit human adopt/merge instruction after clean lead closure (PR #140). Rev 2 folded R1–R9 (direction confirmed: Tauri client, shared daemon ownership, in-repo home, Windows named pipes); rev 3 added cross-platform scope per human direction (Windows, macOS, Linux all ship; Windows first); rev 4 folded C10–C12; rev 5 folded C13–C16; rev 6 folded C17 (C18 reconciled on issue #139). Implementation still starts only on `Ready for dev` child issues owned by #139.

## Authority
- Human direction 2026-07-12: Codex-desktop-style app; Tauri accepted; web UI in the stack already in use (Svelte 5 / Tailwind 4 / shadcn-svelte); distribution must feel natural to a Windows user; C# path declined.
- Human direction 2026-07-12 (rev 3): the app must work on macOS, Linux, and Windows — all three are shipping targets; Windows first.
- Parent: `docs/ARCHITECTURE.md` runtime topology — daemon owns sessions, providers, tools, approvals; clients render only; connector rule binding (ARCHITECTURE.md:10-22).
- Lead closure reviews 2026-07-12/13 (PR #140: R1–R9, C10–C18) are design input.

## Source Grounding (main @ 3f02adb)
- Daemon protocol v1, NDJSON envelopes; methods `hello`, `run.start`, `message.append`, `events.stream`, `approval.decide`, `run.cancel`, `sessions.list`, `transcript.read` (src/daemon/handlers.rs:39-54, src/daemon/protocol.rs:5). `run.start`/`message.append` default `wait: false` (ARCHITECTURE.md:20).
- `TranscriptReadResult.transcript` is preformatted CLI replay text; the TUI recognizes entries by display prefixes (src/daemon/protocol.rs:227-242, src/daemon/handlers.rs:495-548, src/replay.rs:34-97, src/tui/render.rs:355-407). No typed history exists on the wire today (R1).
- The event buffer is transient and capped at 256; `lagged` + omitted `from_offset` resumes after the current tip; pending-approval truth lives only in daemon memory/transient events; `transcript.read` has no pending snapshot (src/daemon/runtime.rs:17,42,93-122,151-195; src/daemon/handlers.rs:277-327) (R2).
- Client precedent: `plato --tui` already attaches-or-starts and stops only its embedded daemon; only raw `plato-tui` is never-spawn (src/bin/plato.rs:126-180,230-249). Daemon bind/lock are atomic arbiters; stale locks are never reclaimed (src/daemon/server.rs:65-75, src/daemon/lock.rs:72-115,119-130,200-213) (R4).
- Transport and process model are Unix-only (std UDS, signals, process groups, HOME/XDG paths); the daemon does not build natively on Windows today (src/daemon/client.rs:15-27, src/daemon/server.rs:9-10, src/bin/plato-agentd.rs:7,47, src/tools.rs:317-326, src/paths.rs:83-102, src/config.rs:274-309) (R9).
- `plato replay` fails closed while the daemon lock is held (README.md:75) (R9).
- The Unix-only daemon code is `cfg(unix)`-shaped, and the runtime/state paths are XDG-with-fallback (README.md Daemon; src/paths.rs:83-102), so macOS is expected to build and run on the existing UDS path unverified today; phase 6 verifies. Windows is the only true port.
- Live assistant deltas are transient `events.stream` events; ledger `model_responded` stays replay truth (ARCHITECTURE.md:21). Thread-per-connection already serves multiple clients, but no client registry, lease, or connection count is exposed to clients (src/daemon/server.rs:93-105) (C10).
- `plato` derives the workspace from process cwd; `hello` requires the canonical workspace root/id — a Start-menu launch has neither (src/bin/plato.rs:94-99, src/daemon/protocol.rs:126-139) (C11).
- Graceful daemon exit is Unix-only (SIGINT/SIGTERM plus a UDS wake, src/bin/plato-agentd.rs:39-48); an abrupt Windows kill skips `WorkspaceLock::drop`, leaving fail-closed stale locks (src/daemon/lock.rs:119-130); Tauri's NSIS uninstall does not stop a detached external binary (C13).
- GUI-launched packaged apps do not inherit the shell-dotfile `$PATH`, and `shell.exec` scrubs the child environment, restoring only the PATH it inherited (src/tools.rs:304-325,408-420) (C15).
- Lock metadata already carries workspace, socket, PID, and executable (src/daemon/lock.rs:13-35) — uninstall enumeration needs no new registry (C17).

## Design

**D1 — Client, not runtime.** The shell is a pure daemon client. Presentation state only: no provider calls, no SQLite, no run/policy/approval semantics. Anything semantic the shell needs becomes a daemon issue first, never a shell workaround.

**D2 — Protocol: v1 envelope and shared methods; no shell-private extensions.** Result shapes are not frozen: the extension path is daemon-owned, additive, backward-compatible fields/capabilities, each landed by its own daemon issue. Three are already required and gate the phases that consume them (R1, R2, C13):
- a typed transcript payload (structured user/assistant/tool history; clients never parse the preformatted `transcript` string);
- a pending-approval snapshot readable after lag/reconnect, so a paused run always re-renders its approval modal;
- an atomic `shutdown_if_idle` daemon control operation (C17): the installer-side control path enumerates current-user workspace locks for the exact installed sidecar, validates each matching daemon, and requests shutdown; an active or approval-paused daemon refuses, which aborts uninstall/upgrade without killing a process or deleting files; idle daemons wake, exit, and remove their locks.

**D3 — Stack and privileged boundary.** Tauri 2 shell (Rust) + Svelte 5 + SvelteKit static adapter (SPA) + Tailwind 4 + shadcn-svelte; one shared UI — Fluent-flavored on Windows, platform-default window chrome on macOS/Linux; local assets only, strict CSP. The security boundary is the Tauri capability model, not CSP (R3): the Rust shell exclusively owns `DaemonClient`, workspace validation, and the spawned child handle; the webview receives typed commands/events only and gets no generic shell, filesystem, raw-socket, or arbitrary-path capability. Phase-1 bridge tests prove this boundary.

**D4 — Daemon lifecycle: attach-or-spawn with an explicit race and lifetime contract (R4).** This extends the existing `plato --tui` embedded precedent, not a new divergence. Contract:
- Attach first: validate via `hello` (workspace, protocol version, required capabilities). Never pre-check the lock.
- On connect failure: spawn `plato-agentd`; the daemon's atomic bind/lock arbitrates concurrent starters. On spawn conflict, bounded-retry `hello`; if it never validates, fail closed with the socket/lock paths in the error.
- Shell exit always detaches and never stops the daemon, spawned or adopted (C10: other-client attachment is not observable in the protocol, and runs are daemon-owned — closing a UI must not kill work). Interactive shutdown stays manual; the atomic `shutdown_if_idle` control operation is a named daemon prerequisite (D2, C13/C17), not a shell behavior.
- Packaged macOS/Linux launches: the Rust shell establishes the user command PATH before spawning the sidecar — GUI launches do not inherit the shell-dotfile PATH, and `shell.exec` only restores what it inherited (C15).
- Spawned-child crash: show disconnected state and re-enter the attach-or-spawn path on user action; no automatic restart loop. The shell never deletes locks; if a stranded stale lock blocks respawn, fail closed and surface the lock path (reclamation is a non-goal, below).
- One-shot SQLite CLI paths remain fail-closed while a daemon holds the workspace (unchanged).

**D5 — Transport matrix (R5).** Unix (Linux and macOS) keeps std UDS untouched — no dependency change on Unix; macOS uses the same UDS and XDG-fallback paths as Linux. Windows-only: named pipes via the `interprocess` crate as the spiked default, behind a hard proof gate: an explicit current-user DACL is required (the Windows default DACL grants Everyone/anonymous read), with other-user rejection and remote-access rejection tests. Ownership model decided now, names refined in-phase: pipe name derived from workspace-id; ledger and lock under per-user `LocalAppData`; user config under `RoamingAppData`.

**D6 — Repo home (R6).** In `plato-agent`: one Tauri shell crate plus a `desktop/` web UI. Desktop checks run as separate CI jobs so the existing root Cargo job stays intact; native Windows CI lands with phase 3. CI isolation is a phase-1 acceptance item. No future-repo clause.

**D7 — Distribution: one coherent route per platform (R7).** Windows first: a single directly distributed **signed NSIS installer** containing the pinned `plato-agentd.exe` sidecar; signatures verified on the installer, app executable, uninstaller, and sidecar. Then, phase 6: macOS as a **signed and notarized DMG** (Developer ID; Gatekeeper blocks anything less — the hard gate for shipping mac), and Linux as an **AppImage** — no platform code-signing gate exists there; it ships with the release integrity evidence chosen in phase 6, at minimum a published checksum (C14). Windows/macOS unsigned artifacts are dev-only and never distributed. MSI, Microsoft Store, and auto-updater are out of v1 (C12).

**D8 — First-launch workspace selection (C11).** `hello` needs a canonical workspace root, and a Start-menu/double-click launch has no meaningful cwd. The Rust shell owns a native folder picker on first launch, persists the last canonical workspace root under per-user app state, and reopens it on later launches; an invalid or missing root returns to the picker. The webview requests selection and never receives generic filesystem capability.

## Sequencing
Each phase is its own issue and PR(s); implementation starts only on a `Ready for dev` phase issue. The three D2 protocol additions are their own daemon issues: phase 1 consumes the typed transcript, phase 2 the pending-approval snapshot, phase 5 the `shutdown_if_idle` control operation.

1. **Shell bootstrap + protocol adequacy gate (Linux).** Tauri window renders `sessions.list` and typed history for a selected run **without parsing the `transcript` string** (consumes the typed-transcript daemon issue). Capability-boundary bridge tests (D3). Workspace selection proofs: first launch (picker), relaunch (persisted root), missing/invalid root (back to picker) (D8). Live comparison via exact-run `transcript.read` against a running daemon; offline `plato replay` parity checked only after daemon shutdown (replay fails closed under the lock). TUI attached simultaneously. CI: separate desktop job; root Cargo job untouched.
2. **Chat parity + state isolation (R2, R8).** Composer (`run.start`/`message.append`, `wait: false`), keyed delta folding, approval modal (`approval.decide`), cancel, session picker. Rendering and actions bind to one selected session and exact run; switching sessions invalidates stale in-flight pages and never cancels or splices another client's run; terminal recovery uses exact-run `transcript.read`, never latest-session readback. Acceptance proofs: full-page catch-up; folding without duplication; lag/resync; reconnect while approval-paused recovering the modal from the pending-approval snapshot; approval resolved by another client; all six `RunStateName` values; `CancelRequested` before terminal; same-session overload; two concurrently active sessions with no cross-session content or decisions.
3. **Windows daemon parity (R9).** More than transport: native Windows build and tests; `LocalAppData`/`RoamingAppData` path model (D5); lock and pipe ACLs with the D5 proof gate; shutdown wake without Unix signals; `shell.exec` cancel/timeout on Windows. Native Windows CI starts here.
4. **Windows shell + sidecar lifecycle on a provisioned dev-VM (R4, R9, C10).** Attach/spawn race proofs (concurrent starters), shell exit detaches with daemon and runs surviving, child crash policy, second-client lifetime, CLI fail-closed preserved. Not a clean-VM proof — packaging does not exist yet.
5. **Windows packaging + distribution (R7, C13).** Signed NSIS installer with pinned sidecar; clean-VM install/uninstall and cold launch with an explicitly pre-provisioned workspace/config and user-scoped provider credential; SmartScreen behavior recorded; signature checks on all four artifacts. Uninstall/upgrade consumes the `shutdown_if_idle` daemon issue (C17): enumerate current-user locks for the installed sidecar, shut idle daemons down (wake, exit, lock removed), and abort with files untouched on any active or approval-paused refusal. Proof: two workspace daemons; one active/paused refusal aborting with files untouched; successful retry after the run reaches a terminal state. No reboot, no forced kill.
6. **macOS + Linux distribution (C14, C15).** macOS: verify the daemon builds and passes tests natively (expected near-parity via the Unix path), WKWebView UI acceptance, signed + notarized DMG with sidecar, cold-launch proof on a clean machine/VM. Linux: AppImage with sidecar plus the chosen integrity evidence (minimum: published checksum), webkit2gtk already proven by phases 1–2, cold-launch proof on a distro without the dev toolchain. Packaged-launch proof on both: a GUI-launched app completes a `shell.exec` run using a user-PATH-only command, under phase 5's pre-provisioned config/credential condition. The phase issue names macOS architectures/minimum version and Linux architecture/oldest WebKitGTK baseline — sidecars are target-triple-specific. macOS CI runner joins here.

Phases 1–2 deliver a usable Linux-dev desktop client early; 3–5 make it a Windows product; 6 makes it cross-platform.

## Non-Goals
- No `platonic-core` changes; no gateway or TUI behavior changes.
- One workspace per window; no multi-workspace switcher in v1.
- No MSI, no Store, no auto-updater in v1 (deferred per D7). macOS/Linux distribution is phase 6, after the Windows v1 — not dropped, sequenced.
- AppImage only on Linux (no Flatpak/Snap/deb/rpm in v1); no macOS-idiomatic path migration (the XDG-fallback paths stay).
- No automatic stale-lock reclamation (fail closed and surface the path; revisit as a daemon issue if phase 4 proofs make it bite).
- No model management, board, or GitHub integration surfaces.

## Open Questions (bounded)
- Exact pipe/path names and the DACL SDDL string: fixed inside phase 3 under the D5 ownership model and proof gate.
- Typed-transcript and pending-approval-snapshot wire shapes: fixed in their daemon issues under D2's additive rule.
- macOS-idiomatic paths (`~/Library/...`): deliberately not adopted (XDG fallbacks work); revisit only if real macOS friction appears.

## Acceptance (for this design going Active)
- Lead review accepts the folded revisions with no open blockers.
- Explicit human adoption: on the merge/adopt instruction, the final pre-merge revision flips this header to Active.
- Issue #139 owns creating and linking the phase and prerequisite issues after adoption; GitHub remains the sole board/WIP authority (C16).

## Proof
Design PR review of this document. No code ships with this PR.
