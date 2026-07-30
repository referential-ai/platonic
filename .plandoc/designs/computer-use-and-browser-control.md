---
title: Computer Use and Browser Control
issue: https://github.com/referential-ai/plato-agent/issues/159
status: active
---

# Plato Computer Use and Browser Control

Revision 1 - **Active**. AJ accepted this revision on 2026-07-30 in the
[canonical #159 decision relay](https://github.com/referential-ai/plato-agent/issues/159#issuecomment-5137657468).
Research and disposable proof are complete from
`develop@7cb8de74caf2465f32b1ad9207907cf97bbda618`. This document does not
itself authorize implementation; follow-up authority remains bounded below.

## Canonical Authority

- Issue #159 plus the explicit human task are the canonical source of truth and
  decision authority for this document; repo/workspace `AGENTS.md` files remain
  the owning engineering constraints.
- Issue [#159](https://github.com/referential-ai/plato-agent/issues/159) is the
  scope contract. Its 2026-07-14 comment authorizes research/design and
  disposable proof only. Its 2026-07-30 refinement marks that work Ready for
  dev while expressly excluding production Rust, a browser extension, a generic
  plugin/MCP framework, a new crate/repo, and a `platonic-core` change.
- AJ's 2026-07-30 acceptance adopts Revision 1 and authorizes merge/close of
  #159. It clears the design dependency for F1/F2/F3 issue refinement, but each
  still needs its own GitHub contract marked `Ready for dev`; F4 retains its
  separate post-F3 human gate.
- `plato-agent` owns provider-visible tools, host I/O, policy overrides,
  approvals, process supervision, artifacts, and durable ledger evidence.
  `platonic-core` remains the unchanged typed harness boundary.
- One-shot `plato` and `plato-agentd` must use the same adapter implementations.
  The daemon owns persistent workspace runtime placement; clients and a future
  browser relay never own run, policy, approval, or ledger semantics.
- This design deliberately stops at a small tool surface. It does not expose raw
  driver commands, raw CDP, JavaScript evaluation, arbitrary selectors, MCP
  passthrough, plugins, profiles, cookies, storage, credentials, downloads,
  uploads, clipboard access, password entry, CAPTCHA/2FA handling, browser
  permission dialogs, or an action named purchase/post/send.

## Desired Outcome

Revision 1 defines four bounded follow-up issues: F1/F2/F3 have their design
dependency cleared, subject to their own `Ready for dev` contracts, while F4
remains behind its separate post-F3 human gate. Together they cover an optional
ref-only desktop adapter, an isolated-browser adapter, a conformance-gated
selected-tab signed-in relay, and the relay's production slice. Every provider
operation has an exact schema, effect, policy decision, approval/lease rule,
stale-state behavior, ledger result, output bound, process owner, cleanup
condition, package pin, and rollback. A worker can implement one follow-up
without choosing an architecture or widening the surface.

This design itself is done because its single file contains those decisions and
the current primary-source/proof record, all disposable state is absent,
validation passed, and AJ's acceptance is recorded in the canonical issue
rather than chat history.

## Source Grounding

Grounding is against the exact base above:

- `src/tool_catalog.rs:17-52` is a static app-owned catalog. Effects are declared
  by the host, provider schemas reject additional properties, and unknown tools
  fail closed as `ExternalSideEffect` (`src/tool_catalog.rs:95-99`).
- `src/tools.rs:286-299` is the concrete app-owned dispatch boundary.
  `ToolExecutionContext` already carries workspace and cancellation state
  (`src/tools.rs:255-269`). Current subprocess handling has bounded output,
  timeout, cancellation, Unix process groups, and Windows kill-on-close jobs.
- The run driver records `ToolCallProposed`, `PolicyEvaluated`, approval,
  `ToolStarted`, and `ToolFinished`/`ToolFailed` in order
  (`src/app.rs:616-744`, `src/app.rs:1006-1065`). Provider-visible results are
  wrapped as untrusted and hard-capped at 65,536 bytes
  (`src/app.rs:194-196`, `src/app.rs:826-850`).
- `--yolo` can auto-grant only `WorkspaceWrite`; it cannot auto-grant Network,
  ExternalSideEffect, or SecretAccess (`src/app.rs:151-169`).
- `platonic-core` 0.2.0 already has exactly the needed effect vocabulary:
  `ReadOnly`, `WorkspaceWrite`, `Network`, `ExternalSideEffect`, and
  `SecretAccess`. Its `ToolResult` already carries structured data and
  `ArtifactId`s. External effects and secret access deny by default, so the app
  must add narrow named `RequireApproval` rules rather than changing core.
- The permanent topology in `docs/ARCHITECTURE.md:7-22` keeps one-shot operation
  daemon-independent, shares run choreography, gives `plato-agentd` persistent
  runtime ownership, and forbids connectors from owning semantics.
- There is no current browser dependency, artifact store, or computer-use
  process in `Cargo.toml` or `src/`. That absence is preserved by this design
  PR.

## Runtime Grounding

- `cua-driver` 0.14.1 was installed under a private temp HOME, run behind a
  generated deny-by-default policy and private socket, and proved for health,
  permissions, app/window discovery, screenshot-free state, session identity,
  one harmless AX click, stale element state, dead target state, and cleanup on
  Linux XWayland/AT-SPI.
- `agent-browser` 0.33.1 was downloaded as its pinned native Linux x64 asset and
  proved with a private HOME/TMPDIR/namespace, explicit Chromium, exact loopback
  allowlist, tabs, refs, harmless click, screenshot, two-session isolation,
  stale-ref rejection, timeout, output truncation, domain refusal, close, and
  cleanup.
- The proof found three design-changing contradictions: Cua accepted a stale
  element token, Cua synthesized state for a dead PID/window, and agent-browser
  ignored `--max-output` for JSON. The adapter guards below are based on those
  observations rather than optimistic upstream claims.
- The Chrome/CDP/extension comparison uses only the primary sources linked
  below. No signed extension or personal-profile action was run in this issue.

## Scope / Anchor Boundary

- The scope boundary is anchored to the one design path named by #159 and only
  the three integration surfaces and disposable proofs named below.
- Design and proof cover exactly desktop window discovery/semantic observation/
  one ref click, isolated HTTP(S) browsing/tabs/semantic observation/one ref
  click/viewport screenshot, and the selected-tab signed-in approach.
- All semantic and I/O ownership stays in `plato-agent`; `platonic-core` stays
  unchanged. Existing run driving, approval events, and ledger events remain
  the anchors.
- Version and runtime claims are pinned to Cua 0.14.1, agent-browser 0.33.1,
  and the 2026-07-30 proof environment. Other platforms or versions are not
  implied.

## Constraints

- F1/F2/F3 work does not start until that follow-up has its own GitHub issue
  contract marked `Ready for dev`. F4 additionally requires its separate
  post-F3 human acceptance.
- Provider inputs are semantic, bounded, and host-validated; raw upstream
  identities, paths, commands, selectors, coordinates, CDP methods, and
  credentials never cross the boundary.
- Every mutation is `ExternalSideEffect`, requires a fresh local approval, and
  is ineligible for yolo. Protected desktop/personal-browser reads use
  `SecretAccess` with exact leases.
- One-shot operation cannot require a permanent daemon. Every run reaches a
  terminal success only after owned live processes/sessions are gone.
- Only exact proved versions are accepted; no runtime downloader, updater,
  mutable tag, or latest range is allowed.

## Non-Goals

- Forbidden operations must not be inferred from the adopted upstream tools;
  anything not explicitly listed in D6 is out of scope.
- No production Rust, browser extension, core change, generic framework,
  crate/repo, or implementation is part of #159.
- No raw desktop coordinates/full-desktop scope, typing/hotkeys, app launch or
  kill, password/OTP entry, CAPTCHA/2FA or permission-dialog interaction,
  purchase/post/send, file transfer, download/upload, clipboard, cookie/storage
  access, arbitrary JavaScript/CDP, profile copying/reuse, or hidden personal
  browser control is admitted.
- No claim is made for unproved operating systems, browser engines, drivers, or
  versions. Cross-platform asset hashes alone are not support evidence.

## Ownership

- The shared `plato-agent` run runtime owns tool registration, policy,
  approvals, driver launch, host identities, artifacts, results, cleanup, and
  ledger truth.
- `plato-agentd` places persistent workspace-owned runtime; one-shot `plato`
  owns its ephemeral adapter processes. The same concrete adapter modules serve
  both.
- Cua and agent-browser own only their private mechanical child runtime.
  A future Chrome extension/native process owns transport and selected-tab CDP
  mechanics only. None owns sessions, policy, approvals, or run semantics.
- The user owns OS/browser permission grants, extension installation and
  action-click consent, and every mutation approval.

## Decisions

| Surface | Decision | Exact boundary |
| --- | --- | --- |
| Desktop | **Adopt conditionally**: external `cua-driver` 0.14.1 | Use only an app-owned, generated allowlist over a run-private `serve` + `call` socket. Do not adopt its MCP, SDK, browser tools, global daemon, updater, or raw tokens. Read-only can proceed after implementation proof; mutation is hard-gated on Plato stale-target and postcondition checks because 0.14.1 did not fail closed in the proof. |
| Isolated browser | **Adopt with a hard availability gate**: native `agent-browser` 0.33.1 | Invoke a hardcoded JSON CLI subset with host-minted namespace/session/tab/ref identities. No npm wrapper at runtime, MCP, plugins, chat, eval, CDP connect, profiles, restore/state, credentials, or raw args. Plato applies its own JSON/output, URL, policy, artifact, and cleanup limits. `browser.open` is not advertised/enabled until F1 proves destination-IP enforcement for initial navigation, redirects, and subresources. |
| Raw CDP / Chrome DevTools MCP | **Reject for Plato runtime** | Do not open a debugging port/pipe and do not embed or proxy `chrome-devtools-mcp`. These routes are broader than a selected tab, expose profile data, and create a generic tool server boundary. |
| Signed-in Chrome | **Build only after a second human gate**: a first-party signed MV3 `chrome.debugger` relay | A Chrome Web Store signed extension, exact extension ID, exact selected tab, one-time native-messaging rendezvous, and a small CDP method allowlist. No unpacked production extension and no hidden/unattended attach. A disposable conformance issue must prove this before a production issue can become Ready for dev. |
| Multimodal model input | **Not a prerequisite** | Accessibility text and ref-bound action are sufficient for the first slices. Only isolated-browser viewport screenshots are durable user/ledger artifacts. Desktop and signed-in screenshots are not in the initial surfaces. Coordinate/vision actions stay unavailable until a later app-owned provider-media design; no core change is needed. |

### D1 - Desktop process and package ownership

The first desktop slice is Linux X11/XWayland only because that is the only
platform proven here. `cua-driver` is an optional, separately installed
executable pinned to exactly 0.14.1. Plato never downloads or updates it during
a run. Resolution is from a trusted absolute config path and then `PATH`; the
model cannot supply a binary, socket, policy path, or argument.

On first use in a run, the shared app adapter starts:

```text
cua-driver serve
  --socket <0700 run-runtime-dir>/cua.sock
  --pid-file <0700 run-runtime-dir>/cua.pid
  --permission-mode standard
```

It also sets an app-generated `CUA_DRIVER_POLICY_FILE` that allows only
`health_report`, `check_permissions`, `list_apps`, `list_windows`,
`get_window_state`, `start_session`, `get_session_state`, `click`, and
`end_session`. The file is mode 0600; the Unix socket must report mode 0600.
The adapter calls only fixed `cua-driver call <known-tool> --socket <owned
socket>` argv vectors and parses JSON. There is no shell, raw command field,
MCP client, generic driver registry, global endpoint, autostart, telemetry, or
update check.

The process is keyed to one run so driver element caches and session identity
cannot cross runs. `plato` owns it directly; `plato-agentd` owns it in the
workspace runtime on behalf of the run. A run terminal state is not recorded
until its session is ended and the process is reaped. macOS fails with a typed
unsupported diagnostic in the first slice: Cua documents that a raw daemon has
no stable TCC identity and is unsupported. A future macOS issue must choose and
prove the signed desktop app's embedded-host identity. Windows likewise waits
for native proof rather than inheriting the Linux result.

Do not bundle Cua in the first slice. Separate installation preserves the
upstream OS-permission identity and avoids silently shipping an unproved
platform binary. A future packaged desktop may bundle it only after that
platform proves checksum, signing/TCC identity, permissions, and cleanup.

### D2 - Desktop stale-state guard

Cua's raw `pid`, `window_id`, snapshot id, element index, and element token
never cross the provider boundary. Plato returns:

- `window_ref`: run-scoped opaque mapping to PID, process start time,
  executable identity, native window id, title, and bounds;
- `observation_id`: monotonically newer generation for one exact
  `window_ref`; and
- `element_ref`: opaque mapping to role, accessible name, states, bounds, and a
  semantic path/fingerprint within that generation.

Every new observation invalidates prior observations for that window. Before a
mutation, under a per-target lock, Plato requires the latest observation,
requires it to be no older than 10 seconds, re-lists the process/window, checks
PID start time and window identity, obtains a fresh screenshot-free state, and
resolves exactly one equivalent element by semantic fingerprint. Missing,
changed, or ambiguous identity returns `stale_observation` without dispatch.
Only the fresh Cua token/index is sent to the driver.

After dispatch, Plato observes again. A Cua response such as
`effect:"unverifiable"` is never reported as success. If a deterministic
postcondition cannot be shown, return `action_unverified` with
`side_effect_possible:true`; the system instruction says not to retry an
uncertain mutation without a new local approval. This is required because the
0.14.1 Linux proof both accepted a stale element token and returned a synthetic
state for a dead PID/window.

Only accessibility element click is admitted initially. Coordinate clicks,
desktop capture/scope, typing, hotkeys, scroll, launch/kill, bring-to-front,
permission-dialog handling, and Cua's browser routes remain disabled.
Every `get_window_state` call injects `include_screenshot:false`; the adapter
rejects an unexpected image block and has no desktop artifact path.

### D3 - Isolated-browser process and package ownership

Plato adopts the single native release asset for the current target, not the npm
wrapper. The npm 0.33.1 package is 40,308,667 compressed bytes and 90,888,487
unpacked bytes because it contains seven platform binaries and requires Node
24+. The proved Linux x64 release binary is 13,852,232 bytes and requires no
Node runtime.

For crates.io/dev CLI use, `agent-browser` is an optional separately installed
executable pinned to exactly 0.33.1. For a signed desktop package, include only
the target asset, its Apache-2.0 notice, and pinned SHA-256 after that target's
acceptance proof. Resolution order is a trusted absolute config path, a
packaged sibling asset, then `PATH`. Packaged assets must match the recorded
digest; external binaries must report the exact version. No run-time download,
`npx`, update, or mutable version range is allowed.

The app mints an unguessable namespace and session name for each run. Every
fixed argv call injects:

```text
--namespace <host value>
--session <host value>
--executable-path <trusted host value>
--content-boundaries
--allowed-domains <host-configured exact domains>
--max-output 32768
--idle-timeout 60s
```

No provider input can select namespace, session, executable, profile, state,
restore, headers, raw Chrome arguments, output path, or action policy. Plato
allows only the native JSON equivalents of `open`, `tab list`, `tab new`,
`snapshot`, `click @ref`, `screenshot <staging-path>`, `back`, `session info`,
and `close`. `back`, session inspection, and close are host mechanics, not
provider tools. `tab new` is used only by `browser.open`.

The host captures the daemon PID from `session info`. On cancel, timeout,
provider failure, or run terminal state it closes the exact session, verifies
that the namespace has no sessions, waits for the recorded PID, kills/reaps only
that process tree if needed, and removes the unique namespace after liveness is
false. One-shot use therefore needs no permanent daemon; daemon-hosted use
persists only for the run.

### D4 - URL and SSRF policy

`browser.open` accepts only an absolute `http` or `https` URL of at most 2,048
bytes. Plato uses the existing `url` crate to reject userinfo, noncanonical
hosts, invalid ports, IP obfuscation, and every other scheme. The host lowercases
and IDNA-normalizes the hostname, resolves it before approval/navigation, and
classifies every answer.

Default-denied destinations are loopback, unspecified, private, link-local,
carrier-grade NAT, multicast, documentation/reserved ranges, IPv4-mapped forms,
and cloud metadata names/addresses. An exact local origin can be enabled only
by trusted config, is shown with all resolved addresses in each Network
approval, and never follows from model input alone. Wildcard domains are not
accepted in Plato's first slice. Each required site/CDN host must be listed
exactly.

The normalized exact-domain set is fixed when the isolated session launches and
is passed to `agent-browser --allowed-domains`. Upstream 0.33.1 intercepts
navigation and subresources and also blocks WebSocket/EventSource/beacon,
workers that cannot receive its guard, and WebRTC while this mode is active.
Plato rechecks top-level navigation and redirects. Hostname filtering does not
itself prove destination-IP containment.

Therefore `browser.open` remains unavailable until F1 proves that denied
destination addresses cannot be reached through the initial navigation, any
redirect, or any subresource, including DNS rebinding after approval. Before
that proof, the tool is not advertised or enabled; attempted configuration
fails with `ssrf_enforcement_unavailable`. Network approval, an opt-in flag, or
a warning cannot waive this gate. If the proved upstream path cannot enforce
it, F1 must either adopt a concrete enforcing transport under its own reviewed
scope or leave `browser.open` unavailable. This design does not select or design
a proxy.

### D5 - Signed-in-browser approach

The signed-in surface is a different trust class and does not reuse
`agent-browser` profile/restore/auto-connect features.

Raw remote debugging is rejected. Chrome 136+ ignores
`--remote-debugging-port` and `--remote-debugging-pipe` for the default data
directory because the route has been abused to extract cookies, and Chrome
recommends a separate data directory/Chrome for Testing for automation. Chrome
DevTools auto-connect is also rejected for Plato: Chrome 144+ asks the user to
allow it, but the official documentation states that the agent then inherits
all open tabs and profile session/local storage, cookies, extensions, and live
state. `chrome-devtools-mcp` adds a broad generic MCP/Node surface, usage
statistics by default, and access to inspect or modify browser/DevTools data.

The only selected approach is a first-party Manifest V3 extension with:

- permissions limited to `debugger`, `nativeMessaging`, and `activeTab`;
- no host permissions, content scripts, `externally_connectable`, remotely
  hosted code, tabs/history/cookies/storage/downloads/clipboard permissions;
- an action-button user gesture that offers only the active tab and only when
  its URL parses to a canonical, non-opaque `http` or `https` origin;
- `chrome.debugger.attach({tabId}, "0.1")` only after local approval;
- root tab and same-process frames only in the first version; out-of-process
  child frames return `unsupported_frame` instead of auto-attaching;
- an internal allowlist limited to Accessibility/DOM/Page observation and
  Input dispatch needed by the typed tools; no arbitrary CDP method and no
  Runtime evaluation, Network, Fetch, Storage, WebAuthn, Target enumeration,
  tracing, or debugger/profiler methods; and
- no screenshot in the first signed-in slice, keeping native-host messages
  well below Chrome's 1 MB host-to-extension limit.

Chrome warns that `debugger` can access the page debugger backend and read/change
all website data. That grant is browser-wide in potential even though Plato
attaches one tab. Production distribution therefore requires an unlisted or
private Chrome Web Store item so Chrome signs it and supplies a stable extension
ID. An unpacked extension is development proof only. If Web Store signing and a
stable ID are unavailable, this feature does not ship.

Before an offer, the extension rejects `chrome:`, `chrome-extension:`,
`devtools:`, `file:`, `data:`, `about:`, and every other non-HTTP(S),
opaque/internal URL with `unsupported_tab_scheme`. Immediately before
`chrome.debugger.attach`, it reads the same `tabId` again and requires the exact
previous canonical URL/origin and an `http`/`https` scheme. A close, replacement,
navigation, opaque origin, or scheme change during consent fails without
attachment. This rule is not waivable by approval or config.

The native-host manifest has one exact `allowed_origins` entry for that ID and
an absolute path to the packaged `plato-agentd`. Chrome launches the same binary
in a narrowly detected native-host mode; it is a relay process, not a second
runtime. A waiting `plato` run or `plato-agentd` run creates a mode-0600,
single-use rendezvous containing its private endpoint, run id, expiry, and a
256-bit random ticket. The native host requires the exact Chrome caller-origin
argument, atomically consumes the ticket, exchanges fresh nonces with the run
owner, and accepts a protocol-versioned offer only for that run. Zero, multiple,
expired, replayed, or mismatched offers fail closed. The local same-user account
is the OS trust boundary; web pages and other extensions receive no path to the
host.

Consent order is fixed:

1. The model proposes `personal_browser.attach`; Plato opens a 60-second pairing
   request but does not attach.
2. The human clicks the extension action on the chosen tab. `activeTab` lets the
   extension offer its current URL/title without debugger attachment.
3. Plato presents a local SecretAccess approval containing the signed extension
   ID, exact origin/title, run, expiry, and capability list.
4. Only a grant authorizes the extension to attach that exact `tabId`; denial,
   timeout, navigation before attach, or disconnect leaves it unattached.

The resulting lease is run-, extension-, tab-, origin-, and native-port-bound,
expires after 10 minutes or 60 seconds idle, and never survives a process or
extension-service-worker generation. Cross-origin navigation suspends it and
requires the full gesture/approval sequence again. Same-origin navigation
invalidates all observations. Revocation occurs on extension action toggle,
Chrome `onDetach` (`target_closed` or `canceled_by_user`), DevTools opening,
native-port loss, run cancel/terminal state, expiry, extension disable/uninstall,
or an explicit local stop. Detach is idempotent.

There is no unattended signed-in attach, persistent approval, profile scan,
startup reconnect, or yolo mode. Every run requires the visible gesture and
SecretAccess approval; every mutation separately requires local approval.

### D6 - Provider-visible typed surface

All schemas use `additionalProperties:false`; strings have explicit bounds.
Names below are internal/provider names.

Shared schema bounds are normative:

- every host-minted `window_ref`, `tab_ref`, `observation_id`, `element_ref`, or
  lease ref is an ASCII string with `minLength:1`, `maxLength:96`, and pattern
  `^[A-Za-z0-9_-]+$` (unpadded base64url alphabet only);
- every `max_elements` has default 100, minimum 1, and maximum 200;
- every returned windows, tabs, or elements array has `maxItems:200` and
  returns `total_count`, `returned_count`, and `truncated`; excess entries are
  omitted deterministically;
- every output app label, title, role, accessible name/value, and bounded error
  message is at most 512 UTF-8 bytes after valid-boundary truncation; and
- every input/output URL string is at most 2,048 UTF-8 bytes. `purpose` retains
  its smaller JSON Schema `maxLength:160`.

| Internal / provider name | Input | Structured result |
| --- | --- | --- |
| `computer.windows` / `computer_windows` | `{}` | At most 200 `windows[]` under the shared count/string/ref rule, with `window_ref`, app label, title, bounds, capability flags, and truncation counts. No raw driver identity. |
| `computer.observe` / `computer_observe` | `{window_ref, max_elements?:integer=100}` under the shared ref/integer rule | Exact target, `observation_id`, semantic elements under the shared ref/string/collection rule, and truncation counts. It never requests or returns an image/artifact. |
| `computer.click` / `computer_click` | `{window_ref, observation_id, element_ref}` under the shared ref rule | Attempted route, pre/post observation hashes, `verified`, and either success or typed uncertainty/error. |
| `browser.open` / `browser_open` | `{url}` under the shared URL rule | `tab_ref`, normalized final URL/origin, title, redirect count, browser/driver versions under the shared ref/string rules. |
| `browser.tabs` / `browser_tabs` | `{}` | At most 200 tabs under the shared count/string/ref rule, with stable `tab_ref`, URL/origin, title, active flag, and truncation counts. |
| `browser.observe` / `browser_observe` | `{tab_ref, max_elements?:integer=100}` under the shared ref/integer rule | `observation_id`, URL/origin/title/document generation, and semantic elements under the shared ref/string/collection rule. |
| `browser.screenshot` / `browser_screenshot` | `{tab_ref}` under the shared ref rule | Viewport artifact id, SHA-256, PNG media type, dimensions, bytes. No provider-controlled path. |
| `browser.click` / `browser_click` | `{tab_ref, observation_id, element_ref}` under the shared ref rule | Attempted route, pre/post document/observation hashes, final URL, `verified`, and typed error/uncertainty. |
| `personal_browser.attach` / `personal_browser_attach` | `{purpose}` under the shared 160-character purpose rule | After gesture/approval: tab/lease refs, origin/title, lease expiry, extension/protocol versions and capability list under the shared ref/string rules. |
| `personal_browser.observe` / `personal_browser_observe` | `{tab_ref, max_elements?:integer=100}` under the shared ref/integer rule | The shared bounded observation shape, explicitly `protected:true`; never cookies/storage/network bodies. |
| `personal_browser.click` / `personal_browser_click` | `{tab_ref, observation_id, element_ref}` under the shared ref rule | Same mutation evidence plus lease/origin generation; never a raw CDP result. |

`purpose` is untrusted explanatory text and never grants scope. Provider input
never contains a PID, native window id, Cua token/index, agent-browser `tN`/`@eN`,
Chrome tab id, CDP target/session id, filesystem path, executable, namespace,
session, lease secret, native-host ticket, selector, coordinate, or command.

No typing tool is in this issue set. Password and one-time-code entry remain
forbidden. Adding typing, scroll, coordinate input, download/upload, or new CDP
domains requires a new issue with its own effect, preview, stale-state, and
postcondition contract.

### D7 - Effect, policy, and approval contract

| Operation | Declared effect | App policy | Approval/lease |
| --- | --- | --- | --- |
| `computer.windows` | `SecretAccess` | `RequireApproval` | Every call; window titles can be protected material. |
| `computer.observe` | `SecretAccess` | `RequireApproval`, then scoped Allow for repeated reads of the exact run/window lease | First exact target and again after target change, expiry, or revocation. |
| `computer.click` | `ExternalSideEffect` | `RequireApproval` override | Every mutation, even under a read lease. Never yolo. |
| `browser.open` | `Network` | `RequireApproval` | Every navigation/new tab. Preview includes normalized URL, exact allowlist, and resolved-address classes. |
| `browser.tabs`, `browser.observe`, `browser.screenshot` | `ReadOnly` | Allow only inside the run-owned isolated session | No separate approval; cannot introduce a new network origin. |
| `browser.click` | `ExternalSideEffect` | `RequireApproval` override | Every mutation. Never yolo. |
| `personal_browser.attach` | `SecretAccess` | `RequireApproval` override | Exact offer plus Chrome action gesture. Never yolo. |
| `personal_browser.observe` | `SecretAccess` | Allow only while the exact approved lease is live; otherwise Deny with `attach_required` | The attach grant is the bounded read capability. |
| `personal_browser.click` | `ExternalSideEffect` | `RequireApproval` override | Every mutation and exact live lease. Never yolo. |
| Owned start/inspect/close/reap/remove mechanics | Not provider tools | Host-only | May affect only resources whose host-minted identity and liveness match the run. |

These are narrow app overrides to core defaults, analogous to the current
`shell.exec` rule. Tests must prove `ApprovalMode::AutoApprove` never grants
Network, ExternalSideEffect, or SecretAccess, including when `--yolo` is set.
The approval preview for a mutation shows adapter/version, app or origin/title,
semantic role/name, observation age, action, possible external effect, and
whether deterministic verification is available. Page text is quoted as
untrusted and cannot alter the preview fields.

### D8 - Stale refs, prompt injection, and result caps

Agent-browser 0.33.1 correctly rejected a ref after navigation in the proof, but
Plato still owns the contract. An observation registry maps host refs to the
current agent-browser ref and a semantic fingerprint. Under a per-tab lock, a
mutation verifies tab/document/origin generation, takes a fresh snapshot, and
resolves exactly one equivalent element. Navigation, tab close, process
generation, newer observation, ambiguity, or age over 10 seconds fails before
dispatch.

All browser/desktop content is attacker-controlled data:

- decode only the expected JSON shape and reject unknown/invalid fields;
- never concatenate content into a command, selector, URL policy, path, or CDP
  method;
- preserve the existing `trust="untrusted"` provider wrapper and neutralize its
  closing token;
- retain upstream content-boundary nonces as a diagnostic only, never an
  authority boundary;
- cap raw stdout at 1 MiB and stderr at 16 KiB while reading; kill the command
  on overflow;
- cap structured provider data at 32 KiB, 200 elements, 512 bytes per
  accessible name/value, and report counts plus `truncated:true`;
- parse and cap JSON after decoding because 0.33.1 ignored `--max-output` for
  JSON in the proof; and
- keep the current final 65,536-byte provider wrapper as defense in depth.

No page can message the signed extension: there are no content scripts or
external connections. The service worker accepts only schema-checked native
messages carrying the live ticket/nonces, and it constructs CDP calls from a
closed enum. Chrome's own guidance says content scripts are less trustworthy
and privileged messages must be validated; this design removes that sender
class entirely.

### D9 - Isolated-browser screenshot artifacts and multimodal boundary

Only `browser.screenshot` enters this boundary. The isolated-browser adapter
writes its viewport screenshot to an app-selected staging directory under the
run's state directory. The directory is mode 0700. After agent-browser exits,
Plato rejects symlinks/non-regular files, validates PNG signature, maximum
4,096 x 4,096 dimensions and 8 MiB bytes, computes SHA-256, changes the file to
0600, and atomically renames it to a host-minted artifact id. The proved
agent-browser screenshot was mode 0644, so private directory traversal and the
post-write chmod are required rather than assumed. `computer.observe` and the
signed-in surface never create or return screenshot artifacts in these slices.

The isolated-browser `ToolResult.artifacts` carries the existing core
`ArtifactId`. `result.data` carries media type, dimensions, bytes, hash, and an
app-state-relative locator; the durable ledger never stores base64 image bytes
or an absolute temp path. Browser screenshot artifacts persist with ledger
evidence. Run cleanup removes staging files only.

Current model requests are text-only and no app artifact resolver feeds image
blocks to a provider. Therefore these isolated-browser screenshots support user
inspection/replay, not model vision. `computer.click` and both browser clicks
are ref-only. A later multimodal issue may add provider-specific image delivery
in `plato-agent`, but must not change `platonic-core` or silently enable
coordinate actions.

### D10 - Errors, cancel, timeout, cleanup, and ledger

Operational failures return a bounded structured result so the ledger and model
see the same code:

```json
{
  "ok": false,
  "error": {
    "code": "stale_observation",
    "message": "target changed; observe again",
    "retryable": true,
    "side_effect_possible": false
  }
}
```

Required codes are `missing_driver`, `version_mismatch`,
`checksum_mismatch`, `unsupported_platform`, `interactive_desktop_unavailable`,
`os_permission_denied`, `target_not_found`, `target_ambiguous`,
`stale_observation`, `attach_required`, `lease_expired`, `url_denied`,
`ssrf_denied`, `ssrf_enforcement_unavailable`, `unsupported_frame`,
`unsupported_tab_scheme`, `output_limit`, `artifact_invalid`, `timeout`,
`canceled`, `driver_crashed`, `driver_protocol`, `action_unverified`, and
`cleanup_failed`. Only
`action_unverified`/post-dispatch crash or timeout sets
`side_effect_possible:true`. Programmer/schema/unknown-tool failures may still
use `ToolFailed`.

Default per-command timeout is 5 seconds, open/navigation is 30 seconds, local
approval/pairing has its stated expiry, and cleanup gets 5 seconds before
owned-process termination. Cancellation is checked before approval, before
dispatch, while draining output, before postcondition observation, and during
cleanup. Process-tree cleanup uses the existing Unix process-group and Windows
job patterns. It never searches/kills by executable name.

Existing ledger events remain sufficient and core stays unchanged:

- proposal records provider input plus host-declared effect;
- policy records Allow/RequireApproval/Deny;
- approval records the local actor;
- start records the dispatch boundary;
- finish data records adapter and browser versions/digests, host session/target
  refs, normalized origin or app/window fingerprint, observation generations,
  action route, pre/post hashes, verification, timing, truncation, artifact ids,
  and whether a side effect may have occurred;
- failure records the stable code and bounded reason.

Run cleanup happens before `RunFinished`. Failure to detach/close/reap/remove
returns `cleanup_failed` and records `RunFailed`; a terminal success therefore
proves cleanup completed. A daemon may retain durable isolated-browser
screenshots/ledger only, never a live driver session. Provider fallback remains
unrelated and continues to be separately recorded.

Missing-driver diagnostics name the expected version, searched trusted
locations, detected version/path if any, platform support state, and the
upstream release/install documentation. They do not auto-install. OS permission
diagnostics report the missing Accessibility/UIA/AT-SPI/screen permission and
the appropriate upstream/system settings route, but Plato never clicks a
permission dialog.

## Source-backed comparison

### Cua

- The [0.14.1 CLI reference](https://cua.ai/docs/reference/cua-driver/cli-reference)
  documents the exact current version and `call`/`serve` roles.
- [Interface contracts](https://cua.ai/docs/reference/cua-driver/contracts)
  say the selected service owns the element cache/session state, window scope
  binds observation/action to PID and window id, and accessibility actions are
  distinct from coordinate routes.
- The [process model](https://cua.ai/docs/reference/cua-driver/process-model)
  distinguishes same-process, private-worker, and daemon shapes; documents
  mode-0600 Unix sockets/same-user pipes; and forbids raw production daemons
  without stable macOS TCC identity.
- [Platform support](https://cua.ai/docs/reference/cua-driver/platform-support)
  explicitly limits Linux Wayland raw input and says support is observed
  behavior, not a successful return alone.
- [Known limits](https://cua.ai/docs/reference/cua-driver/limits) say newer
  snapshots/navigation invalidate browser refs and describe Accessibility and
  Screen Recording boundaries. The 0.14.1 binary proof below shows Plato still
  needs its own native stale guard.
- [Permission policies](https://cua.ai/docs/reference/cua-driver/permission-policies)
  load at process startup, default to deny when configured, and enforce at the
  native registry boundary. Plato uses this only as defense in depth beneath
  its own policy/approval.
- Pin: [release](https://github.com/trycua/cua/releases/tag/cua-driver-rs-v0.14.1),
  [tag source](https://github.com/trycua/cua/tree/cua-driver-rs-v0.14.1),
  MIT license.

### Agent-browser

- The [v0.33.1 source README](https://github.com/vercel-labs/agent-browser/blob/v0.33.1/README.md)
  documents stable non-reused tab ids, isolated sessions, the native Rust
  daemon, default one-hour idle lifetime, no state persistence without restore,
  exact-domain/subresource/WebSocket/WebRTC containment, action policy, content
  boundaries, and output flags.
- The same surface also exposes profile reuse, restore/state, credentials,
  plugins, eval, raw CDP, downloads/uploads, cookies/storage, chat, and MCP.
  Their existence is why Plato adopts a hardcoded subset rather than the tool
  as a framework.
- Pin: [release](https://github.com/vercel-labs/agent-browser/releases/tag/v0.33.1),
  [tag source](https://github.com/vercel-labs/agent-browser/tree/v0.33.1),
  [npm registry metadata](https://registry.npmjs.org/agent-browser/0.33.1),
  Apache-2.0 license.

### Chrome, CDP, and extensions

- Chrome's [remote-debugging security change](https://developer.chrome.com/blog/remote-debugging-port)
  explains the Chrome 136 default-profile restriction and recommends isolated
  profiles/Chrome for Testing.
- Chrome's [agent auto-connect documentation](https://developer.chrome.com/docs/devtools/agents/use-cases/auto-connect)
  requires Chrome 144+, manual enablement and an Allow prompt, while explicitly
  granting the agent access to all profile/open-tab/session/local-storage/cookie
  data.
- The official [CDP specification](https://chromedevtools.github.io/devtools-protocol/)
  says tip-of-tree changes frequently with no backward-compatibility guarantee;
  stable 1.3 is only the smaller Chrome 64 subset. Plato therefore uses
  extension `attach(...,"0.1")` plus a tested method allowlist, never arbitrary
  protocol forwarding.
- The official [`chrome.debugger` reference](https://developer.chrome.com/docs/extensions/reference/api/debugger)
  documents tab-scoped attachment, restricted domains, protocol version 0.1,
  and `target_closed`/`canceled_by_user` detach reasons.
- Chrome's [permissions list](https://developer.chrome.com/docs/extensions/reference/permissions-list)
  gives the debugger and nativeMessaging warnings. The
  [activeTab contract](https://developer.chrome.com/docs/extensions/develop/concepts/activeTab)
  ties temporary current-tab access to a user gesture.
- [Native messaging](https://developer.chrome.com/docs/extensions/develop/concepts/native-messaging)
  requires exact non-wildcard `allowed_origins`, passes the caller extension
  origin, uses length-prefixed JSON, and caps host-to-Chrome messages at 1 MB
  and Chrome-to-host at 64 MiB.
- Chrome's [messaging security guidance](https://developer.chrome.com/docs/extensions/develop/concepts/messaging)
  requires validation of less-trusted senders. This design has no content-script
  sender.
- Chrome's [distribution rules](https://developer.chrome.com/docs/extensions/how-to/distribute)
  say only Chrome Web Store hosted/signed extensions can be directly installed
  by ordinary users; self-hosting is managed-environment only except Linux's
  manual packed path.

## Disposable proof transcript

All proof was isolated from repo/user tool state. Only the local HTML fixtures
and Cua allowlist temporarily lived under untracked `.tmp-proof-159/`; binaries,
homes, screenshots, sockets, and browser temp state lived under unique `/tmp`
or `/run/user/1000` roots. No production file was created.

### Host

```text
$ uname -a
Linux sauron 6.18.38-3-lts #1 SMP PREEMPT_DYNAMIC Mon, 13 Jul 2026 20:47:34 +0000 x86_64 GNU/Linux
$ printf 'arch=%s session=%s desktop=%s display=%s wayland=%s\n' ...
arch=x86_64 session=wayland desktop=Hyprland display=:0 wayland=wayland-1
$ node --version; npm --version; chromium --version; google-chrome-stable --version
v25.2.1
11.8.0
Chromium 148.0.7778.96 Arch Linux
Google Chrome 150.0.7871.124
```

### Cua 0.14.1

Version/release isolation and integrity:

```bash
CUA_ROOT=/tmp/plato-159-cua.INDe47
env \
  HOME="$CUA_ROOT/home" \
  CUA_DRIVER_RS_HOME="$CUA_ROOT/home/.cua-driver" \
  CUA_DRIVER_RS_INSTALL_DIR="$CUA_ROOT/bin" \
  CUA_DRIVER_RS_NO_MODIFY_PATH=1 \
  CUA_DRIVER_RS_KEEP_VERSIONS=1 \
  CUA_DRIVER_RS_VERSION=0.14.1 \
  bash -c "$(curl -fsSL https://cua.ai/driver/install.sh)"

"$CUA_ROOT/bin/cua-driver" telemetry disable
"$CUA_ROOT/bin/cua-driver" --version
sha256sum "$CUA_ROOT/home/.cua-driver/packages/releases/0.14.1-x86_64-unknown-linux-gnu/cua-driver"
gh api repos/trycua/cua/releases/tags/cua-driver-rs-v0.14.1 --jq ...
git ls-remote https://github.com/trycua/cua.git refs/tags/cua-driver-rs-v0.14.1
```

```text
cua-driver 0.14.1
installed ELF sha256:
b0648cc74033f1ccf198becc485a0f7d1e558243d96c7fd90565c691c4dff203
release asset:
cua-driver-rs-0.14.1-linux-x86_64-binary.tar.gz
sha256:8305f5006f9eca47461ac4d04cbd9adad41958c0c3409e5503429aa6c6a8a963
published: 2026-07-29T20:26:31Z
tag commit: 41ae29b44b49b68c6e01c934fffbbe74d22e26fb
```

The untracked YAML policy allowed only the nine tools named in D1. The daemon
used a unique socket/pid file under the temp root. `doctor --json`, `status`,
and calls showed:

```text
version=0.14.1; telemetry=disabled; policy=active
policy sha256=24e82...; overall health=ok
session=Wayland+XWayland; X11=true; AT-SPI reachable=true
screen capture=pass; native Wayland backend=disabled/skipped
check_permissions:
  atspi=true, wayland=true, wayland_enabled=false, x11=true, xsend_event=true
```

Harmless target/actions used `GDK_BACKEND=x11 zenity` dialogs:

```text
start_session("plato-159-cua-proof", capture_scope="window") -> active=true
list_apps -> target PID present among 1075 apps
list_windows -> Zenity PID=912227, XID=69206020, expected title/bounds
get_window_state(include_screenshot=false,max_elements=50,max_depth=8)
  -> snapshot=s00000001, 5 AT-SPI elements, image absent
  -> OK token=s00000001:3
second observation -> snapshot=s00000002, image absent
```

Fresh click proof on a second dialog:

```text
PID=943474; XID=69206020; snapshot=s00000005
click(fresh OK token) ->
  {"effect":"unverifiable","path":"ax","verified":false}
dialog process exit=0; subsequent list_windows match count=0
```

The required stale proof found two upstream fail-open contradictions:

```text
snapshot s00000003 -> retain OK token s00000003:3
snapshot s00000004 -> old token should now be stale
click(old token) ->
  {"effect":"unverifiable","path":"ax","verified":false}
actual result: dialog closed and process exited 0

after the process was gone:
get_window_state(old PID/window) ->
  success, snapshot=s00000006, synthetic one-element blank window
```

The binary's `describe get_window_state` schema accepted
`include_screenshot:false`, and both calls omitted the image, despite the
current contracts page's broader statement that the tool always returns a
screenshot. The observed binary contract controls the pin; the documentation
disagreement is another reason for adapter conformance tests.

`end_session` reported inactive. The daemon was stopped; its PID, socket, and
pid file were absent before the final cleanup below.

### Agent-browser 0.33.1

Current version, package shape, release integrity, and native binary:

```bash
npm view agent-browser version
npm view agent-browser@0.33.1 version engines dist repository --json
npm pack agent-browser@0.33.1 --dry-run --json | jq ...
gh api repos/vercel-labs/agent-browser/releases/tags/v0.33.1 --jq ...
git ls-remote https://github.com/vercel-labs/agent-browser.git refs/tags/v0.33.1
curl -fsSL -o /tmp/plato-159-agent-browser.RDgkU0/agent-browser \
  https://github.com/vercel-labs/agent-browser/releases/download/v0.33.1/agent-browser-linux-x64
sha256sum /tmp/plato-159-agent-browser.RDgkU0/agent-browser
/tmp/plato-159-agent-browser.RDgkU0/agent-browser --version
```

```text
npm current=0.33.1; engines node>=24, pnpm>=11
npm package=40,308,667 bytes; unpacked=90,888,487 bytes
npm package contains darwin arm64/x64, linux arm64/x64/musl, win32 x64 binaries
release published=2026-07-28T06:17:07Z; tag commit=6dcea79b4b567a5671f1e1164807204f69542a5c
release immutable=false
linux-x64 sha256=6e04d06605c4ca62da36e3263086e0f7ceae808b55508de2c3958d4b7fe430aa
agent-browser 0.33.1
```

The release asset digest pins for packaging are:

```text
darwin-arm64  33ce6a3f94322ad8ea4ac28db923737c040db88af8bb199f57778995d451f2c7
darwin-x64    e1196791c202e11875dbcd97de744b088f5a94f3706b3f3b4fa9056a5a2d562b
linux-arm64   281cce8e3e9eb11fd823b13c085996d7361c35923ad454ce5cb06a5515630e9b
linux-musl-a  b4d73875d0842ddfff7f3bfb15173d81705d96b38b81681b9f9331d0df2d402c
linux-musl-x  bc36927d84f4dddab1a13819775c32008f8e4d40e979197206f9402c41fabbfa
linux-x64     6e04d06605c4ca62da36e3263086e0f7ceae808b55508de2c3958d4b7fe430aa
win32-x64     d5520659190d8112833c36ebffa766453a431e6721cce1b03361058080fb38be
```

Only Linux x64 was executed here; the other hashes are release metadata, not
platform proof.

The probe used a private HOME/TMPDIR, three unique namespaces, two sessions, an
explicit `/usr/bin/chromium`, content boundaries, exact
`--allowed-domains 127.0.0.1`, and a loopback fixture server on port 48759.
Doctor found no pre-existing daemon. Representative fixed environment:

```bash
env \
  HOME=/tmp/plato-159-agent-browser.RDgkU0/home \
  TMPDIR=/tmp/plato-159-agent-browser.RDgkU0/tmp \
  AGENT_BROWSER_NAMESPACE=plato159proof \
  AGENT_BROWSER_EXECUTABLE_PATH=/usr/bin/chromium \
  AGENT_BROWSER_CONTENT_BOUNDARIES=1 \
  AGENT_BROWSER_MAX_OUTPUT=20000 \
  AGENT_BROWSER_ALLOWED_DOMAINS=127.0.0.1 \
  AGENT_BROWSER_IDLE_TIMEOUT_MS=60000 \
  AGENT_BROWSER_DEFAULT_TIMEOUT=5000 \
  agent-browser --session plato159-a --json <fixed command>
```

Observed behavior:

```text
open local index -> success; browserLaunched=true; background PID=981428
tab list -> stable t1
snapshot -> e1 heading, e2 button, boundary nonce/origin
click @e2 -> success; get text #harmless -> "Clicked"
navigate t1 to second.html; click old @e2 ->
  exit 1, success=false, "Unknown ref: e2"
back + fresh snapshot -> fresh refs, button name "Clicked"
tab new with label second -> stable t2; two tabs retained t1/t2
screenshot -> PNG 1280x633, 7012 bytes, mode 0644
  sha256=0d6564067a62aa0fbc4e4fd372235f6ec9e73e5f0dc24840f059cf97b5fe0656
```

Session isolation and guard behavior:

```text
session A localStorage plato159=alpha
session B same origin read -> null
session A read -> alpha
B, default timeout 1000ms, wait #never ->
  exit 1, "Wait timed out after 1000ms"
open http://example.com with allowlist 127.0.0.1 ->
  exit 1, "Domain 'example.com' is not in the allowed domains list"
```

Output-cap contradiction:

```text
500-button snapshot, --max-output 2000, plain output:
  2263 bytes including "[truncated: showing 2000 of 19281 chars...]"
same command with --json:
  data.snapshot length=19281, refs=500, no truncation warning
```

Closing both sessions and the cap-test session left each session list empty and
`pgrep -x agent-browser` empty. The daemon removed live processes but retained
mode-0644 namespace `.config` hash sidecars under user-runtime directories; the
final cleanup explicitly removed those run-unique directories.

### Final proof cleanup

The fixture server was interrupted and exited. Then:

```bash
for path in \
  .tmp-proof-159 \
  /tmp/plato-159-cua.INDe47 \
  /tmp/plato-159-agent-browser.RDgkU0 \
  /run/user/1000/agent-browser/namespaces/plato159proof \
  /run/user/1000/agent-browser/namespaces/plato159proof-b \
  /run/user/1000/agent-browser/namespaces/plato159proof-cap
do
  if test -e "$path"; then find "$path" -depth -delete; fi
done
pgrep -a -x cua-driver
pgrep -a -x agent-browser
pgrep -a -f '[p]ython3 -m http.server 48759'
git status --short --branch
```

```text
all six paths: absent
cua-driver processes: none
agent-browser processes: none
fixture server processes: none
repo: no untracked proof artifact
```

## Rollout / Security / Privacy

- Rollout is disabled by default and ordered F1/F2, then disposable F3, then
  separately accepted F4. F1 cannot advertise `browser.open` before its SSRF
  enforcement gate passes; after each gate, tools absent from `tools.enabled`
  remain unavailable. An adapter version change requires a new pinned
  conformance proof rather than a mutable dependency update.
- Rollback is code/config/sidecar removal only. There is no schema or
  `platonic-core` migration. Durable prior ledger events and artifact metadata
  remain readable.
- Protected desktop and personal-browser observations are SecretAccess;
  isolated-browser screenshots and all ledgers are user-private app state. No
  raw cookies, credentials, profile storage, or image bytes enter model context.
- Every browser/desktop mutation is locally approved, target/ref bound,
  generation checked, recorded, post-observed, and never yolo. A cleanup
  failure prevents successful run termination.
- Supply-chain trust is an exact tag/version plus release digest, a packaged
  license, no runtime download/update, and Chrome Web Store signing for the
  extension. Platform hashes without native execution do not authorize ship.

## Minimal dependency-gated follow-up issues

AJ accepted Revision 1 on 2026-07-30, clearing only the design dependency for
F1/F2/F3 issue refinement. Each still requires its own GitHub issue with clear
scope, non-goals, acceptance, target surface, and proof, marked `Ready for dev`
before work starts. This design creates or starts none of them. F4 remains
blocked on the separate post-F3 human gate. GitHub Issues remain the only work
authority.

### F1 - Isolated browser adapter and artifact evidence

One issue, one bounded app slice, only after its own GitHub contract is marked
`Ready for dev`:

- `src/browser_control.rs`: concrete 0.33.1 argv builder, process/session owner,
  JSON parser/caps, URL/SSRF checks, tab/observation registry, stale-ref
  re-resolution, postcondition and cleanup. It is not a generic sidecar/MCP
  runner.
- `src/tool_artifacts.rs`: the concrete run PNG staging/validation/manifest
  path used by browser screenshots.
- `src/tool_catalog.rs`, `src/tools.rs`, `src/app.rs`, and `src/config.rs`:
  exact schemas, dispatch, effect overrides, previews, driver config, structured
  error results, cleanup-before-terminal ordering, and no-yolo tests.
- focused module/integration fixtures prove open, tabs, action, screenshot,
  isolation, stale ref, SSRF policy including an explicitly configured local
  origin, destination-IP enforcement across initial navigation, redirects,
  subresources and DNS rebinding, JSON cap, timeout, cancel, crash, uncertain
  action, artifact modes/hash, missing/wrong driver, and zero residual
  process/state.
- `browser.open` stays unadvertised/disabled with
  `ssrf_enforcement_unavailable` until that destination-IP test passes. A
  warning, approval, or opt-in configuration is not an acceptance substitute.
  If a concrete enforcing transport is needed, F1 must name and review it in
  that issue; #159 does not design one.
- `README.md` or `docs/QUICKSTART.md` documents post-gate opt-in enablement and
  exact missing-driver install/version diagnostics as required for user-visible
  behavior.
- Packaging includes one target-specific asset only after native CI/VM proof;
  development can use an external exact-version binary. Rollback disables the
  tools and removes the sidecar from packaging; there is no data/core migration.

No new Cargo dependency is expected: existing `url`, `sha2`, `serde_json`,
standard process APIs, `rustix`, and Windows job support cover the slice.

### F2 - Cua desktop adapter with mutation conformance gate

One issue, only after its own GitHub contract is marked `Ready for dev`. It
proceeds independently of F1 and neither requests nor creates screenshots or
artifacts:

- `src/computer_control.rs`: concrete 0.14.1 `serve`/`call` supervisor, generated
  Cua allowlist, window/observation registry, PID-start/window revalidation,
  fresh-token semantic resolution, postcondition and cleanup.
- the same catalog/tools/app/config/docs surfaces add only the three desktop
  provider tools and exact policy/error/diagnostic behavior.
- Linux X11/XWayland tests use an owned harmless fixture and must reproduce the
  upstream stale-token/dead-target cases while proving Plato rejects them before
  action. They also prove screenshot-free observation, permission refusal,
  interactive-session absence, timeout/cancel/crash, uncertain action, no-yolo,
  socket/policy modes, and cleanup.
- Mutation remains compile-time/catalog-disabled until the wrapper tests pass.
  macOS/Windows remain typed unsupported diagnostics until separate native proof
  issues are justified.
- Rollback disables/removes the tools and stops the owned sidecar. Cua remains a
  separate user installation; no ledger/core migration exists.

### F3 - Signed relay conformance proof

One research/security issue after F1 and only after its own GitHub contract is
marked `Ready for dev`, still disposable and non-production:

- prove a Chrome Web Store trusted-tester/unlisted signed MV3 item and stable ID;
- prove exact `allowed_origins`, caller origin, one-time rendezvous/nonces,
  replay/expiry/multiple-waiter refusal, 1 MB/64 MiB framing bounds, and
  same-user file/socket modes on Linux, macOS, and Windows;
- prove action-click offer, local approval before debugger attach, exact tab
  scope, pre-offer and pre-attach refusal for `chrome:`, `chrome-extension:`,
  `devtools:`, `file:`, `data:`, `about:`, every other opaque/internal scheme,
  and a navigation/scheme-change race; prove cross-origin suspension, all
  revocation paths, DevTools detach, service worker restart,
  root/same-process-frame behavior, and rejection of every CDP method outside
  the allowlist; and
- threat-model malicious page output, another extension, stale native host,
  compromised same-user process, and Chrome update/CDP drift.

A human must accept the proof, Chrome warnings, distribution route, method list,
and residual risk before F4 can become Ready for dev. Failure or unavailable Web
Store signing closes the signed-in path; it does not fall back to raw CDP,
auto-connect, an unpacked extension, or profile reuse.

### F4 - Signed-in selected-tab tools

One production issue only after the F3 human gate:

- `extension/plato-browser-relay/`: minimal MV3 manifest/service worker/action
  UI with no content script or generic CDP surface;
- `src/personal_browser.rs`: rendezvous, native framing, lease/ref registry,
  method construction, policy, postconditions, revocation, and cleanup;
- `src/bin/plato-agentd.rs`: narrowly detect Chrome native-host invocation and
  enter the relay, while the waiting shared run runtime still owns semantics;
- catalog/tools/app/config/docs plus installer registration for the exact signed
  extension ID and native host on each proved platform; and
- integration tests cover the signed-in invariants in D5-D8 and D10, with no
  screenshot or typing.

Rollback unregisters the native-host manifest, disables the tools, detaches any
owned target, and lets Chrome disable/uninstall the extension. Durable ledgers
remain readable because they contain ordinary existing core events and
structured data only.

## Acceptance Criteria

Revision 1 met its human-review acceptance criteria when:

- the PR changes only this file;
- link readback reaches every cited primary source;
- `git diff --check` passes;
- proof cleanup shows all disposable paths/processes absent; and
- the PR records the exact commit, proof commands, and the decisions above.

AJ accepted Revision 1 on 2026-07-30 and authorized merge/close in the canonical
#159 decision relay. Adoption makes this document Active and clears the design
dependency for F1/F2/F3 issue refinement only; each still needs its own GitHub
contract marked `Ready for dev`. It does not accept F4, which retains its
separate post-F3 human gate.

## Verifiable End Condition

The #159 research/design goal is terminal when this document is the only changed
tracked file, every disposable proof path/process is absent, source links and
quoted release pins read back, repository validation passes, the exact commit is
pushed in a PR to `develop` with `Closes #159`, proof is posted, and the PR is
merged and #159 closed by the authorized lead under AJ's recorded acceptance.
That acceptance does not mark any follow-up `Ready for dev`.

## Proof Expectations

- `git diff --check`
- script/readback of every Markdown link with no broken required source
- release/tag/version/checksum readback for both adopted drivers
- `pgrep` plus exact temp/runtime-path absence after proof cleanup
- `git status` proving only this design before commit and a clean tracked
  worktree after commit
- PR file/commit/base/body readback proving one file, the exact commit,
  `develop`, and `Closes #159`

## Drift Watch / Open Questions

No decision needed by F1 or F2 is left open. The following contradictions and
external dependencies are explicit gates, not questions for an implementer to
silently answer:

- Cua 0.14.1 did not fail closed for stale Linux AX tokens or dead PID/window
  state; the app guard and mutation conformance gate are mandatory.
- Agent-browser 0.33.1 ignored its output cap for JSON and wrote a mode-0644
  screenshot; app caps/private staging are mandatory.
- The agent-browser GitHub release is marked non-immutable even though the tag
  and asset digest are pinned; packaging must verify the digest, and every
  version bump is a new proof issue.
- Only Linux x64 was executed. Other release hashes are not support claims.
- Hostname allowlisting does not settle DNS rebinding. This is an F1
  implementation gate: `browser.open` remains unavailable until destination-IP
  enforcement is proved across initial navigation, redirects, and subresources.
- `chrome.debugger` has intentionally broad potential authority, Chrome/CDP can
  drift, and Web Store review/signing is external. F3 plus a second human gate
  prevents an implicit fallback.
- Prompt injection cannot be eliminated when a model reads hostile UI/page
  content. Structured refs, closed commands, caps, untrusted wrapping,
  exact-origin policy, no-yolo mutations, and local consent bound its authority;
  they do not make page claims trustworthy.
