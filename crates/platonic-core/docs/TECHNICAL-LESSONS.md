# Technical Lessons from Rust Agent Harnesses

This doc captures what Platonic should learn from current Rust agent/harness projects without copying their sprawl.

## Goose

What to learn:

- Native CLI/desktop/API matters for adoption.
- Extension/provider flexibility is useful.
- A Rust core can carry a serious local agent UX.

What not to copy into core:

- Desktop, gateway, provider zoo, and extensions should not live inside the harness kernel.
- Product surface can be broad; core must stay narrow.

Platonic takeaway: keep core separate from app shells.

## Rig

What to learn:

- Provider abstraction and composable LLM calls are library problems.
- Agentic workflows should be built from small, typed Rust pieces.

What not to copy:

- Provider abstraction alone is not an execution harness.

Platonic takeaway: use provider adapters as replaceable edges; do not let provider mechanics define the kernel.

## AutoAgents / ADK-Rust

What to learn:

- Modular agents, models, tools, memory, and deployment are attractive to users.
- Type-safe tool boundaries are a strength of Rust.

What not to copy:

- “Everything framework” gravity. The module list can become the product and bury the harness contract.

Platonic takeaway: expose extension points, but keep the kernel focused on run orchestration and side-effect accounting.

## Chidori

What to learn:

- Durable, replayable, resumable execution is a first-class differentiator.
- Treat every LLM/tool/HTTP operation as a host call crossing a recorded boundary.
- Replay should cost zero model calls when inputs are cached.

Platonic takeaway: event log first. Transcript, replay, metrics, and audit views are projections.

## Cersei / pi_agent_rust / pie

What to learn:

- Single-binary Rust agents can be fast and comprehensible.
- Local sessions, resumability, MCP/tools, and low memory footprint are valuable.
- Stateful loops/cron-like automation should route findings through a triage surface, not spam users.

What not to copy:

- Coding-agent assumptions should not leak into the generic core.
- “Skills” and cron loops can become uncontrolled sprawl.

Platonic takeaway: build the generic harness first; coding-agent features live above it.

## C.A.D.I.S.

What to learn:

- One daemon owning orchestration, tool policy, and approval state is a strong anti-sprawl boundary.
- Local-first and policy-gated tools are the right instinct.

Platonic takeaway: policy must be central, explicit, and inspectable.

## Hermes

What to learn:

- Persistent agents need memory, session search, scheduled jobs, delegation, and platform adapters somewhere in the product stack.
- Tool batching, interruptibility, context compression, and session persistence are real operational needs.

What not to copy:

- One god-class agent owning prompt assembly, provider routing, tool execution, fallback, compression, memory flushing, callbacks, sessions, gateway concerns, and cron concerns.
- Self-improving skills as an unbounded accumulation surface.
- Silent provider fallback in unattended jobs.

Platonic takeaway: Hermes proves the demand; Platonic should be the smaller harness Hermes needed underneath. The god-class decomposes onto the kernel: prompt assembly → `context`, provider routing/fallback → `policy` plus host, tool execution → host-fulfilled commands, compression → recorded compaction events, sessions/search/callbacks → event-log projections outside core.

## IronClaw / OpenFang-style projects

What to learn:

- Rust rewrites of agent OS concepts are active and visible.
- Privacy/security/local-first language resonates.

What not to copy:

- “Agent OS” marketing sprawl.
- Grand claims before a small contract is proven.

Platonic takeaway: do not sell an operating system. Ship a harness contract.

## Security study: Codex, IronClaw, OpenFang, Goose (2026-07-12)

Source study of four Rust agents for security layers and patterns. Read-only
clones live under `~/projects/_reference/` (outside the workspace); citations
below are file/line at these pinned commits:

- openai/codex `c888e8e75a9f0e90ce7d5517f8b9540832cbbf76`
- nearai/ironclaw `1bcbde20332759aa9c7ce00fd55fcac5bd0885fc`
- RightNow-AI/openfang `acf2587e46be174c10200489c9a2d23a39a98aeb`
- block/goose `858e8de359b6bd585813d25397744feffb50e8db`

Full triage lives on plato-agent#132; the shell-sandbox mechanism comparison is
on plato-agent#81. All findings are patterns to evaluate against our own
contract — no vendoring, no code adoption implied; any implementation needs its
own card with a license/API check.

### Codex — the OS-sandbox reference

Paths under `codex-rs/`.

- Linux: **bubblewrap is the default FS sandbox; Landlock is demoted to legacy
  fallback** (`linux-sandbox/src/linux_run_main.rs:76-79`,
  `linux-sandbox/src/landlock.rs:2-4`) because Landlock `ABI::V5` can't express
  restricted-read (`landlock.rs:71-77,140`). Two-stage exec: the helper builds
  the bwrap mount view, re-execs itself to apply seccomp +
  `PR_SET_NO_NEW_PRIVS`, then `execvp`s the command
  (`linux_run_main.rs:140-146,213-255,1410-1432`). bwrap flags:
  `--unshare-user --unshare-pid [--unshare-net] --die-with-parent
  --new-session` (`linux-sandbox/src/bwrap.rs:318-327`), `--ro-bind / /`
  baseline (`bwrap.rs:446-452`), `--bind` writable roots (`bwrap.rs:567-569`),
  re-`--ro-bind` of `.git` (`bwrap.rs:414-419`).
- seccomp deny-set (pattern to evaluate): `ptrace`, `process_vm_readv/writev`,
  `io_uring_*` always denied (`landlock.rs:179-184`); a `SeccompCondition` on
  `socket()` arg0 allows only `AF_UNIX` (restricted) or `AF_INET/6` (proxy
  mode) (`landlock.rs:206-216,225-246`).
- macOS: Seatbelt via `/usr/bin/sandbox-exec` (hardcoded path defends against a
  PATH-injected `sandbox-exec`, `sandboxing/src/seatbelt.rs:26-30`),
  `(deny default)` SBPL (`sandboxing/src/seatbelt_base_policy.sbpl:8`),
  writable roots passed as `-D` params not string-interpolated
  (`seatbelt.rs:761-767`). Cheapest OS sandbox to bolt onto an exec wrapper —
  no helper binary.
- Approval UX (approval → sandbox → attempt → escalate-on-denial,
  `core/src/tools/orchestrator.rs:4-7`): the first attempt runs unprompted
  only when tool + approval policy resolve to
  `ExecApprovalRequirement::Skip`; `NeedsApproval` prompts before the first
  attempt and `Forbidden` rejects (`orchestrator.rs:152-218`). On a sandbox
  denial, an unsandboxed retry is offered only if the tool opts into
  escalation (`orchestrator.rs:318`), only when there are no denied-read
  paths (else the retry silently grants those reads,
  `orchestrator.rs:331-361`, `core/src/tools/sandboxing.rs:280-284`), and
  with re-approval unless policy plus prior approval allow bypass
  (`orchestrator.rs:386-413`); denial detection is heuristic (keyword +
  Linux `128+SIGSYS` exit, `sandboxing/src/denial.rs:14-53`). In-session
  grants are cached by full canonicalized command
  (`core/src/tools/sandboxing.rs:89-115`); durable command-prefix grants are
  a separate exec-policy path (`core/src/exec_policy.rs:886-899`).
- Discovery hardening: accept a `bwrap` only if it resolves outside cwd
  (`sandboxing/src/bwrap.rs:180-190`); probe `--help` for required flags before
  trusting it (`linux-sandbox/src/launcher.rs:108-124`).
- Env: `env_clear()` (`core/src/spawn.rs:75`) then a derived allowlist, with
  default case-insensitive excludes `*KEY*`/`*SECRET*`/`*TOKEN*`
  (`protocol/src/shell_environment.rs:56-86`). Refines our env-scrub.

### IronClaw — untrusted-content and egress patterns

Paths under `crates/`.

- **`ironclaw_prompt_envelope`** (leaf crate, no ironclaw deps, ~430 LOC): one
  primitive `wrap_untrusted(source, trust, body)`
  (`ironclaw_prompt_envelope/src/lib.rs:184-188`). Ingress content matching
  `INSTRUCTION_LIKE_MARKERS` ("ignore previous instructions", `<|im_start|>`,
  "system prompt", … `lib.rs:155-176`) is **rejected, not scrubbed**
  (`lib.rs:211-213`); control chars stripped (`lib.rs:201-205`); byte-capped
  (`lib.rs:218-223`); word-boundary matching avoids `act as`⊂`impact` false
  positives (`lib.rs:242-263`). Pattern to evaluate for our
  gateway/memory/tool-output ingress: plato-agent#172.
- Tool-output pipeline — bounded neutralization of already-accepted output,
  distinct from the ingress rejection above: truncate → leak-scan (blocks on
  secret) → policy Block/Sanitize → injection sanitize
  (`ironclaw_safety/src/lib.rs:99-181`), with `<tool_output>` delimiters
  (`lib.rs:215-221`) and close-tag neutralization (`lib.rs:309-328`,
  boundary-injection defense).
- SSRF/rebinding egress: default-deny allowlist
  (`ironclaw_network/src/policy.rs:131-133`), block private/loopback/
  link-local/CGNAT-`100.64/10`/IPv4-mapped-IPv6 (`policy.rs:177-206`), then
  **resolve DNS and pin the resolved IPs into the transport** (rebinding
  defense, `ironclaw_network/src/egress.rs:91-92`,
  `ironclaw_network/src/transport.rs:180-185`) + reject caller Host header
  (`transport.rs:297-311`). Pattern to evaluate against our network-egress
  gap.
- Env-hygiene primitives for exec: reject raw `*API_KEY*/*TOKEN*/*SECRET*`
  values, block dangerous entrypoint env names (`LD_PRELOAD`,
  `LD_LIBRARY_PATH`, `PATH`, `BASH_ENV`, `IFS`) — one 108-line module
  (`ironclaw_process_sandbox/src/validation.rs:47-107`).
- Their Docker process-sandbox (`--cap-drop ALL`, `no-new-privileges`,
  `--network none`, `--pids-limit`, exec+argv never a shell line,
  blocked-mount-prefix list, no host-env inheritance,
  fail-closed-without-broker;
  `ironclaw_process_sandbox/src/docker.rs:350-428,512-553`,
  `ironclaw_process_sandbox/src/plan.rs:247-256`) is a reference shape for the
  container tier of our fast-follow. Caveat: read-only rootfs exists only in a
  different backend that runs `sh -c`
  (`ironclaw_host_runtime/src/sandbox_process.rs:418,428`); no single backend
  has both.
- Confirms our own posture at scale: effect classes (`EffectKind` ×13 with
  `is_write()`, `ironclaw_host_api/src/capability.rs:21-53`), `Decision =
  Allow(ordered obligations)/Deny/RequireApproval`
  (`ironclaw_host_api/src/decision.rs:22-26,133-191`), event-sourced redacted
  audit log (`ironclaw_events/src/sink.rs:147-164`), and two separate
  host-side channel-identity contracts: first-bound thread ownership via
  host-supplied trusted scope (`trusted_owner_user_id`,
  `ironclaw_conversations/src/traits.rs:21-34`) and external-actor pairing
  that is explicitly not self-service (`traits.rs:54-60`); the inbound path
  only passes host-configured scope through (`inbound.rs:169-183`).

### OpenFang — validation-by-counterexample

Paths under `crates/`.

- **Live remote-grant hole**: channel slash-commands are handled by an early
  return *before* the authorization gate
  (`openfang-channels/src/bridge.rs:886-899`; `authorize_channel_user` runs
  only on the later agent-routing paths, `bridge.rs:1124,1259,1761`), and the
  approval resolver has no owner check (`/approve` dispatch
  `bridge.rs:2163-2168`; resolver matches by id-prefix with a hardcoded
  `"channel"` identity, `openfang-api/src/channel_bridge.rs:649-685`) — so any
  channel user can `/approve` a pending Critical approval. This is exactly the
  "remote channel grants an approval" hole our gateway design (D5) forbids by
  construction. Their `DmPolicy::AllowedOnly` is an empty match arm
  (`bridge.rs:836-838`); default authorize is fail-open when no users are
  registered (`channel_bridge.rs:840-842`,
  `openfang-kernel/src/auth.rs:176-178`).
- Patterns to evaluate: `ChannelAdapter` trait exposing only platform
  I/O/presentation methods while one centralized `dispatch_message`
  chokepoint owns policy and routing for all 43 adapters
  (`openfang-channels/src/types.rs:292-370`, `bridge.rs:754` — validates our
  gateway boundary); `serde(deny_unknown_fields)` on routing/binding config so
  a typo fails closed (`openfang-types/src/config.rs:741,799`); taint guards
  (secret-labeled data blocked from egress, external-tainted data from shell,
  `openfang-types/src/taint.rs:126-147`) — labels come from substring
  heuristics, not real provenance tracking; a shell-wrapper bypass detector
  that unwraps `bash -c "curl evil"` and re-validates the inner command
  (`openfang-runtime/src/subprocess_sandbox.rs:198-217,381-395`).
- Their `SECURITY.md` overstates controls the code doesn't implement:
  "zeroization on all API key fields" (`SECURITY.md:65`) vs plain-`String`
  secrets (`openfang-runtime/src/embedding.rs:37`,
  `openfang-channels/src/email.rs:27`). Reminder to verify at source, never
  trust the security doc.

### Goose — the anti-pattern on gateways

Paths under `crates/goose/src/`.

- **Gateway inherits the global permission mode with no per-tool approval**:
  pairing creates the session with the configured global mode
  (`gateway/handler.rs:157,283-285`), and `Auto` mode approves every tool
  (`permission/permission_inspector.rs:147`) — a paired remote user gets
  auto-approved tool execution; tool requests surface only as typing
  indicators (`gateway/handler.rs:441-467`). Our owner-allowlist + notify-only
  design is categorically safer.
- The inspector chain itself is always registered
  (`agents/agent.rs:642-669`), but the prompt/command injection classifiers
  default off (`security/mod.rs:54,71,97,101`), inspector errors fail open
  (`tool_inspection.rs:107-115`), and the egress inspector extracts exfil
  destinations but **always returns Allow** (log-only, not enforcement,
  `security/egress_inspector.rs:356-383`). Do not mistake detection for a
  control.
- Patterns to evaluate — the shape, not the defaults: read-only-hint →
  auto-approve with a cached LLM "permission judge" for unannotated tools
  (`permission/permission_inspector.rs:159-176,219-247`); an OSV `MAL-*` gate
  before launching npx/uvx extensions (`agents/extension_manager.rs:1097`,
  `agents/extension_malware_check.rs:48-56`) — blocks on a positive hit but
  fails open on lookup errors (`extension_malware_check.rs:214-230`); a
  pluggable `ToolInspector` chain whose merge rule is "restrictive always
  wins, Allow never overrides" (`tool_inspection.rs:213-253`).

### Cross-cutting takeaways

Scoped to the four reviewed repositories.

1. **None of the four builds its primary Linux sandbox on Landlock** — Codex
   demoted it to legacy fallback; bubblewrap+seccomp or a container is what
   ships. Our fast-follow should target bwrap+seccomp first, Landlock only as
   a fallback.
2. **Default-on, fail-closed** is the dividing line. Goose ships its
   injection classifiers off, its egress inspector log-only, and fails open
   on inspector error; OpenFang's authorization is fail-open until users are
   registered. For effect-gated tools the right default is on and closed.
3. **Reject instruction-like untrusted ingress; neutralize accepted tool
   output** (IronClaw). Two distinct rules: ingress that looks like
   instructions is rejected outright; output that passed the gate is bounded,
   delimiter-wrapped, and close-tag-neutralized. Cut as plato-agent#172.
4. **Remote channels must never self-grant** — proven twice by counterexample
   (OpenFang command bypass, Goose mode inheritance). Our D5 notify-only stands
   validated.
5. **env_clear + derived allowlist with `*KEY*/*SECRET*/*TOKEN*` excludes** and
   dangerous-env-name blocking are cheap refinements applied by both Codex and
   IronClaw.

## Core design rules

1. Kernel owns only run orchestration, context packs, tool-call policy, event logs, and replay hooks.
2. Gateways, schedulers, platform adapters, memory stores, UI, and model providers are outer crates/apps.
3. Every side effect emits a structured event.
4. Context assembly is lane-labeled and budget-validated before model calls.
5. Tool schemas are typed and selected, not dumped wholesale.
6. Tool results are summaries plus structured data plus artifact refs.
7. Fallbacks require policy and are recorded.
8. Unsafe code is forbidden in the core.
9. Benchmarks and telemetry are part of the contract, not an afterthought.
10. If a feature increases sprawl, it belongs outside `platonic-core` until proven otherwise.
