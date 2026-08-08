---
title: Hermes-light Product Spine
issue: https://github.com/referential-ai/plato-agent/issues/42
---

> **PARTLY SUPERSEDED, 2026-08-07.** This design predates the platform
> decisions of 2026-08-06/07. The
> [platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
> supersedes it where they disagree, and the map wins.
>
> Its stated goal — a "mini Hermes-style MVP" with `plato-agentd` owning
> runtime semantics — is inverted by P016 and P018: Platonic is the server and
> the product, and Plato Agent is the client distribution built on it. The
> product statement and proof now live in `product/product.md`.

# Hermes-light Product Spine

## Authority
- Human direction: get Plato to a clear usable mini Hermes-style MVP.
- Issue #42: define the product spine before gateway or broad implementation work.
- `plato-agent/docs/ARCHITECTURE.md`: `plato-agentd` owns runtime semantics; gateways are future ingress adapters.
- `platonic-core/docs/ARCHITECTURE.md`: core stays sans-IO; gateways, providers, stores, tools, and UI stay outside core.
- Claude architecture-lane input, 2026-07-05: sessions are the real product gap; do not lead with gateway work.
- Human direction, 2026-07-05: the product should eventually let the user see how the agent learns about them over time.

## Source Grounding At Adoption

Historical 2026-07-05 snapshot; linked code and issues own current behavior.

Checked at adoption:
- `src/bin/plato.rs`: `plato "question"` exists; default ledger is `events.jsonl`; bare `--db` uses the XDG SQLite path.
- `src/paths.rs`: workspace-scoped SQLite/socket/lock paths already exist.
- `src/config.rs`: strict TOML config exists; missing `plato.toml` falls back to defaults; unknown tools fail closed.
- `src/app.rs`: each run builds model messages from only the current question; `MAX_TURNS` is hard-coded.
- `src/daemon/handlers.rs`: `message.append` starts a new run.
- `src/daemon/runtime.rs`: `session_id` is currently the `run_id`.
- `src/tool_catalog.rs`: the MVP tool catalog is file-only today: read, list, write, edit.
- `docs/ARCHITECTURE.md`: connectors and gateways must not own sessions, policy, approvals, provider fallback, or run semantics.

Not checked at adoption:
- External comparable implementations such as Hermes, OpenCode, or OpenClaw-style gateway systems.
- Real-provider product feel after PR #41 lands.

## Desired Outcome
- A local agent that feels simple:
  - `plato "do the task"`
  - `plato -c "follow up"`
  - `plato replay`
  - `plato tui` eventually wraps the current TUI binary.
- A user can start with environment credentials and no project boilerplate.
- Runs persist by default without polluting the workspace directory.
- A continued session remembers prior turns from the ledger.
- Local command execution is useful, approval-gated, and designed before implementation.
- The user can eventually inspect how the agent forms and revises its model of them.
- Gateways come later as thin daemon clients after local sessions and safety are real.

## Scope / Anchor Boundary
- Scope is the `plato-agent` product shell: CLI, config lookup, ledger defaults, sessions, daemon protocol clients, local tools, approvals, and user-facing docs.
- `platonic-core` is an anchor boundary and remains unchanged unless a later recorded semantic decision belongs in the sans-IO kernel.
- The MVP anchor workflow is local terminal use in one workspace, not remote ingress.
- The first product proof must run in a scratch workspace, not in this repo.

## Product Decisions
- Keep TOML as the config format for the MVP. Support path flexibility before format flexibility.
- Config resolution should be:
  1. `--config <path>`
  2. `$PLATO_CONFIG`
  3. `./plato.toml`
  4. `~/.config/plato/config.toml`
  5. built-in defaults
- Expand leading `~` for explicit config paths.
- Do not store API keys in config; continue using `api_key_env`.
- Prefer the XDG SQLite ledger by default for `plato "..."`; keep JSONL as an explicit export/debug path.
- Real sessions must precede gateway work.
- `shell.exec` is the first non-file tool candidate, but it needs a safety design before coding.
- Gateway v1 must not carry approvals. Remote channels may notify or deny approval-required effects; grants stay local.
- Learning must be transparent adaptation, not hidden personalization.
- Durable user-model changes require user approval or correction.

## Transparent Learning Product Shape
- The product should make learning visible as:
  - observation;
  - hypothesis;
  - evidence;
  - confidence;
  - proposed behavior change.
- Example product loop:
  - `plato reflect` proposes what the agent thinks it learned from recent sessions;
  - `plato learnings` lists approved and provisional learnings;
  - `plato learnings why <id>` shows evidence;
  - `plato approve-learning <id>` makes a proposal durable;
  - `plato correct-learning <id>` edits the claim;
  - `plato forget-learning <id>` removes it.
- Learning entries should distinguish facts, preferences, operating rules, current goals, hypotheses, and rejected assumptions.
- Every durable learning must cite local evidence such as run ids, transcript excerpts, explicit user corrections, or repeated observed behavior.
- The agent must state the behavior change caused by a learning, for example: "Before major implementation, gather design consensus and codify the plan."
- Provisional hypotheses may guide a conversation, but they must not become durable defaults without approval.
- Rejected assumptions are useful product evidence when the user approves recording them as corrections.
- A reviewer lane can audit proposed learnings before they become durable, using the same checked-truth rule as design review.

## Constraints
- `platonic-core` remains sans-IO and unchanged for this product spine.
- `plato` and `plato-agentd` must keep one shared run-driving implementation.
- One live writer owns a workspace store.
- Gateways are daemon-protocol clients, like the TUI, not alternate runtimes.
- Provider fallback remains per-run ledger evidence if added later.

## Non-Goals
- No gateway implementation before real local sessions.
- No YAML support in the MVP spine.
- No remote approval grants.
- No provider fallback design in the first MVP slices.
- No Anthropic-native provider work in the first MVP slices.
- No new core crates or crate split from this design alone.
- No hidden background user profile mutation.
- No remote or shared learning store in the MVP.

## Forbidden Operations
- Do not add gateway-owned sessions, policy, approvals, provider fallback, run semantics, or ledger writes.
- Do not put provider keys directly into config files.
- Do not pass the daemon's provider credential environment into `shell.exec`.
- Do not make `--yolo` grant external side effects or secret access by default.
- Do not use wiki pages, tmux panes, or plandoc body text as active work status.

## Ownership Map
- CLI UX and config resolution: `plato-agent/src/bin/plato.rs` plus `src/config.rs`.
- Workspace paths and default state: `plato-agent/src/paths.rs`.
- Run driving and ledger writes: shared `plato-agent/src/app.rs` flow.
- Session projection and replay grounding: SQLite ledger plus `platonic_core::RunReadback`.
- Daemon protocol and local clients: `plato-agent/src/daemon/*`, `src/bin/plato-tui.rs`.
- Tool catalog and safety classes: `plato-agent/src/tool_catalog.rs` and `src/tools.rs`.
- Core harness semantics: `platonic-core`; no product IO belongs there.

## Rollout / Security / Privacy
- Rollout happens as small PRs, each linked to a GitHub issue and proved with tests plus one scratch-workspace product proof when user-facing behavior changes.
- Security and privacy boundaries are part of each slice's acceptance when credentials, command execution, daemon sockets, gateways, or approval permissions are touched.
- Config must keep credentials indirect via `api_key_env`.
- `shell.exec` must run with a deliberately constructed environment and workspace cwd.
- Remote gateway v1 must deny or notify approval-required effects; local CLI/TUI remains the grant surface.
- Learning data must be local, inspectable, correctable, and forgettable.
- Daemon socket access and protocol compatibility must be designed before a gateway or second client class ships.

## Original Slice Boundaries

Linked issues and PRs own implementation and status.
1. Land the existing file.edit approval-preview fix before stacking implementation branches.
2. Config and defaults:
   - implement config resolution order;
   - support `~` expansion for explicit config paths;
   - default `plato "..."` to XDG SQLite;
   - keep JSONL explicit;
   - make turn count configurable.
3. Real sessions:
   - add session identity separate from run identity;
   - hydrate prior turns from the ledger;
   - add `plato -c "follow-up"`;
   - define one-active-run-per-session behavior.
4. `shell.exec` safety design and implementation:
   - decide effect class, yolo boundary, environment hygiene, cwd, output caps, and approval preview;
   - coordinate with issue #6 before enabling any network-class or command-exec behavior.
5. Comparable scan before gateway design:
   - inspect Hermes, OpenCode, and OpenClaw-style systems only for product/runtime lessons;
   - answer session, first-run, minimal-tool, permission, gateway-boundary, and streaming questions.
6. Gateway design after the local spine:
   - define socket security, protocol evolution, pairing/identity, event-stream recovery, and remote-deny behavior.
7. Transparent learning design after sessions:
   - define local storage, evidence links, approval/correction UX, and reviewer audit;
   - keep it out of the config/defaults and first session implementation slices.

## Acceptance Criteria
- `plato "..."` works from a normal workspace without creating `events.jsonl` by default.
- `plato -c "..."` continues the last workspace session and includes prior relevant messages.
- Replay can prove the continued session from persisted events.
- Config discovery works without requiring `--config` for normal use.
- `shell.exec` cannot leak provider credentials to child processes.
- Approval-required local effects can be granted locally and denied remotely.
- Gateway design is not admitted until local sessions and command safety are proven.
- Durable user learning is not admitted until sessions provide replayable evidence and the approval/correction UX is designed.

## Verifiable End Condition
- In a scratch workspace with only provider credentials available, a user can run:
  - `plato "create a scratch note"`
  - `plato -c "append a short follow-up"`
  - `plato replay`
- The replay shows a coherent persisted session, tool approvals where required, and no workspace-local ledger clutter unless explicitly requested.

## Proof Expectations
- Config/defaults slice: unit tests for resolution order, `~` expansion, XDG default, and explicit JSONL behavior.
- Session slice: daemon and CLI tests proving `message.append`/`plato -c` hydrate prior messages from SQLite.
- `shell.exec` slice: containment/cwd/env/output-cap tests, approval tests, yolo-boundary tests, and replay proof.
- Product proof: real-provider scratch run showing first command, continued command, replay, grant, and deny paths.
- Learning proof, when admitted later: a scratch session where `plato reflect` proposes evidence-backed learnings, the user approves one, corrects one, forgets one, and subsequent behavior changes are visible.

## Risks
- Session semantics can sprawl if identity, hydration, concurrency, and token-budget rules are not decided first.
- `shell.exec` is useful enough to be MVP-critical but dangerous enough to require a focused design.
- Remote approvals are a security product in their own right; deferring them is intentional.
- Streaming may matter for product feel, but it should not displace sessions and command safety.
- Comparable research can turn into architecture copying; keep it question-led and timeboxed.
- Visible learning can become creepy or manipulative if it is hidden, overconfident, or hard to correct.

## Questions Recorded at Adoption
- Should the last workspace session be selected by latest successful run, latest session record, or an explicit session pointer?
- How should long sessions compact or select context under token budget?
- Resolved by PR #47: `plato --tui` attaches to an existing workspace daemon or starts an embedded daemon for the TUI session.
- What exact environment variables are safe to pass to `shell.exec`?
- Does the first gateway target need pairing, local-only callback URLs, or daemon socket brokering?
- Should approved learnings be scoped per user, per workspace, or both?
- What level of evidence is enough before proposing a behavior-changing learning?
