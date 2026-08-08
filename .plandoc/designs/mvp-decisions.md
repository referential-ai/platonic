---
title: Hermes-light MVP Decisions
issue: https://github.com/referential-ai/plato-agent/issues/44
---

> **PARTLY SUPERSEDED, 2026-08-07.** This design predates the platform
> decisions of 2026-08-06/07. The
> [platform decision map](https://github.com/referential-ai/platonic-workspace/issues/83)
> supersedes it where they disagree, and the map wins.
>
> The MVP framing predates P018 (the server is the product) and P025 (personal
> use is the same product, not a separate surface). Q5's OS-sandboxing
> fast-follow is now owned by plato-agent#437 under P020, which stores
> confinement intent rather than mechanism.

# Hermes-light MVP Decisions

## Authority
- Human direction: make Plato a usable mini Hermes-style product, then execute from agreed decisions.
- Issue #44 is the scope contract for this artifact.
- PR #43, if accepted, is the product-spine parent: local sessions and safety before gateway work.
- `docs/ARCHITECTURE.md`: `plato-agentd` owns runtime semantics; gateways are future ingress adapters.
- `platonic-core` remains sans-IO and unchanged.
- Claude architecture-lane input, 2026-07-05: decide the remaining product/safety defaults before implementation.

## Desired Outcome
Jerome can answer the MVP-defining questions in one file. A reviewer can verify exactly seven answer slots, specific defaults, cited comparable evidence, and a clear rule that no implementation starts until the answers are accepted.

## How To Answer
Edit each `Jerome:` line. If the recommended default is accepted, write `default`.

Acceptance must be recorded in this file or on issue #44. A pane-level `proceed` does not accept defaults or authorize implementation.

## Scope / Anchor Boundary
- Scope: `plato-agent` product decisions for provider defaults, usable tasks, sessions, context, `shell.exec`, network posture, and streaming.
- Anchor workflow: local terminal use in one scratch-safe workspace.
- Anchor command feel: `plato`, `plato --config <path>`, `plato -c`, `plato replay`, and later `plato tui`.
- `platonic-core` is out of scope.

## Source Grounding At Adoption

Historical 2026-07-05 snapshot; linked code and issues own current behavior.

Checked local sources at adoption:
- `docs/ARCHITECTURE.md`: `plato`, `plato-agentd`, and `plato-tui` topology; gateway boundary; shared run-driving rule.
- `docs/QUICKSTART.md`: current local commands for `plato`, `plato replay`, `plato-agentd`, and `plato-tui`.
- PR #43 `.plandoc/designs/hermes-light-product-spine.md`: proposed product spine, first slices, and transparent-learning direction.
- Issue #44: this artifact's scope, non-goals, acceptance, and proof.

Checked comparable sources are cited in the appendix below.

## Decisions

### Q1. Provider And Model
Question: Which provider/model should be the MVP default path?

Recommended default: OpenRouter through the existing OpenAI-compatible wire path. Native Anthropic, provider fallback, and broad provider abstractions are out for MVP.

Jerome: Accept default.

### Q2. First Usable Tasks
Question: What three tasks must work before calling this usable?

Recommended default:
- Answer questions about the current repo/workspace.
- Create or edit a note/file with approval and visible diff.
- Run a build/test command and report the result.

Jerome: Accept default.

### Q3. Session Shape
Question: What should `plato -c` and bare `plato "..."` mean?

Recommended default: `plato -c "..."` continues the latest session for the current workspace from a SQLite pointer. Bare `plato "..."` starts a fresh session. Named sessions are deferred.

Jerome: Accept default.

### Q4. Long Sessions
Question: What happens when a continued session exceeds the token budget?

Recommended default: drop oldest turns with a visible marker. No automatic summarization in MVP.

Jerome: Accept default.

### Q5. `shell.exec` Safety
Question: What is the MVP safety boundary for local command execution?

Recommended default: every command requires approval; cwd is the workspace root; the child environment is deliberately scrubbed; output is capped; no provider credentials are inherited; `--yolo` does not cover `shell.exec` in MVP. OS/container sandboxing is a fast-follow, not the first gate.

Jerome: Accept default.

### Q6. Network
Question: Is there a dedicated network tool in MVP?

Recommended default: no dedicated network tool. Network effects only happen through approved `shell.exec` commands. Command text is visible at approval time; hidden network behavior inside scripts is a `shell.exec` safety-design concern.

Jerome: Accept default.

### Q7. Streaming
Question: Is streaming required before MVP?

Recommended default: no. Spinner plus final answer is acceptable for MVP. Streaming is a fast-follow after session continuation and `shell.exec` safety are proven.

Jerome: Require scoped streaming for MVP: live user-facing assistant text in CLI/TUI. Token deltas are not durable ledger events; replay records and shows the final assistant message only. Tool-call streaming is not required.

## Constraints
- Keep the first product proof local and replayable.
- Keep credentials out of config values; config may name an environment variable.
- Keep gateways as daemon clients after local sessions and safety are real.
- Keep learning transparent, local, evidence-backed, and separately designed.
- Transparent learning stays linked to the product-spine direction and remains after sessions, `shell.exec`, and local product proof.
- Say each decision once here; downstream issues should link back instead of restating.

## Non-Goals
- No implementation in this PR.
- No config/session/`shell.exec`/gateway code.
- No YAML decision.
- No multi-provider fallback design.
- No native Anthropic client work.
- No session summarization.
- No gateway, cron, memory store, MCP adapter, provider client, or UI work in `platonic-core`.

## Forbidden Operations
- Do not start implementation from this artifact until Jerome answers or explicitly accepts the defaults.
- Do not use this document as active work status.
- Do not copy comparable architectures; use them only to validate or challenge product defaults.
- Do not make remote channels a grant surface for approval-required effects.

## Comparable Scan Appendix: Session And First-Run UX
Scope: timeboxed scan of session and first-run UX only. This is not a gateway or architecture survey.

### Hermes Agent
Checked sources:
- [Quickstart](https://hermes-agent.nousresearch.com/docs/getting-started/quickstart)
- [Configuration](https://hermes-agent.nousresearch.com/docs/user-guide/configuration)
- [CLI Interface](https://hermes-agent.nousresearch.com/docs/user-guide/cli)
- Local inactive source snapshot: `/home/jerome/projects/_ICEBOX/former-active/hermes-agent`.

Findings:
- Hermes centers one simple terminal entry point: `hermes`.
- Hermes stores user config under `~/.hermes/`, with non-secret settings in `config.yaml` and secrets in `.env`.
- Hermes config precedence is CLI arguments, `~/.hermes/config.yaml`, `~/.hermes/.env`, then defaults.
- Hermes treats the CLI working directory as the default command workspace.
- Hermes has a large setup/gateway/memory surface; that validates the target feel but is too broad for Plato MVP.

Product implication for Plato: prefer a simple global config path and a plain `plato` entry point, but keep setup wizard, gateway, and memory out of the first local MVP.

### OpenCode
Checked sources:
- [CLI](https://opencode.ai/docs/cli/)
- [TUI](https://opencode.ai/docs/tui/)
- [Providers](https://opencode.ai/docs/providers/)
- [Intro](https://opencode.ai/docs/)

Findings:
- `opencode` starts the TUI by default; `opencode run "..."` supports non-interactive use.
- `--continue` / `-c` continues the last session, and `--session` / `-s` targets a specific session.
- The TUI exposes `/new`, `/sessions`, `/compact`, and `/connect`.
- Provider credentials are configured with `opencode auth login` and stored outside the project in an app data file.
- `/init` creates project guidance after provider setup; it is not required to start the first session.

Product implication for Plato: `plato -c` continuing the latest workspace session is an established CLI pattern; named session selection and compaction can wait.

### Aider
Checked sources:
- [In-chat commands](https://aider.chat/docs/usage/commands.html)
- [Release history](https://aider.chat/HISTORY.html)
- [Options reference](https://aider.chat/docs/config/options.html)
- [Optional setup](https://aider.chat/docs/install/optional.html)

Findings:
- Aider exposes `/run` and `/test` as user-visible shell workflows.
- Aider added `--restore-chat-history` so a user can continue the prior conversation on launch.
- Aider supports API-key configuration through CLI options and environment/config files.
- Aider calls out OpenRouter as a one-key route to many providers.

Product implication for Plato: local command execution is central to usefulness, but it must be approval-gated and replayable; OpenRouter is a good MVP default because it reduces provider setup decisions.

## Ownership Map
- Provider and config decisions: `src/config.rs`, `src/provider/mod.rs`, `src/provider/openai_compat.rs`, `src/bin/plato.rs`.
- Sessions and replay: `src/app.rs`, `src/paths.rs`, SQLite ledger code, daemon handlers.
- `shell.exec` and file tools: `src/tool_catalog.rs`, `src/tools.rs`, approval flow, TUI approval modal.
- Gateway later: daemon protocol client code only, not alternate runtime semantics.
- Core semantics: `platonic-core`; no product IO belongs there.

## Rollout / Security / Privacy
- Each implementation slice needs its own GitHub issue, PR, proof, and user-facing scratch-workspace validation when behavior changes.
- Credential handling and `shell.exec` environment hygiene are security gates, not polish.
- Any future learning feature must be local, inspectable, correctable, forgettable, and backed by replayable evidence.
- Remote gateway approval grants remain forbidden unless a later security design explicitly changes that contract.

## Acceptance Criteria
- The seven `Jerome:` slots exist and are the only required human answers.
- Each recommended default is specific enough to become issue scope after acceptance.
- The comparable appendix cites checked sources for each product claim.
- The document does not authorize implementation by itself.

## Verifiable End Condition
- Jerome can answer the MVP-defining questions by editing this one file.
- A downstream implementation issue can link this file and name which accepted defaults it implements.

## Proof Expectations
- `git diff --check`
- Design-goal scorer run for this artifact.
- Docs-only PR linked to issue #44.
- CI green.
- Claude architecture lane asked to check contradictions against the product spine and workspace boundaries.

## Risks
- Session semantics can sprawl if latest-session selection, compaction, and named sessions are combined too early.
- `shell.exec` can leak secrets or create unreviewed side effects if environment/cwd/output caps are not part of the first implementation.
- Gateway pressure can distract from the local product proof.
- Comparable research can become architecture copying; this scan intentionally stays at UX/product level.

## Drift Rules
- If a default changes, update this file or the owning implementation issue before coding.
- If an implementation issue contradicts this file, the issue must quote the contradiction and name the new human decision.
- If Claude or another reviewer finds a boundary contradiction, resolve it in GitHub before implementation continues.

## Questions Recorded at Adoption
- Are the recommended defaults accepted as written?
- Which of the first usable tasks is the first scratch-workspace product proof?
- Does Jerome want `plato` to become the interactive TUI entry point before or after `plato -c` works?
- The session implementation issue must name the SQLite single-writer decision before daemon/CLI coexistence changes.
