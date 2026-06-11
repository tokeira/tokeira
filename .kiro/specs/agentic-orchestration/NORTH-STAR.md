# North Star: Tokeira-Orchestrated Agentic Workflows

> Status: vision / design-stage. Not yet a buildable spec. The walking-skeleton spec
> (requirements/design/tasks) will be carved from §"First slice" once posture (A) is confirmed
> against the gap trace in `reference/openai-sandbox-gap.md`.
>
> Working name: **Conductor** (placeholder — the tokeira-native agent orchestrator). Final name TBD.

## 1. The thesis

Agent work *is* a durable workflow. The unit of agent work — a spec task, a conformance run, a
bugfix, a review — is a tokeira workflow execution. tokeira (Temporal-compatible, DSQL-backed)
is the durable **orchestration spine**; the agent model loop and the sandbox are pluggable layers
that run *inside* that spine.

This is not speculative: the agent ecosystem converged on exactly this shape in 2026.
- [OpenAI's Agents SDK shipped native sandbox execution](https://openai.com/index/the-next-evolution-of-the-agents-sdk/)
  with an explicit **harness / sandbox** split, a portable **Manifest**, Unix permissions, and
  built-in snapshot + rehydration.
- [Temporal built the durability bridge](https://temporal.io/blog/introducing-temporal-and-agentic-sandboxes-openai-agents-sdk),
  shipping in OpenAI's own repo: `AgentWorkflow` (durable idle loop), `SessionManagerWorkflow`
  (start/stop/list/rename/**fork** via signals+updates, no database), and a TUI client driving it
  all over signals/queries/updates.
- [Temporal's Sandbox Orchestration Harness](https://temporal.io/blog/temporal-sandbox-orchestration-harness-the-missing-layer-for-running-agents)
  names the missing layer: provisioning-on-demand, driving execution from agent intent, state
  persistence, failure recovery, and idle-timeout cleanup across Modal / E2B / Daytona / GKE Agent
  Sandbox / Bedrock AgentCore.

(All external content paraphrased for licensing compliance.)

Because tokeira is Temporal-compatible, **these standard integrations should run against `tokeirad`
unmodified**. That makes the agentic foundation and a ruthless real-world conformance test the same
effort — and gives the project its defining demonstration: *tokeira durably running the agents that
build tokeira, on the standard ecosystem SDKs.*

## 2. Strategic posture: (A) Conform-and-host

Two postures were considered:
- **(A) Conform-and-host** — run the *existing* OpenAI/Temporal sandbox harness against `tokeirad`
  unmodified; build only the orchestration-spine config and the cockpit. Fastest to a working
  system; turns the OpenAI demo into a tokeira conformance gate.
- **(B) Native harness** — build a Rust-native orchestration harness on `sdk-core` with bespoke
  roles/routing/budgets. More control, much more build, slower.

**Decision: (A) first.** The walking skeleton runs OpenAI's sandbox coding-agent demo against
`tokeirad`; the gap trace (`reference/openai-sandbox-gap.md`) tells us exactly what tokeira must
support. Native pieces (roles, budgets, cockpit) grow around a spine we *know* works. Posture (B)
remains the long-horizon option for the parts where Rust-native control pays off.

## 3. Build-vs-adopt: own the spine, adopt the rest

The industry standardized the sandbox runtime and the harness. We do **not** rebuild them.

| Plane | Owns | Build / Adopt |
|---|---|---|
| **Orchestration spine** | tokeira workflows: session manager, fan-out (`waves`), budgets/policy, gates, cross-workflow memory; DSQL persistence | **Build** (it *is* tokeira) |
| **Agent harness** | OpenAI Agents SDK / Pydantic AI: model loop, tool calls, tool-level **approvals**, session memory / skills / compaction, the **Manifest** | **Adopt** (runs in workers, bridged via the Temporal plugin) |
| **Sandbox runtime** | isolation, snapshot, fork, idle-suspend | **Adopt** behind a provider selector: Docker (local) → E2B / Daytona / Modal / Bedrock AgentCore (remote) |
| **Cockpit** | parallel workflow progress, decision points, intervention prompts | **Build** — a signals/queries/updates client. **Not** Loom. |

### Why the cockpit is not Loom
Loom is service/operational observability — cluster stress, latency propagation, topology health.
The cockpit is a different instrument: it shows **workflow advancement**, surfaces **decision
points**, and **alerts the operator for intervention**. Its primitives are exactly what the
reference clients in the Temporal demos use — read workflow state via **queries / visibility**,
write interventions via **signals / updates**. Two tiers of intervention exist and the cockpit must
expose both:
- **Tool-level approvals** (fine-grained, harness-native): "approve this shell command."
- **Workflow-level gates** (coarse, spine-native): "approve this PR / fork / terminate / revert."

The operator's four verbs — **approve / guide / terminate / revert** — map onto signals + updates
against the durable workflow.

## 4. The five planes in detail

### Orchestration spine (tokeira)
- A **SessionManager-equivalent** workflow tracks the agent-session registry *in workflow state*,
  not a database — start/stop/list/rename/fork via signals + updates (mirrors the demo).
- **Fan-out** uses the spec task-DAG `waves` directly: a wave = a batch of child workflows / agent
  activities dispatched concurrently, each in its own sandbox/worktree.
- **Budgets & policy** live on the workflow as state + search attributes; activities check and
  decrement; over-budget trips a gate. Cost is steerable, not a surprise.
- **Gates** are workflow blocks on Update/Signal with timeouts that escalate rather than hang.
- **Cross-workflow memory** (see §Memory) is the spine's durable project memory in DSQL.

### Agent harness (adopt)
The harness owns the model loop, tool calling, tool-level approvals, and **session-scoped** memory /
skills / compaction. It is bridged to tokeira by the integration seam the Temporal demo calls
`temporal_sandbox_client()` — wrapping every sandbox/LLM operation as a durable, retryable activity
while `Runner.run()` stays pure SDK. One worker can host multiple sandbox clients.

### Sandbox runtime (adopt)
A provider selector chooses the isolation backend by **profile**. Lifecycle patterns adopted
wholesale from the orchestration-harness reference: provision-on-demand tied to workflow lifecycle,
**idle-timeout auto-suspend** (snapshot + stop; transparent resume — the platform-cost lever),
snapshot + **fork** (parallel exploration), and backend switching.

### Workspace = Manifest (adopt)
The OpenAI **Manifest** (stage files, clone git repos, output dirs, mount S3/GCS/Azure/R2, Unix
permissions) is the workspace definition. Our earlier "git worktree per task on a remote Graviton
box" is just **one Manifest profile** (clone + cache mount + permissions + resource profile). The
whole sandboxing discussion collapses into "pick providers + author profiles."

### Memory plane — a clean boundary
- **Session memory** (conversation, compaction, skills) → owned by the **harness**, durable via
  workflow history. We do not duplicate it.
- **Project memory** (cross-workflow, cross-session: decisions, findings, conventions) → owned by
  the **spine**, a thin DSQL-backed store updated via activities, keyed by spec/project.
- **Artifacts** → S3 (reuse `tokeira-state` patterns); snapshots are provider-native.
- Retrieval / vector index → **deferred**.

### Observation / interaction = Cockpit
See §3. Read via queries + visibility; write via signals + updates. Built as its own surface.

## 5. Agent roles (to confirm)

As I understand the division — correct me:
- **ChatGPT** — architect / navigator: design, planning, cross-cutting reasoning, review synthesis.
- **Kiro** — spec lifecycle + interactive in-IDE orchestration UX; the operator's primary console.
- **Codex** — autonomous executor in sandboxes.

Formalize as **participant roles with capability profiles**; the spine *routes* a task to a role by
task type (design → ChatGPT, implement → Codex, spec/steer → Kiro). Routing is spine policy, not
hard-coded.

## 6. Rust-driven — honestly scoped

Rust owns everything that owns durability and control: the orchestration spine (tokeira), the
provisioning + CLI-agent workers (Codex/Kiro drivers on `sdk-core`), and the cockpit backend. The
OpenAI Agents SDK / Pydantic AI harness is Python and stays Python — it is an activity-side concern
bridged over tokeira's wire. `loom-narrator` already proves Python-on-Temporal works against a
DSQL-backed cluster. "Rust-driven" holds for the spine; the model loop is whatever SDK does it best.

## 7. The concern buckets, resolved

| Your concern | Resolution |
|---|---|
| Review / intervention steps | First-class: tool approvals (harness) + workflow gates (spine), via signals/updates; cockpit surfaces and drives them. |
| UI for all workflows | The cockpit — purpose-built workflow-progress + intervention surface; not Loom. |
| Agent usage cost | Metered per model/agent activity; budget on the workflow; over-budget → gate. |
| Platform cost | Idle-timeout auto-suspend + scale-to-zero + Graviton + ephemeral sandboxes; idle reaper workflow. |
| Isolation | Provider sandboxes (process/fs/network) + Manifest Unix permissions; credentials/orchestration outside the code-exec env. |
| Resource allocation by profile | Workflow declares a profile (cpu/mem/gpu/timeout/egress); scheduler places on matching capacity (Karpenter/EKS, capacity providers/ECS). |
| Durable workflow memory | History (authority) + DSQL project memory + S3 artifacts; replayable. |
| Agent SDK support | OpenAI Agents SDK + Pydantic AI via the Temporal plugin bridge against `tokeirad`. |

## 8. First slice (walking skeleton)

> One workflow that runs **one Codex/OpenAI sandbox task** in **one sandboxed worktree**, blocks on
> **one human approval gate**, **meters cost**, and renders as **one node in a minimal cockpit view**
> — persisted in DSQL, survivable across a `tokeirad` restart.

Concretely: stand up OpenAI's sandbox coding-agent demo (`AgentWorkflow` + `SessionManagerWorkflow`
+ TUI) against `tokeirad` with the local Docker sandbox backend. If it survives killing the worker
mid-turn and resuming, every later capability (fan-out, routing, budgets, microVMs, the full
cockpit) is additive rather than architectural.

The gap trace (`reference/openai-sandbox-gap.md`) is the bridge: it lists what tokeira must support
to host the demo, mapped to the conformance work already underway.

## 9. Open decisions

1. **Name** — Conductor? something else? ⚠️ "Conductor" is taken in this space (conductor.build, a
   macOS multi-agent orchestrator — noted in `reference/cockpit.md` prior art); pick a different
   spine name. The cockpit is named **Tokeira Kairo**.
2. **Roles** — is the ChatGPT/Kiro/Codex division above right, or do you want a dedicated reviewer
   role / Kiro-as-coordinator vs Kiro-as-executor?
3. **Sandbox provider for the skeleton** — Docker-local first (no cloud keys), then which remote
   (E2B / Daytona / Modal / Bedrock AgentCore)?
4. **Cockpit form factor** — ✅ resolved (see `reference/cockpit.md`): the cockpit is **Tokeira Kairo**,
   a **standalone GPUI app** (not a Zed fork, not ACP) integrated with a supervised `tokeirad`; TUI
   for the walking skeleton first; git worktrees now with DeltaDB behind a workspace/memory seam
   later; Figma Make as the design source; phased licensing (permissive Phase 1 → optional GPL-derived
   Phase 2).
5. **Project-memory schema** — minimal DSQL key/value keyed by spec, deferring retrieval?

## 10. References

- OpenAI — [The next evolution of the Agents SDK](https://openai.com/index/the-next-evolution-of-the-agents-sdk/)
- Temporal — [Introducing Temporal and agentic sandboxes: the OpenAI Agents SDK](https://temporal.io/blog/introducing-temporal-and-agentic-sandboxes-openai-agents-sdk)
- Temporal — [Sandbox Orchestration Harness: the missing layer for running agents](https://temporal.io/blog/temporal-sandbox-orchestration-harness-the-missing-layer-for-running-agents)
- Demo source — `openai/openai-agents-python` → `examples/sandbox/extensions/temporal`
