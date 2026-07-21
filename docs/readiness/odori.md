# Odori — Delivery Readiness

> Sibling of [`delivery.md`](./delivery.md). Status of the Odori agentic-workflow workstream.
> The engine-dependency facts below are Kiro-verified; the Odori build plan is owner-maintained
> (lives in the `tokeira-odori` repo's specs — this page is the readiness summary, not the spec).

## What Odori is

The agentic coding workflow hosted on tokeira as a durable execution spine, conducting Codex/Claude
Code executors from a Kiro MCP cockpit, with the OpenAI Agents SDK reasoning harness running unmodified
inside a Firecracker guest. tokeira is the spine; Odori is the downstream consumer (see the
`tokeira-odori` AGENTS.md / `agentic-conductor` spec).

## Engine dependencies — status (Kiro-verified)

The Nexus dependency chain Odori's 2.4 build needs from the tokeira engine:

| Dependency | Status | Evidence |
|------------|:------:|----------|
| `CreateNexusEndpoint` (worker-targeted, per-session) | ✅ Verified | C4a complete; 13 edge tests; runtime `CreateNexusEndpoint` resolves for dispatch. |
| Worker-targeted dispatch → external poller | ✅ Verified | Odori `nexus-roundtrip-probe`, 2026-06-22. |
| **Synchronous** Nexus round-trip (poll → respond → route back, cross-namespace) | ✅ Verified | Same probe + Python handler verify; result routes back as workflow output. |
| Cross-language payloads (Rust ↔ Python via nexus-rpc serializer) | ✅ Verified | Python handler verify, 2026-06-22. |
| **Asynchronous** completion (`WorkflowRunOperation` / `AgentWorkflow`) | ⏳ In progress | `nexus-async-completion` spec; `DispatchCompletionCallback` is a no-op stub today. Wave 0 landed; Wave 1 handed to Claude (`docs/HANDOVER-nexus-async-completion.md`, retired to git history). |

**Implication for 2.4:** the synchronous handler path works on `main` today; the durable async
`WorkflowRunOperation` path is blocked on `nexus-async-completion`. Odori can stage 2.4 on a sync stub
and swap to async when that lands.

## Odori build readiness

<!-- OWNER (Odori / Claude): summarize the 2.4 build state, the platforms/odori fleet, the runner
daemons, the MCP cockpit, and acceptance criteria. The detailed plan lives in tokeira-odori specs;
this section is the readiness rollup that gates a tokeira release. -->

_Owner-maintained — to be filled from the `tokeira-odori` `agentic-conductor` spec._
