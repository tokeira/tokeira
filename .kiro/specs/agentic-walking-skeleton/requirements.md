# Requirements Document

## Introduction

The **Agentic Walking Skeleton** is the first concrete, implementable slice of the
agentic-orchestration north star (`.kiro/specs/agentic-orchestration/NORTH-STAR.md` §8).
It is deliberately narrow: one workflow that runs one OpenAI sandbox coding-agent task in one
sandboxed worktree, blocks on one human approval gate, meters cost, and renders as one node in a
minimal cockpit — persisted durably and survivable across a `tokeirad` worker restart.

The strategic posture is **(A) Conform-and-host** (NORTH-STAR §2): the EXISTING OpenAI/Temporal
sandbox harness (`AgentWorkflow` + the Temporal plugin worker + TUI client, from
`openai-agents-python/examples/sandbox/extensions/temporal/`) runs against `tokeirad` **unmodified**.
`tokeira` is the durable orchestration spine; the agent harness (OpenAI Agents SDK, Python) and the
sandbox runtime (local Docker) are adopted, not rebuilt. This spec covers the **integration, spine
configuration, and minimal cockpit** required to host the demo and prove durability — it does NOT
reimplement Temporal RPC primitives.

The gap ledger (`.kiro/specs/agentic-orchestration/reference/openai-sandbox-gap.md`) has already
verified that every critical-path RPC primitive the demo needs is implemented at tokeira `HEAD`
(Start, the workflow-task loop with `wait_condition` blocking, the activity loop, Signals, Queries,
Updates, child workflows with `ParentClosePolicy.ABANDON`, and `SignalExternalWorkflowExecution`),
and that `tokeirad` serves a usable `default` namespace without `RegisterNamespace`. The leaner
standalone `AgentWorkflow` path (start → `send_message` signal → `get_turn_state` query → `destroy`
signal, modelling the human gate as a signal) is the skeleton; the full `SessionManagerWorkflow`
fork/rename/switch surface is out of scope.

The ultimate acceptance is an **operator-run live demo** (boot, drive a turn, kill the worker
mid-turn, resume) — not a unit test. Requirements below distinguish automated verification from
operator-run acceptance where the distinction matters.

## Glossary

- **Tokeirad**: The tokeira server binary (`apps/tokeirad`) exposing the Temporal-compatible gRPC
  frontend on `localhost:7233`, acting as the durable orchestration spine.
- **Agent_Harness**: The unmodified OpenAI Agents SDK Temporal plugin worker
  (`temporal_sandbox_agent.py worker`) that hosts `AgentWorkflow` and its activities, connected to
  Tokeirad via `Client.connect("localhost:7233")`.
- **Agent_Workflow**: The long-lived `AgentWorkflow` execution — a durable idle loop driven by
  signals, polled by queries, that runs one agent turn at a time. The standalone (non-SessionManager)
  path per the gap ledger.
- **Sandbox_Runtime**: The local Docker sandbox backend that the Agent_Harness uses to execute
  shell/file operations for an agent turn, isolated to one worktree.
- **Cockpit**: The minimal operator-facing read/intervene surface for the skeleton. TUI form factor
  (the demo ships a TUI). Reads workflow state via queries/visibility; writes interventions via
  signals/updates. NOT Loom.
- **Operator**: The human running and intervening in the skeleton via the Cockpit.
- **Approval_Gate**: A workflow-level block in Agent_Workflow that suspends progress until the
  Operator approves or rejects, modelled as a signal (`approve`/`reject`) on the standalone path.
- **Cost_Meter**: Workflow state (plus search attributes) on Agent_Workflow that accumulates the
  metered cost of model/sandbox activities and exposes the running total and configured budget.
- **Budget**: The Operator-configured cost ceiling for an Agent_Workflow execution.
- **Persistence_Store**: The durable backing store for workflow history. In-memory store is the
  development default; Aurora DSQL is the persistence target.
- **Turn**: One agent turn — the model calls plus sandbox exec/read/write activities triggered by a
  single user message, ending when Agent_Workflow returns to its idle wait.
- **Broker_Query_Fix**: The query-to-quiescent-workflow delivery fix owned by the external
  `runtime-broker-tiered-delivery` spec, on which live `get_turn_state` polling depends.

## Requirements

### Requirement 1: Host the unmodified OpenAI sandbox demo on Tokeirad

**User Story:** As an Operator, I want to run the unmodified OpenAI sandbox coding-agent harness
against Tokeirad with the local Docker backend, so that tokeira is proven as a drop-in durable spine
for the standard ecosystem harness.

#### Acceptance Criteria

1. THE Agent_Harness SHALL connect to Tokeirad at `localhost:7233` using the standard
   `Client.connect` path without any source modification to the demo.
2. THE Agent_Harness SHALL use the local Docker Sandbox_Runtime backend for sandbox operations.
3. WHEN the Operator starts the Agent_Harness worker against a running Tokeirad, THE Agent_Harness
   SHALL register and poll the `AgentWorkflow` workflow type and its activities on the configured
   task queue.
4. IF hosting the demo requires any code change to the OpenAI demo sources under
   `examples/sandbox/extensions/temporal/`, THEN THE skeleton SHALL record the change as a
   conformance defect against tokeira rather than carry a demo fork.
5. THE skeleton SHALL target the standalone `Agent_Workflow` path (start, `send_message` signal,
   `get_turn_state` query, `destroy` signal) and SHALL NOT require the `SessionManagerWorkflow`
   create/fork/switch/rename surface.

### Requirement 2: Usable default namespace out of the box

**User Story:** As an Operator, I want Tokeirad to serve a usable default namespace without manual
registration, so that the demo (which assumes the default namespace exists) starts without extra
setup.

#### Acceptance Criteria

1. WHEN Tokeirad starts, THE Tokeirad SHALL serve an active `default` namespace before accepting
   client connections.
2. WHEN the Agent_Harness connects without calling `RegisterNamespace`, THE Tokeirad SHALL accept
   workflow Start, Signal, and Query requests against the `default` namespace.

### Requirement 3: Drive one agent turn end-to-end

**User Story:** As an Operator, I want to drive a single agent turn from the Cockpit through to
completion, so that the full integration loop (model loop + sandbox activities + signals + queries)
is exercised against tokeira.

#### Acceptance Criteria

1. WHEN the Operator starts an Agent_Workflow execution, THE Tokeirad SHALL durably record the start
   and the Agent_Workflow SHALL enter its idle wait state.
2. WHEN the Operator sends a user message via the `send_message` signal, THE Agent_Workflow SHALL
   execute one Turn, dispatching model and sandbox operations as activities.
3. WHILE a Turn is in progress, THE Cockpit SHALL be able to read the current turn state via the
   `get_turn_state` query.
4. WHEN a Turn completes, THE Agent_Workflow SHALL return to its idle wait state and THE
   `get_turn_state` query SHALL report the completed turn result.
5. WHEN the Operator sends the `destroy` signal, THE Agent_Workflow SHALL terminate its run.

### Requirement 4: Durability across worker restart (operator-run acceptance)

**User Story:** As an Operator, I want the agent workflow to survive killing the Agent_Harness worker
mid-turn and resuming it, so that durable execution on tokeira is demonstrably real.

#### Acceptance Criteria

1. THE Tokeirad SHALL treat persisted workflow history as the authority for Agent_Workflow state.
2. WHEN the Agent_Harness worker is killed while a Turn is in progress, THE Tokeirad SHALL retain
   the Agent_Workflow execution and its history.
3. WHEN a replacement Agent_Harness worker resumes polling the task queue, THE Agent_Workflow SHALL
   continue the in-progress Turn from history without restarting completed activities.
4. WHEN the resumed Turn completes, THE `get_turn_state` query SHALL report a result consistent with
   a Turn that ran without interruption.
5. THE skeleton SHALL document the kill-and-resume procedure as the operator-run acceptance step,
   distinct from automated integration verification.

### Requirement 5: One human approval gate

**User Story:** As an Operator, I want the agent workflow to block on a single human approval gate,
so that I can approve or reject the agent's proposed action before it proceeds.

#### Acceptance Criteria

1. WHEN the Agent_Workflow reaches the Approval_Gate, THE Agent_Workflow SHALL suspend Turn progress
   until the Operator responds.
2. WHILE the Agent_Workflow is suspended at the Approval_Gate, THE Cockpit SHALL surface the gate as
   a pending decision via the `get_turn_state` query.
3. WHEN the Operator sends the `approve` signal, THE Agent_Workflow SHALL resume the suspended Turn.
4. WHEN the Operator sends the `reject` signal, THE Agent_Workflow SHALL abandon the gated action and
   return to its idle wait state.
5. THE Approval_Gate SHALL be implemented as a workflow-level signal on the standalone
   Agent_Workflow path, mapping to the Operator verbs approve and reject.

### Requirement 6: Cost metering and budget gate

**User Story:** As an Operator, I want the agent workflow to meter its cost against a budget, so that
agent spend is steerable rather than a surprise.

#### Acceptance Criteria

1. WHEN a model or sandbox activity completes, THE Cost_Meter SHALL increase the Agent_Workflow's
   running cost total by the metered cost of that activity.
2. THE Cost_Meter SHALL expose the running cost total and the configured Budget via the
   `get_turn_state` query.
3. THE Agent_Workflow SHALL publish the running cost total as a search attribute for visibility
   queries.
4. IF the running cost total reaches or exceeds the Budget, THEN THE Agent_Workflow SHALL suspend at
   the Approval_Gate before dispatching further billable activities.
5. WHEN the Operator approves continued spend after a budget-triggered gate, THE Agent_Workflow SHALL
   resume Turn progress.

### Requirement 7: Minimal cockpit view

**User Story:** As an Operator, I want a minimal cockpit that renders the agent workflow as one node
and lets me intervene, so that I can observe advancement and drive decisions from a single surface.

#### Acceptance Criteria

1. THE Cockpit SHALL render the Agent_Workflow execution as one node showing its current state
   (idle, running a Turn, or suspended at the Approval_Gate).
2. THE Cockpit SHALL read Agent_Workflow state via queries and visibility, not by holding workflow
   logic.
3. THE Cockpit SHALL display the running cost total and the Budget for the rendered node.
4. THE Cockpit SHALL send Operator interventions (`send_message`, `approve`, `reject`, `destroy`) to
   the Agent_Workflow via signals.
5. THE Cockpit SHALL use the TUI form factor for the skeleton.

### Requirement 8: Persistence target

**User Story:** As an Operator, I want the skeleton to run against the in-memory store for
development and against Aurora DSQL as the persistence target, so that durability is proven on the
production-intended backend.

#### Acceptance Criteria

1. WHERE the in-memory Persistence_Store is configured, THE Tokeirad SHALL host the skeleton for
   development without external storage dependencies.
2. WHERE the Aurora DSQL Persistence_Store is configured, THE Tokeirad SHALL persist Agent_Workflow
   history durably across worker restarts.
3. THE skeleton SHALL document which Persistence_Store backs each verification step, distinguishing
   the development run from the DSQL durability proof.

### Requirement 9: External dependency on broker query delivery

**User Story:** As an Operator, I want live turn-state polling to work against an idle agent
workflow, so that the Cockpit can read state continuously without timing out.

#### Acceptance Criteria

1. THE skeleton SHALL treat the Broker_Query_Fix (owned by the `runtime-broker-tiered-delivery`
   spec) as an external prerequisite for live `get_turn_state` polling.
2. WHEN the Agent_Workflow is quiescent and the Broker_Query_Fix has landed, THE Tokeirad SHALL
   deliver `get_turn_state` queries to a polling Agent_Harness worker without timing out.
3. THE skeleton SHALL NOT own or reimplement the query-delivery fix.

### Requirement 10: Operator run procedure

**User Story:** As an Operator, I want a documented run procedure for the skeleton, so that the live
demo is reproducible by anyone.

#### Acceptance Criteria

1. THE skeleton SHALL document the ordered procedure to boot Tokeirad, start the Agent_Harness worker
   with the local Docker backend, and launch the Cockpit.
2. THE run procedure SHALL state the prerequisites required before the live demo (Docker available,
   the Broker_Query_Fix landed, and the configured Persistence_Store).
3. THE run procedure SHALL describe the kill-and-resume durability step and the expected observable
   outcome.
