# Gap Trace: hosting the OpenAI sandbox agent demo on `tokeirad` (posture A)

> Purpose: enumerate what tokeira must support to run OpenAI's sandbox coding-agent demo
> (`AgentWorkflow` + `SessionManagerWorkflow` + TUI, via the Temporal plugin) **unmodified** against
> `tokeirad`, mapped to tokeira's current conformance status. This is the bridge between the
> agentic-orchestration north star and the functional-conformance effort.
>
> **Tier 1 + Tier 2 (this doc, updated 2026-06-11):** Tier-2 now **resolved** by reading the actual
> demo source at `openai-agents-python/examples/sandbox/extensions/temporal/` (`temporal_sandbox_agent.py`,
> `temporal_session_manager.py`, `local_hello_workflow.py`). Findings folded in below; the ⛏️
> section records what each construct turned out to be.
>
> Status sources: `.kiro/specs/api-conformance-tracker/reference/temporal_api_audit.md`,
> `.kiro/specs/api-conformance-tracker/tracker.md`, and the C3 refresh in
> `temporal-functional-conformance/reference/FINDINGS.md`.

## Demo → capability map (from the public description)

| Demo behaviour | tokeira-facing capability |
|---|---|
| Long-lived `AgentWorkflow` with `workflow.wait_condition` idle loop | Start + workflow-task machinery; zero-cost blocking |
| Agent turn = LLM calls + sandbox exec/read/write as **activities** | Activity task machinery + retries |
| TUI sends user messages | **Signals** |
| TUI polls turn state in real time; `query_workflow_snapshot` activity | **Queries** |
| Fork / rename (transactional session ops) | **Updates** (`UpdateWorkflowExecution` + `PollWorkflowExecutionUpdate`) |
| Fork starts a new session that outlives the manager turn | **Child workflow** + `ParentClosePolicy.ABANDON` |
| `pause_workflow` activity | Signal-driven pause flag in workflow state (demo uses `self._pause_requested`) — **not** the server pause API |
| Session start | Start / possibly `SignalWithStart` |
| SDK handshake / feature gating | `GetSystemInfo` + `DescribeNamespace` capability advertisement |
| Idle-suspend, snapshot, fork of the *sandbox* | Provider-native (Modal/E2B/Daytona/Docker) — **outside** tokeira's RPC surface |
| Production cockpit listing all sessions | `ListWorkflowExecutions` (visibility) — demo itself lists via workflow state |

## Gap table (Tier 1)

Risk = likelihood this blocks the unmodified demo on posture (A).

| # | Capability | tokeira status | Gap / risk | Risk | Conformance owner |
|---|---|---|---|---|---|
| 1 | StartWorkflowExecution | ✅ **Verified adequate** | Handler → `start_request_to_edge` → runtime carries type/id/task-queue/input/request-id. Demo uses basic start only; gaps (callbacks/links/versioning/reuse/delay/cron/memo) are off-path | Low | `api-conformance-start-fields` |
| 2 | Workflow-task loop (Poll/RespondWFTCompleted), `wait_condition` blocking | ✅ **Verified present** | Handlers present; exercised by the functional-conformance corpus. Long-idle blocking + replay-resume is core durable-execution behaviour the corpus covers | Low | core / `api-conformance-wft-completion` |
| 3 | Activity loop (Poll/Respond Completed/Failed, heartbeat) | ✅ **Verified present** | Poll/Respond/heartbeat handlers all present. Demo uses `start_to_close` only (no heartbeats/local activities) | Low | `activity-*` specs |
| 4 | SignalWorkflowExecution | ✅ **Verified adequate** | Handler present. Demo signals carry plain-string messages; header/link gap is off-path | Low | `api-conformance-signal-headers` |
| 5 | **QueryWorkflow** | ✅ **Verified adequate** | Real dispatch: query tasks published to the polling worker, pending-query tracking by task-token+query-id, both modern (`query_results`) and legacy (`RespondQueryTaskCompleted`) transports, consistency-barrier buffering (`drain_satisfied`). Covers continuous `get_turn_state` polling on an idle workflow + the snapshot query | Low (was Med) | existing handler (conformance) |
| 6 | **UpdateWorkflowExecution** + PollWorkflowExecutionUpdate | ✅ **Verified + bug found & fixed via live run.** `update_ref`/`stage`/`outcome` populated and the waiter lifecycle is correct. The live demo exposed a **delivery** defect the unit tests missed: the runtime re-offered an *already-accepted* update on the next WFT (`UpdateRegistry::drain_pending_updates` had no sent/accept state), causing the worker to re-accept → kernel `DuplicateUpdateId`. **Fixed** in `tokeira-runtime/src/update.rs`: registry entry gains an `accepted` flag (`notify_accepted` sets it), `drain_pending_updates` skips accepted entries — mirroring v1.31.0's `needToSend` leaving the sendable set on acceptance (`update.go:404-540 @ v1.31.0`). Regression test `accepted_update_is_not_redelivered_as_pending_transport`; 278 runtime tests green | Low | `api-conformance-update-lifecycle` |
| 7 | Child workflow + **`ParentClosePolicy.ABANDON`** | ✅ **Verified.** Kernel `ParentClosePolicy{Terminate,RequestCancel,Abandon}`; `apply_parent_close_policy` (`kernel.rs:3397`) does `Abandon => {}` ("children left alone"), invoked on all parent-close paths; runtime `handle_start_child_workflow` starts the child; edge round-trips the policy. Kernel + 277 runtime tests green at `HEAD` | Low (was High) | (no gap to file) |
| 7b | **SignalExternalWorkflowExecution** | ✅ **Verified.** Kernel command + `SignalExternalWorkflowExecutionInitiated/Signaled/Failed` history + `DispatchOp::SignalExternalWorkflow`; runtime publisher `handle_signal_external_workflow` delivers it; kernel `apply_external_signal_resolved` applies the result. Covers `destroy_session` | Low (was Med) | tracker |
| 10 | ExecuteMultiOperation / update-with-start | **Stubbed (UNIMPLEMENTED)** | **Resolved: NOT used.** `create_session` uses `start_child_workflow`, not update-with-start; no multi-op anywhere in the demo. Risk removed for posture (A). | None | n/a |
| 8 | Capability advertisement (`workflow_update`) | ✅ **Resolved — non-issue.** `GetSystemInfoResponse.Capabilities` (API v1.62.11, `request_response.proto:1288`) has **no `workflow_update` field** (fields 1–12 only). The SDK does not gate updates on a GetSystemInfo flag; it calls `UpdateWorkflowExecution` directly, which tokeira implements (#6) with no feature-disabled rejection. My earlier premise was wrong | None (was High) | n/a |
| 9 | SignalWithStartWorkflowExecution | Partial | Used if sessions start via signal-with-start; inherits start+signal gaps | Med | `api-conformance-start-fields` |
| 10 | ExecuteMultiOperation | **Stubbed (UNIMPLEMENTED)** | Risk *only if* the harness uses update-with-start / atomic multi-op. ⛏️ confirm | Med (conditional) | (deferred) |
| 11 | DescribeWorkflowExecution | Partial | Cockpit + SDK introspection; confirm fields the SDK reads | Low–Med | `api-conformance-workflow-describe` |
| 12 | ListWorkflowExecutions (visibility) | Partial (projection; C3 largely done) | Demo lists via workflow state, so **not** on the skeleton's critical path; production cockpit needs it | Low (skeleton) / Med (cockpit) | C3 / `api-conformance-visibility-legacy` |
| 13 | RequestCancel / Terminate | Partial | `terminate`/`revert` operator verbs; not on the happy-path skeleton | Low | tracker |

## Critical path for the walking skeleton — ✅ ALL VERIFIED (2026-06-11, worktree off `fa0b085`)

Every critical-path primitive the OpenAI sandbox demo needs is **already implemented and verified** in
tokeira at `HEAD` — for **both** the leaner `AgentWorkflow`-only skeleton *and* the full
`SessionManagerWorkflow` (create/fork/switch/destroy):

| Primitive | Verdict |
|---|---|
| #1 Start / #2 WFT loop / #3 Activity loop / #4 Signal / #5 Query | ✅ verified adequate (handlers present + corpus-exercised) |
| #6 UpdateWorkflowExecution (incl. blocking multi-WFT updates) | ✅ verified — `update_ref`/`stage`/`outcome` populated; `Completed` waiter survives acceptance; 18 update tests pass |
| #7 Child workflow + `ParentClosePolicy.ABANDON` | ✅ verified — `Abandon => {}`; child machinery + 277 runtime tests pass |
| #7b SignalExternalWorkflowExecution | ✅ verified — full command→dispatch→deliver→resolve path |
| #8 `workflow_update` capability gate | ✅ resolved — no such proto field exists; SDK doesn't gate; non-issue |

**No corrective code work is required on the tokeira side to host the demo.** The remaining gap is
purely integration: stand up `tokeirad` + the OpenAI plugin worker + run it (and confirm a usable
default namespace). Verification was by code-read + targeted unit/property tests at `HEAD`, not yet a
live end-to-end `tokeirad` + Python-SDK run — that run is the actual walking-skeleton milestone.

### Off-path rows (not required for the demo)

- **#9 SignalWithStart** — not used: `create_session` uses `start_child_workflow`; messages are
  separate signals after start. Partial status is fine.
- **#11 DescribeWorkflowExecution** — introspection/cockpit; Partial is adequate, not on the turn loop.
- **#12 ListWorkflowExecutions** — cockpit-tier; the demo lists sessions from manager workflow state,
  not visibility. Covered by the C3 work for the production cockpit.
- **#13 RequestCancel / Terminate** — operator verbs (terminate/revert); off the happy path.

## A leaner skeleton that de-risks early — **confirmed viable**

`AgentWorkflow` runs standalone without `SessionManagerWorkflow`: a client can
`start_workflow(AgentWorkflow.run, …)`, drive turns with the `send_message` **signal**, poll
`get_turn_state` via **query**, and end with the `destroy` **signal**. That path needs only #1–#5
(start, WFT loop, activities, signal, query) — **no updates, no child workflows, no ABANDON**. So the
first skeleton can model the human approval gate as a **signal** (`approve`/`reject`) on a single
long-lived `AgentWorkflow`, deferring the two High-risk items (#6 Update lifecycle, #7 ABANDON) to
the second iteration when the SessionManager's create/fork/switch surface arrives.

Trade-off: no fork / rename / backend-switch / multi-session registry until phase 2 — those are
exactly the features that require updates + child workflows.

> Note: the repo's `local_hello_workflow.py` is **not** a tokeira host target — it uses the SDK's
> `WorkflowEnvironment.start_time_skipping()` in-process test server, not a real frontend. The
> tokeira-hosted target is the full worker + TUI pointed at `Client.connect("localhost:7233")`
> (i.e. `tokeirad` in place of `temporal server start-dev`).

## Tier-2 questions — resolved from source

Read from `examples/sandbox/extensions/temporal/{temporal_sandbox_agent.py, temporal_session_manager.py, local_hello_workflow.py}`:

1. **Fork parent-close policy** → `ParentClosePolicy.ABANDON`, used by both `create_session` and
   `fork_session`. Confirmed (#7).
2. **Update-with-start / multi-op** → **not used.** `create_session` uses `start_child_workflow`.
   Gap #10 removed.
3. **Update client requirements** → clients use `execute_update` and **consume the return value**
   (`create_session`/`fork_session` return the new `workflow_id`); `pause`/`switch_backend` are
   awaited updates. So update *outcome delivery* is mandatory; `update_ref`/`stage` population is
   the suspected gap (#6).
4. **Standalone `AgentWorkflow`** → yes (see leaner skeleton). The SessionManager is only needed for
   create/fork/list/rename/switch.
5. **Activity specifics** → the demo's own activities (`pause_workflow`, `query_workflow_snapshot`,
   `switch_workflow_backend`) use `start_to_close_timeout` only — **no heartbeats, no local
   activities.** Model + sandbox ops run as plugin-managed activities.
6. **Continue-as-new** → **not used.** `AgentWorkflow` loops forever via `wait_condition`; history
   grows unbounded. Not a tokeira gap for the demo, but a real long-run history-size caveat to note.
7. **RegisterNamespace** → not called; the demo assumes the default namespace exists. `tokeirad`
   must serve a usable default namespace out of the box.
8. **Capabilities checked** → updates flow through the SDK's `OpenAIAgentsPlugin`; the
   `workflow_update` capability gate (#8) still must be verified against tokeira's `GetSystemInfo`.

Other observed surface: client-side `execute_update`/`query` are invoked **from inside activities**
(via a fresh `Client.connect`) against other workflows — i.e. ordinary client `UpdateWorkflowExecution`
/ `QueryWorkflow` RPCs, plus `SignalExternalWorkflowExecution` from a workflow (`destroy_session`).
A custom `pydantic_data_converter` is used — irrelevant to the server (payloads are opaque bytes).

## Next actions

- [x] #6 UpdateWorkflowExecution — verified (blocking multi-WFT updates; 18 update tests pass).
- [x] #7 child `ParentClosePolicy.ABANDON` — verified (`Abandon => {}`; runtime tests pass).
- [x] #7b SignalExternalWorkflowExecution — verified (full deliver/resolve path).
- [x] #8 `workflow_update` capability gate — resolved (no such proto field; non-issue).
- [x] `tokeirad` serves a usable **default namespace** without `RegisterNamespace` — verified:
      `ResolvedNamespace::active("default")` is inserted into the namespace cache at startup
      (`apps/tokeirad/src/lib.rs:533`), before serving.
- [ ] **Walking-skeleton milestone (live):** boot `tokeirad`, run the OpenAI plugin worker
      (`temporal_sandbox_agent.py worker`, local Docker backend) + a client, drive a turn end-to-end,
      kill+resume to prove durability. This is the real proof; the verifications above remove all the
      a-priori risk.
- [ ] Carve the walking-skeleton spec (requirements/design/tasks) from NORTH-STAR §8.

## Verification provenance

All `HEAD`-state verification was done in a throwaway worktree at `.worktrees/gap-verify`
(branch `verify/gap6`, based on `fa0b085`) to avoid touching the uncommitted C3 WIP in the main
tree. Cold build ~1m; `cargo test -p tokeira-runtime --lib update` (18 pass) and
`cargo test -p tokeira-kernel -p tokeira-runtime --lib` (kernel + 277 runtime pass). Tear down with
`git worktree remove .worktrees/gap-verify && git branch -D verify/gap6`.
