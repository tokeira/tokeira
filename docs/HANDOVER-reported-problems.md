# Handover — Tier 3.22 `TemporalReportedProblems` (forensic code examination)

**To:** the investigating agent (Claude Code)
**From:** Kiro

**Output expected — read this first.** The deliverable is a **written report that identifies and
summarises the issue(s)** — a forensic discovery write-up (see §8), nothing more. **Make no code
changes.** Do **not** edit source, do **not** add temporary instrumentation or log lines, and do
**not** run the functional test suite (or anything that boots `tokeirad`). This is a
diagnosis-and-report task; remediation is a separate, later step owned by someone else. If the cause is
clear, the report **may** describe a recommended fix direction in prose — but it changes no code.

**Mode:** forensic **code** examination — identify the issue(s) by (a) verifying behaviour against
Temporal **v1.31.0** ground truth (AGENTS §8) and (b) reading the **actual** tokeira code paths, not by
conjecture.

**Scope guards.** `tokeira-kernel` is out of bounds (kernel purity), and the conformance override
mechanism is **proven working** and out of scope (see §0 for what it is and where it lives). The
honesty boundary is load-bearing: any fix the report *recommends* must make the real counter
advance/clear correctly — never a hardcoded SA or a test special-case.

---

## 0. Background — the override-config machinery this sits on (introduced by this work)

This investigation sits **on top of** the `conformance-config-override` spec, which this body of work
introduced so the Temporal corpus can deliver `OverrideDynamicConfig(setting, value)` to an
**out-of-process** `tokeirad`. That machinery is the delivery vehicle for the threshold Tier 3.22 sets;
it is **proven working here** (leaf 2 passes; leaf 4's threshold-0 phase correctly *suppresses* the SA)
and is **not** where the defect lives. Orientation for a cold reader:

- **Spec:** `.kiro/specs/conformance-config-override/` (`requirements.md` incl. the Requirement 0
  sanction, `design.md`, `tasks.md`). The reported-problems threshold
  (`system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute`) is the **proving `Wired`
  consumer** (Task 13.3, pulled forward as the Tier 3.22 takeover).
- **Registry** (`crates/tokeira-conformance`): a process-global `key -> typed value` store with a
  `KEY_CLASSIFICATION` honesty table; consult sites read it **live**. Present only under the
  `conformance` feature.
- **Proto + control service** (`crates/tokeira-conformance-proto` →
  `proto/tokeira/conformance/v1/control.proto`; `crates/tokeira-conformance-control`): a Connect-RPC
  (`connectrpc`/`buffa`, pinned `0.8.1`) `Set/Clear/Reset` service that writes the registry. Non-`Wired`
  keys are rejected (`unimplemented`/`invalid_argument`), never faked.
- **Feature + mount:** a per-crate `conformance` Cargo feature (`tokeira-runtime`, `tokeirad`); a
  feature-only, **separate-loopback** control listener in `tokeirad` (never the public gRPC router),
  gated on `TOKEIRA_CONFORMANCE_CONTROL_ADDR`. This **replaced** the earlier boot-time
  `TOKEIRA_CONFORMANCE_REPORTED_PROBLEMS_THRESHOLD` env var (now removed).
- **Fork bridge** (`../temporal/tests/testcore/tokeira_dynamic_config_bridge.go`): translates the
  corpus's `OverrideDynamicConfig` into control-service RPCs; on rejection it falls back to the skip
  registry (never a silent no-op). Leaf 4 is **un-skipped** in scope.

**Tree state:** this is **uncommitted** work on tokeira `main`, landing alongside the reported-problems
base (`crates/tokeira-compatibility/src/matrix.rs`, `crates/tokeira-edge/src/workflow_service.rs`,
`crates/tokeira-runtime/src/runtime/{workflow_task.rs,mod.rs}`, `apps/tokeirad/src/lib.rs`). Division of
labour that matters for this hunt: the reported-problems **consumer** (the `WorkflowTaskProblemTracker`
+ the Describe-side derive) is the base being taken over; the override **delivery** machinery above is
this work's. **The defect is in the consumer, not the delivery.**

---

## 1. The observable (how it was produced, for reference only)

Tier 3.22 = `TestWFTFailureReportedProblemsTestSuite` in the pinned fork
(`../temporal/tests/workflow_task_reported_problems_test.go`), run over the Shape-2 bridge against an
out-of-process `tokeirad` built `--features conformance`. Latest run:

**1 PASS / 4 FAIL / 0 SKIP** (`go test` exit 1)

| Leaf | Result | Mechanism it exercises |
|------|--------|------------------------|
| `_NotClearedBySignals` (2) | ✅ PASS | self-signal → each failure is a **fresh attempt-1** WFT |
| `_SetAndClear` (1) | ❌ FAIL (20s) | plain panic → **transient attempt>1** retries |
| `_SetAndClear_FailAfterActivity` (3) | ❌ FAIL (20s) | activity succeeds, then panic → **sticky→non-sticky** transition + transient retries |
| `_DynamicConfigChanges` (4) | ❌ FAIL (15s) | threshold 0→2 mid-run; transient retries |

Repro (reference only — the task is code reading, not running this):
`bash run_suite.sh '^TestWFTFailureReportedProblemsTestSuite$' 8m` from `../temporal`
(`TOKEIRA_BIN` defaults to `../tokeira/target/debug/tokeirad`; the harness exports
`TOKEIRA_CONFORMANCE_CONTROL_ADDR` so a feature build binds the control listener).

---

## 2. The test contract (what tokeira must produce)

For a workflow whose WFT panicked (`cause = WorkflowWorkerUnhandledFailure`), every leaf asserts the
`TemporalReportedProblems` search attribute is a **KeywordList of exactly two elements**:

```
["category=WorkflowTaskFailed", "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure"]
```

Per-leaf polling (`EventuallyWithT`, 500ms tick):

- **Leaf 1** (threshold 2): SA present with those 2 entries **AND**
  `DescribeWorkflowExecution.PendingWorkflowTask.Attempt >= 2`. First failing assertion is
  `require.True(t, ok)` on SA presence → "Should be true". After unblock: workflow completes, SA
  **absent** (cleared on success).
- **Leaf 3**: same SA assertion (`Len == 2`); the workflow runs an activity **before** the panic,
  deliberately driving the server's sticky-queue-clear-and-retry path.
- **Leaf 4**: with threshold **0**, asserts SA **absent** *and* `PendingWorkflowTask.Attempt >= 2` —
  **this Eventually passes**. Then threshold **2**, asserts SA **present** (2 entries) — **this
  Eventually fails** at the SA-presence check (`test.go:264`, inner assert `:268`).

`SetupTest` sets the threshold to 2 for all leaves; leaf 4 overrides it 2→0→2 within the run.

---

## 3. What is ALREADY VERIFIED (grounded — do not re-derive)

These are established from the run evidence + direct code reading. Treat as settled starting facts.

1. **Override delivery works.** Leaf 2 passes (threshold 2 delivered and consulted), and leaf 4's
   *first* Eventually passes with threshold **0** correctly **suppressing** the SA. So the control
   RPC → registry → live read is sound. (Registry: `crates/tokeira-conformance/src/lib.rs`; live
   accessor: `crates/tokeira-runtime/src/runtime/mod.rs:113/125` `reported_problems_threshold()`.)
2. **The kernel WFT attempt advances.** Leaf 4's first Eventually asserts and satisfies
   `PendingWorkflowTask.Attempt >= 2`, so `WorkflowState.workflow_task_attempt` (the kernel's WFT
   attempt) genuinely reaches ≥2 across the transient retries. The SDK logs confirm the workflow
   re-fails through `"Attempt": 1, 2, 3` (same RunID). **The retries happen and the SDK re-sends
   `RespondWorkflowTaskFailed` each time.**
3. **The derive side is correct.** `apps/tokeirad/src/lib.rs:1623` `apply_reported_problem_search_attribute`
   builds exactly the two asserted strings (`cause=WorkflowTaskFailedCause{failure_cause.as_str()}`),
   and the Describe call site (`:1673–1718`) resolves `run_key` from the repo then reads
   `reported_problem(run_key)`. Because leaf 2 passes, **both the SA format and the `run_key` identity
   are proven to match at runtime.**
4. **The SA counter and the WFT attempt are DIFFERENT counters.** `PendingWorkflowTask.Attempt` =
   kernel `workflow_task_attempt` (verified ≥2). The reported-problems counter =
   `WorkflowTaskProblemTracker.attempts_since_last_success` (runtime-local, volatile), a **separate**
   value incremented only by `record_failure`. Leaf 4 proves the former reaches 2 while the SA (driven
   by the latter) never appears — so **the two have diverged**.
5. **Storage does not reject an empty-events transition.** `crates/tokeira-storage/src/memory.rs`
   `commit_transition` (:309) returns `CommitResult::Duplicate` **only** on a request-id dedupe hit
   (:407–419); an empty-`history_events` transition that passes the OCC seq check commits
   `Applied` (:641). (This is what made my pre-handover hypothesis — "transient failure commits
   non-Applied" — look wrong on paper; the runtime says otherwise, so it must be re-examined against
   the real submit path, not assumed.)

---

## 4. The isolated defect (the question to answer)

> The reported-problems tracker's `attempts_since_last_success` does **not** reach the threshold across
> **transient (attempt>1)** WFT retries — even though (a) the SDK re-fails the WFT on attempts 1→2→3,
> (b) `record_failure` fires correctly for **attempt-1** failures (leaf 2 passes), and (c) the kernel's
> `workflow_task_attempt` reaches ≥2. Why does the counter not advance on the transient path?

The counter lives at `crates/tokeira-runtime/src/runtime/mod.rs:161` (`record_failure`, a
`saturating_add(1)`), and is consulted at `:186` (`reported_problem`, gated on
`attempts_since_last_success >= threshold`). The only caller of `record_failure` is
`crates/tokeira-runtime/src/runtime/workflow_task.rs:808`, inside `fail_workflow_task`, gated on:

```rust
if matches!(&result, Ok(CommitResult::Applied { .. }))
    && reported_failure_cause != WorkflowTaskFailedCause::GrpcMessageTooLarge
{
    self.workflow_task_problem_tracker.record_failure(run_key, reported_failure_cause);
}
```

So for the counter to stall on attempt>1, one of these must hold — **verify which, by reading code**:

### H1 — the transient submit does not return `Ok(CommitResult::Applied)`
`fail_workflow_task` (`workflow_task.rs:772`) submits `Command::WorkflowTaskFailed` via
`submit_for_owned_shard(run_key, …)`. Trace the **actual** return for a transient attempt-2:
`kernel::apply_workflow_task_failed` (`crates/tokeira-kernel/src/kernel.rs:2636`) →
lane commit → storage. Prime suspects, in order:
- **WFT token round-trip for the virtual started id.** For a transient retry the kernel sets a
  **virtual** `scheduled_event_id = last_event_id + 1` and `started_event_id = scheduled_event_id + 1`
  (fail branch ~`kernel.rs:2985–3010`; start branch `apply_workflow_task_started` ~`:1594`). The fail
  path then rejects on `started_event_id != req.started_event_id` → `Reject::WorkflowTaskTokenMismatch`
  (~`:2668`). If the WFT **task token** the SDK echoes on the attempt-2 `RespondWorkflowTaskFailed`
  does not carry that virtual started id (or the edge encodes/decodes it differently), the submit is
  `Err(Reject::…)` and `record_failure` is skipped. **Locate and read the WFT task-token
  encode/decode in `tokeira-edge`** (I did not pin this file — find it: it is where
  `started_event_id`/`logical_seq` are serialised into the token handed to the poller and parsed back
  on `RespondWorkflowTaskFailed`). Confirm the virtual id survives the round-trip.
- **OCC / ownership** on a fast-retrying run: does the attempt-2 submit ever return
  `CommitResult::Conflict` (lane OCC) rather than `Applied`? Read the lane commit path
  (`crates/tokeira-runtime/src/lane.rs`, the `commit_transition_for_bundle` match ~1472–1520).

### H2 — `record_success` inadvertently resets the counter on a transient path
`record_success` (`mod.rs:173`) **removes** the tracker entry (counter → 0). It is called at
`workflow_task.rs:686` in `complete_workflow_task` on `Ok(Applied | Duplicate)`. Verify no transient
retry path routes through a "completion" that clears the tracker — e.g. the bad-update-message /
`drop_without_failing` arms (`workflow_task.rs:565`, `:645`) or any intermediate that lands in the
`complete` success match while the run is still failing.

### H3 — sticky-clear interaction (leaf 3 specifically, and the Go SDK default)
The Go SDK uses a **sticky** task queue by default. v1.31.0 `failWorkflowTask` forces
`incrementAttempt = false` when a sticky queue is set — it **clears sticky and retries non-sticky**,
and does **not** increment `AttemptsSinceLastSuccess` on that sticky-cleared failure
(`workflow_task_state_machine.go:1007–1021 @ v1.31.0`). Two things to verify against **actual** tokeira
behaviour:
- Does tokeira's fail path replicate the sticky-first-failure semantics, and could a sticky-cleared
  failure produce a **non-`Applied`** result (so `record_failure` is skipped) or otherwise not advance
  the counter? Leaf 3 (fail-**after**-activity) is the sharpest probe because the activity completion
  forces exactly the sticky→non-sticky transition.
- Note the **converse risk** (a real but benign divergence I already found): tokeira's
  `fail_workflow_task` counts **every** `Applied` failure with no sticky-first-failure exemption, so in
  principle it could over-count vs v1.31.0. That would make the SA appear *earlier*, not vanish — so it
  is not the failure cause, but it is a genuine v1.31.0 mismatch to record while you are here.

**Method:** disambiguate by **static reading only** — trace the transient-attempt submit path
end-to-end (`fail_workflow_task` → `apply_workflow_task_failed` → lane commit → storage), determine the
**actual** `CommitResult` variant for attempt>1, and follow whether `record_failure` is reached,
cross-checking each step against the v1.31.0 anchors in §5. Do **not** add temporary logging or run the
suite to observe it: if a step genuinely cannot be resolved by reading, say so explicitly in the report
and name the dynamic evidence that would settle it (for whoever owns remediation) rather than gathering
it here.

---

## 5. v1.31.0 ground truth to conform to (verified anchors)

Read from the local checkout at tag `v1.31.0` (`git -C ../temporal show v1.31.0:<path>`), per §8.

| Behaviour | v1.31.0 anchor |
|-----------|----------------|
| `AttemptsSinceLastSuccess += 1` **once per non-sticky** `failWorkflowTask` (transient or not; independent of event emission) | `service/history/workflow/workflow_task_state_machine.go:1017–1021` |
| Sticky set ⇒ `incrementAttempt = false`, clear sticky, retry non-sticky | `workflow_task_state_machine.go:1009–1016` |
| SA **written at failure time** when `AttemptsSinceLastSuccess >= threshold` | `workflow_task_state_machine.go:1050–1055` |
| Transient (attempt>1) failure emits **no** `WorkflowTaskFailed` event | `workflow_task_state_machine.go:892–895` |
| SA persisted / cleared-on-success (write `UpdateReportedProblemsSearchAttribute`; clear) | `service/history/workflow/mutable_state_impl.go:6503–6521` (write), `:6530–6541` (clear) |
| `AttemptsSinceLastSuccess` field semantics | `service/history/interfaces/workflow_task_info.go:24–29` |
| Threshold setting (namespace int, default 5) | `common/dynamicconfig/constants.go:307` |
| SA definition (`TemporalReportedProblems`, KeywordList) | `common/searchattribute/sadefs/constants.go:108–113` |

**Architectural note:** v1.31.0 is **write-at-failure + clear-at-success (persisted SA)**; tokeira
**derives on read** from a volatile tracker (`apps/tokeirad/src/lib.rs:1716`). This is a sanctioned
deviation — but it means the tracker's increment/clear semantics must match v1.31.0's counter exactly,
including the sticky rule. The correctness test is: *does tokeira's Describe return what v1.31.0 would
return for the same execution lineage?*

---

## 6. tokeira code anchors (verified this session)

| Concern | Path:line |
|---------|-----------|
| Counter + threshold | `crates/tokeira-runtime/src/runtime/mod.rs` — `REPORTED_PROBLEMS_THRESHOLD` :106; `reported_problems_threshold()` :113/:125; `record_failure` :161; `record_success` :173; `reported_problem` :186 |
| Failure path / the gate | `crates/tokeira-runtime/src/runtime/workflow_task.rs` — `fail_workflow_task` :772; `record_failure` call + `Ok(Applied)` gate :805–810 |
| Success clears counter | `crates/tokeira-runtime/src/runtime/workflow_task.rs:686` (gated `Applied \| Duplicate`); transient-drop arms :565, :645 |
| Kernel WFT-failed transition | `crates/tokeira-kernel/src/kernel.rs` — `apply_workflow_task_failed` :2636; attempt-1-only event emit ~:2743; transient reschedule + virtual ids + `EnqueueWorkflowTask` ~:2985–3010; token check `Reject::WorkflowTaskTokenMismatch` ~:2668 |
| Kernel WFT-started transition | `crates/tokeira-kernel/src/kernel.rs` — `apply_workflow_task_started` :1551; transient branch `started = scheduled + 1` ~:1594 |
| Storage commit verdict | `crates/tokeira-storage/src/memory.rs` — `commit_transition` :309; `Duplicate` only on dedupe :407–419; `Applied` :641 |
| Describe-side derive | `apps/tokeirad/src/lib.rs` — `apply_reported_problem_search_attribute` :1623; Describe call site + `run_key` resolve :1673–1718 |
| WFT task-token codec | **TODO for you** — locate in `tokeira-edge` (where `started_event_id`/`logical_seq` round-trip into/out of the task token) |

---

## 7. Constraints (binding)

- **No code changes.** This task produces a report only — no source edits, no temporary
  instrumentation or log lines, no "quick fix". Remediation is a separate step owned elsewhere.
- **No test-suite runs** and nothing that boots `tokeirad`. Identify the cause by reading code +
  v1.31.0 only.
- **No kernel additions.** `tokeira-kernel` stays pure and dependency-free of `tokeira-conformance`
  (structural guard: `crates/tokeira-conformance/tests/kernel_purity.rs`).
- **Ground-truth everything** against `v1.31.0` per AGENTS §8; cite anchors (proto path or server
  source + tag) for any behavioural claim.
- **Honesty boundary.** Any fix the report *recommends* must make the *real* counter advance/clear
  correctly — never a hardcoded SA or test special-case. Recommendation only; not implemented here.

## 8. Deliverable — a written report (no code changes)

Produce a **report summarising the identified issue(s)**. No code is changed. It should contain:

1. **The identified issue(s):** which of H1/H2/H3 (or another cause) actually holds — stated as a
   concrete finding, not a maybe.
2. **The proof:** the exact tokeira code path that produces the wrong behaviour (file:line), traced
   step by step, and the **v1.31.0** anchor it diverges from (§5). Cite what you read; no conjecture.
3. **Blast radius:** which of the four leaves each finding explains, and why leaf 2 is unaffected.
4. **The benign divergence:** confirm or refute the sticky over-count (H3 converse) and record it.
5. **Recommended remediation direction (prose, optional):** where a fix would go (expected: runtime
   side — `fail_workflow_task` / the transient submit path / the WFT token codec — **not** the kernel,
   **not** the derive, which §3 verifies sound) and any dynamic evidence that would confirm it before
   implementing. Describe it; do **not** implement it.

Write it up as a doc under `docs/` (e.g. a findings note beside this handover) and hand back the
summary.
