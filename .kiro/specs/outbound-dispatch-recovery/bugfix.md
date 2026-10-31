# Bugfix Requirements: Recoverable Outbound Dispatches

## Status and scope

Requirements approved on 31 October 2026. This is the combined remaining delivery for remediation
item 1.9, in one PR containing the reviewed specification, implementation, verification
and change record. Baseline: `e057750774e38776019bb6c8e7043ebde902a1d5`.

The behavioural authority is Temporal server **v1.31.0**; wire shape comes from the
vendored API **v1.62.11**. This change does not extend the compatibility claim to newer
RPCs. It completes recovery of child starts, external signals/cancellations, Nexus
starts/cancellations, parent-close actions, child-result delivery, reset follow-up and
the oversized-successor fallback. Completion callbacks participate in the preservation
and recovery audit; they already have first-attempt recovery.

[Atomic successors](../atomic-successors/bugfix.md) and
[successor semantics](../successor-semantics/bugfix.md) are merged prerequisites.
Approval of this document authorizes the following specification refinements in this
same PR: replace the atomic-successors size-fallback recovery exclusion and reset
preservation clause; qualify its no-migration clause as applying to that earlier change;
update the child/external/Nexus specifications where their volatile dispatch assumptions
conflict with these requirements. Their unrelated behaviour remains binding.

[Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md)
is a proposal, not an approved implementation design. This document adopts its recovery
objectives; concrete representations and interfaces belong in the subsequent design.
Workflow/activity dispatch reconciliation (item 1.4), general storage-contract coverage
(1.8), queue-home transport (4.7), projection and CHASM/worker-compute redesign are
separate deliveries. This PR includes the storage and routing changes necessary for
these outbound operations, without depending on those later items for recovery.

## Bug summary

A successful run commit can leave required work only in a spawned task. A crash,
ownership change or failed submission can lose that work permanently. Some external
delivery errors also become terminal workflow-history failures even though the target
outcome is unknown. Reconstructing a task without its original identity can create a
second child or deliver an old result to a later child with the same workflow ID.

Reset currently commits its source change before materializing the new run, then
spawns post-reset options and the fork-point workflow-task failure/reapply sequence.
The ordinary successor pair is atomic, but an explicit storage-size refusal takes a
separate-start fallback with no durable recovery obligation.

The corrected system retains sufficient committed information to finish every owed
operation after losing all local tasks, caches and notifications. Duplicate attempts
are expected; duplicate semantic outcomes are not. Reset exposes only a fully prepared
new run. The exceptional successor fallback retains a recoverable obligation and
protects its execution chain until that obligation is satisfied.

## Glossary

- **Operation:** work whose source run records its outcome, such as a child start or
  external signal. Its actionable representation is derived from authoritative state.
- **Close obligation:** work created by a close whose delivery is not subsequently
  recorded in ordinary source workflow history, such as notifying a parent.
- **Incarnation:** the particular initiation, scheduled operation, cancellation request,
  child execution chain or reset to which an attempt belongs.
- **Execution home:** the bundle owning all runs of one namespace/workflow-ID pair.
- **Definitive outcome:** committed success, a proven duplicate of that success, or a
  verified terminal condition allowed by the targeted Temporal behaviour.
- **Eligible:** still required by committed state and due under its persisted timing
  rules; not superseded, closed away, timed out or awaiting a future backoff deadline.
- **Recoverable:** discoverable without the original process, a notification, or a
  caller retry, with enough authoritative data to execute the same obligation safely.

## Evidence from current code

These source-derived failure sequences were confirmed before implementation at
`e057750774e38776019bb6c8e7043ebde902a1d5`: twelve exploration properties failed at
their intended contract assertions. The configured 100 cases stop and shrink at the
first failure, so this is not a successful generated-case count. Commands, minimal
counterexamples and passing preservation controls are retained in
[exploration-baseline.json](../../../crates/tokeira-runtime/src/outbound/exploration-baseline.json).
The table describes that baseline; the delivery ledger records corrected-path results.

| Path | Verified baseline behaviour and gap |
|---|---|
| Child start | `crates/tokeira-runtime/src/publisher.rs::handle_start_child_workflow` allocates a new child run ID for each orchestration invocation. `child_start.rs` retries start/confirmation in memory and retains work on parent-read errors, but neither that invocation nor its chosen identity survives a crash. `kernel.rs` persists initiation configuration but not `reuse_policy`; `ChildWorkflowState` does not retain the full start request. |
| Child ordering/deduplication | `crates/tokeira-kernel/src/kernel.rs::apply_start` schedules the first task during child creation. `apply_child_start_confirmed` checks the initiation but can append another started event on a repeated confirmation. `apply_child_resolved` looks up a child by workflow ID, without an initiation/first-run fence supplied by the request. Recovery must close these races rather than amplify them. |
| External signal/cancel | `publisher.rs::handle_signal_external_workflow` and `handle_cancel_external_workflow` attempt delivery once, map several storage/lane/conflict errors to failed outcomes, and only log failed source confirmation. Stable request IDs already include the source run and initiation; preserve them. Pending-state entries omit payload fields available in initiation history. |
| Nexus start/cancel | `publisher.rs` spawns the first attempt. `nexus.rs::scan_nexus_timeouts_once` retries persisted backoffs, not lost scheduled first attempts. Recovery reconstructs timeout tracking only for operations with a timeout. `storage::reconstructible_nexus_deliveries` supplies advisory worker-capacity sampling, not execution recovery. |
| Completion callbacks | `recovery_index.rs` includes scheduled/backing-off callbacks on closed runs. `publisher.rs::scan_completion_callbacks_once` retries both states and rehydrates the terminal event. Preserve this working recovery path; a new operation mechanism must not introduce a second lifecycle authority. |
| Parent close | `kernel.rs::apply_parent_close_policy` emits terminate/cancel effects; `publisher.rs` resolves/submits them after commit and logs failures. There is no durable record of which close actions remain owed. |
| Child result | The post-close path in `crates/tokeira-runtime/src/lane.rs` spawns `ChildResolved`, follows reset redirects, and returns after failed delivery. It excludes successor/reset closes. No durable obligation survives loss of that task. |
| Reset | `lane.rs::run_activation_with_cache` reads reapply history and calls `materialize_reset_successor` after the source commit, then spawns options followed by `WorkflowTaskFailed(ResetWorkflow)`. A failure can expose a partial reset. |
| Oversized successor pair | `lane.rs` creates `SuccessorEffect::Deferred` after a size refusal; its post-commit task submits the frozen start and logs unsuccessful outcomes. The close has no recoverable successor obligation. |
| Acquisition | `recovery.rs` rebuilds workflow/activity/timer/timeout/callback state. `storage::recovery_needed` does not select runs solely because they have pending children or external operations. Adding an executor without discovery coverage is insufficient. |

### Temporal ground truth

All following paths refer to **v1.31.0** in the Temporal server repository.

- `service/history/transfer_queue_active_task_executor.go::processStartChildExecution`:
  durable create-request identity; retries transient/unknown failures; only existing
  workflow and missing namespace become start-failed events; records the parent started
  event before releasing the child's first task. Closed parents do not start an
  unconfirmed child, including ABANDON. An already-confirmed ABANDON child can still
  receive its first task after the parent closes.
- `service/history/workflow/mutable_state_impl.go::ApplyStartChildWorkflowExecutionInitiatedEvent`
  and `generateChildWorkflowRequestID`: initiation identity survives replay.
- `service/history/transfer_queue_active_task_executor.go::processSignalExecution`,
  `processCancelExecution`, `signalExternalExecution` and
  `requestCancelExternalExecution`: source outcome follows target delivery; stable
  request IDs deduplicate retries; transient/unknown errors remain retryable. Failure
  mappings are listed below.
- `components/nexusoperations/statemachine.go::RegenerateTasks` for `Operation` and
  `Cancelation`: scheduled work regenerates invocation/cancellation tasks independently
  of timeout tasks; backing-off work regenerates its persisted deadline.
  `components/nexusoperations/executors.go::loadOperationArgs`,
  `handleStartOperationError` and `saveCancelationResult` retain request identity and
  distinguish retryable delivery failures from semantic terminal outcomes.
- `components/callbacks/statemachine.go::Callback.RegenerateTasks`: scheduled and
  backing-off callback delivery is recoverable.
- `service/history/transfer_queue_active_task_executor.go::processCloseExecution` and
  `applyParentClosePolicy`: close delivery retries; child results exclude continued
  chains and reset termination; terminate/cancel follows the child execution chain
  using its first-run identity and parent relationship, not an unrelated reused ID.
- `service/history/api/recordchildworkflowcompleted/api.go`: reset redirects, initiation
  and first-child-run validation, and started-event synthesis before completion when
  the parent has not yet recorded the start.
- `service/history/ndc/workflow_resetter.go::ResetWorkflow` and `persistToDB`: prepare
  reapply and post-reset operations before persistence; persist participating runs in
  one transaction before success.
- `common/persistence/sql/execution.go::updateWorkflowExecutionTx`: ordinary successor
  close, creation and conditional current-run update share a transaction. Tokeira's
  size fallback is a storage-specific exception, not a Temporal behaviour claim.

Wire anchors are the child/external/Nexus initiated, resolved and terminal messages in
`proto/upstream/temporal/api/history/v1/message.proto`. No upstream proto is modified.

## Reproduction and bug condition

For each affected operation, commit the initiating or closing transition, discard its
post-commit task before the next durable step, erase volatile state and acquire the
execution home on a new runtime. Run discovery with healthy storage and destinations.
The expected result is delivery or a justified terminal outcome. Baseline gaps include
an unstarted child, unconfirmed external delivery, a Nexus operation without a timeout,
a lost parent notification, a partial reset and a missing fallback successor.

Additional cuts are after destination success but before source confirmation, after
source confirmation but before first-child-task release, and after successful close
delivery but before its retirement. Repeat with a new owner and a reused workflow ID.
Inject transient target lookup/commit errors separately: they must not manufacture a
terminal external-delivery history event.

Let `X` contain committed runs/history, outstanding obligations, ownership epochs,
destination outcomes and all local execution state. For an incarnation `o`, define:

```text
C(X) = exists o:
    (owed(X, o) and not definitive(X, o) and not recoverable(X, o))
    or (terminal_source_failure(X, o) and only_transient_or_unknown_failure(X, o))
    or duplicate_semantic_outcome_or_wrong_incarnation(X, o)
    or published_incomplete_reset(X, o)
```

The exploration property asserts that `C(X)` never holds under injected crash cuts and
duplicate attempts; it is expected to fail on the baseline. Eventual progress assumes
a live owner, fair scheduling and eventual availability of required dependencies.
Safety must hold even while those progress assumptions are false.

## Delivery contract policy

This is a recovery contract over existing accepted commands and effects, not a new RPC
surface. Each field in the existing outbound `DispatchOp` variants is covered below;
additional authority needed for incarnation fences is explicit. Public validation and
unrelated field-conformance work remain with the existing feature specifications.

| Contract elements | Recovery policy | Invalid/stale handling | Persistence or lifecycle impact |
|---|---|---|---|
| Source run key, namespace/workflow/run IDs; initiation or scheduled event ID; operation kind | Bind every obligation and response to this incarnation | Typed stale outcome; never mutate a replacement incarnation | Stable identity before first dispatch |
| Child namespace, workflow ID/type, queue, input, header, memo, search attributes | Reconstruct the accepted start exactly, including hydrated payloads | Preserve admission validation; unreadable persisted data remains outstanding | Authoritative initiation data, not a volatile copy |
| Child execution/run/task timeouts, retry policy, cron schedule, reuse policy | Retain the original accepted policy across restart | Never substitute a default for a lost persisted policy | Persist previously dispatch-only reuse policy for new initiations |
| Parent key/IDs/namespace; root workflow/run IDs; initiated event; child close policy | Preserve lineage and the particular child chain | Reject unrelated parent/child incarnations | Required for confirmation, close actions and result delivery |
| Child priority; deployment/version inheritance | Follow committed-parent inheritance rules used by the existing start path; freeze a selected start across retries | Registry/read failure is retryable | Recovery must use the same documented inheritance point as first delivery |
| Child run ID and start request ID | Stable across process loss and duplicate discoverers | An unrelated duplicate request is not proof of child creation | Recover the same child; do not allocate a replacement on retry |
| External target namespace/workflow/optional run, signal name/input/header; cancel reason; source identity; initiation ID | Preserve accepted target selection and payload; use existing stable request IDs | Use the terminal mapping below; otherwise retry | Read full initiation history where pending state omits payload |
| External namespace name and deprecated control retained in initiation/resolution history | Preserve source history and resolving-event metadata | Namespace lookup failure is not automatically namespace absence | Rehydrate from committed data without rewriting initiation history |
| Nexus operation ID, endpoint, service, operation, input, source run, scheduled event/time and three timeout fields | Reconstruct first attempts independently of timeouts; preserve existing wire translation | Outcome/deadline checked against authoritative operation state | Derive eligibility from scheduled/backoff state |
| Nexus cancellation operation ID, endpoint/service, source run, scheduled and requested event IDs; operation token, attempt/deadline from pending state | Deliver the same cancellation incarnation with its accepted token | Reject stale attempt outcomes | Source transition owns attempt and cancellation lifecycle |
| Callback index, callback configuration and terminal outcome | Preserve existing scheduled/backoff scanner and outcome hydration | Stale/terminal callback is not invoked | One existing lifecycle authority, including closed runs |
| Parent-close target namespace/workflow/first child run, policy, reason and parent identity | Act on the matching child chain; ABANDON creates no close action | Unrelated reuse is a no-op; transient lookup is not absence | Close atomically establishes each owed action |
| Child result: parent initiation/lineage, first and final child run, completed/failed/canceled/terminated/timed-out outcome and event attributes | Deliver the final chain outcome to the matching parent or reset successor | Stale initiation/chain is a no-op | Close atomically establishes the notification obligation |
| Reset request identity, base/current/new runs, fork point, reapply exclusions/events, ordered post-reset options | Prepare one complete reset with stable identity | Failure before publication exposes no partial reset | Atomic final publication includes pointer and all follow-up state |
| Fallback successor frozen start, predecessor/successor IDs, request ID, clocks, expected current execution | Reuse the already-built successor from the refused pair | Generic duplicate/conflict is not proof of success | Durable obligation and chain protection commit with the close |

### Terminal delivery outcomes

| Operation | Definitive source outcome | Retry without terminal history |
|---|---|---|
| Child start | Verified start; `WORKFLOW_ALREADY_EXISTS`; `NAMESPACE_NOT_FOUND` | Storage/OCC/ownership/transport failures and unclassified errors |
| External signal | Verified delivery; `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`; `NAMESPACE_NOT_FOUND`; `SIGNAL_COUNT_LIMIT_EXCEEDED` | Transient/context failures, failed dedupe lookup and unclassified errors |
| External cancel | Verified request; `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`; `NAMESPACE_NOT_FOUND` | Transient/context failures and unclassified errors |
| Parent-close action or child result | Verified application, duplicate, or verified absence/closed/stale relationship under the Temporal rules above | Lookup/commit errors, lost replies and unresolved reset redirects |
| Nexus/callback | Existing protocol outcome classification and authoritative timeout/backoff transitions | Delivery/confirmation failure whose outcome remains unknown |

## Expected behaviour

### Requirement 1: Commit the obligation with its cause

**User story:** As an operator, I want committed work to survive process loss, so that
workflow progress does not depend on successful publication.

1. WHEN a transition creates an outbound obligation, THE storage layer SHALL commit its recoverable representation atomically with that transition.
2. IF the obligation cannot be made recoverable within storage limits, THEN THE runtime SHALL fail the originating commit without exposing its new history or state.
3. THE runtime SHALL derive delivery payloads and identities from authoritative committed data.
4. WHEN a source outcome transition makes an operation no longer eligible, THE storage layer SHALL retire its derived actionable representation in that transition.
5. THE operation executor SHALL leave derived operation lifecycle changes to source transitions.
6. THE close-obligation executor SHALL durably retire only the exact incarnation for which it has established a definitive outcome.
7. THE recovery process SHALL preserve retired close-obligation completion rather than regenerating delivered work from old close history.

### Requirement 2: Continuous discovery and ownership

**User story:** As an operator, I want every active execution home to rediscover due
work, so that missed notifications and owner replacement do not stall runs.

1. WHILE an execution home is active, THE runtime SHALL repeatedly discover every eligible obligation through bounded, fairly scheduled passes independent of notifications.
2. WHEN ownership is acquired, THE runtime SHALL reconcile reconstructible operation state before admitting work for that acquisition.
3. THE discovery path SHALL include runs whose only pending work is a child, external operation, untimed Nexus operation or closed-run obligation.
4. IF a read or reconciliation fails, THEN THE runtime SHALL retain the affected work for retry.
5. WHEN ownership is lost, THE old executor SHALL stop initiating further attempts for that acquisition.
6. THE storage layer SHALL fence operation creation, reconciliation, result commits and obligation retirement against the owning execution home.
7. THE destination SHALL validate an attempt's incarnation independently of the discoverer's cached state.
8. THE runtime SHALL bound concurrent attempts without using an in-memory claim as evidence that work is complete.

### Requirement 3: Recoverable child-start handshake

**User story:** As a workflow author, I want an initiated child to start once despite
crashes, so that a retry cannot create a second execution or lose its confirmation.

1. WHEN a child initiation commits, THE system SHALL retain enough authoritative information to recover the accepted child start, including reuse policy and stable request identity.
2. WHEN a child-start attempt is repeated, THE executor SHALL address the same chosen child execution identity.
3. IF a previous attempt may have committed, THEN THE executor SHALL verify the matching child incarnation before reporting success or an already-existing-workflow failure.
4. WHEN a verified start outcome is available, THE runtime SHALL retry recording that outcome on the parent until committed or definitively obsolete.
5. WHEN a confirmation is duplicated, THE parent transition SHALL append at most one started or failed event for that initiation.
6. THE runtime SHALL make the child's first workflow task runnable only after the parent has durably recorded its start, subject to the v1.31.0 already-started ABANDON case.
7. WHEN parent confirmation commits but first-task release is lost, THE runtime SHALL recover that release from durable state.
8. IF the parent is closed before an unconfirmed child can be started, THEN THE executor SHALL abandon further start attempts as specified by `processStartChildExecution @ v1.31.0`.
9. IF child delivery fails without a definitive outcome, THEN THE runtime SHALL retain the initiation without recording a start-failed event.

### Requirement 4: External signal and cancellation recovery

**User story:** As a workflow author, I want external operations to survive failures,
so that a transient outage neither loses a message nor reports a false failure.

1. WHEN a pending external operation is rediscovered, THE executor SHALL reconstruct its accepted payload and reuse the original stable request ID.
2. IF target delivery or outcome lookup is transient or unknown, THEN THE executor SHALL retain the operation for retry without recording terminal source history.
3. WHEN a definitive outcome is obtained, THE runtime SHALL retry the corresponding source resolution transition until applied or definitively obsolete.
4. WHEN target or source delivery is repeated, THE system SHALL record at most one semantic outcome for that initiation on each run.
5. THE runtime SHALL apply the terminal-cause mapping in this document's delivery-outcome table.
6. IF the source is already closed or the initiation is no longer pending, THEN THE executor SHALL stop new attempts for that operation.
7. THE runtime SHALL preserve the existing self-target, cross-namespace, explicit-run and request-deduplication rules across recovery.

### Requirement 5: Nexus and callback recovery

**User story:** As a workflow author, I want scheduled Nexus work to run even without
a timeout, so that losing its first task cannot leave it permanently pending.

1. WHEN an eligible Nexus start or cancellation has no recorded attempt outcome, THE runtime SHALL rediscover it independently of timeout tracking.
2. WHEN a Nexus operation is backing off, THE runtime SHALL preserve its authoritative next-attempt deadline across restart.
3. WHEN an attempt outcome arrives, THE source transition SHALL reject stale scheduled/requested-event or attempt identities.
4. THE runtime SHALL retain Nexus request identity, operation token, payload and timeout anchors across retries.
5. IF an operation is terminal, timed out, or no longer eligible for the attempted phase, THEN THE executor SHALL suppress further attempts for that phase.
6. WHEN cancellation delivery succeeds, THE system SHALL preserve the distinction between cancellation-request completion and operation completion.
7. THE runtime SHALL preserve scheduled and backing-off completion-callback recovery on closed runs.
8. THE runtime SHALL use a single attempt lifecycle authority for each Nexus operation, cancellation and callback.

### Requirement 6: Durable consequences of closing a run

**User story:** As a workflow author, I want parent-close actions and child results to
survive crashes, so that related executions converge to the recorded outcome.

1. WHEN a close requires child termination, cancellation or parent notification, THE storage layer SHALL establish each required obligation in the close's transaction.
2. WHEN a parent-close action is retried, THE executor SHALL target only the child chain identified by its parent relationship and first run.
3. IF a workflow ID has been reused for an unrelated execution, THEN THE executor SHALL leave that execution unchanged.
4. WHEN a child chain ends, THE runtime SHALL deliver its final result to the matching parent initiation, including valid reset redirects.
5. IF a close continues into a successor or is a reset termination, THEN THE runtime SHALL suppress the premature child-result notification specified by `processCloseExecution @ v1.31.0`.
6. WHEN a result arrives before the parent has recorded the matching child start, THE parent transition SHALL establish the started event before the terminal event as in `recordchildworkflowcompleted/api.go @ v1.31.0`.
7. WHEN a child result is duplicated or stale, THE parent transition SHALL leave another child incarnation unchanged.
8. WHEN delivery succeeds but retirement fails, THE executor SHALL safely repeat delivery before retiring the same obligation.
9. IF delivery has no definitive outcome, THEN THE executor SHALL retain the obligation across retry exhaustion, drain and process loss.

### Requirement 7: Atomic reset publication

**User story:** As a workflow author, I want a successful reset to expose a complete
new branch, so that its options and reapplied events cannot disappear after success.

1. WHEN a reset is prepared, THE runtime SHALL construct the fork-point task outcome, eligible reapplied events and ordered post-reset options before publishing the reset.
2. WHEN a reset commits, THE storage layer SHALL atomically publish the participating authoritative run changes and conditional current-run update.
3. IF preparation or publication fails, THEN THE system SHALL expose no partially applied reset.
4. WHILE a large reset is being staged, THE runtime SHALL keep its incomplete new run unavailable to command admission and task delivery.
5. WHEN a reset request is retried, THE system SHALL preserve its request/new-run identity and prevent duplicate reapply or option application.
6. THE runtime SHALL make the new run's fresh workflow task discoverable only with the completed reset state.
7. THE reset implementation SHALL preserve existing reapply exclusions, event ordering, versioning-option semantics and child reconnection rules.

### Requirement 8: Recoverable oversized-successor fallback

**User story:** As a workflow author, I want a storage-size fallback to retain my
workflow chain, so that a committed close never silently loses its successor.

1. THE runtime SHALL retain the existing atomic paired commit as the ordinary successor path.
2. IF an explicit storage-size refusal selects the fallback, THEN THE storage layer SHALL commit a recoverable successor obligation with the predecessor close.
3. THE fallback obligation SHALL retain the frozen start request, identities and clock values selected for the refused paired commit.
4. WHILE that successor remains owed, THE storage layer SHALL protect its execution-chain claim against an unrelated start taking the workflow ID.
5. WHEN the fallback is rediscovered, THE runtime SHALL retry successor materialization through the execution home's fenced internal path.
6. IF a retry returns a duplicate or conflict, THEN THE runtime SHALL verify the expected successor's committed identity and lineage before retiring the obligation.
7. IF the close plus obligation cannot be committed safely, THEN THE runtime SHALL leave the predecessor unchanged.
8. WHEN successor materialization is complete, THE runtime SHALL recover its tasks and timeout tracking through the ordinary committed-run recovery paths.
9. THE runtime SHALL preserve the merged successor-semantics field policy for both ordinary and fallback starts.

### Requirement 9: Persisted data and upgrade boundaries

**User story:** As an operator, I want recovery to retain original intent, so that an
upgrade cannot silently invent missing policies or erase undelivered work.

1. THE storage layer SHALL preserve existing payload chunking and hydration guarantees for outbound data exceeding an inline row budget.
2. THE storage layer SHALL account for operation/obligation writes in transaction-size admission before exposing the source transition.
3. WHEN legacy pending work has sufficient authoritative data, THE recovery process SHALL reconstruct it without changing its identity or accepted payload.
4. IF legacy data lacks required identity or policy, THEN THE recovery process SHALL report a typed unresolved-recovery condition identifying the affected run and missing information.
5. WHILE such a condition remains unresolved, THE system SHALL retain the evidence without marking the work delivered or substituting guessed defaults.
6. THE rollout procedure SHALL require compatible writers before enabling the new recovery guarantee, using an explicit checked cutover condition defined in the design.
7. THE implementation SHALL provide a documented inspection and resolution procedure for legacy unresolved-recovery conditions.

Already-lost historical close obligations cannot be inferred as delivered or undelivered
from close history alone. The design must state its cutover boundary and any supported
repair evidence explicitly. The PR must not claim to have retroactively recovered work
whose intent or outcome is no longer knowable.

### Requirement 10: Preservation and verification

**User story:** As a maintainer, I want executable recovery and preservation evidence,
so that completing item 1.9 does not weaken existing workflow semantics.

1. THE implementation SHALL preserve the kernel's deterministic, I/O-free single-run transition boundary.
2. THE implementation SHALL preserve successful-path payloads, public history ordering and validation outside the explicitly corrected recovery/ordering cases.
3. THE implementation SHALL preserve ordinary atomic successors and their approved metadata, retry, cron, delay and timeout semantics.
4. THE verification suite SHALL demonstrate the baseline bug condition with a failing exploration property before the corrective implementation.
5. THE verification suite SHALL cover crash cuts before dispatch, after target success, before source confirmation and before obligation retirement for every affected operation class.
6. THE verification suite SHALL cover duplicate discovery, transient failures, competing owners, stale incarnations and payload hydration in both storage backends.
7. THE verification suite SHALL cover missing-timeout Nexus operations, first-child-task release, child-chain reuse, reset publication and oversized-successor recovery.
8. THE delivery SHALL include pinned Temporal conformance evidence for the affected child, external-operation, Nexus, reset and successor behaviours.
9. THE PR SHALL include the repository completion checks, a change record and a path-by-path item 1.9 completion ledger.

## Preservation set

Outside `C(X)`, preserve ordinary atomic commits, request validation and deduplication,
history payloads, task/timer ordering, successor inheritance and clocks, versioned
worker routing, callback recovery, reset reapply semantics and all unaffected execution
types. Test unchanged successful paths alongside each correction. Storage remains DSQL
centred; no new dependency or compatibility-version change is authorized by this draft.

The next design must enumerate each obligation's incarnation, discoverer, executor,
remover, retry authority and deduplication proof. It must address storage bounds,
fencing, cutover and unresolved legacy data before implementation begins.
