# Requirements: Durable Activity Dispatch

## Introduction

Complete the remaining activity delivery work in remediation item 1.4. Workflow
task dispatch already uses durable actionable state; workflow-backed activity
tasks still depend on execution-owner publication, a first-page reconciliation
scan, and a second persisted backlog. This is the activity successor to
[workflow-dispatch](../workflow-dispatch/requirements.md), following
[architecture 042](../../../docs/architecture/042-durable-actionable-state.md).

This is an architectural completion, with requirements reviewed before design or
implementation. The baseline is `bc5fc2070943b640a76dbc0c3147de990f207953`, which
includes storage-contract PR 342. Behavior authority is Temporal server v1.31.0;
wire authority is the vendored API v1.62.11. The newer proto surface does not
extend the behavioral claim.

Scope includes activity-derived storage, queue-home discovery, disposable offers,
the complete execution-owner start path, acquisition repair, and coordinated
retirement of legacy activity backlog delivery. Expected implementation owners
are `tokeira-storage`, `tokeira-runtime`, `tokeira-edge`, internal runtime protos,
and server wiring. Any durable start identity or pure transition change belongs
in the existing kernel/state contracts, with its encoding and replay treatment
reviewed in the design. No dependency change is proposed.

## Glossary

- **Workflow activity:** an activity in a workflow run's authoritative activity
  map; standalone CHASM activity executions are a separate surface.
- **Slot:** `(run_key, activity_id)`, which may hold successive schedules and retries.
- **Incarnation:** the schedule event, attempt and stamp identifying one startable
  instance of a slot. An old offer cannot start a replacement incarnation.
- **Wanted row:** the routing and timing metadata derived from a running run's
  pending, unstarted, unpaused activity with a current attempt schedule.
- **Due:** the row's committed eligibility time has arrived. A future retry has a
  wanted row even before it is due; timeout and workflow-rule gates still apply.
- **Execution home:** the bundle owning the workflow identity and its fenced writes.
- **Queue home:** the current owner of the activity task-queue partition, which
  is the queue's workflow-key partition, shared with its workflow tasks. This home
  serves poll demand independently of the execution home.
- **Pass:** a traversal of a due discovery range with a fixed eligibility cutoff,
  a total-order continuation, and bounded scheduler slices.
- **Offer:** a disposable candidate held ready or in flight at a queue home; it
  confers no authority to start or remove durable work.
- **Start request identity:** an internal identity retained across retries of one
  poller's start attempt, distinct from the activity incarnation and worker name.
- **Acquisition generation:** one local ownership attempt, with its captured lease
  fence and cancellation token; a later attempt must not inherit its work.

## Target State

`activity_dispatch` is the sole durable discovery representation for workflow
activities. Each relevant committed state change maintains its wanted rows in the
same atomic write. Queue homes discover due work across execution homes and keep
only bounded, disposable offers. Lost publication, broker contents or discovery
cursors cannot strand a due activity while compatible poll demand and storage
service remain available. The execution owner alone validates and commits start.

Acquisition repairs missing, obsolete and incorrectly routed activity rows before
the home serves. The activity grace spill, destructive backlog drain and per-home
dispatch reconciliation loop retire together after that replacement is verified.
Activity timeout tracking and timeout transitions remain active and authoritative.

The full queue-home-to-execution-owner start path is included: forwarding an offer
alone cannot make an activity poll succeed. Proactive cross-node offer push and
other remaining item 4.7 work are not prerequisites for correctness here. General
run-scoped RPC forwarding, speculative workflow tasks, standalone CHASM activities,
new public RPC support, timeout architecture redesign, and changes to other
derived-operation or intent families are out of scope. Existing eager workflow
activity delivery must continue to work. This specification does not amend the
workflow-task start retry contract.

## Evidence From Current Code

### Tokeira baseline

- [`runtime/activity.rs`](../../../crates/tokeira-runtime/src/runtime/activity.rs),
  `reconcile_due_activity_dispatches_once`, always supplies `None` as its storage
  cursor. `reconcile_activity_dispatch_candidates` leaves valid rows in place,
  including rows already in the broker. A retained full first page can therefore
  hide later due rows on every tick. This is a code-derived counterexample, not
  an executed reproducer yet.
- [`activity_timeout.rs`](../../../crates/tokeira-runtime/src/activity_timeout.rs),
  `run_activity_timeout_scanner`, invokes that pass for active execution homes.
  [`discovery.rs`](../../../crates/tokeira-runtime/src/discovery.rs) currently
  provides demand-driven, resumable queue-home discovery for workflow tasks only.
- [`workflow_service.rs`](../../../crates/tokeira-edge/src/workflow_service.rs),
  `poll_activity_task_queue_inner`, enforces local activity queue ownership.
  `start_activity_task_inner` in `runtime/activity.rs` instead requires the
  execution home's local write fence. The
  [queue-home delivery boundary](../queue-home-delivery/bugfix.md#boundaries-and-deferred-work)
  explicitly reserves the complete activity path for a separate specification.
- [`commit.rs`](../../../crates/tokeira-storage/src/dsql/run_repository/commit.rs)
  maintains `activity_dispatch` through activity and enqueue operations.
  `update_existing_activity_dispatch` is update-only; it cannot rebuild a missing
  row. [`recovery.rs`](../../../crates/tokeira-runtime/src/recovery.rs),
  `RepairAcquisition::repair`, repairs workflow dispatch and outbound operations,
  not activity dispatch. Its complete due-activity republish helper is test-only.
- [`activity.rs`](../../../crates/tokeira-storage/src/dsql/run_repository/activity.rs)
  has a per-home keyset listing but queue listings order by `dispatch_at,
  created_at` with no continuation. The latter is not a total order for rows
  created together. [`api.rs`](../../../crates/tokeira-storage/src/api.rs) defines
  the existing activity task, due-row and conditional-deletion identity fields.
- [`broker.rs`](../../../crates/tokeira-runtime/src/broker.rs) deduplicates ready
  activity tasks by run, activity, attempt and stamp, then removes that identity
  on take. Unlike workflow offers, it has no queue-home generation or bounded
  in-flight offer lifecycle. [`backlog.rs` at the baseline](https://github.com/tokeira/tokeira/blob/bc5fc2070943b640a76dbc0c3147de990f207953/crates/tokeira-runtime/src/backlog.rs)
  still spills and destructively drains activity copies.
- `prepare_activity_dispatch_publish` in `runtime/activity.rs` checks current
  activity state, eligibility, schedule deadlines and workflow rules; applying a
  rule can itself commit a pause under the execution owner's fence. Moving this
  helper unchanged to a foreign queue home would violate that ownership rule.
- [`state.rs`](../../../crates/tokeira-kernel/src/state.rs), `ActivityState`, stores
  attempt times, pause, stamp, priority, heartbeat and worker identity.
  [`kernel.rs`](../../../crates/tokeira-kernel/src/kernel.rs),
  `apply_activity_started`, records start and advances the stamp. The new start
  protocol must distinguish a replay of its committed request from an old offer.

### Pinned Temporal authority

The following server functions were read at **v1.31.0**; these citations are
behavioral evidence, not permission to copy their implementation.

| Source | Verified contract relevant to this work |
|---|---|
| `service/history/transfer_queue_active_task_executor.go`, `processActivityTask` | Reload activity state; discard missing, stamp-mismatched or paused work; verify running execution; derive matching directive and merged priority. |
| `service/history/timer_queue_active_task_executor.go`, `executeActivityRetryTimerTask` | Recheck stamp, pause, attempt, started state and running execution; evaluate current workflow rules before adding a retry to matching. |
| `service/history/api/recordactivitytaskstarted/api.go`, `Invoke` | Missing/closed/stale activity is not started; same request ID can recover an existing start; a different request receives already-started; enforce workflow rules, stamp and deployment transition rules before start. |
| Same file, `getDeploymentVersionAndRevisionNumberForWorkflowID` and `processActivityWorkflowRules` | Activity start consults workflow-task routing for deployment transitions; independent activity routing must not spuriously transition pinned workflows; first-attempt rule evaluation occurs at start. |
| `service/matching/matching_engine.go`, `PollActivityTaskQueue` and `recordActivityTaskStarted` | Matching asks history to record start using a generated request ID and the poller's metadata; stale/already-started candidates are consumed internally rather than returned as worker tasks. |

Vendored wire shape is
[`request_response.proto`](../../../proto/upstream/temporal/api/workflowservice/v1/request_response.proto),
`PollActivityTaskQueueRequest/Response`, and
[`history/message.proto`](../../../proto/upstream/temporal/api/history/v1/message.proto),
`ActivityTaskScheduledEventAttributes` and `ActivityTaskStartedEventAttributes`.
The latter documents request-ID recovery after a lost start reply; the server
source above verifies the behavior. No public field or enum is added here.

## Contract Policy

### Durable row and disposable activity offer

This accounts for every existing `DispatchableActivityTask` field, the due-row
wrapper, and `ActivityDispatchIdentity`. Additional storage/proto fields require
an explicit policy in the reviewed design.

| Field | Target policy | Invalid or obsolete outcome | Persistence / side-effect impact |
|---|---|---|---|
| `run_key` | Bind to the authoritative run and its persisted execution home | Missing/deleted run has no wanted row; foreign binding is refused | No mutation of a different run or guessed home |
| `queue.namespace_id` | Bind to the run's namespace | Mismatch is stale; malformed peer input is `INVALID_ARGUMENT` | Namespace isolation |
| `queue.task_queue` | Derive from current activity routing policy | Superseded queue is stale | Replace in the state transaction |
| `queue.task_kind` | Activity only | Wrong peer task kind is `INVALID_ARGUMENT` | No workflow offer or row is consumed |
| `queue.deployment`, `queue.build_id` | Preserve optional coordinates and current exact/live routing semantics | Incompatible poller receives no task; routing lookup failure is retryable | Discovery cannot freeze an obsolete deployment choice |
| `activity_id` | Identify the slot within its run | Absent or superseded slot is stale | One wanted row per slot |
| `schedule_event_id` | Preserve the exact scheduled-event identity | Mismatch is stale | Input/header read only from that history event |
| `attempt` | Preserve the positive current attempt | Invalid peer encoding is `INVALID_ARGUMENT`; mismatch is stale | Retry replaces the incarnation |
| `stamp` | Preserve the current supersession token without narrowing or saturation | Unrepresentable value is an explicit storage error; mismatch is stale | No old offer starts after a superseding transition |
| `dispatch_revision` | Preserve routing decision revision and re-resolve under current routing policy | Obsolete decision cannot bypass version checks | Never manufacture a revision from wall-clock time |
| `priority` | Merge workflow/activity priority field by field under existing policy | Existing priority validation remains authoritative | Persist effective metadata; do not change fairness semantics |
| `order` | Disposable broker ordering under existing priority/fairness/rate policy | Expired ordering grants no durable claim | No second persisted fairness/backlog authority |
| `dispatch_at` | Derive from the current attempt's committed schedule | Missing required schedule blocks successful derivation/repair; future time is not due | Retry/backoff survives all volatile-state loss |
| Full conditional-delete identity | Compare every observed incarnation, routing revision and eligibility coordinate | Changed replacement is retained | Cleanup cannot delete a concurrently replaced row |

### Discovery and owner-start contract

This is the semantic inventory for the new internal path; protobuf layout and
finite limits are design decisions, not implicit changes to the public API.

| Input / output | Target policy | Error or refused outcome | Persistence / side-effect impact |
|---|---|---|---|
| Discovery namespace, queue, routing range | Restrict to the local activity queue-home acquisition and compatible demand | Retired home/range is cancelled | Read-only across execution homes |
| Eligibility cutoff, exclusive continuation, page size | Fixed cutoff within a pass; stable total order; positive bounded pages | Invalid local arguments are typed errors; query/decode failure is retryable | No durable cursor or claim |
| Candidates, last-examined position, exhaustion | Continue past examined rows even when none can be offered | Never infer exhaustion from zero admissions | Later rows remain reachable |
| Offered identity and routing metadata | Carry the row/offer values above; bind them again to current authority | Stale offer is typed internal `NOT_FOUND`, distinguishable from transport failure | No start based solely on supplied metadata |
| Namespace and poll target | Preserve the admitted namespace, queue, deployment and build scope | `UNAUTHENTICATED` / `PERMISSION_DENIED` under existing admission | Owner independently authorizes and binds the target |
| Worker identity | Preserve exactly, including empty identity where already accepted | Existing public validation | Recorded by the authoritative start |
| Start request identity | Stable for retries of one matched poll; different for competing polls | Malformed identity is `INVALID_ARGUMENT`; different already-started request is consumed as stale | Same-request recovery is tied to the consumed incarnation |
| Peer identity and caller credentials | Use existing peer transport plus worker admission; never persist credentials | `UNAUTHENTICATED` / `PERMISSION_DENIED` | A peer credential alone grants no worker access |
| Deadline, cancellation, routing destination | Bound calls by the original poll lifetime; refresh placement through the existing directory | Wrong/inactive owner and transient transport failure are retryable `UNAVAILABLE`; timeout is not proof of non-commit | No unbounded forwarding chain or replacement start |
| Started result | Preserve run/activity identity, task token, workflow metadata, input/header, heartbeat details, all schedule/start times, attempt, timeouts, retry policy and priority | Never emit a successful poll result before authoritative start | Same-request replay creates no additional start |
| Encoded request/response size | Enforce finite transport limits and preflight the successful response before commit | `RESOURCE_EXHAUSTED` before starting if the result cannot be transported | No knowingly committed but undeliverable start |

Public poll fields, including metadata, version capabilities, deployment options,
worker instance/control queue fields, poller groups and scaling hints, retain their
existing admission and response treatment. This work neither drops those fields
from existing paths nor claims new support for newer proto-only behavior. The
design must enumerate every field of its new internal messages and show how each
existing `StartedActivityTask` field reaches the public response.

## Requirements

### Requirement 1: Derived rows are complete and atomic

**User Story:** As a workflow operator, I want committed activity work to remain
discoverable, so that publication failure cannot lose it.

#### Acceptance Criteria

1. WHEN a state transition changes a workflow activity, THE repository SHALL make
   that run's activity dispatch rows equal its wanted rows in the same atomic write.
2. WHEN an attempt is scheduled for a future retry time, THE repository SHALL retain
   its wanted row with that committed eligibility time.
3. WHEN start, cancellation resolution, pause, removal or close makes a row unwanted,
   THE committing transition SHALL remove that row atomically.
4. WHEN reset, unpause, options, priority or versioning changes a wanted row, THE
   committing transition SHALL replace its superseded metadata atomically.
5. IF a commit conflicts, loses its fence, fails or is refused for physical limits,
   THEN THE repository SHALL expose no partial activity-row write set.
6. THE derivation SHALL use committed run state without live registry reads,
   publication side effects or a second durable copy of activity input/header.
7. THE design SHALL define derivation for every relevant activity/run lifecycle
   state, including legacy missing timing data, without treating row absence as
   proof that an otherwise actionable activity is intentionally undispatchable.

### Requirement 2: Discovery traverses complete due ranges

**User Story:** As a worker operator, I want all compatible due activities to be
reachable, so that a retained first page cannot hide later work.

#### Acceptance Criteria

1. WHILE compatible poll demand exists at an active queue home, THE runtime SHALL
   discover its due activity rows across all execution homes.
2. THE storage listing SHALL order every range by a stable total key with exclusive
   continuation, including tied eligibility times and rows whose cursor row is deleted.
3. WHEN a scheduler slice ends before exhaustion, THE discoverer SHALL retain the
   last examined position for the next slice of that pass.
4. WHEN a row is duplicate, incompatible, stale, temporarily suppressed or fails
   per-row preparation, THE pass SHALL remain able to examine later rows.
5. WHEN a pass finishes, THE next pass SHALL begin at the head with a fresh cutoff.
6. WHEN rows are inserted or rescheduled behind a continuation, THE next pass SHALL
   consider them without requiring ownership change or a publication notification.
7. IF storage reading fails, THEN THE discoverer SHALL retain durable work and
   schedule a bounded retry without declaring successful exhaustion.
8. WHILE a finite due set remains stable and compatible demand, capacity, storage
   availability and fair scheduling persist, THE discoverer SHALL eventually
   examine every row in that set.
9. THE queue-home discovery path SHALL perform no activity pause/start transition
   or durable row deletion under a foreign execution home's ownership.
10. THE edge SHALL route each activity poll, and `DescribeTaskQueue` for an activity
    queue, to the queue home.

### Requirement 3: Offers are bounded and disposable

**User Story:** As a cluster operator, I want queue memory bounded independently of
backlog size, so that rediscovery can replace volatile delivery state safely.

#### Acceptance Criteria

1. THE runtime SHALL bound ready offers, in-flight offers, demand registrations,
   discovery concurrency and work per scheduler slice by explicit finite limits.
2. WHEN an incarnation is ready or in flight, THE broker SHALL suppress duplicate
   admission of that same incarnation within the acquisition generation.
3. WHEN a start is temporarily refused, a poll is cancelled, or an offer expires,
   THE runtime SHALL release its volatile capacity without deleting wanted work.
4. WHEN a broker restarts or all offers are lost, THE queue home SHALL rediscover
   wanted due activities without draining or rebuilding a second persistent queue.
5. WHEN a queue-home acquisition is retired, THE runtime SHALL retire its offers,
   registrations and in-flight work without affecting a replacement generation.
6. WHILE a range has no poll demand, THE runtime SHALL avoid unbounded scanning or
   accumulation of offers for that range.
7. WHEN offers are admitted or selected, THE broker SHALL preserve existing
   activity priority, fairness, rate-limit and worker-shutdown policy.

### Requirement 4: The execution owner starts each incarnation safely

**User Story:** As an activity worker, I want a poll to start work wherever its run
lives, so that independent queue and execution placement is functional.

#### Acceptance Criteria

1. WHEN a queue-home poll selects an activity owned elsewhere, THE runtime SHALL
   obtain the start result from the current execution owner over the internal path.
2. WHEN the owner processes a start, THE runtime SHALL revalidate authoritative run,
   activity, incarnation, pause, eligibility, routing and workflow-rule state.
3. WHEN a start commits, THE repository SHALL atomically apply its state/history,
   dispatch-row removal and existing derived effects under the execution-home
   fence and expected transition sequence.
4. IF competing polls or delayed offers target the same incarnation, THEN THE
   authoritative state SHALL admit at most one start for that incarnation.
5. IF an offer is stale or incompatible, THEN THE poll path SHALL consume that
   candidate internally without returning it as a successful worker task.
6. WHEN a committed start reply is lost and the same internal request is retried
   while that started incarnation remains current, THE owner SHALL return the
   existing start result without another transition or changed start timestamp.
7. WHEN the started incarnation has resolved or been superseded, THE owner SHALL
   refuse replay as a current task even if an old request identity is presented.
8. WHEN a peer call is ambiguous or its owner moves, THE caller SHALL keep its
   request identity across bounded retries within the original poll lifetime.
9. WHEN the caller abandons an ambiguous committed start, THE existing activity
   timeout/retry lifecycle SHALL remain responsible for recovery; the dispatch
   layer SHALL NOT manufacture an unstarted copy of that incarnation.
10. THE owner SHALL complete response-size validation and required immutable input
    reads before committing a new start.
11. THE new transport SHALL preserve the contract policy's authentication,
    authorization, namespace binding, size and cancellation limits.

The same-request behavior is scoped to this activity start path and follows
`service/history/api/recordactivitytaskstarted/api.go @ v1.31.0`; it does not amend
the separately deferred workflow-start request-ID work.

### Requirement 5: Acquisition repairs before serving

**User Story:** As a cluster operator, I want ownership transfer to restore the
durable activity representation, so that old or incomplete rows do not strand work.

#### Acceptance Criteria

1. WHEN an execution home is acquired, THE runtime SHALL keep it non-serving until
   its activity dispatch reconciliation and required timeout reconstruction succeed.
2. THE reconciliation SHALL enumerate both candidate run state and existing
   activity rows, including orphan/stale rows outside the recovery-candidate set.
3. WHEN committed state wants a missing or incorrect row, THE repair SHALL create
   or replace it from current authority, including future retries.
4. WHEN a row is unwanted, THE repair SHALL remove only the observed obsolete
   version without deleting a concurrent replacement.
5. WHEN repair writes, THE repository SHALL serialize the state/row decision with
   competing run writes under the acquisition's captured durable fence.
6. WHEN repair conflicts, THE runtime SHALL retry from fresh authority within a
   finite budget while retaining the original acquisition identity and fence.
7. IF any page, decode, write or retry budget fails, THEN THE home SHALL remain
   non-serving until acquisition retry or failure handling resolves it.
8. WHEN an acquisition is superseded, THE runtime SHALL prevent its delayed repair,
   tracking publication and completion from changing the replacement's serving state.
9. WHEN repaired timeout tracking is already overdue, THE runtime SHALL make it
   eligible at the first timeout-processing opportunity after serving begins.
10. THE repair SHALL use bounded pages and bounded DSQL write units without
    weakening existing large-value, bulk-write or lease-fencing contracts.

### Requirement 6: Preserve activity lifecycle and worker-visible behavior

**User Story:** As an SDK user, I want durable delivery to preserve activity
semantics, so that reconciliation does not change workflow results.

#### Acceptance Criteria

1. WHEN retry backoff has not elapsed, THE delivery path SHALL NOT start that
   attempt through either ordinary or eager delivery.
2. WHEN a rule is created after publication, THE execution owner SHALL enforce the
   existing retry-publication and start-time rule contracts before a worker starts.
3. WHEN pause, unpause, reset, cancellation, options or retry races with an old
   offer, THE owner SHALL use current state and preserve the existing stamp and
   timeout semantics of the corresponding activity lifecycle transition.
4. WHEN deployment routing changes, THE delivery path SHALL preserve pinned,
   independently versioned, auto-upgrade and dispatch-revision start rules verified
   in `recordactivitytaskstarted/api.go @ v1.31.0`.
5. WHEN an ordinary poll or eager claim succeeds, THE response SHALL preserve every
   existing started-activity field and the exact scheduled input/header values.
6. WHEN activity start is transient or buffered by a running workflow task, THE
   implementation SHALL preserve its existing history materialization contract.
7. WHEN a deployment first gains a poller, THE queue-home discovery path SHALL find
   compatible activity work previously left on unversioned routing without a
   destructive legacy backlog drain.
8. WHEN activity timeout processing is due, THE retirement of reconciliation and
   backlog loops SHALL leave timeout scanning and deadline reconstruction intact.

### Requirement 7: Cut over and retire duplicate delivery mechanisms

**User Story:** As a maintainer, I want one durable activity discovery mechanism,
so that two backlog implementations cannot disagree about outstanding work.

#### Acceptance Criteria

1. WHEN the new path is enabled, THE runtime SHALL stop activity backlog writes,
   grace demotion, destructive drains and per-execution-home dispatch reconciliation.
2. THE design SHALL inventory every producer, consumer, sampler and migration use
   of activity dispatch/backlog before removing or redirecting it.
3. THE rollout SHALL require all old writers stopped before the replacement serves
   and repairs existing runs; mixed old/new writers SHALL NOT be claimed safe.
4. WHEN legacy backlog entries are retired, THE implementation SHALL first establish
   authoritative row coverage and then dispose of those copies idempotently in
   bounded work without treating them as authority for missing run state.
5. THE schema plan SHALL respect the locked migration baseline and specify any new
   indexes, compatibility decoding and safe physical table retirement explicitly.
6. THE change record and operator documentation SHALL state rollout prerequisites,
   rollback restrictions, repair failure behavior and removed delivery settings.
7. THE implementation SHALL preserve existing workflow dispatch, outbound intent,
   standalone activity and projection ownership contracts.

### Requirement 8: Verification demonstrates safety and eventual delivery

**User Story:** As a reviewer, I want executed evidence for storage and distributed
delivery, so that memory-only tests cannot conceal a DSQL or ownership gap.

#### Acceptance Criteria

1. THE design SHALL map each requirement to executable correctness properties and
   required property-based test tasks with independent expected results.
2. THE verification SHALL reproduce the retained-full-head-page failure and prove
   completion with more than one page, tied times, skipped prefixes, deleted cursor
   rows, backdated insertions, future retries and temporarily unavailable storage.
3. THE shared memory/DSQL contract suite SHALL verify activity derivation, atomic
   replacement/removal, cursor completeness, conditional cleanup and fenced repair.
4. THE distributed tests SHALL separate queue and execution owners and exercise
   concurrent polls, dropped publication, broker restart, lost start replies,
   cancelled polls, stale offers, both kinds of owner handoff and superseded repair.
5. THE preservation tests SHALL cover eager delivery, workflow rules, retry timing,
   lifecycle races, deployment transitions, priority/fairness/rates and complete
   activity poll payloads through the real internal transport boundary.
6. THE DSQL verification SHALL exercise production-shaped discovery queries and
   bounded acquisition repair beyond one page, recording query-plan evidence for
   the intended indexes and distinguishing foreground from background read counts.
7. THE tests SHALL use synchronization or controlled clocks rather than explicit
   sleeps to establish race order and failure windows.
8. THE delivery receipt SHALL record source/head identity, exact test selections,
   actually executed live checks, failures/skips and temporary-resource cleanup.
9. THE PR SHALL pass the repository's complete enforced-command bar and contain an
   unreleased change record before it is presented for exact-head merge approval.

## Review Status

Requirements approved on 1 November 2026, with explicit approval to prepare design
and tasks together. Ground truth inspected at the baseline and pinned server tag
above. No activity implementation, schema migration, reproducer execution or live
verification is claimed. The joint design/tasks review covers physical indexes,
bounded budgets, start-replay encoding, routing ranges and the stopped-upgrade/legacy
disposal procedure.
