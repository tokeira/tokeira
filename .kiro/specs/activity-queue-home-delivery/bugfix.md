# Bugfix Requirements: Activity Queue-Home Delivery

## Introduction

A dispatchable workflow activity must reach its queue home promptly even when another node owns its run.
Local publication currently drops that acceleration opportunity. This extends
[activity-dispatch](../activity-dispatch/design.md#offers-and-eager-start) with the disposable push described for
workflow tasks in [queue-home-delivery](../queue-home-delivery/design.md#publication-admission-and-acknowledgement).
It also closes activity-specific retirement and redundant start-read gaps. Temporal server **v1.31.0** is the
behavioral authority; the vendored protos define public wire shape. No public API changes are proposed.

### Terms and target state

| Term | Meaning |
| --- | --- |
| Execution owner | Current serving owner of the run's execution home; only its fenced transition can start work. |
| Queue home | Home derived from namespace and activity queue name; deployment/build coordinates select matching within it. |
| Acquisition | One local ownership generation, its cancellation token, serving gate and renewable deadline. |
| Incarnation | `(run_key, activity_id, schedule_event_id, attempt, stamp)`, as in `activity_offers::incarnation`. |
| Due | The current attempt's committed `dispatch_at` has arrived; pause, rules and start gates still apply. |
| Offer | Bounded, expiring, volatile identity/routing/ordering metadata; neither a start nor a durable acknowledgement. |

Execution owners publish due offers locally or push them to the current remote queue owner. All sources share
activity admission and deduplication. Discovery remains sufficient after lost notifications, process restart or
unsupported peer methods. Recovery reconstructs delivery obligations from existing state. Completion-owned eager
starts never publish their started incarnation. No-peer startup retains local delivery without peer components.

## Evidence and reproduction

These are source observations, anchored by function names. No reproduction or implementation test has run.

| Current fact | Source anchor |
| --- | --- |
| `publish_activity_task` discards `publish_offer`'s admission result and returns success. With configured queue homes, a nonlocal queue returns `Full`; it is not sent elsewhere. | [broker.rs](../../../crates/tokeira-runtime/src/broker.rs), `publish_activity_task`, `publish_offer` |
| Activity discovery reads canonical rows, hydrates each candidate and resolves current routing before `publish_discovered`. | [activity_discovery.rs](../../../crates/tokeira-runtime/src/activity_discovery.rs), `ActivitySource::page`, `ActivitySource::resolve`, `run_slice` |
| Both kinds share `DISCOVERY_PERIOD = 1s`, `PAGE_SIZE = 64`, `PASS_ADMISSIONS = 64` and the eight-slot scheduler. Activity's bound is per slice: `ActivityPass` retains cutoff/cursor and yields immediately until exhaustion. Workflow's pass counts admissions. One second is pacing, not a maximum delivery latency. | [discovery.rs](../../../crates/tokeira-runtime/src/discovery.rs), `run_discovery`, `run_slice`; activity `run_slice`, `ActivityDiscoveryRegistry::complete` |
| Remote activity start exists and returns the committed public poll payload, with independent peer and worker admission. | [edge activity_dispatch.rs](../../../crates/tokeira-edge/src/grpc/activity_dispatch.rs), `RemoteActivityTaskStarter::start`, `ActivityTaskDispatchGrpc::start_activity_task`; [activity_dispatch.proto](../../../proto/tokeira/internal/runtime/v1/activity_dispatch.proto), `ActivityTaskDispatch.StartActivityTask` |
| Normal remote start loads state once to locate the execution home and once to validate/commit. It commits directly, without a lane-cache load; warm and cold each have two logical loads before conflicts. | [runtime activity.rs](../../../crates/tokeira-runtime/src/runtime/activity.rs), `route_activity_start`, `start_forwarded_activity_task`, `start_activity_task_inner` |
| Activity take/expiry reject cancelled acquisitions, but do not check their deadline or current serving generation. Cleanup runs from discovery or broker activity; only the workflow broker has an independent watcher. | `ActivityOffers::take`, `expire`, `retire` in [activity_offers.rs](../../../crates/tokeira-runtime/src/activity_offers.rs); activity `try_take`, `try_claim_activity_offer`, workflow `run_retirement` in `broker.rs` |
| Eager selections start in workflow completion's own transition; `start_completion_activity` removes their enqueue operation. Canonical derivation excludes started activities. | [kernel.rs](../../../crates/tokeira-kernel/src/kernel.rs), `apply_workflow_task_completed`, `TransitionBuilder::start_completion_activity`; [storage activity_dispatch.rs](../../../crates/tokeira-storage/src/activity_dispatch.rs), `derive_activity_dispatch` |

Under uniform independent run/queue placement, about `(N-1)/N` of ordinary publications on `N` nodes need discovery.
This is a placement estimate, not a throughput or latency guarantee.

### Publication inventory

The production calls to `publish_activity_task` are exactly the publisher, eligibility preparation and the two
options-update sites below. Discovery's `publish_discovered` is another admission entry. Broker rerouting moves
existing offers; `republish_activity_queue` and `republish_activity_offer` only wake discovery.

| Dispatchable path | Current behavior and anchor | Required push behavior |
| --- | --- | --- |
| First schedule, including a request not selected for eager execution | `apply_workflow_command` emits `EnqueueActivityTask`; [publisher.rs](../../../crates/tokeira-runtime/src/publisher.rs), `publish_routed`, routes and publishes if already due, otherwise registers eligibility. | Push after commit when due; retain the deferred obligation otherwise. |
| Worker failure retry | `retry_activity_task` calls `commit_activity_retry` in `runtime/activity.rs`. Its direct commit refreshes tracking rather than invoking the publisher. `prepare_activity_dispatch` offers only entries deferred at registration. | Push at eligibility after owner rules; also cover retries already due when registered. |
| Timeout retry | [activity_timeout.rs](../../../crates/tokeira-runtime/src/activity_timeout.rs), `scan_activity_timeouts_once`, uses the same `commit_activity_retry`. | Same delivery obligation; timeout tracking and transitions do not change. |
| Activity unpause/reset; workflow unpause | Kernel `apply_unpause_activity`, `apply_reset_activity`, `apply_unpause_workflow` emit through `enqueue_activity_dispatch` for eligible unstarted activities. Jitter/backoff may defer delivery. Running or retained-paused activities do not become dispatchable. | Push through publication/eligibility for the resulting incarnation, only once due. |
| Options update, including restore | Kernel `apply_update_activity_options` already emits an enqueue operation. Runtime `update_activity_options` additionally publishes immediately or spawns a delayed broker publication. These extra calls construct fallback coordinates without `route_activity_task_queue`. | Use canonical routed publication/eligibility; remove the duplicate immediate/detached delivery branches. |
| Workflow reset successor | Kernel `prepare_reset_fork` calls `apply_workflow_task_failed_for_reset`, which creates activity effects; staged [runtime reset.rs](../../../crates/tokeira-runtime/src/runtime/reset.rs), `publish_reset`, restores tracking through `track_published_outbound` and calls `publish` with no effects after publication. | Arm current successor activity obligations after the publication barrier; push only under Active ownership. |
| Acquisition/restart recovery | [recovery.rs](../../../crates/tokeira-runtime/src/recovery.rs), `RepairAcquisition::repair_activity`, `sweep_shard_inner`, repairs rows and restores eligibility. [activity_eligibility.rs](../../../crates/tokeira-runtime/src/activity_eligibility.rs), `ActivityEligibility::record`, sets `offer_locally` only for future eligibility. Already-due recovery relies on discovery. | Reconstructed due obligations push after activation; future ones push when due. No separate durable recovery queue. |
| Discovery, failed-match republish and late versioned poll | `ActivitySource::resolve` → `publish_discovered`; runtime `republish_activity_offer`, `republish_activity_queue` wake the family. `reprocess_unversioned_activity_backlog` uses `reroute_unversioned_ready_tasks` to move retained offers. | Discovery admits locally, never echoes a push. Retain wake and reroute behavior. |
| Eager returned activity | Completion starts it and removes its enqueue effect; no canonical dispatch row. | Never push that incarnation. A later retry follows the retry row above. |

### Pinned Temporal authority

Every following source was read at the tag. Push means eventual transfer/retry-task execution, not necessarily an
RPC inside the caller's state transaction. Tokeira keeps its existing durable-row mechanism rather than matching's
persisted backlog.

| Tagged source and function | Verified behavior |
| --- | --- |
| `service/history/transfer_queue_active_task_executor.go @ v1.31.0`, `processActivityTask` | Checks mutable state/activity existence, stamp, pause, **task version**, and running execution; derives schedule-to-start timeout, directive and merged priority, releases the workflow lock, then pushes. It has no separate already-started check. |
| `service/history/transfer_queue_task_executor_base.go @ v1.31.0`, `pushActivity` | Sends namespace, workflow/run identity, normal queue, schedule event, schedule-to-start timeout, vector clock, version directive, stamp and priority. Propagates failures; assignment-rule success can update the independent activity build ID. |
| `client/matching/client.go @ v1.31.0`, `AddActivityTask`, `pickClientForWrite`, `Route`, `getClientForTaskQueuePartition` | Clones the request, selects a write partition, uses its RPC name, resolves the partition routing key to a host and uses the cached client. |
| `service/matching/matching_engine.go @ v1.31.0`, `AddActivityTask`; `service/matching/task_queue_partition_manager.go @ v1.31.0`, `AddTask` | Builds task metadata/expiry and attempts sync match in an active cluster, otherwise spools. Failed forwarded sync match returns to the child partition for persistence. Busy-workflow failure can backlog even after a match; other failures propagate as defined by `AddTask`. |
| `service/history/queues/executable.go @ v1.31.0`, `HandleErr`, `Nack` | Classifies stale/safe-to-drop and terminal errors separately; retryable push failures are resubmitted or rescheduled, not acknowledged as successful delivery. |
| `service/history/api/recordactivitytaskstarted/api.go @ v1.31.0`, `Invoke`, `recordActivityTaskStarted`, `processActivityWorkflowRules` | Checks running execution/active namespace and activity existence; refreshes potentially stale mutable state; reads the schedule event; same request ID recovers a started attempt, another receives already-started. Then evaluates first-attempt rules, stamp, version directive and deployment transitions before recording start. Rules skip already-paused or stamp-mismatched activities; the anomalous matching-stamp paused case is logged, not an unconditional pause rejection here. Scheduled pause normally invalidates the old stamp. |
| `service/history/workflow/task_generator.go @ v1.31.0`, `GenerateActivityTasks`, `GenerateActivityRetryTasks`; `service/history/workflow/mutable_state_impl.go @ v1.31.0`, `AddActivityTaskScheduledEvent`, `RetryActivity`, `RegenerateActivityRetryTask` | Ordinary scheduling creates transfer work; retryable failure creates a due retry timer. Paused retry parks without that timer. |
| `service/history/timer_queue_active_task_executor.go @ v1.31.0`, `executeActivityRetryTimerTask`, `processSingleActivityTimeoutTask` | Retry execution checks existence, stamp, pause, not-older attempt, unstarted state, task version and open run, reevaluates rules and pushes. The attempt test is `<`, not equality. Timeout retry calls `RetryActivity`, hence uses the same eventual push. |
| `service/history/workflow/activity.go @ v1.31.0`, `PauseActivity`, `ResetActivity`, `UnpauseActivity` | Scheduled pause advances stamp. Eligible reset/unpause regenerates retry work at its selected time, including jitter; running or keep-paused reset does not immediately redispatch. |
| `service/history/api/updateactivityoptions/api.go @ v1.31.0`, `updateActivityOptions` | Advances stamp, resets timer status and regenerates retry work for scheduled state using `GetNextScheduledTime`; delivery subsequently pushes through the timer executor. |
| `service/history/workflow/mutable_state_impl.go @ v1.31.0`, `ApplyWorkflowExecutionUnpausedEvent`, `reschedulePendingActivities` | Workflow resume generates transfer or future retry tasks; deployment-transition completion reschedules unstarted activities with new stamps. Both reach matching through generated tasks. |
| `service/history/workflow/task_refresher.go @ v1.31.0`, `refreshTasksForActivity`; `service/history/ndc/state_rebuilder.go @ v1.31.0`, `Rebuild` | Rebuild calls task refresh. Running, unstarted, unpaused activities regenerate transfer work for first attempts and retry timers for later attempts; both eventually push. |
| `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go @ v1.31.0`, `handleCommandScheduleActivity`, `handlePostCommandEagerExecuteActivity` | Eager selection bypasses task generation and starts surviving activities after commands, within completion. Closed/cancelled selections are not returned. Workflow pause prevents eager and ordinary generation. |

### Cases to execute after specification approval

Use controlled clocks, discovery gates and commit/transport barriers, never sleeps. These are future cases, not
results. First assert the intended behavior on unchanged `main` and retain the counterexample:

- Separate execution and queue owners, suspend discovery, and make each inventory path due. A parked poll must
  receive the push without a discovery tick. Include retry due at registration, options queue moves and recovery.
- Expire an acquisition deadline without cancellation; a waiting poll must not dequeue its activity offer. Cancel
  another acquisition while discovery/broker work is paused; cleanup must complete independently. Replacement
  acquisitions and another colocated home are preservation controls.
- Count warm/cold remote starts from dequeue. Assert zero queue-home loads and one owner load; the current path
  instead has one at each node. Inject OCC conflicts separately.
- Keep loss/discovery, local delivery and eager nonpublication as preservation controls. Those controls are not
  expected to fail. If a defect case passes unexpectedly, investigate the premise before implementing a fix.

## Bug condition and cause

For a history of operations `X`, let `DueRemote(X)` mean a committed dispatchable activity has a serving execution
owner, a different serving queue owner, compatible demand and available transport/capacity. Let `Ended(X)` mean
an activity acquisition has ended. Let `FreshStart(X)` mean one conflict-free ordinary remote start.

`C(X)` holds when `DueRemote(X)` requires discovery despite a publication/eligibility opportunity, or an
`Ended(X)` acquisition can still expose work or retain it until discovery/another broker operation, or a
`FreshStart(X)` reloads the run at the queue home solely to locate its owner. Causes are local-only admission,
delivery obligations limited to deferred registrations, discovery-driven cleanup and missing volatile home hints.

## Requirements

### Requirement 1: Correction

**User Story:** As an activity worker, I want due tasks delivered promptly to my queue home, so placement does not
add discovery delay to every remote activity.

1. WHEN an inventory path makes an ordinary activity due, THE execution owner SHALL attempt local admission or
   enqueue a bounded push to the selected queue home's current owner without waiting for discovery or start.
2. WHEN eligibility or recovery reconstructs due work, THE runtime SHALL apply the existing owner eligibility/rule
   gates before pushing under its serving acquisition.
3. THE activity sender SHALL reuse the existing placement, channel, scheduling and peer-push policy contracts.
4. THE activity admission set SHALL retain at most one ready/in-flight offer per incarnation within an acquisition
   across local publication, push and discovery.
5. THE activity offer SHALL contain only the bounded identity, routing, ordering and deadline fields in the design.
6. WHEN an acquisition ends, THE activity broker SHALL retire its offers, reservations and discovery work without
   waiting for discovery's timer.
7. WHILE an acquisition is no longer valid, THE activity broker SHALL refuse admission and dequeue for it.
8. THE conflict-free remote activity start SHALL meet the provisional zero queue-home and one execution-owner
   logical run-load budget for both warm and cold caches, subject to the design's measurement gate.
9. THE activity delivery path SHALL use `route_activity_task_queue` and the existing priority/fairness/rate policy
   for every newly published offer.
10. THE activity delivery diagnostics SHALL use bounded source/outcome/reason labels without credentials or per-run
    metric labels.

### Requirement 2: Preservation

**User Story:** As an operator, I want acceleration to remain disposable, so loss, duplication and ownership changes
cannot change activity correctness or the existing deployment contract.

1. THE push and its acknowledgement SHALL leave authoritative state and canonical dispatch rows unchanged.
2. WHILE work remains due under compatible demand and a fair available suffix, THE existing discovery path SHALL
   eventually deliver it despite lost, full, expired or unsupported pushes.
3. WHEN a start is requested, THE execution owner SHALL preserve existing worker authorization, incarnation,
   eligibility, rules, routing, response preflight, receipt replay, OCC and lease-fence checks.
4. WHEN a stale asynchronous result arrives, THE runtime SHALL restrict its cleanup to its captured acquisition
   and offer token.
5. WHEN completion starts an eager activity, THE runtime SHALL publish no offer for that started incarnation.
6. WHERE startup prohibits peers, THE runtime SHALL install no activity push endpoint, sender or credential work.
7. THE change SHALL preserve workflow-task delivery and the eight feature interactions specified in the design.
8. THE implementation SHALL retain existing durable formats, schema, credentials, operator steps and public APIs.

## Boundaries and deferred work

This specifies workflow-backed ordinary activities only. Nexus, standalone activities, speculative workflow
delivery, a separate listener, mutual TLS and activity timeout tracking remain outside scope. Existing priority
comparison, owner-attempt backoff and in-flight lease-duration behavior are independent; this design needs none
changed. Any need for new durable state, a new credential/operator step/public API or changed workflow delivery
requires stopping for review before expanding the specification.
