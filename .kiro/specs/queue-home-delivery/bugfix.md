# Bugfix Requirements Document

## Introduction

Normal workflow tasks must reach their task queue's home promptly after their scheduling transition commits, even
when another node owns the execution. Today that path relies on periodic discovery. Sticky tasks can miss both
their sticky home and their normal fallback home and wait for schedule-to-start expiry.

This specification adds a disposable push, complete durable recovery for sticky pushes and their normal fallback,
prompt offer retirement, and fewer run loads during a remote start. It extends
[workflow-dispatch](../workflow-dispatch/design.md); its acquisition repair and lease fence remain prerequisites.
Temporal server **v1.31.0** is the behavioural authority. This PR specifies future implementation; it contains no
implementation or executed exploration tests.

### Terms and target state

| Term | Meaning |
| --- | --- |
| Execution home | The home derived from a run's namespace and workflow identity; its current owner commits transitions. |
| Queue home | The home derived from the task queue's placement coordinates; its current owner receives polls and offers. |
| Acquisition | One local ownership generation, with its cancellation token and serving gate. |
| Incarnation | `(run_key, logical_seq)`, identifying one scheduled workflow task. |
| Normal task | A durable, non-speculative workflow task; it may be addressed to a sticky queue. |
| Offer | A bounded, expiring, volatile hint. Admission acknowledges no start and changes no authoritative state. |
| Sticky fallback | Delivery to the original normal queue after sticky unavailability; the pending deadline and durable affinity survive. |

The execution owner pushes a normal task to its selected queue home. That home combines local publication, remote
push and discovery in one bounded offer set. A poll starts through the existing execution-owner path. Ordinary
normal-queue discovery remains the backup for non-sticky rows; an execution-home sweep republishes durable sticky
rows, including those whose previous normal fallback was lost. Neither path claims or deletes dispatch rows.

## Evidence and reproduction

The following code was inspected at `a7592f5a9c06ef2686073868892371991ff315b4`. Function names are the stable anchors.

| Evidence | Source |
| --- | --- |
| A normal offer is refused when the publisher lacks its queue home. Sticky availability is observed on the publisher. | [broker.rs](../../../crates/tokeira-runtime/src/broker.rs), `publish_workflow_offer` |
| Periodic discovery uses a one-second period and a 64-admission pass. Exact routing skips the state hydration used by live routing. | [discovery.rs](../../../crates/tokeira-runtime/src/discovery.rs), `run_discovery`, `run_slice`, `WorkflowSource::resolve` |
| Sticky rows are excluded from normal discovery in both stores; sticky polls register no discovery demand. | [workflow_dispatch.rs](../../../crates/tokeira-storage/src/workflow_dispatch.rs), `WorkflowDiscoveryRange::matches`; [DSQL query](../../../crates/tokeira-storage/src/dsql/run_repository/workflow_dispatch.rs), `queue_page_query`; [workflow_task.rs](../../../crates/tokeira-runtime/src/runtime/workflow_task.rs), `poll_workflow_task` |
| Deployment-change fallback exists, but publication and local fallback discard the sticky deadline. | [publisher.rs](../../../crates/tokeira-runtime/src/publisher.rs), workflow-task publication; `publish_workflow_offer` in the broker |
| Remote start already uses pooled peer channels; its credentials come from the current poll. | [dispatch.proto](../../../proto/tokeira/internal/runtime/v1/dispatch.proto); [workflow_dispatch.rs](../../../crates/tokeira-edge/src/grpc/workflow_dispatch.rs), `ForwardedPoll`, `RemoteWorkflowTaskStarter::start` |
| The poll resolver reads state, and the owner separately reads metadata, sticky identity where applicable, and state with statistics. | `resolve_polled_workflow_task_target` in runtime; `start_workflow_task` in edge |
| Offer expiry permits later rediscovery; acquisition retirement currently runs from discovery. | [workflow_offers.rs](../../../crates/tokeira-runtime/src/workflow_offers.rs), `expire`, `retire_home`; `retire_offers` in the broker |

Under roughly uniform, independent execution and queue placement, about `(N-1)/N` of tasks on `N` nodes take the
discovery path. About 50 tasks/second is an estimate for the modelled scenarios, not a universal throughput cap.

The implementation must first reproduce these cases with controlled clocks and barriers, without explicit sleeps:

1. A owns the normal queue home and B owns the execution home. Suspend discovery, schedule on B, and poll A. The
   expected immediate offer is absent; resuming discovery makes it available.
2. The execution owner owns neither the sticky home nor the normal home. A sticky worker has polled recently at its
   home. Schedule, suspend normal discovery and advance a controlled clock: the old path waits for sticky expiry.
3. Start normal and sticky tasks remotely with warm and cold execution caches. Attribute every run load to its
   caller, then force an OCC retry separately. The static minimum is three explicit loads for normal and four for
   sticky starts before cache misses and retries.
4. Cancel a queue-home acquisition while discovery is paused. Check whether ready offers remain visible to a poll
   before the discovery loop wakes.

These are source-derived reproductions to execute later, not claimed test results.

### Temporal ground truth

Vendored wire shape: [TaskQueue](../../../proto/upstream/temporal/api/taskqueue/v1/message.proto) carries `name`,
`kind` and, for sticky queues, `normal_name`. No upstream proto fields change.

The following anchors were read in the local Temporal reference checkout at **v1.31.0**:

| Contract | Tagged server source |
| --- | --- |
| Use the transfer task's queue to select sticky delivery and timeout, release the workflow lock before pushing, retry the original normal queue on `StickyWorkerUnavailable`, and retain stickiness and the sticky timeout as matching TTL. | `service/history/transfer_queue_active_task_executor.go`, `processWorkflowTask`, lines 260–331; `service/history/workflow/mutable_state_impl.go`, `TaskQueueScheduleToStartTimeout` |
| Push to matching and route to the chosen partition owner. | `service/history/transfer_queue_task_executor_base.go`, `pushWorkflowTask`; `client/matching/client.go`, `AddWorkflowTask` |
| A missing sticky partition or no poller within the ten-second availability window is unavailable. A currently waiting poll is not necessary. | `service/matching/matching_engine.go`, `AddWorkflowTask`, `stickyWorkerAvailable`, `stickyPollerUnavailableWindow` |
| A changed current deployment or a build-id redirect makes the sticky destination unavailable. | `service/matching/task_queue_partition_manager.go`, `getPhysicalQueuesForAdd`, deployment and redirect branches |
| In older compatible-version-set routing, a known build that is no longer its set's default also makes sticky delivery unavailable. An unknown build does not trigger this condition. | `service/matching/version_sets.go`, `checkVersionForStickyAdd`, lines 430–448; its call in `task_queue_partition_manager.go` |

The last row is an additional fallback condition, distinct from a redirect rule. All four conditions are required.

## Bug Analysis

### Bug condition

Let `X` contain a committed pending normal task, placement/acquisition generations, poll observations, fault
schedule, cache state and an execution trace. Let `eligible(X)` mean that the task remains pending, the selected
home is serving, the relevant worker is eligible, and capacity and credentials permit the attempt. Define:

```text
C(X) = (eligible(X) and the execution owner is off-home
        and publication provides no push path before discovery/timeout)
    or (a sticky or fallback push is lost and no durable retry path covers its sticky row)
    or (a cancelled home can still supply an offer before the discovery tick)
    or (a successful, conflict-free remote start repeats avoidable run loads)
```

For the last clause, the initial test oracle is more than one logical run load with a warm execution cache, or more
than two with a cold cache. Task 1 measures this provisional target before implementation. If evidence requires
another target, it must explain the necessary read and amend this spec before the fix; eliminating authorization
or conflict reloads is never a justification. Finite observations of latency use a drained deterministic scheduler,
not a guessed wall-clock bound.

### Current Behavior (Defect)

1.1 WHEN publication runs away from a normal queue's home THEN it supplies no remote offer and waits for discovery.

1.2 WHEN sticky availability is known only at a different home THEN publication can choose fallback from the wrong
node's observations, discard its deadline, and leave a sticky dispatch row invisible to normal discovery.

1.3 WHEN a remote workflow task starts THEN separate routing, identity, metadata and target checks reload the run.

1.4 WHEN a queue-home acquisition ends THEN volatile offers can survive until discovery performs retirement.

### Expected Behavior (Correct)

2.1 WHEN a normal task's scheduling transition commits THEN the publisher SHALL attempt bounded delivery to the
selected queue home's current owner, without awaiting discovery or a worker start.

2.2 WHEN local publication, push and discovery race for the same incarnation in one queue-home acquisition THEN
the broker SHALL retain at most one offer for that incarnation within that acquisition's retention interval.

2.3 WHEN offers expire or homes change THEN rediscovery and repeated admission SHALL remain permitted; the execution
owner SHALL commit at most one start per incarnation using authoritative state and the existing lease fence.

2.4 WHEN an old publisher's delayed hint still names the current incarnation THEN the current execution owner SHALL
not reject it solely because its publisher lost ownership; WHEN the incarnation is obsolete or the writer has
lost its execution home THEN it SHALL not commit.

2.5 WHEN sticky delivery is attempted THEN availability SHALL be evaluated at the sticky queue's current home using
the ten-second recent-poller condition, not merely the presence of a waiting poll.

2.6 WHEN any Temporal v1.31.0 sticky-unavailability condition applies THEN delivery SHALL use the original normal
queue with current version routing: missing/recently unpolled sticky partition, changed current deployment,
build-id redirect, or a known build superseded as its compatible version set's default. An unknown build alone
SHALL not trigger the last condition.

2.7 WHEN delivery falls back THEN the offer SHALL retain the pending task's absolute sticky schedule-to-start
deadline and the run SHALL retain durable sticky affinity; repeated offers SHALL not extend that deadline.

2.8 WHEN either a sticky push or its normal fallback is lost, refused, ambiguously acknowledged or interrupted by a
crash THEN recovery SHALL revisit the durable sticky row, re-evaluate routing and availability, and retry delivery.

2.9 WHILE ownership eventually stabilizes, current owners finish acquisition repair, peer credentials and transport
work, recovery and timeout processing progress fairly, eligible workers keep polling, and capacity becomes
available fairly THEN every pending normal task SHALL eventually be offered and startable through push or durable
recovery. Before-deadline sticky delivery additionally requires that these conditions hold soon enough for an
attempt before its absolute deadline; otherwise the existing timeout transition supplies normal delivery.

2.10 WHEN a queue-home acquisition is cancelled or its admission deadline expires THEN its offers SHALL immediately
be ineligible for dequeue/admission; acquisition-driven cleanup SHALL remove them without waiting for discovery.

2.11 WHEN a remote start succeeds without an OCC conflict THEN the path from offer selection through committed
start SHALL provisionally use zero queue-home run loads and at most one execution-owner run load with a warm
cache, or two with a cold cache, under the counting method in [design.md](design.md).

2.12 WHEN validating a remote start THEN the owner SHALL preserve worker authorization, namespace/normal/sticky
queue and version bindings, execution-home derivation, envelope/attachment checks before commit, and the reload
after each write conflict.

2.13 WHEN a background offer RPC arrives THEN it SHALL authenticate a configured peer identity under the separate
push policy in [design.md](design.md); worker/admin permissions or anonymous public access alone SHALL not grant it.

2.14 WHEN push work is queued or received THEN it SHALL obey the message, byte, count, concurrency and deadline
bounds in [design.md](design.md); overload or transport failure SHALL leave durable intent untouched.

2.15 WHEN the sticky recovery query is implemented THEN both repositories SHALL supply bounded, continuing pages
over sticky rows of one execution home; DSQL SHALL use a ready asynchronous index and its actual first/continuation
plans SHALL be verified on live DSQL before completion.

2.16 WHEN delivery fails or falls back THEN bounded diagnostics SHALL distinguish ownership, capacity, transport,
authentication, deadline and sticky-routing outcomes, without credentials or unbounded per-run metric labels.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN a task is published and polled on its execution owner's queue home THEN existing successful delivery,
priority, eager/reserved delivery and worker-visible response semantics SHALL be preserved.

3.2 WHEN queues, offers or RPC replies are lost or duplicated THEN history and fenced transitions SHALL remain the
authority; discovery/repair SHALL remain read-only or use their existing fenced repair contract, respectively.

3.3 WHEN speculative tasks or queries are used THEN their existing delivery, held-update handling, query waiters and
completion/failure forwarding SHALL be preserved; no durable row SHALL be invented for a speculative task.

3.4 WHEN versioning or sticky timeout transitions race with delivery THEN existing routing semantics and pending
deadline reconstruction SHALL be preserved, except for the explicitly corrected sticky delivery conditions above.

3.5 WHEN an acquisition is replaced THEN cleanup from its predecessor SHALL not remove the replacement's offers,
cancel its work or undo its serving gate.

## Boundaries and deferred work

Activities need a separate specification for the complete path. Today `publish_activity_task` in the broker and
`reconcile_due_activity_dispatches_once` in
[activity.rs](../../../crates/tokeira-runtime/src/runtime/activity.rs) refill the execution owner's local broker;
activity polls enforce `ensure_local(route_task_queue(..., TaskKind::Activity))` in
[workflow_service.rs](../../../crates/tokeira-edge/src/workflow_service.rs); activity start requires a local
`owner.write_fence(bundle_id)`. Forwarding an activity offer alone cannot complete that start.

Speculative remote delivery is deferred: `derive_workflow_dispatch` excludes speculative tasks, so this design's
durable recovery cannot protect them. Eager/reserved and query paths remain as they are.

Also deferred: a separate internal listener or mutual TLS; stable request IDs across start retries; restricting
memory storage to one shard; general run-scoped request/reply forwarding; changes to large-value parts, update
admission, command validation, child workflows or unstarted activity failure; single-node ownership claims;
used-version tracking; visibility apply/index/startup copying; pair-commit and successor paths. This design needs
none of those paths changed. If implementation reveals otherwise, return the scope conflict for review.
