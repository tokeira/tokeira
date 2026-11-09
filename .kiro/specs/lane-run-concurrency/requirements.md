# Requirements Document: Concurrency Across Runs Within a Lane

## Introduction

A lane works on one run at a time. It only needs to keep each run's own work one at a time.

Every command that changes a run goes through a lane. The runtime picks the lane from the run key, so many
unrelated runs share each lane. Today a lane is one Tokio task that takes one activation at a time: it loads the
run, applies the kernel, commits, publishes the dispatch batch and replies, and only then looks at the next
command. While run A waits on its DSQL round trip, runs B and C on the same lane wait too, even though their
writes touch different rows and could commit at the same moment.

Serialising one run's commands is what keeps that run's transitions in order and keeps its commits from
racing each other on optimistic concurrency control. Serialising *different* runs adds no guarantee: two runs
on different lanes already commit concurrently today, and runs from one shard already spread across lanes
([dsql-throughput-optimization](../dsql-throughput-optimization/requirements.md), Requirement 9). The lane's
cross-run serialisation only costs latency and caps a node at one in-flight activation per lane, however
many commit connections it holds.

This spec keeps per-run serialisation and lets different runs on one lane proceed concurrently. It bounds the
concurrent work by the node's DSQL commit capacity, makes every lane and run queue bounded with a fast refusal
as the overload signal, and moves the signal path's admission ahead of its run-state reads.

It builds on these specs and changes them only where it says so:

- [runtime-lane-occ-retry](../runtime-lane-occ-retry/requirements.md): the OCC retry loop, same-run
  coalescing, and publication after commit stay. Its Requirement 3.4 (two runs on one lane never interleave
  kernel invocations) and Requirement 2.6 (a run that reaches its drain bound yields the whole lane) are
  replaced by Requirements 1 and 2 here.
- [dsql-throughput-optimization](../dsql-throughput-optimization/requirements.md): run-key lane routing
  (Requirement 9) is unchanged. The lane-local cache (Requirement 8) keeps its contract; Requirement 3 here
  makes it safe for concurrent runs.
- [shard-aware-lane-routing](../shard-aware-lane-routing/requirements.md): its shard-to-lane function is
  retained only for sweep enumeration. Command routing is by run key, so this spec doesn't depend on it.
- The lane carry-over fix (`spawn_lane_with_id` in
  [lane.rs](../../../crates/tokeira-runtime/src/lane.rs)): a lane never sends to its own channel, and commands
  run in arrival order. Both rules stay. The carry-over slot itself disappears, because a command for another
  run no longer stops anything.

**Behaviour authority:** Temporal server **v1.31.0**. It serialises the work of one workflow execution and
runs different executions concurrently (see *Evidence*). Below the bounds this spec sets, no reply changes.
At the bounds, refusals take the shape v1.31.0 gives the matching condition.

**Depends on an open decision:** which callers, if any, are refused before others when a lane nears its
budget belongs to a separate admission decision, not yet made. This spec admits every caller against the
same budgets and reserves no headroom for anyone. The admission decision plugs into the one reservation step
Requirement 6 defines.

## Glossary

- **Run**: one workflow execution, identified by its run key.
- **Lane**: one of the node's `lane_count` command executors. A run always routes to the same lane.
- **Command**: one unit of work for one run, such as a signal, a timer firing, an activity resolution, a
  workflow-task completion, or an outbound metadata commit.
- **Activation**: one turn of work for one run: it takes commands from the front of that run's queue, one at a
  time, and for each one loads or reuses the run's state, applies the kernel, commits (retrying on OCC
  conflict), publishes the dispatch batch, and replies.
- **Run queue**: the commands for one run that the lane has accepted and not yet started, in arrival order.
- **Ready run**: a run with a non-empty run queue and no activation in flight.
- **Activation slot**: permission to run one activation. Slots are shared by every lane on the node.
- **Node activation bound (`N`)**: the number of activation slots on a node.
- **Commit permits**: the `Commit` class permits of the DSQL connection director
  ([connection.rs](../../../crates/tokeira-storage/src/dsql/connection.rs), `ClassBudgets`). Every DSQL
  commit holds one.
- **Connection ceiling**: the reservoir's configured warm-connection target, which controller budgets can
  lower but never raise (`reconfigure_target` in
  [reservoir.rs](../../../crates/tokeira-storage/src/dsql/reservoir.rs)).
- **Lane budget (`L`)**: the most commands one lane holds accepted but not started, summed over its runs.
- **Run share (`R`)**: the most commands one run holds accepted but not started.
- **Submit reservation**: a counted claim on one place in a lane's budget and one place in a run's share,
  taken before the command is built and released when the command starts or the claim is dropped.
- **Busy-workflow refusal**: RESOURCE_EXHAUSTED with cause BUSY_WORKFLOW, scope NAMESPACE, message
  `Workflow is busy.`
- **Overloaded refusal**: RESOURCE_EXHAUSTED with cause SYSTEM_OVERLOADED, scope SYSTEM.
- **Lane cache**: the lane's cache of loaded run images. It saves reloads and is never authority.
- **Cache generation**: the shard owner's counter that changes on every acquisition and relinquishment. An
  image read under an older generation must not be committed against.
- **Serving gate**: the per-shard read/write barrier that lets acquisition repair wait for admitted writers
  ([serving_gate.rs](../../../crates/tokeira-runtime/src/serving_gate.rs)).

## Target State

- One activation per run at a time, commands in arrival order, same-run coalescing up to
  `max_drain_per_activation`, and draining stops at the first failed command, as today.
- Activations for different runs on one lane run concurrently, up to the node activation bound.
- `N = max(lane_count, C)`, where `C` is the commit-permit allocation at the connection ceiling. A repository
  with no commit budget (the in-memory store) gives `N = lane_count`. No configuration option is added.
- Each lane holds at most `L` accepted-but-unstarted commands, and each run at most `R = L / 2`. `L` is the
  lane's existing mailbox capacity (1024). A submit past either bound is refused at once: past the run share
  with the busy-workflow refusal, past the lane budget with the overloaded refusal.
- Nobody waits for lane capacity. Internal submitters treat a refusal as transient and leave durable state to
  drive the retry.
- The signal path takes its submit reservation as soon as it knows the run key, before any run-state read.
- Shard handoff, recovery and shutdown keep their contracts: every accepted command gets exactly one reply,
  and no command commits twice.
- Out of scope: lane routing, the number of lanes, the kernel, the storage schema, and the admission priority
  decision.

## Evidence From Current Code

Code inspected at `e8258fb9`. Function names are the stable anchors.

| What | Where |
| --- | --- |
| A lane is one task. It takes a message, runs one activation to completion, then takes the next. | `spawn_lane_with_id` in [lane.rs](../../../crates/tokeira-runtime/src/lane.rs) |
| An activation holds the lane through commit, publish and reply, and drains only same-run commands; a command for another run ends the drain and becomes the carry-over. | `run_activation_with_cache` |
| The channel is bounded at 1024 and every submit `send(...).await`s, so a full lane parks its submitters without limit. | `spawn_lane_with_id`, `LaneHandle::submit_message` |
| Identity lookups and outbound metadata commits have their own channels, served between activations so they don't starve. | `resolve_identity_request`, `commit_outbound_metadata` |
| The cache is lane-local. The first reader to notice a new cache generation clears every entry; a commit reloads only if *its own* check cleared the cache. | `LaneCache::synchronize_generation`, `handle_message_with_successor` |
| A start that asks for a parked poll reserves it when its lane takes the start, not when it is submitted. | `reserve_start_poller`, `hand_over_start_poller` |
| Writers hold a serving-gate read permit through commit and post-commit bookkeeping; repair takes the write side. | `admit`, `quiesce` in [serving_gate.rs](../../../crates/tokeira-runtime/src/serving_gate.rs) |
| A DSQL commit takes a `Commit` class permit. The class gets half the pool above the minimum, and the target follows controller budgets up to the configured ceiling. | `ClassBudgets::acquire`, `default_allocations`, `apply_connection_budget` in [connection.rs](../../../crates/tokeira-storage/src/dsql/connection.rs) |
| A commit locks only the run's own rows (`workflow_hot`, and `current_execution` for its workflow id); the lease check is a read. | `run_repository/commit.rs`, `check_fence` in [run_repository](../../../crates/tokeira-storage/src/dsql/run_repository/) |
| The controller's lane pressure reports queue depth and at most one active actor per lane. | `from_runtime_components` in [membership.rs](../../../crates/tokeira-runtime/src/membership.rs) |

The signal path reads before it reaches the lane. In order:

1. The edge resolves the run key from the workflow id and run id (`resolve_execution_run_key` in
   `signal_workflow_execution`, [workflow_service.rs](../../../crates/tokeira-edge/src/workflow_service.rs)).
2. The runtime adapter loads the whole run to rebuild an execution reference
   (`execution_for_run`, [runtime_adapter.rs](../../../crates/tokeira-edge/src/grpc/runtime_adapter.rs)).
3. The runtime resolves the run key again (`signal_run`,
   [lifecycle.rs](../../../crates/tokeira-runtime/src/runtime/lifecycle.rs)).
4. For a run in close-attempt tracking, it loads the run again, and may read the applied-request record.
5. Admission resolves the execution home through the lane's identity lookup, which loads the run on a cache
   miss (`command_home`, `shard_id_for` in [commit.rs](../../../crates/tokeira-runtime/src/runtime/commit.rs)).
6. Only then does the submit reach the lane, which today waits for room.

A signal to a full lane therefore pays a run-key lookup, up to three run loads, and a second lookup before it
learns anything.

Temporal v1.31.0 ground truth:

| What | Where |
| --- | --- |
| Each workflow execution's context carries a semaphore of weight one, and `Lock` acquires it: one holder per execution. | `NewContext` and `ContextImpl.Lock`, `service/history/workflow/context.go @ v1.31.0` |
| Contexts live in a host-level cache keyed by namespace, workflow id, run id, archetype and shard; callers for different executions take different locks and proceed concurrently. | `getOrCreateWorkflowExecutionInternal`, `service/history/workflow/cache/cache.go @ v1.31.0` |
| Lock waits are bounded: API callers until their deadline less 500 ms, other callers by `history.cacheNonUserContextLockTimeout` (500 ms). A timed-out wait returns `ErrResourceExhaustedBusyWorkflow`. | `lockWorkflowExecution`, `cache.go @ v1.31.0`; `common/dynamicconfig/constants.go @ v1.31.0` |
| `ErrResourceExhaustedBusyWorkflow` is RESOURCE_EXHAUSTED, cause BUSY_WORKFLOW, scope NAMESPACE, message `Workflow is busy.` | `service/history/consts/const.go @ v1.31.0` |
| A host cache full of locked entries refuses with RESOURCE_EXHAUSTED, cause SYSTEM_OVERLOADED, scope SYSTEM. | `ErrCacheFull`, `common/cache/lru.go @ v1.31.0` |
| Internal task processing treats a busy-workflow refusal as transient and reschedules the task. | `Nack`, `service/history/queues/executable.go @ v1.31.0` |

## Requirements

### Requirement 1: Per-run order

**User Story:** As a workflow author, I want my run's commands applied one at a time in the order they
arrived, so that concurrency elsewhere never reorders or races my run's history.

#### Acceptance Criteria

1. THE Lane SHALL run at most one activation per run at any instant.
2. THE Lane SHALL start a run's commands in the order the lane accepted them.
3. WHEN an activation finishes a command and its run queue is not empty, THE Lane SHALL take the next command
   for the same run within that activation, up to `max_drain_per_activation` commands in total.
4. IF a command in an activation fails, THEN THE Lane SHALL end that activation after replying to the failed
   command.
5. WHEN an activation ends and its run queue is not empty, THE Lane SHALL make the run ready again behind every
   run already ready on that lane.
6. THE Lane SHALL NOT send a message to its own input.
7. THE Lane SHALL apply every command of an activation to the state left by the previous command of that
   activation.

### Requirement 2: Concurrency across runs

**User Story:** As an operator, I want unrelated runs on one lane to proceed together, so that one run's
round trips don't add to another run's latency.

#### Acceptance Criteria

1. WHILE an activation slot is free, THE Lane SHALL start an activation for a ready run without waiting for
   another run's activation to end.
2. WHILE run A's activation waits on storage, THE Lane SHALL complete commands for other runs on the same lane,
   provided activation slots are free.
3. THE Lane SHALL start activations for its ready runs in the order the runs became ready.
4. THE Lane SHALL keep accepting and queueing commands while every activation slot is taken.

### Requirement 3: A lane cache safe for concurrent runs

**User Story:** As a runtime developer, I want the lane cache to stay a correct optimisation when several runs
of one lane are in flight, so that no commit uses an image it shouldn't.

#### Acceptance Criteria

1. THE Lane cache SHALL keep the contract of dsql-throughput-optimization Requirement 8: it is bounded by
   `cache_max_entries` and `cache_idle_timeout`, it is evicted on OCC conflict, and storage OCC stays the
   conflict detector.
2. THE Lane cache SHALL record with each image the cache generation under which it was read.
3. WHEN an activation is about to commit and the shard owner's cache generation differs from its image's,
   THE Lane SHALL reload the run before committing.
4. THE Lane cache SHALL NOT return an image recorded under an older cache generation.
5. WHEN an identity lookup loads a run, THE Lane SHALL install the image only if the cache holds no entry for
   that run and no activation for it is in flight.
6. WHILE an activation for a run is in flight, THE Lane SHALL answer identity lookups for that run from the
   cached entry or from storage, without waiting for the activation.
7. WHEN a commit conflicts, THE Lane SHALL evict only the conflicting run's entry.
8. THE Lane SHALL NOT hold the cache's lock across an await.

### Requirement 4: Existing lane machinery keeps its contracts

**User Story:** As a runtime developer, I want every mechanism that relies on the lane to behave as it does
today, so that concurrency changes only who waits for whom.

#### Acceptance Criteria

1. THE Lane SHALL retry an OCC conflict within the command's own activation, up to `max_occ_retries`, as
   runtime-lane-occ-retry Requirement 1 specifies.
2. THE Lane SHALL run an outbound metadata commit as a command in its run's queue, in arrival order with that
   run's other commands.
3. WHEN a start asks for a parked poll, THE Lane SHALL reserve the poll only after the start's activation holds
   an activation slot.
4. THE Lane SHALL keep the fences of outbound, observed and activity-timeout submissions: each is validated
   against the state its commit uses.
5. THE Lane SHALL publish a command's dispatch batch and finish its post-commit bookkeeping before replying to
   that command.
6. THE Lane SHALL hold a serving-gate read permit for each command from admission through its post-commit
   bookkeeping.
7. WHEN a command commits a run together with its successor, THE Lane SHALL keep the successor commit's
   current contract; the successor's own commands still serialise on the successor's lane.

### Requirement 5: The bound on concurrent activations

**User Story:** As an operator, I want the node's concurrent work bounded by the database capacity it
actually holds, so that more concurrency never means unbounded memory or a slower handoff.

#### Acceptance Criteria

1. THE Runtime SHALL hold at most `N` activations in flight across all lanes of a node.
2. THE Runtime SHALL derive `N` as `max(lane_count, C)`, where `C` is the commit-permit allocation at the
   connection ceiling.
3. WHERE the repository reports no commit budget, THE Runtime SHALL use `N = lane_count`.
4. THE Runtime SHALL fix `N` when it builds its lanes and SHALL NOT read it from configuration.
5. THE Storage SHALL keep gating every DSQL commit on a commit permit, so the live commit budget still bounds
   concurrent commits when a controller lowers it below `C`.
6. THE Runtime SHALL count an activation against `N` from before it takes its first command until after it
   has handed its run back.

### Requirement 6: Bounded submit as the overload signal

**User Story:** As a client, I want an overloaded node to tell me so at once, so that I can back off instead of
waiting on a queue that never drains in time.

#### Acceptance Criteria

1. THE Lane SHALL hold at most `L` accepted-but-unstarted commands, where `L` is the lane's existing mailbox
   capacity.
2. THE Lane SHALL hold at most `R = L / 2` accepted-but-unstarted commands for any one run.
3. IF a submit would take a run past `R`, THEN THE Lane SHALL refuse it with the busy-workflow refusal.
4. IF a submit would take a lane past `L` and its run is within `R`, THEN THE Lane SHALL refuse it with the
   overloaded refusal.
5. THE Lane SHALL decide a submit reservation without awaiting.
6. WHEN a command starts, THE Lane SHALL release its reservation.
7. WHEN a reservation is dropped without a command, THE Lane SHALL release it.
8. THE Edge SHALL map the busy-workflow refusal to RESOURCE_EXHAUSTED with a `ResourceExhaustedFailure` detail,
   cause BUSY_WORKFLOW, scope NAMESPACE, and message `Workflow is busy.`
9. THE Edge SHALL map the overloaded refusal to RESOURCE_EXHAUSTED with a `ResourceExhaustedFailure` detail,
   cause SYSTEM_OVERLOADED, and scope SYSTEM.
10. WHEN an internal submitter is refused, THE Runtime SHALL leave the work recoverable from durable state, and
    the submitter's existing retry or rescan SHALL deliver it later.
11. THE Lane SHALL admit every caller against the same `L` and `R`, until the admission decision defines
    otherwise.

### Requirement 7: Admission before reads

**User Story:** As an operator, I want a refused signal to cost almost nothing, so that overload doesn't
multiply into database reads.

#### Acceptance Criteria

1. WHEN the signal path has resolved the run key, THE Runtime SHALL take the signal's submit reservation before
   any run-state read.
2. IF the reservation is refused, THEN THE signal path SHALL return the refusal having issued no read other
   than the run-key resolution.
3. WHEN the signal path holds a reservation and returns early (a duplicate, a closing workflow, a missing run),
   THE Runtime SHALL release the reservation.
4. WHEN the signal path submits its command, THE Runtime SHALL spend the reservation it took, not a new one.

### Requirement 8: Shard handoff, recovery and shutdown

**User Story:** As an operator, I want shard moves and restarts to lose and duplicate nothing with many
activations in flight, so that concurrency doesn't weaken durability.

#### Acceptance Criteria

1. THE Lane SHALL send exactly one reply for every command it accepted.
2. THE Runtime SHALL commit any command at most once, across any sequence of shard acquisitions and
   relinquishments.
3. WHEN a command's shard is no longer owned when its activation admits it, THE Lane SHALL answer it with the
   not-shard-owner error.
4. WHEN acquisition repair quiesces a shard, THE serving gate SHALL wait for every in-flight activation already
   admitted for that shard, on every lane.
5. WHEN recovery submits work and is refused, THE recovery path SHALL leave that work for its next pass.
6. WHEN every handle to a lane is dropped, THE Lane SHALL end only after its run queues are empty and its
   in-flight activations have ended.
7. IF an activation task ends without finishing, THEN THE Lane SHALL mark its run idle so later commands for
   that run still run.

### Requirement 9: Observability

**User Story:** As an operator, I want lane metrics that describe concurrent lanes truthfully, so that I can
see backlog and overload.

#### Acceptance Criteria

1. THE Runtime SHALL report each lane's accepted-but-unstarted command count as
   `tokeira_runtime_lane_queue_depth`.
2. THE Runtime SHALL report each lane's in-flight activation count as a gauge.
3. THE Runtime SHALL count refusals by lane and refusal kind.
4. THE Runtime SHALL report each lane's in-flight activation count as the lane pressure's active actors in the
   controller heartbeat.
5. THE Runtime SHALL stop emitting `tokeira_runtime_lane_carry_overs_total`, which no longer measures anything.

### Requirement 10: No observable change below the bounds

**User Story:** As an SDK user, I want the same answers as before, so that the change is invisible unless the
node is overloaded.

#### Acceptance Criteria

1. WHILE no lane or run is at its bound, THE Runtime SHALL return the same reply for every command as the
   serial lane returns for the same per-run command sequence.
2. THE Runtime SHALL keep every existing refusal (growth limits, signal limits, closing workflow, not shard
   owner) unchanged in shape and precedence.
