# Implementation Plan: Concurrency Across Runs Within a Lane

## Overview

The plan lands in two code PRs, each test-first: every group starts with the test that pins its behaviour.

- **First PR (tasks 1–6):** per-run queues, bounded submit and its refusals, internal submitters that tolerate
  a refusal, and admission before reads on the signal path. Each lane still runs one activation at a time, so
  the change is observable only at the bounds.
- **Second PR (tasks 7–12):** the commit ceiling, a lane cache safe for concurrent runs, node-wide activation
  slots, metrics, and the documentation that follows.

Property tests use `proptest`. Pure-core properties run 256 cases; concurrent properties run 64, each on a fresh
multi-thread Tokio runtime. No test sleeps.

## Tasks

- [x] 1. The pure per-run queue core
  - [x] 1.1 Property test: Property 5 — reservation accounting is exact and refusals don't wait
    - In `crates/tokeira-runtime/src/lane/run_queues.rs`, generate operation sequences over a few run keys
      and small `L`; compare claims, per-run claims and refusal kinds with a reference model.
    - Tag: `// Feature: lane-run-concurrency, Property 5: Reservation accounting is exact and refusals don't wait`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_
    - **DONE.** `reservation_accounting_is_exact` in `lane/run_queues.rs`.
  - [x] 1.2 Property test: Property 6 — ready order and the drain bound
    - Same module and generator; assert a run is ready at most once and never while active, FIFO ready order,
      and re-entry at the back after `finish` with a non-empty queue.
    - Tag: `// Feature: lane-run-concurrency, Property 6: Ready order and the drain bound`
    - _Requirements: 1.3, 1.5, 2.3_
    - **DONE.** `ready_order_and_drain_bound` in `lane/run_queues.rs`.
  - [x] 1.3 Implement `RunQueues<T>` and `LaneRefusal`
    - `L` is the lane's existing mailbox capacity, named once as a constant; `R = L / 2`. Check the run share
      before the lane budget. Remove an entry once it is idle with no claims and an empty queue.
    - _Requirements: 1.2, 1.5, 2.3, 6.1, 6.2, 6.5, 6.6, 6.7, 6.11_
    - **DONE.** `RunQueues` with `LANE_BUDGET = 1024` and the run share `L / 2`; `drain_all` serves a lane
      whose dispatcher has gone (`drain_all_returns_every_queued_item_and_its_claims`).

- [x] 2. Refusal errors and their wire mapping
  - [x] 2.1 Edge tests for both refusals
    - One example per refusal asserting gRPC RESOURCE_EXHAUSTED, the `ResourceExhaustedFailure` cause and
      scope, the busy-workflow message `Workflow is busy.`, and HTTP 429.
    - _Requirements: 6.8, 6.9_
    - **DONE.** `a_busy_run_answers_v1_31_0s_busy_workflow` and
      `an_overloaded_lane_answers_the_system_overloaded_shape` in `grpc/errors.rs`.
  - [x] 2.2 Add `WorkflowBusy` and `LaneOverloaded`
    - In `crates/tokeira-runtime/src/errors.rs`, with `From<LaneRefusal>`, citing `service/history/consts/const.go @ v1.31.0`
      and `common/cache/lru.go @ v1.31.0`. Map them in `crates/tokeira-edge/src/errors.rs` and
      `crates/tokeira-edge/src/grpc/errors.rs` through the existing `resource_exhausted_status` helper.
    - _Requirements: 6.3, 6.4, 6.8, 6.9, 10.2_
    - **DONE.** Runtime errors with `From<LaneRefusal> for anyhow::Error`; `EdgeError::WorkflowBusy` and
      `EdgeError::LaneOverloaded` map through `resource_exhausted_status` and answer HTTP 429.

- [x] 3. The lane on per-run queues
  - [x] 3.1 Rewrite the lane's overload regression tests
    - Rewrite `lane_drains_full_mailbox_with_parked_submitters_in_arrival_order` to fill the lane budget, then
      expect `LaneOverloaded` from the next submit and every admitted reply in arrival order. Add a test that a
      submit past a run's share returns `WorkflowBusy` on its first poll.
    - _Requirements: 1.2, 6.3, 6.4, 6.5_
    - **DONE.** `a_full_lane_refuses_at_once_and_drains_in_arrival_order` replaces the parked-submitter test;
      `a_submit_past_its_runs_share_is_refused_on_its_first_poll`.
  - [x] 3.2 Property test: Property 1 — per-run order holds under concurrency
    - In `crates/tokeira-runtime/src/lane/tests/concurrency.rs`, use the in-memory store behind a test-only
      gated repository that parks chosen runs' commits and records kernel entry and exit per run. Generate
      runs, interleavings and parked commits; assert one activation per run and arrival order per run.
    - Tag: `// Feature: lane-run-concurrency, Property 1: Per-run order holds under concurrency`
    - _Requirements: 1.1, 1.2, 1.7, 4.2_
    - **DONE.** `per_run_order_holds_under_concurrency` in `lane/tests/concurrency.rs`, on the lane test
      repository with per-run commit gates and an overlap detector.
  - [x] 3.3 Implement `LaneShared`, `SubmitReservation`, `LaneItem` and the dispatcher
    - The dispatcher wakes on a capacity-one channel whose closing tells it every handle is gone. It acquires
      a slot from a semaphore the lane is given, pops the first ready run, and spawns its activation into a
      `JoinSet`; on join it calls `finish`. In this PR each lane gets its own one-slot semaphore, which keeps
      today's concurrency.
    - Every existing `LaneHandle` submit method reserves first and keeps its signature.
    - Outbound metadata commits become `LaneItem::OutboundMetadata` in their run's queue.
    - Identity lookups run in the caller's task from the cache or a load there, and install an image only for
      an idle run with no entry.
    - Remove the carry-over slot and the identity and metadata channels. Keep the module doc's rule that the
      lane never sends to its own input.
    - _Requirements: 1.1–1.7, 2.4, 3.5, 3.6, 3.8, 4.1–4.7, 6.5, 6.6, 6.7, 8.1, 8.3_
    - **DONE.** The lane state is one mutex over `RunQueues<LaneItem>` and the cache. Activations reach the
      cache through `CacheAccess`, which remembers the generation of its own image, so the check before a
      commit reloads however the shared cache was cleared. Identity lookups synchronise the generation and
      install only when the cache did not change during their load.
  - [x] 3.4 Reserve the start's parked poll after the slot
    - Keep `a_parked_poll_stays_free_while_its_start_waits_in_the_lane_queue` and the other reserved-poll tests
      green; add a case where the start waits for a slot.
    - _Requirements: 4.3_
    - **DONE.** `a_start_reserves_its_parked_poll_only_once_it_holds_a_slot`.

- [x] 4. Internal submitters treat a refusal as transient
  - [x] 4.1 Tests per internal submitter
    - For the timer, workflow-timeout, workflow-task-timeout, activity-timeout, Nexus-timeout and
      completion-callback scanners, recovery, the publisher's `submit_to_run`, and the outbound driver
      (`submit_outbound`, `submit_observed`, `commit_outbound_metadata`): with the target run at its share,
      the work is still delivered once the run drains.
    - _Requirements: 6.10, 8.5_
    - **DONE.** `a_refused_due_timer_keeps_its_row` (timer scanner and recovery),
      `a_refused_workflow_timeout_is_delivered_on_a_later_pass`, `a_refused_workflow_task_timeout_stays_tracked`,
      `a_refused_activity_timeout_is_delivered_once_the_run_has_room` and
      `a_refused_nexus_timeout_is_delivered_once_the_run_has_room`. The outbound driver retries any error that is
      not a recovery classification, as its injected-failure tests pin; the completion-callback attempt leaves
      its tracking entry for the scanner on any commit error.
  - [x] 4.2 Make any submitter that drops refused work retry from its durable source
    - Cite `Nack`, `service/history/queues/executable.go @ v1.31.0` where the retry needs a reason.
    - _Requirements: 6.10, 8.5_
    - **DONE.** No submitter drops refused work: each keeps its durable row or tracked entry for a later
      pass. The publisher's `submit_to_run` has no caller.

- [x] 5. Admission before reads on the signal path
  - [x] 5.1 Property test: Property 8 — a refused signal reads only its run key
    - Use a counting repository wrapper that records every read by kind; fill the target lane, or the run's
      share, with reservations; generate signals with and without a run id; assert the refusal and the reads.
      Cover early returns (duplicate, closing workflow, missing run) releasing their claim.
    - Tag: `// Feature: lane-run-concurrency, Property 8: A refused signal reads only its run key`
    - _Requirements: 7.1, 7.2, 7.3, 7.4_
    - **DONE.** `a_refused_signal_reads_only_its_run_key` in `runtime/signal_admission_tests.rs`;
      `signal_path_refuses_a_run_at_its_share_at_once` in the edge.
  - [x] 5.2 Reserve before `execution_for_run`
    - Add `TokeiraRuntime::reserve_submit`. `RuntimeAdapter::signal_workflow` reserves first and passes the
      reservation through `signal_workflow` and `signal_run` to `submit_admitted`, which spends it. If the
      runtime resolves a different run key, drop the reservation and reserve for the resolved key.
    - _Requirements: 7.1, 7.2, 7.3, 7.4_
    - **DONE.** The adapter reserves, then calls `TokeiraRuntime::signal_workflow_reserved`;
      `signal_workflow` keeps its signature and reserves in `signal_run` once the run key resolves.

- [x] 6. Checkpoint: first PR green
  - `cargo +nightly fmt --all`; `cargo clippy -p tokeira-runtime --all-targets --locked -- -D warnings`, and
    the same for `tokeira-edge`; the lane, signal-path and edge tests above under `cargo nextest run`.
  - Add one `changed` changelog fragment: an overloaded lane or run answers RESOURCE_EXHAUSTED at once instead
    of making the caller wait.
  - **DONE.** Both crates clippy-clean; the runtime and edge suites pass under nextest.

- [x] 7. The commit ceiling
  - [x] 7.1 Tests for the ceiling
    - `DsqlRunRepository::commit_concurrency_ceiling` equals the `Commit` allocation `default_allocations`
      gives the configured `target_ready`; `InMemoryStore` returns `None`; the `Arc<T>` impl and
      `HistoryNotifyingRepository` forward.
    - _Requirements: 5.2, 5.3_
    - **DONE.** `the_commit_ceiling_is_the_commit_allocation_at_the_configured_target` (which also lowers the
      live budget and checks the ceiling stays), `the_memory_store_reports_no_commit_ceiling` and
      `the_commit_ceiling_is_the_inner_repositorys`.
  - [x] 7.2 Add `RunRepository::commit_concurrency_ceiling` with a `None` default and the forwards
    - _Requirements: 5.2, 5.3, 5.5_
    - **DONE.** `DsqlRunRepository` reports `DsqlConnectionDirector::commit_concurrency_ceiling`, the `Commit`
      share of `default_allocations` at the reservoir's configured target. The memory store reports a ceiling
      only when a test asks for one with `with_commit_concurrency_ceiling`.

- [x] 8. A lane cache safe for concurrent runs
  - [x] 8.1 Property test: Property 7 — the cache never feeds a commit a stale-generation image
    - Generate interleavings of activations, identity lookups, outbound metadata commits and generation
      changes against the gated repository; record the generation each commit's image carried.
    - Tag: `// Feature: lane-run-concurrency, Property 7: The cache never feeds a commit a stale-generation image`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.7_
    - **DONE.** `the_cache_never_feeds_a_commit_a_stale_generation_image` in `lane/tests/cache_generations.rs`
      parks loads, changes the generation under them and stamps each image with the generation it was read
      under.
  - [x] 8.2 Record generations on images and compare before commit
    - Add `generation` to `CachedRun`; replace `&mut LaneCache` in the activation path with a `CacheAccess`
      that locks per operation and remembers the generation it handed out; reload before committing when the
      owner's generation differs. Keep the existing cache tests green.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.7, 3.8_
    - **DONE.** `CachedRun` records its generation. The cache only moves forward and never installs or returns
      an older image (`the_cache_refuses_an_image_from_an_older_generation`). `CacheAccess` tracks the
      generation of the image it hands out and reloads before a commit when the owner's is newer.

- [x] 9. Node-wide activation slots
  - [x] 9.1 Property test: Property 2 — a slow run doesn't delay another run on the same lane
    - Park run A's commit; generate commands for other runs on the same lane; assert all complete while A is
      parked. Fails against one-slot lanes.
    - Tag: `// Feature: lane-run-concurrency, Property 2: A slow run doesn't delay another run on the same lane`
    - _Requirements: 2.1, 2.2, 2.4_
    - **DONE.** `a_slow_run_does_not_delay_another_run_on_the_same_lane` in `lane/tests/concurrency.rs`.
  - [x] 9.2 Property test: Property 3 — the bound on activations is never exceeded
    - Generate lane counts, `N` and workloads with parked commits; track the in-flight count with an atomic
      high-water mark; assert it never passes `N` and reaches `min(N, ready runs)`.
    - Tag: `// Feature: lane-run-concurrency, Property 3: The bound on activations is never exceeded`
    - _Requirements: 5.1, 5.6_
    - **DONE.** `the_bound_on_activations_is_never_exceeded`, over several lanes sharing one semaphore.
  - [x] 9.3 Share one semaphore of `N` slots across the node's lanes
    - `N = max(lane_count, ceiling)`, fixed at construction, with a comment deriving it; standalone
      `spawn_lane` uses `max(1, ceiling)`. On a failed join, mark the run idle and re-ready it if it has work.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 8.7_
    - **DONE.** `node_activation_bound` derives `N`; `TokeiraRuntime` builds one semaphore of `N` slots for all
      its lanes (`the_node_bound_is_the_larger_of_the_lane_count_and_the_commit_ceiling`).
  - [x] 9.4 Property test: Property 4 — no command is lost or duplicated across a shard handoff
    - Interleave submissions with relinquish and re-acquire through `ShardOwner`; assert one reply per
      accepted command and that history holds exactly the `Applied` commands, each once.
    - Tag: `// Feature: lane-run-concurrency, Property 4: No command is lost or duplicated across a shard handoff`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 3.3, 3.4_
    - **DONE.** `no_command_is_lost_or_duplicated_across_a_shard_handoff`, against the memory store's histories.
  - [x] 9.5 Property test: Property 9 — below the bounds, replies match the serial lane
    - Run the same per-run sequences through a one-slot lane and an `N`-slot lane; compare replies and
      committed histories.
    - Tag: `// Feature: lane-run-concurrency, Property 9: Below the bounds, replies match the serial lane`
    - _Requirements: 10.1, 10.2, 4.1, 4.5_
    - **DONE.** `below_the_bounds_replies_match_the_serial_lane`, including runs that were never started.
  - [x] 9.6 Shutdown and abandoned-activation examples
    - Drop every handle with work queued: every reply arrives and the dispatcher ends after its last
      activation. Abort an activation task: the run's later commands still run.
    - _Requirements: 8.6, 8.7_
    - **DONE.** `dropping_every_handle_still_answers_every_queued_command` and
      `an_activation_that_ends_without_finishing_leaves_its_run_runnable`.

- [x] 10. Observability
  - [x] 10.1 Metrics and heartbeat
    - `tokeira_runtime_lane_queue_depth` reports accepted-but-unstarted commands; add an in-flight activation
      gauge per lane and a refusal counter by lane and kind; report in-flight activations as the heartbeat's
      `active_actors`; stop emitting `tokeira_runtime_lane_carry_overs_total`. Extend the metrics and
      membership tests.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_
    - **DONE.** `tokeira_runtime_lane_active_activations` and `tokeira_runtime_lane_refusals_total{lane_id, kind}`
      (`helpers_emit_expected_metrics_and_labels`); the heartbeat reports in-flight activations
      (`the_heartbeat_reports_in_flight_activations_as_active_actors`).

- [x] 11. Align documentation and specs
  - [x] 11.1 Update the lane's module and item docs to the per-run model, citing `ContextImpl.Lock`
    (`service/history/workflow/context.go @ v1.31.0`) for the per-run exclusion
    - _Requirements: 1.1, 2.1_
    - **DONE.** The `lane` module doc, `spawn_lane`, `spawn_lane_with_id` and the scanner's routing notes.
  - [x] 11.2 Amend runtime-lane-occ-retry Requirements 2.6 and 3.4, and its design, to point to this spec; correct
    [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md) and `docs/crates/runtime.md` wherever
    they say a lane serialises different runs
    - _Requirements: 1.5, 2.1_
    - **DONE.** runtime-lane-occ-retry's glossary, Requirements 2.6 and 3.4, and its design's activation loop
      and Properties 5 and 6; 030-runtime-lanes. `docs/crates/runtime.md` already describes per-execution
      serialisation only.
  - [x] 11.3 Changelog fragments: one `changed` for concurrent activations within a lane, one `removed` for
    the carry-over metric
    - _Requirements: 9.5_
    - **DONE.**

- [x] 12. Final checkpoint
  - The crate-scoped commands from task 6, plus `tokeira-storage`; then the live DSQL suite on an ephemeral
    cluster, per the [runbook](../../../docs/testing/dsql-live-suites.md), with commit permits as the
    observed limit. The live check is `tokeira-runtime`'s `dsql_lane_concurrency` integration test
    (`dsql_lane_concurrent_commits_stay_within_commit_permits`): `3 * N` runs on one lane, each started and
    signalled in order, every submit answered and every history whole, and the peak of concurrent commit
    statement windows above one and at most the `Commit` allocation.
  - **DONE.** The crate-scoped commands passed for `tokeira-runtime`, `tokeira-storage` and `tokeira-edge`, and
    so did the full workspace checks. On an ephemeral cluster,
    `dsql_lane_concurrent_commits_stay_within_commit_permits` ran nine runs on one lane with three `Commit`
    permits and made 81 commits; peak concurrent commits reached 3 and never exceeded the permits. Every
    run's history held its start and its eight signals once each, in order. The cluster was deleted
    afterwards.

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1", "2"],
  "4": ["3"],
  "5": ["3"],
  "6": ["4", "5"],
  "7": ["6"],
  "8": ["6"],
  "9": ["7", "8"],
  "10": ["9"],
  "11": ["9"],
  "12": ["10", "11"]
}
```

## Notes

- The first PR changes behaviour only at the bounds: a full lane or run refuses where it used to make callers
  wait. Concurrency arrives in the second PR, after the cache can carry it.
- Which callers are refused first is an open admission decision. Until it is made, `RunQueues::try_reserve`
  applies one budget to everyone; the admission policy replaces that check and nothing else.
- The in-memory store reports no ceiling, and the embedded engine's default eight connections give a commit
  allocation of two, so both get `N = lane_count`. They still gain: the slots are shared, so a busy lane can
  use slots that idle lanes leave free.
- Every concurrent property test parks commits on channels and checks the recorded order; none relies on
  timing.
