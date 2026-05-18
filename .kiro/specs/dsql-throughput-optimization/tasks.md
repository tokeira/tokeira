# Implementation Plan: DSQL Throughput Optimization

## Overview

Five-phase optimization plan to increase Tokeira's sustained throughput from ~20 wf/s to 130 wf/s on compose DSQL. Each phase is independently deployable and testable. Phases are ordered by implementation risk and dependency safety: metrics → read amplification → commit reduction → run-key routing → caching.

## Tasks

- [x] 1. Phase 1: Metrics Additions
  - [x] 1.1 Add commits-in-flight gauge metric constant and helpers
    - Add `DSQL_COMMITS_IN_FLIGHT` constant and `MetricType::Gauge` entry to `METRIC_NAMES` in `crates/tokeira-storage/src/metrics.rs`
    - Implement `increment_dsql_commits_in_flight()` and `decrement_dsql_commits_in_flight()` helper functions
    - _Requirements: 2.1, 2.2, 2.3, 10.4_

  - [x] 1.2 Add history-read event count histogram metric
    - Add `READ_HISTORY_EVENTS` constant and `MetricType::Histogram` entry to `METRIC_NAMES` in `crates/tokeira-storage/src/metrics.rs`
    - Implement `record_read_history_events(count: usize)` helper function
    - _Requirements: 3.2, 10.5_

  - [x] 1.3 Instrument commit path with in-flight gauge
    - In `crates/tokeira-storage/src/dsql/run_repository.rs`, wrap `commit_transition_for_bundle` with increment/decrement calls
    - Ensure decrement fires on both success and failure paths
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 1.4 Instrument read_history with event count recording
    - In `crates/tokeira-storage/src/dsql/run_repository.rs`, record `events.len()` as histogram observation at the end of `read_history`
    - _Requirements: 3.1, 3.2_

  - [x]* 1.5 Write property test for commits-in-flight gauge accuracy
    - **Property 1: Commits-in-flight gauge accuracy**
    - For any sequence of concurrent commit operations (starts and completions interleaved in any order), the gauge SHALL always equal the number of currently-in-flight operations
    - Use `DebuggingRecorder` to verify gauge value matches expected in-flight count
    - **Validates: Requirements 2.3**

  - [x]* 1.6 Write property test for read-history event count histogram
    - **Property 2: Read-history event count histogram accuracy**
    - For any call to `read_history` that returns N events, the histogram SHALL record exactly N
    - **Validates: Requirements 3.2**

- [x] 2. Checkpoint — Phase 1 complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Phase 2: Read Amplification Fixes
  - [x] 3.1 Thread page size from edge to storage
    - In `crates/tokeira-edge/src/workflow_service.rs`, pass `self.config.max_history_page_size` to the runtime's history read path instead of `usize::MAX`
    - Add `DEFAULT_HISTORY_PAGE_SIZE` constant (1000) in `crates/tokeira-storage/src/dsql/run_repository.rs` for legacy callers
    - _Requirements: 4.1, 4.2, 4.3, 4.5_

  - [x] 3.2 Implement incremental history reads for non-first WFTs
    - Add `is_sticky_match: bool` to `StartedWorkflowTask`
    - Set `is_sticky_match` true only when the matched poller is the workflow's current sticky worker
    - In `crates/tokeira-edge/src/workflow_service.rs` (or `from_internal.rs`), use `previous_started_event_id` as the read offset when building WFT poll responses
    - First WFT (previous_started_event_id == 0): read from event 0
    - Subsequent sticky-match WFTs (`previous_started_event_id > 0 && is_sticky_match`): read from that offset onward
    - Non-sticky polls, sticky timeout fallback, and cache-miss fallback: read from event 0
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [x] 3.3 Add MutationMetadata struct and populate from CommitResult
    - Define `MutationMetadata` struct in `crates/tokeira-runtime/src/runtime.rs` with fields: workflow_id, run_id, first_execution_run_id, transition_seq, last_event_id, execution_status
    - Extend `StartWorkflowResult::Started` to carry `mutation_metadata: Option<MutationMetadata>`
    - In `start_workflow_with_policy`, extract metadata from `CommitResult::Applied { new_state }`
    - _Requirements: 6.1, 6.2_

  - [x] 3.4 Use MutationMetadata in edge to skip post-commit load_run
    - In `crates/tokeira-edge/src/workflow_service.rs`, when `StartWorkflowResult::Started` carries metadata, build the gRPC response directly without calling `load_run`
    - Retain fallback to `load_run` when metadata is absent
    - _Requirements: 6.3, 6.4, 6.5_

  - [x]* 3.5 Write property test for page size limit
    - **Property 3: Read-history respects page size limit**
    - For any history of any length and any finite `maximum_page_size`, `read_history` SHALL return at most `maximum_page_size` events
    - Generator: random `Vec<HistoryEvent>` + random `limit: 1..1000`
    - **Validates: Requirements 4.4**

  - [x]* 3.6 Write property test for partial history reads
    - **Property 4: Partial history reads from previous_started_event_id**
    - For any sticky-match WFT poll response where `previous_started_event_id > 0`, all events in the returned history SHALL have `event_id > previous_started_event_id`
    - For non-sticky poll responses, history SHALL start from event 0 regardless of `previous_started_event_id`
    - Generator: random event sequences + random `previous_started_event_id` + random `is_sticky_match`
    - **Validates: Requirements 5.1, 5.3**

  - [x] 3.7 Write property test for start transition metadata
    - **Property 5: Start transition produces metadata fields**
    - For any valid `StartRequest` applied to an absent run, the resulting `Transition.next_state` SHALL contain the `workflow_id`, `run_id`, and `first_execution_run_id` from the request
    - Generator: random `StartRequest` via proptest `Arbitrary`
    - **Validates: Requirements 6.1**

- [x] 4. Checkpoint — Phase 2 complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Phase 3: Sync-Match Eager Start
  - [x] 5.1 Add reserved poller contract to broker
    - In `crates/tokeira-runtime/src/broker.rs`, add `ReservedPoller` carrying queue, worker identity, and response channel
    - Add `try_reserve_poller(queue) -> Option<ReservedPoller>` that atomically removes one waiting poller
    - Add `return_reserved_poller(reserved)` for failed Start commits
    - _Requirements: 7.1, 7.9_

  - [x] 5.2 Add reserved poller metadata to StartRequest and kernel transition
    - In `crates/tokeira-kernel/src/command.rs`, add `pub reserved_poller_identity: Option<WorkerIdentity>` to `StartRequest`
    - In `crates/tokeira-kernel/src/kernel.rs`, modify `apply_start` to emit WorkflowTaskStarted event when `reserved_poller_identity` is present
    - Stamp WorkflowTaskStarted with the reserved poller's worker identity
    - Set `pending_workflow_task` with started state in the combined transition
    - Ensure the transition is atomic (all events or none)
    - _Requirements: 7.2, 7.3, 7.5_

  - [x] 5.3 Wire reserved poller delivery in runtime start path
    - In `crates/tokeira-runtime/src/runtime.rs`, before submitting the Start command, call `broker.try_reserve_poller(&queue)`
    - Set `request.reserved_poller_identity` from the reservation when present
    - In the lane's post-commit dispatch processing, when the committed command is a Start with `reserved_poller_identity.is_some()`, strip the matching `DispatchOp::EnqueueWorkflowTask` from the transition's `dispatch_ops` before they reach `publisher.publish(...)`
    - After successful commit, construct `StartedWorkflowTask` from committed state and send it to the reserved poller's response channel
    - Insert `WftTimeoutEntry` for the reserved-start WFT from `new_state.pending_workflow_task` before direct delivery
    - Do not call `start_polled_workflow_task` or publish the same WFT through the normal broker path for reserved starts
    - If submit or commit fails before a durable Start result exists, return the reservation to the broker before propagating the error
    - If direct delivery fails after commit, leave the timeout entry registered, log/metric the delivery failure, and still return the successful Start result so the WFT timeout scanner can redispatch normally
    - _Requirements: 7.1, 7.2, 7.6, 7.7, 7.8, 7.9_

  - [x] 5.4 Write property test for combined transition events
    - **Property 6: Sync-match combined transition events**
    - For any valid `StartRequest` with `reserved_poller_identity = Some(worker)` applied to an absent run, the transition SHALL contain WorkflowExecutionStarted, WorkflowTaskScheduled, and WorkflowTaskStarted events
    - WorkflowTaskStarted identity SHALL equal `worker`
    - **Validates: Requirements 7.2, 7.3**

  - [x] 5.5 Write property test for no WFT Started without sync-match
    - **Property 7: No WFT Started without sync-match**
    - For any valid `StartRequest` with `reserved_poller_identity = None` applied to an absent run, the transition SHALL NOT contain a WorkflowTaskStarted event
    - **Validates: Requirements 7.4**

- [x] 6. Checkpoint — Phase 3 complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Phase 4: Run-Key Based Lane Routing
  - [x] 7.1 Implement lane_index_for_run_key function
    - In `crates/tokeira-runtime/src/scanner.rs`, add `lane_index_for_run_key(run_key: RunKey, lane_count: usize) -> usize`
    - Implement it with a domain-separated stable spread key: `let lane_key = dsql_spread_uuid(&[b"lane", run_key.0.as_bytes()]); lane_key.as_u128() as usize % lane_count.max(1)`
    - Do not use raw `run_key.0.as_u128() % lane_count`, because it uses the same low bits as `shard_for` and collapses to shard routing when `shard_count == lane_count`
    - Retain existing `lane_index_for(shard_id, lane_count)` for sweep/scanner iteration only
    - _Requirements: 9.1, 9.2_

  - [x] 7.2 Update pick_lane to use run-key routing
    - In `crates/tokeira-runtime/src/runtime.rs`, change `pick_lane` to call `lane_index_for_run_key` instead of shard-based routing
    - Update timer, WFT timeout, activity timeout, and Nexus timeout scanner command submissions to route by `lane_index_for_run_key(entry.run_key, lane_count)`
    - Update `RuntimeDispatchPublisher` direct command submissions to route by `lane_index_for_run_key`
    - Retain `lane_index_for(shard_id, lane_count)` only for shard-scoped iteration, never command submission
    - Add a code comment next to shard-based routing warning that `lane.submit(run_key, ...)` call sites must use run-key routing
    - Retain shard ownership check as the admission boundary
    - _Requirements: 9.1, 9.3, 9.4, 9.7, 9.8_

  - [x] 7.3 Write property test for deterministic run-key routing
    - **Property 12: Deterministic run-key lane routing**
    - For any RunKey and lane_count, `lane_index_for_run_key` SHALL always return the same value, and the result SHALL be in `[0, lane_count)`
    - Generator: random RunKey + random lane_count: 1..128
    - **Validates: Requirements 9.1, 9.2**

  - [x] 7.4 Write property test for same-shard distribution
    - **Property 13: Same-shard runs distribute across lanes**
    - Generate or fixture 1000 distinct RunKeys that map to the same shard_id
    - Assert at least 2 distinct lane indices appear with `lane_index_for_run_key`
    - **Validates: Requirements 9.3, 9.6**

  - [x] 7.5 Write property test for all lanes reachable
    - **Property 14: All lanes reachable when lane_count exceeds shard_count**
    - With a fixed deterministic seed, generate 10,000 random RunKeys using `lane_count = 64` and `shard_count = 32`
    - Assert every lane index in `[0, 64)` appears at least once
    - **Validates: Requirements 9.5**

- [x] 8. Checkpoint — Phase 4 complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Phase 5: Lane-Local Actor Residency
  - [x] 9.1 Implement LaneCache struct
    - Create `LaneCache` in `crates/tokeira-runtime/src/lane.rs` with HashMap-based LRU eviction
    - Implement `get`, `put`, `evict`, `clear` methods
    - Support configurable `max_entries` and `idle_timeout`
    - _Requirements: 8.1, 8.5, 8.7_

  - [x] 9.2 Integrate LaneCache into lane message handling
    - Depends on completing task 7.2 so every command submission for a RunKey reaches the same lane
    - Modify `handle_message` in `crates/tokeira-runtime/src/lane.rs` to try cache before `load_run`
    - On `CommitResult::Applied`: update cache with new state
    - On `CommitResult::Conflict`: evict cache entry and retry from storage
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.8_

  - [x] 9.3 Add LaneCache configuration to LaneConfig
    - Add `cache_max_entries: usize` (default 1024) and `cache_idle_timeout: Duration` (default 30s) to `LaneConfig`
    - Clear cache on lane drain (shutdown)
    - _Requirements: 8.5, 8.6, 8.7_

  - [x] 9.4 Write property test for cache round-trip
    - **Property 8: Cache round-trip — commit populates, next command uses cache**
    - For any run where a command commits successfully, the resulting WorkflowState SHALL be cached, and a subsequent command for the same RunKey SHALL use the cached state
    - **Validates: Requirements 8.1, 8.2**

  - [x] 9.5 Write property test for OCC conflict eviction
    - **Property 9: OCC conflict evicts cache**
    - For any cached RunKey where a commit returns Conflict, the cache entry SHALL be evicted and the next attempt SHALL load from storage
    - **Validates: Requirements 8.4**

  - [x] 9.6 Write property test for idle timeout eviction
    - **Property 10: Idle timeout eviction**
    - For any cached entry whose `last_accessed` time exceeds `cache_idle_timeout`, a subsequent `get` SHALL return None
    - **Validates: Requirements 8.5**

  - [x] 9.7 Write property test for LRU bounded cache size
    - **Property 11: LRU bounded cache size**
    - For any sequence of cache insertions, the cache size SHALL never exceed `max_entries`; when the limit is reached, the LRU entry SHALL be evicted
    - Generator: random insertion sequences exceeding max_entries
    - **Validates: Requirements 8.7**

- [ ] 10. Final checkpoint — All phases complete
  - Ensure all tests pass, ask the user if questions arise.
  - Validate with `cargo run -p tokeira-bench -- --workflows 1000 --concurrency 150` against compose DSQL
  - Confirm 130 wf/s sustained, sub-200ms p50 latency, 2 commits per echo workflow with sync-match
  - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP; only metric accuracy and bounded-read tests are optional
- Each phase is independently deployable — phases can be merged separately
- Property tests use `proptest` (already in workspace) with minimum 100 iterations
- The kernel stays pure: StartRequest carries only reserved poller metadata, not a broker handle or response channel
- LaneCache is an optimization, not a correctness boundary — storage OCC remains authoritative
- Run-key routing preserves per-run serialization while decoupling from shard ownership
- All metrics are registered at process startup via the existing `METRIC_NAMES` manifest

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "1.4"] },
    { "id": 2, "tasks": ["1.5", "1.6", "3.1"] },
    { "id": 3, "tasks": ["3.2", "3.3"] },
    { "id": 4, "tasks": ["3.4", "3.5", "3.6", "3.7"] },
    { "id": 5, "tasks": ["5.1"] },
    { "id": 6, "tasks": ["5.2", "5.3"] },
    { "id": 7, "tasks": ["5.4", "5.5"] },
    { "id": 8, "tasks": ["7.1"] },
    { "id": 9, "tasks": ["7.2"] },
    { "id": 10, "tasks": ["7.3", "7.4", "7.5"] },
    { "id": 11, "tasks": ["9.1"] },
    { "id": 12, "tasks": ["9.2", "9.3"] },
    { "id": 13, "tasks": ["9.4", "9.5", "9.6", "9.7"] }
  ]
}
```
