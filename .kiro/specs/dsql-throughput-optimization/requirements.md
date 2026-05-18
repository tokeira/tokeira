# Requirements Document

## Introduction

This spec addresses a 5-phase performance optimization plan to reach 130 wf/s sustained on compose DSQL deployment. The current baseline is ~20 wf/s with 150 concurrency, 44ms average DSQL commit latency, 31ms average lane queue wait, and 3 commits per echo workflow (~132 commits/sec observed with only ~6 concurrent commits active despite capacity for more).

The phases are ordered by implementation risk, expected impact, and dependency safety:
1. Fix measurement noise — ensure metrics are trustworthy before optimizing.
2. Remove accidental read amplification — eliminate unnecessary storage reads on the hot path.
3. Reduce commit count — collapse the echo workflow from 3 commits to 2 via sync-match eager start.
4. Revisit lane routing — decouple lane partitioning from shard ownership before any lane-local cache depends on per-run lane affinity.
5. Add lane-local actor residency — cache hot workflow state to eliminate per-command load_run after routing guarantees all commands for a RunKey reach the same lane.

Target: 130 wf/s sustained on compose DSQL deployment with sub-200ms p50 latency for echo workflows.

## Glossary

- **Lane**: A single-threaded executor in `tokeira-runtime` that processes commands serially for runs routed to it. Each lane owns a bounded channel and guarantees per-run serialization.
- **RunKey**: The unique identifier for a workflow execution used for lane routing and storage lookups.
- **CommitResult**: The outcome of a durable transition commit, carrying the new WorkflowState on success.
- **WorkflowState**: The in-memory representation of a workflow execution's current state, including pending tasks, transition_seq, and closed_at.
- **transition_seq**: A monotonically increasing sequence number on each workflow run, used for OCC validation on commit.
- **Kernel**: The pure deterministic state machine in `tokeira-kernel` that processes commands and emits events. No I/O, no async, no storage.
- **Edge**: The compatibility layer (`tokeira-edge`) that translates gRPC requests into runtime commands and runtime results back into gRPC responses.
- **WFT**: Workflow Task — a unit of work dispatched to a worker SDK for replay and command processing.
- **sync-match**: The fast path where a workflow task is matched to a reserved waiting poller immediately without going through the durable backlog or a separate WorkflowTaskStarted commit.
- **ReservedPoller**: A runtime-owned reservation of a waiting poller for one workflow task response. It carries the worker identity and a response channel; it is either completed after the Start commit or returned to the broker if the commit fails.
- **is_sticky_match**: A flag on `StartedWorkflowTask` indicating that the matched poller is the workflow's current sticky worker and can receive incremental history.
- **previous_started_event_id**: The event ID of the last WorkflowTaskStarted event in a prior WFT, used as the replay boundary for incremental history delivery.
- **maximum_page_size**: The computed upper bound on history events to return in a single page, derived from edge configuration.
- **load_run**: The storage operation that reads WorkflowState from the run repository, currently executed on every lane command.
- **OCC**: Optimistic Concurrency Control — DSQL validates transition_seq on commit and returns a serialization failure if stale.
- **Broker**: The runtime component that matches task queue publications (scheduled tasks) with waiting pollers.
- **LaneMessage**: The internal message struct carrying run_key, command, reply channel, and enqueued_at timestamp through the lane's bounded channel.
- **shard_id**: A logical partition identifier derived from run_key, used for ownership fencing and (currently) lane routing.
- **lane_index_for**: The function `lane_index_for(shard_id, lane_count)` that maps a shard to a lane. Currently `shard_id % lane_count`.
- **StartWorkflowResult**: The enum returned by the runtime after processing a StartWorkflow command, carrying `Started { run_key, run_id, mutation_metadata }`, `UsedExisting`, or `Rejected`.
- **mutation_metadata**: The set of fields extracted by the runtime from the committed `WorkflowState` after a successful Start commit, sufficient to build the gRPC StartWorkflowExecutionResponse without a post-commit storage read.

## Requirements

### Requirement 1: Reset Metrics Per Bench Run

**User Story:** As a developer running benchmarks, I want metrics counters to start at zero for each bench run, so that rate calculations and totals reflect only the current run without residual noise from prior runs.

#### Acceptance Criteria

1. WHEN tokeirad starts, THE Server SHALL initialize all Prometheus counters, histograms, and gauges at their zero values.
2. WHEN a bench run requires clean metrics, THE Operator SHALL restart tokeirad between runs so that all metric families begin from zero.
3. THE Server SHALL NOT carry forward counter values from a previous process lifetime into a new process lifetime.

### Requirement 2: Report Commit In-Flight Count as Gauge

**User Story:** As an operator, I want a gauge showing how many commits are currently in-flight (awaiting DSQL response), so that I can identify commit concurrency bottlenecks.

#### Acceptance Criteria

1. WHEN a commit operation begins (enters the DSQL write path), THE DsqlRunRepository SHALL increment a `tokeira_dsql_commits_in_flight` gauge.
2. WHEN a commit operation completes (success or failure), THE DsqlRunRepository SHALL decrement the `tokeira_dsql_commits_in_flight` gauge.
3. THE `tokeira_dsql_commits_in_flight` gauge SHALL reflect the exact number of concurrent commit operations at any point in time.
4. THE `tokeira_dsql_commits_in_flight` gauge SHALL be process-scoped and unlabeled by node; Prometheus scrape target labels distinguish nodes in multi-node deployments.

### Requirement 3: History-Read Counts Per Run

**User Story:** As a developer diagnosing read amplification, I want a counter of history read operations per bench run, so that I can quantify how many reads are performed relative to the number of workflows executed.

#### Acceptance Criteria

1. WHEN `DsqlRunRepository::read_history` is called, THE DsqlRunRepository SHALL increment `tokeira_storage_repository_operation_total` with label `operation="read_history"`.
2. WHEN `DsqlRunRepository::read_history` is called, THE DsqlRunRepository SHALL record the number of events returned as a `tokeira_storage_read_history_events` histogram observation.
3. THE Operator SHALL derive per-run read counts by dividing the total `read_history` operation count by the number of workflows completed in the bench run.

### Requirement 4: Honor History Page Size

**User Story:** As a developer, I want the storage layer to respect the computed maximum_page_size from the edge, so that history reads do not fetch unbounded event ranges and waste DSQL read capacity.

#### Acceptance Criteria

1. WHEN the Edge computes a `maximum_page_size` for a history read, THE Edge SHALL pass that value to the runtime's history read path.
2. WHEN the runtime calls `read_history`, THE runtime SHALL pass the `maximum_page_size` as the limit parameter to the storage layer.
3. THE DsqlRunRepository SHALL NOT use `usize::MAX` as the page size when a finite `maximum_page_size` is provided by the caller.
4. WHEN `maximum_page_size` is provided, THE DsqlRunRepository SHALL return at most `maximum_page_size` events per read_history call.
5. WHEN `maximum_page_size` is not provided (legacy callers), THE DsqlRunRepository SHALL fall back to a sensible default (1000 events) rather than `usize::MAX`.

### Requirement 5: Avoid Full-History WFT Poll Reads

**User Story:** As a developer, I want WFT poll responses for sticky/non-first workflow tasks to read history only from `previous_started_event_id` instead of event 0, so that repeat WFT polls do not re-read the entire history.

#### Acceptance Criteria

1. WHEN a WFT poll response is built for a workflow task where `previous_started_event_id > 0` and `is_sticky_match` is true, THE Edge SHALL read history starting from `previous_started_event_id` rather than from event 0.
2. WHEN a WFT poll response is built for the first workflow task (previous_started_event_id is 0 or absent), THE Edge SHALL read history from event 0.
3. WHEN reading partial history for a non-first WFT, THE Edge SHALL set the `history` field in the poll response to contain only events from `previous_started_event_id` onward.
4. THE Edge SHALL send full history from event 0 for non-sticky polls, sticky timeout fallback, cache-miss fallback, or any poller that is not the recorded sticky worker, regardless of `previous_started_event_id`.
5. WHEN a worker receives a partial history starting from `previous_started_event_id`, THE Worker SDK SHALL use its cached state up to that point and replay only the new events.

### Requirement 6: Return Start Mutation Metadata Without Post-Commit load_run

**User Story:** As a developer, I want the StartWorkflow path to return sufficient metadata from the commit result to build the gRPC response, so that the edge does not need a post-commit `load_run` call.

#### Acceptance Criteria

1. WHEN a StartWorkflow commit succeeds, THE runtime SHALL extract mutation_metadata (workflow_id, run_id, first_execution_run_id, transition_seq, last_event_id, execution_status) from `CommitResult::Applied { new_state }`.
2. WHEN `StartWorkflowResult::Started` is returned, THE runtime SHALL carry the mutation_metadata from the CommitResult to the edge.
3. WHEN the Edge receives `StartWorkflowResult::Started` with mutation_metadata, THE Edge SHALL build the `StartWorkflowExecutionResponse` directly from the metadata without calling `load_run`.
4. THE Edge SHALL NOT call `load_run` after a successful StartWorkflow commit when mutation_metadata is present in the result.
5. IF mutation_metadata is absent from the result (backward compatibility), THEN THE Edge SHALL fall back to the existing `load_run` path.

### Requirement 7: Sync-Match Eager Start (Combined Start + First WFT)

**User Story:** As a developer, I want the echo workflow's Start and first WFT Started to be committed as a single durable transition when a poller is already waiting, so that the echo workflow completes in 2 commits instead of 3.

#### Acceptance Criteria

1. WHEN a StartWorkflow command is processed and the Broker can reserve a waiting poller for the target task queue, THE runtime SHALL attach the reserved poller's worker identity to the Start command.
2. WHEN a reserved poller is attached to the Start command, THE Kernel SHALL emit WorkflowExecutionStarted, WorkflowTaskScheduled, and WorkflowTaskStarted events in a single transition.
3. WHEN a reserved poller is attached, THE Kernel SHALL stamp WorkflowTaskStarted with the reserved poller's worker identity.
4. WHEN no poller can be reserved, THE Kernel SHALL produce only the Start events (WorkflowExecutionStarted + WorkflowTaskScheduled) and the WFT Started SHALL be committed separately.
5. THE combined transition SHALL be atomic — either all events (Start + WFT Scheduled + WFT Started) commit together or none do.
6. WHEN the combined transition commits, THE runtime SHALL deliver the WFT directly to the reserved poller's response channel without calling the normal `start_polled_workflow_task` path.
7. WHEN the combined transition commits, THE runtime SHALL suppress the scheduled WFT's normal `DispatchOp::EnqueueWorkflowTask` publication so the already-started WFT is not delivered through the broker ready queue.
8. WHEN the combined transition commits, THE runtime SHALL insert WFT timeout tracking before direct delivery so a lost reserved poller is recovered by the WFT timeout scanner.
9. IF the Start commit fails, THE runtime SHALL return the reserved poller to the Broker's waiting queue so it can continue polling.
10. THE echo workflow SHALL complete in 2 commits (Start+WFTStarted, WFTCompleted+WorkflowCompleted) instead of 3 commits when a poller is reserved successfully.

### Requirement 8: Lane-Local Actor Residency (WorkflowState Cache)

**User Story:** As a developer, I want hot workflow runs to retain their WorkflowState and transition_seq in the lane's memory, so that the 7.5ms load_run per command is eliminated for runs that are actively processing.

#### Acceptance Criteria

1. WHEN a command completes successfully for a run, THE Lane SHALL cache the resulting WorkflowState and transition_seq for that RunKey.
2. WHEN a subsequent command arrives for a cached RunKey, THE Lane SHALL use the cached WorkflowState instead of calling `load_run` from storage.
3. THE Lane SHALL validate the cached transition_seq against the storage OCC check on commit — the cache is an optimization, not a correctness boundary.
4. IF the commit fails with an OCC conflict (transition_seq mismatch), THEN THE Lane SHALL evict the cached entry and reload from storage on the next attempt.
5. WHEN a cached run is idle for longer than the configured idle timeout, THE Lane SHALL evict the cached WorkflowState to bound memory usage.
6. WHEN the lane is drained (shutdown), THE Lane SHALL discard all cached entries without persisting them.
7. THE Lane SHALL bound the cache size to a configurable maximum number of entries, evicting least-recently-used entries when the limit is reached.
8. THE Lane SHALL NOT use the cache as a correctness boundary — storage OCC (transition_seq check) remains the authoritative conflict detector.

### Requirement 9: Run-Key Based Lane Routing

**User Story:** As a developer, I want lane routing to be based on run_key rather than shard_id, so that hot shards can distribute work across more lanes without violating per-run serialization.

#### Acceptance Criteria

1. THE runtime SHALL route commands to lanes using a domain-separated stable spread key, `lane_key = dsql_spread_uuid([b"lane", run_key.0.as_bytes()])`, and `lane_index = lane_key.as_u128() % lane_count`, instead of `shard_id % lane_count`.
2. THE runtime SHALL guarantee that all commands for the same RunKey are routed to the same lane (per-run serialization invariant).
3. THE runtime SHALL NOT require per-shard serialization — multiple runs from the same shard MAY execute on different lanes concurrently.
4. THE runtime SHALL retain shard ownership as the admission and fencing boundary — a command is rejected if the node does not own the shard for the run_key.
5. WHEN lane_count exceeds shard_count, THE runtime SHALL still distribute runs across all available lanes (not limited to shard_count lanes).
6. WHEN a single shard has many active runs, THE runtime SHALL distribute those runs across multiple lanes based on their individual run_keys.
7. THE runtime SHALL NOT change the shard ownership or epoch fencing semantics — only the lane routing derivation changes.
8. ALL command submissions to a lane, including scanner and dispatch-publisher submissions, SHALL route by `lane_index_for_run_key(run_key, lane_count)`.

### Requirement 10: Bench Metrics Completeness

**User Story:** As a developer running benchmarks, I want all key performance metrics to be observable per bench run, so that I can identify bottlenecks without code instrumentation.

#### Acceptance Criteria

1. THE Server SHALL expose `tokeira_runtime_lane_queue_wait_seconds` as a histogram measuring time from LaneMessage enqueue to processing start.
2. THE Server SHALL expose `tokeira_runtime_lane_processing_duration_seconds` as a histogram measuring lane command processing time, labeled by command type.
3. THE Server SHALL expose `tokeira_dsql_class_permit_wait_duration_seconds` as a histogram measuring time waiting for a ConnectionDirector class permit.
4. THE Server SHALL expose `tokeira_dsql_commits_in_flight` as a gauge measuring concurrent commit operations.
5. THE Server SHALL expose `tokeira_storage_repository_operation_total` as a counter labeled by operation and outcome for all storage operations on the DSQL path.
6. ALL metrics in criteria 1–5 SHALL be registered at process startup and begin at zero, ensuring clean per-run measurement when tokeirad is restarted between bench runs.

### Requirement 11: Performance Target Validation

**User Story:** As a developer, I want a documented benchmark configuration and success criteria, so that I can validate the optimization phases achieve the 130 wf/s target.

#### Acceptance Criteria

1. WHEN all five optimization phases are applied, THE benchmark SHALL target 130 wf/s sustained throughput on compose DSQL deployment.
2. THE benchmark SHALL use the echo workflow pattern (Start → WFT → Complete) as the primary workload.
3. THE benchmark SHALL run with 150 concurrency against a single-node compose DSQL deployment with 32 lanes and 32 shards.
4. THE benchmark SHALL define success as achieving 130 wf/s with sub-200ms p50 end-to-end latency for echo workflows.
5. THE benchmark SHALL report: wf/s achieved, p50/p95/p99 latency, commits/sec, average commit latency, lane queue wait, and commits per workflow.
6. WHEN the echo workflow uses sync-match eager start, THE benchmark SHALL observe 2 commits per workflow instead of 3.
7. THE benchmark SHALL be reproducible using `cargo run -p tokeira-bench -- --workflows <N> --concurrency 150` against a compose DSQL deployment with the documented configuration.
