# Requirements Document

## Introduction

This feature extends the projection sink contract with batched apply semantics and configurable per-sink failure policies. The current `ProjectionSink` trait only supports single-record `apply()`, forcing one SQL round-trip per record. The worker's failure handling is hardcoded (exponential backoff 100ms→5s, no poison-record escape). This feature adds `apply_batch()` for multi-row DSQL inserts, and a declarative failure policy model (retry backoff, max retries, dead-letter) so that operators can tune resilience per sink without code changes.

## Glossary

- **Projection_Worker**: The async task driving one `(partition_id, fanout)` substream, responsible for reading batches from the projection log, applying them to a sink, and persisting checkpoints.
- **Projection_Sink**: The trait contract for applying projection records to a downstream store (visibility, custom read models).
- **Batch_Sink**: An extension of Projection_Sink that accepts a slice of records for atomic multi-row application.
- **Failure_Policy**: A declarative configuration governing how the Projection_Worker handles sink errors — retry parameters, maximum attempts, and dead-letter routing.
- **Dead_Letter_Store**: A durable store where permanently-failing records are persisted after exhausting retries, unblocking the partition cursor.
- **Poison_Record**: A projection record that consistently fails application regardless of retry attempts.
- **OCC_Conflict**: An optimistic concurrency control conflict (SQLSTATE 40001) raised by Aurora DSQL when concurrent transactions modify the same rows.
- **Connection_Director**: The connection pool manager that allocates DSQL connections by DbClass budget (Projection gets 10%).
- **Checkpoint**: The persisted cursor position marking the last successfully applied batch for a sink.

## Requirements

### Requirement 1: Batched Apply Trait Extension

**User Story:** As a sink implementor, I want to apply multiple projection records in a single call, so that I can use multi-row INSERT statements and reduce DSQL round-trips.

#### Acceptance Criteria

1. THE Projection_Sink trait SHALL provide an `apply_batch` method that accepts a slice of ProjectionRecords and a partition_id, and returns a `Result<()>` indicating success or failure of the batch
2. THE Projection_Sink trait SHALL provide a default implementation of `apply_batch` that calls `apply` sequentially for each record in the slice, returning the first error encountered and stopping iteration on failure
3. THE Projection_Worker SHALL call `apply_batch` with the full set of records read from the projection log (up to `batch_size` records) instead of iterating `apply` for each record individually
4. IF `apply_batch` returns an error, THEN THE Projection_Worker SHALL treat the entire batch as failed, not advance the checkpoint cursor, and retry the batch from the same cursor position on the next iteration

### Requirement 2: DSQL Multi-Row Batch Sink

**User Story:** As an operator, I want the DSQL visibility sink to insert multiple rows per transaction, so that projection throughput scales with batch size without proportional connection consumption.

#### Acceptance Criteria

1. WHEN `apply_batch` is called on the DSQL visibility sink, THE DSQL_Visibility_Sink SHALL execute all row upserts within a single DSQL transaction containing at most 128 rows per CTE statement
2. IF an OCC_Conflict occurs during a batched transaction, THEN THE DSQL_Visibility_Sink SHALL retry the entire batch up to 5 attempts with exponential backoff starting at 50 milliseconds, capped at 1 second, with random jitter of ±50% applied to each delay
3. THE DSQL_Visibility_Sink SHALL acquire at most one connection permit from the Connection_Director per `apply_batch` invocation
4. WHILE executing a batched transaction, THE DSQL_Visibility_Sink SHALL use CTE-based multi-row upsert statements rather than temp tables (DSQL does not support temp tables)
5. IF a non-OCC error occurs during a batched transaction (network failure, constraint violation, or timeout), THEN THE DSQL_Visibility_Sink SHALL return the error immediately without retrying, propagating it to the Projection_Worker for Failure_Policy handling

### Requirement 3: Failure Policy Configuration

**User Story:** As an operator, I want to declare per-sink failure policies, so that I can tune retry behavior and dead-letter routing without modifying code.

#### Acceptance Criteria

1. THE Failure_Policy SHALL specify an initial retry backoff duration as a value between 1 millisecond and 60,000 milliseconds inclusive
2. THE Failure_Policy SHALL specify a maximum retry backoff duration as a value between 1 millisecond and 600,000 milliseconds inclusive
3. THE Failure_Policy SHALL specify a backoff multiplier for exponential growth as a value between 1.0 and 10.0 inclusive
4. THE Failure_Policy SHALL specify a maximum number of retry attempts before a record is considered a Poison_Record as a value between 1 and 1000 inclusive
5. THE Failure_Policy SHALL specify whether dead-letter routing is enabled for Poison_Records
6. THE Failure_Policy SHALL be configurable per sink instance at sink registration time without requiring code changes
7. THE Failure_Policy SHALL enforce that the initial retry backoff duration does not exceed the maximum retry backoff duration
8. IF a Failure_Policy is provided with any field outside its valid range or with initial backoff exceeding maximum backoff, THEN THE Projection_Worker SHALL reject the configuration at sink registration time and return an error indicating which field is invalid

### Requirement 4: Retry Backoff Behavior

**User Story:** As an operator, I want the projection worker to apply exponential backoff with configurable bounds on retry, so that transient failures recover without overwhelming DSQL.

#### Acceptance Criteria

1. WHEN a sink apply fails and retries remain, THE Projection_Worker SHALL wait for the current backoff duration before retrying
2. THE Projection_Worker SHALL multiply the backoff duration by the configured multiplier after each failed attempt
3. THE Projection_Worker SHALL cap the backoff duration at the configured maximum backoff
4. WHEN a sink apply succeeds after retries, THE Projection_Worker SHALL reset the retry counter and backoff to initial values
5. THE Projection_Worker SHALL NOT advance the Checkpoint while retries are in progress

### Requirement 5: Maximum Retry Exhaustion

**User Story:** As an operator, I want a hard limit on retries per record, so that a single Poison_Record does not block a partition indefinitely.

#### Acceptance Criteria

1. WHEN the retry count for a record (identified by run_key and transition_seq) reaches the configured maximum (valid range: 1–100, default: 5), THE Projection_Worker SHALL classify the record as a Poison_Record
2. WHEN a Poison_Record is identified, IF dead-letter is enabled, THEN THE Projection_Worker SHALL route the record to the Dead_Letter_Store
3. WHEN a Poison_Record is identified, IF dead-letter is disabled, THEN THE Projection_Worker SHALL skip the record and log a warning with the record's run_key and transition_seq
4. WHEN a Poison_Record has been handled (routed or skipped), THE Projection_Worker SHALL advance the cursor past the failed record and continue processing subsequent records
5. IF routing a Poison_Record to the Dead_Letter_Store fails, THEN THE Projection_Worker SHALL fall back to skip-and-log behavior (as in criterion 3) and advance the cursor past the record
6. THE Projection_Worker SHALL persist per-record retry counts durably (keyed by run_key and transition_seq) so that a worker crash and restart does not reset the count and allow a Poison_Record to block the partition indefinitely across restarts

### Requirement 6: Dead-Letter Store

**User Story:** As an operator, I want permanently-failing records stored durably with diagnostic context, so that I can investigate and replay them later.

#### Acceptance Criteria

1. THE Dead_Letter_Store SHALL persist the full ProjectionRecord including all ops, context, run_key, and transition_seq
2. THE Dead_Letter_Store SHALL persist the error message from the final failed attempt, truncated to a maximum of 4096 characters if the original message exceeds that length
3. THE Dead_Letter_Store SHALL persist the sink_id, partition_id, timestamp of dead-lettering (wall-clock UTC at the moment of write), and the number of retry attempts that were exhausted before dead-lettering
4. THE Dead_Letter_Store SHALL be queryable by sink_id and partition_id, returning results ordered by dead-letter timestamp descending, with cursor-based pagination supporting a maximum page size of 100 records
5. IF the Dead_Letter_Store write fails, THEN THE Projection_Worker SHALL log the failure at error level with sink_id, partition_id, run_key, and transition_seq, and skip the record rather than blocking the partition

### Requirement 7: Failure Policy Defaults

**User Story:** As an operator, I want sensible defaults for failure policies, so that the system behaves safely without explicit configuration.

#### Acceptance Criteria

1. THE Failure_Policy default initial backoff SHALL be 100 milliseconds
2. THE Failure_Policy default maximum backoff SHALL be 5 seconds
3. THE Failure_Policy default backoff multiplier SHALL be 2
4. THE Failure_Policy default maximum retry attempts SHALL be 10
5. THE Failure_Policy default dead-letter routing SHALL be enabled
6. WHEN no explicit Failure_Policy is provided for a sink, THE Projection_Worker SHALL use the default Failure_Policy

### Requirement 8: Batch-Level Failure Semantics

**User Story:** As a sink implementor, I want clear semantics for what happens when a batch partially fails, so that I can implement correct transactional behavior.

#### Acceptance Criteria

1. WHEN `apply_batch` returns an error, THE Projection_Worker SHALL treat the entire batch as failed and retry from the first record in the batch
2. THE Projection_Worker SHALL NOT advance the Checkpoint for a partially-applied batch
3. WHEN `apply_batch` succeeds, THE Projection_Worker SHALL advance the Checkpoint past all records in the batch
4. THE Projection_Worker SHALL count each failed `apply_batch` invocation as one retry attempt against the first record in the batch for Poison_Record classification

### Requirement 9: Observability

**User Story:** As an operator, I want metrics and structured logs for retry and dead-letter events, so that I can monitor projection health and detect Poison_Records.

#### Acceptance Criteria

1. WHEN a retry occurs, THE Projection_Worker SHALL increment a retry counter metric labeled by sink_id and partition_id
2. WHEN a record is dead-lettered, THE Projection_Worker SHALL increment a dead-letter counter metric labeled by sink_id and partition_id
3. WHEN a Poison_Record is identified, THE Projection_Worker SHALL emit a structured warning log with sink_id, partition_id, run_key, transition_seq, and the final error message
4. THE Projection_Worker SHALL expose a gauge metric for current retry backoff duration per sink_id and partition_id
5. WHEN `apply_batch` succeeds, THE Projection_Worker SHALL record the batch size in the existing WORKER_BATCH_RECORDS metric

### Requirement 10: Connection Budget Compliance

**User Story:** As an operator, I want batched apply to respect the DSQL connection budget, so that projection does not starve other subsystems of connections.

#### Acceptance Criteria

1. WHILE retrying a failed batch, THE Projection_Worker SHALL release the connection permit between retry attempts
2. THE DSQL_Visibility_Sink SHALL acquire a connection permit from the Connection_Director before each `apply_batch` attempt
3. THE DSQL_Visibility_Sink SHALL release the connection permit after each `apply_batch` attempt completes (success or failure)
4. THE Projection_Worker SHALL NOT hold more than one connection permit per partition substream at any time
