# Design Document: Nexus Operation Dispatch

## Overview

This design wires the runtime's `DispatchPublisher` to handle `DispatchOp::ScheduleNexusOperation` and `DispatchOp::CancelNexusOperation`, replacing the current stub logging in `RuntimeDispatchPublisher` (the `other =>` catch-all arm) with working implementations. It introduces outbound HTTP I/O from the runtime for the first time — all previous dispatch ops (child workflows, external signals, external cancels) target internal runs via lane submission.

The kernel already handles all Nexus commands authoritatively:
- `WorkflowCommand::ScheduleNexusOperation` emits `DispatchOp::ScheduleNexusOperation` and inserts a `PendingNexusOperation` entry.
- `WorkflowCommand::CancelNexusOperation` emits `DispatchOp::CancelNexusOperation` and emits a `NexusOperationCancelRequested` history event.
- `Command::NexusOperationResolved` processes the resolution (Started, Completed, Failed, Canceled, TimedOut), emits the appropriate history event, and updates `pending_nexus_operations`.

The runtime's job is orchestration: translate dispatch ops into HTTP calls to Nexus endpoints, track timeout deadlines, and deliver resolution results back to the originator via `publisher.submit_to_run`.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing) and Feature 2 (Activity Pump), both already implemented. The design follows the same patterns established in Feature 7 (External Signal and Cancel Delivery) and Feature 3 (Activity Heartbeat and Timeouts).

## Architecture

```mermaid
flowchart TD
    subgraph "Originator Workflow Task Completion"
        WTC_SCHED[WorkflowTaskCompleted<br/>with ScheduleNexusOperation cmd] -->|kernel emits| DOP_SCHED[DispatchOp::ScheduleNexusOperation]
        WTC_CANCEL[WorkflowTaskCompleted<br/>with CancelNexusOperation cmd] -->|kernel emits| DOP_CANCEL[DispatchOp::CancelNexusOperation]
    end

    subgraph "RuntimeDispatchPublisher"
        DOP_SCHED -->|tokio::spawn| RESOLVE_EP[NexusEndpointRegistry<br/>resolve endpoint name → address]
        RESOLVE_EP -->|found| HTTP_START[NexusHttpClient::start_operation]
        RESOLVE_EP -->|not found| FAIL_RES[NexusResolution::Failed<br/>"endpoint not found"]
        HTTP_START -->|sync complete| RES_COMPLETE[NexusResolution::Completed]
        HTTP_START -->|sync failure| RES_FAILED[NexusResolution::Failed]
        HTTP_START -->|async accept| RES_STARTED[NexusResolution::Started]
        HTTP_START -->|transient error| RES_FAILED
        RES_COMPLETE --> SUBMIT_RES[submit_to_run<br/>Command::NexusOperationResolved<br/>→ originator]
        RES_FAILED --> SUBMIT_RES
        RES_STARTED --> SUBMIT_RES
        FAIL_RES --> SUBMIT_RES

        DOP_CANCEL -->|tokio::spawn| RESOLVE_EP_C[NexusEndpointRegistry<br/>resolve endpoint name]
        RESOLVE_EP_C -->|found| HTTP_CANCEL[NexusHttpClient::cancel_operation]
        RESOLVE_EP_C -->|not found| NOOP_C[no-op, log warn]
        HTTP_CANCEL -->|success| RES_CANCELED[NexusResolution::Canceled<br/>→ originator]
        HTTP_CANCEL -->|failure| NOOP_CF[no-op, log debug]
    end

    subgraph "Nexus Timeout Scanner"
        TRACKING[NexusTimeoutTrackingState] -->|periodic scan| SCANNER[NexusTimeoutScanner]
        SCANNER -->|elapsed > timeout| TIMEOUT_CMD[Command::NexusOperationResolved<br/>NexusResolution::TimedOut<br/>→ originator lane]
    end

    subgraph "Tracking Lifecycle"
        DOP_SCHED -->|if timeout configured| INSERT_TRACK[insert tracking entry]
        INSERT_TRACK --> TRACKING
        SUBMIT_RES -->|on commit| REMOVE_TRACK[remove tracking entry]
        REMOVE_TRACK --> TRACKING
        RUN_CLOSE[Run reaches terminal state] -->|remove all for run| TRACKING
    end
```

### Key design decisions

**Extend dispatch ops with originator identity (same pattern as Features 6 and 7).** `DispatchOp::ScheduleNexusOperation` gains `originator_run_key: RunKey` and `scheduled_event_id: i64`. `DispatchOp::CancelNexusOperation` gains `originator_run_key: RunKey`, `operation_id: String`, `endpoint: String`, and `service: String`. The kernel's `apply_workflow_command` populates these from `builder.state.run_key` and the matching `PendingNexusOperation` entry.

**NexusHttpClient trait for testability.** A `NexusHttpClient` trait abstracts outbound HTTP. The trait has two methods: `start_operation` (returns sync-complete, sync-fail, or async-accept) and `cancel_operation` (returns success or failure). Tests inject mock implementations. The `RuntimeDispatchPublisher` holds an `Arc<dyn NexusHttpClient>`.

**NexusEndpointRegistry as an in-memory map.** An `Arc<HashMap<String, NexusEndpointConfig>>` maps endpoint names to network addresses. Configured at runtime construction time. Unknown endpoints produce an immediate `NexusResolution::Failed` for schedule ops and a no-op for cancel ops.

**Async, fire-and-forget dispatch via `tokio::spawn`.** Each Nexus dispatch op is processed in a spawned task, consistent with child workflow and external signal patterns. This ensures one slow or failing HTTP call does not block other dispatch ops in the same batch.

**Resolution always delivered for schedule ops.** Regardless of whether the HTTP call succeeds or fails (transient error, unknown endpoint), the publisher always delivers a `NexusOperationResolved` command back to the originator. This ensures the originator's `PendingNexusOperation` entry is resolved.

**Cancel failures are harmless no-ops.** If the cancel HTTP request fails or the endpoint reports the operation already completed, the publisher logs and moves on. No resolution command is submitted for failed cancels — the operation will either complete normally or time out.

**Runtime-local timeout tracking (same pattern as activity timeouts and workflow timeouts).** A `NexusTimeoutTrackingState` (in-memory `HashMap<(RunKey, String), NexusTimeoutEntry>`) records pending Nexus operations with `schedule_to_close_timeout`. A background `NexusTimeoutScanner` periodically checks for violations and submits `NexusOperationResolved(TimedOut)` through the lane. The scanner is non-authoritative — the kernel is the final arbiter.

**Tracking lifecycle mirrors activity tracking.** Entries are inserted when `ScheduleNexusOperation` dispatch ops with a timeout are published, using the authoritative `scheduled_at` from the dispatch op (not the wall-clock time at publication). Entries are removed when a terminal `NexusOperationResolved` is committed (Completed, Failed, Canceled, or TimedOut) or when the run closes. The `Started` resolution is non-terminal and does NOT remove the tracking entry — the operation is still pending and may still time out.

## Components and Interfaces

### NexusHttpClient trait

```rust
/// Result of a Nexus start_operation HTTP call.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusStartResult {
    /// The operation completed synchronously with a result payload.
    SyncCompleted { result: Payloads },
    /// The operation failed synchronously with an error message.
    SyncFailed { message: String },
    /// The operation was accepted for asynchronous execution.
    AsyncAccepted,
}

/// Abstraction over outbound Nexus HTTP transport.
///
/// Allows test implementations to mock network I/O without
/// requiring real HTTP endpoints.
#[async_trait]
pub trait NexusHttpClient: Send + Sync {
    /// Dispatch a Nexus operation to the resolved endpoint address.
    async fn start_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
        operation: &str,
        input: &Payloads,
        schedule_to_close_timeout: Option<Duration>,
    ) -> Result<NexusStartResult>;

    /// Send a cancellation request for a Nexus operation.
    /// The cancel protocol identifies by operation_id; the
    /// operation name is not needed.
    async fn cancel_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
    ) -> Result<()>;
}
```

### NexusEndpointRegistry

```rust
/// Configuration for a single Nexus endpoint.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointConfig {
    /// Network address (e.g., "http://nexus-service:8080").
    pub address: String,
}

/// In-memory registry mapping endpoint names to network addresses.
///
/// Configured at runtime construction time. Thread-safe via Arc.
#[derive(Clone, Default)]
pub struct NexusEndpointRegistry {
    endpoints: Arc<HashMap<String, NexusEndpointConfig>>,
}

impl NexusEndpointRegistry {
    pub fn new(endpoints: HashMap<String, NexusEndpointConfig>) -> Self {
        Self { endpoints: Arc::new(endpoints) }
    }

    /// Resolve an endpoint name to its network address.
    /// Returns None if the endpoint is not registered.
    pub fn resolve(&self, endpoint_name: &str) -> Option<&NexusEndpointConfig> {
        self.endpoints.get(endpoint_name)
    }
}
```

### Extended DispatchOp::ScheduleNexusOperation

```rust
DispatchOp::ScheduleNexusOperation {
    operation_id: String,
    endpoint: String,
    service: String,
    operation: String,
    input: Payloads,
    schedule_to_close_timeout: Option<Duration>,
    // New fields:
    originator_run_key: RunKey,
    scheduled_event_id: i64,
    scheduled_at: OffsetDateTime,
}
```

### Extended DispatchOp::CancelNexusOperation

```rust
DispatchOp::CancelNexusOperation {
    scheduled_event_id: i64,
    // New fields:
    originator_run_key: RunKey,
    operation_id: String,
    endpoint: String,
    service: String,
}
```

### Kernel changes: apply_workflow_command

The `ScheduleNexusOperation` arm currently emits:

```rust
builder.dispatch_ops.push(DispatchOp::ScheduleNexusOperation {
    operation_id,
    endpoint,
    service,
    operation,
    input,
    schedule_to_close_timeout,
});
```

This becomes:

```rust
builder.dispatch_ops.push(DispatchOp::ScheduleNexusOperation {
    operation_id,
    endpoint,
    service,
    operation,
    input,
    schedule_to_close_timeout,
    originator_run_key: builder.state.run_key,
    scheduled_event_id,
});
```

The `CancelNexusOperation` arm currently emits:

```rust
builder.dispatch_ops.push(DispatchOp::CancelNexusOperation { scheduled_event_id });
```

This becomes:

```rust
builder.dispatch_ops.push(DispatchOp::CancelNexusOperation {
    scheduled_event_id,
    originator_run_key: builder.state.run_key,
    operation_id: known.clone(),
    endpoint: pending_entry.endpoint.clone(),
    service: pending_entry.service.clone(),
});
```

Where `pending_entry` is the `PendingNexusOperation` found by `scheduled_event_id` (already looked up as `known`).

### NexusTimeoutTrackingState

```rust
/// Per-operation tracking entry for Nexus timeout detection.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusTimeoutEntry {
    pub run_key: RunKey,
    pub operation_id: String,
    pub scheduled_event_id: i64,
    pub schedule_to_close_timeout: Duration,
    pub scheduled_at: OffsetDateTime,
}

/// Thread-safe container for Nexus timeout tracking entries.
/// Keyed by (RunKey, operation_id) for O(1) lookup.
#[derive(Default, Clone)]
pub struct NexusTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, String), NexusTimeoutEntry>>>,
}

impl NexusTimeoutTrackingState {
    pub fn insert(&self, entry: NexusTimeoutEntry) {
        self.inner.lock().unwrap()
            .insert((entry.run_key, entry.operation_id.clone()), entry);
    }

    pub fn remove(&self, run_key: RunKey, operation_id: &str) {
        self.inner.lock().unwrap().remove(&(run_key, operation_id.to_string()));
    }

    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.inner.lock().unwrap().retain(|k, _| k.0 != run_key);
    }

    pub fn snapshot(&self) -> Vec<NexusTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }
}
```

### NexusTimeoutScanner

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NexusTimeoutScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_timeouts_per_scan: usize,
}

impl Default for NexusTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}
```

### Timeout evaluation (pure function)

```rust
/// Evaluate whether a Nexus operation has exceeded its schedule-to-close timeout.
pub fn evaluate_nexus_timeout(
    entry: &NexusTimeoutEntry,
    now: OffsetDateTime,
) -> bool {
    let elapsed = now - entry.scheduled_at;
    elapsed > entry.schedule_to_close_timeout
        || (entry.schedule_to_close_timeout.is_zero() && now >= entry.scheduled_at)
}
```

### RuntimeDispatchPublisher — new handler methods

```rust
async fn handle_schedule_nexus_operation(
    &self,
    operation_id: String,
    endpoint_name: String,
    service: String,
    operation: String,
    input: Payloads,
    schedule_to_close_timeout: Option<Duration>,
    originator_run_key: RunKey,
    scheduled_event_id: i64,
) {
    let resolution = match self.nexus_registry.resolve(&endpoint_name) {
        Some(config) => {
            match self.nexus_client.start_operation(
                &config.address,
                &operation_id,
                &service,
                &operation,
                &input,
                schedule_to_close_timeout,
            ).await {
                Ok(NexusStartResult::SyncCompleted { result }) => {
                    NexusResolution::Completed { result }
                }
                Ok(NexusStartResult::SyncFailed { message }) => {
                    NexusResolution::Failed { failure: message }
                }
                Ok(NexusStartResult::AsyncAccepted) => {
                    NexusResolution::Started
                }
                Err(error) => {
                    NexusResolution::Failed {
                        failure: error.to_string(),
                    }
                }
            }
        }
        None => NexusResolution::Failed {
            failure: format!("nexus endpoint not found: {endpoint_name}"),
        },
    };

    let command = Command::NexusOperationResolved(NexusOperationResolvedRequest {
        operation_id,
        scheduled_event_id,
        resolution,
        now: OffsetDateTime::now_utc(),
    });
    if let Err(error) = self.pick_lane(originator_run_key)
        .submit(originator_run_key, command).await
    {
        tracing::warn!(
            ?error,
            originator_run_key = ?originator_run_key,
            scheduled_event_id,
            "failed to deliver NexusOperationResolved to originator"
        );
    }
}

async fn handle_cancel_nexus_operation(
    &self,
    originator_run_key: RunKey,
    operation_id: String,
    endpoint_name: String,
    service: String,
    scheduled_event_id: i64,
) {
    let Some(config) = self.nexus_registry.resolve(&endpoint_name) else {
        tracing::warn!(
            endpoint = endpoint_name,
            operation_id,
            "cancel nexus operation skipped: endpoint not found"
        );
        return;
    };

    // The cancel protocol identifies by operation_id; no operation name needed.
    match self.nexus_client.cancel_operation(
        &config.address,
        &operation_id,
        &service,
    ).await {
        Ok(()) => {
            let command = Command::NexusOperationResolved(
                NexusOperationResolvedRequest {
                    operation_id,
                    scheduled_event_id,
                    resolution: NexusResolution::Canceled,
                    now: OffsetDateTime::now_utc(),
                },
            );
            if let Err(error) = self.pick_lane(originator_run_key)
                .submit(originator_run_key, command).await
            {
                tracing::warn!(
                    ?error,
                    originator_run_key = ?originator_run_key,
                    "failed to deliver NexusOperationResolved(Canceled) to originator"
                );
            }
        }
        Err(error) => {
            tracing::debug!(
                ?error,
                operation_id,
                endpoint = endpoint_name,
                "cancel nexus operation failed (treating as no-op)"
            );
        }
    }
}
```

### RuntimeDispatchPublisher::publish — new match arms

```rust
DispatchOp::ScheduleNexusOperation {
    operation_id,
    endpoint,
    service,
    operation,
    input,
    schedule_to_close_timeout,
    originator_run_key,
    scheduled_event_id,
} => {
    // Insert timeout tracking entry if timeout is configured
    if let Some(timeout) = schedule_to_close_timeout {
        self.nexus_timeout_tracking.insert(NexusTimeoutEntry {
            run_key: *originator_run_key,
            operation_id: operation_id.clone(),
            scheduled_event_id: *scheduled_event_id,
            schedule_to_close_timeout: *timeout,
            scheduled_at: *scheduled_at,
        });
    }
    let publisher = RuntimeDispatchPublisher::clone(self);
    let operation_id = operation_id.clone();
    let endpoint = endpoint.clone();
    let service = service.clone();
    let operation = operation.clone();
    let input = input.clone();
    let schedule_to_close_timeout = *schedule_to_close_timeout;
    let originator_run_key = *originator_run_key;
    let scheduled_event_id = *scheduled_event_id;
    tokio::spawn(async move {
        publisher
            .handle_schedule_nexus_operation(
                operation_id,
                endpoint,
                service,
                operation,
                input,
                schedule_to_close_timeout,
                originator_run_key,
                scheduled_event_id,
            )
            .await;
    });
}

DispatchOp::CancelNexusOperation {
    scheduled_event_id,
    originator_run_key,
    operation_id,
    endpoint,
    service,
} => {
    let publisher = RuntimeDispatchPublisher::clone(self);
    let originator_run_key = *originator_run_key;
    let operation_id = operation_id.clone();
    let endpoint = endpoint.clone();
    let service = service.clone();
    let scheduled_event_id = *scheduled_event_id;
    tokio::spawn(async move {
        publisher
            .handle_cancel_nexus_operation(
                originator_run_key,
                operation_id,
                endpoint,
                service,
                scheduled_event_id,
            )
            .await;
    });
}
```

### Updated RuntimeDispatchPublisher

```rust
pub struct RuntimeDispatchPublisher<R> {
    broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
    repo: Arc<R>,
    lanes: Arc<Mutex<Vec<LaneHandle>>>,
    lane_count: usize,
    // New fields:
    nexus_client: Arc<dyn NexusHttpClient>,
    nexus_registry: NexusEndpointRegistry,
    nexus_timeout_tracking: NexusTimeoutTrackingState,
}
```

### Updated TokeiraRuntime

```rust
pub struct TokeiraRuntime<R> {
    // ... existing fields ...
    // New fields:
    nexus_timeout_tracking: NexusTimeoutTrackingState,
    nexus_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>,
    nexus_timeout_scanner_cancel: CancellationToken,
}
```

### Lane integration: tracking cleanup on run close and resolution commit

The lane's `run_activation` already handles workflow timeout tracking cleanup on run close:

```rust
if new_state.closed_at.is_some() {
    workflow_timeout_tracking.remove(message.run_key);
}
```

This extends to also clean up Nexus timeout tracking:

```rust
if new_state.closed_at.is_some() {
    workflow_timeout_tracking.remove(message.run_key);
    nexus_timeout_tracking.remove_all_for_run(message.run_key);
}
```

For resolution commit cleanup, after a successful `NexusOperationResolved` commit, the lane removes the specific tracking entry. This requires the lane to inspect the committed command and, if it was a `NexusOperationResolved`, call `nexus_timeout_tracking.remove(run_key, &operation_id)`.

## Data Models

### New types

| Type | Crate | Role |
|------|-------|------|
| `NexusHttpClient` | `tokeira-runtime` | Trait abstracting outbound Nexus HTTP transport |
| `NexusStartResult` | `tokeira-runtime` | Enum: `SyncCompleted`, `SyncFailed`, `AsyncAccepted` |
| `NexusEndpointConfig` | `tokeira-runtime` | Network address for a Nexus endpoint |
| `NexusEndpointRegistry` | `tokeira-runtime` | In-memory map from endpoint name to config |
| `NexusTimeoutEntry` | `tokeira-runtime` | Per-operation tracking entry for timeout detection |
| `NexusTimeoutTrackingState` | `tokeira-runtime` | Thread-safe container for timeout tracking entries |
| `NexusTimeoutScannerConfig` | `tokeira-runtime` | Configurable scan interval and batch size |
| `evaluate_nexus_timeout` | `tokeira-runtime` | Pure function: `(entry, now) -> bool` |

### Modified types

| Type | Crate | Change |
|------|-------|--------|
| `DispatchOp::ScheduleNexusOperation` | `tokeira-kernel` | Add `originator_run_key: RunKey`, `scheduled_event_id: i64`, `scheduled_at: OffsetDateTime` |
| `DispatchOp::CancelNexusOperation` | `tokeira-kernel` | Add `originator_run_key: RunKey`, `operation_id: String`, `endpoint: String`, `service: String` |
| `apply_workflow_command` (ScheduleNexusOperation arm) | `tokeira-kernel` | Populate new dispatch op fields from `builder.state` and emitted event ID |
| `apply_workflow_command` (CancelNexusOperation arm) | `tokeira-kernel` | Populate new dispatch op fields from `builder.state` and `PendingNexusOperation` |
| `RuntimeDispatchPublisher` | `tokeira-runtime` | Add `nexus_client`, `nexus_registry`, `nexus_timeout_tracking` fields; add handler methods; add match arms |
| `TokeiraRuntime` | `tokeira-runtime` | Add `nexus_timeout_tracking`, scanner handle, cancel token; accept `NexusHttpClient` and `NexusEndpointRegistry` at construction |
| `spawn_lane` | `tokeira-runtime` | Accept `NexusTimeoutTrackingState` parameter for cleanup on run close |

### Existing types used (no changes needed)

| Type | Crate | Role |
|------|-------|------|
| `NexusOperationResolvedRequest` | `tokeira-kernel` | Command payload for delivering resolution to originator |
| `NexusResolution` | `tokeira-kernel` | `Started`, `Completed`, `Failed`, `Canceled`, `TimedOut` |
| `PendingNexusOperation` | `tokeira-kernel` | Kernel state tracking a scheduled Nexus operation |
| `CommitResult` | `tokeira-storage` | `Applied`, `Conflict`, `Duplicate` |

### Data flow: Schedule Nexus Operation

```
Originator WFT Completed with ScheduleNexusOperation command
  → Kernel emits DispatchOp::ScheduleNexusOperation {
      operation_id, endpoint, service, operation, input,
      schedule_to_close_timeout, originator_run_key, scheduled_event_id
    }
  → Lane commits originator transition
  → Publisher.publish() handles ScheduleNexusOperation:
      1. If schedule_to_close_timeout is Some, insert NexusTimeoutEntry
      2. tokio::spawn:
         a. Resolve endpoint name via NexusEndpointRegistry → address
         b. Call NexusHttpClient::start_operation(address, ...)
         c. Map result to NexusResolution variant
         d. Submit Command::NexusOperationResolved to originator lane
```

### Data flow: Cancel Nexus Operation

```
Originator WFT Completed with CancelNexusOperation command
  → Kernel emits DispatchOp::CancelNexusOperation {
      scheduled_event_id, originator_run_key, operation_id, endpoint, service
    }
  → Lane commits originator transition
  → Publisher.publish() handles CancelNexusOperation:
      1. tokio::spawn:
         a. Resolve endpoint name via NexusEndpointRegistry → address
         b. Call NexusHttpClient::cancel_operation(address, ...)
         c. On success: submit NexusOperationResolved(Canceled) to originator
         d. On failure: log debug, no-op
```

### Data flow: Nexus Timeout

```
NexusTimeoutScanner periodic scan:
  1. Snapshot NexusTimeoutTrackingState
  2. For each entry (up to max_timeouts_per_scan):
     a. evaluate_nexus_timeout(entry, now)
     b. If timed out: submit Command::NexusOperationResolved(TimedOut) via lane
     c. On success or kernel rejection: remove tracking entry
     d. On transient error: log warn, leave entry for next cycle
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: HTTP client start_operation receives correct parameters

*For any* `DispatchOp::ScheduleNexusOperation` with arbitrary `operation_id`, `endpoint`, `service`, `operation`, `input`, and `schedule_to_close_timeout`, when the endpoint is present in the `NexusEndpointRegistry`, the `NexusHttpClient::start_operation` call shall receive:
- `address` equal to the registry's resolved address for the endpoint name
- `operation_id` equal to the dispatch op's `operation_id`
- `service` equal to the dispatch op's `service`
- `operation` equal to the dispatch op's `operation`
- `input` equal to the dispatch op's `input`
- `schedule_to_close_timeout` equal to the dispatch op's `schedule_to_close_timeout`

**Validates: Requirements 1.1, 1.2**

### Property 2: Schedule resolution always delivered with correct variant

*For any* `DispatchOp::ScheduleNexusOperation` and any outcome of the HTTP call (sync complete, sync failure, async accept, transient error, or unknown endpoint), the publisher shall submit a `Command::NexusOperationResolved` to the originator run with:
- `operation_id` matching the dispatch op's `operation_id`
- `scheduled_event_id` matching the dispatch op's `scheduled_event_id`
- `resolution` equal to `Completed` when the HTTP client returns `SyncCompleted`, `Failed` when the HTTP client returns `SyncFailed` or a transient error or the endpoint is unknown, `Started` when the HTTP client returns `AsyncAccepted`

**Validates: Requirements 1.3, 1.4, 1.5, 5.3, 9.1**

### Property 3: Kernel populates schedule dispatch op fields from workflow state

*For any* `WorkflowState` with arbitrary `run_key` and `last_event_id`, when the kernel processes a `WorkflowCommand::ScheduleNexusOperation`, the emitted `DispatchOp::ScheduleNexusOperation` shall have:
- `originator_run_key` equal to `state.run_key`
- `scheduled_event_id` equal to the event ID of the emitted `NexusOperationScheduled` history event (which is `state.last_event_id + 1`)

**Validates: Requirements 2.1, 2.2, 2.3, 2.4**

### Property 4: Kernel populates cancel dispatch op fields from workflow state and pending operation

*For any* `WorkflowState` with arbitrary `run_key` and a `PendingNexusOperation` entry matching the `scheduled_event_id`, when the kernel processes a `WorkflowCommand::CancelNexusOperation`, the emitted `DispatchOp::CancelNexusOperation` shall have:
- `originator_run_key` equal to `state.run_key`
- `operation_id` equal to the matching `PendingNexusOperation.operation_id`
- `endpoint` equal to the matching `PendingNexusOperation.endpoint`
- `service` equal to the matching `PendingNexusOperation.service`

**Validates: Requirements 4.1, 4.2, 4.3, 4.4**

### Property 5: Cancel success delivers Canceled resolution

*For any* `DispatchOp::CancelNexusOperation` where the endpoint is present in the registry and the `NexusHttpClient::cancel_operation` succeeds, the publisher shall submit a `Command::NexusOperationResolved` to the originator run with:
- `operation_id` matching the dispatch op's `operation_id`
- `scheduled_event_id` matching the dispatch op's `scheduled_event_id`
- `resolution` equal to `NexusResolution::Canceled`

**Validates: Requirements 3.1, 3.3**

### Property 6: Nexus timeout evaluation correctness

*For any* `NexusTimeoutEntry` with arbitrary `schedule_to_close_timeout` and `scheduled_at`, and *for any* `now` timestamp:
- If `now - scheduled_at > schedule_to_close_timeout`, then `evaluate_nexus_timeout` shall return `true`
- If `now - scheduled_at <= schedule_to_close_timeout` (and timeout is non-zero), then `evaluate_nexus_timeout` shall return `false`
- If `schedule_to_close_timeout` is zero and `now >= scheduled_at`, then `evaluate_nexus_timeout` shall return `true`

**Validates: Requirements 7.1**

### Property 7: Tracking entry inserted only when schedule_to_close_timeout is present

*For any* `DispatchOp::ScheduleNexusOperation` published through `RuntimeDispatchPublisher`:
- If `schedule_to_close_timeout` is `Some(d)`, the `NexusTimeoutTrackingState` shall contain an entry keyed by `(originator_run_key, operation_id)` with `schedule_to_close_timeout` equal to `d`
- If `schedule_to_close_timeout` is `None`, the `NexusTimeoutTrackingState` shall not contain an entry for that operation

**Validates: Requirements 7.2, 8.1**

### Property 8: Tracking entry removed on any terminal resolution

*For any* Nexus operation that is terminally resolved (Completed, Failed, Canceled, or TimedOut) via a successfully committed `Command::NexusOperationResolved`, the `NexusTimeoutTrackingState` shall no longer contain an entry for `(run_key, operation_id)` after the resolution is processed. The `Started` resolution is non-terminal and SHALL NOT remove the tracking entry.

**Validates: Requirements 8.2**

### Property 9: Tracking entries removed on run close

*For any* run that reaches a terminal state (closed), the `NexusTimeoutTrackingState` shall no longer contain any entries with that run's `RunKey` after the close is processed.

**Validates: Requirements 8.3**

### Property 10: Scanner batch bound

*For any* set of `N` timed-out Nexus operations in the `NexusTimeoutTrackingState` where `N > max_timeouts_per_scan`, the scanner shall submit at most `max_timeouts_per_scan` timeout commands per scan cycle.

**Validates: Requirements 7.5**

### Property 11: Endpoint registry lookup correctness

*For any* set of endpoint name/address pairs inserted into the `NexusEndpointRegistry`, looking up a registered name shall return the corresponding address, and looking up an unregistered name shall return `None`.

**Validates: Requirements 5.1, 5.2**

## Error Handling

### Unknown endpoint (schedule)

If `NexusEndpointRegistry::resolve` returns `None` for a `ScheduleNexusOperation`, the publisher delivers a `NexusResolution::Failed` with message `"nexus endpoint not found: {endpoint_name}"` to the originator. The originator's `PendingNexusOperation` entry is resolved and a failure history event is emitted by the kernel.

### Unknown endpoint (cancel)

If `NexusEndpointRegistry::resolve` returns `None` for a `CancelNexusOperation`, the publisher logs at `warn` level and takes no further action. The operation will either complete normally or time out.

### Transient HTTP errors (schedule)

If `NexusHttpClient::start_operation` returns an `Err` (network timeout, connection refused, HTTP 5xx), the publisher delivers a `NexusResolution::Failed` with the error message to the originator. This ensures the originator's pending operation is resolved rather than left dangling.

### Transient HTTP errors (cancel)

If `NexusHttpClient::cancel_operation` returns an `Err`, the publisher logs at `debug` level and takes no further action. Cancel failures are harmless — the operation will complete or time out independently.

### Resolution delivery failure

If the `Command::NexusOperationResolved` delivery to the originator fails (lane channel closed, OCC exhaustion), the publisher logs at `warn` level with `originator_run_key` and `scheduled_event_id` for operational diagnosis. The originator's `PendingNexusOperation` entry remains until the sweeper (Feature 11) or a future reconciliation mechanism resolves it.

### Timeout scanner kernel rejections

When the scanner submits `NexusOperationResolved(TimedOut)` and the kernel rejects it (`UnknownNexusOperation` because the operation was already resolved, or `RunClosed`), the scanner treats this as a successful cleanup: the tracking entry is removed. The rejection is logged at `debug` level.

### Timeout scanner transient errors

If the lane submission returns an `Err` (lane channel closed, OCC exhaustion), the scanner logs at `warn` level and continues to the next entry. The tracking entry is not removed, so the timeout will be retried in the next cycle.

### Async dispatch isolation

Each Nexus dispatch op is processed in a `tokio::spawn` task. Failure in one dispatch does not block or affect other dispatch ops in the same batch. This is consistent with the child workflow and external signal dispatch patterns.

## Testing Strategy

### Property-based testing

All 11 correctness properties will be implemented as property-based tests using the [`proptest`](https://docs.rs/proptest) crate, consistent with the existing test infrastructure in `tokeira-runtime` and `tokeira-kernel`.

Each property test will:
- Run a minimum of 100 iterations (proptest default is 256).
- Use mock implementations of `NexusHttpClient`, `NexusEndpointRegistry`, `RunRepository`, and `LaneHandle` that are configurable per test.
- Be tagged with a comment referencing the design property.
- Tag format: `// Feature: runtime-nexus-dispatch, Property N: <title>`

Each correctness property MUST be implemented by a SINGLE property-based test.

**Property 1 (HTTP client receives correct parameters):** A generator produces random `operation_id`, `endpoint`, `service`, `operation`, `input`, and `schedule_to_close_timeout`. A mock registry maps the endpoint to a random address. A mock HTTP client captures the `start_operation` call arguments. The test verifies all fields match.

**Property 2 (Schedule resolution always delivered):** A generator produces random dispatch ops and a random outcome selector (sync-complete, sync-fail, async-accept, transient-error, unknown-endpoint). Mock registry and HTTP client are configured per the outcome. A mock originator lane captures the `Command::NexusOperationResolved`. The test verifies the resolution is always delivered with the correct variant and fields.

**Property 3 (Kernel populates schedule dispatch op fields):** A generator produces random `WorkflowState` values with random `run_key` and `last_event_id`, plus random `ScheduleNexusOperation` command parameters. The test applies the command via `BasicKernel` and verifies `originator_run_key` and `scheduled_event_id` on the emitted dispatch op.

**Property 4 (Kernel populates cancel dispatch op fields):** A generator produces random `WorkflowState` values with a random `PendingNexusOperation` entry. The test applies a `CancelNexusOperation` command and verifies `originator_run_key`, `operation_id`, `endpoint`, and `service` on the emitted dispatch op.

**Property 5 (Cancel success delivers Canceled):** A generator produces random cancel dispatch ops with endpoints present in the registry. Mock HTTP client returns success. The test verifies `NexusResolution::Canceled` is submitted to the originator.

**Property 6 (Timeout evaluation):** A generator produces random `NexusTimeoutEntry` values with random `schedule_to_close_timeout` and `scheduled_at`, plus a random `now`. The test calls `evaluate_nexus_timeout` and verifies the result matches the expected boolean based on elapsed time vs timeout.

**Property 7 (Tracking entry insertion):** A generator produces random `ScheduleNexusOperation` dispatch ops with random `schedule_to_close_timeout` (Some or None). The test publishes the op and verifies the tracking state contains an entry if and only if the timeout was Some.

**Property 8 (Tracking entry removal on resolution):** A generator produces random resolution variants. The test inserts a tracking entry, commits a `NexusOperationResolved`, and verifies the entry is removed for terminal resolutions.

**Property 9 (Tracking entries removed on run close):** A generator produces random runs with multiple tracking entries. The test closes the run and verifies all entries for that run are removed.

**Property 10 (Scanner batch bound):** A generator produces a random number of timed-out entries exceeding `max_timeouts_per_scan`. The test runs one scan cycle and verifies at most `max_timeouts_per_scan` commands are submitted.

**Property 11 (Endpoint registry lookup):** A generator produces random endpoint name/address pairs. The test inserts them and verifies lookup correctness for both registered and unregistered names.

### Unit tests

Unit tests complement property tests for specific examples and edge cases:

- **Cancel with unknown endpoint**: verify no resolution command is submitted and no error propagated.
- **Cancel with HTTP failure**: verify no resolution command is submitted, logged at debug.
- **Scanner default config**: verify `NexusTimeoutScannerConfig::default()` has `scan_interval = 1s` and `max_timeouts_per_scan = 100`.
- **Scanner removes tracking entry after kernel rejection**: verify the entry is gone when kernel returns `UnknownNexusOperation`.
- **Scanner removes tracking entry after RunClosed rejection**: verify the entry is gone when kernel returns `RunClosed`.
- **Resolution delivery failure logging**: verify warn-level log when originator lane submission fails.
- **Zero-duration timeout fires immediately**: verify `evaluate_nexus_timeout` returns true when timeout is zero and `now >= scheduled_at`.

### Integration tests

Integration tests exercise the full `TokeiraRuntime` with `InMemoryStore` and a mock `NexusHttpClient`:

- Schedule a Nexus operation with a mock client returning sync completion. Verify the originator receives `NexusOperationCompleted` history event.
- Schedule a Nexus operation with a mock client returning async acceptance. Verify the originator receives `NexusOperationStarted` history event.
- Schedule a Nexus operation with a short `schedule_to_close_timeout` and a mock client returning async acceptance. Wait for the timeout scanner to fire. Verify the originator receives `NexusOperationTimedOut` history event.
- Schedule a Nexus operation targeting an unknown endpoint. Verify the originator receives `NexusOperationFailed` history event with "endpoint not found" message.
- Cancel a Nexus operation with a mock client returning success. Verify the originator receives `NexusOperationCanceled` history event.

### Test configuration

```toml
[dev-dependencies]
proptest = "1"
tokio-util = { version = "0.7", features = ["rt"] }
```
