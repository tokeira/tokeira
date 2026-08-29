# tokeira-runtime

Lane-based execution orchestration — the shell around the pure kernel. The runtime is where pure semantics meet scheduling and delivery. It serializes commands for a run, persists transitions, and publishes derived effects.

## Dependencies

- `tokeira-kernel` — `Command`, `WorkflowState`, `Transition`, `BasicKernel`
- `tokeira-storage` — `RunRepository`, `LeaseRepository`
- `tokeira-types` — identity types, queue keys, tokens
- External: `anyhow`, `async-trait`, `smallvec`, `time`, `tokio`, `tokio-util`, `tracing`

## Module Structure

| File | Contents |
|---|---|
| `runtime.rs` | `TokeiraRuntime` facade |
| `lane.rs` | Lane executor, run actor map, OCC retry, mailbox coalescing, eviction |
| `broker.rs` | Workflow + activity task brokers, sync match, sticky routing, live-ready tier |
| `publisher.rs` | `RuntimeDispatchPublisher` — handles all dispatch ops |
| `backlog.rs` | Grace scanner, drain loop, durable backlog (Tier C) |
| `retry.rs` | Activity retry evaluation (pure functions) |
| `timeout.rs` | Workflow timeout tracking + scanner |
| `scanner.rs` | Timer scanner + lane routing helpers |
| `nexus.rs` | Nexus types, endpoint registry, timeout scanner |
| `query.rs` | Query dispatch (`QueryTask`, `QueryResult`) |
| `update.rs` | Update lifecycle (`UpdateRegistry`, `UpdateOutcome`, `UpdateWaitPolicy`) |
| `fairness.rs` | Delivery metrics, drain share, control loop |
| `activity_timeout.rs` | Activity tracking + timeout scanner |
| `shard.rs` | Shard ownership, epoch fencing |
| `recovery.rs` | `sweep_shard`, lease renewer |
| `worker_registry.rs` | Worker version metadata |

## Key Types

- `StartedWorkflowTask` — dispatched WFT with task token and history
- `StartedActivityTask` — dispatched activity with `activity_type`, `workflow_id`, `workflow_type`, `workflow_namespace`, `header`, `retry_policy`
- `StartWorkflowResult` — Started / UsedExisting / Rejected
- `SignalWithStartResult` — Started / Signaled
- `ResetWorkflowResult` — successor run key and run ID
- `UpdateOutcome` — Accepted / Completed / Rejected
- `UpdateWaitPolicy` — Accepted / Completed
- `QueryResult` — query response with optional rejection status
- `WorkflowMutationOutcome` (from edge) — transition_seq, last_event_id, execution_status, new_run_id

## Orchestration Flow

1. Receive command (from edge or internal source)
2. Ensure shard ownership (check epoch)
3. Route to lane: `hash(shard_id, run_key) mod lane_count`
4. Load actor if absent (via storage)
5. Check request dedup (via storage)
6. Call `kernel.apply(loaded_state, command)`
7. Commit transition (via storage, fenced by expected_seq)
8. Storage appends a full versioned visibility snapshot in the fenced commit
9. Publish DispatchOps → delivery broker
10. Projection workers consume the snapshot log asynchronously
11. Park or evict actor

On OCC conflict at step 7, the runtime reloads state and retries from step 6.

## Activity Support

- `ActivityTaskStarted` event emitted when dispatching activity tasks to workers
- `StartedActivityTask` carries `activity_type`, `workflow_id`, `workflow_type`, `workflow_namespace`, `header`, `retry_policy`
- Activity retry, heartbeat processing, and timeout detection handled by runtime (outside kernel)

## All 15 Features Implemented

1. Lane OCC retry + mailbox coalescing
2. Activity pump (dispatch, poll, complete, retry)
3. Activity heartbeat + timeouts
4. Timer scanner
5. Workflow timeouts
6. Child workflow orchestration
7. External signal + cancel delivery
8. Continue-as-new
9. Nexus operation dispatch
10. Worker versioning + deployment routing
11. Sweeper and recovery
12. Durable backlog integration
13. Query dispatch
14. Update two-phase lifecycle
15. Broker fairness and admission

## Tests

125 unit tests + 37 integration tests, all passing.
