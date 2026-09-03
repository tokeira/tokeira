# Temporal Rust SDK embedded-engine spike

This standalone spike runs the published Temporal Rust SDK `0.8.0` against
`tokeira-engine` in one process. It starts an SDK worker, executes a workflow
that calls an activity, waits for the typed result, and shuts both worker and
engine down cleanly. The directory keeps its original name from the `0.7.0`
run so links to it stay valid; the pins are what moved.

The client and worker use `ConnectionOptions::service_override`; no Temporal
TCP server or DNS lookup is involved. The program keeps the configured gRPC
and Nexus listener addresses occupied for its entire run, so an accidental
network-listener fallback fails deterministically.

Run it from the repository root:

```console
cargo run --manifest-path spikes/temporal-rust-sdk-v0-7-embedded/Cargo.toml --locked
```

Successful output ends with:

```text
Temporal Rust SDK: 0.8.0
Transport: temporalio-client::service_override (no TCP listener)
Workflow run_id: <generated run id>
Workflow result: Hello, embedded Tokeira!
```

The spike is excluded from the main workspace so its exact SDK `0.8.0` pins
and standalone lockfile do not expand or constrain the product workspace. The
client crate must be the same version `tokeira-engine` itself depends on,
because `Engine::service_override` hands back that crate's callback service
type; moving the spike therefore moves the engine's `temporalio-client`
dependency with it.

## Rerun on 0.8.0

Recorded 2026-09-03 against `tokeira-engine` with `temporalio-client 0.8.0`:

- the clean path completed with the expected result;
- both in-memory shutdown probes exited cleanly (0.56 s and 0.52 s);
- the two managed-DSQL probes were not run in this rerun: they need a live
  cluster descriptor and AWS credentials, and remain the acceptance for the
  race fix described below.

The 0.8.0 release notes state that worker shutdown "no longer loses an
activity result it was still reporting" and drains in-flight completions
before the final slot-permit wait, which is exactly the sequence the probes
below order.

## Retry-pending shutdown probes

These probes document the Temporal Rust SDK `0.7.0` lifecycle failure and now
serve as the regression check that `0.8.0` keeps it fixed. They do not change
Tokeira product behaviour or constitute a Tokeira or DSQL fix.

The existing binary above remains the clean completed-workflow path. Separate,
additive tests use a handshake inside the Activity to order the lifecycle precisely:

1. the provider-side path announces that it is about to return a retryable failure;
2. the host requests worker shutdown while the Activity still owns its slot;
3. the provider failure is released, its event-to-heartbeat pump drains, and the
   Activity records one final heartbeat;
4. the Activity returns its retryable error to SDK core;
5. `Worker::run` exits and Describe confirms attempt 2 is durably `SCHEDULED`.

Each storage mode has two placements. The original-spike control keeps the
worker on the host Tokio runtime with workflow caching disabled. The
Odori-shaped placement constructs and runs the worker on a named OS thread with
its own current-thread Tokio runtime, uses SDK 0.7.0's default workflow cache,
sends the shutdown handle to the host, and joins the worker thread while the
embedded engine remains alive.

Run the in-memory control with:

```console
cargo test --manifest-path spikes/temporal-rust-sdk-v0-7-embedded/Cargo.toml \
  --test retry_pending_shutdown --locked \
  in_memory_worker_shutdown_at_retryable_failure_boundary_exits_cleanly -- --exact --nocapture
```

This exact in-memory sequence exits cleanly on current Tokeira. That result only
rules out a storage-independent failure; it does not represent the managed DSQL
path exercised by Odori.

Run the Odori-shaped in-memory control with:

```console
cargo test --manifest-path spikes/temporal-rust-sdk-v0-7-embedded/Cargo.toml \
  --test retry_pending_shutdown --locked \
  in_memory_dedicated_worker_thread_shutdown_exits_cleanly -- --exact --nocapture
```

The ignored live-DSQL comparator reuses an existing managed descriptor and runs
the identical worker lifecycle against its cluster:

```console
TOK_REPRO_DSQL_ACK=USE_EXISTING_CLUSTER \
TOK_REPRO_DSQL_REGION=eu-west-2 \
TOK_REPRO_DSQL_DESCRIPTOR_PATH=/absolute/private/path/managed-dsql.json \
cargo test --manifest-path spikes/temporal-rust-sdk-v0-7-embedded/Cargo.toml \
  --test retry_pending_shutdown --locked \
  managed_dsql_worker_shutdown_at_retryable_failure_boundary_exits_cleanly \
  -- --ignored --exact --nocapture
```

The descriptor path must be absolute and must already exist. Confirm it is the
intended ready descriptor before acknowledging the run: managed mode retains its
explicit `CreateOrRecover` authority. The probe creates a uniquely named workflow
but never invokes cluster destruction; ordinary engine shutdown retains deletion
protection and the descriptor.

The closest focused comparator for Odori's worker lifecycle uses the same live
DSQL setup but selects the dedicated-thread test:

```console
TOK_REPRO_DSQL_ACK=USE_EXISTING_CLUSTER \
TOK_REPRO_DSQL_REGION=eu-west-2 \
TOK_REPRO_DSQL_DESCRIPTOR_PATH=/absolute/private/path/managed-dsql.json \
cargo test --manifest-path spikes/temporal-rust-sdk-v0-7-embedded/Cargo.toml \
  --test retry_pending_shutdown --locked \
  managed_dsql_dedicated_worker_thread_shutdown_exits_cleanly \
  -- --ignored --exact --nocapture
```

A failure with `Waiting for all slot permits to release took too long!` comes from
Temporal SDK Core `0.7.0`'s final five-second wait for all workflow, Activity, and
local-Activity slot permits to be released. Core's `dbg_panic!` always logs the
invariant violation; builds with debug assertions enabled then panic, while builds
without debug assertions continue after the log.

### Verified comparison

The earlier, later-boundary version of these probes passed all four
storage/placement combinations against Tokeira base revision `8815c925` on 26
August 2026:

- the two in-memory placements completed in under one second each;
- the first managed-DSQL control run completed in 304.59 seconds;
- subsequent warm managed-DSQL runs, including the Odori-shaped placement with
  the SDK default workflow cache, completed in approximately 4.6 seconds.

Those results are retained only as historical comparison: that version did not
model the critical race because it requested shutdown after the Activity had
already returned its failure. The current probes request shutdown earlier and
keep the Activity slot occupied through heartbeat-pump drain and final-heartbeat
recording.

### Corrected early-boundary result

On 26 August 2026, the corrected probes established the bug condition on the
same Tokeira base revision:

- both in-memory worker placements exited cleanly in 0.62 seconds combined;
- the Odori-shaped managed-DSQL probe panicked after 9.28 seconds at Temporal
  SDK Core `0.7.0`'s final slot-permit wait with
  `Waiting for all slot permits to release took too long!`;
- the test surfaced the worker-thread panic through its join result, then the
  embedded engine performed ordinary shutdown without destroying the cluster.

The ignored managed-DSQL test deliberately asserts clean shutdown. It therefore
fails on unfixed code, which is the expected exploration-test confirmation of
the bug. Do not mark the panic as an expected test success: after a valid fix,
the same command must exit successfully while retaining the in-memory behavior.

### Diagnosis

The failure is a shutdown/completion race in Temporal Rust SDK Core `0.7.0`
that managed DSQL makes visible; it is not evidence of a leaked Tokeira DSQL
connection or a need to change the shared reservoir mechanics.

The source anchors are `ActivityHalf::activity_task_handler` in
`temporalio-sdk-0.7.0/src/lib.rs`, `WorkerActivityTasks::complete` in
`temporalio-sdk-core-0.7.0/src/worker/activities.rs`, `Worker::shutdown` in
`temporalio-sdk-core-0.7.0/src/worker/mod.rs`, and the `dbg_panic!` definition in
`temporalio-sdk-core-0.7.0/src/abstractions.rs`.

The verified sequence is:

1. the Rust SDK starts each Activity in a detached Tokio task;
2. when that task returns, Core removes the Activity from its outstanding-task
   map before waiting for heartbeat eviction and `RespondActivityTaskFailed`;
3. removing the map entry lets the Activity poll stream finish its shutdown,
   but the removed entry still owns the Activity slot permit in the detached
   completion future;
4. Core waits for any already-running heartbeat RPC, then reports the retryable
   failure to Tokeira; the slot is released only when that work returns and the
   removed entry is dropped;
5. `Worker::shutdown` independently advances to its final permit check and,
   after a hard-coded five seconds, logs the invariant violation. Builds with
   debug assertions enabled then panic.

Tokeira's in-memory heartbeat and retry transition finish inside that window.
The DSQL path durably commits heartbeat details and the replacement attempt,
including the dispatch row, so the same valid completion can remain in flight
past Core's deadline. Tokeira's engine is still running when the panic occurs;
engine shutdown is a consequence of unwinding the failed probe, not the trigger.

In a build without debug assertions, Core instead continues after logging the
deadline violation. It remains unverified whether dropping the dedicated worker
runtime can then cancel the detached completion before attempt 2 is durable; the
probe must establish that release-mode outcome before anyone claims release safety.

The probe now records callback-level timings for heartbeat and completion RPCs
and includes them in its error context. The first instrumented reruns were
blocked earlier by a separate managed-cluster-resolution failure, so one detail
remains deliberately unclaimed: whether the live five-second interval is spent
primarily waiting for the in-flight heartbeat to be evicted or in
`RespondActivityTaskFailed` itself. That distinction affects where a mitigation
belongs, but not the confirmed SDK permit-lifecycle race.
