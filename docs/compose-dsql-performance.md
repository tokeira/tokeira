# Compose + DSQL Performance Analysis

## Summary

A compose+DSQL deployment running tokeirad on a developer laptop against Aurora DSQL in eu-west-2 achieved **5 wf/s** under a 2000-workflow bench (20 concurrency). The in-memory baseline is **700+ wf/s**. This document identifies the three root causes, estimates theoretical throughput limits, and recommends concrete fixes.

## Observed Behaviour

| Metric | Value |
|--------|-------|
| Bench config | 2000 workflows, concurrency 20, random IDs |
| In-memory throughput | 704 wf/s |
| DSQL throughput (observed) | ~5 wf/s |
| commit_transition avg latency | 52.7 ms |
| load_run avg latency | 8.6 ms |
| DSQL region | eu-west-2 |
| Client location | Developer laptop (UK) |
| Pool size | 50 connections (default) |
| Shard count | 1 (default) |
| Projection partition count | 16 (legacy default) |

## Root Causes

### 1. Network Latency on the Critical Path (52ms per commit)

Each `commit_transition` takes 52ms — this is the DSQL network round-trip from the developer's laptop to eu-west-2. This is physics, not a bug.

A workflow lifecycle requires 3 sequential commits on the critical path:
1. `commit_transition` — Start (create the workflow)
2. `commit_transition` — WorkflowTaskStarted (task dispatched to worker)
3. `commit_transition` — WorkflowTaskCompleted (worker returns result)

**Minimum latency per workflow: 3 × 52ms = 156ms**

At concurrency 20: `20 / 0.156 = ~128 wf/s` theoretical maximum.

### 2. Projection Sink OCC Conflicts (vis_rollup hotspot)

The `DsqlVisibilityStore::apply()` method writes to `vis_rollup` using:
```sql
INSERT INTO vis_rollup (namespace_id, dimension, value, counter)
VALUES ($1, $2, $3, $4)
ON CONFLICT (namespace_id, dimension, value) DO UPDATE
SET counter = vis_rollup.counter + EXCLUDED.counter
```

With 16 projection workers all incrementing the same `(namespace_id, dimension, value)` row (e.g., the "Running" status counter), DSQL's OCC model causes serialization failures (SQLSTATE OC000). Each conflict triggers a worker-level retry with exponential backoff (100ms–5s), holding connections longer and eventually exhausting the pool.

**Cascade effect:** Pool exhaustion from projection retries starves `commit_transition` of connections, dropping throughput from the theoretical 128 wf/s to 5 wf/s.

### 3. Single-Shard Serialization (shard_count = 1)

The runtime execution-home routing hashes `(namespace_id, workflow_id) % shard_count`. With `shard_count = 1`, every workflow maps to shard 0. The `commit_transition_for_bundle` path reads `shard_lease WHERE shard_id = <shard_0_uuid>` for epoch fencing — all commits serialize on this single row.

Additionally, when `shard_count > 1` is configured without a placement controller, the node must self-assign those shards before admitting work. Without that startup self-assignment, commits for shards above zero are rejected as "not shard owner".

## Theoretical Maximum Throughput

### Best case (all issues fixed, same network)

| Factor | Constraint | Throughput limit |
|--------|-----------|-----------------|
| Network RTT | 52ms per commit, 3 commits per workflow | 128 wf/s at concurrency 20 |
| Pool size | 50 connections, 52ms hold time | 961 ops/s → ~320 wf/s |
| DSQL write throughput | Effectively unlimited for this scale | Not a bottleneck |

**Theoretical max with fixes: ~128 wf/s** (limited by sequential commit latency on the critical path at concurrency 20).

With higher concurrency (e.g., 50): `50 / 0.156 = ~320 wf/s` (limited by pool size).

### Best case (co-located in same region, e.g., ECS in eu-west-2)

| Factor | Constraint | Throughput limit |
|--------|-----------|-----------------|
| Network RTT | ~3ms per commit (same-AZ) | 2,222 wf/s at concurrency 20 |
| Pool size | 50 connections, 3ms hold time | 16,666 ops/s → ~5,555 wf/s |
| DSQL write throughput | ~10,000 TPS (single-region cluster) | ~3,333 wf/s |

**Theoretical max co-located: ~2,000–3,000 wf/s** (limited by DSQL TPS and sequential commit chain).

## Recommended Fixes

### Fix 1: Projection Sink OCC Retry (Priority: Critical)

**Problem:** `DsqlVisibilityStore::apply()` has no OCC retry logic. Conflicts propagate to the worker, which retries the entire batch with backoff, holding connections.

**Fix:** Add per-statement OCC retry with jittered backoff inside `accumulate_rollup`:

```rust
async fn accumulate_rollup(&self, entries: &[RollupDelta]) -> Result<()> {
    for entry in entries {
        let mut attempts = 0;
        loop {
            match self.try_accumulate_one(entry).await {
                Ok(()) => break,
                Err(e) if is_occ_conflict(&e) && attempts < 5 => {
                    attempts += 1;
                    let jitter = rand::random::<u64>() % 50;
                    tokio::time::sleep(Duration::from_millis(10 * attempts + jitter)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}
```

**Impact:** Eliminates pool exhaustion cascade. Projection conflicts resolve locally without starving the commit path.

### Fix 2: Redesign vis_rollup for OCC Compatibility (Priority: High)

**Problem:** `counter = counter + delta` on a shared row is fundamentally incompatible with OCC at high concurrency.

**Option A — Per-partition rollup sharding:**
```sql
CREATE TABLE vis_rollup (
    namespace_id UUID NOT NULL,
    dimension SMALLINT NOT NULL,
    value TEXT NOT NULL,
    partition_id INT NOT NULL,
    counter BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace_id, dimension, value, partition_id)
);
```
Each projection worker writes to its own `partition_id`. Read-time sums across partitions. Zero cross-worker conflicts.

**Option B — Append-only rollup log:**
```sql
CREATE TABLE vis_rollup_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL,
    dimension SMALLINT NOT NULL,
    value TEXT NOT NULL,
    delta BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
No conflicts ever. Read-time aggregation: `SELECT SUM(delta) FROM vis_rollup_log WHERE ...`. Periodic compaction reduces read cost.

**Option C — Remove rollup, compute on read:**
Delete `vis_rollup` entirely. `CountWorkflowExecutions` queries scan `vis_execution` with appropriate filters. Simpler but slower reads at scale.

**Recommendation:** Option A (per-partition sharding) — preserves fast reads, eliminates conflicts, minimal schema change.

### Fix 3: Self-Assign Shards in Single-Node Mode (Priority: High)

**Problem:** Without a placement controller, the node never acquires leases for shards > 0. With `shard_count = 1`, all commits serialize on one shard_lease row.

**Fix:** On startup, when `controller_endpoint` is not configured, the node should self-assign all shards by inserting/updating `shard_lease` rows and recording the returned lease epoch in runtime-owned `ShardOwner` state:

```rust
// In tokeirad startup, after runtime construction:
if config.infrastructure.placement.controller_endpoint.is_none() {
    let shard_count = config.infrastructure.placement.shard_count;
    for shard_id in 0..shard_count {
        let outcome = repo
            .try_acquire_bundle(ShardId(shard_id), node_id.to_string(), node_endpoint)
            .await?;
        if let LeaseOutcome::Acquired { epoch } | LeaseOutcome::Renewed { epoch } = outcome {
            runtime.record_self_assigned_shard(ShardId(shard_id), epoch);
        }
    }
}
```

**Impact:** Enables `shard_count > 1` for compose deployments. In single-node no-controller mode, the lane validates local ownership and passes `ShardEpoch::ZERO` to storage, so DSQL skips the per-transition lease-row read. Controller-managed deployments still pass the real epoch and keep the durable takeover fence.

### Fix 4: Increase Default Pool Size for DSQL (Priority: Medium)

**Problem:** Default pool size of 50 connections is adequate for steady-state but insufficient during OCC retry storms.

**Fix:** For DSQL deployments, increase the default pool to match the class budget total:
- commit: 25
- read: 10
- projection: 5
- control: 5
- maintenance: 5
- **Total: 50** (matches current default)

The pool size is correct, but the projection class budget (5) is too low for 16 workers. Each worker can hold a connection during retry backoff. With 5 permits and 16 workers, 11 workers queue.

**Fix:** Reduce projection partition count to match the budget, or increase the projection budget:
```toml
# In DsqlPoolConfig (internal, not operator-facing):
# projection_budget: 16  (match partition count)
```

### Fix 5: Batch Projection Log Writes (Priority: Medium)

**Problem:** Each `ProjectionSink::apply()` call acquires a separate connection for each sub-operation (upsert_execution, search attrs, rollup). A single record apply can acquire 5+ connections sequentially.

**Fix:** Wrap the entire `apply()` in a single transaction:

```rust
async fn apply(&self, record: &ProjectionRecord) -> Result<()> {
    let mut permit = self.director.acquire(DbClass::Projection).await?;
    let mut tx = permit.connection()?.begin().await?;
    // All operations use &mut tx instead of acquiring separate connections
    upsert_execution_row(&mut tx, ...).await?;
    upsert_search_attrs(&mut tx, ...).await?;
    accumulate_rollup(&mut tx, ...).await?;
    tx.commit().await?;
    Ok(())
}
```

**Impact:** Reduces connection acquisitions per record from 5+ to 1. Reduces pool pressure. Makes the entire apply atomic (no partial application on failure).

## Deployment Recommendations

### For compose+DSQL (developer laptop):

```toml
[infrastructure.placement]
shard_count = 32
partition_count = 4   # Reduce from 16 to limit projection concurrency
```

Tokeira promotes legacy DSQL defaults by value: `shard_count = 1` becomes `32`, and `partition_count = 16` becomes `4`. Existing explicit non-legacy values are preserved.

Expected throughput: **50–128 wf/s** after the structural fixes, limited primarily by network RTT from a developer machine.

### For ECS+DSQL (co-located in same region):

```toml
[infrastructure.placement]
shard_count = 64
partition_count = 16
```

Expected throughput: **1,000–3,000 wf/s** (with multi-node, multi-shard distribution).

## Priority Order

1. **Fix 1** (projection OCC retry) — unblocks compose+DSQL immediately
2. **Fix 5** (single-transaction apply) — reduces pool pressure, makes apply atomic
3. **Fix 2** (vis_rollup redesign) — eliminates the OCC hotspot permanently
4. **Fix 3** (self-assign shards) — enables multi-shard compose for higher throughput
5. **Fix 4** (pool/budget tuning) — fine-tuning after the structural fixes land

## Dashboard and Metrics Gaps

### Metric Emission Split

The in-memory and DSQL storage paths emit different metric names:

| Backend | Metric | Emitted? |
|---------|--------|----------|
| InMemory | `tokeira_storage_repository_operation_total` | ✓ |
| InMemory | `tokeira_storage_commit_transition_duration_seconds` | ✓ |
| InMemory | `tokeira_storage_load_run_duration_seconds` | ✓ |
| InMemory | `tokeira_storage_read_history_duration_seconds` | ✓ |
| DSQL | `tokeira_storage_dsql_operation_total` | ✓ |
| DSQL | `tokeira_storage_dsql_operation_duration_seconds` | ✓ |
| DSQL | `tokeira_storage_dsql_query_duration_seconds` | ✓ |
| DSQL | `tokeira_storage_dsql_shard_operation_total` | ✓ |
| DSQL | `tokeira_storage_dsql_shard_duration_seconds` | ✓ |
| DSQL | `tokeira_storage_repository_operation_total` | ✗ (not wired) |
| DSQL | `tokeira_storage_dsql_occ_conflict_total` | ✗ (not wired or no conflicts) |
| DSQL | `tokeira_storage_dsql_retry_total` | ✗ (not wired or no retries) |

The generic `tokeira_storage_repository_*` metrics from the Phase 2 observability foundation were wired into `InMemoryStore` but never into `DsqlRunRepository`. The DSQL path has its own metrics (`tokeira_storage_dsql_*`) but the dashboard's "Repository Operations" row queries the generic names — resulting in "No data" on DSQL deployments.

### Missing OCC/Retry Metrics

The `tokeira_storage_dsql_occ_conflict_total` and `tokeira_storage_dsql_retry_total` counters are either:
1. Not wired into the DSQL code paths (the recording calls exist in the lane retry loop but may not cover all conflict sources), or
2. No conflicts occurred during the bench run (unlikely given the projection OCC logs observed earlier)

Investigation needed: verify the recording call placement covers both the lane-level retry path and the projection sink conflict path.

### Recommended Dashboard Structure

The current single `storage-projection-health.json` dashboard tries to serve both backends but fails because the metric names diverge. Recommended split:

| Dashboard | Purpose | Metrics |
|-----------|---------|---------|
| `tokeira-server-health.json` | Always works regardless of backend | gRPC, broker, runtime, kernel metrics |
| `tokeira-dsql-storage.json` | DSQL-specific storage health | `tokeira_storage_dsql_*`, `tokeira_dsql_pool_*`, `tokeira_dsql_reservoir_*` |
| `tokeira-projection.json` | Projection pipeline health | `tokeira_projection_*` (same names for both backends) |

The "Repository Operations" row (generic metrics) should be removed from the DSQL dashboard. The DSQL dashboard should only reference `tokeira_storage_dsql_*` and `tokeira_dsql_pool_*` metrics.

### Fix 6: Unify or Split Storage Metrics (Priority: Medium)

**Option A — Emit generic metrics from DSQL path too:**

Wire `record_storage_operation`, `record_commit_transition_duration`, etc. into `DsqlRunRepository` alongside the DSQL-specific metrics. Both metric families are emitted. The generic dashboard works for both backends; the DSQL dashboard adds DSQL-specific detail.

**Option B — Separate dashboards per backend:**

Split into `tokeira-dsql-storage.json` and `tokeira-inmemory-storage.json`. Each queries only the metrics its backend emits. The compose platform generates the appropriate dashboard based on `StorageKind`.

**Recommendation:** Option A (emit both) — simpler for operators who switch between backends. The generic metrics provide a consistent baseline; DSQL metrics add depth. The cost is ~10 additional counter/histogram calls per operation, which is negligible.

## Schema Reset Requirement

The per-partition rollup fix changes the fresh `vis_rollup` schema by adding `partition_id` to the primary key. This is intentionally not a forward `ALTER TABLE` migration for already-applied DSQL databases. Existing compose+DSQL databases that have recorded the old `V039__vis_rollup.sql` migration must be reset before running this spec's code.

For development deployments, destroy and recreate the DSQL-backed schema before validation. Do not expect old rollup rows to coexist with the new partitioned key shape.

## Validation Benchmark

Run the validation benchmark after the self-assignment, projection retry, single-transaction projection apply, rollup partitioning, and DSQL defaulting fixes are applied:

```bash
cargo run -p tokeira-bench -- --workflows 2000 --concurrency 20
```

Prerequisites:

- Compose deployment uses DSQL storage with a freshly initialized schema.
- Effective placement has `shard_count = 32` and `partition_count = 4`.
- The benchmark process is pointed at the compose `tokeirad` endpoint.

Success criteria: sustained throughput should be at least **50 wf/s** from a developer machine. The expected ceiling for the original laptop-to-eu-west-2 setup remains roughly **128 wf/s** at concurrency 20 because each workflow still needs three sequential network-bound commits.
