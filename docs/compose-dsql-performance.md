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
| Bundle count | 1 (default) |
| Shard count | 1 (default) |

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

### 3. Single-Bundle Serialization (bundle_count = 1)

The `execution_home_bundle()` function hashes `(namespace_id, workflow_id) % bundle_count`. With `bundle_count = 1`, every workflow maps to bundle 0. The `commit_transition_for_bundle` path reads `shard_lease WHERE shard_id = <bundle_0_uuid>` for epoch fencing — all commits serialize on this single row.

Additionally, when `bundle_count > 1` is configured without a placement controller, the node never acquires leases for bundles 1–63, causing "not shard owner" rejections.

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

### Fix 3: Self-Assign Bundles in Single-Node Mode (Priority: High)

**Problem:** Without a placement controller, the node never acquires leases for bundles > 0. With `bundle_count = 1`, all commits serialize on one shard_lease row.

**Fix:** On startup, when `controller_endpoint` is not configured, the node should self-assign all bundles by inserting/updating `shard_lease` rows:

```rust
// In tokeirad startup, after runtime construction:
if config.infrastructure.placement.controller_endpoint.is_none() {
    let bundle_count = config.infrastructure.placement.bundle_count;
    for bundle_id in 0..bundle_count {
        repo.try_acquire_bundle(ShardId(bundle_id), node_id, Duration::from_secs(3600)).await?;
        runtime.shard_owner().record_acquired(ShardId(bundle_id), ShardEpoch(1));
    }
}
```

**Impact:** Enables `bundle_count > 1` for compose deployments. Distributes epoch-fence reads across multiple `shard_lease` rows, eliminating the single-row serialization point.

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
shard_count = 1
bundle_count = 1
partition_count = 4   # Reduce from 16 to limit projection concurrency
```

Expected throughput: **50–128 wf/s** (after Fix 1, limited by network RTT).

### For ECS+DSQL (co-located in same region):

```toml
[infrastructure.placement]
shard_count = 64
bundle_count = 64
partition_count = 16
```

Expected throughput: **1,000–3,000 wf/s** (with multi-node, multi-bundle distribution).

## Priority Order

1. **Fix 1** (projection OCC retry) — unblocks compose+DSQL immediately
2. **Fix 5** (single-transaction apply) — reduces pool pressure, makes apply atomic
3. **Fix 2** (vis_rollup redesign) — eliminates the OCC hotspot permanently
4. **Fix 3** (self-assign bundles) — enables multi-bundle compose for higher throughput
5. **Fix 4** (pool/budget tuning) — fine-tuning after the structural fixes land
