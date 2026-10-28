# Projection Passes — Bugfix Design

## Overview

The projection workers' cursor goes, and a projector per partition runs passes over `projection_log`, as
[Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md) specifies:
- **The pass.** Each pass reads the partition from its head, applies each run's newest image through the
  strictly-newer guard, and deletes the images it read once their runs' rows are at least as new. The next pass starts
  at the head again, where any late image waits. Nothing durable records progress.
- **Deleting keeps the accumulator.** On DSQL, a seed takes the newer of the run's latest image and its applied value
  in the visibility store. In memory, the store keeps the latest image of an unseeded run.
- **One projector per partition.** It runs on the node that holds the partition's home bundle, while that home's
  acquisition lasts. A failure ends neither the projector nor its passes.
- **An exact apply.** The stored row's read, the row, its attribute rows and its rollup counters share one transaction,
  and the read binds the full primary key.
- **Checkpoints go.** Nothing reads or writes `projection_checkpoint`; a later migration drops it.

## Glossary

- **Image:** a `projection_log` row, a full post-transition visibility snapshot of one run (`ProjectionRecord` in
  `crates/tokeira-storage/src/api.rs`). Its version is `(authority_epoch, transition_seq)`.
- **Partition:** the log rows with one `(partition_id, fanout)`. A run's partition is `run_key % partition_count`
  (`partition_for` in `crates/tokeira-storage/src/dsql/run_repository/mod.rs`).
- **Pass:** one sweep of a partition from its head to its end, a page at a time.
- **Newest image:** the image with the greatest version among one run's images in a page.
- **Outcome:** what an apply reports. `Advanced` when the image became the stored row; `Current` when the stored row
  was already at least as new.
- **Home bundle:** `bundle_for_partition(partition, bundle_count)`, the bundle that is also home to the task queues
  in that partition (`PlacementQueueHomes` in `crates/tokeira-runtime/src/discovery.rs`).
- **Unseeded, durably ready:** as in [projection-accumulator](../projection-accumulator/bugfix.md). An unseeded run's
  stored state has no accumulator section; a durably ready run's has one.
- **Rollup stripe:** `run_key % 16` (`stripe_for` in `crates/tokeira-projection/src/rollup.rs`). A run's counter rows
  carry its stripe.

## The pass

```text
pass (partition):
  after := none
  loop:
    page := up to 256 images of the partition after `after`, in (run_key, transition_seq) order
    for each run in page, at most one run of a stripe at a time:
      outcome[run] := apply the run's newest image
    delete every image in page whose run's outcome is Advanced or Current
    after := the last key in page
    stop when the page is short
next pass: the head again, 1 s later if this pass applied and deleted nothing, otherwise at once
never:     a position that outlives its pass
```

- **The position lives inside one pass.** It keeps a pass moving across the whole partition, so new images at low keys
  can't hold back high ones. An image committed behind the position is at or after the head of the next pass.
- **Deletes name keys the pass read.** A late image the pass never saw survives to the next pass, and the projector
  never deletes a range. Every image of a run in a page is at most as new as the run's newest image there, so an
  `Advanced` or `Current` outcome leaves every one of them at most as new as the stored row.
- **Dominated images go too.** Deleting only applied images would leak: an image dominated by a newer one is never
  applied, so it would never go (042, Discovery).
- **A failure keeps its run's images.** When the apply of a run's newest image fails, none of the run's images in the
  page are deleted, and the next pass meets them again. A failed read or delete ends the pass early; the next pass
  starts after the backoff.
- **Pacing.** An idle partition costs one read a second. An image committed during an idle wait is applied within
  about a second, inside the 4 s that v1.31.0's functional tests allow visibility to settle
  (`WaitForESToSettle`, `tests/testcore/constants.go:11 @ v1.31.0`). A busy partition runs passes back to back.
- **Throughput.** Applying only each run's newest image skips the superseded ones. Runs of different stripes are
  applied concurrently, within the projection class's connections; their rows and counter rows are disjoint, so they
  never conflict. Batched multi-row applies can follow if live measurement asks for them.

## Deleting and the accumulator

An unseeded run takes its accumulator from its latest image when it is next loaded, until a successful transition
makes it durably ready (projection-accumulator criteria 2.7 and 2.15). Deleting that image must not lose the list.

**On DSQL, the visibility store holds it.** When a load seeds an unseeded workflow run, it reads the run's latest image
as now, and the run's applied value in the same snapshot, and seeds from the newer:

```sql
SELECT v.authority_epoch, v.source_transition_seq, v.business_id, v.run_id, a.value_data
FROM execution_visibility_current v
LEFT JOIN sa_registry r
  ON r.namespace_id = v.namespace_id AND r.attr_name = 'TemporalUsedWorkerDeploymentVersions'
LEFT JOIN execution_visibility_attr_value a
  ON a.namespace_id = v.namespace_id AND a.run_key = v.run_key
  AND a.authority_epoch = v.authority_epoch AND a.source_transition_seq = v.source_transition_seq
  AND a.attr_id = r.attr_id
WHERE v.namespace_id = $1 AND v.archetype_id = 0 AND v.run_key = $2
LIMIT 1
```

The newer of the two is exactly the newest image's list:
- **The newest image is applied before it goes.** An image is deleted only once its run's row is at least as new, and
  nothing dominates the newest image, so the row became that image before the delete. Its values were written at its
  version in the transaction that advanced the row ([bounded-visibility-apply](../bounded-visibility-apply/design.md)).
- **A snapshot without the image sees the applied row.** The apply committed before the delete, so a snapshot in which
  the delete committed also holds the apply. While the newest image is still in the log, it is at least as new as the
  row, and seeds as now.
- **An old image can't shadow a newer one.** An image left under an earlier partition count is never read, so it is
  never deleted. Taking the newer of the two keeps such an image from seeding once the run's newest image is gone.
- **The value is stored verbatim.** `value_data` in `execution_visibility_attr_value` is the encoded attribute as the
  image gave it, once per image (`ImageAttributes::build`, `crates/tokeira-projection/src/dsql_attributes.rs`), and a
  keyword list is not normalized (`normalize_search_attr_value`, `crates/tokeira-projection/src/types.rs`).
- **The edge cases keep criterion 2.10.** An empty list writes no attribute rows for a workflow, and seeds an empty
  list. A value of another type fails the apply (`dsql_store.rs:549-557`), so its image stays and seeds as before.
- **Only an image's apply writes a workflow row.** On DSQL, `upsert_execution_row` is the only writer of
  `execution_visibility_current`, called by the apply and by `VisibilityStore::upsert_execution`, which nothing on
  DSQL calls outside tests. CHASM writes rows of its own archetypes. A deletion's tombstone goes through the same
  apply (`apply_tombstone`), from the log or from `apply_deletion`. But the transaction that logs it also deletes the
  run's hot row, and the apply commits after that transaction, so a snapshot that holds an applied tombstone no longer
  loads the run, and a load never seeds from one.
- **The registry holds the attribute for good.** `TemporalUsedWorkerDeploymentVersions` is a predefined attribute,
  registered in every namespace (`seed_predefined_search_attributes`, `crates/tokeira-projection/src/system_attrs.rs`),
  and nothing removes a registered attribute: `RemoveSearchAttributes` is unimplemented
  (`crates/tokeira-edge/src/grpc/operator_service.rs`), and v1.31.0 refuses to remove a predefined one. A namespace
  without the registry row seeds an empty list, which is right: an image carrying the attribute couldn't have been
  applied there, so it is still in the log. The attribute's id is `deterministic_attr_id(namespace, name)`,
  registration inserts it `ON CONFLICT (namespace_id, attr_name) DO NOTHING`, and V030 makes that pair unique, so the
  join finds at most one registry row, and an index row's `attr_id` always matches it.

The applied value validates as criterion 2.11 validates an image. A row naming another workflow id or run id, or newer
than the loaded state, fails the load. A run with neither an image nor a visibility row seeds an empty list, as a run
with no image does now (criterion 2.9).

**In memory, the store keeps the image.** The in-memory store holds every run's decoded state, so `delete_images`
checks readiness directly. It keeps the latest image of a run whose stored state is unseeded, and deletes the rest.
Unseeded runs reach the in-memory store only from a snapshot written before the accumulator moved into run state.

**Earlier releases.** A node of an earlier release seeds from images alone, so it must not run once images are deleted
(projection-accumulator criterion 4.1). The release that first deletes images carries a migration past the previous
release's maximum readable version: dropping `activity_state` (V083) does, if it ships in that release, and so would
dropping `projection_checkpoint`. Every release that checks its schema at startup then refuses it
([distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md)). A release before that check
refuses nothing, so the release also says "Upgrade with every node stopped."

## Ownership

- **One projector per partition.** The node that holds a partition's home bundle runs its projector, as it serves that
  partition's task queues. A node that assigns itself every home runs every projector.
- **The acquisition supervises it.** The engine checks each partition's home once a second. It starts a projector when
  its node gains the home, with the acquisition's cancellation token, and the projector stops when the acquisition
  ends. The runtime exposes the lookup that `PlacementQueueHomes` already makes, keyed by partition.
- **Two projectors are safe.** During a move, an old and a new owner can run one partition. Each apply reads the
  stored row inside its transaction and writes it, so of two appliers of one run, the later commit fails with a write
  conflict and retries with a fresh read. Two runs of one stripe write the same counter rows, and conflict the same way.
  A delete names keys, and deleting a deleted key does nothing. Ownership saves work; it is not a fence, as for queue
  homes (042, Discovery).
- **Stripes and partitions.** When `partition_count` divides 16, as DSQL's default of 4 and the in-memory default of
  16 do, every run of a stripe falls in one partition, so one projector writes each counter row. Another count only
  adds conflicts, which retry.

## The apply

`DsqlVisibilityStore::apply` and `apply_deletion` become one transaction each:

```text
begin
  previous := the stored row, by (namespace_id, archetype_id, run_key)
  if the image is not strictly newer than previous: rollback; outcome Current
  upsert the row under the guard
  replace the run's attribute rows
  upsert the rollup deltas from previous to the image
commit                              -- a write conflict retries from begin
outcome Advanced
```

- **Exact counters.** The deltas, the row and the counters commit together, so a failure applies nothing, and a retry
  starts from the stored row. A failed read is an error, not an absent row.
- **The full key.** The image carries the namespace and archetype, so the read binds the whole primary key of
  `execution_visibility_current` (V050). `get_row`'s other callers keep their lookups.
- **Attributes first.** Attribute names resolve before the transaction, as they do now. An unknown attribute fails the
  apply before anything is written.
- **The outcome.** `ProjectionSink::apply` returns `Advanced` or `Current`. The CHASM adapter
  (`crates/tokeira-runtime/src/chasm/visibility_adapter.rs`) and the CHASM visibility repair ignore it.
- **In memory.** The visibility sink over the in-memory store resolves attributes before it writes, and only one
  projector applies a run's images, so it keeps its sequence of calls. It gains the outcome.

## Bug Details

### Bug Condition

The bug condition holds wherever a consumer of `projection_log` depends on a cursor, a checkpoint or a worker that
never restarts, wherever nothing deletes applied images, and wherever an apply commits its counters apart from its
row or treats a failed read as no row.

### Examples

- **A new run below the cursor.** On DSQL a worker has read up to run key `0x80…`. A run with key `0x10…` starts. The
  worker never reads its image, and List never shows the run.
- **A second process.** Two processes start. Within seconds, one of each partition's two workers ends on a checkpoint
  conflict, and the survivor still skips keys below its cursor.
- **An unregistered attribute.** An image names a search attribute its namespace doesn't register. Its worker retries
  that batch for as long as it runs, and no later image of the partition is applied.
- **Counters after the commit.** A rollup upsert conflicts six times. The row committed, the apply fails, and its retry
  finds the row current. The run's new status is never counted.
- **A failed pre-read.** A run moves from Running to Completed, and the pre-read fails. The apply counts Completed and
  leaves Running, so Count includes the run twice.

## Expected Behavior

### Preservation Requirements

- Commits insert images as they do now, and never write one again (3.1).
- The guard, tombstones, and List and Count over applied rows are unchanged (3.2).
- Seeding from an image, and loading a ready run, are unchanged (3.3).
- CHASM visibility is applied and repaired as now (3.4).
- In-memory snapshots carry the log as a list (3.5).
- Stripes and partitions are unchanged (3.6).

## Root Cause

- **A cursor over a hash order.** A key cursor assumes rows arrive in key order. Run keys are hashes, so commits arrive
  in no key order at all.
- **A shared, fatal checkpoint.** Every process writes one row per partition, a conflict on it isn't retried, and every
  error returns from the worker.
- **Head-of-line retries.** A failed batch is retried from the same cursor, so one failing image blocks the rest.
- **Nothing could delete.** Until run state carried the accumulator, images were its only durable copy. The
  accumulator now lives in run state, and the visibility store holds every applied value.
- **Counters apart from the row.** The deltas commit in later statements, from a pre-read made outside the
  transaction that returns no row when it fails.

## Correctness Properties

Property 1: Images converge

_For any_ runs and images, committed in any order and visible at any time, with any page size and any failures that
later clear, passes SHALL leave every run's row equal to its newest image, and every rollup counter equal to the count
over the rows.

**Validates: Requirements 2.1, 2.2, 2.3, 2.6, 2.8**

Property 2: Deleting is safe

_For any_ sequence of commits, applies and passes, an image SHALL be deleted only after its run's row is at least as new,
and every unseeded run's seed SHALL equal its newest image's list: on DSQL from the newer of its latest image and its
applied value, and in memory from the image the store keeps.

**Validates: Requirements 2.3, 2.4**

Property 3: Every image is reached

_For any_ partition, an image visible when a pass starts SHALL, absent failures, be applied or deleted by the end of
that pass.

**Validates: Requirements 2.1, 2.3, 2.7**

Property 4: Count is exact under faults

_For any_ apply, with a failure injected after any of its statements, a failed read, or a second applier of the same
run or stripe, every rollup counter SHALL equal the count over the rows once the images are applied.

**Validates: Requirements 2.5, 2.8, 2.10**

## Fix Implementation

### Storage (`crates/tokeira-storage`)

- **The log interface.** `ProjectionLog::read_from` and `ProjectionBatch` give way to:
  - `read_page(partition, fanout, after, limit)`: the images after `after`, or from the head, in
    `(run_key, transition_seq)` order;
  - `delete_images(partition, fanout, keys)`: deletes exactly the named images, and returns how many went.
- **DSQL** (`dsql/projection_log.rs`). `read_page` returns each image's key and reads a page of whole images in one
  query. If any `context_parts` count is present, it discards that page and reads it again with its
  `projection_image_part` rows in one `REPEATABLE READ READ ONLY` transaction, joining before decoding. A concurrent
  deletion therefore cannot leave a reader with a count but no parts. `delete_images` names at most 100
  `(run_key, transition_seq)` pairs, within one partition and fanout, and deletes their parts in the same transaction.
  It counts both images and parts toward 3,000 changed rows, rolling back and reducing the batch if necessary.
- **In memory** (`memory.rs`). The log becomes a map in key order, with each run's latest image indexed for seeding.
  `delete_images` keeps the latest image of a run whose stored state is unseeded. Snapshots still carry a list of
  images, and restore accepts any order. The disposable cursor index goes.
- **The applied value** (`dsql/run_repository/load.rs`). For an unseeded workflow run, the load runs the visibility
  query after `READ_LEGACY_PROJECTION`, in the same transaction, and seeds from the newer. A split image's context
  is joined with its parts in that transaction before the accumulator is seeded.
  `seed_workflow_projection_accumulator` in `api.rs` takes the image, the applied value's version, identity and value,
  or both, and validates each the same way.

### Projection (`crates/tokeira-projection`)

- **The sink.** `ProjectionSink::apply` returns `ApplyOutcome::{Advanced, Current}`.
- **The DSQL apply** (`dsql_store.rs`). `apply` and `apply_deletion` take the shape in [The apply](#the-apply), with
  the read by full key inside the transaction and the counters in it. `accumulate_rollup_striped`'s autocommit
  upserts become statements of the apply's transaction.
- **Large memos** (`dsql_store.rs`). The bounded apply's single transaction or staged flip writes a memo's first
  part and `memo_parts` count with the visibility row. Only a winning guarded upsert replaces
  `visibility_memo_part` rows and deletes the unused tail, in that transaction. Its row budget counts written and
  deleted parts. Stored-row reads join inside the apply's transaction; a list page containing a split memo is
  re-read with its parts in one read-only snapshot. Whole values and legacy rows have no count or overflow rows
  ([large-value-parts](../large-value-parts/design.md)).
- **Stripe locks** (`dsql_store.rs`). A counter conflict now aborts the whole apply under one retry budget, so the
  store holds one lock per rollup stripe, shared by its clones, and an apply holds its run's stripe for its whole
  transaction. Applies in one process, the projector's and CHASM's direct ones alike, never conflict on a counter row,
  and the projector's one run per stripe at a time (2.10) follows. Applies on different nodes can still conflict, and
  retry.
- **Grouped counts** (`dsql_store.rs`). The exploration tests found that `count_from_rollup` failed on DSQL whenever
  counter rows existed: `SUM` over the BIGINT `counter` is NUMERIC, which doesn't decode as `i64`. It casts the sum back
  to BIGINT, so an unfiltered Count grouped by status, type or queue answers again.
- **The projector** (`worker.rs`, renamed `projector.rs`). `Projector` replaces `ProjectionWorker`: passes, newest
  images, the deletion rule, concurrency by stripe, backoff and pacing.
- **Checkpoints.** `load_checkpoint` and `save_checkpoint` leave `VisibilityStore` and both stores, with the cursor
  codec they use. `projection_checkpoint` stays until a later migration.
- **Metrics** (`metrics.rs`). Per pass and partition: images read, applied, found current, deleted and failed by kind,
  and the pass's duration; on DSQL, the age of the oldest image read, from `created_at`. The checkpoint metrics go.

### Runtime and engine

- **The runtime** exposes a partition's home acquisition, the lookup `PlacementQueueHomes::local_home` makes for a
  queue, keyed by partition.
- **The engine** (`crates/tokeira-engine/src/lib.rs`) replaces the spawn of one worker per partition with a
  supervisor that checks each partition's home once a second and starts its projector under the acquisition's token.

### Release

A `fixed` fragment says that List and Count no longer miss transitions or stop updating, and that the projection log
no longer grows without bound. The release that carries it says "Upgrade with every node stopped." once, unless an
unreleased fragment already does.

### Specs and documentation this changes

Each says what holds once passes replace the cursor:
- [dsql-projection-persistence](../dsql-projection-persistence/requirements.md): `read_from`, cursor advancement and
  checkpoint management;
- [projection-visibility](../projection-visibility/requirements.md): its note on checkpoint management;
- [projection-batched-apply-and-failure-policies](../projection-batched-apply-and-failure-policies/requirements.md):
  its cursor, checkpoint and poison-record rules, which assume a cursor;
- [projection-accumulator](../projection-accumulator/bugfix.md): criterion 2.7 gains the fallback, and criteria 2.15,
  4.1 and 4.2 say how deleting meets them;
- the visibility checkpoint decision in [chasm-foundation](../chasm-foundation/reference/DECISION-visibility-checkpoint-partition.md),
  which passes retire;
- the observability specs that name checkpoint metrics: [dsql-observability-metrics](../dsql-observability-metrics/requirements.md),
  [observability-foundation](../observability-foundation/requirements.md) and
  [observability-production](../observability-production/requirements.md);
- [Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md): the projector's ownership by
  home bundle.

The code change sweeps every other mention of the worker, its cursor and the checkpoint.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, each failing there:
- **live:** an image whose key sorts below a DSQL worker's cursor is never applied;
- **live:** two workers on one partition, and one ends on a checkpoint conflict;
- **offline:** an image that always fails to apply stops every later image of its partition;
- **live:** a rollup failure injected after the row commits leaves Count wrong;
- **live:** a pre-read failure leaves the run counted twice.

### Property-Based Tests

- Property 1 runs offline over the in-memory store and a test log that keeps DSQL's key order, with generated runs,
  commit interleavings, page sizes and transient failures.
- Property 2 runs offline over the in-memory store with unseeded runs restored from a snapshot, and live for the DSQL
  fallback.
- Property 3 runs offline over the test log.
- Property 4 runs live, with injected failures and two projectors, and offline over a fault-injecting store for the
  projector's handling of each outcome.

### Unit Tests

- Each run's newest image, the deletion rule for each outcome and for a failure, pacing, backoff and the metrics.
- The applied value's cases: no visibility row, no value, an image older than the row, an image as new as it, a row
  newer than the state, another run's row.
- The in-memory store keeps the latest image of an unseeded run, and deletes everything else it is asked to.
- `read_page` and `delete_images` on both stores, including deleting a key twice.

### Live Test

On an ephemeral cluster:
1. Commit images below and above a pass's position, and late during a pass. The next pass applies all of them.
2. Run two runtimes with a projector each on one partition. Neither ends, and Count is exact.
3. Leave an unseeded run whose image was applied and deleted, and one with an older image left under another partition
   id. Each run's next load seeds its newest image's list from the visibility store.
4. Inject a counter failure and a read failure into applies. Count is exact afterwards.
5. `EXPLAIN` the page read, the delete and the apply's read. Each uses the primary key.
6. Measure applies a second for one partition, serially and by stripe, and record them with the run's evidence.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test
that covers it:
- a pass that starts after the previous pass's last key, which live step 1 catches;
- deleting only applied images, which leaves a dominated image for ever;
- deleting before applying, which live step 3 catches;
- the counters after the commit, which live step 4 catches;
- a failed read as no row, which live step 4 catches;
- seeding from the image alone, which live step 3 catches.

### Preservation Checking

The visibility query tests, the accumulator suite on both stores and live, the CHASM visibility tests and the
snapshot decode tests stay green.
