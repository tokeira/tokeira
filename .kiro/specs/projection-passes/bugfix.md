# Bugfix Requirements Document

## Introduction

Every transition commit inserts an image of its run into `projection_log`: a full post-transition visibility
snapshot, keyed by `(partition_id, fanout, run_key, transition_seq)` (V010; `insert_projection_log` in
`crates/tokeira-storage/src/dsql/run_repository/commit.rs`). Projection workers apply images to the visibility store,
which serves List and Count. The store applies an image only when it is strictly newer than the run's stored row, by
`(authority_epoch, transition_seq)` (`upsert_execution_row` in `crates/tokeira-projection/src/dsql_store.rs`), so
images can be applied in any order, and more than once.

The workers read the log through a cursor, and they lose images:
- **Images below the cursor.** On DSQL a worker reads its partition in `(run_key, transition_seq)` order after its
  cursor (`read_from`, `crates/tokeira-storage/src/dsql/projection_log.rs:43-123`). Run keys are hashes, so most new
  runs, and later transitions of runs the cursor has passed, land below it and are never read. List and Count go stale.
- **Workers that end.** Every process runs a worker for every partition
  (`crates/tokeira-engine/src/lib.rs:3740-3758`), and every worker upserts its partition's one checkpoint row (V055;
  `save_checkpoint`, `crates/tokeira-projection/src/dsql_store.rs:383-410`). A write conflict on that row, or a failed
  read, ends the worker (`crates/tokeira-projection/src/worker.rs:130-188`), and nothing restarts it.
- **One bad image stops its partition.** A failed apply retries the same batch from the same cursor (`worker.rs:155-185`).
  An image that can never apply, such as one naming a search attribute its namespace hasn't registered
  (`dsql_store.rs:542-548`), holds back every later image of its partition.

[Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md) replaces the cursor:
`projection_log` becomes an unordered set of unapplied images, and a projector per partition runs passes from the head
that apply images and delete exactly the ones it observed, once each is applied or dominated by an image it applied. A
late image is at the head of the next pass, so nothing needs a cursor. This spec builds that projector.

Deleting images has one constraint. An unseeded run, one whose stored state predates the accumulator in run state,
takes its `TemporalUsedWorkerDeploymentVersions` list from its latest image when it is next loaded, so that image must
survive ([projection-accumulator](../projection-accumulator/bugfix.md) criteria 2.7 and 4.2). Once an image is applied,
though, the visibility store holds the same list:
- the attribute index stores each attribute's whole encoded value on every row it writes (V052;
  `upsert_search_attr_index_row` in `dsql_store.rs`);
- a keyword list is stored as given, order and duplicates included (`normalize_search_attr_value`,
  `crates/tokeira-projection/src/types.rs:84-104`);
- an image naming an attribute its namespace hasn't registered fails to apply, so it is never deleted.

So on DSQL a seed takes the newer of the run's latest image and its applied value, and the projector deletes applied
images freely. In memory, the store keeps the latest image of a run whose stored state is unseeded.

Three Count defects sit on the same apply path:
- **Counters after the commit.** The apply commits the row and its attribute rows, then upserts the rollup counters in
  separate statements (`dsql_store.rs:656-664`; `:213` for deletions). When their retries run out, the apply fails
  after its row committed. A retry finds the row current and skips the counters, so Count drifts.
- **A failed read as no row.** The pre-read of the stored row treats a failed read as no row (`get_row`,
  `dsql_store.rs:491-509`). An image applied after a failed pre-read takes its deltas from nothing, and counts its run
  twice.
- **A pre-read outside the transaction.** Two appliers of one run's images can take their deltas from the same stale
  pre-read. The guard keeps the row right; the counters drift.

The pre-read also binds the run key alone, while the primary key of `execution_visibility_current` is
`(namespace_id, archetype_id, run_key)` (V050), and the table keeps every run ever started
(`get_execution_row`, `dsql_store.rs:1864-1904`). Passes make that read once for each image they apply.

The in-memory store reads its log in commit order (`crates/tokeira-storage/src/memory.rs:2918-2948`), so it skips
nothing. It never deletes an image either: its log, and every snapshot that carries it, grows with each transition.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a commit inserts an image whose `(run_key, transition_seq)` sorts below a DSQL worker's cursor THEN the worker
never reads it, and List and Count keep the run's older row, or none.

1.2 WHEN two processes run the worker for one partition THEN both upsert its checkpoint row, the later commit fails
with a write conflict, and that worker ends. WHEN a worker's read fails THEN it ends. Nothing restarts either.

1.3 WHEN an image fails to apply THEN its worker retries the same batch, and no later image of its partition is
applied while that image keeps failing.

1.4 WHEN images are applied THEN nothing deletes them, on either store. `projection_log` grows with every transition,
and the seed lookup reads it by run key alone (`crates/tokeira-storage/src/dsql/run_repository/load.rs:10-12`).

1.5 WHEN an apply's rollup upserts run out of retries after its row committed THEN the apply fails, and its retry
skips the rollups because the row is current.

1.6 WHEN an apply's pre-read fails THEN the apply proceeds as if the run had no row, adding the run's counts without
removing its old ones.

1.7 WHEN two appliers apply images of one run at once THEN each can take its deltas from the same pre-read, made
outside its transaction, and the counts drift.

1.8 WHEN an apply pre-reads the stored row on DSQL THEN it filters on `run_key` alone.

### Expected Behavior (Correct)

2.1 A projector for a partition SHALL run passes. Each pass SHALL read the partition from its head, a page at a time
in `(run_key, transition_seq)` order, holding its position in memory for the rest of the pass only. The next pass
SHALL start at the head again. No cursor or checkpoint SHALL be read or written.

2.2 Within a page, the projector SHALL apply each run's newest image, by `(authority_epoch, transition_seq)`, through
the strictly-newer guard.

2.3 After a page, the projector SHALL delete each image it read whose run's stored row is then at least as new as the
image, naming each key, and no other row. WHEN the apply of a run's newest image fails THEN none of that run's images
in the page SHALL be deleted.

2.4 Deleting SHALL lose no run's accumulator:
- on DSQL, WHEN a load seeds an unseeded workflow run THEN it SHALL also read the run's applied
  `TemporalUsedWorkerDeploymentVersions` value from the visibility store, in the same snapshot as the state, and seed
  from whichever is newer: the latest image, or the applied value, where a visibility row without the value gives an
  empty list. A run with neither an image nor a visibility row seeds an empty list. The load SHALL fail, as
  projection-accumulator criterion 2.11 fails it for an image, when the visibility row identifies another run or is
  newer than the state;
- in memory, the store SHALL keep the latest image of a run whose stored state is unseeded, whatever the projector
  asks it to delete.

2.5 A partition SHALL have one projector in a deployment. The node that holds the partition's home bundle,
`bundle_for_partition(partition, bundle_count)` as for a task queue, SHALL run it while that home's acquisition is
active, and stop it when the acquisition ends. A node that assigns itself every home SHALL run every partition's
projector. Two projectors on one partition, as during a move, SHALL stay correct.

2.6 A failed read, apply or delete SHALL end neither the pass loop nor the projector. The projector SHALL count the
failure by kind, back off from 100 ms to at most 1 s, and continue. An image that failed SHALL wait for a later pass.

2.7 WHEN a pass applies nothing and deletes nothing THEN the next pass SHALL start 1 s later; otherwise it SHALL start
at once.

2.8 An apply SHALL, in one transaction, read the stored row by its full primary key, upsert the row under the guard,
replace the run's attribute rows, and upsert its rollup deltas. A write conflict SHALL retry the whole transaction,
with a fresh read. WHEN retries run out THEN no part of the image SHALL be applied. A failed read SHALL fail the
apply. A deletion tombstone SHALL be applied the same way.

2.9 The sink SHALL report whether an apply advanced the row or found it at least as new, and SHALL report a failure
as an error.

2.10 The projector SHALL apply the runs of different rollup stripes concurrently, at most one run of a stripe at a
time, within the projection class's connections.

2.11 The visibility store's checkpoint methods, and every read and write of `projection_checkpoint`, SHALL go. The
table SHALL stay until a later migration drops it.

2.12 For each partition, the projector SHALL report each pass's images read, applied, found current, deleted and
failed, its duration, and the age of the oldest image it read. The checkpoint metrics SHALL go.

2.13 The release that first deletes images SHALL carry a migration past the previous release's maximum readable
version, so that the nodes of every release that checks its schema at startup refuse its schema, as
[distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md) provides: those nodes seed from
images alone. A release before that check refuses nothing, so it SHALL say "Upgrade with every node stopped."

### Unchanged Behavior (Regression Prevention)

3.1 Each transition SHALL CONTINUE TO insert one image, with the same key and content, and no commit SHALL update or
delete an image.

3.2 The visibility store SHALL CONTINUE TO apply only strictly newer images, keep deletion tombstones, and return the
same List and Count results for the rows it holds.

3.3 A load of an unseeded run whose latest image is at least as new as its visibility row SHALL CONTINUE TO seed from
that image (projection-accumulator criteria 2.7-2.14), and a load of a durably ready run SHALL CONTINUE TO read
neither.

3.4 CHASM components SHALL CONTINUE TO apply their visibility rows directly after their commits, and the CHASM
visibility repair SHALL CONTINUE TO rebuild them. Neither depends on the projector; both gain the atomic apply.

3.5 In-memory snapshots SHALL CONTINUE TO carry the log as a list of images, so that snapshots decode across releases
in both directions.

3.6 Rollup stripes (`run_key % 16`) and projection partitions (`run_key % partition_count`) SHALL CONTINUE as they
are.

### Out of Scope

- **Batched multi-row applies** ([projection-batched-apply-and-failure-policies](../projection-batched-apply-and-failure-policies/requirements.md)).
  Its batching can follow; its cursor and checkpoint rules no longer apply.
- **Dropping `projection_checkpoint`.** A later migration drops it, once no node of an earlier release can run.
- **A dead-letter store** for images that can never apply. Such an image is retried each pass, and counted.
- **Images under the partition ids of an earlier partition count.** Neither the workers nor the projector read them.
