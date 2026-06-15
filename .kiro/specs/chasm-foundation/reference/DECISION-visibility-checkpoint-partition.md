# Decision note — projection checkpoint keyed by `partition_id`

**For:** task 23.8 (port the DSQL projection checkpoint onto V055 `projection_checkpoint`;
design.md "Pipeline, checkpoints, isolation").
**Status:** decided. Conforms an internal Rust trait to an already-settled requirement +
committed migration; **Standard** change per root `AGENTS.md` § Change Classification (not
architectural — the "spec update or approval" the Architectural tier asks for already exists as
Req 10.9 + V055, and `VisibilityStore` is internal plumbing, not a `proto/upstream/` wire contract).

---

## What was already decided (not re-litigated here)

- **`partition_id` is the checkpoint key.** Committed in V055 `projection_checkpoint
  (partition_id PRIMARY KEY, last_applied_version BYTEA, updated_at)` (`31c6ca7c`) and specified
  in design.md:530 / Req 10.9 ("checkpoint projection progress per partition … so high-volume
  activity churn cannot starve workflow visibility").
- **`partition_id` is not invented here.** It is already a first-class field on
  `ProjectionCursor` (`partition_id: u32`, `fanout: u16`); the projection log already reads
  `WHERE partition_id = $1 AND fanout = $2` in `(partition_id, fanout, run_key, transition_seq)`
  order; and the bootstrap already spawns **one worker per partition**
  (`for partition_id in 0..partition_count`).

## The decisions this task makes

1. **Drop `sink_id` from the checkpoint contract.** The legacy `projector_checkpoint` keyed on
   `(sink_id, partition_id, fanout)` and the runtime smuggled the partition through the sink-id
   string (`"visibility-{partition_id}"`) while *also* carrying the real `partition_id` in the
   cursor — redundant. There is exactly one logical visibility consumer per partition, so the
   trait methods become `load_checkpoint(partition_id: u32)` and `save_checkpoint(&cursor)` (the
   partition comes from `cursor.partition_id`). `VisibilitySink`'s now-dead `sink_id` field,
   constructor parameter, and unused `sink_id()` getter are removed, and the bootstrap sink
   factory drops its `String` argument. A second *independent* projection consumer (not the
   visibility plane) would be a future schema change, not a today concern.

2. **`fanout` lives inside `last_applied_version`, with a resume-time guard.** V055 keys by
   `partition_id` alone, but the serialized cursor in `last_applied_version` still carries
   `fanout` (the cursor doc: "stored so that a fanout change can be detected"). The legacy
   composite key separated checkpoints per fanout for free; under a single-column key the worker
   restores that property explicitly: on resume it loads the stored cursor and **uses it only if
   `stored.fanout == expected.fanout`; otherwise it restarts the partition from the beginning.**
   This is safe because snapshot apply is idempotent + monotonic (Properties 12/13, apply-iff-
   newer): re-scanning already-applied transitions under the new partitioning is a no-op, never a
   regression. Keeping the resume policy in the worker (not the store) matches the worker module's
   existing ownership of replay/checkpoint semantics; the store stays a dumb keyed lookup.

## Out of scope

Retiring the legacy `projector_checkpoint` *migration file* (and the other `vis_*`/`sa_*`
build-phase migrations) is deferred: deleting a non-highest build-phase migration would create a
version gap. The code stops using `projector_checkpoint`; the table lingers empty until the
batched migration-retirement cleanup. (Tracked alongside 23.7's carved-out cleanup.)
