# Decision note — CHASM engine→projection visibility adapter (task 24.2)

**For:** task 24.2 (engine→projection adapter + bootstrap; design.md "The component contribution
interface" + "Delivery stages" §2).
**Status:** decided; the architectural choice is **settled by the spec**, the mappings follow
established 23.x conventions. **Standard** change per root `AGENTS.md` (internal trait widening +
an adapter conforming to a committed contract; no new dependency — `tokeira-runtime` already
depends on `tokeira-projection`).

## The one fork — settled by the spec, not by me

Where does an activity's post-commit snapshot get written: append to `projection_log` (unified
worker pipeline) or straight to the shared visibility store? **design.md:562-564 settles it**:

> "The adapter holds the engine + registry, reads the component state by `ExecutionKey` on close,
> and **writes the snapshot to the shared visibility store** (post-commit, off the correctness path)."

So: **the adapter writes the shared `VisibilityStore` directly**, post-commit. Workflows keep their
`projection_log → worker → store` path; activities take the direct adapter path. Both write the one
logical index. This is exactly the "**one log, two producers**" case the design proves safe
(design.md:403-416): apply is monotonic + idempotent on `(authority_epoch, source_transition_seq)`,
so two producers interleaving on the shared store affect only *freshness*, never correctness — which
is why a second DSQL writer to `execution_visibility_current` is sound, not a hazard.

## Threading (mirrors the existing `search_attributes` flow)

The typed layer (`chasm/typed.rs`) already holds the live, deserialized component and builds string
`search_attributes` from it on transition close. It now also builds the typed
`tokeira_chasm::VisibilitySnapshot` via [`VisibilityContributor`] and threads it through
`UpdateRequest`/`StartRequest` → engine → the visibility hook. No re-read/re-deserialize is needed
(the component is already in hand), and this realizes the design's "the component produces the
snapshot on transition close".

The engine's hook is **widened** from `record(key, Vec<(String,String)>)` to
`record(key, archetype_id, version, snapshot)` — the engine knows `archetype_id` (root metadata /
`StartRequest`) and the committed `VersionedTransition` at commit, which are the fields the adapter
stamps (per the 24.1 contract). The old string `SearchAttributeProvider` hook is superseded; per
24.1 it stays until no caller depends on it.

## Adapter (lives in `tokeira-runtime`)

`ProjectionVisibilitySink` implements the runtime's `VisibilitySink` trait and holds the projection
`ProjectionSink` (the `VisibilitySink<Store>` apply path). On `record(...)` it converts the chasm
snapshot to a `ProjectionRecord` and calls `ProjectionSink::apply(&record, partition_id)` — reusing
the *exact* apply path workflows use (row + SA-index + rollup + apply-iff-newer), so activities and
workflows share one apply implementation, not just one table.

## Mappings (chasm `VisibilitySnapshot` + `ExecutionKey` + `VersionedTransition` → `ProjectionContext`)

- `run_key` = the execution's `run_id` UUID (unique per run); `namespace_id` = parsed
  `ExecutionKey.namespace_id`; `run_id` = same UUID; `business_id` = `ExecutionKey.business_id`;
  `workflow_id` mirrors `business_id` (the generic business identifier).
- `archetype_id` = the engine's root `component_type_id` (activity's registry id).
- version `(authority_epoch, source_transition_seq)` = `(namespace_failover_version,
  transition_count)`; `transition_count`/`state_transition_count` = `transition_count`.
- `status_keyword` = `snapshot.status_keyword`; `lifecycle_state`: `Running`→`Open`,
  `Completed`/`Failed`→`Closed`.
- `workflow_type` = `snapshot.execution_type` (→ the generic `execution_type` column);
  `task_queue` = `snapshot.task_queue`; `execution_status` = `Running` placeholder — the typed enum
  is workflow-internal and not the query key (23.7); `status_keyword` is authoritative.
- `start_time`/`close_time` from the snapshot's unix-nanos; `search_attributes`/`memo` from snapshot.
- `partition_id` = `hash(run_key) % partition_count` (consistent with the workflow producer's
  hash partitioning); `fanout` = 1 (bootstrap default). For a direct store write `partition_id` is
  only cosmetic (no worker consumes it), but deriving it keeps metrics coherent.

## Reserved-field enforcement (Req 10.10)

The adapter rejects any reserved system-field name (`archetype`, `status`, `lifecycle_state`,
`namespace`, `run_id`, `business_id`) appearing in `snapshot.search_attributes`, so a component
cannot spoof a system field via a like-named user SA.

## Durability boundary

The post-commit write has a crash window (commit lands, projection write lost). That is **by design**
(Req 10.15 keeps it off the correctness path); Req 10.11's "transition-derived, repairable outbox"
guarantee is **task 26** (Stage 4), not 24.2. 24.2 does the best-effort post-commit write.
