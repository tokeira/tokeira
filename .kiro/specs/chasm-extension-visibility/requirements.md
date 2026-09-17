# Requirements: CHASM extension visibility

## Introduction

Expose the existing archetype-neutral visibility projection to embedding applications
to support Deployment discovery without exposing components through Temporal workflow APIs. This
is a Tokeira-native in-process contract, with no upstream embedded API analog. The
v1.31.0 compatibility claim and existing Temporal query endpoints remain unchanged.
Embedded in-memory snapshot restart recovery is unsupported and outside this work.

## Target state and evidence

`Engine::chasm_visibility<C>(namespace_id)` returns a read-only query handle for a
registered root. It shares the engine's actual projection store. Existing
`VisibilityContributor` snapshots, attribute seeding and repair remain the producers.
The embedder authorizes namespace access; the handle is not a credential.

Baseline evidence before this slice:

- `crates/tokeira-engine/src/lib.rs`: `Engine::chasm` resolves registered roots;
  bootstrap owns the visibility query store but does not expose it.
- `crates/tokeira-projection/src/query_service.rs`: workflow/activity endpoints force
  their own archetype after filter compilation.
- `crates/tokeira-chasm-acceptance/tests/runtime.rs`: the resource projection already
  filters typed desired/observed generations through the real adapter.
- `crates/tokeira-projection/src/dsql_store.rs`: `list_executions` reads generic rows
  but `row_to_execution` currently returns no search attributes. The existing attribute
  index retains encoded typed values; component reads must hydrate them consistently.
- `crates/tokeira-runtime/src/chasm/repair.rs`: failed derived writes can be repaired
  from committed root data. Visibility is never authority for commands.

## Glossary

- **Scope:** fixed namespace UUID and registered root archetype id.
- **Summary:** one projected execution, not the component's authoritative state.
- **Cursor:** opaque continuation bound to scope and trimmed filter text.

## Contract policy

| Input/output | Policy | Failure/effect |
|---|---|---|
| root type | Must be registered; same lookup as typed commands | Unregistered root error |
| namespace UUID | Explicit fixed scope; caller authorizes access | Absent namespaces have no rows; predicates still require registered attributes |
| query | Existing typed visibility predicate grammar; blank means all in scope | Invalid query error; no mutation |
| page size | Default 100; range 1–1000 | Invalid page-size error |
| cursor | Versioned; scope/filter bound; existing default keyset order | Invalid/mismatched cursor error |
| summary | Namespace/archetype, business/run ids, status keyword, lifecycle, start/close times, version, typed search attributes and memo | Store errors propagate |
| count | Number matching the same scoped predicate; no grouping in this slice | Store errors propagate |

## Requirements

### Requirement 1: Public engine access

**User Story:** As an embedder, I want to discover my component executions without
depending on engine internals.

1. WHERE `chasm-extensions` is enabled, THE engine SHALL expose a query handle resolved
   from a registered root type and explicit namespace UUID.
2. IF the root is unregistered, THEN THE engine SHALL return the existing unregistered
   archetype error before querying storage.
3. THE handle SHALL share the store used by the engine's projection producers for both
   in-memory and DSQL startup paths.
4. THE handle SHALL document caller-owned authorization and eventual consistency;
   authoritative reads and mutations SHALL remain on the typed component handle.

### Requirement 2: Scoped queries and results

**User Story:** As a Cloud operator, I want accurate Deployment listings and counts
without confusing an operational failure with a closed Deployment.

1. THE list and count paths SHALL compile against the namespace attribute registry and
   force the handle's archetype independently of the caller predicate.
2. THE list SHALL return generic status and lifecycle separately, preserving typed
   attribute values, identity, memo and the projected version without workflow enum conversion.
3. THE count SHALL apply the same scope and predicate as list and exclude deletion tombstones.
4. THE API SHALL perform no authoritative writes, dispatch, attribute registration or repair.
5. THE existing Temporal workflow and activity query paths SHALL retain their behavior.
6. THE DSQL component list SHALL read rows and attributes in one transaction snapshot;
   empty list and tokenless text attribute values SHALL remain representable.

### Requirement 3: Pagination and validation

**User Story:** As an embedder, I want bounded, resumable listings with useful errors.

1. THE list SHALL reject page sizes outside 1–1000 and default to 100.
2. THE list SHALL use existing default keyset ordering; over unchanged projections,
   traversing pages SHALL return each matching row exactly once.
3. IF a cursor is malformed, has an unsupported version/order, or belongs to another
   namespace, archetype or trimmed query, THEN THE list SHALL reject it.
4. THE API SHALL distinguish invalid input from store failures.
5. THE API SHALL document that separate pages/counts are separate reads, not a frozen snapshot.

### Requirement 4: Consumer and store proof

**User Story:** As the engine owner, I want an independent archetype to prove the surface.

1. THE public-builder acceptance scenario SHALL list/count the resource after successful
   and failed operations with exact typed generation values while workflow listing stays empty.
2. THE tests SHALL cover namespace/archetype isolation, pagination, invalid queries/cursors,
   and generic statuses independently of lifecycle.
3. THE repair test SHALL prove a missing derived projection becomes queryable through this
   handle after repair from the same authoritative store, without snapshot restart support.
4. THE env-gated DSQL suite SHALL cover component summaries and typed attribute hydration.
