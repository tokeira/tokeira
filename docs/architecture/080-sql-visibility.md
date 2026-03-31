# 080 SQL Visibility

**Status:** draft for architecture review  
**Related docs:** [070-projection-plane](070-projection-plane.md), [050-dsql-storage](050-dsql-storage.md)

## Purpose

This document defines the **canonical visibility model** for Tokeira.

The design goal is to support Temporal-style list/filter/count/query workflows using **Aurora DSQL only**, without Elasticsearch. Temporal’s docs explicitly support visibility on SQL backends and document List Filters, Search Attributes, and Dual Visibility in that world.[^visibility][^list-filter][^search-attributes][^dual-visibility]

## Design claim

Tokeira should implement visibility as:

1. one **current execution row store**,
2. one **namespace-scoped search-attribute registry**,
3. one set of **typed side-index tables**,
4. a **query compiler** from Temporal’s list-filter language to DSQL SQL.

This is the canonical read model. Everything else is optional.

## Why SQL visibility is viable

Temporal documents that on supported SQL databases, visibility supports custom Search Attributes and SQL-like List Filters.[^visibility][^search-attributes] Temporal also documents that, on SQL backends, custom Search Attributes are namespace-scoped, whereas on Elasticsearch they are global.[^search-attributes]

That is a very good match for Tokeira because:

- DSQL has one database per cluster and limited schema/table counts,[^dsql-quotas]
- namespace-scoped attribute registries avoid global explosion,
- typed side tables map well to DSQL’s primary-key and index model.

## Why not one giant JSON document

Aurora DSQL recommends storing JSON as text and casting at query time rather than building a whole design around JSONB as the primary indexing substrate.[^dsql-json] That pushes Tokeira away from a document-first visibility design.

A JSON-heavy design also makes it harder to:

- choose selective indexes,
- paginate stably,
- perform count/group-by efficiently,
- evolve query plans predictably.

## Canonical tables

### `proj.vis_execution`

One row per run with the system fields most list views need:

- run key,
- namespace,
- workflow ID,
- run ID,
- workflow type,
- task queue,
- execution status,
- start / execution / close time,
- history length,
- transition count,
- memo blob,
- search-attribute version.

### `proj.sa_registry`

Maps `(namespace_id, attr_name)` to:

- `attr_id`,
- `attr_type`.

### `proj.sa_current`

Current value of each custom search attribute for a run.

### Typed index tables

- `sa_keyword_idx`
- `sa_keyword_list_idx`
- `sa_int_idx`
- `sa_bool_idx`
- `sa_datetime_idx`
- `sa_double_idx`
- `sa_text_token_idx`

## Why typed side tables

Typed side tables are the right fit here because:

- exact/range predicates want different sort orders,
- DSQL limits indexes per table,[^dsql-quotas]
- per-type storage avoids overloading one universal index,
- query compilation can choose the most selective driver.

This is a cleaner fit than trying to make `vis_execution` itself carry every access path.

## Attribute typing policy

Temporal’s Search Attribute docs make a strong practical distinction between `Keyword` and `Text`: `Keyword` is for exact whole-string values, while `Text` is tokenized and behaves like searchable text rather than identity.[^search-attributes]

Tokeira should adopt the same spirit:

- use `Keyword` for IDs, tenant codes, enums, business keys,
- use `Int`, `Bool`, `Datetime`, `Double` for structured predicates,
- use `Text` sparingly and model it as tokenized side-index data,
- do not rely on `Text` for stable ordering or exact identity matching.

## Query compiler

The query compiler should accept Temporal’s list-filter language and produce a DSQL-native plan.

Steps:

1. parse filter to AST,
2. normalize aliases/system attributes,
3. classify predicates by:
   - system row predicate,
   - custom SA predicate,
   - full-text token predicate,
4. estimate/select a driver predicate,
5. build candidate `run_key` sets via CTEs/subqueries,
6. join back to `vis_execution`,
7. apply stable pagination.

Aurora DSQL’s migration guide explicitly recommends CTEs and subqueries instead of temporary tables, which is exactly the shape this compiler should target.[^dsql-migration]

## Pagination

Temporal’s list behavior is naturally ordered around close time / start time, with closed workflows first when relevant.[^list-filter] Tokeira should expose a stable page token keyed by something like:

```text
(close_time DESC NULLS FIRST, start_time DESC, run_key DESC)
```

The page token should be opaque to clients but semantically represent the last emitted sort tuple.

## Count and group-by

Temporal’s visibility surface includes counting workflows and supports grouped counting in relevant contexts.[^list-filter][^visibility] Tokeira should support this in two ways:

### Direct count

For selective filters, compile to a direct count over candidate sets.

### Rollups

For common low-cardinality operational dimensions, maintain projection-driven rollup tables:

- by execution status,
- by workflow type,
- by task queue,
- by namespace,
- optionally by time bucket.

Rollups are an optimization, not the canonical source of truth.

## Custom Search Attribute limits and scope

Temporal’s docs specify supported SQL-backed custom Search Attribute types and note namespace scoping on SQL backends.[^search-attributes] Tokeira should mirror the namespace-scoped model from day one, because it avoids creating a fragile global registry that fights the single-database nature of DSQL.

## Memo handling

Memo should be stored for operator/debugging use but should not drive the main query plan.

Good rule:

- store memo as a blob or text field in `vis_execution`,
- only index specific memo-derived fields if they are promoted into explicit Search Attributes.

This keeps memo useful without turning it into accidental schema.

## Schema evolution

When a new frequently queried attribute or access path appears:

1. register the attribute in `sa_registry`,
2. backfill `sa_current` and its typed index table from projection replay or authoritative rebuild,
3. create supporting indexes asynchronously if needed.

Aurora DSQL’s `CREATE INDEX ASYNC` is a good operational fit because it avoids blocking base-table operations.[^dsql-create-index]

## Why this is better than Elasticsearch for the canonical path

For Tokeira, SQL visibility is the right canonical layer because:

- it keeps one operational model for correctness and canonical read paths,
- it matches DSQL constraints directly,
- it keeps search-attribute typing explicit,
- it leaves room for optional analytics/search sinks without forcing them into the critical path.

If later we add a specialized search/analytics sink, that should be additive.

## Suggested review discipline

For every new visibility requirement, ask:

1. Is this needed for the canonical Temporal-compatible API?
2. If yes, can it be expressed as a typed side index?
3. If no, should it be a custom sink instead?

This keeps the canonical SQL visibility plane from becoming an accidental warehouse.

## Review questions

1. Should the first milestone support only `Keyword`, `Int`, `Bool`, and `Datetime`, adding `Text` later?
2. Do we want count/group-by rollups in the first visibility milestone, or only raw query compilation?
3. Should `sa_current` be physically required, or can projection updates maintain typed indexes directly and treat `sa_current` as optional debug state?

## References

[^visibility]: Temporal Visibility docs: https://docs.temporal.io/visibility  
[^list-filter]: Temporal List Filter docs: https://docs.temporal.io/list-filter  
[^search-attributes]: Temporal Search Attributes docs: https://docs.temporal.io/search-attribute  
[^dual-visibility]: Temporal Dual Visibility docs: https://docs.temporal.io/dual-visibility  
[^dsql-migration]: Aurora DSQL migration guide: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html  
[^dsql-quotas]: Aurora DSQL quotas and limits: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html  
[^dsql-json]: Aurora DSQL supported data types / JSON guidance (same guide family): https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html  
[^dsql-create-index]: Aurora DSQL asynchronous index creation: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html
