# Bugfix Requirements Document

## Introduction

A projection apply on DSQL is one transaction per image (`apply_row` in `crates/tokeira-projection/src/dsql_store.rs`;
[projection-passes](../projection-passes/bugfix.md) criterion 2.8). In that transaction the apply:
- reads the run's visibility row by its primary key;
- writes the image under the version guard;
- deletes every attribute-index row of the run;
- writes the image's attribute-index rows;
- adds its rollup deltas.

The attribute index, `execution_visibility_attr_index` (V052), holds:
- one row for each single-valued attribute;
- one row for each element of a `KeywordList`;
- one row for each distinct token of a `Text` value (`attr_cells`; `text_search_tokens` in
  `crates/tokeira-projection/src/types.rs`).

Each row also carries the attribute's whole encoded value in `value_data`. The List hydration of component executions
and the load's used-versions seed read the value from any one of the attribute's rows (`hydrate_component_attributes`;
`READ_APPLIED_PROJECTION_SEED` in `crates/tokeira-storage/src/dsql/run_repository/load.rs`; projection-passes
criterion 2.4).

DSQL refuses a transaction that writes more than 3,000 rows or 10 MiB. The search-attribute limits Tokeira enforces,
as v1.31.0 does, admit images far larger than that: up to 100 keys, 2 KiB a value and 40 KiB in all
(`crates/tokeira-kernel/src/limits.rs`; `common/searchattribute/validator.go:60-75, 145-175 @ v1.31.0`):
- A 2 KiB `Text` value holds about 690 distinct tokens of one or two characters.
- A 2 KiB `KeywordList` holds over 400 distinct elements.
- So one image within the limits can need about 13,000 index rows.
- An update deletes the previous image's rows in the same transaction, so it writes both sets.

v1.31.0 keeps a run's search attributes in one visibility record, however many tokens they hold:
- **SQL visibility**: one row per execution, with a `search_attributes` JSONB column. `Text` attributes are `TSVECTOR`
  columns and keyword lists JSONB columns generated from it, and a `_version` column rejects out-of-order upserts
  (`schema/postgresql/v12/visibility/schema.sql:19, 33, 85-90 @ v1.31.0`).
- **Elasticsearch**: one document per execution.

So v1.31.0 can index any image it accepts.

V052 replaced a run's attributes inside the apply's own transaction, on the expectation, recorded in its comment, that
attribute sets are tiny. The schema decision it departed from did otherwise: it wrote each image's attribute rows at the
image's version, and made them visible by advancing the run's row to that version. That keeps every transaction small
([chasm-foundation](../chasm-foundation/reference/DECISION-visibility-dsql-schema.md) Decision 3 and hazard H4).

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN an image's apply needs more than 3,000 rows or 10 MiB in its transaction (the run's row, the deletes of its
previous attribute-index rows, its new rows and its rollup deltas) THEN DSQL refuses it on every attempt. The run's
visibility row and attributes stay at the previous image, List and Count answer from them, and the image stays in the
log.

1.2 WHEN such an image is in its partition's log THEN every pass applies it again and fails again. Each attempt sends
one statement per index row, up to thousands of statements, before DSQL refuses the transaction.

1.3 WHEN an image writes N index rows for an attribute THEN every one of them repeats the attribute's whole encoded
value, so the index holds the value N times.

### Expected Behavior (Correct)

2.1 WHEN an image's attributes are within the search-attribute limits THEN its apply SHALL commit on DSQL, however many
index rows it needs, and every transaction the apply commits SHALL write at most 2,500 rows.

2.2 Index rows SHALL be written at their image's version, `(authority_epoch, source_transition_seq)`. Advancing the
run's visibility row to that version, under the version guard, SHALL make them the run's attributes. Queries SHALL
match only rows at the run's current version, so a query never matches rows of two images of one run, or part of one
image.

2.3 WHEN an image's rows fit in one transaction with the deletes of the run's older rows and the version guard's
writes THEN the apply SHALL keep that one transaction. Otherwise it SHALL first write the image's rows in transactions
of at most 2,500 rows, then advance the run's row in a transaction of its own, and then delete the older rows. Index
rows SHALL be written by statements of at most 128 rows, and both ways SHALL leave alone a row already written at the
image's version, as an interrupted or concurrent apply of the same image leaves one.

2.4 Rows below the run's current version SHALL be deleted once the version has advanced, in transactions of at most
2,500 rows. This cleanup SHALL never delete a row at or above the run's current version.

2.5 Each attribute's encoded value SHALL be stored once per image, at the image's version, written in the transaction
that advances the run's row. Index rows SHALL hold only what queries match. The component List hydration and the
load's used-versions seed (projection-passes criterion 2.4) SHALL read the value from there, at the run's current
version.

2.6 WHEN a node of this release, listener-backed or embedded, starts on a cluster whose attribute index holds rows
written by an earlier release THEN it SHALL copy them into the new layout before it builds anything that can apply an
image:
- each run's rows at the run's current version, one value per attribute;
- in transactions of at most 2,500 rows, each recording the copy's position durably, so a copy that stops part-way
  resumes where it stopped;
- recording completion durably when nothing is left to copy.
A copy transaction refused with a write conflict SHALL run again, so concurrent copies take turns and none fails its
node's start. No image SHALL be applied before completion is recorded, and a copy SHALL stop once it is.

2.7 The fix SHALL not change what List, Count, the component hydration or the seed returns for any run.

### Unchanged Behavior (Regression Prevention)

3.1 The version guard SHALL CONTINUE TO make an apply a no-op unless its image is strictly newer than the run's row. The
apply SHALL CONTINUE TO read that row by its whole primary key and to commit its rollup deltas with the row
(projection-passes criterion 2.8).

3.2 A run whose apply fails SHALL CONTINUE TO keep its images while its pass goes on (projection-passes).

3.3 Filters on custom attributes SHALL CONTINUE TO match as they do:
- equality on a keyword list's elements and on a `Text` value's tokens;
- the typed comparisons;
- `IS NULL` and negation;
- Count grouped by a custom attribute.

3.4 A deletion tombstone SHALL CONTINUE TO remove a run's attributes from every query.

3.5 The in-memory visibility store SHALL CONTINUE TO behave as it does. It has no transaction limits.

3.6 The search-attribute limits SHALL CONTINUE TO be checked at the edge as they are.

## Out of Scope

- A commit over 3,000 rows, such as a workflow task completion that schedules thousands of activities: it gets a
  design of its own.
- A worker deployment's record that outgrows a transaction.
- The plans of `Text` and range predicates, which read a namespace's index rows today. The new index keeps the plans
  the old one had.
- Dropping `execution_visibility_attr_index`. That takes a later migration, after a full release with the copy's
  completion recorded and no reads of the old table, as `crates/tokeira-storage/AGENTS.md` has it for CHASM's legacy
  pointers.
