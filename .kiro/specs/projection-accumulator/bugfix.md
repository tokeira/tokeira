# Bugfix Requirements Document

## Introduction

Every ordinary workflow transition commit reads the run's preceding projection image
to recover one field: the ordered `TemporalUsedWorkerDeploymentVersions` list. On
DSQL this adds a query and a decode inside the commit transaction; in memory it
requires a disposable index into the projection log. Applied images cannot safely
be deleted while they are the only durable copy of that list.

This change carries the same accumulator in run state and removes the lookup from
the commit path. It is the independent prerequisite identified in the Cost and
Dependencies sections of [Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md).
It implements no other part of that proposal.

The scope is deliberately storage-only. Temporal server **v1.31.0** remains the
behavioural authority; the differences documented below are existing defects,
preserved for a separate compatibility change. This spec does not claim that the
current projection derivation matches Temporal in those cases. The preservation
baseline is Tokeira commit `689e89a8622d114de1dd80232a22bb63705d50d1`.

## Terms and preservation model

- **Image:** the complete `ProjectionContext` appended for an ordinary committed
  workflow transition. A deletion tombstone is a separate, deliberately redacted
  image.
- **Accumulator:** the ordered list that the baseline merge retains from the
  preceding image. It is scoped to one run, not its execution chain.
- **Unseeded:** an existing run whose decoded state has no accumulator section.
- **Ready:** state whose accumulator is known, including an explicitly empty list.
  Readiness in a loaded copy does not imply that readiness has been persisted.
- **Durably ready:** the run's stored state carries the accumulator section.
- **Legacy writer:** a writer that ignores the new section and still derives the
  accumulator by reading the preceding image.
- **Equivalent successful transitions:** the same accepted transition sequence,
  post-transition workflow fields and history statistics on both derivations.
  Commits newly refused because the state encodes larger are the stated exception.

The baseline oracle is `workflow_projection_context_with_previous` at the baseline
commit. Let `P` be the preceding image and `S` the post-transition state:

1. Read `P`'s `TemporalUsedWorkerDeploymentVersions` keyword list, or start empty if
   there is no preceding image or no keyword list at that key.
2. Build the baseline current image from `S`. Its current-version observation is
   `deployment_name:build_id` from `S.versioning_info.deployment_version`, only when
   both components are non-empty.
3. Append each value from the current image's keyword list if it is not already in
   the accumulated list. Preserve the preceding list verbatim, including its order.
4. Install the accumulated list when non-empty. Otherwise leave the baseline
   current image unchanged, including the distinction between an absent attribute
   and an explicitly supplied empty value.

Steps 2 and 4 matter because `projection_context` first clones
`state.search_attributes`. The reference test must retain the complete baseline
function, including this handling; it must not replace it with a completion-only
model or call a production helper whose semantics changed with this fix.

The accumulator in a newly materialized reset run starts ready and empty: that
materialization writes no projection image. Its first ordinary projection commit
performs the same merge with no preceding image. Replaying a prefix is not a series
of projection commits for the new run.

## Evidence from current code

All Tokeira paths below refer to the preservation baseline.

| Evidence | Source |
|---|---|
| DSQL loads the last image by `run_key`, then writes the new image inside the commit transaction | `crates/tokeira-storage/src/dsql/run_repository/commit.rs:792-838`, `insert_projection_log` |
| The primary key begins with partition and fanout | `crates/tokeira-storage/migrations/V010__projection_log.sql` |
| The partition depends on configured partition count as well as run key | `crates/tokeira-storage/src/dsql/run_repository/mod.rs:667-671`, `partition_for`; `crates/tokeira-storage/src/dsql/config.rs:327-331` |
| In-memory commits use the preceding image through a disposable index | `crates/tokeira-storage/src/memory.rs:150-232, 1355-1368` |
| The merge and current-version observation | `crates/tokeira-storage/src/api.rs:1898-1957, 2082-2096` |
| Completion with unspecified behaviour clears the stored version | `crates/tokeira-kernel/src/state.rs:470-501`, `apply_wft_versioning` |
| Reset replay calls that same versioning helper | `crates/tokeira-kernel/src/kernel.rs:4144-4149`, `apply_replayed_event` |
| Reset materialization persists the replayed state and history without an image | `crates/tokeira-storage/src/dsql/run_repository/load.rs:306-462`; `crates/tokeira-storage/src/memory.rs:1520-1654` |
| Continue-as-new and child starts can inherit a stored deployment version | `crates/tokeira-runtime/src/runtime/workflow_task.rs`, `resolve_continue_as_new_versioning`, `resolve_child_versioning`; `crates/tokeira-kernel/src/kernel.rs:501-526, 4711-4729` |
| Both public commit entry points own their transition; successful commits return authoritative state | `crates/tokeira-storage/src/api.rs:125-135, 1030-1050` |
| The lane caches the returned state; OCC retries evict and reload it | `crates/tokeira-runtime/src/lane.rs:1566-1573, 1653-1654, 1697-1701` |
| Direct activity commits load run state outside the lane | `crates/tokeira-runtime/src/runtime/activity.rs`, heartbeat, completion, failure, start and retry paths |
| The section codec leaves positional state unchanged; snapshots reuse the run-state sections | `crates/tokeira-storage/src/codec.rs:13-41, 145-195`; `crates/tokeira-storage/src/memory.rs`, `snapshot_extension`, `apply_snapshot_extension` |
| Stored extension bytes contribute to measured state size | `crates/tokeira-storage/src/codec.rs:156-172`; `crates/tokeira-storage/src/growth.rs:88-93` |

There is no stale-lane argument against folding in storage: storage returns the
committed state and the lane uses it. The design must select the owner of the pure
fold while preserving the persisted-state/image/returned-state invariant below.
Folding only in `apply_wft_versioning` is insufficient: starts can inherit a
version, and reset replay must not accumulate the prefix's versions in this slice.

## Temporal ground truth and deferred compatibility corrections

The list is mutable run state in Temporal: `loadUsedDeploymentVersions` reads it
from `executionInfo.SearchAttributes`, and `saveUsedDeploymentVersions` writes it
there (`service/history/workflow/mutable_state_impl.go:3564-3585, 3696-3722 @ v1.31.0`).
Completed workflow tasks supply the deployment from their event to the update
helper (`service/history/workflow/workflow_task_state_machine.go:1293-1301, 1381-1389 @ v1.31.0`).
The helper appends a non-empty version only if absent
(`service/history/workflow/mutable_state_impl.go:3637-3662 @ v1.31.0`). The vendored
`proto/upstream/temporal/api/history/v1/message.proto`,
`WorkflowTaskCompletedEventAttributes`, defines the completion fields; it does not
define the accumulator lifecycle.

| Case | Tokeira behaviour preserved here | Temporal evidence for the deferred correction |
|---|---|---|
| Reset after a copied prefix completing on v1 then v2 | First projection commit observes only the final stored version, normally `[v2]` | Replay applies every completion using the same bookkeeping with an unlimited search-attribute size: `service/history/workflow/mutable_state_rebuilder.go:252-257`; `service/history/workflow/workflow_task_state_machine.go:250-257`; `service/history/ndc/workflow_resetter.go:471-492 @ v1.31.0` |
| New run with an inherited stored deployment version | First image can include the inherited version before a completion | Continue-as-new takes start search attributes from its command and start bookkeeping supplies no used version: `service/history/workflow/mutable_state_impl.go:2568, 2903-2904, 2982-2985 @ v1.31.0` |
| Completion reporting a deployment with unspecified workflow behaviour | No new observation from the cleared stored version; earlier entries remain | Reported deployment reaches the used-version update independently of behaviour: `service/history/workflow/workflow_task_state_machine.go:1293-1301, 1326-1345, 1381-1389 @ v1.31.0`; request validation does not forbid unspecified behaviour with versioned worker options: `service/history/api/respondworkflowtaskcompleted/api.go:216-225 @ v1.31.0` |
| List size | No trimming | Live saves remove oldest values until the encoded payload fits the value limit: `service/history/workflow/mutable_state_impl.go:3696-3722`; default 2 KiB: `common/dynamicconfig/constants.go:812-816 @ v1.31.0` |

These cases stay outside the correction set. In particular, migration copies the
legacy image's list; it does not repair that list from history. A future correction
must specify its own treatment of legacy runs, mixed-version writers and trimming.

## Bug Analysis

### Reproduction and bug condition

Start a run, commit a workflow task completion on v1, then commit another transition
on the same run. Observe the DSQL statements in the second commit: it reads
`projection_log.context_data` before writing the new image. The in-memory commit
performs the analogous `latest_projection` lookup. The read repeats on unversioned
runs and on transitions that observe no new version.

For a successful ordinary transition commit `T`, define `C(T)` as: the commit
retrieves a preceding projection image to construct its new image. The fix removes
`C(T)` by making the preceding accumulator available in ready run state before the
commit begins. A legacy load may still retrieve an image; moving that work out of
every commit is intentional.

### Current Behavior (Defect)

1.1 WHEN an ordinary DSQL workflow transition commits THEN the repository reads and
decodes the run's preceding projection image to recover the accumulator.

1.2 WHEN an ordinary in-memory workflow transition commits THEN the store looks up
the run's preceding projection image to recover the accumulator.

1.3 WHEN a run loses its preceding projection images THEN its hot state alone cannot
recover the accumulated list that those images retained.

### Expected Behavior (Correct)

2.1 WHEN either workflow commit entry point constructs an ordinary projection image,
THE repository SHALL derive its accumulator from ready run state using the baseline
merge, without retrieving any preceding projection image.

2.2 WHEN a commit succeeds, THE repository SHALL persist the resulting accumulator
atomically with that transition's state and projection image.

2.3 WHEN a commit returns `Applied`, THE repository SHALL return state containing
the same accumulator that it persisted and projected.

2.4 IF an existing-run transition reaches a commit with an unseeded accumulator,
THEN THE repository SHALL reject it as an internal readiness error before writing
any part of the transition, rather than reading an image inside the commit.

2.5 WHEN a fresh run is initialized, THE system SHALL provide a ready empty
accumulator as the input to its first projection-producing commit.

2.6 WHEN a reset successor is materialized, THE system SHALL initialize its
accumulator as ready and empty, regardless of versions in its replayed prefix.

2.7 WHEN `load_run` or `load_run_with_stats` loads an unseeded existing run, THE
repository SHALL return it ready using the accumulator from its latest committed
projection image visible in the same storage snapshot as the loaded state.

2.8 WHEN a legacy seed is read, THE repository SHALL locate it independently of the
current projection partition count; a miss in the currently computed partition is
not evidence that the run has no preceding image.

2.9 WHEN the consistent legacy lookup finds no image for the run, THE repository
SHALL seed an empty accumulator, matching the baseline's absent-previous-image
case, including a reset materialized by a legacy writer before its first image.

2.10 WHEN a legacy image has an absent or non-keyword-list accumulator attribute,
THE repository SHALL seed an empty list, matching the baseline merge.

2.11 IF a seed image is undecodable, identifies another run, or is newer than the
state loaded with it, THEN THE repository SHALL fail the load with an error naming
the run and the defect, without returning ready state.

2.12 WHEN a seed image precedes the loaded state's sequence, THE repository SHALL
retain its list as the baseline latest-image merge does; sequence equality alone
is not a readiness test for legacy state.

2.13 WHEN loading a durably ready run, THE repository SHALL perform no projection
image lookup for the accumulator, including when its persisted list is empty.

2.14 WHEN an older writer removes the section while writing a new baseline image,
THE next new-reader load SHALL seed from that image under criteria 2.7-2.12.

2.15 WHILE a loaded seed has not been written in a successful transition, THE system
SHALL treat it as non-durable for deletion eligibility, including after a no-op or
an unsuccessful commit.

2.16 WHEN ready state is encoded, THE codec SHALL persist the accumulator in
state-extension tag **3**, including an empty list, with the positional state
layout and envelope version unchanged.

2.17 WHEN the section is absent, THE codec SHALL decode the accumulator as unseeded.

2.18 WHEN encoding or decoding the section, THE codec SHALL obey
[activity-heartbeat-time Requirement 1](../activity-heartbeat-time/requirements.md#requirement-1-state-extension),
including a frozen payload layout, unique ordered emitted tags, strict framing and
unknown-tag skipping; tag 2 remains reserved for separate work.

2.19 WHEN the in-memory store snapshots or restores run state, THE store SHALL carry
the accumulator through its existing per-run state-extension mechanism.

2.20 WHEN an accumulator is durably ready, THE system SHALL preserve it across lane
eviction, process restart and loss of all applied projection images, provided no
legacy writer can subsequently rewrite the run.

2.21 WHEN checking a commit's growth, THE repository SHALL include the accumulator
section's actual encoded bytes, including its framing, in measured state size.

2.22 IF the additional encoded state crosses a growth threshold, THEN THE system
SHALL use the existing growth-limit response defined by
[run-growth-limits](../run-growth-limits/bugfix.md), without adding a separate limit.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN equivalent successful transitions commit, THE system SHALL emit the same complete
projection images as the baseline oracle, including list order, string formatting,
duplicate suppression for new observations and attribute presence.

3.2 WHEN a start carries an inherited stored deployment version, THE first image
SHALL CONTINUE TO include it as the baseline does, without copying the predecessor's
or parent's accumulated list.

3.3 WHEN a reset's first projection commit sees stored version v2 after replaying
completions on v1 then v2, THE image SHALL CONTINUE TO contain `[v2]`, not `[v1, v2]`.

3.4 WHEN an unspecified-behaviour completion clears the stored version, THE system
SHALL CONTINUE TO retain prior accumulated observations without adding the cleared
version from the completion parameter.

3.5 WHEN applying the baseline merge, THE system SHALL CONTINUE TO leave the
accumulated list untrimmed.

3.6 WHEN a transition is rejected, conflicts, is a duplicate or performs no durable
write, THE system SHALL CONTINUE TO leave the durable projection image unchanged.

3.7 WHEN a run is deleted, THE system SHALL CONTINUE TO emit its existing redacted
tombstone without exposing the accumulator through the new state field.

3.8 THE system SHALL CONTINUE TO use the existing fencing, OCC, request deduplication,
history accounting and growth thresholds.

### Constraint on later deletion work

4.1 WHILE legacy writers or rollback to them remain permitted, THE deleting system
SHALL retain the latest projection image of every extant run.

4.2 WHILE an extant run is not durably ready, THE deleting system SHALL retain its
latest projection image even after all legacy writers have been excluded.

These are independent conditions. Neither a minimum writer version nor a ready
in-memory copy is sufficient by itself. This spec adds no deleter, rollout control,
backfill or readiness scanner. It does not recover images lost before migration;
criterion 2.9 preserves the existing missing-image behaviour.

## Required verification

- A bug-condition exploration property test observes projection lookups on ordinary
  commits and fails against the baseline before the fix.
- A frozen baseline oracle and generated transition sequences compare complete
  images after every equivalent successful commit. Cases include repeated and
  changing versions, unspecified behaviour, non-versioning transitions, empty
  attributes, continue-as-new, child, retry and cron starts, and repeated resets.
  Each successor uses the initial routing state that its current production path
  actually supplies; the test must not invent inheritance for a path that lacks it.
- Named reset and inherited-version tests pin criteria 3.2-3.4. A reset materialized
  without an image is tested separately from an ordinary fresh start.
- Contract tests run against both stores: seeded-empty and seeded-nonempty commits
  perform no projection reads; an unseeded existing input is refused before writes.
  DSQL uses statement-level observation where the harness supports it.
- Load tests cover a same-snapshot seed, a concurrent commit, historical partition
  counts, older image sequences, no image, missing or differently typed attributes,
  corrupt images, and inconsistent run identity or sequence.
- Survival tests cover eviction before and after seed persistence, restart and
  in-memory snapshot restore, and continued commits after all applied images have
  been removed from a durably ready run with legacy writers excluded.
- An actual legacy-writer cycle ignores the section, performs the old image merge,
  rewrites without the section, then reloads through the new seeding path. It covers
  inherited initial versions and reset runs as well as ordinary v1-to-v2 changes.
- Codec properties cover round trips, an explicitly empty section, absence,
  unrelated known and unknown tags, deterministic ordering, and malformed framing.
- Growth tests measure the exact encoded section overhead and verify the existing
  response when it takes an otherwise accepted transition over the state-size limit.
- The design inventories every production source of state that can feed a commit:
  lane cold loads and OCC reloads; direct activity commits using `load_run`;
  `load_run_with_stats`; fresh starts; and reset materialization. Recovery candidate,
  dispatch and visibility reads must be classified by actual consumers rather than
  assumed to feed a lane cache. Any additional path found must satisfy readiness.

## Out of Scope

- Correcting the four Temporal differences above, or migrating old lists to
  completion-derived semantics.
- Changing routing or inheritance, introducing a new public field, or adding a
  user-visible configuration setting.
- History/event format changes, schema migrations, dependencies or lockfile changes.
- Projection pagination, image deletion, dispatch durability, atomic successor
  creation and other parts of the proposed durable-actionable-state architecture.

These requirements, the [design](design.md) and the [implementation plan](tasks.md)
are approved. Implementation belongs to a later task; no code is implemented by
these documents.
