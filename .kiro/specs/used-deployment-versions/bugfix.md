# Bugfix Requirements Document

## Introduction

`TemporalUsedWorkerDeploymentVersions` is a system search attribute: a keyword list of the worker deployment versions
on which a run's workflow tasks have completed, each written `deployment-name:build-id`, oldest first. A client lists
workflows by it to find the runs a version has served. No client can set it: v1.31.0's validator refuses it at a start
and in any command that sets search attributes (`common/searchattribute/validator.go:113-122 @ v1.31.0`), and so does
Tokeira's (`is_banned_predefined_search_attribute` in `crates/tokeira-types/src/search_attributes.rs`).

Since [projection-accumulator](../projection-accumulator/bugfix.md), Tokeira keeps the list in run state, as
`WorkflowState::used_worker_deployment_versions`. That specification moved the list there exactly as Tokeira had
derived it, and deferred each difference from v1.31.0 to a change of its own. This is that change.

Today, each write that produces a visibility image observes the run's stored deployment version, and appends it to the
list if the list doesn't hold it (`prepare_workflow_projection` and `projection_context` in
`crates/tokeira-storage/src/api.rs`). The stored version is `versioning_info.deployment_version`, and two things set
it:
- **A workflow task's completion.** One with a specified versioning behaviour stores the completion's version, and one
  with unspecified behaviour clears it (`apply_wft_versioning` in `crates/tokeira-kernel/src/state.rs`).
- **A start that inherits a version.** A continue-as-new successor of an auto-upgrade run is one such start. The start
  stores the inherited version (`apply_start` and `apply_signal_with_start` in `crates/tokeira-kernel/src/kernel.rs`).

In v1.31.0 the list is mutable state, and only a recorded workflow task completion adds to it:
- **The completion adds its own version.** That is the version of a worker with versioned deployment options, or the
  deprecated deployment, as the completed event records it
  (`service/history/workflow/workflow_task_state_machine.go:825, 1294-1301 @ v1.31.0`). The completion passes it to
  the update whatever the run's versioning behaviour (`workflow_task_state_machine.go:1381-1389`). An unversioned
  worker's completion passes none.
- **Only once.** The version is appended when the list doesn't hold it
  (`addUsedDeploymentVersionToLoadedSearchAttribute`, `service/history/workflow/mutable_state_impl.go:3637-3662 @
  v1.31.0`).
- **Within the value limit.** If that changed the list, and the list's JSON encoding then passes the search-attribute
  value limit, 2 KiB by default, the oldest versions are removed until it fits (`saveUsedDeploymentVersions`,
  `mutable_state_impl.go:3696-3722`; `frontend.searchAttributesSizeOfValueLimit`,
  `common/dynamicconfig/constants.go:812-816 @ v1.31.0`). A list emptied that way is removed, not stored empty
  (`mutable_state_impl.go:7024-7031`; `common/payload/payload.go:73-76 @ v1.31.0`).
- **Not on a failed completion.** A completion whose command fails its task is discarded whole
  (`service/history/api/respondworkflowtaskcompleted/api.go:485, 1042-1045 @ v1.31.0`), and a speculative completion
  that is skipped adds nothing (`workflow_task_state_machine.go:763-765`).
- **Never at a start or an override.** A start passes no version, an inherited one included, and neither does a
  versioning override (`mutable_state_impl.go:2982-2985, 5589-5592 @ v1.31.0`). A successor's search attributes come
  from its command or from the previous run's started event, which can't hold this attribute
  (`mutable_state_impl.go:2568`; `service/history/workflow/retry.go:299 @ v1.31.0`), so every successor starts with
  none.
- **Replayed on reset.** A reset builds its run by replaying the copied history. The replay applies each completion's
  update with no size limit (`service/history/ndc/workflow_resetter.go:471-489`;
  `service/history/workflow/mutable_state_rebuilder.go:252-257`; `workflow_task_state_machine.go:250-257 @ v1.31.0`).

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a reset copies a history whose workflow tasks completed on version v1 and then on v2 THEN the reset run lists
only `[v2]`. Its list starts empty, and its first image observes the one version replay leaves stored.

1.2 WHEN a run starts with an inherited deployment version THEN its first image lists that version before any of its
workflow tasks has completed.

1.3 WHEN a workflow task completes on a deployment version with unspecified versioning behaviour THEN the version isn't
listed. The completion clears the stored version, so there is nothing to observe.

1.4 WHEN the list's JSON encoding passes the value limit THEN nothing trims it. Each new version makes it longer.

### Expected Behavior (Correct)

2.1 WHEN a workflow task's completion is recorded AND it reports a worker deployment version THE system SHALL append
`deployment-name:build-id` to the run's list, unless the list already holds it, whatever the run's versioning
behaviour.

2.2 THE list SHALL change only when a workflow task's completion is recorded, live or replayed. A start, a successor's
start, an inherited version, a versioning override, a deployment transition, a completion that fails its task, a
skipped speculative completion and every other write SHALL leave it as it is.

2.3 WHEN a live completion changes the list AND the list's JSON encoding passes the completion's search-attribute value
limit THE system SHALL remove the oldest versions until it fits or is empty. The encoding SHALL be measured as v1.31.0
measures it: the length of Go's `json.Marshal` of the list, which escapes `<`, `>`, `&`, U+2028 and U+2029. The limit
SHALL be 2 KiB, or the harness's override of it
([conformance-config-override](../conformance-config-override/requirements.md)).

2.4 WHEN a reset materializes its run THE run's list SHALL hold each version the copied history's completions reported,
in the order each first appeared, untrimmed. Versions reported only at or after the reset point SHALL be absent. The
next live completion that changes the list SHALL trim it.

2.5 THE run's visibility images SHALL carry the run's list as `TemporalUsedWorkerDeploymentVersions` while the list
isn't empty, and SHALL leave the attribute out while it is empty.

2.6 WHEN a run written before this change is loaded THE system SHALL keep its stored list as it is, and SHALL apply
2.1-2.3 from the run's next completion on. It SHALL NOT rebuild the list from the run's history.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN an unseeded run is loaded THE system SHALL CONTINUE TO seed its list from the newer of its latest image and its
applied visibility value ([projection-accumulator](../projection-accumulator/bugfix.md),
[projection-passes](../projection-passes/bugfix.md)).

3.2 THE system SHALL CONTINUE TO derive `TemporalWorkerDeployment`, `TemporalWorkerDeploymentVersion`,
`TemporalWorkflowVersioningBehavior` and `BuildIds` as it does today.

3.3 THE list SHALL CONTINUE TO write each version as `deployment-name:build-id`, oldest first, without duplicates.

3.4 WHEN a transition is rejected, conflicts, is a duplicate or writes nothing THE system SHALL CONTINUE TO leave the
list and the run's visibility image unchanged.

3.5 WHEN a run is deleted THE system SHALL CONTINUE TO write its tombstone as today.

## Scope

Criteria 2.6 and 3.2-3.5 of [projection-accumulator](../projection-accumulator/bugfix.md), which kept the four
differences above, give way to this specification.

Not covered: v1.31.0 checks an upsert's merged search attributes against the size limits with this list included, and
with the other attributes the server writes, such as `BuildIds` and the deployment attributes
(`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1250-1264 @ v1.31.0`).
Tokeira's check measures only the run's own search attributes and the upsert. That is a difference of its own, across
every attribute the server writes.
