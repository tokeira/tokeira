# Used Deployment Versions — Bugfix Design

## Overview

The list moves from storage's image preparation into the kernel, to the one place v1.31.0 changes it: a workflow task's
completion.
- **The completion adds its own version.** The live completion and its replay both add the version the completion
  reported, if absent, whatever the run's versioning behaviour.
- **A live completion trims.** When it changes the list, it removes the oldest versions until the list's JSON encoding
  fits the completion's search-attribute value limit. Replay doesn't trim, as v1.31.0's rebuild doesn't.
- **Storage only copies.** An image carries the run's list, and leaves the attribute out while the list is empty. It
  no longer observes the stored deployment version, so a start's inherited version and an unspecified-behaviour
  completion stop deciding the list.
- **Older runs keep their lists.** Nothing rebuilds a stored list from history. The next completion that adds a version
  trims it.

## Bug Details

### Bug Condition

A run's list differs from v1.31.0's when any of these happens: a reset copies completions on more than one version; a
start inherits a deployment version; a workflow task completes on a version with unspecified behaviour; or the list's
JSON encoding grows past the value limit.

### Examples

- **A reset.** A run's workflow tasks complete on `d:v1` and then on `d:v2`, and the run is reset after both. v1.31.0
  lists `["d:v1","d:v2"]`; Tokeira lists `["d:v2"]`.
- **An inherited version.** An auto-upgrade run on `d:v1` continues as new. Until the successor's first workflow task
  completes, v1.31.0 lists nothing for it; Tokeira lists `["d:v1"]`.
- **Unspecified behaviour.** A workflow task completes on `d:v3` and reports no versioning behaviour. v1.31.0 appends
  `d:v3`; Tokeira doesn't.
- **A long list.** A run completes on enough versions that its list encodes to more than 2,048 bytes. v1.31.0 drops the
  oldest until it fits; Tokeira keeps them all.

## Expected Behavior

The list is the fold of the run's recorded completions, in order: each appends its reported version when absent, and
each live completion that changes the list then trims it to the value limit. A reset's list is the same fold over the
copied completions, without trimming.

### Preservation Requirements

- An unseeded run is seeded as today.
- The other versioning attributes, `TemporalWorkerDeployment`, `TemporalWorkerDeploymentVersion`,
  `TemporalWorkflowVersioningBehavior` and `BuildIds`, are derived as today.
- The format and order of the list's entries don't change.

## Root Cause

The baseline derived the list from the image, not from the completion: `projection_context` puts the run's stored
deployment version into each image, and `prepare_workflow_projection` folds that into the stored list. The stored
version is the wrong input. A start can store an inherited one, an unspecified-behaviour completion clears it, and
replay leaves only the last. Nothing measured the list against the value limit.

## Correctness Properties

Property 1: The list is the fold of the completions

_For any_ sequence of a run's recorded workflow task completions, each with a reported version or none, and any other
writes in between, the run's list SHALL equal the fold that appends each completion's version when absent and then,
for a live completion that changed the list, removes the oldest versions until the list fits the limit.

**Validates: Requirements 2.1, 2.2, 2.3**

Property 2: A changed list fits

_For any_ live completion that changes the list, the list it leaves SHALL be empty or SHALL encode within the value
limit, measured as Go's `json.Marshal` measures it, and SHALL be the longest suffix of the untrimmed list that does.

**Validates: Requirements 2.3**

Property 3: Replay doesn't trim

_For any_ copied history, the list of the run a reset materializes SHALL equal the untrimmed fold of the copied
completions' versions.

**Validates: Requirements 2.4**

Property 4: Images carry the list

_For any_ write that produces an image, the image's `TemporalUsedWorkerDeploymentVersions` SHALL be the run's list
after the write when that list isn't empty, and SHALL be absent when it is.

**Validates: Requirements 2.5, 3.1**

## Fix Implementation

### The kernel (`crates/tokeira-kernel/src/state.rs`, `kernel.rs`, `payload_size.rs`)

- **The fold.** A new `WorkflowState::add_used_deployment_version(version, limit)` formats a version as `name:build`
  and appends it when absent. When it appended and `limit` is `Some`, it removes the oldest entries until
  `used_deployment_versions_len` is at most the limit or the list is empty. An unseeded list (`None`) is left alone:
  the repository refuses a commit from an unseeded run before it writes anything (projection-accumulator criterion
  2.4).
- **The live completion.** `apply_workflow_task_completed` calls it with the request's `deployment_version` and
  `Some(limits.search_attribute_value_size_limit)` where it records the `WorkflowTaskCompleted` event: after the
  branch that drops a speculative, rejection-only completion without recording it (K3 of
  [speculative-wft](../speculative-wft/design.md); v1.31.0's `skipWorkflowTaskCompletedEvent`,
  `service/history/workflow/workflow_task_state_machine.go:763-765 @ v1.31.0`), and before the commands. So a
  dropped completion adds nothing, as 2.2 requires. The edge derives that version as
  v1.31.0's completed event records it: from versioned deployment options, or else the deprecated deployment
  (`respond_completed_request_to_edge` in `crates/tokeira-edge/src/grpc/translate.rs`). So a completion that closes the
  run, or continues it as new, lists its version on the closing run, as in v1.31.0. A completion whose command fails
  its task is discarded whole before the runtime fails the task (`complete_workflow_task` in
  `crates/tokeira-runtime/src/runtime/workflow_task.rs`), so it adds nothing.
- **The replayed completion.** `apply_replayed_event` calls it with the event's `deployment_version` and `None`.
- **The measure.** A new `used_deployment_versions_len(&[String]) -> usize` in `payload_size.rs` returns the length of
  Go's `json.Marshal` of the list: 2 for an empty list, and otherwise 1 + 3n plus the escaped lengths of the n entries.
  An entry's escaped length is its UTF-8 length, plus 1 for each `"` or `\`, plus 1 for each `\b`, `\f`, `\n`, `\r` or
  `\t`, plus 5 for each other control character below U+0020, plus 5 for each `<`, `>` or `&`, and plus 3 for each
  U+2028 or U+2029. That is `serde_json`'s length with Go's extra escapes added. The short escapes for `\b` and `\f`
  date from Go 1.22's `encoding/json`, before which they were `\u0008` and `\u000c`; v1.31.0 builds with Go 1.26.2
  (`go.mod @ v1.31.0`).

### Storage (`crates/tokeira-storage/src/api.rs`)

- `projection_context` stops inserting `TemporalUsedWorkerDeploymentVersions` from the stored deployment version.
- `prepare_workflow_projection` stops folding. It still refuses an unseeded run, and inserts the run's list into the
  image when the list isn't empty.
- The projection-accumulator oracle (`crates/tokeira-storage/src/projection_accumulator_oracle.rs`) follows: its
  baseline image no longer observes the stored version.

### Release

The list is already in the run-state extension, so there is no schema change. Its release note says "Upgrade with
every node stopped." ([distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md)), so no older
writer folds a list after the upgrade. The fragment says once that a run's list follows v1.31.0's rules from its next
workflow task completion.

### Specs and documentation this changes

- [projection-accumulator](../projection-accumulator/bugfix.md): criteria 2.6 and 3.2-3.5 and its table of deferred
  corrections give way to this specification, and its design's fold moves to the kernel's completion.

## Testing Strategy

### Exploratory Bug Condition Checking

Before the fix, write one test per defect, 1.1-1.4, on the in-memory store, and confirm each fails on the current code
as predicted: the reset lists `["d:v2"]`, the successor lists its inherited version before a completion, the
unspecified-behaviour completion adds nothing, and the long list isn't trimmed.

### Unit Tests

- `add_used_deployment_version`: a present version adds nothing and doesn't trim; an appended version trims the oldest
  first and keeps the newest; a limit no single entry fits under empties the list; `None` stays `None`.
- `used_deployment_versions_len`: matches lengths recorded from Go's `json.Marshal` for ASCII, `"`, `\`, `<`, `>`, `&`,
  U+2028, U+2029, each class of control character, and multibyte text.
- The live completion passes the completion's limit and the replay passes none. A completion with unspecified
  behaviour appends its version; an unversioned completion appends nothing.
- A completion that closes the run, and one that continues it as new, list the version in the closing image; the
  successor's first image has no list.
- A completion whose command fails its task leaves the list unchanged, and so does a speculative, rejection-only
  completion that is dropped, even when it reports a version the list doesn't hold.
- A start with inherited versioning info, an override and a transition leave the list unchanged.
- The image carries the list, and omits the attribute while it is empty.

### Property-Based Tests

Properties 1 to 4, with `proptest`, over generated completions interleaved with other writes, starts with inherited
versions, and resets at generated points. Completions draw versions from a small pool, some none, with behaviours of
all three kinds.

### Preservation Checking

The projection-accumulator suites and the versioning functional suites still pass on both stores,
`TestVersioning3FunctionalSuite/TestSearchByUsedVersion` included.
