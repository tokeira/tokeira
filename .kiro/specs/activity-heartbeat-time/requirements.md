# Requirements Document: Activity Heartbeat Time

## Introduction

Temporal v1.31.0 stores the time of every heartbeat in the activity's durable info. `UpdateActivityProgress` sets `LastHeartbeatUpdateTime` together with the heartbeat details (`service/history/workflow/mutable_state_impl.go:1956-1966 @ v1.31.0`). The heartbeat deadline is computed from that stored time on whichever history host evaluates the activity (`timer_sequence.go:341-351`). `DescribeWorkflowExecution` reports it as `PendingActivityInfo.LastHeartbeatTime` (`workflow/activity.go:147-150`).

Tokeira stores heartbeat details in the activity's durable state, but keeps the heartbeat time only in the owning node's volatile timeout tracking. When another node takes the shard, the recovery sweep rebuilds tracking with no heartbeat time, so the deadline runs from the attempt's start. An activity whose attempt has run for longer than its heartbeat timeout is then timed out at the new owner's first scan, even though its worker is alive and heartbeating. runtime-sweeper-recovery Requirement 8.6 accepted this as a trade-off. Describe never reports the time.

Kernel state is persisted with postcard, which is positional. Adding a field to `WorkflowState` directly would make every stored state unreadable. This feature defines a compatible way to add data to a stored state: an extension appended after the state, inside the existing envelope. It then uses that extension to persist each activity's last heartbeat time.

- States written by Tokeira 0.2.0 through 0.5.1 still decode; their activities have no time.
- States written by this feature still decode in 0.2.0–0.5.1; those releases ignore the extension.

The target behaviour is Temporal v1.31.0's.

Depends on:
- continue-as-new-advice Requirement 10: the hot-state envelope.
- recovery-index: the sweep derives activity entries from the run's state.
- runtime-sweeper-recovery: the sweep and activity timeout tracking.
- inmemory-store-snapshots: the snapshot format.
- api-conformance-activity-events: heartbeat details in durable activity state.

Amends:
- runtime-sweeper-recovery Requirement 8.6.
- inmemory-store-snapshots Requirement 2.3.
- continue-as-new-advice Requirement 10: adds criterion 10.8.

## Glossary

- **Last_Heartbeat_Time**: the instant an activity's progress was last recorded; v1.31.0's `ActivityInfo.LastHeartbeatUpdateTime`.
- **Progress_Record**: one update that stores an activity's heartbeat details and sets its Last_Heartbeat_Time to the current time (`UpdateActivityProgress`, `mutable_state_impl.go:1956-1966`).
- **Heartbeat_Reset**: an operator request that clears an activity's heartbeat details and Last_Heartbeat_Time:
  - `UnpauseActivity` with reset heartbeat (`activity.go:401-404`);
  - `ResetActivity` with reset heartbeats. For a scheduled activity it applies at once (`activity.go:361-365`). For a started activity it applies when the next attempt is prepared (`activity.go:86-90`).
- **Hot_State_Blob**: the bytes in `workflow_hot.state_data`: the envelope version, then the State_Layout, then, optionally, a State_Extension.
- **State_Layout**: the positional postcard encoding of `WorkflowState` that Tokeira 0.2.0–0.5.1 write after the envelope version.
- **Extension_Section**: a 32-bit tag and a byte payload. The payload layout is fixed for that tag.
- **State_Extension**: the bytes after the State_Layout in a Hot_State_Blob: the state extension magic, then a sequence of Extension_Sections.
- **Heartbeat_Section**: the Extension_Section of a State_Extension that holds each activity's Last_Heartbeat_Time.
- **Pre-Extension_Reader**: the hot-state decoder of Tokeira 0.2.0–0.5.1. It checks the envelope version, then decodes the State_Layout with `postcard::from_bytes`, which ignores any bytes after the decoded value.
- **Sweep**: `sweep_shard`, the one-time rebuild of a shard's volatile state on takeover (`crates/tokeira-runtime/src/recovery.rs:101`).
- **Activity_Timeout_Scanner**: the runtime task that evaluates tracked activities against their timeouts (`scan_activity_timeouts_once`, `crates/tokeira-runtime/src/activity_timeout.rs:354`).
- **Heartbeat_Deadline**: the later of the current attempt's start and the activity's Last_Heartbeat_Time, plus the heartbeat timeout (`timer_sequence.go:341-351`).
- **Snapshot_Extension**: the bytes after the snapshot document in an in-memory snapshot: the snapshot extension magic, then a sequence of Extension_Sections.

## Target State

- Each Progress_Record persists the Last_Heartbeat_Time in the commit that persists the heartbeat details. No commit or statement is added.
- The heartbeat deadline is computed as v1.31.0 computes it, from durable state, on whichever node owns the shard.
- `DescribeWorkflowExecution` reports `last_heartbeat_time`.
- Hot_State_Blobs written by 0.2.0–0.5.1 decode unchanged.
- Blobs written by this feature decode in 0.2.0–0.5.1, without the times.
- No schema migration, backfill or cluster recreation is needed.
- In-memory snapshots written by 0.2.0–0.5.1 still restore, and snapshots carry the times.
- The State_Extension can be reused: a later feature adds data to stored state with a new Extension_Section, without a new envelope version.
- Out of scope:
  - **Blob-size limits on heartbeat details.** v1.31.0 drops oversize failure details and returns a server failure to the worker (`frontend/workflow_handler.go:1804-1822`). Tokeira enforces no blob-size limit on heartbeat details today. [payload-admission-limits](../payload-admission-limits/bugfix.md) adds it (criteria 2.7 and 2.8).
  - **Standalone (CHASM) activities.** They already persist their heartbeat time (`crates/tokeira-chasm-activity/src/state.rs:339`).
  - **The `activity_state` side-table blob.** It keeps its layout and carries no time. No reader has used it since recovery-index.
  - **History batch blobs.**

## Evidence From Current Code

**Temporal v1.31.0** (all paths under `service/` at tag `v1.31.0`):

- **Every accepted heartbeat records progress.** RecordActivityTaskHeartbeat validates the token and then calls `UpdateActivityProgress`, whether or not the activity is paused or cancel-requested (`history/api/recordactivitytaskheartbeat/api.go:86-87`). `UpdateActivityProgress` sets `LastHeartbeatDetails` and then `LastHeartbeatUpdateTime = now` (`history/workflow/mutable_state_impl.go:1964-1966`).
- **A failure that carries heartbeat details records progress too.** RespondActivityTaskFailed calls `UpdateActivityProgress` when `LastHeartbeatDetails` is set (`history/api/respondactivitytaskfailed/api.go:87-94`), before `RetryActivity` (`:105`). The frontend forwards the whole request (`frontend/workflow_handler.go:1842-1845`).
- **RespondActivityTaskFailedById does not.** The frontend builds the history request with only the task token, the failure and the identity (`frontend/workflow_handler.go:1965-1969`).
- **Retries keep the time; resets clear it.**
  - `UpdateActivityInfoForRetries` clears the details and the time only when `ResetHeartbeats` is set (`history/workflow/activity.go:86-90`).
  - `ResetActivity` clears both for a scheduled activity when asked (`activity.go:361-365`).
  - `UnpauseActivity` clears both when asked (`activity.go:401-404`).
  - Starting an attempt does not change the time.
- **Deadline.** The last heartbeat starts as `StartedTime` and is replaced by `LastHeartbeatUpdateTime` when that is later. The deadline is that time plus the heartbeat timeout (`history/workflow/timer_sequence.go:341-351`).
- **Describe.** `GetPendingActivityInfo` sets `LastHeartbeatTime` and `HeartbeatDetails` when `LastHeartbeatUpdateTime` is set and non-zero (`history/workflow/activity.go:147-150`).

**Tokeira:**

- **Heartbeat commit.** `record_activity_heartbeat` writes the details to durable state (`crates/tokeira-runtime/src/runtime/activity.rs:588`). It records the time only in volatile tracking, from a second clock read after the commit (`activity.rs:645-649`, `crates/tokeira-runtime/src/activity_timeout.rs:175-185`).
- **Failure path.** For workflow activities, the edge drops `last_heartbeat_details` from RespondActivityTaskFailed (`crates/tokeira-edge/src/grpc/translate.rs:5957-5979`). The runtime's `fail_activity_task` takes no details (`runtime/activity.rs:388-396`).
- **Clears.** Three sites clear heartbeat details:
  - kernel unpause with reset heartbeat (`crates/tokeira-kernel/src/kernel.rs:1330-1332`);
  - kernel reset of a scheduled activity with reset heartbeat (`kernel.rs:1413-1417`);
  - retry preparation with `reset_heartbeats` set (`runtime/activity.rs:1432-1434`).
- **Deadline today.** `evaluate_activity_timeout` uses `last_heartbeat_at.unwrap_or(started_at)` (`activity_timeout.rs:321`). The sweep and the reset-successor path build tracking with `last_heartbeat_at: None` (`crates/tokeira-runtime/src/recovery.rs:183`, `crates/tokeira-runtime/src/lane.rs:767`).
- **Describe today.**
  - `PendingActivityDescription` has no heartbeat time (`crates/tokeira-edge/src/translate/mod.rs:483-510`).
  - `pending_activity_to_proto` leaves `last_heartbeat_time` unset (`crates/tokeira-edge/src/grpc/translate.rs:3592-3640`).
  - edge-complete-implementation Requirement 4.1, criterion 2, lists `last_heartbeat_time` among the fields of each `PendingActivityInfo`.
- **Hot-state format.**
  - `encode_workflow_state` writes `(version, state)` with postcard. `decode_workflow_state` checks the version and decodes the state with `postcard::from_bytes` (`crates/tokeira-storage/src/codec.rs:72-89, 120-149`).
  - Every release from 0.2.0 to 0.5.1 decodes the same way, with postcard 1.1.3.
  - That version's `from_bytes` returns once the value is decoded and ignores any bytes after it (`postcard-1.1.3/src/de/mod.rs:12-19`).
- **Snapshot format.**
  - A snapshot is a version stamp followed by the postcard document. Restore refuses any other version and any trailing bytes (`crates/tokeira-storage/src/memory.rs:366-420`).
  - The version has been 4 since 0.2.0.
  - The embedded engine restores its snapshot at boot and persists it at shutdown (`crates/tokeira-engine/src/lib.rs:1912-1990`).

## Field / Contract Policy

| Field | Today | Target | Persistence | Source |
|---|---|---|---|---|
| `RecordActivityTaskHeartbeatRequest.details` and the by-id variant | Stored as heartbeat details | Unchanged; the same commit sets the Last_Heartbeat_Time | Heartbeat_Section | `recordactivitytaskheartbeat/api.go:86-87` |
| `RespondActivityTaskFailedRequest.last_heartbeat_details` (workflow activities) | Dropped | A Progress_Record, applied before any pending Heartbeat_Reset | Heartbeat details and Heartbeat_Section, when the activity is retried | `respondactivitytaskfailed/api.go:87-94` |
| `RespondActivityTaskFailedByIdRequest.last_heartbeat_details` | Dropped | Dropped | None | `frontend/workflow_handler.go:1965-1969` |
| `PendingActivityInfo.last_heartbeat_time` | Unset | The Last_Heartbeat_Time, when set | Read only | `activity.go:147-150` |
| `PendingActivityInfo.heartbeat_details` | The details, when present | Unchanged (Requirement 6.3) | Read only | `activity.go:147-150` |

No request gains a new validation error.

## Requirements

### Requirement 1: State extension

**User Story:** As a storage developer, I want to add data to a stored workflow state without changing its positional layout, so that stored states stay readable across releases.

#### Acceptance Criteria

1. THE storage codec SHALL write the envelope version and the State_Layout of a Hot_State_Blob unchanged.
2. THE storage codec SHALL write any State_Extension after the State_Layout.
3. THE State_Extension SHALL consist of the 32-bit state extension magic followed by a postcard sequence of Extension_Sections, each a 32-bit tag and a byte payload.
4. WHEN no Extension_Section has data for the state, THE storage codec SHALL write no State_Extension.
5. THE storage codec SHALL write each tag at most once, in ascending tag order, with bytes that are a deterministic function of the state.
6. WHEN decoding a Hot_State_Blob, THE storage codec SHALL decode the State_Layout and, IF bytes remain, THEN SHALL decode those bytes as a State_Extension.
7. IF the bytes after the State_Layout do not start with the state extension magic, or the section sequence fails to decode, or bytes remain after the sequence, or a tag repeats, or a known section's payload fails to decode exactly, THEN THE storage codec SHALL return an error naming the blob kind, the run and the defect, and SHALL NOT return a decoded value.
8. WHEN a State_Extension holds a section whose tag the storage codec does not know, THE storage codec SHALL ignore that section.
9. THE storage codec SHALL place in an Extension_Section only data whose absence leaves a reader's behaviour as it was before that data existed. Data without this property SHALL use a new envelope version (continue-as-new-advice Requirement 10).
10. THE payload layout of a released tag SHALL NOT change; a different layout SHALL use a new tag.
11. A change to the State_Layout SHALL use a new envelope version (continue-as-new-advice Requirement 10).

### Requirement 2: Compatibility of stored states

**User Story:** As an operator, I want existing stored states to keep working across the upgrade, so that upgrading needs no migration, backfill or cluster recreation.

#### Acceptance Criteria

1. WHEN the storage codec decodes a Hot_State_Blob without a State_Extension, THE storage codec SHALL return the state with no Last_Heartbeat_Time on any activity.
2. WHEN the storage codec encodes a state with no Last_Heartbeat_Time, THE storage codec SHALL write the bytes that Tokeira 0.2.0–0.5.1 write for that state.
3. THE storage codec SHALL write Hot_State_Blobs that a Pre-Extension_Reader decodes to the written state without its Last_Heartbeat_Times.
4. THE feature SHALL require no schema migration, no backfill and no cluster recreation.

### Requirement 3: Heartbeat section

**User Story:** As a runtime developer, I want each activity's Last_Heartbeat_Time stored with the run's state, so that every node reads the same time.

#### Acceptance Criteria

1. THE kernel `ActivityState` SHALL hold an optional Last_Heartbeat_Time that is not part of the State_Layout.
2. WHEN any activity of the state has a Last_Heartbeat_Time, THE storage codec SHALL write a Heartbeat_Section listing each such activity's id and time, in activity-id order.
3. WHEN decoding a Heartbeat_Section, THE storage codec SHALL set the Last_Heartbeat_Time of each listed activity that the state holds.
4. IF a Heartbeat_Section lists an activity id that the state does not hold, THEN THE storage codec SHALL ignore that entry.
5. IF a Heartbeat_Section lists an activity id twice, THEN THE storage codec SHALL return the error of criterion 1.7.
6. THE `activity_state` side-table blob SHALL keep its layout and SHALL NOT carry the Last_Heartbeat_Time.

### Requirement 4: Recording the time

**User Story:** As an activity worker, I want each heartbeat's time recorded durably, as Temporal records it, so that the heartbeat deadline survives a change of shard owner.

#### Acceptance Criteria

1. WHEN the runtime commits a heartbeat from RecordActivityTaskHeartbeat or RecordActivityTaskHeartbeatById, THE same commit SHALL set the activity's Last_Heartbeat_Time to the time of that commit attempt.
2. WHEN a heartbeat commit is applied, THE owning node SHALL record in its volatile tracking the time that the commit persisted.
3. WHEN RespondActivityTaskFailed for a workflow activity carries last heartbeat details and the runtime retries the activity, THE retry commit SHALL store those details and set the Last_Heartbeat_Time to the commit's time before it applies any pending Heartbeat_Reset.
4. WHEN RespondActivityTaskFailedById carries last heartbeat details, THE runtime SHALL leave the activity's heartbeat details and Last_Heartbeat_Time unchanged.
5. WHEN the runtime prepares a retry of an activity whose `reset_heartbeats` flag is not set, THE retry SHALL keep the activity's Last_Heartbeat_Time.
6. WHEN a Heartbeat_Reset clears an activity's heartbeat details, THE same transition SHALL clear its Last_Heartbeat_Time.
7. WHEN an activity attempt starts, THE runtime SHALL leave its Last_Heartbeat_Time unchanged.
8. THE kernel SHALL NOT set the Last_Heartbeat_Time, only clear it, so that the kernel reads no clock; the runtime sets it.

### Requirement 5: Heartbeat deadline

**User Story:** As an activity worker, I want the heartbeat deadline computed from the last recorded heartbeat on whichever node owns the shard, so that a takeover neither times out a live activity nor misses a dead one.

#### Acceptance Criteria

1. THE Activity_Timeout_Scanner SHALL evaluate a started activity's heartbeat timeout against its Heartbeat_Deadline.
2. WHEN the Last_Heartbeat_Time is earlier than the current attempt's start, THE Heartbeat_Deadline SHALL run from the attempt's start.
3. WHEN the Sweep rebuilds an activity's tracking entry, THE entry SHALL carry the Last_Heartbeat_Time from the run's state.
4. WHEN the runtime builds tracking entries for the activities of a reset's successor run, THE entries SHALL carry the Last_Heartbeat_Time from the successor's state.

### Requirement 6: Describe

**User Story:** As an operator, I want `DescribeWorkflowExecution` to report when each pending activity last heartbeated, as Temporal does.

#### Acceptance Criteria

1. WHEN a pending activity has a Last_Heartbeat_Time, THE Edge SHALL set the activity's `PendingActivityInfo.last_heartbeat_time` to that time.
2. WHEN a pending activity has no Last_Heartbeat_Time, THE Edge SHALL leave `last_heartbeat_time` unset.
3. WHEN a pending activity has heartbeat details, THE Edge SHALL report them in `heartbeat_details`, whether or not the activity has a Last_Heartbeat_Time. A state written by 0.2.0–0.5.1 can hold details without a time; v1.31.0 never holds details without a time.

### Requirement 7: In-memory store and snapshots

**User Story:** As an embedded-engine operator, I want snapshots to carry heartbeat times and to keep restoring after the upgrade, so that a restart neither loses the times nor the snapshot.

#### Acceptance Criteria

1. THE InMemoryStore SHALL return each activity's Last_Heartbeat_Time as committed.
2. WHEN any run in the store has a State_Extension, THE snapshot SHALL append a Snapshot_Extension after the snapshot document. Its section SHALL list, in run-key order, each such run's key and the State_Extension bytes the storage codec writes for that run.
3. WHEN no run in the store has a State_Extension, THE snapshot SHALL write no Snapshot_Extension.
4. THE snapshot format version SHALL remain 4.
5. WHEN restoring a snapshot without a Snapshot_Extension, THE store SHALL hold no Last_Heartbeat_Time.
6. IF the bytes after the snapshot document are not a well-formed Snapshot_Extension, or a section names a run that the document does not hold, or names a run twice, THEN `from_snapshot` SHALL return a decode error and SHALL NOT construct a store.
7. WHEN a Snapshot_Extension holds a section whose tag `from_snapshot` does not know, `from_snapshot` SHALL ignore that section.

## Iteration and Feedback Notes

- **Why not a new envelope version.** A new envelope version could carry the state and then the extension. Appending the extension inside the existing version is better on two counts:
  - Pre-Extension_Readers reject an unknown envelope version. Every run that holds a time would then fail to load on a 0.2.0–0.5.1 node, with a `BlobFormatError` whose message tells the operator to recreate the cluster.
  - Every later addition would need yet another version.

  The appended extension relies on Pre-Extension_Readers ignoring bytes after the State_Layout. That is fixed released behaviour: every release from 0.2.0 to 0.5.1 uses postcard 1.1.3's `from_bytes`, checked at each tag. This feature's own reader is strict about the extension's framing.
- **Mixed versions.** A 0.2.0–0.5.1 node that is still running when this release is deployed reads every run. When it rewrites a run, the run loses its times and behaves as before this feature until its next heartbeat.
- **Rollback.** A rollback to 0.5.1 on DSQL is already refused, because recovery-index's migrations exceed 0.5.1's maximum readable schema version. Embedded engines of 0.2.0–0.5.1 refuse a snapshot that carries a Snapshot_Extension, as trailing bytes (`memory.rs:413-415`). Snapshots without one stay readable by those releases.
- **Why the snapshot keeps version 4.** A version bump would make every embedded engine's existing snapshot unrestorable after the upgrade (inmemory-store-snapshots Requirement 2.2), discarding its workflows. The snapshot document does not change, so the bump rule in `memory.rs` does not apply. The extension is the only new data, and earlier releases refuse it loudly.
- **Follow-up, not required:** blob-size limits on heartbeat details, matching `frontend/workflow_handler.go:1804-1822`.
