# Requirements Document

## Introduction

Implement Temporal v1.31.0 Workflow Rules: namespace-scoped CRUD plus automatic activity-pause
evaluation. Wire shape comes from `proto/upstream/temporal/api/rules/v1/message.proto` and
`workflowservice/v1/request_response.proto`; behavior comes from
`service/frontend/{workflow_handler,namespace_handler}.go`,
`service/history/workflow/{activity,mutable_state_impl}.go`, and the activity-start/retry call sites
at tag `v1.31.0`.

The four CRUD RPCs are gated by `frontend.workflowRulesAPIsEnabled` (default false). The gate is a
frontend API-admission control only: v1.31.0 history processing evaluates already-stored rules
without consulting it. The functional conformance harness supplies the suite override as an
independent feature-mode run. Although the proto describes `TriggerWorkflowRule`, the v1.31.0
handler always returns `UNIMPLEMENTED`; this spec preserves that exact exception rather than
inventing behavior.

## Glossary

- **Workflow rule:** A namespace configuration entry containing a rule spec, creation metadata, and
  description.
- **Activity-start trigger:** The only v1.31.0 rule trigger; evaluates before an activity starts and
  when retry processing decides whether another attempt may be scheduled.
- **Activity-pause action:** The only v1.31.0 action; marks a matching pending activity paused with
  rule-derived pause metadata.
- **Visibility query:** A restricted workflow predicate evaluated before the activity predicate.
- **Activity predicate:** The SQL-like predicate over activity type/id/attempt/backoff/status/task
  queue defined by the rules proto.
- **Rule gate:** The namespace-filtered `frontend.workflowRulesAPIsEnabled` setting, which controls
  CRUD admission but not automatic evaluation of stored rules.
- **Activity offer:** A task selected for a waiting poller but not yet started; rule evaluation must
  occur against current state at this boundary.

## Target State

With the rule gate enabled, Create/Describe/Delete/List match v1.31.0 namespace-config behavior.
Stored ActivityPause rules apply to new activities and retries independently of the gate value or
the value observed when a poll was admitted. With the gate disabled, all four CRUD methods return
their exact v1.31.0 `UNIMPLEMENTED` errors before request validation.
`TriggerWorkflowRule` remains unimplemented unconditionally because that is the target behavior.

## Confirmed Bug Condition

Let `X = (P, G0, G1, R, A)`, where `P` is an admitted activity poll, `G0` is the CRUD-gate value at
poll admission, `G1` is its value when an activity is offered, `R` is the current namespace rule
set, and `A` is the offered activity. The bug condition `C(X)` holds when a matching unexpired pause
rule exists at offer time but Tokeira bypasses evaluation because `G0` was false, or when changing
the CRUD gate after rule creation otherwise changes whether the stored rule is evaluated.

This condition is reachable because a long poll can be admitted before the conformance suite
enables the CRUD gate and receive an activity after the matching rule has been created. Temporal
v1.31.0 cannot exhibit this behavior: the gate is checked only by the frontend CRUD handlers, while
activity start and retry paths read namespace rules directly
(`service/frontend/workflow_handler.go:6979-7088`,
`service/history/api/recordactivitytaskstarted/api.go:332-372`, and
`service/history/workflow/mutable_state_impl.go:9232-9277 @ v1.31.0`).

## Evidence From Current Code

- **Wire shape:** `proto/upstream/temporal/api/rules/v1/message.proto` and
  `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`.
- **Gate/default/limit:** `common/dynamicconfig/constants.go @ v1.31.0` defines the false default and
  maximum of 10 rules per namespace.
- **CRUD behavior:** `service/frontend/workflow_handler.go:6979-7087` and
  `service/frontend/namespace_handler.go:665-845 @ v1.31.0`.
- **Automatic evaluation:** `service/history/workflow/activity.go:529`,
  `service/history/workflow/mutable_state_impl.go:9232`,
  `service/history/api/recordactivitytaskstarted/api.go:360`, and
  `service/history/timer_queue_active_task_executor.go:955 @ v1.31.0`.
- **Current Tokeira state:** CRUD and a partial evaluator exist, but
  `crates/tokeira-edge/src/workflow_service.rs` snapshots the CRUD gate before a long-poll await and
  can directly start a later activity offer without evaluating rules. The registry is process-local,
  expired entries are pruned from CRUD results, predicate matching is incomplete, and eager/retry
  paths are not yet equivalent to ordinary offer-time evaluation.

## Field Policy

### `CreateWorkflowRuleRequest` / Response

| Field | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `namespace` | Resolve namespace and its rule gate | Standard namespace error | Selects namespace config |
| `spec` | Required; preserve all trigger/query/action/expiration fields | `INVALID_ARGUMENT`: `Rule Specification is not set.` | Stored by rule id |
| `force_scan` | v1.31.0 ignores it | None | `job_id` remains empty |
| `request_id` | v1.31.0 ignores it | None | No dedupe record |
| `identity` | Preserve as `created_by_identity` | None | Stored metadata |
| `description` | Preserve as description | None | Stored metadata |
| response `rule` | Return the stored rule with server create time | N/A | None |
| response `job_id` | Return empty | N/A | None |

### `WorkflowRuleSpec`

| Field | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `id` | Required, namespace-unique, bounded by max ID length | `INVALID_ARGUMENT` for empty/too long/duplicate | Namespace map key |
| `activity_start.predicate` | Preserve and evaluate at activity-start/retry call sites | Match errors are logged and treated as no match | May pause an activity |
| `visibility_query` | Preserve and evaluate before activity predicate | Match errors are logged and treated as no match | May filter a rule out |
| `actions.activity_pause` | Preserve; apply pause when both predicates match | Unknown variants have no v1.31.0 action | Writes rule pause info |
| `expiration_time` | Skip automatic evaluation after expiry but retain the entry for CRUD reads | Invalid timestamp conversion | Capacity eviction candidate only when creation needs space |

### Describe/Delete/List/Trigger

| Surface | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| Describe `rule_id` | Return the exact namespace-config entry | `INVALID_ARGUMENT` when empty or missing | Read only |
| Delete `rule_id` | Remove the exact namespace-config entry | `INVALID_ARGUMENT` when empty or missing | Updates namespace config |
| List `next_page_token` | v1.31.0 returns the full map and an empty token | None | Read only; order unspecified |
| Trigger request | Always reject | `UNIMPLEMENTED`: `method TriggerWorkflowRule not supported` | None |

## Requirements

### Requirement 1: Feature Gate

**User Story:** As an operator, I want Workflow Rules controlled by the same namespace feature gate
as Temporal, so that default and enabled modes are predictable.

#### Acceptance Criteria

1. WHILE the namespace rule gate is disabled, THE Edge SHALL return `UNIMPLEMENTED` from each CRUD
   RPC before validating the request body.
2. WHILE the namespace rule gate is enabled, THE Edge SHALL admit the four CRUD RPCs.
3. THE default rule gate SHALL be false.
4. THE default maximum rule count SHALL be 10 per namespace.
5. WHILE a workflow rule remains stored, THE runtime SHALL evaluate it independently of the current
   CRUD gate value and of the value observed when any activity poll was admitted.

### Requirement 2: Create Workflow Rule

**User Story:** As an operator, I want to create a namespace rule with provenance, so that future
matching activities are controlled automatically.

#### Acceptance Criteria

1. WHEN a valid create request is admitted, THE rule store SHALL persist the complete spec,
   creation time, identity, and description under the namespace and rule id.
2. WHEN creation succeeds, THE Edge SHALL return the stored rule and an empty job id.
3. IF the spec is absent, THEN THE Edge SHALL return `INVALID_ARGUMENT` with the v1.31.0 message.
4. IF the rule id is empty or exceeds the maximum id length, THEN THE Edge SHALL return
   `INVALID_ARGUMENT`.
5. IF the namespace already contains the rule id, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
6. WHEN the namespace is at capacity and has an entry with an expiration time, THE rule store SHALL
   evict the entry with the earliest expiration time before rechecking capacity.
7. IF the namespace remains at capacity after eviction, THEN THE Edge SHALL return
   `INVALID_ARGUMENT` naming the configured maximum.

### Requirement 3: Describe, Delete, and List

**User Story:** As an operator, I want to inspect and remove rules, so that namespace automation is
observable and reversible.

#### Acceptance Criteria

1. WHEN Describe names an existing rule id, THE Edge SHALL return the stored rule unchanged.
2. IF Describe names an empty or missing rule id, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
3. WHEN Delete names an existing rule id, THE rule store SHALL remove it from namespace config.
4. IF Delete names an empty or missing rule id, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
5. WHEN List is called, THE Edge SHALL return every stored namespace rule exactly once.
6. WHEN List is called, THE Edge SHALL return an empty next-page token and make no ordering promise.
7. WHILE an expired rule remains stored, Describe and List SHALL continue to return it until an
   explicit Delete or capacity eviction removes it.

### Requirement 4: Automatic Activity-Pause Evaluation

**User Story:** As an operator, I want matching activities paused automatically, so that namespace
policy applies before work starts and across retries.

#### Acceptance Criteria

1. WHEN an activity becomes eligible to start, THE runtime SHALL evaluate every unexpired namespace
   rule against the workflow visibility query and then the activity predicate.
2. WHEN retry processing considers another attempt, THE runtime SHALL evaluate the same rule set
   before dispatching that retry.
3. WHEN both predicates match an ActivityPause action, THE runtime SHALL pause the activity before
   worker dispatch.
4. WHEN a rule pauses an activity, THE runtime SHALL store the rule id and pause time as rule-derived
   pause information.
5. WHEN Describe projects rule-derived pause information, THE response SHALL include the current
   rule creator identity and description when that rule still exists.
6. IF a rule is expired, THEN THE runtime SHALL skip it during automatic evaluation.
7. IF predicate evaluation fails, THEN THE runtime SHALL treat that rule as non-matching without
   failing the workflow transition.
8. WHEN a waiting activity poll receives an offer, THE runtime SHALL evaluate the current rule set
   at offer time regardless of the CRUD-gate value captured at poll admission.
9. IF a failed activity will not receive another attempt, THEN retry-rule evaluation SHALL NOT
   pause it as though a retry were pending.
10. WHEN a retry timer becomes eligible, THE runtime SHALL evaluate rules before enqueueing the
    retry task.
11. WHEN an activity is started through eager dispatch, THE runtime SHALL apply the same rule
    evaluation and pause-before-start behavior as ordinary dispatch.

### Requirement 5: Rule Deletion and Existing Pauses

**User Story:** As an operator, I want rule deletion to stop future matches without silently
resuming existing work, so that pause state remains explicit.

#### Acceptance Criteria

1. WHEN a rule is deleted, THE runtime SHALL exclude it from subsequent activity evaluations.
2. WHEN a rule that already paused an activity is deleted, THE activity SHALL remain paused until an
   explicit unpause operation.
3. WHEN Describe projects a pause for a deleted rule, THE response SHALL retain the stored rule id.

### Requirement 6: Trigger RPC Target Exception

**User Story:** As a compatibility client, I want the unimplemented Trigger RPC to fail exactly like
Temporal v1.31.0, so that the target does not claim behavior it never served.

#### Acceptance Criteria

1. WHEN `TriggerWorkflowRule` is called, THE Edge SHALL return `UNIMPLEMENTED` with message
   `method TriggerWorkflowRule not supported`.
2. WHEN `TriggerWorkflowRule` is rejected, THE system SHALL leave namespace and workflow state
   unchanged.
