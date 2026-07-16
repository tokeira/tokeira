# Implementation Plan

- [x] 1. Reproduce and remove the poll-admission bug condition
  - [x] 1.1 Add Property 6 as an exploration test before changing the implementation.
    - Admit a long poll while the CRUD gate is false, create a matching rule after enabling the
      gate, then offer an activity and prove the unfixed direct-start branch bypasses the rule.
    - Record the expected failure against the unfixed code before implementing 1.2.
    - Tag: `// Feature: workflow-rules, Property 6: poll-admission independence`
    - _Requirements: 1.5, 4.1, 4.3, 4.8_
  - [x] 1.2 Remove the CRUD-gate snapshot from activity polling and evaluate current rules at the
    activity-offer boundary before every ordinary start.
    - The CRUD gate remains on Create/Describe/Delete/List only.
    - Cite `recordactivitytaskstarted/api.go:332-372 @ v1.31.0` at the non-obvious boundary.
    - _Requirements: 1.5, 4.1, 4.3, 4.8_
  - [x] 1.3 Convert the exploration test into a passing regression and cover the symmetric gate
    transition.
    - _Requirements: 1.5, 4.8_

- [x] 2. Complete the durable namespace rule registry
  - [x] 2.1 Define the durable rule key/value and registry interface.
    - Preserve the complete spec, create time, identity, and description.
    - _Requirements: 2.1, 2.2, 3.1, 3.3, 3.5_
  - [x] 2.2 Implement namespace isolation, duplicate/missing behavior, the default limit of 10, and
    earliest-expiration capacity eviction.
    - _Requirements: 1.4, 2.5, 2.6, 2.7, 3.2, 3.4_
  - [x] 2.3 Retain expired rules for Describe/List while excluding them from automatic evaluation.
    - _Requirements: 2.6, 3.1, 3.5, 3.7, 4.6_
  - [x] 2.4 Checkpoint: storage, registry, and edge focused tests pass.

- [x] 3. Complete Workflow Rule CRUD behavior
  - [x] 3.1 Translate every Create/Describe/Delete/List field without dropping spec content.
    - Preserve identity/description; return empty job/page tokens; retain unspecified List order.
    - _Requirements: 2.1, 2.2, 3.1, 3.5, 3.6_
  - [x] 3.2 Apply the namespace gate before body validation and pin the false default.
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 3.3 Implement Trigger as the exact v1.31.0 `UNIMPLEMENTED` exception.
    - _Requirements: 6.1, 6.2_
  - [x] 3.4 Delegate CRUD to the durable registry and preserve gate-before-validation precedence.
    - _Requirements: 1.1, 1.2, 2.1, 3.1, 3.3, 3.5_

- [x] 4. Complete automatic activity-pause evaluation
  - [x] 4.1 Build full workflow and activity predicate contexts from authoritative state.
    - _Requirements: 4.1, 4.2, 4.6, 4.7_
  - [x] 4.2 Evaluate current rules at ordinary and eager activity-start boundaries.
    - _Requirements: 4.1, 4.3, 4.8, 4.11_
  - [x] 4.3 Evaluate retry rules only after failure retryability is known and before retry dispatch.
    - _Requirements: 4.2, 4.3, 4.9_
  - [x] 4.4 Evaluate current rules when a retry timer becomes eligible and before enqueueing work.
    - _Requirements: 4.2, 4.3, 4.10_
  - [x] 4.5 Persist rule-derived pause provenance and enrich Describe from the current registry.
    - _Requirements: 4.4, 4.5, 5.1, 5.2, 5.3_
  - [x] 4.6 Checkpoint: runtime/edge compile, lint, and focused tests pass.

- [x] 5. Required property tests
  - [x] 5.1 Property 1 — namespace isolation and CRUD model (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 1: namespace isolation and CRUD model`
  - [x] 5.2 Property 2 — gate-before-validation precedence (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 2: gate-before-validation precedence`
  - [x] 5.3 Property 3 — rule evaluation reference model (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 3: rule evaluation reference model`
  - [x] 5.4 Property 4 — rule-pause provenance (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 4: rule-pause provenance`
  - [x] 5.5 Property 5 — rejection has no side effect (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 5: rejection has no side effect`
  - [x] 5.6 Property 7 — expiration separates evaluation from retention (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 7: expiration separates evaluation from retention`
  - [x] 5.7 Property 8 — activity-start path equivalence (minimum 100 cases).
    - Tag: `// Feature: workflow-rules, Property 8: activity-start path equivalence`

- [x] 6. Functional conformance checkpoint
  - [x] 6.1 Stress `TestActivityRulesApi_PrePause` across fresh rule-enabled processes to exercise
    poll-admission timing.
  - [x] 6.2 Build `tokeirad` with conformance support and run
    `TestActivityApiRulesClientTestSuite` twice against fresh rule-enabled processes.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["1.3", "2.1"] },
    { "id": 3, "tasks": ["2.2", "2.3"] },
    { "id": 4, "tasks": ["2.4", "3.1", "3.2", "3.3", "3.4", "4.1"] },
    { "id": 5, "tasks": ["4.2", "4.3", "4.4", "4.5"] },
    { "id": 6, "tasks": ["4.6", "5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7"] },
    { "id": 7, "tasks": ["6.1"] },
    { "id": 8, "tasks": ["6.2"] }
  ]
}
```

## Notes

- `TriggerWorkflowRule` is deliberately unimplemented because v1.31.0 is; it is not a deferred
  Tokeira subset.
- Rule evaluation errors are non-fatal non-matches, matching
  `ActivityMatchWorkflowRules @ v1.31.0`.
- Activity lifecycle correctness remains owned by `api-conformance-activity-by-id`; this spec owns
  registry and match-to-pause orchestration.
- Property 6 is the required bug-condition exploration test and precedes implementation changes.
