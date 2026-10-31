# Successor semantics implementation plan

Prerequisites: approved [requirements](bugfix.md), reviewed [design](design.md),
and the merged atomic-successor implementation. All tasks below are outstanding;
writing this specification is not evidence that a test ran or a fix shipped.

- [ ] 1. Write and run the bug-condition exploration property tests first
  - Use existing runtime/kernel entry points and the real memory repository with
    deterministic times. Generate at least 100 cases per bug family and assert
    absence of `C(X)`: original metadata/links, cron schedule across retry, timeout
    failure/result, retry-declined cron selection/retry state, and minimum delay.
  - Record a runtime assertion failure and shrunk counterexample for every family
    on the baseline before changing production code. Use real cron calculation
    for calendar cases, including the zero-delay exact-fire boundary.
  - Stop and re-investigate any unexpected pass. Do not count compilation errors,
    mocks encoding the target result, or comparison with the old builder as proof.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.1, 2.2, 3.2, 3.3, 4.1_

- [ ] 2. Add immutable started-event input extraction and bounded plan ownership
  - Implement runtime-private `SuccessorStartedEvent` and one checked extractor for
    legacy/V2 started events. Read through the repository so large values remain
    hydrated by its normal path. Reject missing/wrong-kind history without empty
    input fallbacks.
  - Extend `SuccessorPlan` with explicit continue-as-new links and boxed retry/cron
    started inputs, concrete cause, selected delay and evaluation time. Retain the
    existing started-versioning input without changing versioning policy.
  - Place fallible reads before taking update waiters. Add read-failure and
    malformed-history tests that show no close write and no lost waiter.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 5.1, 5.9_

- [ ] 3. Separate retry outcome from cron successor choice
  - Add the concrete failure retry state to `CronContinuation`; update all affected
    constructors and command serialization tests. Keep persisted state/event
    layouts unchanged. Document why a cron successor can have a terminal retry
    state.
  - Have the failed-cron kernel arm record the supplied state. Keep the existing
    command variants, single-run purity and close event taxonomy.
  - Evaluate retry before cron in workflow-task preparation. Remove the guard
    that suppresses cron merely because a policy exists; pass the declined retry
    state into a selected cron continuation. Preserve command validation order.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 5.9_

- [ ] 4. Correct successor fields and apply the minimum interval
  - Route started header/memo/search attributes/links into retry and cron; route
    original links into explicit continue-as-new. Preserve the cron schedule on
    retry and the existing command-sourced continue-as-new fields.
  - Replace `timeout_prior_outcome` with explicit cause-specific failure/result
    construction from the closing event, timeout failure and started result.
  - Expose and reuse the existing pure minimum-backoff helper without moving its
    constant or adding a mutable override. Freeze a close evaluation instant;
    derive the delay from execution age and use that instant for successor start
    and timers, including delayed timeout scans and OCC retries.
  - Keep the corrected start shared by normal pair and explicit size fallback.
    Remove obsolete builder comments that describe intentional field loss.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 4.1, 4.2, 4.3, 4.4, 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 5. Checkpoint: focused compilation, lint and exploration tests are green
  - Run nightly formatting, locked checks/clippy for kernel/runtime/edge consumers,
    and the exploration cases under nextest. Confirm all previously failing
    assertions now pass and no migration or dependency file changed.
  - _Requirements: 1.1, 2.1, 3.2, 4.1, 5.9_

- [ ] 6. Required PBT: Property 1 — original metadata survives each successor
  - At least 100 cases; vary legacy/V2 events, empty/absent metadata, nonempty
    headers/links and mutations after start. Assert persisted successor values.
  - Tag: `// Feature: successor-semantics, Property 1`
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

- [ ] 7. Required PBT: Property 2 — outcome data follows the actual cause
  - At least 100 cases; cover workflow-task retry, successful/failed cron, timeout
    retry/cron and absent/empty/nonempty carried results. Check decoded failures
    and result presence in persisted, edge-serialized started history.
  - Tag: `// Feature: successor-semantics, Property 2`
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9_

- [ ] 8. Required PBT: Property 3 — retry outcome and successor kind are independent
  - At least 100 cases; reference-model the full selection table, with retry
    available, maximum attempts, non-retryable failure, no policy, cron absent and
    cron present. Include retry → completion and retry → exhaustion → cron chains.
  - Tag: `// Feature: successor-semantics, Property 3`
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

- [ ] 9. Required PBT: Property 4 — minimum interval and timer agree
  - At least 100 cases; reference-model the minimum formula and use controlled
    clocks to check first-task eligibility before/at its deadline. Cover delayed
    predecessor execution and scanner lag; obtain cron delays from real schedules.
  - Tag: `// Feature: successor-semantics, Property 4`
  - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [ ] 10. Required PBT: Property 5 — corrected metadata retains commit safety
  - At least 100 cases in lane atomic-successor tests; inject conflicts, duplicates,
    pair rejection and explicit size refusal. Compare corrected fields between
    normal/fallback starts and assert same identity across retries. Retain existing
    fence, pointer, notification, drain and crash-before-publication coverage.
  - Tag: `// Feature: successor-semantics, Property 5`
  - _Requirements: 5.1, 5.2, 5.3, 5.6, 5.7_

- [ ] 11. Required PBT: Property 6 — expired chains remain terminal
  - At least 100 cases over execution deadlines, retry policies and schedules.
    Drive execution timeouts and assert no successor or current-pointer advance.
  - Tag: `// Feature: successor-semantics, Property 6`
  - _Requirements: 5.8_

- [ ] 12. Required PBT: Property 7 — explicit continue-as-new keeps correct fields
  - At least 100 cases; choose distinct command/start/current metadata, verify
    command fields and original links, and retain short-lifetime delay coverage.
  - Tag: `// Feature: successor-semantics, Property 7`
  - _Requirements: 5.4, 5.5_

- [ ] 13. Required PBT: Property 8 — kernel result is deterministic
  - At least 100 kernel cases across concrete cron failure retry states; repeat
    application to identical input and compare transitions. Verify by source/lint
    review that the helper and handoff introduce no I/O or configuration read.
  - Tag: `// Feature: successor-semantics, Property 8`
  - _Requirements: 5.9_

- [ ] 14. Reconcile prior tests and add end-to-end history coverage
  - Replace the atomic-successor legacy-builder equivalence assertion for approved
    fields with the new field policy; preserve comparisons for unchanged identity,
    lineage and versioning. Historical completed tasks stay completed; the new
    semantics tests are tracked here.
  - Add real runtime → repository → edge serializer cases for metadata and result
    policies; include both started variants and history-read failure. No new public
    RPC or proto fields are needed.
  - Extend live DSQL pair fixtures with inherited metadata and run from a nextest
    archive per the live runbook; assert both runs and current pointer after commit.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.1, 2.2, 3.2, 3.3, 5.1, 5.2, 5.7_

- [ ] 15. Checkpoint: complete implementation acceptance
  - Run the root §10.4 bar, affected DSQL-feature lint and the pinned continue-as-new,
    workflow-retry, cron and cron-client corpus suites. Account for any skipped test
    by the existing harness rules. Record the exploration failures, corrected
    passes, PBT results and live evidence against the reviewed source head.
  - Verify no new dependency, migration or mutable minimum-interval setting; review
    the final diff for only the approved scope.
  - _Requirements: 1.1, 2.1, 3.2, 4.1, 5.1, 5.9_

## Task dependency graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["2"],
  "4": ["3"],
  "5": ["4"],
  "6": ["5"],
  "7": ["5"],
  "8": ["5"],
  "9": ["5"],
  "10": ["5"],
  "11": ["5"],
  "12": ["5"],
  "13": ["5"],
  "14": ["6", "7", "8", "9", "10", "11", "12", "13"],
  "15": ["14"]
}
```

## Notes

This graph expresses prerequisites, not authorization to run multiple agents or
unbounded heavy builds. All eight PBT tasks are required; exploration generators
may be retained and expanded for them without duplicating equivalent tests.
Read crate-local AGENTS before implementation. Preserve unrelated work and
snapshot any later specification amendments under root §6. The explicit oversized
pair fallback remains crash-unsafe until separate durable recovery work addresses it.
