# Implementation Plan: Recoverable Outbound Dispatches

Requirements, design and this plan approved on 31 October 2026.
Implementation and verification are complete; PR review and exact-head merge
approval remain. Keep the specification, all work below, tests and change record in **one PR** on
`agent/codex/remediation-outbound-recovery`.

Prerequisites: [bugfix.md](bugfix.md), [design.md](design.md), merged atomic successors
and successor semantics. Requirements references below refer to `bugfix.md`.
Approval includes the explicit reset ordering clarification in the design.
No separate specification PR is planned.

- [x] 1. Write the bug-condition exploration property before corrective code
  - Add `crates/tokeira-runtime/src/outbound/bug_condition_exploration_tests.rs` with a
    test-only driver over the current lane/publisher/repository paths. Generate crash
    cuts, duplicate attempts, transient errors, owner replacement and child-ID reuse.
    Use controlled barriers/Notify and injected clocks, never sleeps.
  - Include child start and first-task release; external signal and cancellation;
    untimed Nexus start and cancellation; parent-close actions; child results; reset
    with a distinct current run; oversized successor fallback. Existing callback and
    ordinary successor paths are preservation controls.
  - Implement **Property 20**, at least 100 generated cases, asserting that `C(X)` does
    not occur. Confirm that the unfixed baseline fails for the defective paths; retain
    shrunk counterexamples and the base SHA as fixtures/evidence. If a supposed defect
    passes, inspect that path and the oracle before proceeding with its fix.
  - Tag: `// Feature: outbound-dispatch-recovery, Property 20: End-to-end crash refinement`
  - _Requirements: 10.4, 10.5, 10.6, 10.7._
  - Baseline evidence: twelve per-path properties fail at their intended contract
    assertions; minimal inputs and the command are retained in
    [exploration-baseline.json](../../../crates/tokeira-runtime/src/outbound/exploration-baseline.json).
    The configured 100 cases stop and shrink on failure; these are not successful
    generated-case counts. Existing successor tests (16), callback unit tests (2)
    and callback integration/recovery tests (8) pass. Task 16 completes the generated
    crash/owner schedules and converts these assertions into passing regression tests.

- [x] 2. Add versioned authoritative identity and recovery metadata
  - [x] 2.1 Add operation/incarnation, child seed/gate/release and child-outcome identity
    types in kernel/types modules. Add the runtime-owned `OutboundState` extension;
    clocks and generated IDs remain explicit inputs. Add expected Nexus attempt
    generations to outcome/task-token paths. Do not add side-effecting kernel commands.
    - _Requirements: 1.3, 2.7, 3.1, 3.2, 5.3, 5.4, 6.7, 10.1._
  - [x] 2.2 Introduce explicit new state/history envelope readers/writers and the new
    child-initiation event variant carrying reuse policy. Freeze old postcard layouts;
    add legacy fixtures, unknown-version rejection and wire mapping for the existing
    upstream field. No mutation of `proto/upstream`.
    - _Requirements: 3.1, 9.1, 9.3, 9.4, 9.5, 9.6, 10.2._
  - [x] 2.3 Add typed operation/intent/preparation errors and outcome identities. Replace
    recovery decisions based on error-string substrings with explicit classifications.
    - _Requirements: 1.6, 3.3, 4.2, 4.5, 6.9, 8.6, 9.4._

- [x] 3. Implement durable repository contracts in memory and DSQL
  - [x] 3.1 Add forward-only tables/indexes for operations, intents/parts, reservations,
    preparations/parts and upgrade progress; extend dedupe provenance. Allocate
    migrations contiguously above the actual head; update schema contract/digests and
    build metadata together without changing the immutable baseline.
    - _Requirements: 1.1, 1.6, 2.3, 8.2, 8.4, 9.1, 9.2, 9.6._
  - [x] 3.2 Implement operation derivation and close-intent construction from authoritative
    state/history. Wire all single, paired and state-only commit paths; persist source
    mutations and their obligations together. Include closed parents retaining child
    release work, payload hydration and deterministic close-action ordering.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 3.7, 6.1, 6.5, 9.1, 9.2._
  - [x] 3.3 Implement bounded operation/intent reads, per-run reconciliation, exact fenced
    intent retirement and full forwarding in repository wrappers. Capture original
    fences across retries. Do not use successful no-op defaults for authoritative stores.
    - _Requirements: 1.4, 1.5, 1.6, 1.7, 2.1, 2.4, 2.6, 6.8, 6.9._
  - [x] 3.4 Implement preparation generation/visibility guards, bounded immutable-page
    writes and manifest verification shared by reset/successor publication. Reuse parts
    and write-budget mechanisms. Prevent purge/reprepare races for the same run key.
    - _Requirements: 7.3, 7.4, 8.5, 9.1, 9.2._
  - [x] 3.5 Implement **Property 1**, at least 100 cases, in shared repository tests:
    atomically visible cause/operations and exact post-resolution derivation.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 1: Cause and derived operation atomicity`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 9.2._
  - [x] 3.6 Implement **Property 2**, at least 100 cases: retirement failures, duplicate
    target success, exact-key deletion and no resurrection by acquisition repair.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 2: Intent retirement cannot lose or resurrect work`
    - _Requirements: 1.6, 1.7, 6.8, 6.9._
  - [x] 3.7 Implement **Property 15**, at least 100 cases: values around part/transaction
    boundaries, interrupted preparation pages and corrupt/missing parts. Include live
    DSQL representative cases with actual row/byte counts.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 15: Large recovery data round-trips within bounded writes`
    - _Requirements: 9.1, 9.2, 10.6._

- [x] 4. Checkpoint: storage and codec contracts compile and pass
  - Run nightly formatting, focused kernel/storage checks and all-target lint with
    `--locked`; run the new storage/codec tests under nextest. Verify DSQL-feature
    compilation and migration validation. Baseline exploration failures remain recorded;
    do not weaken their assertions to make this intermediate checkpoint green.
  - _Requirements: 1.1, 9.1, 9.2, 10.1, 10.6, 10.9._

- [x] 5. Wire continuous owned discovery and runtime metadata transitions
  - [x] 5.1 Add the outbound executor, finite-bound scan rounds, home/kind fairness,
    bounded in-flight admission and disposable local retry backoff. Publication becomes
    a wakeup/acceleration path rather than the sole delivery mechanism.
    - _Requirements: 2.1, 2.3, 2.4, 2.8, 6.9._
  - [x] 5.2 Extend recovery candidates/acquisition repair and active-home scanner startup.
    Add lane-serialized preparation/observed-outcome metadata transitions through the
    existing fenced commit API; retain the acquisition fence for every attempt outcome.
    Drain/lease-loss cancellation must leave durable obligations intact.
    - _Requirements: 1.3, 1.5, 2.2, 2.3, 2.5, 2.6, 2.7, 6.9, 10.1._
  - [x] 5.3 Implement **Property 3**, at least 100 cases: lower-key insertions, failing
    first-page records, no notifications, volatile loss and bounded eventual coverage.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 3: Bounded discovery makes progress after volatile loss`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.8, 10.5._
  - [x] 5.4 Implement **Property 4**, at least 100 cases: stale repair, late outcome,
    replacement acquisition and competing-writer fence interleavings; include live DSQL.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 4: Ownership changes cannot authorize stale commits`
    - _Requirements: 2.5, 2.6, 2.7, 10.6._

- [x] 6. Implement the durable child-start handshake
  - [x] 6.1 Persist the winning child seed before target I/O; hydrate initiation data,
    retain reuse policy and verify existing child lineage after ambiguous outcomes.
    Preserve versioning/priority inheritance at the documented preparation point.
    - _Requirements: 3.1, 3.2, 3.3, 3.9, 9.1._
  - [x] 6.2 Gate the child's first task, commit idempotent parent confirmation, derive
    release work and record verified release on the source. Handle already-confirmed
    ABANDON after parent close, stale confirmations, elapsed delay and creation-time
    timeout anchors. Replace volatile-only orchestration ownership.
    - _Requirements: 3.4, 3.5, 3.6, 3.7, 3.8, 3.9._
  - [x] 6.3 Implement **Property 5**, at least 100 cases, over accepted configuration,
    preparation OCC races, restart, hydration and unrelated dedupe collisions.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 5: Child preparation survives restart without identity drift`
    - _Requirements: 3.1, 3.2, 3.3, 9.1, 10.1._
  - [x] 6.4 Implement **Property 6**, at least 100 cases, covering every handshake cut,
    parent close-policy case, repeated confirmation and lost first-task release reply.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 6: Child start precedes execution exactly once`
    - _Requirements: 3.4, 3.5, 3.6, 3.7, 3.8, 3.9._

- [x] 7. Implement external signal/cancellation recovery
  - [x] 7.1 Reconstruct full accepted initiation data and stable request identity;
    distinguish terminal causes from transient/unknown failures. Record destination
    provenance with dedupe and verify ambiguous outcomes before source resolution.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7._
  - [x] 7.2 Implement **Property 7**, at least 100 cases: repeated target/source commits,
    lost replies, failure classification, closed source, self-target/cross-namespace and
    explicit/omitted run targeting. Add fixed wire-cause and header/control examples.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 7: External delivery records only definitive outcomes`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7._

- [x] 8. Recover Nexus first attempts and preserve callback recovery
  - [x] 8.1 Derive scheduled Nexus/cancellation work independently of timeouts. Move
    retry dispatch authority out of the timeout scanner into the operation driver;
    keep authoritative backoff and timeout transitions. Thread expected attempt
    generations through external and worker-backed delivery/results.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.8._
  - [x] 8.2 Implement **Property 8**, at least 100 cases: untimed first attempts,
    cancellation before/after async acceptance, expired deadlines, late results from
    older generations and successful cancellation distinct from operation completion.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 8: Nexus phase and attempt fencing`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.8._
  - [x] 8.3 Implement **Property 9**, at least 100 cases, against the existing callback
    scanner. Preserve scheduled/backoff rehydration, closed-run tracking and concurrent
    callback batches without adding callback rows to the new operation driver.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 9: Callback recovery preservation`
    - _Requirements: 5.7, 5.8, 10.2._

- [x] 9. Implement durable parent-close actions and child results
  - [x] 9.1 Execute committed close intents using parent/child chain fences; follow
    reset redirects with typed outcomes. Add initiation/first-run validation to child
    result transitions and started-event synthesis where required. Remove unowned
    post-close spawned tasks after the durable path is wired.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9._
  - [x] 9.2 Implement **Property 10**, at least 100 cases: child continue-as-new/retry
    chains, unrelated workflow-ID reuse, ABANDON and reset suppression.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 10: Parent-close actions respect child lineage`
    - _Requirements: 6.1, 6.2, 6.3, 6.5._
  - [x] 9.3 Implement **Property 11**, at least 100 cases: duplicate/reordered results,
    missing start confirmation, valid reset redirection, stale initiation/first-run
    identities and all terminal result kinds with their preserved attributes.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 11: Child results resolve the correct initiation`
    - _Requirements: 6.4, 6.5, 6.6, 6.7._

- [x] 10. Prepare and publish the complete reset atomically
  - [x] 10.1 Extract pure reset construction helpers for fork failure, in-flight activity
    handling, eligible chain reapply, ordered options and final task scheduling. Build
    base/current/new-run changes before committing any; preserve principals and clock
    semantics. Scope retry identity to targeted current-reset semantics.
    - _Requirements: 7.1, 7.5, 7.7, 10.1._
  - [x] 10.2 Implement guarded staging and atomic final publication in both stores.
    Validate participant sequences and current pointer under one captured home fence;
    include all derived state and close obligations. Add exclusive local publication
    admission and shared cache-generation invalidation without waiting on other lanes.
    - _Requirements: 7.2, 7.3, 7.4, 7.6, 9.1, 9.2._
  - [x] 10.3 Remove post-commit reset materialization/options/failure spawning, separate
    current-run termination and the poll-then-return-success fallback. Resolve lost
    replies from published identity; recover or safely purge incomplete preparations.
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.6._
  - [x] 10.4 Implement **Property 12**, at least 100 cases: base equals/differs from
    current, closed/open current, staging/publication cuts, failed pointer/sequence CAS,
    stale cache no-ops and old purge-generation interference.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 12: Reset publication is all or nothing`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.6._
  - [x] 10.5 Implement **Property 13**, at least 100 cases: repeated reset requests,
    reapply exclusions, options ordering and child reconnection. Add fixed Temporal
    event-order examples and a later-current-run request-deduplication case.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 13: Reset retry preserves request and semantic order`
    - _Requirements: 7.5, 7.7._

- [x] 11. Close the oversized-successor recovery gap
  - [x] 11.1 Narrow pair-size-refusal classification; atomically commit close, frozen
    start intent and reservation for the exceptional fallback. Update every current-
    pointer writer to honor the same reservation inside its fenced transaction.
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.7._
  - [x] 11.2 Implement resumable successor staging/publication and verified duplicate
    settlement. Retire reservation/intent with publication, preserve frozen clocks,
    and feed ordinary task/timeout recovery. Remove the volatile deferred-start task.
    - _Requirements: 8.3, 8.5, 8.6, 8.8, 8.9._
  - [x] 11.3 Implement **Property 14**, at least 100 cases: pair refusal vs unrelated
    `54000`, failed smaller commit, unrelated starts/signals-with-start/resets, crash
    after close, lost publish reply, unverified duplicates and restart under drain.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 14: Successor fallback preserves the chain claim`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9._

- [x] 12. Add narrowly scoped cross-home delivery
  - [x] 12.1 Define the reference-only internal RPC and thin edge adapter; hydrate and
    validate in runtime. Add bounded sender/receiver routing and owner refresh using
    current placement, with no recursive forwarding or arbitrary command payload.
    - _Requirements: 2.5, 2.6, 2.7, 3.4, 4.3, 6.4, 10.6._
  - [x] 12.2 Extend existing peer signing/verifying with the distinct outbound audience.
    Keep public and offer credentials insufficient for this route. Wire validated
    bootstrap material without new secrets/configuration or routes in no-peer modes.
    - _Requirements: 2.5, 2.6, 2.7, 10.6._
  - [x] 12.3 Implement **Property 18**, at least 100 cases: local/remote parity, owner
    changes, stale references, wrong-cluster/purpose credentials, bounded decoding and
    no state mutation on rejected traffic. Add fixed no-peer route tests.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 18: Remote delivery grants no extra authority`
    - _Requirements: 2.5, 2.6, 2.7, 10.6._

- [x] 13. Implement retention and checked legacy preparation
  - [x] 13.1 Extend bounded purge eligibility to preserve referenced intent payloads,
    preparation rows and destination dedupe evidence. Preserve logical deletion while
    delaying necessary physical reclamation; release pins after obligations settle.
    - _Requirements: 1.3, 1.7, 6.9, 9.1._
  - [x] 13.2 Add read-only maintenance planning, revision/digest-guarded apply, stopped-
    writer preconditions, legacy classification and progress marker handling. Block
    activation before preparation completes; never reconstruct unknown close delivery.
    - _Requirements: 9.3, 9.4, 9.5, 9.6, 9.7._
  - [x] 13.3 Add guarded repair inputs for missing original policy or verified child
    identity, recording supplied evidence in a fenced source metadata transition.
    Add bounded inspection output and low-cardinality progress/error counters without
    exposing workflow payloads, credentials or raw transport errors.
    - _Requirements: 9.4, 9.5, 9.7._
  - [x] 13.4 Implement **Property 16**, at least 100 cases: legacy known/unknown data,
    interrupted scan/apply, stale plan digests, unsupported envelopes and incomplete
    preparation. Add CLI plan/confirm/apply examples and startup-gate tests.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary`
    - _Requirements: 9.3, 9.4, 9.5, 9.6, 9.7._
  - [x] 13.5 Implement **Property 19**, at least 100 cases: purge races with destination
    success/source confirmation, closed-run intent execution and preparation restart;
    prove eventual bounded reclamation after dependencies disappear.
    - Tag: `// Feature: outbound-dispatch-recovery, Property 19: Retention cannot erase outstanding proof`
    - _Requirements: 1.3, 1.7, 6.9, 9.1._

- [x] 14. Checkpoint: all corrected paths are wired and focused suites pass
  - Run kernel/storage/runtime/edge/auth all-target lint and checks with `--locked`,
    including DSQL and applicable conformance features. Run focused nextest suites.
    Verify no old publisher/scanner still owns a competing attempt lifecycle and no
    reset/fallback path retains an untracked post-commit task.
  - _Requirements: 1.5, 5.8, 7.2, 8.2, 10.1, 10.6, 10.9._

- [x] 15. Add the preservation property suite
  - Implement **Property 17**, at least 100 cases, comparing unaffected successful and
    rejected paths against the preservation model/fixtures. Include ordinary atomic
    successors, original metadata, two clocks, retry-to-cron selection, minimum delay,
    callback payloads, validation, task/timer ordering and pure deterministic inputs.
  - Tag: `// Feature: outbound-dispatch-recovery, Property 17: Unaffected semantics and pure transitions are preserved`
  - _Requirements: 10.1, 10.2, 10.3._

- [x] 16. Complete the exploration harness as an end-to-end regression suite
  - Run Property 20's original assertions against the corrected runtime with at least
    100 generated cases per required operation class. Exercise every persisted cut
    through real commit/reload paths, including two owners and multiple lanes.
  - Add representative live DSQL runs for fencing, interrupted preparation, payloads
    above the inline threshold and pair refusal. Report selected/filtered tests and
    generated-case counts by backend; an empty/filtered run is not a pass.
  - _Requirements: 10.4, 10.5, 10.6, 10.7._

- [x] 17. Add public-wire and pinned functional regression coverage
  - Add/extend Rust wire fixtures for the corrected histories and typed refusals.
    Select actual v1.31.0 child, external, Nexus/callback, reset and successor corpus
    cases from the local reference checkout; record per-test outcomes and prescribed
    named skips without changing corpus bodies to manufacture a pass.
  - Include reset history immediately after RPC success, parent start before child task
    execution, child-chain close policies, cancellation-request vs operation completion,
    and the successful successor behaviour delivered in PR 340.
  - _Requirements: 3.6, 4.5, 5.6, 6.2, 6.6, 7.6, 7.7, 8.9, 10.8._

- [x] 18. Reconcile specification, operational documentation and change record
  - Apply the approved refinements to atomic-successors and affected child/external/
    Nexus/reset/peer specifications; remove conflicting volatile-only ownership claims.
    Keep architecture 042's unrelated proposals explicitly proposed.
  - Document the checked cutover, legacy inspection/repair evidence, rollback limits,
    reservation diagnostics and retained physical dependencies. Add module/public-item
    and correctness-invariant comments with verified Temporal source citations.
  - Add and validate a Changie change record. Maintain a path-by-path completion ledger
    linking each operation to implementation, crash tests and functional evidence.
  - _Requirements: 9.6, 9.7, 10.1, 10.2, 10.8, 10.9._

- [ ] 19. Checkpoint: finish the combined PR green
  - Run the full root bar: `cargo +nightly fmt --all`, `cargo lint --locked`,
    `cargo check --workspace --locked`, `cargo nextest run --workspace --locked`,
    `cargo test --workspace --doc --locked`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Validate Markdown links, migration/schema contracts, change record and unchanged
    lockfile; inspect the final diff for unintended work. Use approved disposable
    verification infrastructure according to its runbook and retain evidence of cleanup.
  - At the PR boundary, fetch and rebase once; rerun required checks if the source
    changes. Stop on semantic/out-of-scope conflicts and report the merge-order issue.
    Commit with attribution and create/attach one PR containing this entire delivery.
    Update the progress Page with exact head, evidence and remaining risks. Merge only
    after human approval of that exact head and green checks.
  - _Requirements: 10.8, 10.9._

## Verification evidence

The [delivery ledger](../../../docs/readiness/outbound-recovery.md) records the
final 4,121-test workspace pass, generated property coverage, named pinned-corpus
outcomes and six passing live DSQL cases. All implementation/verification gates
passed; the final publication/approval checkpoint remains open. All executed functional leaves now pass;
one upstream skip and four existing registry exclusions are reported separately.

## Task Dependency Graph

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
  "9": ["6", "7"],
  "10": ["6", "9"],
  "11": ["3", "5", "10"],
  "12": ["6", "7", "8", "9"],
  "13": ["9", "10", "11", "12"],
  "14": ["8", "10", "11", "12", "13"],
  "15": ["14"],
  "16": ["14", "15"],
  "17": ["16"],
  "18": ["17"],
  "19": ["18"]
}
```

## Notes

- Task dependencies describe code ordering, not permission to start other agents.
  One owner/worktree/branch remains responsible for the combined PR.
- Each named property is mandatory. At least 100 cases run in its full model/memory
  suite; representative live DSQL cases supplement rather than replace that count.
- No production database cutover or deployment is included in executing this coding
  plan. Maintenance commands are implemented/tested here; operator use is separate.
- No dependency or lockfile change is planned. Migrations remain forward-only; exact
  numbers are selected against the actual schema head before implementation.
- Runtime tuning stays internal. Unknown legacy history is reported explicitly;
  completing new recovery guarantees does not assert recovery of unknowable past work.
- Unexpectedly passing exploration cases and failed preservation properties trigger
  source investigation. Do not weaken properties or amend reviewed requirements
  without the required review.
