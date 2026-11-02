# Implementation Plan: Durable Activity Dispatch

Implement the approved [requirements](requirements.md). This plan and the
[design](design.md) were jointly approved on 2 November 2026. Keep the
activity delivery cutover in one coherent implementation PR; no intermediate PR
may retire a recovery mechanism before its replacement is verified.

- [x] 1. Establish the baseline counterexample and retirement inventory
  - [x] 1.1 Add a synchronized retained-head-page exploration test using more than
    one reconciliation page. Confirm and record its expected baseline failure,
    then retain the independent scenario oracle for the new discovery path.
    - _Requirements: 2.3, 2.4, 2.8, 8.2, 8.7_
  - [x] 1.2 Add an executable call-site/operation inventory for publication,
    retries, backlog spill/drain, on-demand republish, version registration,
    acquisition, inspection/sampling, reset, purge, pair/successor publication
    and repository adapters. Add counters/probes to expose legacy writes and
    foreign discovery mutations during final tests.
    - _Requirements: 2.9, 7.1, 7.2, 7.7, 8.8_

- [x] 2. Implement canonical rows, forward-only schema and ordered storage reads
  - [x] 2.1 Add `activity_dispatch.rs` with checked derivation, incarnation,
    due/home positions, page/repair models and explicit errors. Reuse bounded
    queue-digest encoding without changing existing workflow digests. Define
    every field and invalid-state case from the design.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 1.7, 2.2_
  - [x] 2.2 Add the five ordered migration operations: nullable queue digest,
    queue-due index, complete home index and retirement of the two replaced
    indexes. Allocate contiguous versions above the immutable prefix; update
    schema contract/digests and build information without changing locked SQL.
    Add schema validation, index-readiness and migration compatibility fixtures.
    - _Requirements: 2.2, 7.3, 7.5, 8.6_
  - [x] 2.3 Implement desired-set delta maintenance in memory and DSQL under the
    existing fenced transaction. Cover ordinary, paired and successor writes,
    staged reset/bulk publication, logical deletion and purge. Keep normal writes
    atomic and retain DSQL's physical refusal behavior.
    - _Requirements: 1.1, 1.3, 1.4, 1.5, 4.3, 7.7_
  - [x] 2.4 Implement read-only queue-family and home pages, including covered-key
    continuation seeks, raw-coordinate collision checks, checked decode and
    last-examined/exhaustion semantics. Adapt needed inspection and test methods
    to the canonical row shape; update all repository implementations/test doubles.
    - _Requirements: 2.1, 2.2, 2.5, 2.6, 2.7, 8.3, 8.6_
  - [x] 2.5 Required PBT — Property 1: Complete deterministic derivation
    - Use `proptest`, at least 100 cases, in shared storage activity-dispatch
      tests. Oracle enumerates wanted slots and all expected fields independently.
    - Tag: `// Feature: activity-dispatch, Property 1`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 1.7_
  - [x] 2.6 Required PBT — Property 2: Atomic activity maintenance
    - At least 100 generated commit/refusal/conflict traces in the shared storage
      suite, with memory and live DSQL adapters. Include pair/staging visibility.
    - Tag: `// Feature: activity-dispatch, Property 2`
    - _Requirements: 1.1, 1.3, 1.4, 1.5, 4.3, 7.7_
  - [x] 2.7 Required PBT — Property 3: Ordered backend traversal
    - At least 100 generated sets/page sizes/cursor deletions and injected hash
      collisions against a sorted fixture oracle, run through both adapters.
    - Tag: `// Feature: activity-dispatch, Property 3`
    - _Requirements: 2.1, 2.2, 2.5, 2.6, 8.3, 8.6_

- [x] 3. Persist and preserve authoritative start-request identity
  - [x] 3.1 Add the receipt map and frozen extension section, preserving the
    positional state payload. Add TKW4 writing and explicit legacy decoding,
    including TKW3's outbound preparation semantics and large-state paths.
    - _Requirements: 4.6, 4.7, 7.3, 7.5, 7.7_
  - [x] 3.2 Thread runtime-supplied start request IDs through pure activity start,
    buffered/transient materialization and all receipt lifecycle clearing paths.
    Preserve by-ID synthetic starts; clear delivery receipts on reset forks and
    replacement attempts without clearing the identity of a still-started attempt.
    - _Requirements: 4.6, 4.7, 6.3, 6.6, 7.7_
  - [x] 3.3 Required PBT — Property 16: Receipt encoding and lifecycle
    - At least 100 generated codec/lifecycle traces in storage codec and kernel
      reset/start tests. Include malformed/duplicate sections, legacy empty
      receipts, frozen bytes, unrelated extensions and unsupported old readers.
    - Tag: `// Feature: activity-dispatch, Property 16`
    - _Requirements: 4.6, 4.7, 6.6, 7.3, 7.5, 7.7_

- [x] 4. Checkpoint: storage and receipt contracts compile and pass
  - Run nightly formatting, focused `cargo check --locked` and all-target clippy
    for touched crates, plus nextest storage/kernel tests. Record the baseline
    exploration failure separately from tests expected to pass at this stage.
    Do not change dependencies or suppress failures to reach this checkpoint.
  - _Requirements: 8.1, 8.3, 8.7, 8.9_

- [x] 5. Repair every activity row before an acquisition serves
  - [x] 5.1 Implement bounded wanted/existing repair pages with fresh transactional
    state reads, run serialization, observed-row conditional deletion, captured
    lease fences, byte budgets and explicit restart on changed run sequence.
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.10_
  - [x] 5.2 Integrate both candidate and complete-row walks into managed and embedded
    acquisition. Bind retry/cancellation/tracking to the acquisition generation;
    publish Active only after all repair and deadline restoration succeeds.
    - _Requirements: 5.1, 5.2, 5.7, 5.8, 5.9, 6.8_
  - [x] 5.3 Required PBT — Property 13: Complete bounded repair
    - At least 100 generated missing/wrong/future/legacy/orphan row sets against
      both stores. Include a single run larger than one write page and split state.
    - Tag: `// Feature: activity-dispatch, Property 13`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.10, 8.3, 8.6_
  - [x] 5.4 Required PBT — Property 14: Repair serializes against owners and writers
    - At least 100 controlled competing-writer/owner traces using channels and
      storage pause points, including deletion/recreation and replacement rows.
    - Tag: `// Feature: activity-dispatch, Property 14`
    - _Requirements: 5.4, 5.5, 5.6, 5.8, 8.3, 8.4_
  - [x] 5.5 Required PBT — Property 15: Failed acquisition never serves
    - At least 100 runtime failure/cancellation/deadline traces; assert Sweeping
      remains until a complete valid acquisition and overdue deadlines stay overdue.
    - Tag: `// Feature: activity-dispatch, Property 15`
    - _Requirements: 5.1, 5.7, 5.8, 5.9, 6.8_

- [x] 6. Add queue-home activity discovery and bounded offer accounting
  - [x] 6.1 Add queue-family demand registration, fixed-cutoff passes and activity
    source/resolution. Share the query semaphore and scheduler fairly with workflow
    discovery; keep continuation across skipped rows, renewals and capacity pauses.
    - _Requirements: 2.1, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 6.7_
  - [x] 6.2 Add complete-incarnation ready/in-flight accounting and reservation/drop
    guards to the activity broker, with the design's budgets and acquisition tokens.
    Retain priority/fairness/rate/shutdown policy and disposable ordering.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_
  - [x] 6.3 Required PBT — Property 4: Progress through skipped prefixes
    - At least 100 generated demand/capacity/failure traces over finite ranges,
      including retained prefixes larger than both page and admission budgets.
    - Tag: `// Feature: activity-dispatch, Property 4`
    - _Requirements: 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 8.2_
  - [x] 6.4 Required PBT — Property 5: Bounded disposable offers
    - At least 100 generated admission/take/expiry/cancellation traces against an
      independent counted-set model; include activity-ID reuse and broker restart.
    - Tag: `// Feature: activity-dispatch, Property 5`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.6_
  - [x] 6.5 Required PBT — Property 6: Acquisition-scoped volatile state
    - At least 100 overlapping-generation traces with late page/offer/tracker
      completions. Replacement capacities and entries must remain unchanged.
    - Tag: `// Feature: activity-dispatch, Property 6`
    - _Requirements: 3.5, 5.8, 8.4_

- [x] 7. Keep retry eligibility and rule application on the execution owner
  - [x] 7.1 Split read-only candidate checks from fenced rule application. Replace
    retry sleep publication with restored eligibility tracking in the existing
    activity scanner, independent bounded continuation and the unchanged timeout
    precedence/budget. Ensure rule evaluation also runs without poll demand.
    - _Requirements: 2.9, 5.9, 6.1, 6.2, 6.3, 6.8_
  - [x] 7.2 Required PBT — Property 10: Rules at retry eligibility and start
    - At least 100 generated clock/rule/restart traces with no sleeps, covering
      delayed unpause/reset, rules created during backoff and no-poller pauses.
    - Tag: `// Feature: activity-dispatch, Property 10`
    - _Requirements: 2.9, 6.1, 6.2, 6.3, 6.8, 8.5_
  - [x] 7.3 Required PBT — Property 11: Routing and lifecycle preservation
    - At least 100 generated state/registry/lifecycle traces, preserving pinned
      and independently versioned activity behavior and dispatch-revision ordering.
      Compare broker policy outcomes for priority, fairness, rate and shutdown.
    - Tag: `// Feature: activity-dispatch, Property 11`
    - _Requirements: 3.7, 4.2, 6.3, 6.4, 6.7, 8.5_

- [x] 8. Implement the common owner-start and receipt-replay decision
  - [x] 8.1 Add typed Started/Replayed/Suppressed/Stale outcomes, same-ID replay
    validation, authoritative due/rule/routing gates, precommit input/response
    preparation and fenced/OCC commit. Keep request identity stable on retries.
    Bind replay reads to the current lease; never resurrect resolved work.
    - _Requirements: 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10_
  - [x] 8.2 Route eager matching directly through the same owner decision using
    validated fresh-schedule input/header and existing eager response limits.
    Preserve transient/buffered history and scope authorization.
    - _Requirements: 4.4, 6.1, 6.2, 6.5, 6.6_
  - [x] 8.3 Required PBT — Property 7: One authoritative start
    - At least 100 competing ordinary/eager/remote-start fixture traces with
      controlled commit races, stale schedules, attempts, stamps and lease epochs.
    - Tag: `// Feature: activity-dispatch, Property 7`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 8.4_
  - [x] 8.4 Required PBT — Property 8: Same-request recovery without resurrection
    - At least 100 commit-then-drop/retry/takeover traces, checking one transition,
      preserved attempt/start time, stale replay after resolution and timeout recovery.
    - Tag: `// Feature: activity-dispatch, Property 8`
    - _Requirements: 4.6, 4.7, 4.8, 4.9, 8.4_

- [x] 9. Connect the authenticated activity start transport and public poll
  - [x] 9.1 Add the reviewed internal proto messages and runtime starter interface.
    Implement client/owner edge adapters and server wiring with bounded request,
    response, metadata, handlers, deadlines and placement retries. Preserve every
    field in the design's request/response tables and native payload bytes.
    - _Requirements: 4.1, 4.8, 4.10, 4.11, 6.5_
  - [x] 9.2 Add the activity-start peer audience and separate peer header without
    changing credential files or public grants. Repeat worker admission and bind
    namespace/origin independently on the owner. Keep local no-peer mode direct.
    - _Requirements: 4.11, 7.7_
  - [x] 9.3 Thread effective priority through started-activity and public response
    conversion. Implement exact Started/Replayed optional-field treatment from
    pinned matching/history/frontend source. Keep token epochs and namespace IDs
    distinct from public display names; preserve poller scaling at the queue owner.
    - _Requirements: 4.6, 4.10, 6.5, 6.6, 7.7_
  - [x] 9.4 Required PBT — Property 9: Wire and admission boundaries
    - At least 100 malformed/scope/purpose/cluster/size/cancellation cases through
      real transport, with mutation counters and bounded-frame fixtures.
    - Tag: `// Feature: activity-dispatch, Property 9`
    - _Requirements: 4.10, 4.11, 8.5_
  - [x] 9.5 Required PBT — Property 12: Response and eager preservation
    - At least 100 generated binary payload/header and metadata cases through
      local, eager and distinct-owner wire paths, including replay and transient
      history materialization. Expected field values come from fixture inputs.
    - Tag: `// Feature: activity-dispatch, Property 12`
    - _Requirements: 4.6, 4.10, 6.5, 6.6, 8.5_

- [x] 10. Retire legacy delivery only after replacement coverage is green
  - [x] 10.1 Remove activity grace spill/drain, per-home dispatch reconciliation,
    destructive unversioned backlog absorption and duplicate delayed publication.
    Rewire on-demand republish/registration to discovery and remove obsolete
    settings. Preserve timeout/eligibility scanning and unrelated task families.
    - _Requirements: 6.7, 6.8, 7.1, 7.2, 7.7_
  - [x] 10.2 Extend bounded legacy disposal with activity-slot coverage checks,
    safe missing-run handling, deferred corrupt/unknown copies and physical-key
    continuation. Keep the legacy table; do not add an unconditional DROP.
    - _Requirements: 7.2, 7.4, 7.5, 7.7_
  - [x] 10.3 Required PBT — Property 17: Legacy disposal is bounded and conditional
    - At least 100 generated mixed legacy sets in shared storage tests, including
      deferred prefixes, missing runs, concurrent replacements and repeat passes.
    - Tag: `// Feature: activity-dispatch, Property 17`
    - _Requirements: 7.2, 7.4, 7.5, 7.7_
  - [x] 10.4 Required PBT — Property 18: Delivery survives retirement
    - At least 100 workload/failure traces with legacy producers and consumers
      instrumented to fail if called. Prove delivery/resolution through new paths
      with all notifications dropped and both owner placements varied.
    - Tag: `// Feature: activity-dispatch, Property 18`
    - _Requirements: 3.4, 6.8, 7.1, 7.2, 7.7, 8.4, 8.5_

- [x] 11. Add reproducible live and compatibility evidence
  - [x] 11.1 Add explicit live DSQL adapters and selections for the shared cases,
    with at least 100 generated traces per property entry point. Add production-
    shaped queue/home index-plan fixtures, deep tied cursors, 101+ run acquisition,
    a multi-page single-run repair and split-state cases. Make missing live test
    configuration a preflight refusal rather than a counted success.
    - _Requirements: 5.10, 8.1, 8.3, 8.6, 8.7, 8.8_
  - [x] 11.2 Add isolated remote-start read probes for discovery, fresh start,
    replay and handoff. Record exact expected SQL operations/counts after the
    implementation is fixed; separate autonomous discovery reads. Add immutable
    source/binary/result manifests and retained failure receipts to the test harness.
    - _Requirements: 8.4, 8.5, 8.6, 8.8_
  - [x] 11.3 Update the existing dispatch-handoff refinement mapping for activity
    incarnations and replay-as-observation, preserving its negative controls.
    Add full traceability checks and fixed public/internal field/error fixtures.
    - _Requirements: 4.4, 4.6, 8.1, 8.2, 8.4, 8.5_
  - [x] 11.4 Author the unreleased change record, rollout/rollback and removed-
    setting notes, storage/live-suite matrix and verification receipt. Include
    source identity, actual commands, named skips/failures and temporary-resource
    cleanup evidence. Record schema/format changes and legacy table retention.
    - _Requirements: 7.3, 7.5, 7.6, 8.8, 8.9_

- [x] 12. Checkpoint: the complete implementation is green and reviewable
  - Run the full enforced bar: `cargo +nightly fmt --all`, `cargo lint --locked`,
    `cargo check --workspace --locked`, `cargo nextest run --workspace --locked`,
    `cargo test --workspace --doc --locked`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Execute the explicitly configured live suites and transport/property selections;
    validate Markdown links, schema compatibility, traceability and the retirement
    inventory. Retain evidence for each result; default ignored/skipped live tests
    do not satisfy this checkpoint. Re-run required checks if integration changes
    the implementation. Present one PR with an unreleased change record and exact
    head for human merge review; no deployment is authorized by this checklist.
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9_

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1", "2"],
  "4": ["2", "3"],
  "5": ["4"],
  "6": ["4", "5"],
  "7": ["5", "6"],
  "8": ["3", "5", "6", "7"],
  "9": ["8"],
  "10": ["5", "6", "7", "8", "9"],
  "11": ["2", "3", "5", "6", "7", "8", "9", "10"],
  "12": ["10", "11"]
}
```

## Notes

- Implementation and verification are complete. The
  [verification receipt](../../../docs/testing/activity-dispatch-verification.md)
  records the final 4,194-test workspace bar, all 18 property tags, nine shared
  100-case live properties, three 100-case physical race entries, transaction
  refusal, production query plans, isolated 101-run acquisition and exact remote
  read counts. Six positive model configurations and nine negative controls pass
  their intended outcomes. Retained manifests distinguish each tested revision,
  named skips and earlier failures. All five task-owned ephemeral databases and
  both disposable devboxes are verified deleted. The unreleased change record
  and stopped-upgrade/rollback notes are complete. The single implementation
  [PR #343](https://github.com/tokeira/tokeira/pull/343) is open for exact-head
  human review; publication completes checkpoint 12, while merge awaits approval.
- Requirements, design and tasks are approved for implementation in one PR.
  Checked boxes must reflect completed evidence.
- Property tasks require `proptest` with at least 100 cases and the exact tags above;
  fixed schema constants/status codes use example tests in addition. Live physical
  tests remain DSQL-only and do not add DSQL limits to the memory store.
- Run focused formatting/check/clippy/nextest checks after each coherent group,
  including the explicit checkpoint at task 4. Use controlled clocks and channels,
  never sleeps to order test races. Do not interrupt a build under five minutes.
- Before editing specs, snapshot their state per root §6. A discovered contradiction
  with pinned Temporal behavior is resolved against source and reviewed before
  amending approved requirements/design. Routine migration version allocation above
  the immutable prefix is already part of this plan, not a baseline re-cut.
- Temporary verification infrastructure is not deployment of Tokeira. Obtain any
  required payload/resource authorization using a concrete filtered manifest at
  that point; prior storage-contract uploads do not imply activity-source upload
  approval. Record and verify cleanup for every created verification resource.
- No dependencies or shared cache/toolchain configuration changes are planned.
  Commits use the required human author and Codex attribution; merge requires the
  user's approval of the final exact head under root §10.7.


### Verification checkpoint

The shared scheduler and broker policy preservation properties pass 100 cases
apiece. The real loopback gRPC read-count fixture passes fresh start, same-request
replay and replay at a replacement execution owner. All-target clippy for storage,
runtime and edge, including `dsql-integration`, passes with the added plan,
physical race/fence and 101-run/split-state acquisition fixtures. The full
workspace bar passes for source
`dc372deeb74ada2cf5cba24de961a6ddca8c68c328b35c747664cdbcc59eac08`:
4,194 tests passed, two default SDK integration tests ignored, one doctest passed
and 21 generated/documentation examples ignored, with formatting, lint, check
and warning-free rustdoc green. Fresh DSQL schema bootstrap passes. Atomic,
ordered traversal, malformed/legacy/collision traversal, paired commits,
bounded repair, staged publication and replay observation each pass all 100 live
cases. The physical row-limit refusal and legacy replacement race also pass.
Repair finishes in 1,172 seconds
under the unchanged 20-minute limit. The separate 101-run acquisition fixture
passes in 739 seconds, including all 229 slots, the 129-slot run's 2 MiB split
authoritative state, exact deadline restoration and the non-serving barrier.
Legacy disposal timed out; its fixture now
batches setup writes while preserving the production path, assertions, case
count and deadline. The revised memory property, workspace bar and all 100 live
disposal cases pass; the live rerun finishes in 1,081 seconds within the unchanged
20-minute limit. Two physical replay/fence fixtures initially
expected an error from the public adapter, which deliberately maps serialization
conflicts to `Ok(false)` so the owner rereads authority. Their corrected checks
require the raw SQL serialization error before that mapping, preserving all
100 cases and authoritative-state invariants. Both corrected physical entries
pass all 100 cases, in 277 and 154 seconds respectively. No production replay
change is included. The original query-plan test exposed a residual key filter
within timestamp ties. The corrected constant cutoff comparison passes the full
bar; production plan and traversal reruns on a fresh database remain required.
Isolated read counts remain open. Property 1's independent
full-field oracle passes all 100 cases in memory and on DSQL; the live entry
finishes in 130 seconds. All nine shared storage property entries have now
passed on DSQL, with the final query correction still awaiting its targeted
plan and ordered/malformed traversal reruns. Compiled fixtures do not close
shared-backend tasks.

`tla 0.21.2` checked all six positive dispatch-handoff/repair configurations and
all nine unchanged negative controls: positives pass; each negative violates its
intended safety or liveness property. The mapping documents receipt replay as
an authority stutter without claiming the unmodeled UUID/response protocol is
formally proved. All 18 property tags and required PBT tasks are present.

The change record and [verification/rollout notes](../../../docs/testing/activity-dispatch-verification.md)
are authored. Their completion receipt stays open until the immutable live
archives, exact SQL evidence and resource cleanup are recorded. The approved
historical link correction in the older workflow-dispatch requirements passes
the full repository offline Markdown check with zero errors. Deletion of the
original disposable devbox and database is verified; their replacements remain
active for the live campaign. No implementation commit or PR exists yet.
