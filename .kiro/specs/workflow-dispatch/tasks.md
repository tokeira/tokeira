# Implementation Plan: Durable Workflow-Task Dispatch

This plan implements the [requirements](requirements.md) and [design](design.md).
Implementation proceeds in independently verified PRs. The initial scope is
single-owner; deferred competing-owner work remains separately unchecked below.
Completed checks and remaining verification are recorded in
[implementation evidence](implementation-evidence.md).

The external `lease-fence` prerequisite means the passing transaction-local
ownership protocol described in the design and
[30_bundle_lease.tla](../../../spec/tla/30_bundle_lease.tla). It must be implemented
and verified separately before competing-owner acquisition repair is enabled.
Tasks 17–18 consume that protocol and do not redesign lease acquisition/renewal.
Single-owner reconciliation and backlog retirement do not depend on those tasks.

## Tasks

- [x] 1. Add the shared dispatch model and storage schema
  - [x] 1.1 Implement pure row derivation and typed coordinates
    - Add `crates/tokeira-storage/src/workflow_dispatch.rs` with the row,
      incarnation, range, and position types from the design. Derive a row only
      for a running run's unstarted normal task, including transient retries.
    - Use the execution-home bundle written to hot state. Classify explicit
      stored deployment coordinates as Exact and other policies as Live; do not
      consult the registry, broker, clock, or existing dispatch row.
    - Carry priority/fairness metadata and the pending absolute deadline. Use
      normal fallback for legacy affinity without a recoverable sticky deadline;
      preserve separate speculative delivery callers.
    - _Requirements: 1.1, 1.6, 1.7, 7.5, 7.8, 10.1, 10.4, 10.5, 10.6_
  - [x] 1.2 Add forward-only table and index migrations
    - Use V074–V076 after the immutable V073 prefix. Add one
      table statement, the partial normal-queue index, and the complete home
      index as separate migrations; await ASYNC index success before readiness.
    - Implement the design's domain-separated, length-prefixed SHA-256 lookup
      keys using the existing dependency. Keep full raw coordinates for exact
      comparisons and preserve absent versus empty values.
    - Add migration/DDL contract tests, checked integer and size conversions,
      and fixed digest vectors. Preserve all existing migration bytes, frozen
      state extensions, and activity schema.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 12.8_
  - [x] 1.3 Add read-only repository paging on both backends
    - Extend `RunRepository` with normal-range and execution-home page methods.
      Implement first-page and strict keyset queries ordered by
      `(priority_key, scheduled_at, run_key)` and home walks ordered by run key.
    - Match that order explicitly in memory. Return the last examined position
      even when later filtering rejects every row. Read every execution home
      for a queue; never restrict discovery to locally owned run shards.
    - Use existing storage-director permits and short page transactions. Do not
      retain a connection, database snapshot, or cursor across an entire pass.
    - _Requirements: 3.1, 3.2, 3.3, 3.5, 3.7, 8.3, 10.2, 10.7_
  - [x] 1.4 Add reusable generated-test adapters and controlled failures
    - Use existing storage test infrastructure to run the same reference traces
      on memory and real DSQL. Add narrowly scoped transaction-failure hooks,
      injected lookup collisions, and deterministic synchronization where needed.
    - Keep the oracle independent of production derivation and map ordering. Do
      not substitute a mocked database or PostgreSQL for DSQL transaction tests.
    - _Requirements: 12.1, 12.2, 12.8_

- [x] 2. Make normal workflow-task incarnation allocation complete
  - [x] 2.1 Centralize checked normal-task sequence allocation
    - In `tokeira-kernel`, add the pure allocator and retained-pending renewal
      helper. Renew the sequence in ordinary retained failure retries and when
      resuming a retained unstarted normal task; retain existing fresh-schedule,
      timeout, priority, and version supersession semantics.
    - Reject exhaustion before mutation. Preserve attempts, event suppression,
      virtual event IDs, advice resets, and the allocator's persisted layout.
      Do not modify the speculative scheduling loop or admitted-update recovery.
    - _Requirements: 1.5, 1.7, 2.1, 2.2, 2.7, 10.4, 10.6_
  - [x] 2.2 Apply the authoritative normal-start fence consistently
    - Require Running status, matching sequence, and unstarted pending state.
      Preserve completion of already-started tasks under existing pause policy.
    - Carry the renewed sequence through publication, start submission, timeout
      tracking, and existing task-token construction. Keep one request value/ID
      during retries of a single submission; add no durable start-result cache.
    - Retain public error/token behavior and the existing recovery through
      start-to-close timeout after an ambiguous committed start.
    - _Requirements: 2.2, 2.3, 2.4, 2.7, 6.4, 10.5_
  - [x] 2.3 Required property test: Property 2 — Incarnation fencing and observable retry preservation
    - Implement `proptest` with at least 100 cases in the kernel tests, with
      runtime submission/token cases where required. Generate retained failures,
      pause/resume, supersession, delayed old offers, repeated submissions,
      timeout and reset lineage; distinguish repeated results from commits.
    - Compare attempts and history/event suppression to independent expected
      traces grounded in Temporal v1.31.0. Add a fixed exhaustion regression.
    - Tag: `// Feature: workflow-dispatch, Property 2: Incarnation fencing and observable retry preservation`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.7, 12.2_

- [x] 3. Maintain dispatch in every authoritative storage transaction
  - [x] 3.1 Integrate shared derivation into memory and DSQL commits
    - Apply the full upsert or run-key delete within the existing transaction or
      memory critical section, after rejection/deduplication gates. Include
      scheduling, start, retry, priority/routing changes, pause/resume, close,
      and reset materialization.
    - Remove the derived row in the existing fenced physical deletion path.
      Preserve hot-state CAS, history, projection accumulation, and recovery-flag
      updates; add no per-commit prior-dispatch read for lifecycle authority.
    - Use the initial one-maintenance-statement-per-mutating-commit policy.
      Document transaction/OCC reasoning rather than optimizing unchanged writes
      before the invariant is established.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.5, 10.4_
  - [x] 3.2 Required property test: Property 1 — Atomic derived equality
    - Implement at least 100 `proptest` cases per backend through the shared
      adapters. Generate transitions and reset materializations, including
      duplicate/rejected commands and failures between state and dispatch writes.
    - Assert reference-derived equality after each successful commit and no
      partial mutation on failure. Add direct boundary checks for kernel purity
      and derivation's independence from prior dispatch rows.
    - Tag: `// Feature: workflow-dispatch, Property 1: Atomic derived equality`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.5, 12.1_

- [x] 4. Checkpoint: storage and incarnation changes are green
  - Run nightly formatting, locked checks and all-target clippy for the affected
    kernel/storage crates, and their focused nextest suites and doctests. Verify
    migration contracts and frozen-state fixtures without changing dependencies.
  - The shared DSQL test cases must exist here; their live execution and evidence
    are mandatory in Task 15. No competing-owner claim is made at this checkpoint.
  - _Requirements: 1.2, 2.1, 10.3, 10.4, 12.1, 12.9_

- [ ] 5. Add bounded memory-only workflow offers
  - [ ] 5.1 Retain incarnation identity across ready and in-flight delivery
    - In `broker.rs`, move `(run_key, logical_seq)` from ready to in flight on
      take. Deduplicate notification and discovered copies against both sets.
    - Implement the 5-second offer lease, 5-second ready retention interval,
      256-incarnation per-physical-queue cap, and 8,192-incarnation home cap.
      Share capacity across delivery sources and release it on the designed
      outcome, expiry, cancellation, or eviction; make no durable claim writes.
    - Preserve query/speculative and already-started eager response lifecycles.
    - _Requirements: 2.6, 3.6, 3.8, 4.1, 4.5, 4.6, 6.1, 6.2, 6.5, 10.5, 10.6_
  - [ ] 5.2 Wire start outcomes and cancellation to the offer lease
    - Integrate `start_polled_workflow_task_inner` outcomes without treating
      lease expiry as permission to undo a committed start. Stale/closed/paused
      work releases the entry and follows existing poll handling.
    - Keep ambiguous delivery suppression bounded; retry a submission only under
      the existing retry policy. Broker restart and home retirement discard
      volatile entries without deleting durable intent.
    - _Requirements: 5.6, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_
  - [ ] 5.3 Required property test: Property 6 — Volatile offer loss and ambiguity
    - Implement at least 100 `proptest` cases in broker/runtime tests over
      notifications, duplicate pages, takes, cancellation, expiry, and ambiguous
      starts. Inspect repository writes to prove consumers author no delivery
      state. Assert lost unstarted offers can return and consumed starts cannot.
    - Tag: `// Feature: workflow-dispatch, Property 6: Volatile offer loss and ambiguity`
    - _Requirements: 2.6, 3.8, 6.1, 6.2, 6.4, 6.5, 6.6, 12.2_

- [ ] 6. Implement queue-home passes with bounded scheduler slices
  - [ ] 6.1 Add the reusable discovery source and pass scheduler
    - Add `runtime/discovery.rs` at `crates/tokeira-runtime/src/discovery.rs`,
      with the design's typed range/page/position interfaces and workflow source.
      Keep activity integration possible without adding an activity source yet.
    - Implement 64-row pages, one-page slices, 64 new admissions per range/pass,
      at most eight concurrent queries, and the one-second periodic trigger.
      Fairly grant slices and capacity reservations across runnable ranges.
    - Preserve a pass's continuation on slice yield. Discard it on completion,
      cancellation, failure, retirement, or a capacity stop; wake a fresh head
      pass when capacity returns. Never add a hard scan cap that restarts a long
      blocked prefix on every slice.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 4.1, 4.3, 4.4, 4.5, 4.6, 4.7, 10.7_
  - [ ] 6.2 Resolve routing and filter candidates before admission
    - Select Exact ranges and a coalesced Live range per queue family. Compare
      raw coordinates after digest lookup, advance past collisions/known/stale
      rows, and hydrate Live candidates serially for existing registry resolution.
    - Preserve deployment feature modes, pinned/transition and current/ramping
      behavior, and authoritative start revalidation. Skip incompatible rows
      without charging admissions or ending the pass.
    - Pass priority/fairness metadata to existing broker policy. Treat a failed
      post-commit notification as acceleration loss, not loss of durable intent.
    - _Requirements: 3.6, 3.7, 3.8, 4.2, 4.3, 10.2, 10.5_
  - [ ] 6.3 Required property test: Property 3 — Ordered read-only traversal
    - Implement at least 100 `proptest` cases across storage page adapters and
      runtime filtering. Generate ties, page sizes, collisions, mutations between
      pages, held rows, and multiple execution homes. Verify read-only traversal,
      strictly advancing positions, and later head discovery of moved/new rows.
    - Tag: `// Feature: workflow-dispatch, Property 3: Ordered read-only traversal`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 10.2_
  - [ ] 6.4 Required property test: Property 4 — Bounded slices without prefix starvation
    - Implement at least 100 `proptest` cases in discovery tests, generating
      finite held/incompatible prefixes, capacity changes, and fair competing
      ranges. Include a fixed prefix longer than both 64-row budgets, and track
      one pass through multiple slices until the later compatible row is reached.
    - Assert separate retained, admission, query, and slice bounds. Check constant
      defaults directly; distinguish finite total-prefix work from a per-slice
      bound. Full capacity must leave durable intent intact.
    - Tag: `// Feature: workflow-dispatch, Property 4: Bounded slices without prefix starvation`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 12.3_

- [ ] 7. Register discovery from polling and runtime ownership
  - [ ] 7.1 Add poll-demand guards and bounded retirement
    - Register normal poll demand before immediate take in the existing workflow
      poll path, independently of publication. Reference-count active demand,
      coalesce family Live ranges, and keep active demand scheduled.
    - Bridge poll renewal with 30 seconds of idle grace and at most 4,096 idle
      registrations per home. Pause admission without compatible demand; resume
      a retained pass on renewal; evict only idle registrations under pressure.
    - Connect startup, shutdown, and queue-home changes to scheduler cancellation.
      New polls recreate registration after restart; overlapping homes require
      no new durable queue-home fence. Sticky polls do not start periodic scans.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.3, 7.1_
  - [ ] 7.2 Required property test: Property 5 — Demand registration and home independence
    - Implement at least 100 `proptest` cases in runtime/broker tests over polls,
      renewals, cancellation, idle eviction, broker restart, and overlapping or
      retiring homes. Drop all publications and verify registration still works.
    - Assert memory accounting, fair scheduling of active registrations, and no
      duplicate committed start despite duplicate offers from different homes.
    - Tag: `// Feature: workflow-dispatch, Property 5: Demand registration and home independence`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.3_

- [ ] 8. Reconstruct sticky schedule-to-start deadlines
  - [ ] 8.1 Add deadline recovery entries and epoch-scoped tracking
    - Extend `RecoveryEntries` and `is_empty()` with unstarted normal-task
      deadline entries, preserving the recovery flag's coverage of pending tasks.
      Derive from pending state even after ResetStickyTaskQueue clears affinity.
    - Arm the same absolute deadline after commits and during acquisition. Install
      before serving, process only after serving, and make overdue deadlines due
      at the first processing opportunity. Cancel tracking with its acquisition.
    - Preserve paused-task timeout policy, stale-sequence rejection, normal
      fallback, and speculative timers owned by their existing mechanism.
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.1, 10.6_
  - [ ] 8.2 Required property test: Property 7 — Sticky recovery and affinity independence
    - Implement at least 100 `proptest` cases across recovery/timeout and storage
      entry tests. Generate restart before/after deadline, affinity reset,
      supersession, pause/resume, and start/timeout races using injected clocks.
    - Assert restoration before Active, no deadline extension, no periodic sticky
      scans, no affinity mutation from fallback reads, and normal discoverable
      replacement after a valid timeout. Include legacy missing-deadline fallback.
    - Tag: `// Feature: workflow-dispatch, Property 7: Sticky recovery and affinity independence`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 12.2_

- [ ] 9. Add complete single-owner execution-home reconciliation
  - [ ] 9.1 Implement one-run repair transactions on both stores
    - Implement `reconcile_workflow_dispatch_run` for single-owner acquisition.
      Carry execution-home context and the run key; read authoritative state in
      that same transaction, derive, and upsert/delete. Task 17 supplies the
      transaction-local competing-owner fence when its prerequisite lands.
    - Validate home agreement and row encoding; account for all transaction writes
      within aggregate limits. Read pages are not write batches. Retry complete
      transactions after OCC conflict with fresh state and acquisition validation.
    - Make repair idempotent without changing history, hot state, transition
      sequence, projection accumulator, or recovery flag. Fail on corruption,
      ownership loss, decode error, and exhausted retries rather than skipping.
    - _Requirements: 8.7, 8.8, 8.9, 8.10, 10.8_
  - [ ] 9.2 Wire the two walks and execution-home serving gate
    - Keep the acquired home Sweeping. Page recovery candidates through NULL/true
      phases, repair them, and reconstruct timeouts; then sweep every home-index
      dispatch key, including sticky, absent-run, and recovery-excluded rows.
    - Verify cancellation, owner/epoch, and local expiry before publishing Active
      for that acquisition. Gate every relevant run writer, including retention
      and materialization, on the same execution home; test where lane-local
      run-key placement differs. Retain cancellation and local lifecycle checks;
      competing-owner write fencing remains Task 17.
    - Abandon lost acquisitions and restart both walks from the head after failure;
      never persist a continuation or serve after partial repair.
    - _Requirements: 8.1, 8.2, 8.3, 8.7, 8.9, 8.11, 12.5_
  - [ ] 9.3 Required property test: Property 8 — Complete bounded repair
    - Implement at least 100 `proptest` cases per backend over missing, stale,
      and mismatched rows; NULL/false recovery flags consistent with their state;
      sticky tasks, closed runs, and absent runs. Interrupt after arbitrary batches.
    - Assert both row invariants only after complete activation, no authoritative
      mutation from repair, bounded writes, and head-restart convergence. Inject
      decode/write/query failures and prove the home remains non-serving.
    - Tag: `// Feature: workflow-dispatch, Property 8: Complete bounded repair`
    - _Requirements: 8.1, 8.2, 8.3, 8.7, 8.8, 8.9, 8.10, 8.11, 12.2, 12.5_

- [ ] 10. Checkpoint: runtime discovery and acquisition are green
  - Run affected-crate locked checks, all-target clippy, focused nextest suites,
    and doctests for broker, discovery, registration, sticky timers, and repair.
    Confirm every background task cancels and every new error has the design's
    existing admission/poll behavior and actionable diagnostics.
  - Leave competing-owner integration and its verification in Tasks 17–18 until
    the external lease prerequisite is satisfied. Single-owner completion may
    proceed; do not report its results as competing-owner verification.
  - _Requirements: 4.1, 5.4, 6.5, 7.3, 8.1, 8.9, 10.8, 12.9_

- [ ] 11. Retire workflow backlog use and implement bounded legacy disposal
  - [ ] 11.1 Remove workflow writes, grace demotion, and regular draining together
    - In `backlog.rs`, broker, publisher, and runtime wiring, remove only workflow
      backlog paths after the replacement discovery/recovery paths are connected.
    - Preserve activity backlog, its shared loops, `activity_dispatch`, and
      `reconcile_due_activity_dispatches_once`. Add no mixed-release branch or
      durable workflow delivery checkpoint.
    - _Requirements: 9.1, 9.2, 9.3, 9.5, 9.6, 10.7_
  - [ ] 11.2 Implement the reconstruction-coverage cleanup predicate
    - Enumerate legacy workflow backlog keys with pass-local paging. In each delete
      transaction, check the design's state/dispatch equality predicate, including
      absence after stale-row repair; defer uncovered entries and page past them.
    - Delete only covered workflow keys, at most 64 keys/4 MiB per transaction,
      splitting further as necessary. Wake retries after acquisition progress;
      restart at the head after failure without changing run or dispatch state.
    - Put the exact upgrade line in the implementation's release/upgrade note:
      `Upgrade with every node stopped.`
    - _Requirements: 9.3, 9.4, 9.5, 9.7, 12.8_
  - [ ] 11.3 Required property test: Property 10 — Workflow-only backlog retirement
    - Implement at least 100 `proptest` cases with old workflow and activity
      entries, uncovered/covered/absent runs, cleanup interruption, and concurrent
      new-release commits. Assert idempotent bounded disposal and unchanged
      activity bytes/delivery; assert no new workflow backlog write/grace/drain.
    - Add a direct assertion for the exact upgrade note and a regression proving
      activity reconciliation remains wired.
    - Tag: `// Feature: workflow-dispatch, Property 10: Workflow-only backlog retirement`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 10.7_

- [ ] 12. Preserve routing, scheduling policy, statistics, and state contracts
  - [ ] 12.1 Update workflow accounting and discovery diagnostics
    - Derive workflow durable-backlog statistics from dispatch intent without
      double counting retained copies. Preserve activity/query sources and the
      existing priority/fairness delivery policy.
    - Add low-cardinality counters/timing for reads, known/stale/incompatible
      skips, admissions, slice yields, capacity blocks, pass duration, repairs,
      and ownership loss. Do not add queue/run/worker/deployment metric labels.
    - _Requirements: 4.1, 4.6, 10.5, 11.4_
  - [ ] 12.2 Required property test: Property 11 — Routing, state, and delivery preservation
    - Implement at least 100 `proptest` cases across storage and runtime tests
      over routing configurations, version policies, feature modes, priorities,
      fairness metadata, and delivery modes. Check compatible resolution without
      freezing Live targets, plus query/eager/speculative/activity preservation.
    - Reuse frozen-state fixtures and generated commit traces to preserve the
      accumulator on both stores. Add direct migration-checksum, schema-shape,
      index-width, and no-double-count statistics assertions.
    - Tag: `// Feature: workflow-dispatch, Property 11: Routing, state, and delivery preservation`
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_

- [ ] 13. Encode the concrete hand-off refinement in model checks
  - [ ] 13.1 Add bounded paging, retry, and repair refinement cases
    - Extend or add a companion to `40_dispatch_handoff.tla` with enough distinct
      positions for a multi-page blocked prefix, slice continuation, retained
      failure retry as a new incarnation, and interrupted two-walk acquisition.
    - Map intermediate repairs to non-serving stuttering steps and the final
      same-epoch serving gate to Reconcile. Keep weak fairness and explicit
      eventual fault cessation; add no notification fairness assumption.
    - _Requirements: 6.6, 12.4, 12.5, 12.6_
  - [ ] 13.2 Add positive and negative configuration checks
    - Assert both row invariants for complete repair. Add negative controls for
      restarting at every slice, retained retry identity, and omitted stale-row
      deletion; preserve existing controls and their intended failure outcomes.
    - Keep the stale-rows configuration's deliberately weaker claim explicit.
      Connect concrete action/property names to code/test anchors in the model
      documentation; record actual model commands/results at the final checkpoint.
    - _Requirements: 12.3, 12.4, 12.5, 12.6, 12.9_

- [ ] 14. Add end-to-end loss and progress regressions
  - [ ] 14.1 Required property test: Property 12 — Next-pass recovery and eventual resolution
    - Implement at least 100 `proptest` cases over end-to-end runtime traces with
      finite faults and a fair completion phase. Generate healthy serving homes,
      compatible demand, sufficient capacity, and competing finite prefixes.
    - Drop the scheduled task's publication, synchronize the next head pass, and
      assert an offer by its completion without restart. Separately test eventual
      start or no-longer-wanted resolution under the documented assumptions.
    - Use injected clocks, channels, barriers, or Notify rather than sleeps. Do
      not turn the scheduling period into an unsupported wall-clock deadline.
    - Tag: `// Feature: workflow-dispatch, Property 12: Next-pass recovery and eventual resolution`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 12.4, 12.6_
  - [ ] 14.2 Add fixed cross-component regressions for failure boundaries
    - Cover all publications lost then fresh polling after broker restart;
      committed start with lost reply then start-to-close recovery; overlap of
      queue homes; overdue sticky acquisition after affinity reset; and repeated
      acquisition failure before a successful full repair.
    - Include stopped-upgrade fixtures that reconstruct missing rows and clean
      legacy workflow backlog while activity recovery remains operational.
    - _Requirements: 5.3, 5.5, 6.3, 6.4, 7.4, 7.5, 8.7, 9.3, 9.6, 12.2_

- [ ] 15. Add and execute reproducible live DSQL verification
  - [ ] 15.1 Implement real-DSQL query-plan and transaction integration tests
    - Use the existing DSQL test connection path and schema runner. Exercise first
      and continuation pages, Exact/Live plans, partial index selection, digest
      widths at valid name limits, deployment/build selection, and ASYNC readiness.
    - Add multi-statement rollback and aggregate row/byte-limit boundary cases for
      commit, reset, repair, and legacy disposal; verify whole-transaction abort.
    - Include generated single-owner storage suites for Properties 1, 3, 8, 10,
      and the storage portions of Property 11, at least 100 cases per property.
    - _Requirements: 1.2, 1.3, 8.8, 9.4, 10.2, 10.4, 12.1, 12.8_
  - [ ] 15.2 Checkpoint: run the DSQL suites and preserve accurate evidence
    - Run only on a newly created ephemeral real DSQL cluster under the authorized
      profile and region. Ensure cleanup runs on success or failure and touches
      only that cluster; keep endpoints/identifiers/credentials out of artifacts.
    - Record test/model revisions, cases, transaction outcomes, query plans,
      rows/statements read, and pass durations. Separate reported prior playground
      observations from experiments actually performed by this implementation.
    - If live verification is unavailable, report the unmet checks and leave this
      task incomplete; a local simulation or PostgreSQL result cannot complete it.
    - _Requirements: 10.2, 10.8, 12.1, 12.8, 12.9_

- [ ] 16. Single-owner implementation checkpoint
  - Verify every in-scope property below has its required executable test, every
    in-scope acceptance criterion has implementation or verification coverage, and all positive/negative
    model checks have their expected outcomes. Add module/public-item and inline
    concurrency reasoning required by AGENTS.md, citing Temporal source where
    observable behavior is non-obvious.
  - Run the implementation completion bar with the lockfile unchanged: nightly fmt,
    `cargo lint --locked`, `cargo check --workspace --locked`,
    `cargo nextest run --workspace --locked`, `cargo test --workspace --doc --locked`,
    and `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
    Run Markdown link and whitespace checks for implementation documentation.
  - Reconcile actual commands/results with the evidence record. Do not mark live
    DSQL, ownership safety, or a property test complete from plan text alone.
    Keep Tasks 17–18 and Requirements 8.4–8.6 outstanding; this checkpoint does not
    declare the full competing-owner design implemented.
  - _Requirements: 10.3, 10.4, 10.5, 10.6, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9_

- [ ] 17. Consume the lease fence and verify competing-owner repair
  - Deferred: requires the separately implemented and verified transaction-local lease fence.
  - [ ] 17.1 Protect every repair and final activation against ownership loss
    - Consume the landed home/owner/epoch fence inside every repair transaction.
      Preserve takeover/release conflict ordering, compatible renewal, and local
      expiry self-fencing. A lost acquisition cannot publish serving state.
    - _Requirements: 8.4, 8.5, 8.6, 10.8, 12.5_
  - [ ] 17.2 Required property test: Property 9 — Ownership loss fences repair
    - Implement at least 100 generated interleaving cases with deterministic
      barriers in runtime and storage tests, including the real DSQL adapter.
      Race old-owner repair, takeover, renewal, successor writes, and activation.
    - Verify stale repair cannot overwrite successor state, no lost acquisition
      becomes serving, and renewal does not spuriously abort valid owner writes.
      Include differing execution-home/local-lane mappings and final-gate races.
    - Tag: `// Feature: workflow-dispatch, Property 9: Ownership loss fences repair`
    - _Requirements: 8.4, 8.5, 8.6, 10.8, 12.5, 12.8_

- [ ] 18. Execute competing-owner live DSQL verification
  - Deferred: requires Task 17's transaction-local fence and concurrency tests.
  - Run deterministic renewal/takeover/repair and final-activation races on an
    owned ephemeral real DSQL cluster, including at least 100 Property 9 cases.
    Record actual outcomes and lifecycle times under Task 15's evidence rules.
  - _Requirements: 8.4, 8.5, 8.6, 10.8, 12.5, 12.8, 12.9_

## Task Dependency Graph

The graph covers top-level tasks. Within each task, sub-tasks execute in listed
order. `lease-fence` is an external prerequisite, not an instruction to implement
unrelated lease work in this feature.

```json
{
  "external_prerequisites": {
    "lease-fence": "Landed and verified transaction-local execution-home ownership fence described in design.md"
  },
  "depends_on": {
    "1": [],
    "2": ["1"],
    "3": ["1", "2"],
    "4": ["1", "2", "3"],
    "5": ["4"],
    "6": ["4", "5"],
    "7": ["5", "6"],
    "8": ["3", "5"],
    "9": ["3", "8"],
    "10": ["5", "6", "7", "8", "9"],
    "11": ["10"],
    "12": ["7", "11"],
    "13": ["2", "6", "9"],
    "14": ["10", "11", "12", "13"],
    "15": ["14"],
    "16": ["12", "13", "14", "15"],
    "17": ["9", "lease-fence"],
    "18": ["15", "17"]
  }
}
```

## Property-to-Task Coverage

| Design property | Required PBT task | Live DSQL execution where applicable |
|---|---|---|
| 1: Atomic derived equality | 3.2 | 15 |
| 2: Incarnation fencing and observable retry preservation | 2.3 | Storage effects also exercised by 3.2/15 |
| 3: Ordered read-only traversal | 6.3 | 15 |
| 4: Bounded slices without prefix starvation | 6.4 | Plan/read-cost evidence in 15 |
| 5: Demand registration and home independence | 7.2 | Start checks in 15; competing-owner fence deferred to 17.2/18 |
| 6: Volatile offer loss and ambiguity | 5.3 | Atomic start/rollback also exercised by 3.2/15 |
| 7: Sticky recovery and affinity independence | 8.2 | State/reconstruction also exercised by 9.3/15 |
| 8: Complete bounded repair | 9.3 | 15 |
| 9: Ownership loss fences repair | 17.2 (deferred) | 18 (deferred) |
| 10: Workflow-only backlog retirement | 11.3 | 15 |
| 11: Routing, state, and delivery preservation | 12.2 | Storage portions in 15 |
| 12: Next-pass recovery and eventual resolution | 14.1 | Query-cost evidence in 15; no wall-clock guarantee |

## Notes

- All PBT tasks are required, use the workspace-standard `proptest`, and run at
  least 100 cases. An independent reference model must supply expected outcomes;
  comparing production Derive to itself is insufficient. Direct contract checks
  complement generated tests for fixed facts such as schema and upgrade text.
- The default test suite needs no live credentials. Real DSQL tests follow the
  repository's explicit integration-test convention and remain a required
  implementation checkpoint, not an implicit default-suite dependency. No new
  dependency, workspace configuration change, or shared cache/toolchain change is
  authorized by this plan.
- Runtime delivery wiring and workflow backlog retirement form one coherent
  release boundary. Intermediate code may be reviewed in dependency order, but
  intermediate PRs keep existing workflow delivery and recovery operational.
  Enable bounded, expiring offers and retire workflow backlog paths only when
  dispatch reconstruction and the serving gate are wired and verified together.
  Upgrade with every node stopped.
- Read crate-local instructions before implementing kernel/storage/runtime work.
  Keep API shape and public behavior tied to the vendored protos and Temporal
  v1.31.0 sources cited in requirements/design. Existing request-result gaps are
  not authorization for a new public RPC or persistent delivery ledger.
- Keep signal/update limits, frozen extension tag 2, external-signal outcome
  mapping, the verified affected-row behavior, and admitted-update/speculative
  scheduling work in their owning changes. Rebase around those changes. Preserve
  the already-landed projection accumulator and extension tag 3.
- Activity queue-home discovery and activity backlog retirement are the dependent
  follow-on specification. Keep their present dispatch/pass/backlog paths intact.
  Operations, close intents, successors, and projection discovery remain outside
  this plan.
- Each implementation PR starts with a code/tests commit and a spec-alignment
  commit recording completed tasks and actual evidence. Review fixes follow as
  additional commits. Run the full completion bar for each implementation PR.
