# Implementation Plan

## Tasks

- [x] 1. Establish the durable delivery-order contract
  - [x] 1.1 Add `DeliveryOrder` to the storage API
    - Define the dependency-neutral `(priority_key, fair_pass, insertion_tie)` value
      in `tokeira-storage`, with the traits required by runtime ready maps, codecs,
      and repository implementations.
    - Keep Priority and fair-pass policy out of storage; storage receives an already
      assigned order.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 11.5_
  - [x] 1.2 Extend the backlog entry and codec
    - Carry raw/effective Priority and `DeliveryOrder` through `BacklogEntry`,
      workflow/activity backlog payloads, and their postcard codec.
    - Add genuine pre-change bytes fixtures proving absent fields decode to the
      authorized pre-baseline defaults.
    - _Requirements: 6.1, 6.5, 6.6, 6.9_
  - [x] 1.3 Correct durable dispatch identity
    - Derive workflow row identity from `(run_key, logical_seq)` and activity row
      identity from `(run_key, activity_id, attempt, stamp)`.
    - Preserve idempotence for the same logical dispatch while preventing collisions
      between distinct work whose broker insertion counters happen to match.
    - _Requirements: 6.8_
  - [x] 1.4 Amend the pre-baseline DSQL backlog schema and index
    - Read `crates/tokeira-storage/AGENTS.md` before editing.
    - Amend the authorized pre-baseline migrations to store non-null
      `priority_key`, `fair_pass`, and `insertion_tie`, and index the queue prefix
      followed by that ordering tuple.
    - Do not add an `ALTER TABLE` migration or a shared mutable fairness counter.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.8, 11.5_
  - [x] 1.5 Implement identical memory and DSQL backlog ordering
    - Make both repositories drain by
      `(priority_key ASC, fair_pass ASC, insertion_tie ASC)`.
    - Keep queue-family filters, task-kind isolation, deployment routing, and
      idempotent deletion behavior unchanged.
    - _Requirements: 6.2, 6.3, 6.4, 6.9_
  - [x] 1.6 Property test: Property 9 — backlog identity and storage-order equivalence
    - Generate distinct workflow/activity dispatch identities and arbitrary order
      tuples; prove identity uniqueness and memory/DSQL model ordering equivalence
      over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 9`
    - _Requirements: 6.2, 6.3, 6.4, 6.8, 6.9_

- [x] 2. Checkpoint: storage contract green
  - Run formatting plus focused `tokeira-storage` check, clippy, unit tests, codec
    fixture tests, and DSQL query-shape tests with `--locked`.
  - Confirm the build does not dirty migration or generated files.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.8, 6.9_

- [x] 3. Extend the pure kernel priority model
  - [x] 3.1 Add raw activity and original-option Priority
    - Read `crates/tokeira-kernel/AGENTS.md` before editing.
    - Add trailing, defaulted Priority fields to `ActivityState`,
      `ActivityOriginalOptions`, the activity-scheduled internal event, and all
      constructors/replay reducers.
    - Preserve raw override semantics; do not resolve task-queue configuration in
      the kernel.
    - _Requirements: 3.1, 3.5, 3.6, 7.10, 11.1, 11.2_
  - [x] 3.2 Carry activity and child command Priority
    - Add raw Priority to `ScheduleActivity` and `StartChildWorkflow`, record it in
      the corresponding history attributes, and field-wise merge against workflow
      Priority only when producing declarative dispatch/start effects.
    - _Requirements: 3.1, 3.2, 3.3, 3.7, 3.8, 3.9_
  - [x] 3.3 Stamp workflow and activity dispatch effects
    - Add Priority to workflow-task, activity-task, and child-start `DispatchOp`
      variants and every constructor.
    - Keep clipping, fair-pass assignment, rate limiting, clocks, config reads, and
      broker state exclusively outside the kernel.
    - _Requirements: 2.6, 3.3, 3.8, 11.1, 11.2, 11.3, 11.4_
  - [x] 3.4 Add workflow Priority option updates and WFT redispatch fencing
    - Add a Priority field change to the execution-options command/event path.
    - On a real change, update authoritative state and author one options-updated
      event; if a WFT is pending and unstarted, replace its internal logical
      delivery identity and emit one replacement dispatch without a second public
      `WorkflowTaskScheduled`.
    - Preserve no-op behavior for equivalent patches.
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.11_
  - [x] 3.5 Add activity Priority option updates and stamp fencing
    - Add Priority to activity option field changes.
    - Merge nested fields independently for every matched activity. On every accepted
      pre-start update, including a value-equivalent one, advance the activity stamp
      and emit one replacement effective dispatch; restore-original recovers Priority
      from the schedule-time options.
    - _Requirements: 7.7, 7.8, 7.9, 7.10, 7.12_
  - [x] 3.6 Document the pre-baseline kernel postcard boundary
    - Add recorded pre-change postcard fixtures for every extended state/event
      shape and assert postcard rejects missing trailing positional fields with
      `DeserializeUnexpectedEnd`.
    - State explicitly that the authorized pre-baseline store boundary, rather
      than Serde defaults, makes the state/event layout change safe.
    - _Requirements: 2.1, 3.1, 3.7, 11.5_
  - [x] 3.7 Property test: Property 2 — workflow-lineage priority preservation
    - Generate Priority plus continue-as-new/retry/cron successor sequences and
      prove committed start metadata and WFT dispatch metadata remain equal over at
      least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 2`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_
  - [x] 3.8 Property test: Property 3 — activity and child field-wise inheritance
    - Generate base and raw override Priority values; prove history retains raw
      values while activity dispatch/poll inputs and child starts receive the
      field-wise merged result over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 3`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9_
  - [x] 3.9 Property test: Property 11 — workflow priority update fencing
    - Generate open workflow states and valid Priority patches; prove change/no-op
      event behavior, single replacement dispatch, logical-sequence advance, and
      stale old dispatch admission over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 11`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.11_
  - [x] 3.10 Property test: Property 12 — activity update fencing and restore
    - Generate pending activities, patches, and original options; prove raw state,
      stamp advance, replacement dispatch, stale-offer fencing, restore-original,
      per-activity nested merging, and value-equivalent stamp fencing over at least
      100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 12`
    - _Requirements: 7.6, 7.7, 7.8, 7.9, 7.10, 7.12_
  - [x] 3.11 Property test: Property 17 — kernel determinism and placement invariance
    - Apply identical generated inputs twice and prove byte-equivalent transitions;
      vary only Priority and prove changes remain confined to documented
      state/history/dispatch/fence fields while shard/lane placement is unchanged.
    - Run at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 17`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.8_

- [x] 4. Checkpoint: pure kernel changes green
  - Run formatting plus focused kernel check, clippy, unit tests, property tests, and
    serialization goldens with `--locked`.
  - Audit the diff for I/O, async, storage, metrics, network, mutable config, clocks,
    random values, and retained process state; none may enter `tokeira-kernel`.
  - _Requirements: 2.1, 2.6, 3.1, 3.3, 3.7, 7.2, 7.3, 7.7, 11.1–11.8_

- [x] 5. Implement runtime Priority normalization and delivery modes
  - [x] 5.1 Add the pure Priority resolver
    - Implement default key 3, five-band clipping, field-wise inheritance, task
      weight default/clamping, and queue-override precedence in a pure runtime
      module.
    - Preserve zero/empty as inheritance markers before effective normalization.
    - _Requirements: 1.4, 1.5, 1.6, 1.7, 1.8, 1.9_
  - [x] 5.2 Add `DeliveryModeProvider`
    - Return stock v1.31.0 defaults in production: priority on, User Fairness off,
      auto-enable off.
    - Under `conformance`, consult the three wired Boolean overrides at the live
      decision point; do not introduce a production configuration knob.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.11_
  - [x] 5.3 Add queue-local auto-enable state
    - Track process-lifetime activation in workflow/activity broker policy state.
    - Activate normal root queues under the exact fairness-key/priority conditions;
      never activate sticky queues or persist a Temporal-style task-queue user-data
      object.
    - _Requirements: 10.7, 10.8, 10.9, 10.10, 11.3, 11.8_
  - [x] 5.4 Implement fair-pass and insertion-tie assignment
    - Assign ordering atomically with broker insertion under the broker lock.
    - Resolve weight as queue override → task value → 1.0, clamp effective weight,
      use the v1.31.0 stride reference model, initialize new keys at the band
      frontier, and preserve an existing order on backlog rehydration.
    - _Requirements: 4.1, 4.2, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 6.5_
  - [x] 5.5 Property test: Property 1 — Priority validation/effective reference model
    - Generate base/override/config combinations and prove validation, field-wise
      merge, defaulting, clipping, clamping, and weight precedence against a simple
      reference model over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 1`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9_
  - [x] 5.6 Property test: Property 4 — priority-band ordering and FIFO fallback
    - Generate tasks and publish permutations; prove nondecreasing band order plus
      within-band FIFO when enabled, and global FIFO when disabled, over at least
      100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 4`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.9, 5.1_
  - [x] 5.7 Property test: Property 6 — weighted fair-pass reference model
    - Generate fairness keys and weights; prove stride progression, band-frontier
      initialization, per-key monotonicity, and work-conserving single-key service
      over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 6`
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
  - [x] 5.8 Property test: Property 7 — User/Drain Fairness independence
    - Generate queue budgets and within-queue workloads; prove user weights cannot
      change the inter-queue budget and budget changes cannot reorder already
      assigned tasks over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 7`
    - _Requirements: 5.9, 11.7_
  - [x] 5.9 Property test: Property 16 — mode and auto-enable state machine
    - Generate override changes and observed task Priority; prove stock defaults,
      fairness implication, priority/FIFO mode, sticky exclusion, and monotonic
      process-lifetime activation over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 16`
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11_

- [x] 6. Replace ready FIFOs with ordered workflow/activity brokers
  - [x] 6.1 Add a reusable ordered ready structure
    - Replace per-queue FIFO storage with `DeliveryOrder`-keyed ready maps while
      retaining queue-local ownership, dedupe, wake, expiry, and cancellation
      behavior.
    - Keep direct handout when no competing work and keep speculative WFTs on their
      existing exception path.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.9_
  - [x] 6.2 Carry Priority and preserved order on dispatchable tasks
    - Extend workflow/activity dispatchables and every producer/consumer with
      Priority plus optional preserved `DeliveryOrder`.
    - Change activity broker dedupe to include the activity stamp so a replacement
      can coexist with, and fence, its obsolete offer.
    - _Requirements: 2.6, 3.3, 6.5, 7.8, 7.9_
  - [x] 6.3 Compare sticky and declared-normal work by Priority
    - Thread non-empty sticky `normal_name` into runtime poll admission.
    - Wake sticky waiters for normal-queue publication and choose the lower key
      across both pools, using sticky only as an equal-key tie-break.
    - Keep worker-version routing, reserved pollers, sticky expiry, and poller
      history behavior unchanged.
    - _Requirements: 4.5, 4.6, 4.7, 4.8, 5.8_
  - [x] 6.4 Apply User Fairness only within normal priority bands
    - Use fair pass as the second ordering dimension when fairness is enabled.
    - Keep sticky queues fairness-disabled and keep inter-queue Drain Fairness
      mechanically separate.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9_
  - [x] 6.5 Property test: Property 5 — sticky/normal priority selection
    - Generate eligible sticky and normal candidates; prove lower-key selection and
      sticky equal-key tie-breaking over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 5`
    - _Requirements: 4.5, 4.6, 4.7, 4.8, 5.8_

- [x] 7. Preserve ordering through publication, backlog, recovery, and lineage
  - [x] 7.1 Publish kernel Priority into broker work
    - Thread Priority from every workflow/activity dispatch effect into the broker.
    - Assign a new order exactly once on initial publication and preserve the order
      on demotion/rehydration.
    - _Requirements: 2.6, 3.3, 4.1, 5.2, 6.1, 6.5_
  - [x] 7.2 Demote and drain with the same delivery order
    - Persist live-ready order on demotion, return ordered backlog batches, and
      republish without recomputing fair pass or insertion tie.
    - Spend the existing per-queue Drain Fairness budget in ordered sequence without
      making user fairness affect budget allocation.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 11.7_
  - [x] 7.3 Reconstruct raw/effective Priority after broker loss
    - Derive WFT Priority from committed workflow state and activity Priority from
      committed raw activity/workflow state.
    - Allow fair-pass frontier reset while preserving task identity, stamps,
      logical sequences, and authoritative start admission.
    - _Requirements: 6.6, 6.7, 11.5_
  - [x] 7.4 Preserve workflow successor Priority
    - Carry predecessor workflow Priority through continue-as-new, retry, and cron
      start construction.
    - Merge child raw overrides field by field before creating the child
      `StartRequest`; remove the current hard-coded `None`.
    - _Requirements: 2.3, 2.4, 2.5, 3.8, 3.9_
  - [x] 7.5 Preserve activity Priority through retry and replacement
    - Carry raw override through retry state, re-merge with current workflow
      Priority for each dispatch, and publish a replacement after a pre-start option
      update.
    - Ensure obsolete stamps are discarded at start without terminating the poll.
    - _Requirements: 3.2, 3.3, 7.7, 7.8, 7.9, 7.10_
  - [x] 7.6 Property test: Property 8 — broker/backlog order preservation
    - Generate ordered task sets; demote, encode, decode, drain, and republish them,
      then prove Priority/order preservation and poll-sequence equivalence over at
      least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 8`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.9_
  - [x] 7.7 Property test: Property 10 — recovery preserves correctness
    - Generate committed open states and lost/reset delivery-policy state; prove
      reconstructed task identity and Priority match live publication and that
      start-token admission is unchanged over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 10`
    - _Requirements: 6.6, 6.7, 11.5_

- [x] 8. Checkpoint: ordered delivery path green
  - Run formatting plus focused storage/kernel/runtime check, clippy, and tests with
    `--locked`, including broker expiry, reserved poller, sticky fallback, recovery,
    stale-task, activity retry, and backlog regressions.
  - Confirm lane/shard routing and existing Drain Fairness tests are unchanged.
  - _Requirements: 2.3–2.6, 3.2–3.9, 4.1–4.9, 5.1–5.9, 6.1–6.9, 7.3–7.10, 11.1–11.8_

- [x] 9. Make task-queue configuration atomic and kind-isolated
  - [x] 9.1 Correct the configuration key
    - Introduce explicit Workflow/Activity/Nexus config kind in the store key so
      identically named queues cannot overwrite each other.
    - Update get/list/describe call sites without changing the accepted volatile
      persistence boundary.
    - _Requirements: 8.14, 8.15, 9.6_
  - [x] 9.2 Move patch merging into the store
    - Replace handler-side read/merge/write with one validated atomic `apply`.
    - Enforce set/unset conflict, key length/non-empty, positive weights, and active
      override cap before mutation; rejected patches leave prior bytes unchanged.
    - _Requirements: 8.8, 8.9, 8.10, 8.11, 8.12, 8.13_
  - [x] 9.3 Add per-key config change notification
    - Emit a generation/notification only after a successful atomic update so
      blocked polls can re-evaluate rates without restart.
    - Do not make config part of workflow history or transition validity.
    - _Requirements: 8.4, 8.13, 8.15_
  - [x] 9.4 Property test: Property 13 — atomic task-queue config patch state machine
    - Generate valid/rejected patch sequences; compare to a reference map, prove
      task-kind isolation, and prove rejected updates preserve prior bytes over at
      least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 13`
    - _Requirements: 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 8.12, 8.13, 8.14, 8.15, 9.6_

- [x] 10. Enforce queue and fairness-key handout rates
  - [x] 10.1 Add the pure dispatch eligibility model
    - Compute queue-wide and per-key eligibility from monotonic time, using the later
      deadline, task effective weight, and zero-rate indefinite block semantics.
    - Separate eligibility inspection from token consumption for safe candidate
      comparison.
    - _Requirements: 8.1, 8.2, 8.3, 8.4_
  - [x] 10.2 Enforce activity handout limits
    - Consult current config at take time, not ingress; do not hold broker locks
      while waiting.
    - Wait on candidate readiness, queue wake, config-change wake, cancellation, or
      finite eligibility deadline.
    - _Requirements: 8.1, 8.2, 8.3, 8.4_
  - [x] 10.3 Enforce Nexus handout limits
    - Apply queue-wide rate and the empty-key/default-weight path to Nexus without
      adding Nexus Priority semantics not present in the target behavior.
    - _Requirements: 8.1, 8.2, 8.4_
  - [x] 10.4 Reject invalid workflow/rate configuration at the edge
    - Ground exact errors to v1.31.0 and reject workflow queue rates, negative rates,
      invalid keys/weights, conflicts, and override-cap violations.
    - Preserve all-or-nothing store mutation and public status mapping.
    - _Requirements: 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 8.12, 8.13_
  - [x] 10.5 Property test: Property 14 — queue and fairness-key rate model
    - Generate rates, weights, config changes, and monotonic times; compare handout
      eligibility and consumption to a reference token-time model over at least 100
      cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 14`
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 11. Validate and project Priority at the compatibility edge
  - [x] 11.1 Validate all inbound Priority surfaces
    - Ground exact v1.31.0 error text and reject negative keys, >64-byte fairness
      keys, and negative task weights on start, signal-with-start, activity, child,
      workflow update, and activity update paths.
    - Preserve zero values for inheritance/default semantics.
    - _Requirements: 1.1, 1.2, 1.3, 7.1, 7.6_
  - [x] 11.2 Translate activity and child Priority
    - Carry raw Priority from SDK commands into kernel commands and serialize it into
      activity-scheduled/child-initiated history.
    - Return effective Priority on activity polls and raw Priority in both pending
      activity Describe locations.
    - _Requirements: 3.1, 3.4, 3.5, 3.6, 3.7_
  - [x] 11.3 Translate workflow Priority options updates
    - Support whole and nested Priority field masks alongside the existing
      versioning override.
    - Serialize options-updated history and return current Priority on workflow poll
      and Describe responses.
    - _Requirements: 2.7, 2.8, 7.1, 7.2, 7.11_
  - [x] 11.4 Translate activity Priority option updates
    - Support Priority masks and restore-original in public request/response/history
      translation, preserving raw schedule-time values and nested per-activity merge
      intent.
    - _Requirements: 7.6, 7.7, 7.10, 7.12_
  - [x] 11.5 Thread sticky `normal_name` without changing versioning validation
    - Populate the runtime normal-queue alias only for sticky polls with a non-empty
      public `normal_name`.
    - Add edge tests for absent, normal, sticky, and versioned sticky cases.
    - _Requirements: 4.5, 4.6, 4.7, 4.8_

- [x] 12. Report real per-priority backlog statistics
  - [x] 12.1 Collect broker and storage band statistics
    - Count ready/backlogged work by effective priority key and preserve oldest
      schedule time per band.
    - Merge broker and storage images once, sum counts, and take the older age.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_
  - [x] 12.2 Project enhanced Describe results
    - Populate `stats_by_priority_key` from real band stats, place defaults under key
      3, omit absent bands, and derive aggregate count from the same band map.
    - Remove the placeholder that mirrors aggregate stats into key 3.
    - Keep current stored task-queue config echo intact for all task kinds.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_
  - [x] 12.3 Property test: Property 15 — per-priority stats conservation
    - Generate broker/backlog multisets; prove exact-once grouping, aggregate=sum,
      default key 3, correct clipping, and no fabricated bands over at least 100
      cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 15`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [x] 13. Checkpoint: runtime and edge public behavior green
  - Run formatting plus focused `tokeira-runtime`, `tokeira-storage`,
    `tokeira-kernel`, `tokeira-edge`, and conformance-control check, clippy, and
    tests with `--locked`.
  - Include wire-level integration tests for start/poll, activity inheritance,
    child/lineage inheritance, sticky-vs-normal ordering, priority updates, stale
    offer fencing, live config updates, rate limits, and Describe stats/config echo.
  - _Requirements: 1.1–1.9, 2.1–2.8, 3.1–3.9, 4.1–4.9, 5.1–5.9, 7.1–7.11, 8.1–8.15, 9.1–9.6, 10.1–10.11_

- [x] 14. Wire scoped conformance feature controls
  - [x] 14.1 Register the three mode overrides
    - Add `matching.useNewMatcher`, `matching.enableFairness`, and
      `matching.autoEnableV2` to the conformance registry only after their runtime
      consult sites exist.
    - Reset scoped mode and auto-enable state between test scopes without exposing a
      production config surface.
    - _Requirements: 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11_
  - [x] 14.2 Keep the weight-override cap live
    - Route the existing `matching.maxFairnessKeyWeightOverrides` override into the
      atomic config-store update and preserve default 1000 outside the scoped test.
    - _Requirements: 8.12, 8.13_

- [x] 15. Add the Temporal-fork observation adapter and classifications
  - [x] 15.1 Wrap only the Tokeira onebox Admin client
    - In `tests/testcore/tokeira_conformance_cluster.go`, intercept
      `DescribeTaskQueuePartition` and `GetTaskQueueTasks` only.
    - Answer the exact read-only observations needed by active leaves from public
      `DescribeTaskQueue`; delegate every unrelated method.
    - Do not edit `test_env.go`, corpus test bodies, or Tokeira product APIs to mimic
      Temporal's internal matching topology.
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_
  - [x] 15.2 Classify internal-topology, in-process-metrics, and pinned lifecycle leaves
    - Add full-name skip entries for the six classic/new/fair matcher migration
      leaves, the non-auto-enable pending-task invalidation metrics leaf, and the
      sticky-priority leaf whose pinned predecessor leaves the same workflow IDs
      running under default conflict policy.
    - Cite the v1.31.0 behavior and the precise architectural/harness limitation in
      each reason; keep all public ordering-tendency leaves active except the pinned
      workflow-ID collision, whose unmodified leaf is required to pass in isolation.
    - _Requirements: 12.5, 12.6, 12.7, 12.8, 12.10_
  - [x] 15.3 Property test: Property 18 — scoped observation mapping
    - Use Go standard `testing/quick` with generated public Describe responses to
      prove poller/timestamp preservation, honest count mapping, delegation, and
      stable classification over at least 100 cases.
    - Tag: `// Feature: task-queue-priority-fairness, Property 18`
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.10_
  - [x] 15.4 Verify the fork seam
    - Run the focused `tests/testcore` adapter/skip tests with
      `GOTOOLCHAIN=go1.26.2`, then compile the three target suites without executing
      them.
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.10_

- [x] 16. Add end-to-end tendency and regression coverage
  - [x] 16.1 Cover priority ordering
    - Add saturated slow-poller tests showing high-priority work tends to precede
      low-priority work, FIFO holds within equal bands, and FIFO returns when the
      matcher mode is disabled.
    - Cover direct delivery and speculative WFT exceptions without asserting a
      stricter sequence than v1.31.0 promises.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.9, 10.1, 10.6_
  - [x] 16.2 Cover sticky interaction
    - Reproduce the corpus ordering: high normal, default sticky, low normal.
    - Preserve sticky-first behavior only among equal-priority candidates.
    - _Requirements: 4.5, 4.6, 4.7, 4.8, 5.8_
  - [x] 16.3 Cover weighted User Fairness
    - Exercise saturated 1:1 and 2:1 fairness-key workloads within one priority band
      over a tolerance window; verify a sole runnable key remains work-conserving.
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
  - [x] 16.4 Cover durable backlog and restart
    - Exercise live-ready demotion, memory drain, DSQL query contract, and
      recovery-derived Priority; verify fairness reset cannot affect correctness.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9_
  - [x] 16.5 Cover priority updates and config enforcement
    - Exercise workflow/activity update masks, replacement fencing,
      restore-original, kind-isolated config, queue/per-key rate shaping, zero-rate
      wake on config change, and real priority stats.
    - _Requirements: 7.1–7.11, 8.1–8.15, 9.1–9.6_

- [x] 17. Update compatibility and conformance records
  - [x] 17.1 Record the supported behavioral surface
    - Update the compatibility matrix and supported/conformance documentation for
      stock priority defaults, opt-in fairness, inheritance, updates, rates, and
      enhanced statistics.
    - State explicitly that this implements public behavior without Temporal
      matching/history service objects, that User Fairness is disabled by default,
      and that config storage remains volatile pending the configuration decision.
    - _Requirements: 8.15, 10.1, 10.2, 10.3, 11.8, 12.8_
  - [x] 17.2 Record classified skips and campaign evidence
    - Update the functional-order and conformance ledgers with active leaves,
      full-name classifications, commands, fresh-process results, and relevant
      Tokeira/Temporal SHAs.
    - Do not record the tier green until two consecutive clean runs succeed.
    - _Requirements: 12.6, 12.7, 12.8, 12.9, 12.10_

- [x] 18. Functional conformance acceptance
  - Run `TestPrioritySuite`, `TestFairnessSuite`, and
    `TestFairnessAutoEnableSuite` using the current root-AGENTS harness invocation
    against a fresh conformance-feature `tokeirad`.
  - Require two consecutive fresh-process clean runs for every active leaf; confirm
    only the named internal-topology/in-process-metrics leaves and pinned
    workflow-ID lifecycle defect are classified, and run the sticky-priority leaf
    separately against a fresh process.
  - Investigate any unexpected result against v1.31.0 behavior before changing code.
  - _Requirements: 10.1–10.11, 12.1–12.10_

- [x] 19. Completion bar
  - Run the root `AGENTS.md §10.4` bar with `--locked`:
    `cargo +nightly fmt --all`, `cargo lint --locked`,
    `cargo check --workspace --locked`, `cargo test --workspace --locked`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Run `git diff --check`, offline Markdown link validation for changed docs/specs,
    and confirm no generated or unrelated files are dirty.
  - _Requirements: 1.1–1.9, 2.1–2.8, 3.1–3.9, 4.1–4.9, 5.1–5.9, 6.1–6.9, 7.1–7.11, 8.1–8.15, 9.1–9.6, 10.1–10.11, 11.1–11.8, 12.1–12.10_

## Task Dependency Graph

```json
{
  "waves": [
    {
      "id": 0,
      "tasks": ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "2"]
    },
    {
      "id": 1,
      "tasks": [
        "3.1", "3.2", "3.3", "3.4", "3.5", "3.6",
        "3.7", "3.8", "3.9", "3.10", "3.11", "4"
      ]
    },
    {
      "id": 2,
      "tasks": [
        "5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7", "5.8", "5.9"
      ]
    },
    {
      "id": 3,
      "tasks": ["6.1", "6.2", "6.3", "6.4", "6.5"]
    },
    {
      "id": 4,
      "tasks": ["7.1", "7.2", "7.3", "7.4", "7.5", "7.6", "7.7", "8"]
    },
    {
      "id": 5,
      "tasks": ["9.1", "9.2", "9.3", "9.4", "10.1", "10.4", "11.1"]
    },
    {
      "id": 6,
      "tasks": [
        "10.2", "10.3", "10.5",
        "11.2", "11.3", "11.4", "11.5",
        "12.1", "12.2", "12.3", "13"
      ]
    },
    {
      "id": 7,
      "tasks": ["14.1", "14.2", "15.1", "15.2", "15.3", "15.4"]
    },
    {
      "id": 8,
      "tasks": ["16.1", "16.2", "16.3", "16.4", "16.5"]
    },
    {
      "id": 9,
      "tasks": ["17.1", "18", "17.2", "19"]
    }
  ]
}
```

## Notes

- Ground truth is the local Temporal checkout at `v1.31.0`; cite repo-relative
  source path plus tag beside every non-obvious behavior decision. Do not copy
  Temporal implementation structure.
- Read each crate-local `AGENTS.md` before editing that crate. The kernel remains a
  stateless pure transition function; all ordering counters, clocks, config,
  brokers, and rate state remain runtime/storage delivery policy.
- No new dependency is planned. All Cargo commands use `--locked`; never clean or
  reconfigure kache.
- DSQL migration edits are permitted only because the baseline is not cut. Re-check
  the storage crate rule immediately before editing and stop if that status has
  changed.
- The `TaskQueueConfigStore` remains volatile by approved scope. Do not silently
  turn the configuration proposal into a durability decision.
- Functional suites assert best-effort ordering tendencies, not globally strict
  sequences. Preserve v1.31.0's direct-match, sticky, and speculative exceptions.
- The Temporal-fork adapter is a harness-only observation bridge. It must not grow a
  product-side Temporal matching/history architecture, edit `test_env.go`, or alter
  upstream test bodies.
- Requirements 12.9 and Task 17.2 are evidence gates: documentation becomes green
  only after two clean fresh-process runs, never by prediction.
