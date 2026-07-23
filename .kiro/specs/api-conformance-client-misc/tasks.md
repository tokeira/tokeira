# Implementation Plan

- [x] 1. Establish the clean internal data shapes
  - [x] 1.1 Add completion-limit and auto-reset-point domain types
    - Add `WorkflowTaskCompletionLimits` to
      `crates/tokeira-kernel/src/command.rs`, with the production v1.31.0
      default of four enabled `2000` limits.
    - Add `AutoResetPoint` and
      `DEFAULT_HISTORY_MAX_AUTO_RESET_POINTS` to
      `crates/tokeira-kernel/src/state.rs`, then append
      `WorkflowState.auto_reset_points`.
    - Document that `WorkflowState` is supplied to and returned from the
      stateless kernel; the kernel retains no process-local state.
    - _Requirements: 1.2, 1.4, 1.5, 5.1, 5.2, 5.4, 5.6_
  - [x] 1.2 Correct the sticky-affinity and WFT-start command shapes
    - Remove `StickyAffinity.expires_at`; retain only worker identity, sticky
      queue, and schedule-to-start timeout.
    - Replace the poll-side `StartWorkflowTaskRequest.sticky_ttl` hint with the
      actual `polled_task_queue`.
    - Update module/public-item documentation to distinguish durable affinity,
      a pending WFT deadline, and volatile broker liveness.
    - _Requirements: 4.1, 4.2, 4.3, 4.7, 4.9, 4.10_
  - [x] 1.3 Correct the derived workflow-task dispatch envelope
    - Replace `sticky_expires_at` with `sticky_deadline` on
      `DispatchableWorkflowTask`.
    - Add the fully resolved optional normal fallback `QueueKey`.
    - Update constructors mechanically without adding fallback decisions yet.
    - _Requirements: 4.2, 4.5, 4.6, 4.8, 4.9_
  - [x] 1.4 Update serialization and construction fixtures for the approved
        pre-baseline state-format correction
    - Update postcard round trips and test constructors for the clean state
      shapes.
    - Retain old history-event and failure-cause discriminants; do not solve a
      state-summary shape change by modifying persisted history variants.
    - _Requirements: 2.10, 4.1, 4.2, 5.6_

- [x] 2. Enforce pending-command limits atomically in the pure kernel
  - [x] 2.1 Append and render the four pending-limit failure causes
    - Append `PendingChildWorkflowsLimitExceeded`,
      `PendingActivitiesLimitExceeded`, `PendingSignalsLimitExceeded`, and
      `PendingRequestCancelLimitExceeded` after every existing
      `WorkflowTaskFailedCause` variant.
    - Add exact v1.31.0 `as_str()` renderings and kernel-side cause details.
    - _Requirements: 2.8, 2.9, 2.10_
  - [x] 2.2 Thread completion limits through every WFT-completion path
    - Add the limits to `WorkflowTaskCompletedRequest`.
    - Pass them through ordinary, cron, and retry completions into
      `apply_workflow_command`; never store them on `WorkflowState`.
    - _Requirements: 1.1, 1.5, 2.1, 2.2, 2.3, 2.4_
  - [x] 2.3 Add provisional-count admission checks
    - Check the relevant transition-builder map immediately before each
      bounded command is admitted.
    - Reject at `count >= limit`; bypass the check for `None`.
    - Return `Reject::InvalidCommandAttributes` so the existing runtime seam
      authors the WFT failure and discards the candidate completion.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_
  - [x] 2.4 Property test: Property 2 — pending-command boundary and atomicity
    - Implement a reference-model PBT over generated initial map sizes,
      enabled/disabled limits, and ordered mixed command batches.
    - Run at least 100 cases and prove that a rejected batch returns no
      transition, including when earlier commands were provisionally admitted.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 2: pending-command boundary and atomicity`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_
  - [x] 2.5 Add exact pending-limit failure examples and old-byte fixture
    - Assert all four exact cause/message combinations, including the count
      before the rejected insertion.
    - Deserialize a pre-change `WorkflowTaskFailedCause` fixture and prove all
      existing discriminants are unchanged.
    - _Requirements: 2.9, 2.10, 8.1, 8.2_

- [x] 3. Move authoritative ScheduleActivity normalization into the kernel
  - [x] 3.1 Implement the pure activity-command normalizer
    - Validate that schedule-to-close or start-to-close is positive.
    - Apply schedule-to-close inheritance/caps, workflow-run-timeout
      inheritance/caps, heartbeat capping, and normal-task-queue defaulting in
      `tokeira-kernel/src/kernel.rs`.
    - Use the normalized values identically in history, `ActivityState`,
      dispatch effects, and timeout tracking.
    - Return `BadScheduleActivityAttributes` with the exact v1.31.0 diagnostic
      when both controlling close timeouts are absent.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_
  - [x] 3.2 Property test: Property 3 — activity timeout normalization
    - Compare the kernel helper with a small reference model over generated
      timeout tuples, run timeouts, and task queues for at least 100 cases.
    - Assert all ordering/capping invariants and rejection of the missing
      controlling timeout pair.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 3: activity timeout normalization`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_
  - [x] 3.3 Add fixed activity-validation examples
    - Cover the exact missing-timeout error text, only-start-to-close with a
      run timeout, schedule-to-close inheritance, heartbeat capping, explicit
      longer-timeout capping, and empty task queue defaulting.
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.6, 8.3_

- [x] 4. Derive durable auto-reset points on hot apply and replay
  - [x] 4.1 Implement one shared reset-point evolution helper
    - Derive binary checksum and build ID with Worker Deployment build
      precedence.
    - Compute resettable from the pre-command pending child, external-signal,
      and external-cancel sets.
    - Preserve the first observation of a pair and retain the newest 20
      distinct pairs.
    - Call the helper after authoring a successful WFT-completion event and
      before applying commands; call it from replay at the same event boundary.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.8_
  - [x] 4.2 Property test: Property 7 — auto-reset-point reference model
    - Generate completion-version metadata, repeated/distinct pairs, event
      times/IDs, and pre-command pending sets.
    - Compare state evolution with a first-observation/tail-retention reference
      model for at least 100 cases.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 7: auto-reset-point reference model`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.8_
  - [x] 4.3 Property test: Property 8 — reset-point replay equivalence
    - Generate valid histories with WFT completions and intervening
      child/external lifecycle events.
    - Assert that hot application and `replay_from_history` yield identical
      ordered reset-point summaries for at least 100 cases.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 8: reset-point replay equivalence`
    - _Requirements: 5.6_
  - [x] 4.4 Add reset-point fixed examples
    - Cover empty version values, deployment precedence, duplicate pairs,
      resettable false for each pending-set class, a rejected completion
      retaining no point, and 21 distinct pairs retaining the newest 20.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 8.5_

- [x] 5. Correct sticky lifecycle transitions in the pure kernel
  - [x] 5.1 Persist only real sticky attributes on WFT completion
    - Store queue, completing-worker identity, and schedule-to-start timeout
      without an affinity expiry.
    - Treat absent or empty sticky attributes as clearing affinity.
    - Continue deriving each sticky WFT's concrete deadline at schedule time
      on `PendingWorkflowTask`.
    - _Requirements: 4.1, 4.2, 4.8_
  - [x] 5.2 Clear affinity only when a fallback task starts elsewhere
    - Compare `StartWorkflowTaskRequest.polled_task_queue` to the recorded
      sticky queue.
    - Preserve affinity on a sticky-queue start and clear it on a
      normal/other-queue start.
    - Ensure normal-queue delivery remains classified non-sticky so the edge
      returns history from event 1.
    - _Requirements: 4.6, 4.7, 4.8_
  - [x] 5.3 Property test: Property 4 — sticky lifecycle is queue-start driven
    - Generate sticky completions, task schedules/deadlines, elapsed times, and
      start queues for at least 100 cases.
    - Prove elapsed task deadlines never clear affinity and only a
      non-sticky-queue start does.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 4: sticky lifecycle is queue-start driven`
    - _Requirements: 4.1, 4.2, 4.6, 4.7, 4.8_

- [x] 6. Checkpoint: data models and pure-kernel behavior green
  - Run `cargo +nightly fmt --all`.
  - Run focused checks, clippy, and tests for `tokeira-types` and
    `tokeira-kernel`, including all new property tests.
  - Confirm no kernel dependency, I/O, async, storage, metrics, nondeterminism,
    or side-effecting command was introduced.
  - _Requirements: 1.5, 2.7, 3.7, 4.2, 5.6, 8.1, 8.2, 8.3, 8.5, 8.6_

- [x] 7. Make storage dispatch derivation non-mutating and deadline-correct
  - [x] 7.1 Remove read-time sticky-affinity mutation
    - Delete `clear_expired_sticky_if_needed` and every load/scan call site.
    - Prove `load_run` and dispatch scans never clear or rewrite authoritative
      run state.
    - _Requirements: 4.2, 4.6, 4.9_
  - [x] 7.2 Derive preferred and fallback queues from committed state
    - Update memory and DSQL workflow-task scans to build the same
      `DispatchableWorkflowTask`.
    - Source `sticky_deadline` exclusively from
      `PendingWorkflowTask.schedule_to_start_deadline`.
    - Include a normal fallback `QueueKey` only for a real sticky offer.
    - _Requirements: 4.5, 4.6, 4.8, 4.9_
  - [x] 7.3 Add storage examples and round trips
    - Assert load idempotence across times before/after a pending task deadline.
    - Assert memory/DSQL helper parity for normal, live sticky, and already-due
      pending tasks without explicit sleeps.
    - _Requirements: 4.2, 4.6, 4.9, 8.4, 8.6_

- [x] 8. Centralize sticky-poller availability and fallback in the broker
  - [x] 8.1 Add recent/active sticky-poller observations
    - Record poll admission and non-cancelled completion/timeout under
      namespace, queue name, and worker identity.
    - Treat a live parked waiter as active even after ten seconds.
    - Do not refresh on dropped client cancellation or broker-denied shutdown;
      remove the observation when installing a shutdown deny.
    - _Requirements: 4.3, 4.4_
  - [x] 8.2 Route sticky offers atomically at broker publication
    - Check liveness and enqueue under one broker lock.
    - Preserve an available sticky offer.
    - Rewrite an unavailable sticky offer to its supplied normal `QueueKey`
      and clear only delivery-envelope stickiness.
    - Apply the same path to live publication, backlog drain, and recovery.
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.8_
  - [x] 8.3 Stop treating the task deadline as affinity expiry
    - Remove broker promotion that silently turns a lapsed sticky deadline
      into general readiness.
    - Leave authoritative schedule-to-start timeout authoring to the existing
      WFT timeout transition and stale-offer fence.
    - _Requirements: 4.2, 4.6, 4.9_
  - [x] 8.4 Property test: Property 5 — sticky availability and immediate fallback
    - Generate observation ages around the ten-second boundary, live/closed
      waiter channels, shutdown-deny state, and sticky/normal destinations for
      at least 100 cases.
    - Assert routing follows the availability reference model and never
      changes committed workflow state.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 5: sticky availability and immediate fallback`
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.8_

- [x] 9. Wire runtime policy, publication, recovery, and query deadlines
  - [x] 9.1 Add live completion-limit accessors and honest key classification
    - Add the four integer keys to the conformance registry as `Wired`.
    - Implement production and conformance accessors with `2000` fallback and
      zero/negative disabling.
    - Resolve once per WFT completion and populate the explicit kernel input
      for ordinary, cron, and retry paths.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_
  - [x] 9.2 Property test: Property 1 — live completion-limit resolution
    - Generate optional signed override values and registry lifecycle changes
      for at least 100 cases.
    - Assert default, positive, disabled, and live-read behavior for all four
      keys.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 1: live completion-limit resolution`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_
  - [x] 9.3 Supply both version-aware destinations for live and recovered WFTs
    - Update `RuntimeDispatchPublisher` to resolve sticky and normal
      `QueueKey` values before broker publication.
    - Route normal and speculative sticky offers through the central broker
      decision.
    - Update recovery to republish the storage-derived envelope unchanged
      through that same decision.
    - _Requirements: 4.5, 4.6, 4.8, 4.9_
  - [x] 9.4 Derive fresh per-query sticky deadlines
    - Update runtime direct-query and edge buffered-query-release paths to use
      `enqueue_time + affinity.schedule_to_start_timeout`.
    - Preserve existing sticky-first then full-history fallback behavior.
    - _Requirements: 4.10_
  - [x] 9.5 Property test: Property 6 — derived dispatch and query deadlines
    - Generate committed WFT/affinity states and query enqueue times for at
      least 100 cases.
    - Compare memory/DSQL dispatch derivation and assert query deadlines are
      independent of WFT-completion age.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 6: derived dispatch and query deadlines`
    - _Requirements: 4.9, 4.10_

- [x] 10. Checkpoint: storage and runtime delivery behavior green
  - Run `cargo +nightly fmt --all`.
  - Run focused checks, clippy, and tests for `tokeira-storage` and
    `tokeira-runtime`, including broker, recovery, query, and property tests.
  - Run the existing speculative-WFT, sticky-query, worker-shutdown, and
    recovery regression tests.
  - _Requirements: 1.1, 4.3, 4.4, 4.5, 4.8, 4.9, 4.10, 8.4, 8.6_

- [x] 11. Complete edge command and Describe translation
  - [x] 11.1 Preserve ScheduleActivity wire values at the edge
    - Accept omitted/empty task queue and pass an empty domain queue for kernel
      defaulting.
    - Stop edge timeout normalization that hides the original command shape;
      retain only wire-type conversion and structural duration parsing.
    - Map kernel command rejection through the existing WFT-failure path rather
      than an early transport error.
    - _Requirements: 3.6, 3.7_
  - [x] 11.2 Map the four new failure causes to/from Temporal proto values
    - Add exhaustive conversion for values 26-29 without changing any existing
      numeric mapping.
    - _Requirements: 2.9, 2.10_
  - [x] 11.3 Expose auto-reset points through Describe
    - Add reset points to `WorkflowExecutionDescription`.
    - Populate them from committed `WorkflowState` in the `tokeirad` resolver.
    - Translate every `ResetPointInfo` field and preserve order.
    - _Requirements: 5.7_
  - [x] 11.4 Property test: Property 9 — reset-point Describe translation
    - Generate valid point lists, including optional expiries and both
      resettable values, for at least 100 cases.
    - Assert field-for-field and order-preserving proto translation.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 9: reset-point Describe translation`
    - _Requirements: 5.7_

- [x] 12. Resolve batch reset by build ID from auto-reset points
  - [x] 12.1 Replace the current-build/first-WFT substitution
    - Select the requested build ID from `WorkflowState.auto_reset_points`.
    - Validate resettable and optional expiry with the v1.31.0 error text.
    - Pass the exact `first_workflow_task_completed_id` to the unchanged reset
      transition.
    - Never consult `WorkflowState.build_id` as membership evidence.
    - Leave cross-run rollover and `current_run_only` under the existing
      field-support classification; this tier retains only current-run points.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_
  - [x] 12.2 Property test: Property 10 — batch build-ID resolution
    - Generate current-run point lists, requested build IDs, times, and
      unrelated current state build IDs for at least 100 cases.
    - Assert exact selection/error behavior and that invalid cases never invoke
      reset.
    - Tag:
      `// Feature: api-conformance-client-misc, Property 10: batch build-ID resolution`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_
  - [x] 12.3 Add batch-reset integration examples
    - Cover absent, unresettable, expired, and valid build points.
    - Prove a valid build target preserves the existing reset fork/reapply
      semantics and does not re-run pre-boundary work.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 8.5_

- [x] 13. Add cross-plane regression coverage
  - [x] 13.1 Exercise the invalid-command runtime seam
    - Prove each pending-limit rejection and the missing-activity-timeout
      rejection discard the candidate completion, author one
      `WorkflowTaskFailed`, and surface `INVALID_ARGUMENT`.
    - Prove progress resumes after one pending item resolves.
    - _Requirements: 2.7, 2.8, 2.9, 3.1, 3.2, 8.1, 8.2, 8.3_
  - [x] 13.2 Exercise sticky unavailability end to end
    - Use injected times/synchronization to prove immediate normal fallback,
      durable affinity survival before start, normal-start clearing, and full
      history from event 1.
    - Cover both normal and speculative WFT publication and recovery
      republishing.
    - _Requirements: 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 8.4, 8.6_
  - [x] 13.3 Exercise reset-point Describe and batch integration
    - Complete WFTs under multiple build IDs, assert first-observation points
      through Describe, and reset to the selected build boundary.
    - Assert existing BuildIds search-attribute and Worker Deployment metadata
      remain unchanged.
    - _Requirements: 5.7, 5.8, 6.1, 6.4, 8.5_
  - [x] 13.4 Audit the upstream data-converter skips
    - Add no Tokeira skip-registry entries.
    - Keep/report the three existing corpus `SkipNow` results as
      upstream-authored skips.
    - _Requirements: 7.1, 7.2, 7.3_
  - [x] 13.5 Preserve transient retry-activity history semantics
    - Keep retry-policy activity starts transient until terminal resolution.
    - Prove retryable attempts persist neither start nor failure, while a
      terminal result materializes the start immediately before the result.
    - Keep non-retry activity-start history unchanged.
    - _Requirements: 8.9_
  - [x] 13.6 Complete the scoped Shape-2 harness bridge
    - Forward only the four suite-global pending-command limit overrides
      through the existing override bridge.
    - Add a read-only `GetWorkflowExecution` adapter backed by
      `DescribeMutableState` for the suite's direct cancellation-state
      assertion; leave every unrelated persistence method unavailable.
    - Do not edit corpus test bodies.
    - _Requirements: 1.3, 1.6, 8.10_

- [x] 14. Checkpoint: focused edge and application tests green
  - Run `cargo +nightly fmt --all`.
  - Run focused checks, clippy, and tests for `tokeira-edge`, `tokeirad`,
    `tokeira-conformance`, and affected compatibility/translation modules.
  - Re-run existing activity-command, Describe, reset, BuildIds, Worker
    Deployment, sticky-query, and speculative-WFT regression tests.
  - _Requirements: 2.9, 3.7, 5.7, 5.8, 6.4, 7.2, 8.1, 8.2, 8.3, 8.4, 8.5, 8.9, 8.10_

- [x] 15. Checkpoint: Tier 9.44 functional conformance green twice
  - Build the conformance-enabled `tokeirad` required for the live override
    bridge.
  - Invoke `TestClientMiscTestSuite` and
    `TestClientDataConverterTestSuite` using the current root-AGENTS/runbook
    command.
  - Require two consecutive clean isolated runs: every testable ClientMisc leaf
    passes and exactly the three upstream data-converter methods skip.
  - Distil and retain the two run tallies for the readiness ledger.
  - _Requirements: 1.3, 1.6, 7.1, 7.2, 7.3, 8.8_

- [x] 16. Update support documentation and run the full completion bar
  - [x] 16.1 Correct supported-field and conformance records
    - Remove the `workflow_execution_info.auto_reset_points` unsupported row
      from `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`.
    - Record the verified Tier 9.44 result, behavioral sources, and three
      corpus-authored skips in `docs/readiness/conformance.md`.
    - _Requirements: 7.3, 8.7, 8.8_
  - [x] 16.2 Run the repository completion bar
    - Run `cargo +nightly fmt --all`.
    - Run `cargo lint --locked`.
    - Run `cargo check --workspace --locked`.
    - Run `cargo test --workspace --locked`.
    - Run
      `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
    - Confirm all commands pass, the build leaves no tracked diff, no
      dependency changed, and all spec task checkboxes accurately reflect
      completed work.
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8_

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1"],
  "4": ["1"],
  "5": ["1"],
  "6": ["2", "3", "4", "5"],
  "7": ["1", "5", "6"],
  "8": ["1", "5", "6"],
  "9": ["2", "5", "7", "8"],
  "10": ["7", "8", "9"],
  "11": ["2", "3", "4", "10"],
  "12": ["4", "11"],
  "13": ["9", "11", "12"],
  "14": ["13"],
  "15": ["14"],
  "16": ["15"]
}
```

## Notes

- The user approved the pure-kernel completion-limit input, replay-derived
  reset-point summary, and pre-baseline removal of durable affinity expiry.
- Temporal v1.31.0 is the behavior authority; code comments for non-obvious
  decisions cite the source paths listed in `requirements.md` and `design.md`.
  The implementation remains original and follows Tokeira's architecture.
- Broker liveness and delivery queues are disposable. A restart with no
  observations conservatively falls back to the normal queue; it never changes
  workflow authority.
- The four completion limits are the only new wired override keys. No generic
  production dynamic-config surface is introduced.
- No explicit sleeps are added. Boundary and cancellation tests inject time or
  synchronize with channels/notifications.
- Check off each task immediately after its code, documentation, and specified
  tests are complete; do not mark a parent complete while any child remains.
