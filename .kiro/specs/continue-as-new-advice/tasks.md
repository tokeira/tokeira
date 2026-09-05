# Implementation Plan

- [x] 0. Approval checkpoint: state-format break
  - The integration seat approves the postcard-layout change to `WorkflowState`,
    `PendingWorkflowTask`, and `HistoryEventKind::WorkflowTaskStarted`, the envelope
    introduction, and the V068 guard before any task below starts.
  - DONE 2026-09-05: approved by the integration seat, together with including the
    `TOO_MANY_UPDATES` reason and reporting the persisted-encoding size from Describe.
  - _Requirements: 10.7_

- [ ] 1. Storage: envelopes, statistic, and stats read
  - [ ] 1.1 Blob envelopes and `BlobFormatError`
    - Add the versioned `WorkflowStateEnvelope` and `HistoryBatchEnvelope` with magic
      versions; decode rejects any other version with `BlobFormatError` naming kind, run,
      and observed version. Move the codec's shared parts out of the `dsql` feature so the
      in-memory store can use them; bump `SNAPSHOT_FORMAT_VERSION` to 4.
    - _Requirements: 10.1, 10.2, 10.3, 10.5_
  - [ ] 1.2 `history_batch_encoded_len` and `RunHistoryStats`
    - One size function for both stores; the `load_run_with_stats` trait method with
      `load_run` delegating; forwarders in `HistoryNotifyingRepository` and the `Arc`
      impls.
    - _Requirements: 1.4, 1.8_
  - [ ] 1.3 Migration V068 and the startup guard
    - `V068__workflow_hot_history_size.sql`; the runner's version-boundary check that
      fails when `workflow_hot` holds rows, with the recreate message; schema-baseline
      and digest updates per the storage crate rules.
    - _Requirements: 1.2, 10.4_
  - [ ] 1.4 DSQL accounting in commit, load, reset, and delete
    - `FOR UPDATE` read includes the column (NULL → 0); upsert writes prior plus batch
      size, saturating; load returns the stats; reset materialization sets the prefix
      size; visibility record built with the statistic.
    - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 1.7, 1.9, 1.10_
  - [ ] 1.5 In-memory accounting and snapshot inclusion
    - `history_size` map maintained on every commit path, copied for reset successors,
      removed with the run, persisted in the snapshot document.
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.7, 1.9, 1.10_
  - [ ] 1.6 Property test: Property 1 — History Size is a reference-model accumulator
    - `proptest` over generated commit sequences in `memory.rs`; the DSQL leg under
      `dsql-integration`.
    - Tag: `// Feature: continue-as-new-advice, Property 1: History Size is a reference-model accumulator`
    - _Requirements: 1.1–1.7, 1.9, 1.10_
  - [ ] 1.7 Property test: Property 9 — envelopes round-trip and reject pre-envelope blobs
    - Round trip generated states and batches; assert pre-envelope bytes (the 0.1.2
      layout, produced by the test with the old struct shapes) decode to
      `BlobFormatError`; snapshot version 3 fails startup.
    - Tag: `// Feature: continue-as-new-advice, Property 9: envelopes round-trip and reject pre-envelope blobs`
    - _Requirements: 10.1, 10.2, 10.3, 10.5_

- [ ] 2. Checkpoint: storage compiles, clippy clean, storage tests green

- [ ] 3. Kernel: types, rule, and every started-event site
  - [ ] 3.1 Types
    - `SuggestContinueAsNewReason`; the reasons field on the event; the three recorded
      fields on `PendingWorkflowTask`; `completed_update_count` on `WorkflowState`;
      `ContinueAsNewAdvicePolicy` and the request changes on `StartWorkflowTaskRequest`,
      `StartRequest`, `SignalWithStartRequest`, and `FailWorkflowTaskRequest`.
    - _Requirements: 2.5, 2.6, 2.11, 2.12_
  - [ ] 3.2 `continue_as_new_advice` rule
    - Pure function with ≥ comparisons, enum-ordered reasons, and the disabled update
      threshold; documented with the v1.31.0 anchors.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_
  - [ ] 3.3 Derive at the polled-start branches, the sync-match start, and the reset
    synthesis; copy at completion, failure, timeout, and forced-close materialization;
    clear at both schedule sites
    - _Requirements: 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.13_
  - [ ] 3.4 Count completed updates and copy Advice during rebuild
    - Increment in the completed-update arm; `replay_history_prefix` copies the event's
      Advice into the pending record without reading thresholds.
    - _Requirements: 2.11, 6.1, 6.2_
  - [ ] 3.5 Property test: Property 2 — the advice rule is deterministic
    - Tag: `// Feature: continue-as-new-advice, Property 2: the advice rule is a deterministic function of its operands`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.12_
  - [ ] 3.6 Property test: Property 4 — each attempt recomputes from its own start
    - Generated attempt sequences with signals and threshold changes; assert monotone
      recorded size, per-attempt rule, and cleared Advice at schedule.
    - Tag: `// Feature: continue-as-new-advice, Property 4: each attempt recomputes from the values at its own start`
    - _Requirements: 2.7, 2.8_
  - [ ] 3.7 Property test: Property 5 — successors account for themselves (kernel leg)
    - Continue-as-new, retry, cron, and reset successors; sync-match first task records
      `0`; scheduled-but-unstarted reset derives by the rule.
    - Tag: `// Feature: continue-as-new-advice, Property 5: successors account for themselves`
    - _Requirements: 2.9, 2.10_
  - [ ] 3.8 Property test: Property 10 — the Advice is advisory
    - Apply generated command sequences under two policies; diff states, events, and
      effects.
    - Tag: `// Feature: continue-as-new-advice, Property 10: the Advice is advisory`
    - _Requirements: 8.1, 8.2_
  - [ ] 3.9 Property test: Property 11 — update counting matches the registry model
    - Tag: `// Feature: continue-as-new-advice, Property 11: update counting matches the registry model`
    - _Requirements: 2.3, 2.11_
  - [ ] 3.10 Property test: Property 3 (rebuild leg) — rebuilt pending record equals the
    recorded event
    - Tag: `// Feature: continue-as-new-advice, Property 3: recorded Advice is identical on every delivery path`
    - _Requirements: 6.1, 6.2, 6.3_

- [ ] 4. Checkpoint: kernel compiles, clippy clean, kernel property tests green

- [ ] 5. Runtime: policy, operands, started-task Advice, metric
  - [ ] 5.1 Pinned constants and `continue_as_new_advice_policy` accessors
    - Off-feature constants; on-feature reads of the four keys with per-key fallback;
      update threshold derivation.
    - _Requirements: 3.1, 3.2, 3.4, 3.6_
  - [ ] 5.2 Stats read and operands on every command that may emit a started event
    - `resolve_polled_workflow_task_target` uses `load_run_with_stats`; polled start,
      `Start`, `SignalWithStart`, and the reset-failing command carry the operands.
    - _Requirements: 1.8, 2.9, 2.10, 2.12_
  - [ ] 5.3 `StartedWorkflowTask.advice` and the suggest metric
    - Copy from `new_state.pending_workflow_task`; record
      `tokeira_workflow_suggest_continue_as_new_total{reason}` after a start commit.
    - _Requirements: 4.3, 8.3_
  - [ ] 5.4 Property test: Property 6 — policy accessors equal the pinned constants
    off-feature
    - Compile-configuration test plus an on-feature test with installed overrides.
    - Tag: `// Feature: continue-as-new-advice, Property 6: policy accessors equal the pinned constants off-feature`
    - _Requirements: 3.1, 3.2, 3.4, 3.6_

- [ ] 6. Edge and engine: wire, synthesis, Describe
  - [ ] 6.1 Serializer emits reasons; both virtual-task syntheses fill the Advice
    - `history_serializer.rs` field 8 mapping; `workflow_service.rs` suffix synthesis
      from the pending record; `from_internal.rs` poll synthesis from `started.advice`.
    - _Requirements: 4.1, 4.2, 5.1, 5.2_
  - [ ] 6.2 Describe from the stats read
    - `StoreExecutionResolver` uses `load_run_with_stats`, external-payload statistics
      from state, no history read; description doc comment updated.
    - _Requirements: 7.1, 7.2, 7.3_
  - [ ] 6.3 Property test: Property 7 — wire round trip preserves the Advice
    - Tag: `// Feature: continue-as-new-advice, Property 7: wire round trip preserves the Advice`
    - _Requirements: 5.1, 5.2_

- [ ] 7. Checkpoint: workspace compiles, clippy clean, edge and engine tests green

- [ ] 8. Conformance wiring and ledger
  - [ ] 8.1 Mark the four keys `Wired` in `KEY_CLASSIFICATION` (add `ValueType::Float`
    if absent) and re-disposition their rows in
    `docs/conformance/v1.31.0/temporal-configuration.md`
    - _Requirements: 3.3, 3.5_
  - [ ] 8.2 Remove the `TestTransientWorkflowTaskHistorySize` skip in the fork and run
    Tier 1.6 against a `--features conformance` `tokeirad`; record the outcome in
    `docs/readiness/conformance.md`
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [ ] 9. Integration evidence (`crates/tokeira-engine/tests/continue_as_new_advice.rs`)
  - [ ] 9.1 Property test: Property 3 (delivery paths) — identical Advice on every path
    - Generated runs with transient and speculative tasks; compare persisted event,
      history-read synthesis, poll synthesis, and late materialization; change thresholds
      mid-run through the override seam and assert recorded values persist; run the same
      sequence over `Engine::listen` and the in-process endpoint.
    - Tag: `// Feature: continue-as-new-advice, Property 3: recorded Advice is identical on every delivery path`
    - _Requirements: 2.5, 2.6, 2.13, 4.1, 4.2, 4.3, 4.4, 5.4_
  - [ ] 9.2 Property test: Property 8 — one statistic, three readers
    - After generated commits assert Describe, visibility `HistorySizeBytes`, and the next
      start operand agree; assert no history read on Describe.
    - Tag: `// Feature: continue-as-new-advice, Property 8: one statistic, three readers`
    - _Requirements: 1.8, 7.1, 7.2, 7.3, 7.4_
  - [ ] 9.3 Boundary examples through real commits
    - 4095/4096/4097 events; one byte below/at/above 4 MiB; the in-process reproduction of
      the v1.31.0 transient sequence.
    - _Requirements: 2.1, 2.2, 2.8_
  - [ ] 9.4 SDK worker evidence from the `spikes/` SDK crate
    - The pinned Rust SDK worker observes `continue_as_new_suggested()` flip, finishes
      handlers, continues as new; workflow id and namespace stable, run id changes, state
      survives; both transports.
    - _Requirements: 5.3, 5.4_
  - [ ] 9.5 Live DSQL accounting under `dsql-integration`
    - Property 1's DSQL leg, the V068 guard on a non-empty table, and clean
      restart against the same history.
    - _Requirements: 1.1, 1.2, 1.3, 10.4_

- [ ] 10. Documentation and release notes
  - [ ] 10.1 Crate docs for storage (envelopes, statistic) and kernel (advice rule)
    - _Requirements: 1.8, 2.12, 10.3_
  - [ ] 10.2 Changie entries for the 0.1.3 train: `Added` (continue-as-new advice),
    `Changed` (Describe reports the persisted history size; state-format break and
    recreate requirement)
    - _Requirements: 10.6_

- [ ] 11. Checkpoint: the §10.4 bar is green

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0"] },
    { "id": 1, "tasks": ["1.1", "1.2"] },
    { "id": 2, "tasks": ["1.3", "1.4", "1.5"] },
    { "id": 3, "tasks": ["1.6", "1.7", "2"] },
    { "id": 4, "tasks": ["3.1"] },
    { "id": 5, "tasks": ["3.2", "3.4"] },
    { "id": 6, "tasks": ["3.3"] },
    { "id": 7, "tasks": ["3.5", "3.6", "3.7", "3.8", "3.9", "3.10", "4"] },
    { "id": 8, "tasks": ["5.1", "5.2", "5.3", "5.4"] },
    { "id": 9, "tasks": ["6.1", "6.2", "6.3", "7"] },
    { "id": 10, "tasks": ["8.1", "8.2"] },
    { "id": 11, "tasks": ["9.1", "9.2", "9.3", "9.4", "9.5"] },
    { "id": 12, "tasks": ["10.1", "10.2", "11"] }
  ]
}
```

## Notes

- Task 0 is the Destructive-class approval the root change classification requires for a
  state-compatibility break; nothing else starts before it.
- Task 9.1's transport leg depends on `Engine::listen` from
  [embedded-engine-listener](../embedded-engine-listener/tasks.md) task 2; the rest of
  this plan does not.
- Task 8.2 edits the sibling conformance fork branch, which is updated only in dedicated
  functional-conformance work; it is part of this feature's acceptance and runs on the
  operator's build host.
- The removed `suggest_continue_as_new: bool` request field has no external consumer;
  `StartWorkflowTaskRequest` is kernel-internal.
- Both features ship in the 0.1.3 train; Cloud consumes the published crates only.
