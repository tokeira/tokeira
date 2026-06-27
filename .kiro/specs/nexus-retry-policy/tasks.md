# Nexus Outbound Operation Retry — Tasks

Status: **RAISED — awaiting greenlight.** This is a deliberate kernel feature, not a corpus
patch. Do not start under a `temporal-functional-conformance` task; it needs its own approval.

- [ ] 1. **Kernel state.** Add `attempt: u32`, `last_attempt_failure: Option<Payload>`,
  `next_attempt_at: Option<OffsetDateTime>` to `PendingNexusOperation` (`state.rs:590-619`),
  all `#[serde(default)]`. (R2, R5)
- [ ] 2. **Kernel transition.** Add `NexusResolution::AttemptFailed { failure, next_attempt_at }`
  and its `apply_*` arm (records failure, bumps attempt, keeps op pending, emits no history
  event, schedules no WFT); keep `Failed` terminal. Clear `last_attempt_failure` when the next
  attempt begins. (R2, R3, R4) — mirrors `kernel.rs:2240-2252`.
- [ ] 3. **Kernel tests.** Retryable keeps pending + records failure; non-retryable terminal
  with intact error chain; budget-exhausted terminal; reset/replay (`kernel.rs:2625-2629`)
  fidelity for a backing-off op.
- [ ] 4. **Runtime classification.** Apply `mapped_handler_error_retryable` + `HEADER_RETRYABLE`
  to the `StartOperation` response (external); thread `NexusHandlerFailureInfo.retry_behavior`
  + default table (worker). Operation-failed terminal; transport/deadline retryable. (R1)
- [ ] 5. **Runtime backoff + submit.** Compute `next_attempt_at` (backoff constant-as-config +
  attempt); submit `AttemptFailed` when retryable and within schedule-to-close, else `Failed`.
  (R2, R3, R6)
- [ ] 6. **Runtime retry scanner.** Re-dispatch backing-off ops at `next_attempt_at` (mirror the
  completion-callback scanner); coordinate with the schedule-to-close timeout scanner for the
  terminal cap. (R4)
- [ ] 7. **Edge Describe.** Populate `PendingNexusOperations[].LastAttemptFailure` in
  `pending_nexus_operation_to_proto` (`grpc/translate.rs:2966`) and the daemon resolver
  (`apps/tokeirad/src/lib.rs:1700-1719`) from the new kernel field. (R5)
- [ ] 8. **Conformance.** Remove the `TestNexusSyncOperationErrorRehydration` skip entry on
  `tokeira/conformance-v1.31.0`; verify green out-of-process (`GOTOOLCHAIN=go1.26.2`).
- [ ] 9. **(follow-up)** Cancelation-attempt retries (`executors.go:801`) on the same machinery.
