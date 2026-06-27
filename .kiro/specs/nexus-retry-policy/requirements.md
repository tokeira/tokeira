# Nexus Outbound Operation Retry — Requirements

## Status

**RAISED, not implemented.** This spec was promoted from `.placeholder.md` during the
functional-conformance fix-to-green loop when ground-truthing revealed that two corpus
leaves (`TestNexusSyncOperationErrorRehydration`, and the `StartOperation` retry-classification
behaviour generally) require a **kernel-level Nexus-operation invocation-retry state
machine** that tokeira does not have. Per the conformance ground rules ("no kernel additions
for a corpus fix — stop and raise; spec it, don't inline-patch"), the work is captured here
for a deliberate greenlight rather than patched into the kernel under a corpus task.

## Ground truth (Temporal v1.31.0)

All citations are against the pinned `v1.31.0` tag (`git -C ../temporal show v1.31.0:<path>`).

A Nexus outbound `StartOperation` that returns an error is classified in
`components/nexusoperations/executors.go:485-533` (`handleStartOperationError`):

- `*nexus.HandlerError` **and** `!handlerErr.Retryable()` → `handleNonRetryableStartOperationError`
  → emits `NEXUS_OPERATION_FAILED` (terminal; the caller workflow observes the failure).
  (`executors.go:499-502`)
- `*nexus.OperationError` (operation-failed, not a handler error) → `handleOperationError`
  (terminal). (`executors.go:497-498`)
- otherwise — a **retryable** handler error, a transport error, or a context deadline —
  → `callErrToFailure(callErr, true)` then `TransitionAttemptFailed.Apply(...)`:
  the operation transitions `SCHEDULED → BACKING_OFF`, records `LastAttemptFailure` and
  `NextAttemptScheduleTime`, increments `Attempt`, and is **re-dispatched** after the
  backoff. It stays pending. (`executors.go:521-532`, `statemachine.go:268-289`)

`HandlerError.Retryable()` (vendored `github.com/nexus-rpc/sdk-go@v0.6.0/nexus/errors.go:255-279`,
pinned at `v1.31.0:go.mod:40`) honours an explicit `RetryBehavior` override, else defaults
by type: **non-retryable** = `BAD_REQUEST, UNAUTHENTICATED, UNAUTHORIZED, NOT_FOUND,
NOT_IMPLEMENTED, CONFLICT`; **retryable** = `RESOURCE_EXHAUSTED, INTERNAL, UNAVAILABLE,
UPSTREAM_TIMEOUT, REQUEST_TIMEOUT`; default retryable.

`statemachine.go:91-94` (`recordAttempt`) increments `Attempt` and clears
`LastAttemptFailure` when the next attempt begins. `DescribeWorkflowExecution` surfaces the
backing-off operation's failure on `PendingNexusOperationInfo.LastAttemptFailure`.

The corpus leaf `tests/nexus_workflow_test.go` `TestNexusSyncOperationErrorRehydration` makes
this observable: the `fail-handler-internal` / `fail-handler-app-error` cases (retryable →
`INTERNAL`) assert via `checkPendingError` that the operation is **still pending** and that
`desc.PendingNexusOperations[0].LastAttemptFailure != nil` (read through `Describe`), then
terminate the workflow; the `fail-handler-bad-request` case (non-retryable) asserts via
`checkWorkflowError` that the **workflow fails** with `NexusOperationError → HandlerError{BAD_REQUEST}`.
Both groups assert exactly one `nexus_outbound_requests` record in the capture window.

## tokeira today (the gap)

- Kernel `PendingNexusOperation` (`crates/tokeira-kernel/src/state.rs:590-619`) has **no**
  `attempt`, `last_attempt_failure`, or `next_attempt_at`, and no backing-off state.
- Kernel `NexusResolution::Failed` (`crates/tokeira-kernel/src/kernel.rs:1932-1945`)
  **unconditionally** emits `NexusOperationFailed` and removes the pending op. There is no
  retryable-failure arm that keeps the op pending while stashing the failure. Same for
  `Canceled`/`TimedOut`.
- Outbound `StartOperation` is single-attempt by construction
  (`crates/tokeira-runtime/src/nexus.rs:30` "**Single attempt.** Retry/backoff is
  `nexus-retry-policy`, not here."; `publisher.rs:746-747` "single attempt, no retry — Req 5.3").
- The retryability signal is dropped before any decision: the worker path discards
  `NexusHandlerFailureInfo.retry_behavior` (`crates/tokeira-edge/src/translate/nexus.rs`
  `proto_handler_error_to_resolution` / `wrap_handler_failure_as_resolution`); the external
  path keeps only the `error_type` string (`nexus_http.rs:252-267`) and never calls
  `mapped_handler_error_retryable` on the `StartOperation` response.

**Net:** tokeira treats *every* `StartOperation` failure as terminal. A non-retryable
`BAD_REQUEST` is (coincidentally) handled correctly; a **retryable** `INTERNAL` is wrongly
made terminal instead of staying pending with a recorded `LastAttemptFailure`.

## Precedent in the codebase

The inbound completion **`CompletionCallback`** already implements this exact shape
(`state.rs:752-770`: `state: CallbackState{BackingOff,…}`, `attempt: u32`,
`last_attempt_failure: Option<Payload>`, `next_attempt_at: Option<OffsetDateTime>`;
`kernel.rs:2240-2252`: on `RetryableFailure` set `BackingOff` + bump `attempt` + record
`last_attempt_failure` + set `next_attempt_at`; on `NonRetryableFailure` set `Failed`). The
runtime computes the backoff (`next_attempt_at`) and the kernel stays free of backoff math
and config. The outbound Nexus operation must mirror this division of labour.

## Requirements (EARS)

1. **R1 — Retryability classification.** When an outbound `StartOperation` attempt fails,
   the runtime SHALL classify the failure as retryable or terminal using v1.31.0's rules:
   the worker path SHALL honour `NexusHandlerFailureInfo.retry_behavior` then the per-type
   default table; the external path SHALL use the HTTP-status→type→retryable mapping
   (`mapped_handler_error_retryable`) and the `nexus-request-retryable` header override; an
   `*nexus.OperationError`-equivalent operation failure and a transport error SHALL follow
   v1.31.0 (`OperationError` terminal; transport/deadline retryable).

2. **R2 — Backing-off (pending) on retryable failure.** WHEN a `StartOperation` attempt
   fails retryably AND the schedule-to-close budget is not exhausted, the kernel SHALL keep
   the operation in `pending_nexus_operations`, record `last_attempt_failure`, increment
   `attempt`, set a `next_attempt_at` computed by the runtime, and NOT emit
   `NexusOperationFailed`.

3. **R3 — Terminal on non-retryable failure.** WHEN a `StartOperation` attempt fails
   non-retryably (or the schedule-to-close budget is exhausted), the kernel SHALL emit
   `NexusOperationFailed` and remove the pending op (current behaviour), preserving the
   `NexusOperationError → HandlerError/ApplicationError` chain.

4. **R4 — Retry dispatch.** A runtime Nexus-operation retry scanner SHALL re-dispatch a
   backing-off operation once `now >= next_attempt_at`, mirroring the completion-callback
   scanner, and SHALL clear `last_attempt_failure` as the new attempt begins (v1.31.0
   `recordAttempt`).

5. **R5 — Describe surface.** `DescribeWorkflowExecution` SHALL populate
   `PendingNexusOperations[].LastAttemptFailure` from the kernel field
   (`pending_nexus_operation_to_proto`, currently `..Default::default()`).

6. **R6 — Kernel purity.** The kernel SHALL NOT perform backoff math or read Nexus config;
   the runtime SHALL compute `next_attempt_at` and pass it in, exactly as the
   `CompletionCallback` path does.

## Out of scope (this spec)

- Cancelation retries (`executors.go:801` cancelation path) — a follow-up; same machinery.
- `TestNexusAsyncOperationErrorRehydration` — does **not** depend on this feature (its
  `StartOperation` succeeds → "pending"); it is an async-completion error-rehydration
  concern tracked separately.
