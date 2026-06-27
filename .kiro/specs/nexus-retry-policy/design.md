# Nexus Outbound Operation Retry — Design

## Why this is a kernel feature (and a stop-and-raise)

Surfacing `PendingNexusOperations[].LastAttemptFailure` and honouring retry classification
both reduce to one missing capability: an outbound Nexus operation must be able to live in a
**pending-with-recorded-failure** state and be re-dispatched on a timer. tokeira's only
durable representation of a pending op is the kernel `PendingNexusOperation`
(`state.rs:590-619`); the runtime holds nothing (the resolution is submitted fire-and-forget,
`publisher.rs:876-886`). So the state must live in the kernel. That is a kernel addition, and
the conformance ground rules require raising it rather than inlining it under a corpus task.

The design is fully precedented by the inbound `CompletionCallback` retry path, which already
carries `state/attempt/last_attempt_failure/next_attempt_at` and is driven by a runtime
scanner with the kernel free of backoff math. This spec mirrors that path onto the outbound
operation. Mirroring an established in-kernel pattern (rather than inventing one) keeps the
addition small and reviewable.

## Framing correction (do not re-implement the wrong fix)

An earlier framing held that "tokeira retries `BAD_REQUEST` ~5× and must suppress that retry."
That was a **metrics-bridge artifact** (an over-count, flagged "secondary" in the metrics-bridge
handover), **not** real attempts. Ground truth: tokeira is single-attempt and unconditionally
terminal (`kernel.rs:1932-1945`), so a non-retryable `BAD_REQUEST` is already terminal-correct
**by accident**. The real gap is the **inverse**: a *retryable* StartOperation error (e.g.
`INTERNAL`) must enter `BACKING_OFF` and **stay pending** with a recorded `LastAttemptFailure`.
Do not implement a "suppress BAD_REQUEST retry" non-fix — there is no retry to suppress.

## Component changes

### 1. Kernel state — `PendingNexusOperation` (`crates/tokeira-kernel/src/state.rs`)

Add, mirroring `CompletionCallback` (`state.rs:752-770`):

```rust
/// Invocation attempts already made (starts at 1 once dispatched). v1.31.0
/// `recordAttempt` (`components/nexusoperations/statemachine.go:91-94`).
#[serde(default)]
pub attempt: u32,
/// Failure of the most recent failed-but-retryable attempt, surfaced on
/// `DescribeWorkflowExecution.PendingNexusOperations[].LastAttemptFailure`.
/// `None` while no attempt has failed, and cleared when a new attempt begins.
#[serde(default)]
pub last_attempt_failure: Option<Payload>,
/// When a backing-off operation is next eligible for re-dispatch. Set by the
/// retryable-failure transition from a runtime-computed backoff; `None` unless
/// backing off. The runtime's Nexus-operation retry scanner re-fires only once
/// `now >= next_attempt_at`.
#[serde(default)]
pub next_attempt_at: Option<OffsetDateTime>,
```

A `started: bool` already distinguishes Scheduled vs Started. Backing-off is represented by
`next_attempt_at.is_some()` (no new enum needed), matching how the timeout scanner already
reads `started_at`. All fields `#[serde(default)]` so existing persisted state rehydrates.

### 2. Kernel transition — retryable attempt failure

Today `NexusResolution::Failed` (`kernel.rs:1932-1945`) is unconditionally terminal. Split the
failure path by introducing a retryable variant. Two viable shapes:

- **(preferred) New resolution variant** `NexusResolution::AttemptFailed { failure, next_attempt_at }`:
  records `last_attempt_failure = Some(failure)`, `attempt += 1`, `next_attempt_at = Some(..)`,
  leaves the op in `pending_nexus_operations`, emits **no** history event (v1.31.0 records no
  event for `EventAttemptFailed` — it is internal HSM state, `statemachine.go:268-289`), and
  does **not** schedule a workflow task. The existing `Failed` arm stays terminal for R3.
- (alternative) Carry a `retryable` flag + `next_attempt_at` on the existing `Failed` variant.
  Rejected: it overloads a terminal name with a non-terminal outcome and muddies replay.

A re-dispatch acceptance (the scanner firing) clears `last_attempt_failure` as the new attempt
begins (v1.31.0 `recordAttempt`), via the existing dispatch path or a tiny
`NexusOperationAttemptStarted` book-keeping transition.

### 3. Runtime classification + backoff (`crates/tokeira-runtime`)

Reuse the existing retryability logic — `mapped_handler_error_retryable`
(`nexus_http.rs:396-404`) + the `nexus-request-retryable` header (`HEADER_RETRYABLE`) — but
apply it to the **`StartOperation`** response (currently only the completion path consults it).
For the worker path, thread `NexusHandlerFailureInfo.retry_behavior` (currently discarded in
`translate/nexus.rs`) plus the per-type default table. The publisher then:

- terminal → submit `NexusResolution::Failed` (unchanged; preserves the error chain built by
  `external_handler_error_resolution` / `wrap_handler_failure_as_resolution`);
- retryable AND schedule-to-close budget remains → compute `next_attempt_at` from a backoff
  policy (constant-as-config, like the completion-callback retry config) and the current
  `attempt`, then submit `NexusResolution::AttemptFailed { failure, next_attempt_at }`;
- retryable BUT budget exhausted → terminal (v1.31.0 caps retries by schedule-to-close).

### 4. Runtime retry scanner

Add a Nexus-operation retry scanner mirroring the completion-callback scanner
(`publisher.rs` `deliver_completion_callback` + its scanner) and the existing
`NexusTimeoutTrackingState` scanner (`publisher.rs:1637`): index backing-off ops by
`next_attempt_at`, and once `now >= next_attempt_at` re-issue the StartOperation dispatch
(worker re-publish or external re-`start_operation`) with the incremented attempt. Reuses the
schedule-to-close timeout scanner for the terminal cap so the two scanners do not race (the
timeout scanner already owns schedule-to-close).

### 5. Edge Describe surface

`pending_nexus_operation_to_proto` (`crates/tokeira-edge/src/grpc/translate.rs:2966`, currently
`..Default::default()` for the failure) and the daemon resolver
(`apps/tokeirad/src/lib.rs:1700-1719`, which builds `PendingNexusOperationDescription`) set
`last_attempt_failure` from the new kernel field. This is the mechanical leaf once the state
exists — the same pattern the `CallbackInfo` surface already uses (`grpc/translate.rs:2763`).

## What this unblocks

- `TestNexusSyncOperationErrorRehydration` — all five sub-cases (retryable → pending +
  `LastAttemptFailure`; non-retryable/operation-failed → terminal chain).
- The general `StartOperation` retry-classification correctness for both worker and external
  endpoints.

## Invariants (must hold) — `NexusResolution::AttemptFailed` carries all four

These are binding; bake them into the kernel transition and the scanner, and assert them in
tests.

1. **Durable-but-no-event.** `AttemptFailed` is a tokeira per-run *transition* — it mutates
   durable run state (so `BACKING_OFF` survives a shard takeover and feeds Describe) but emits
   **no** Temporal history event and schedules **no** workflow task (matches v1.31.0
   `EventAttemptFailed`, `statemachine.go:268-289`, which is internal HSM state, not history).
   Document the state the way `started_at` is documented: *"authority the scanner reads, rebuilt
   from state"* — `next_attempt_at` is that authority for re-dispatch, as `started_at` is for
   start-to-close.
2. **Fenced re-dispatch.** The Nexus-op retry scanner re-firing a `BACKING_OFF` op must be
   stamp/OCC-fenced so a slow in-flight StartOperation plus a scanner re-fire cannot
   double-submit the call or double-bump `attempt`. Mirror the completion-callback scanner's
   fence (`publisher.rs` completion scanner + the `wft_stamp`/transition-seq fencing the lane
   submit already enforces); cite it in the implementation.
3. **Terminal-cap precedence.** Compute `next_attempt_at` against the **same** schedule-to-close
   the StC timeout scanner reads, and make an StC-terminal outcome **dominate** a pending
   re-dispatch (the StC scanner already owns schedule-to-close; a backing-off op past StC
   resolves terminally, it does not re-dispatch).
4. **Clear-on-redispatch.** Clear `last_attempt_failure` when an attempt is (re)dispatched
   (v1.31.0 `recordAttempt`, `statemachine.go:91-94`), so Describe shows the *current* attempt's
   failure and shows **nothing** while an attempt is in flight.

## Verification

Per the fix-to-green loop: build `tokeirad`, run
`TestNexusWorkflowTestSuite/TestNexusSyncOperationErrorRehydration` out-of-process with the
skip entry removed (`GOTOOLCHAIN=go1.26.2`), confirm all sub-cases green, then remove the skip
entry on `tokeira/conformance-v1.31.0`. Kernel unit tests for the new transition (retryable
keeps pending + records failure; non-retryable terminal; budget-exhausted terminal; replay
fidelity) mirroring the existing `CompletionCallback` transition tests.

## Risks / notes

- **Replay fidelity.** v1.31.0 records no history event for an attempt failure (it is HSM
  state, not history). tokeira reconstructs `PendingNexusOperation` from history on replay, so
  `attempt`/`last_attempt_failure`/`next_attempt_at` are derived runtime/scan state, not
  replayed from events — consistent with how `CompletionCallback` backoff state is handled.
  Confirm the reset/replay path (`kernel.rs:2625-2629`) treats a backing-off op correctly.
- **Scope.** Cancelation-attempt retries (`executors.go:801`) share this machinery and should
  follow as a small extension, not block the operation-retry landing.
