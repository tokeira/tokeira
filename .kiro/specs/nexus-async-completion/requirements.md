# Requirements Document

> **Status (2026-06-22):** requirements drafted; **design.md and tasks.md not yet written.** This spec
> captures the C4b "async Nexus completion-callback delivery" gap identified while driving
> `TestNexusWorkflowTestSuite` — the started operation's result is never delivered back to the caller
> because the runtime's `DispatchCompletionCallback` is a no-op stub (`crates/tokeira-runtime/src/publisher.rs`).
> Tracked in `temporal-functional-conformance/reference/FINDINGS.md` (C4b). Approach approved (Option A,
> complete/conformant — not a shortcut); awaiting design.

## Introduction

When a tokeira workflow schedules a Nexus operation backed by an asynchronous handler (e.g. the Go
SDK's `temporalnexus.NewWorkflowRunOperation`), the handler's `StartOperation` returns an **operation
token** rather than an inline result, and the operation stays `STARTED`. The eventual outcome —
success, failure, or cancellation, produced when the handler-side work (here, the handler workflow)
reaches a terminal state — must be delivered back to the caller and recorded as the terminal
`NexusOperationCompleted` / `NexusOperationFailed` / `NexusOperationCanceled` event on the caller's
history. Only then does the caller's `NexusOperationFuture` resolve.

tokeira does not implement this delivery. `nexus_http.rs` documents the gap explicitly ("No outbound
caller links / callback URL ... tokeira hosts no inbound completion-callback endpoint yet"), the
outbound `StartOperation` carries no callback target, and the runtime's `DispatchCompletionCallback`
op is a no-op that only logs. Consequently a started async operation never completes: the caller
workflow blocks on `fut.Get()` until its run/operation timeout. This is the sole remaining blocker for
`TestNexusAsyncOperationWithMultipleCallers` (both `conflict-policy-fail` and
`conflict-policy-use-existing` subtests) and for every other async Nexus operation that completes via
callback.

This feature implements the complete, v1.31.0-conformant async completion path:

1. **Outbound attachment** — when invoking a handler's `StartOperation`, tokeira sends a callback URL
   and a signed completion token so the handler side attaches a completion callback to the backing
   workflow.
2. **Handler-close dispatch** — when a tokeira workflow that carries a Nexus completion callback
   reaches a terminal state, tokeira fires the callback, delivering the workflow's result or failure
   over the Nexus completion HTTP protocol, with retry/backoff.
3. **Inbound completion endpoint** — tokeira hosts the Nexus completion callback HTTP route, decodes
   the token, and routes the completion to the caller's pending operation.
4. **Originator resolution** — the completion is applied to the caller run, recording the terminal
   `NexusOperation*` event and resolving the pending operation.

The behaviour is ground-truthed to Temporal v1.31.0: the caller-side callback/token construction in
`components/nexusoperations/executors.go` (`buildCallbackURL`, `CallbackTokenGenerator.Tokenize`); the
handler-close callback firing in `components/callbacks/` (`nexus_invocation.go`,
`statemachine.go`, `request.go`); the inbound completion handler in
`components/nexusoperations/frontend/handler.go` (`completionHandler.CompleteOperation`); the
`temporal://system` URL and `/nexus/callback` route in `common/nexus/constants.go` + `routes.go`; the
`Temporal-Callback-Token` header and token codec in `common/nexus/callback_token.go`; and the
completion token shape `tokenspb.NexusOperationCompletion`
(`proto/internal/temporal/server/api/token/v1/message.proto`) — all @ v1.31.0.

## Glossary

- **Originator / caller run** — the workflow run that issued the `ScheduleNexusOperation` command and
  owns the pending operation; the target of the completion.
- **Handler workflow** — the workflow a `WorkflowRunOperation` starts to back the operation; its
  terminal state produces the operation outcome. tokeira owns this run for Worker-target endpoints.
- **Pending Nexus operation** — the caller-side record (keyed by `scheduled_event_id`) of an
  operation that has been scheduled and possibly started but not yet resolved.
- **Operation token** — the handler-issued identifier for a started async operation (carried on
  `NexusOperationStarted`), opaque to the caller.
- **Completion callback** — a callback (`CallbackSpec::Nexus { url, header }`) registered on the
  handler workflow at start, fired when that workflow closes, that delivers the outcome to the caller.
- **Completion token** — a tokeira-issued, signed token carried in the `Temporal-Callback-Token`
  header of a completion request, encoding the originator reference needed to locate the pending
  operation. Mirrors `tokenspb.NexusOperationCompletion`.
- **System callback URL** — the sentinel `temporal://system` used for Worker-target endpoints,
  meaning "deliver to this cluster's own Nexus completion endpoint" rather than an external HTTP host.
- **Completion HTTP protocol** — the Nexus wire format for delivering an operation outcome: a POST to
  the completion URL carrying the operation state (`succeeded`/`failed`/`canceled`), the result
  payload or failure, links, and the callback token header.
- **NexusResolution** — tokeira's kernel-level operation outcome (`Started`, `Completed`, `Failed`,
  `Canceled`, `TimedOut`) submitted to the originator run to record the terminal event.
- **Callback lifecycle state** — the durable state of a registered callback (`Standby`, `Scheduled`,
  `BackingOff`, `Failed`, `Succeeded`, `Blocked`), advanced as delivery is attempted.

## Target State

In scope (becomes `Implemented`):

- Outbound `StartOperation` requests for a scheduled async operation carry a callback URL
  (`temporal://system` for Worker-target endpoints; the configured external template otherwise) and a
  `Temporal-Callback-Token` encoding the originator's `(namespace, workflow_id, run_id,
  scheduled_event_id, request_id)`.
- A tokeira workflow that closes with one or more registered Nexus completion callbacks fires each
  callback once it reaches a terminal state, delivering the terminal outcome (success result, or
  failure for failed/terminated/timed-out/canceled handler workflows) via the completion HTTP
  protocol, with bounded retry/backoff and durable callback lifecycle state.
- tokeira hosts the Nexus completion callback HTTP route (`POST /nexus/callback`), decoding the token
  and the operation state, and routes a `temporal://system` callback to that same endpoint in-process
  (no real network hop) while remaining a real, externally reachable HTTP endpoint.
- The decoded completion is applied to the originator run as a `NexusResolution`, recording exactly
  one terminal `NexusOperationCompleted` / `NexusOperationFailed` / `NexusOperationCanceled` event for
  the addressed pending operation, after which the caller's operation future resolves.
- Delivery is idempotent: a completion for an already-resolved or absent pending operation is
  acknowledged (success/NotFound) per v1.31.0 and never double-records an event.

Out of scope (and why):

- **Synchronous completions.** A handler that returns an inline result (HTTP 200) is already handled
  by `NexusStartResult::SyncCompleted`; this feature is the async path only.
- **Locally-computed timeouts.** `schedule-to-close` / `start-to-close` expiry producing
  `NexusOperationTimedOut` is the existing `nexus_timeout_tracking` path; this feature delivers
  handler-originated outcomes, not local timer expiry.
- **Outbound cancellation request delivery.** Issuing `NexusOperationCancelRequested` to the handler
  is the existing `handle_cancel_nexus_operation` path; this feature delivers the resulting
  *completion* (a `canceled` outcome) back to the caller.
- **External (non-Worker) endpoint callback templating beyond URL construction.** tokeira will build
  the external callback URL from the configured template but real cross-cluster forwarding
  (`forwardCompleteOperation`) is not in scope; same-cluster Worker-target delivery is.

## Evidence From Current Code

- `crates/tokeira-runtime/src/nexus_http.rs` — module doc states the outbound callback URL and inbound
  completion endpoint are deferred; `StartOperation` is built without a callback.
- `crates/tokeira-runtime/src/publisher.rs` — `DispatchOp::DispatchCompletionCallback { callback_index,
  callback }` arm only logs `"completion callback scheduled for dispatch"`; no delivery.
- `crates/tokeira-kernel/src/kernel.rs` — `schedule_completion_callbacks` already advances
  `WorkflowClosed`-triggered callbacks `Standby → Scheduled` and emits `DispatchCompletionCallback` on
  close; `crates/tokeira-kernel/src/state.rs` defines `CompletionCallback` / `CallbackSpec::Nexus` /
  `CallbackState`.
- `crates/tokeira-runtime/src/publisher.rs` — `handle_schedule_nexus_operation` already submits a
  `NexusResolution` (`Started`/`Completed`/`Failed`) to the originator via the lane; the originator
  resolution mechanism exists and is reused for completion delivery.
- Target behaviour authority (all @ v1.31.0): `components/nexusoperations/executors.go`,
  `components/callbacks/{nexus_invocation,statemachine,request}.go`,
  `components/nexusoperations/frontend/handler.go`, `common/nexus/{constants,routes,callback_token}.go`,
  `proto/internal/temporal/server/api/token/v1/message.proto` (`NexusOperationCompletion`).

## Requirements

### Requirement 1: Outbound StartOperation attaches a completion callback

**User Story:** As a caller scheduling an async Nexus operation, I want the handler side to know where
to deliver the eventual outcome, so that my operation can complete.

#### Acceptance Criteria

1. WHEN tokeira dispatches a `StartOperation` for a scheduled Nexus operation THEN the request SHALL
   carry a callback URL and a `Temporal-Callback-Token` header.
2. WHERE the endpoint target is a Worker (in-cluster) target THEN the callback URL SHALL be the
   system sentinel `temporal://system`.
3. WHERE the endpoint target is an External target THEN the callback URL SHALL be built from the
   configured callback URL template for the namespace.
4. THE completion token SHALL encode the originator's `namespace_id`, `workflow_id`, `run_id`,
   `scheduled_event_id`, and the operation's `request_id`, sufficient to locate the pending operation
   on completion.
5. THE completion token SHALL be tamper-evident (signed/verifiable by tokeira) and SHALL be rejected
   on the inbound path if it fails verification.

### Requirement 2: A closed workflow fires its registered completion callbacks

**User Story:** As the caller, I want the handler workflow's terminal outcome delivered automatically
when it closes, so that I do not have to poll.

#### Acceptance Criteria

1. WHEN a workflow with one or more `WorkflowClosed`-triggered Nexus completion callbacks reaches a
   terminal state THEN the system SHALL attempt to deliver each callback exactly once per attempt,
   advancing its lifecycle state from `Scheduled`.
2. WHEN the workflow completed successfully THEN the delivered outcome SHALL be `succeeded` carrying
   the workflow's result payload.
3. WHEN the workflow failed, timed out, was terminated, or was canceled THEN the delivered outcome
   SHALL be `failed` (or `canceled` for a canceled workflow) carrying the corresponding failure.
4. WHEN a delivery attempt fails with a retryable error THEN the callback SHALL transition to
   `BackingOff` and be retried with bounded exponential backoff; WHEN it fails with a non-retryable
   error THEN it SHALL transition to `Failed`.
5. WHEN a delivery attempt succeeds THEN the callback SHALL transition to `Succeeded` and SHALL NOT be
   re-delivered.

### Requirement 3: tokeira hosts the inbound Nexus completion endpoint

**User Story:** As the handler-side callback dispatcher (tokeira itself, or a peer cluster), I want a
completion endpoint that accepts an outcome and routes it to the caller.

#### Acceptance Criteria

1. THE SYSTEM SHALL expose an HTTP route `POST /nexus/callback` that accepts a Nexus completion
   request (operation state, result or failure body, links, and the `Temporal-Callback-Token` header).
2. WHEN a `temporal://system` callback is dispatched in-cluster THEN the system SHALL route it to the
   same completion logic in-process, without requiring a real network round trip.
3. WHEN the callback token is missing or fails verification THEN the endpoint SHALL reject the request
   as a bad request and SHALL NOT resolve any operation.
4. WHEN the operation state is `succeeded` THEN the endpoint SHALL extract the result payload; WHEN it
   is `failed` or `canceled` THEN it SHALL extract the failure; any other state SHALL be rejected as a
   bad request.
5. THE endpoint SHALL surface delivery outcomes as Nexus handler results (success, or a handler error
   whose retryability matches v1.31.0) so the dispatcher's retry/backoff behaves correctly.

### Requirement 4: The completion resolves the originator's pending operation

**User Story:** As the caller workflow, I want the delivered outcome recorded as the terminal event on
my pending operation, so that my operation future resolves.

#### Acceptance Criteria

1. WHEN a verified completion addresses a pending operation that is `STARTED` THEN the system SHALL
   submit the corresponding `NexusResolution` to the originator run: `succeeded` →
   `Completed { result }`, `failed` → `Failed { failure }`, `canceled` → `Canceled`.
2. WHEN the resolution is applied THEN the originator run SHALL record exactly one terminal event —
   `NexusOperationCompleted`, `NexusOperationFailed`, or `NexusOperationCanceled` — for that
   `scheduled_event_id`, and SHALL schedule a workflow task so the caller observes it.
3. THE terminal event SHALL reference the originating `scheduled_event_id` (and `started_event_id`
   where v1.31.0 records it) so the SDK correlates it to the pending operation.

### Requirement 5: Completion delivery is idempotent and staleness-safe

**User Story:** As an operator, I want callback retries and duplicate deliveries to never corrupt a
caller's history.

#### Acceptance Criteria

1. WHEN a completion addresses a pending operation that has already been resolved (terminal event
   already recorded) THEN the system SHALL acknowledge the delivery as successful and SHALL NOT record
   a second terminal event.
2. WHEN a completion addresses an originator run or pending operation that does not exist THEN the
   endpoint SHALL return a Not Found handler error (matching v1.31.0), which the dispatcher MAY treat
   as terminal.
3. WHEN the same completion is delivered more than once (dispatcher retry after a lost acknowledgment)
   THEN the resulting originator state SHALL be identical to a single delivery.
4. WHEN a completion's token references a run that has been superseded (e.g. reset) THEN resolution
   SHALL follow v1.31.0's behaviour for a stale reference rather than recording against the wrong run.

### Requirement 6: The async completion path is observable

**User Story:** As an operator, I want to see registered callbacks and their delivery state.

#### Acceptance Criteria

1. WHEN `DescribeWorkflowExecution` is called on a workflow carrying completion callbacks THEN the
   response SHALL report each callback with its current lifecycle state and attempt/last-failure
   metadata, consistent with v1.31.0's `callbacks` field.
2. THE SYSTEM SHALL emit structured logs/metrics for completion delivery attempts (outcome, attempt,
   retryability) without inlining payload bodies.

### Requirement 7: End-to-end multi-caller fan-out completes

**User Story:** As a caller fanning out multiple async Nexus operations to one handler workflow, I want
every started operation to complete once the handler closes.

#### Acceptance Criteria

1. WHEN K operations are `STARTED` against one handler workflow and that workflow completes THEN all K
   pending operations SHALL receive the `succeeded` completion and resolve with the handler's result.
2. WHEN a fan-out mixes started and rejected operations (e.g. conflict-policy `Fail` losers) THEN only
   the started operations SHALL receive completions; the rejected ones remain resolved by their start
   failure.
3. THE caller workflow SHALL complete (its operation futures all resolve) without reaching its run or
   operation timeout.
