# Implementation Plan: Async Nexus Operation Completion Delivery

## Overview

Deliver the eventual outcome of an asynchronous Nexus operation back to the caller. Data/kernel first
(dispatch-op outcome, callback lifecycle command, `next_attempt_at`), then runtime types (completion
token, completion HTTP client) and the delivery path + retry scanner, then the edge (outbound callback
attachment, inbound `/nexus/callback` handler, wire translation), then the `tokeirad` HTTP server
wiring, then tests and the operator conformance re-run. Every behaviour is ground-truthed to `v1.31.0`
(see `design.md` / `requirements.md`); the token is versioned/opaque + version-checked (not signed),
and the inbound HTTP endpoint is in scope (cross-cluster routing of the POST is not). The HTTP server
is hand-rolled on `hyper` (no new dependency); the lockfile is bumped to `hyper 1.10.1` in Wave 5.

## Tasks

- [x] 0. Configuration + dependency (raise, never hardcode — Implementer mandate rule 3)
  - [x] 0.1 Add config knobs to `crates/tokeira-config/src/lib.rs`
    - `nexus_completion` section: HTTP listener address (bind + the address `temporal://system`
      resolves to for loopback firing) and callback retry policy (initial interval, max interval,
      backoff coefficient, max attempts) with v1.31.0 `components/callbacks` default values.
      `serde(deny_unknown_fields)`; all fields defaulted.
    - Landed: `NexusCompletionConfig` (http_addr `0.0.0.0:7253`, system_callback_url
      `http://127.0.0.1:7253`, retry 1s/1h/2.0, `retry_max_attempts = 0` = unbounded per v1.31.0
      `NoInterval`), wired into `PolicyConfig`, with validation + 2 unit tests; fmt + workspace lint clean.
    - _Requirements: 2.4, 3.1_

- [x] 1. Kernel — outcome, lifecycle command, durable field
  - [x] 1.1 Add `CallbackCompletionOutcome` to the dispatch op (`transition.rs`, `event.rs`/`state.rs` as needed)
    - `enum CallbackCompletionOutcome { Success { result: Option<Payload> }, Failure { failure: Payload }, Canceled { failure: Payload } }`.
    - Add `outcome: CallbackCompletionOutcome` to `DispatchOp::DispatchCompletionCallback`.
    - In `schedule_completion_callbacks` (`kernel.rs`), build the outcome from the closing event,
      mirroring `GetNexusCompletion @ v1.31.0`: completed→`Success` (first payload or nil);
      failed/timed-out/terminated→`Failure`; canceled→`Canceled` ("operation canceled").
    - Landed: `CallbackCompletionOutcome { Success { result: Option<Payload> } | Failed { failure } |
      Canceled { details: Option<Payloads> } | Terminated | TimedOut | ContinuedAsNew }` in
      `transition.rs`; `schedule_completion_callbacks` derives the variant from the run's terminal event
      via the free fn `callback_completion_outcome`. Terminated/timed-out/continued-as-new are forwarded
      as bare variants so the runtime owns failure *synthesis* (kernel stays free of proto). ContinuedAsNew
      is a documented deviation (v1.31.0 `GetNexusCompletion` errors; tokeira maps it to `failed`).
    - _Requirements: 2.2, 2.3, 4.1, 4.2, 4.3_
  - [x] 1.2 Add `next_attempt_at` to `CompletionCallback` (`state.rs`)
    - `next_attempt_at: Option<OffsetDateTime>` (`serde(default)`; build-phase fold, no ALTER).
    - _Requirements: 2.4_
  - [x] 1.3 Add `CompletionCallbackAttempted` command + apply logic (`command.rs`, `kernel.rs`)
    - `CompletionCallbackAttempted { callback_index: usize, outcome: CallbackAttemptOutcome }` where
      `CallbackAttemptOutcome { Succeeded | RetryableFailure { failure } | NonRetryableFailure { failure } }`.
    - Apply: `Succeeded`→`state=Succeeded`; `RetryableFailure`→`state=BackingOff`, `attempt+=1`,
      `last_attempt_failure`, `next_attempt_at=now+backoff(attempt)`; `NonRetryableFailure`→`state=Failed`.
      Fence: reject if `callback_index` out of range / callback already terminal.
    - Landed: `RetryableFailure` carries `next_attempt_at: OffsetDateTime` (the runtime computes backoff;
      kernel reads no config). `apply_completion_callback_attempted` **accepts a closed run** (callbacks
      fire post-close), emits no history event and no dispatch op, and bumps `transition_seq` as a
      state-only fenced commit. New rejects `UnknownCompletionCallback(idx)` /
      `CompletionCallbackAlreadyTerminal(idx)`.
    - _Requirements: 2.4, 2.5_
  - [x] 1.4 Fix downstream exhaustive matches / construction sites for the new variants + field
    - Landed: six `CompletionCallback {…}` literals gained `next_attempt_at: None`; `publisher.rs`
      `DispatchCompletionCallback` stub binds `outcome` (real delivery is Wave 4); `lane.rs`
      `command_type_name` handles `CompletionCallbackAttempted`. `cargo check --workspace` clean (the
      typed `Reject` is stringified across the lane boundary, so the two new rejects need no edge arm).
    - _Requirements: (compile integrity)_
  - [x] 1.5 Kernel golden tests
    - `DispatchCompletionCallback` carries the correct outcome per close kind (completed/failed/
      canceled/timed-out/terminated); `CompletionCallbackAttempted` transitions for each attempt outcome;
      out-of-range / terminal-callback rejection.
    - Landed (`golden_tests.rs`): six `dispatch_completion_callback_outcome_*` (incl. continued-as-new);
      `completion_callback_attempted_{succeeded,retryable_backs_off,non_retryable}`; rejection tests for
      out-of-range index, already-terminal, and absent run.
    - _Requirements: 2.2, 2.3, 2.4, 2.5, 4.1, 4.2, 4.3_
  - [x] 1.6 Property test P4 — callback lifecycle is well-formed and bounded
    - Landed (`property_tests.rs`): `property_p4_attempt_advances_lifecycle_well_formed` (each attempt
      outcome → one well-formed lifecycle state, attempt bump + future `next_attempt_at` on retry, no
      history/dispatch, seq bumped) and `property_p4_terminal_callback_never_reattempted`.
    - _Feature: nexus-async-completion, Property 4_
    - _Requirements: 2.1, 2.4, 2.5_
  - [x] 1.7 Property test P2 — closed workflow yields the matching outcome→resolution mapping
    - The kernel-side half: close kind → `CallbackCompletionOutcome` variant.
    - Landed (`property_tests.rs`): `property_p2_close_kind_yields_matching_outcome` over all six close
      kinds (completed/failed/canceled/continued-as-new/terminated/timed-out).
    - _Feature: nexus-async-completion, Property 2_
    - _Requirements: 2.2, 2.3, 4.1, 4.2, 4.3_

- [x] 2. Checkpoint — kernel
  - `cargo +nightly fmt`, `cargo lint`, `cargo test -p tokeira-kernel`.
  - Done (2026-06-23): fmt clean; `cargo lint -p tokeira-kernel -p tokeira-runtime` exit 0 (`-D warnings`);
    `cargo test -p tokeira-kernel` 272 pass (8 lib + 182 golden + 82 property); `cargo check --workspace` clean.

- [x] 3. Runtime — completion token + completion HTTP client
  - [x] 3.1 Add `NexusCompletionToken` to `crates/tokeira-runtime/src/nexus.rs`
    - `{ version, originator_run_key, operation_id, scheduled_event_id, request_id }`; `encode`/`decode`
      as a `{v,d}` base64 envelope (mirrors `callback_token.go @ v1.31.0`); `decode` rejects a version
      mismatch (`InvalidArgument`). Add `TEMPORAL_CALLBACK_TOKEN_HEADER` + `SYSTEM_CALLBACK_URL` consts.
    - Landed: `NexusCompletionToken { originator_run_key, operation_id, scheduled_event_id, request_id }`
      (the version lives on the outer `{v,d}` envelope only — matching `CallbackToken.Version`; the inner
      proto has no version field, so no redundant inner field). `encode`/`decode` mirror
      `Tokenize`/`DecodeCallbackToken`: outer `{v,d}` JSON, `d = URL_SAFE-base64(serde_json(inner))`;
      `decode` version-checks before base64-decoding. URL-safe **padded** base64 = Go `base64.URLEncoding`.
      Inner codec is `serde_json` not `proto.Marshal` (documented deviation — inner carries `RunKey`, not
      the proto identity tuple; single-cluster opaque; outer envelope matched for future wire-parity).
      Consts `COMPLETION_TOKEN_VERSION` / `TEMPORAL_CALLBACK_TOKEN_HEADER` / `SYSTEM_CALLBACK_URL` verbatim
      from `common/nexus/{callback_token,constants}.go @ v1.31.0`. `base64.workspace` added.
    - _Requirements: 1.4, 1.5_
  - [x] 3.2 Property test P5 — completion token round-trip + version rejection
    - Landed (`tests/runtime_nexus.rs`): `property_p5_completion_token_round_trip` (256 cases, arbitrary
      unicode op/request ids + i64 extremes; asserts `{v,d}` envelope shape + `decode(encode(t)) == t`),
      plus unit tests for wrong-version, malformed (non-JSON / non-base64), and valid-envelope-with-garbage-
      inner-payload rejections.
    - _Feature: nexus-async-completion, Property 5_
    - _Requirements: 1.4, 1.5_
  - [x] 3.3 Define `NexusCompletionClient` trait + `reqwest` impl + `NoopNexusCompletionClient`
    - `complete_operation(url, token, state, body, links)` → POST per the Nexus completion wire shape
      (`Nexus-Operation-State`, `Temporal-Callback-Token`, payload/failure body, `Nexus-Link`);
      content-type per the payload serializer. 2xx→ok; retryable status/transport→retryable err;
      non-retryable 4xx→terminal err.
    - Landed: trait `NexusCompletionClient` + `HttpNexusCompletionClient` (reqwest, in `nexus_http.rs`,
      reusing `payload_to_body`) + `NoopNexusCompletionClient` (→ `Delivered` for tests). `state`+`body`
      collapsed into one `NexusCompletion { Succeeded(Payloads) | Failed(Vec<u8>) | Canceled(Vec<u8>) }`
      so an inconsistent state/body pair is unrepresentable (mirrors `applyToHTTPRequest`'s single
      discriminator @ v1.31.0) — a deliberate refinement of the spec's separate-params sketch. Sets
      `Nexus-Operation-State` / `Temporal-Callback-Token` / `User-Agent: temporalio/server`; succeeded→
      payload body, failed/canceled→JSON failure (`application/json`). Retryability mirrors the firing path
      (`nexus_invocation.go` + `client.go @ v1.31.0`): mapped handler-error statuses classified
      (`408`/`429`/`5xx` retryable; `400`/`401`/`403`/`404`/`409`/`501` terminal), **unmapped statuses
      retryable** (`UnexpectedResponseError`), transport errors retryable; `nexus-request-retryable` header
      overrides only for mapped statuses. (`409`/`501` defaults follow the un-vendored nexus-rpc sdk-go
      v0.6.0 convention — flagged for the Wave 8 conformance pass.) 9 client tests.
    - **Deferred to Wave 4:** `Nexus-Link` header *emission* (the `links` param is in the signature but its
      encoder lands with its producer — the firing handler — in Wave 4/5; links are best-effort,
      non-essential to resolution per design §5). The production runtime-constructor default for the client
      (loud-vs-silent vs the `Delivered` Noop) is also a Wave 4 wiring decision.
    - Verified by a 3-reviewer adversarial workflow against v1.31.0 (caught + fixed the unmapped-4xx
      retryability divergence and the redundant inner `version` field).
    - _Requirements: 2.1, 5.5_

- [ ] 4. Runtime — outbound attachment, firing, retry scanner
  - [ ] 4.1 Outbound attachment in `publisher.rs` (`handle_schedule_nexus_operation`, Worker arm)
    - Generate a `NexusCompletionToken` from `(originator_run_key, operation_id, scheduled_event_id,
      request_id)`; attach `callback_url = SYSTEM_CALLBACK_URL` + `callback_token` to the published
      `NexusTask`. Add `callback_url`/`callback_token` to `NexusTaskRequest::StartOperation`.
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 4.2 Implement `DispatchCompletionCallback` handler (replace the no-op stub) in `publisher.rs`
    - Decode `header[Temporal-Callback-Token]`; map `outcome`→Nexus completion (state + body); resolve
      `SYSTEM_CALLBACK_URL` to the configured local listener address; POST via `NexusCompletionClient`.
      On 2xx → submit `CompletionCallbackAttempted(Succeeded)`; retryable → `RetryableFailure`;
      non-retryable → `NonRetryableFailure`. (Delivery reaches the originator through the inbound
      endpoint, task 5.2.)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_
  - [ ] 4.3 Completion-callback retry scanner (mirror `scan_nexus_timeouts_once`)
    - Volatile index of `(run_key, callback_index)` for `BackingOff` callbacks; per tick reload the run
      (history is authority), re-fire `DispatchCompletionCallback` for those past `next_attempt_at`,
      bounded by `max_per_scan`; rebuild on shard takeover. Wire scanner config + lifecycle into
      `TokeiraRuntime`.
    - _Requirements: 2.4, 2.5_
  - [ ] 4.4 Property test P1 — outbound StartOperation carries a decodable, version-checked token
    - _Feature: nexus-async-completion, Property 1_
    - _Requirements: 1.1, 1.2, 1.4, 1.5_
  - [ ] 4.5 Runtime tests — delivery + idempotency + cross-namespace
    - In-process delivery submits the matching `NexusOperationResolved`; re-delivery to an already-
      resolved op records no second event and leaves the callback `Succeeded`; a handler workflow in
      namespace B resolves an originator in namespace A. Synchronize on observable state, no sleeps.
    - _Feature: nexus-async-completion, Property 3, Property 6_
    - _Requirements: 4.1, 5.1, 5.3, 7.1_

- [ ] 5. Edge + HTTP server — wire translation, inbound endpoint, server
  - [ ] 5.1 Emit callback fields in `start_operation_to_proto` (`translate/nexus.rs`)
    - Populate `callback`/`callback_header` from the task's `callback_url`/`callback_token` (replacing
      the empty synthesis from `edge-nexus-task-transport`).
    - _Requirements: 1.1, 1.2_
  - [ ] 5.2 Inbound `/nexus/callback` handler (`tokeira-edge`)
    - Parse `Temporal-Callback-Token` (decode + version), `Nexus-Operation-State`, and body (result
      payload for `succeeded`; Nexus failure for `failed`/`canceled`, reusing the `RespondNexusTaskFailed`
      failure conversion); map → `NexusResolution`; call `resolve_nexus_operation`. Return 2xx; bad/
      missing token → bad-request handler error; absent/already-resolved op (kernel
      `Stale`/`Unknown`) → not-found handler result. Mirrors `completionHandler.CompleteOperation @ v1.31.0`.
    - _Requirements: 3.1, 3.3, 3.4, 3.5, 5.1, 5.2_
  - [ ] 5.3 HTTP server wiring in `apps/tokeirad` (hyper)
    - Stand up a `hyper` HTTP/1.1 listener serving `POST /nexus/callback` alongside the gRPC server in
      the serve path; bind from config; point the runtime completion client's `temporal://system`
      resolution at this address. Bump the lockfile: `cargo update -p hyper@1.9.0 --precise 1.10.1`.
    - _Requirements: 3.1, 3.2_
  - [ ] 5.4 Property test P9 — inbound endpoint resolves valid / bad-token / not-found
    - _Feature: nexus-async-completion, Property 9_
    - _Requirements: 3.1, 3.3, 3.4, 3.5, 5.2_
  - [ ] 5.5 Edge test P7 — Describe surfaces callback state/attempt/last_attempt_failure
    - _Feature: nexus-async-completion, Property 7_
    - _Requirements: 6.1_

- [ ] 6. Checkpoint — runtime + edge
  - `cargo +nightly fmt`, `cargo lint`, `cargo test-lint`, `cargo test -p tokeira-runtime -p tokeira-edge`,
    `cargo check --workspace`.

- [ ] 7. Integration — end-to-end async round-trip (`apps/tokeirad/tests/`)
  - [ ] 7.1 Async analogue of the verified sync round-trip
    - Schedule an async op; external poller replies `AsyncSuccess`; a second (handler) workflow closes;
      tokeira fires the callback (loopback POST to its own `/nexus/callback`); assert the caller observes
      `NexusOperationCompleted` with the handler's result. Cross-namespace (handler in agents-ns,
      caller in control-ns).
    - _Feature: nexus-async-completion, Property 8_
    - _Requirements: 7.1, 7.2, 7.3_

- [ ] 8. Final checkpoint + operator conformance
  - Full enforced gates green; `cargo doc --workspace --no-deps` (`RUSTDOCFLAGS="-D warnings"`).
  - Operator re-run of `^TestNexusWorkflowTestSuite` async-completion cases; revisit the deferred-skip
    async-completion-callback tests.

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 0, "tasks": ["0.1"] },
    { "wave": 1, "tasks": ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7"] },
    { "wave": 2, "tasks": ["2"] },
    { "wave": 3, "tasks": ["3.1", "3.2", "3.3"] },
    { "wave": 4, "tasks": ["4.1", "4.2", "4.3", "4.4", "4.5"] },
    { "wave": 5, "tasks": ["5.1", "5.2", "5.3", "5.4", "5.5"] },
    { "wave": 6, "tasks": ["6"] },
    { "wave": 7, "tasks": ["7.1"] },
    { "wave": 8, "tasks": ["8"] }
  ]
}
```

Kernel (W1) precedes its checkpoint (W2). Runtime types (W3) precede the runtime delivery/scanner +
outbound attachment (W4). Edge + HTTP server (W5) depend on the runtime client/token (W3) and the
outbound attachment shape (W4.1). The runtime+edge checkpoint (W6) precedes the integration test (W7)
and the final gate (W8). Config (W0) is a prerequisite of W4/W5.

## Notes

- Every correctness property P1–P9 maps to a required test task: P1→4.4, P2→1.7, P3→4.5, P4→1.6,
  P5→3.2, P6→4.5, P7→5.5, P8→7.1, P9→5.4.
- Reuses (no new work): `Command::NexusOperationResolved` → `NexusOperation*` terminal events
  (`kernel-nexus-operations`), the `CompletionCallback`/`CallbackState` model, and the
  `RespondNexusTaskFailed` failure conversion (`translate/nexus.rs`).
- Kernel stays pure: the dispatch-op `outcome` and `CompletionCallbackAttempted` are derived
  data/transitions — no I/O. Delivery (HTTP POST, scanner) is runtime; the endpoint is edge.
- HTTP server is hand-rolled on `hyper` (no new dependency); only `POST /nexus/callback` is served —
  the inbound Nexus StartOperation HTTP API is out of scope.
- Build-phase schema discipline: `next_attempt_at` is folded into `CompletionCallback` (serde-default),
  no `ALTER`.
- Cross-cluster routing of the completion POST (active-cluster lookup, client cache,
  `forwardCompleteOperation`) is `nexus-multi-cluster`, not here.
