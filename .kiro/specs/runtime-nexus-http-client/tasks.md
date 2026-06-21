# Implementation Plan

## Overview

Add a reqwest-backed `HttpNexusClient` implementing the existing `NexusHttpClient` trait, thread
handler-returned links through `NexusStartResult` and the kernel's Nexus start/completed events, and
wire the real client into tokeirad in place of `NoopNexusHttpClient`. Ground-truth the wire format to
`common/nexus/nexusrpc/client.go` + `handle.go @ v1.31.0`.

## Tasks

- [x] 1. Extend `NexusStartResult` and the kernel events for links/token
  - Add `operation_token` (async) and `links` (sync + async) to `NexusStartResult`; add `links` to
    `HistoryEventKind::NexusOperationStarted`/`NexusOperationCompleted` (fold into base events, no
    ALTER) and thread through `NexusResolution::{Started,Completed}`, the kernel apply/replay, and the
    edge history serializer.
  - _Requirements: 3.1, 3.2_

- [x] 2. Implement `HttpNexusClient` StartOperation
  - reqwest `POST {base}/{pct(service)}/{pct(operation)}`, body = input payload (content-type per
    `payload_serializer.go @ v1.31.0`); set `Nexus-Request-Id`, content-type, operation-timeout header,
    trace headers; per-request timeout from schedule-to-close.
  - Parse: 200 → `SyncCompleted{result, links}`; 201 + `OperationStateRunning` + non-empty token →
    `AsyncAccepted{token, links}`; 424 → `SyncFailed{message}`; else `Err`. Response `Nexus-Link`
    headers decoded (RFC 8288) and workflow-event links converted to kernel `Link` via the
    `temporal://` scheme (`link_converter.go @ v1.31.0`).
  - DEVIATION (Req 1.5 caller-links/callback): the trait carries neither caller links nor a callback
    URL, and tokeira hosts no inbound callback endpoint yet (deferred surface), so neither is sent.
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 3. Implement `HttpNexusClient` CancelOperation
  - reqwest `POST {base}/{pct(service)}/{pct(operation)}/cancel` with `Nexus-Operation-Token`; trait
    extended to carry the operation name (resolved from the pending op) since the cancel URL needs it.
  - DEFERRED: cancel-request *retry* and the `NexusOperationCancelRequest{Failed,Completed}` history
    lifecycle, plus persisting the handler-issued token (tokeira sends its own operation id as the
    token today; the v1.31.0 conformance handler does not gate cancel on the token value). Tracked as a
    follow-up cancel-lifecycle surface.
  - _Requirements: 2.1, 2.2_

- [x] 4. Wire the real client into tokeirad
  - Replace `Arc::new(NoopNexusHttpClient)` with `HttpNexusClient`; keep `Noop`/`Mock` for tests.
  - _Requirements: 4.1, 4.2_

- [x] 5. Tests
  - [x] 5.1 StartOperation mapping over a stub HTTP listener: 200 sync, 201 async, unsuccessful, link parse.
    - _Feature: runtime-nexus-http-client, Property 1, Property 2, Property 3_
    - _Requirements: 1.2, 1.3, 1.4, 3.2_
  - [x] 5.2 Request-shape assertions (paths, token header) + link converter / payload content-type units.
    - _Feature: runtime-nexus-http-client, Property 4_
    - _Requirements: 1.1, 2.1_
  - [-] 5.3 Runtime integration: External 200 → completed event with link; 201 → started event + pending op.
    - Covered at the client level (5.1) plus the existing mock-based runtime resolution→event tests;
      a full in-runtime live-listener flow is left to the operator conformance re-run.
    - _Feature: runtime-nexus-http-client, Property 1, Property 2_
    - _Requirements: 3.1, 3.2_

- [ ] 6. Verification gate and operator re-run
  - `cargo +nightly fmt`, `cargo lint`, `cargo test`, `cargo doc -D warnings` on touched crates: DONE.
  - Operator re-run of `^TestNexusWorkflowTestSuite`: PENDING (rebuild `tokeirad` first).
  - _Requirements: 4.1, 4.2_

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1"] },
    { "wave": 2, "tasks": ["2", "3"] },
    { "wave": 3, "tasks": ["4", "5"] },
    { "wave": 4, "tasks": ["6"] }
  ]
}
```

Wave 1 extends the result/event shape. Wave 2 implements the client methods. Wave 3 wires it in and adds
tests. Wave 4 verifies.

## Notes

- New outbound HTTP surface + a kernel event field (build-phase, folded into base events). No new
  dependency — `reqwest` is already vendored.
- Out of scope (tracked): `__temporal_system` internal endpoint, the inbound completion-callback
  endpoint, and start-attempt retry/backoff (`nexus-retry-policy`).
- Keep the kernel pure and history authoritative: the client lives in `tokeira-runtime`; only the event
  shape (links) touches the kernel.
