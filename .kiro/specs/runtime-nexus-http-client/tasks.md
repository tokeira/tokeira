# Implementation Plan

## Overview

Add a reqwest-backed `HttpNexusClient` implementing the existing `NexusHttpClient` trait, thread
handler-returned links through `NexusStartResult` and the kernel's Nexus start/completed events, and
wire the real client into tokeirad in place of `NoopNexusHttpClient`. Ground-truth the wire format to
`common/nexus/nexusrpc/client.go` + `handle.go @ v1.31.0`.

## Tasks

- [ ] 1. Extend `NexusStartResult` and the kernel events for links/token
  - Add `operation_token` (async) and `links` (sync + async) to `NexusStartResult`; add `links` to
    `HistoryEventKind::NexusOperationStarted`/`NexusOperationCompleted` (fold into base events, no
    ALTER) and thread through `NexusResolution::{Started,Completed}`, the kernel apply/replay, and the
    edge history serializer.
  - _Requirements: 3.1, 3.2_

- [ ] 2. Implement `HttpNexusClient` StartOperation
  - reqwest `POST {base}/{pct(service)}/{pct(operation)}?callback=...`, body = input payload; set
    `Nexus-Request-Id`, content-type, callback-token header, `Nexus-Link`, request/operation-timeout
    headers (constants pinned from `client.go @ v1.31.0`).
  - Parse: 200 → `SyncCompleted{result, links}`; 201 + `OperationStateRunning` + token →
    `AsyncAccepted{token, links}`; unsuccessful status → `SyncFailed{failure}`; else `Err`.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ] 3. Implement `HttpNexusClient` CancelOperation
  - reqwest `POST {base}/{pct(service)}/{pct(operation)}/cancel` with `Nexus-Operation-Token`; map
    success/failure as the trait contract requires today.
  - _Requirements: 2.1, 2.2_

- [ ] 4. Wire the real client into tokeirad
  - Replace `Arc::new(NoopNexusHttpClient)` with `HttpNexusClient`; keep `Noop`/`Mock` for tests.
  - _Requirements: 4.1, 4.2_

- [ ] 5. Tests
  - [ ] 5.1 StartOperation mapping over a stub HTTP listener: 200 sync, 201 async, unsuccessful, link parse.
    - _Feature: runtime-nexus-http-client, Property 1, Property 2, Property 3_
    - _Requirements: 1.2, 1.3, 1.4, 3.2_
  - [ ] 5.2 Request-shape assertions (paths, token header).
    - _Feature: runtime-nexus-http-client, Property 4_
    - _Requirements: 1.1, 2.1_
  - [ ] 5.3 Runtime integration: External 200 → completed event with link; 201 → started event + pending op.
    - _Feature: runtime-nexus-http-client, Property 1, Property 2_
    - _Requirements: 3.1, 3.2_

- [ ] 6. Verification gate and operator re-run
  - `cargo +nightly fmt`, `cargo lint`, `cargo test`, `cargo doc -D warnings` on touched crates; then
    operator re-run of `^TestNexusWorkflowTestSuite`.
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
