# Design Document — Runtime Nexus HTTP Client

## Overview

Implement the outbound `NexusHttpClient` that tokeirad currently stubs with `NoopNexusHttpClient`. The
trait, its `NexusStartResult` vocabulary, the endpoint registry, the dispatch wiring
(`RuntimeDispatchPublisher::handle_schedule_nexus_operation` / `handle_cancel_nexus_operation`), and the
resolution path (`NexusResolution` → kernel events) already exist and are correct — they just never run
because the outbound call errors. This adds a real HTTP implementation and threads handler-returned
links onto the operation's history events.

Wire format is ground-truthed to Temporal v1.31.0's own client wrapper
(`common/nexus/nexusrpc/client.go`, `handle.go` @ v1.31.0); the underlying protocol is the nexus-rpc
HTTP spec. No Rust Nexus crate exists; transport is `reqwest` (already a workspace dependency).

## Architecture

```
RuntimeDispatchPublisher::handle_schedule_nexus_operation
  └─ NexusHttpClient::start_operation(url, service, operation, input, timeout, callback, links, headers)
        └─ reqwest POST {url}/{service}/{operation}?callback=...   body = input payload
             ├─ 200 OK                     -> SyncCompleted { result, links }
             ├─ 201 Created (Running,token)-> AsyncAccepted { operation_token, links }
             └─ operation-unsuccessful     -> SyncFailed { failure }
        (transport / parse error)          -> Err  (publisher maps to NexusResolution::Failed)

RuntimeDispatchPublisher::handle_cancel_nexus_operation
  └─ NexusHttpClient::cancel_operation(url, service, operation, token)
        └─ reqwest POST {url}/{service}/{operation}/cancel   header Nexus-Operation-Token
```

The new type is `HttpNexusClient` (reqwest-backed) in `tokeira-runtime`, implementing the existing
`NexusHttpClient` trait. The publisher's existing outcome mapping is preserved.

## Components and Interfaces

- **`tokeira-runtime::nexus`**
  - `HttpNexusClient { http: reqwest::Client }` implementing `NexusHttpClient`.
  - `NexusStartResult` gains the data the resolution needs: `AsyncAccepted { operation_token: String,
    links: Vec<NexusLink> }`, `SyncCompleted { result: Payloads, links: Vec<NexusLink> }`. (`NexusLink`
    is the existing kernel link value type, reused.)
  - Request assembly per v1.31.0: path `{base}/{percent-encoded service}/{percent-encoded operation}`,
    `?callback=` query, headers `Nexus-Request-Id`, content-type, the callback-token header, `Nexus-Link`
    (caller links), and the request/operation timeout header. Exact header constants and the
    operation-unsuccessful status code are pinned from `common/nexus/nexusrpc/client.go @ v1.31.0`
    during implementation.
  - Response parsing per v1.31.0: read response `Nexus-Link` headers (handler links); 200 → success
    (body = result); 201 → require `OperationStateRunning` + non-empty token; unsuccessful status →
    failure body → `SyncFailed`; anything else → `Err`.
- **`tokeira-runtime` dispatch** — `handle_schedule_nexus_operation` already maps the three outcomes to
  `NexusResolution`; extend `NexusResolution::{Started,Completed}` (and the kernel events) to carry
  `links` so they reach history.
- **`tokeira-kernel`** — `HistoryEventKind::NexusOperationStarted` / `NexusOperationCompleted` gain a
  `links` field (fold into the existing base events; build-phase migration rule — no ALTER).
- **`apps/tokeirad`** — construct `HttpNexusClient` and pass it where `NoopNexusHttpClient` is wired.

## Data Models

- `NexusStartResult` (runtime): variants gain `operation_token` (async) and `links` (sync + async). No
  persisted form.
- Kernel `NexusOperationStarted` / `NexusOperationCompleted` events gain `links: Vec<Link>` (the
  existing kernel `Link` type, already used by completion callbacks and the timed-out failure). This is
  the only durable shape change; it round-trips through the edge history serializer (which already
  emits `links` on other events).
- No `tokeira-storage` schema change beyond the embedded-event shape.

## Correctness Properties

### Property 1: Sync completion produces a completed event

A 200 response yields `NexusResolution::Completed` and a `NexusOperationCompleted` history event
carrying the handler's response links.

**Validates: Requirements 1.2, 3.1, 3.2**

### Property 2: Async start produces a started event with a token

A 201 `OperationStateRunning` response yields `NexusResolution::Started`, a `NexusOperationStarted`
event (with links), and the operation remains pending under its token.

**Validates: Requirements 1.3, 3.1, 3.2**

### Property 3: Failure resolves, does not strand

An operation-unsuccessful status or a transport error yields `NexusResolution::Failed`; the operation
leaves the pending set.

**Validates: Requirements 1.4, 3.1**

### Property 4: Request shape matches v1.31.0

StartOperation targets `{url}/{service}/{operation}` and CancelOperation targets `.../cancel` with the
operation-token header.

**Validates: Requirements 1.1, 1.5, 2.1**

## Error Handling

- Transport errors, non-2xx/non-recognized statuses, and body/link parse failures map to
  `NexusResolution::Failed` with the cause in the failure payload — never a panic, never a stranded
  pending operation.
- The HTTP request honours the dispatch timeout (operation/schedule timeout) so a hung handler does not
  hold the dispatch task.
- A single start attempt per dispatch; retry/backoff is out of scope (Req 5.3).

## Testing Strategy

- Unit tests with a stub HTTP server (or the existing `MockNexusClient` for the mapping layer) covering
  sync-200, async-201, and unsuccessful responses, plus link parsing.
- A runtime integration test (in-process HTTP listener) asserting an External StartOperation 200 yields
  a `NexusOperationCompleted` event with the handler link, and a 201 yields a `NexusOperationStarted`
  event and a pending operation — no live external endpoint in the default suite.
- Verification gate on touched crates: `cargo +nightly fmt`, `cargo lint`, `cargo test`,
  `cargo doc -D warnings`.
- Operator: re-run `^TestNexusWorkflowTestSuite`; expect `SyncCompletion`, `SyncCompletion_LargePayload`,
  `Cancelation`, `CancelBeforeStarted`, and `StartToCloseTimeout`'s pending-op assertion to clear
  (`SystemEndpoint` remains, being internal).

## Out of Scope

- `__temporal_system` internal endpoint (`startOnHistoryService` @ v1.31.0) — separate surface.
- Inbound completion-callback HTTP endpoint (receiving the handler's async callback) — deferred; the
  client still sends the callback URL/token for wire-faithfulness.
- Start-attempt retry/backoff — `nexus-retry-policy`.

## Change Classification

**Standard-to-Architectural**: a new outbound HTTP surface in `tokeira-runtime` and a kernel event
field addition (build-phase, folded into base events). Carries this design note; no new third-party
dependency (reqwest already vendored).
