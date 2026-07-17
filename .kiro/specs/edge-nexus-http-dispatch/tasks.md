# Implementation Plan: Edge Nexus HTTP Dispatch

## Tasks

- [x] 1. Transport and route model
  - [x] 1.1 Add neutral Nexus HTTP request/response/error types and route parser;
    PBT for route resolution/delegation (Property 1).
    - _Requirements: 1.1-1.4, 2.1-2.8_
  - [x] 1.2 Install a same-listener tower layer around the tonic router; cap body
    collection and delegate unrelated requests byte-for-byte.
    - _Requirements: 1.1-1.4, 3.8_

- [x] 2. Resolution and request translation
  - [x] 2.1 Resolve namespace and endpoint routes through `NamespaceCache` and the
    live endpoint store; exact preprocessing errors + metrics.
    - _Requirements: 2.1-2.8, 7.1-7.2_
  - [x] 2.2 Translate Start body/content metadata, callback data, links, general
    headers, capability, arrival time, and timeout; enforce the 2 MiB payload
    boundary; PBT for Property 2.
    - _Requirements: 3.1-3.10_
  - [x] 2.3 Translate Cancel; implement header→query precedence on the current
    route and independent path-token selection on the deprecated route; PBT for
    Property 3.
    - _Requirements: 4.1-4.5_

- [x] 3. Authorization
  - [x] 3.1 Invoke the shared auth foundation after target resolution and before
    broker publication with the exact namespace/endpoint API target; implement
    Nexus denial/error mapping.
    - _Requirements: 8.1-8.6, 8.8_
  - [x] 3.2 Add a conformance-only callback client keyed by resolved namespace;
    production builds contain no callback surface; PBT for Property 6.
    - _Requirements: 8.7-8.9_
  - [x] 3.3 Extend the Temporal fork's Shape-2 host with a callback server that
    retains the exact `SetOnAuthorize` closure and maps registered namespaces to
    their dedicated cluster; clear mappings on cleanup.
    - _Requirements: 8.7_

- [x] 4. Synchronous broker dispatch
  - [x] 4.1 Publish an HTTP-correlated task only after validation and authorization;
    await the exact waiter with timeout/cancellation cleanup; PBT for Property 4.
    - _Requirements: 5.1-5.6_
  - [x] 4.2 Preserve the remaining/buffered request timeout on the worker request.
    - _Requirements: 5.5-5.6_

- [x] 5. HTTP response serialization and metrics
  - [x] 5.1 Map every worker outcome to the v1.31.0 status/body/header contract;
    legacy and modern failure modes; PBT for Property 5.
    - _Requirements: 6.1-6.11_
  - [x] 5.2 Emit preprocess, terminal Nexus, latency, and service telemetry exactly
    once with namespace/method/outcome/endpoint dimensions; PBT for Property 7.
    - _Requirements: 7.1-7.7_

- [x] 6. Verification
  - [x] 6.1 Unit and integration tests for every Tier 7.36 HTTP leaf, same-listener
    multiplexing, broker round-trip, callback isolation, and exact metrics.
    - _Requirements: 1.1-8.9_
  - [x] 6.2 Run `TestNexusAPIValidationTestSuite` twice clean in fresh processes.
    - _Requirements: 1.1-8.9_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2"] },
    { "id": 4, "tasks": ["5.1", "5.2"] },
    { "id": 5, "tasks": ["6.1", "6.2"] }
  ]
}
```

## Notes

- No second public listener is introduced.
- The callback bridge is conformance-only and namespace-scoped; it is not a
  production policy mechanism.
- No kernel change belongs to this feature.
