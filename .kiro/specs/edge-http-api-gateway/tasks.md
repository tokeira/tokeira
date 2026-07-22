# Implementation Plan: Edge HTTP API Gateway

## Overview

Implement Tier 9.43 as a descriptor-driven compatibility adapter around the existing Tonic router.
The work is confined to the public proto assets, config, compatibility edge, `tokeirad` transport
wiring, observability/conformance seams, and the Temporal fork's Shape-2 harness. No task changes the
kernel, runtime, storage, history, or projection planes.

Every behavior task is grounded in `http_api_server.go`, `protojson_marshaler.go`,
`openapi_http_handler.go`, generated API gateway handlers, and `tests/http_api_test.go @ v1.31.0`.
The Rust shape remains Tokeira-native: one immutable descriptor catalog and one same-process Tonic
dispatch, with no copied Temporal service architecture.

## Tasks

- [x] 1. Establish descriptor and policy foundations
  - [x] 1.1 Add workspace `prost-reflect` 0.12 with its serde feature, consume it only from
    `tokeira-edge`, and decode `tokeira_proto::public::FILE_DESCRIPTOR_SET` into an immutable shared
    pool. Add startup diagnostics for missing services/extensions and confirm the dependency stays
    out of kernel/runtime/storage crates.
    - _Requirements: 2.1-2.2, 13.1-13.2_
  - [x] 1.2 Add strict, defaulted `[policy.http_api]` config with allow-all Host policy and no
    additional forwarded headers. Validate host globs and exact/trailing-star header rules; cover
    defaults, unknown fields, invalid rules, and TOML round trips.
    - _Requirements: 8.2, 8.5, 12.1-12.2_
  - [x] 1.3 Add edge module boundaries and documented neutral request/dispatch/response/error types;
    keep Hyper/Tonic body ownership in `tokeirad` and protobuf field semantics in `tokeira-edge`.
    - _Requirements: 1.5-1.6, 13.1-13.3_

- [x] 2. Build the descriptor-derived route catalog
  - [x] 2.1 Decode `google.api.http` method options for both target services and flatten primary plus
    direct additional bindings into typed verb/template/body/gRPC/input/output records. Reject
    streaming annotations, nested additional bindings, invalid selectors, and indistinguishable
    routes with method-naming diagnostics; property-test descriptor route closure (Property 1,
    at least 100 cases where generation applies).
    - _Requirements: 2.1-2.7, 10.5_
  - [x] 2.2 Implement the bounded path-template compiler and v1.31.0 legacy matcher for literals,
    `*`, `**`, nested variable templates, percent decoding, and custom verbs. Add deterministic
    precedence, the observed wrong-method `UNIMPLEMENTED`/HTTP `501` response, route misses, and
    form POST-to-GET fallback behavior;
    property-test capture fidelity and wrong-verb non-invocation (Property 2, at least 100 cases).
    - _Requirements: 1.4, 1.8, 3.1-3.3_
  - [x] 2.3 Narrow the existing Nexus recognizer to its concrete worker/endpoint route grammars and
    add non-interference tests proving ordinary `/namespaces/...` workflow paths reach the HTTP API
    catalog while real Nexus paths retain existing ownership.
    - _Requirements: 1.3-1.4, 1.7, 13.5_

- [x] 3. Implement protobuf-JSON and payload representation
  - [x] 3.1 Configure descriptor-aware canonical protobuf JSON for lower-camel names, symbolic
    enums, base64 bytes, string-form 64-bit integers, maps, well-known types, and pool-resolved
    `Any`; reject unknown/mistyped JSON. Property-test dynamic protobuf wire round trips
    (Property 4, at least 100 cases).
    - _Requirements: 4.4, 4.6, 5.8, 9.3-9.4_
  - [x] 3.2 Implement descriptor-guided `Payload`/`Payloads` shorthand expansion and contraction,
    including `binary/null`, compact `json/plain`, `_protoMessageType`, canonical-input detection,
    exact metadata eligibility, and all-or-nothing list fallback. Property-test eligible round trips
    and ineligible fallback (Property 5, at least 100 cases).
    - _Requirements: 6.1-6.9_
  - [x] 3.3 Implement the four inbound/outbound media modes plus `pretty` and
    `noPayloadShorthand` query overrides. Add goldens for compact no-newline and two-space pretty
    output on both success and error paths.
    - _Requirements: 5.1-5.9_

- [x] 4. Implement request transcoding
  - [x] 4.1 Decode complete and named-field bodies, then apply path overwrites and unbound query
    population in v1.31.0 order. Support text/JSON field names, nested singular messages, repeated
    scalars, maps, booleans, numbers, bytes, enum names/numbers, singular-duplicate errors, and
    ignored unknown query keys; property-test source precedence (Property 3, at least 100 cases).
    - _Requirements: 3.3-3.8, 4.1-4.6_
  - [x] 4.2 Enforce the 4 MiB limit only when a binding consumes a body; reject before full
    buffering and inner admission, while no-body generated handlers discard without reading solely
    to trigger the cap. Add exact boundary, chunk-crossing, malformed-body, and GET-with-body tests.
    - _Requirements: 4.3, 4.7-4.8_

- [x] 5. Implement Host and metadata policy
  - [x] 5.1 Compile/evaluate full-string case-sensitive Host globs and exact literal 403 denial.
    Wire `frontend.httpAllowedHosts` as a JSON conformance override in the same change as the live
    per-request consult; cover replace, clear, malformed fail-closed, and static fallback behavior.
    Property-test glob/reference equivalence (Property 6, at least 100 cases).
    - _Requirements: 8.1-8.7, 12.3, 12.6_
  - [x] 5.2 Reproduce v1.31.0 incoming metadata behavior: default gRPC-Gateway headers,
    `Grpc-Metadata-*`, Temporal additions, configured exact/prefix additions, Authorization's
    backward-compatible unprefixed alias, forwarded host/peer chain, binary validation, and default
    HTTP client identity/version. Property-test forwarding confinement (Property 7, at least 100
    cases).
    - _Requirements: 7.1-7.9_

- [x] 6. Add same-listener in-process Tonic dispatch
  - [x] 6.1 Add an app-owned `HttpApiLayer` around the existing router with logical precedence
    Nexus → OpenAPI → annotated HTTP → unchanged Tonic/gRPC-Web. Move original request extensions,
    preserve cancellation by awaiting inline, and never open a client socket or spawn detached RPC
    work.
    - _Requirements: 1.1-1.8, 9.9, 13.3-13.5_
  - [x] 6.2 Frame each transcoded message as one uncompressed unary gRPC request with the exact
    `/package.Service/Method` URI and ordinary incoming metadata, then invoke the existing inner
    Tower service once. Property-test frame/parser integrity and invalid-frame rejection (Property
    8, at least 100 cases).
    - _Requirements: 7.8-7.9, 9.1-9.2, 9.8_
  - [x] 6.3 Parse message/trailers and `grpc-status-details-bin`; render dynamic response messages or
    canonical `google.rpc.Status` with typed `Any` details and the pinned v2.27.1 HTTP-code table.
    Cover trailers-only errors, malformed/multiple/compressed frames, `WWW-Authenticate`,
    AlreadyStarted `runId`, and all status codes; property-test status fidelity (Property 9, at
    least 100 cases).
    - _Requirements: 9.3-9.8_
  - [x] 6.4 Add layer integration tests with a mock inner service for exact call count, metadata,
    extensions, cancellation/drop, success, error, and protocol delegation; property-test exclusive
    protocol ownership (Property 10, at least 100 cases).
    - _Requirements: 1.1-1.8, 9.1, 9.9, 13.3-13.5_

- [x] 7. Serve pinned OpenAPI artifacts and HTTP metrics
  - [x] 7.1 Obtain the official uncompressed OpenAPI v2 JSON and v3 YAML artifacts for
    `TEMPORAL_PROTO_VERSION`, vendor them with provenance beside the upstream API input, expose
    immutable bytes from `tokeira-proto`, and test that both parse and cover the descriptor-derived
    route paths.
    - _Requirements: 10.1-10.6_
  - [x] 7.2 Serve the v1.31.0 `/swagger.json` and `/openapi.yaml` prefixes before generic bindings,
    with observable `text/plain; charset=utf-8`, no runtime filesystem I/O, and no inner-service
    invocation.
    - _Requirements: 1.2, 10.1-10.6_
  - [x] 7.3 Emit `tokeira_edge_http_service_requests_total` exactly once immediately before inner
    invocation with full operation and decoded/unknown namespace. Cover every pre-admission and
    inner-error outcome; property-test metric singularity (Property 11, at least 100 cases).
    - _Requirements: 11.1-11.5_

- [x] 8. Complete conformance delivery seams
  - [x] 8.1 Extend the Temporal fork's generated Shape-2 `tokeirad` TOML with the onebox's exact and
    prefix forwarded-header rules, without editing `tests/http_api_test.go` or another corpus body.
    - _Requirements: 12.4-12.5_
  - [x] 8.2 Map the native HTTP-service counter to Temporal's `http_service_requests` name in the
    fork's metrics bridge and add seam tests for operation/namespace/value preservation.
    - _Requirements: 11.6_
  - [x] 8.3 Verify the conformance control path accepts, applies, and clears
    `frontend.httpAllowedHosts` JSON between leaves; retain tolerant handling for unrelated keys.
    - _Requirements: 8.6-8.7, 12.3, 12.6_
  - [x] 8.4 Forward synthetic-gRPC metadata through the Shape-2 authorization callback and confine
    the post-signal scheduling grace to conformance builds.
    - _Requirements: 7.8-7.9, 12.5, 13.1-13.3_

- [x] 9. Retire the placeholder and close documentation/matrix state
  - [x] 9.1 Remove the obsolete `/api/v1/{service}/{method}` `HttpProxy` module, constants, exports,
    and stale module documentation; update focused callers/tests to the real annotated gateway.
    - _Requirements: 13.6_
  - [x] 9.2 Update the API compatibility matrix, HTTP/readiness/configuration documentation, and
    Tier 9.43 conformance ledger with the same-listener gateway, Host/header policy, OpenAPI
    provenance, dependency rationale, and no-kernel/no-runtime boundary. Do not mark the tier green
    before task 10.3.
    - _Requirements: 12.1-12.6, 13.1-13.7_

- [x] 10. Verification and green gate
  - [x] 10.1 Run nightly format and focused locked checks/tests for every touched Tokeira crate,
    including all eleven properties at at least 100 cases and native gRPC/gRPC-Web/Nexus/authz
    regressions.
    - _Requirements: 1.1-13.7_
  - [x] 10.2 Build the conformance-enabled `tokeirad` once after implementation, run Temporal fork
    seam tests, and execute `TestHttpApiTestSuite`; diagnose any miss against v1.31.0 source rather
    than altering corpus assertions.
    - _Requirements: 1.1-13.7_
  - [x] 10.3 Run `TestHttpApiTestSuite` twice consecutively in fresh suite processes with every leaf
    passing, then record Tier 9.43 green.
    - _Requirements: 13.7_
  - [x] 10.4 Run the repository enforced merge bar: `cargo +nightly fmt --all --check`,
    `cargo lint`, `cargo check --workspace`, `cargo test --workspace`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`. The configured `cargo lint` alias
    already covers workspace test targets; this checkout has no separate `cargo test-lint` alias.
    Run `tkr ci check` when its Dagger module is available; it currently reports that the module has
    not been scaffolded.
    - _Requirements: 13.1-13.7_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["2.1", "2.2", "3.1", "5.1"] },
    { "id": 2, "tasks": ["2.3", "3.2", "3.3", "4.1", "5.2", "7.1"] },
    { "id": 3, "tasks": ["4.2", "6.1", "6.2", "7.2", "7.3", "8.1", "8.2", "8.3"] },
    { "id": 4, "tasks": ["6.3", "6.4", "9.1"] },
    { "id": 5, "tasks": ["10.1", "10.2"] },
    { "id": 6, "tasks": ["10.3", "9.2", "10.4"] }
  ]
}
```

## Notes

- **No kernel/runtime work belongs to this feature.** Any apparent need for workflow state,
  transition commands, history edits, queue semantics, or projection writes is an architecture
  stop-and-review condition.
- The descriptor pool determines wire shape; Temporal v1.31.0 determines behavior. Neither implies
  copying Temporal's listener or frontend implementation.
- `prost-reflect` is the single new Rust dependency and is pinned to the workspace's Prost line.
  Any additional dependency requires a separate justification before it is added.
- OpenAPI bytes are upstream artifacts, not regenerated approximations. Advancing
  `TEMPORAL_PROTO_VERSION` must advance and revalidate both documents.
- The Temporal fork changes only Shape-2 delivery/metrics seams. Corpus test bodies remain pristine.
- Task 9.2 is ordered after the clean conformance gate for its green ledger update even though
  supporting documentation may be drafted earlier.
