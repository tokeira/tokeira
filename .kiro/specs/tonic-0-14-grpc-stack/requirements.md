# Tonic 0.14 gRPC Stack — Requirements

## Introduction

Tokeira's public gRPC edge runs on tonic 0.11, which brings hyper 0.14, http 0.2, h2 0.3,
axum 0.6, and tower-http 0.4 with it. The Temporal Rust SDK 1.0.0 seam that shipped in
0.2.0 runs on tonic 0.14 (hyper 1, http 1, h2 0.4). The workspace therefore compiles two
HTTP stacks and bridges between them: the in-process endpoint copies headers between two
`http` crates, the HTTP API and Nexus HTTP transport adapters are written against legacy
body types, and the engine translates status codes between two `tonic` crates.

The h2 0.3 line is end of life. RUSTSEC-2026-0258 (a peer can queue empty DATA frames
without bound) is fixed only from 0.4.16, and tonic 0.11 serves the public gRPC port on
the 0.3 copy, so the flaw is reachable by any peer of that listener. `deny.toml` carries
the advisory as a declared stop-gap whose stated exit is this feature.

This feature moves every engine-owned gRPC and HTTP surface to tonic 0.14, prost 0.14, and
hyper 1, the line the SDK seam already uses, and removes the legacy stack from the
workspace. The checked-in protobuf bindings are regenerated with the matching code
generator, and the vendored protos are compiled with `protox`, so regeneration no longer
needs a system `protoc`. Observable wire behaviour does not change: the same RPCs, status
codes, error details, metadata, compression, gRPC-Web framing, reflection, HTTP API, and
Nexus HTTP behaviour, proven against goldens captured on the current stack before any
dependency moves and against the functional conformance corpus afterwards.

prost 0.14 message types and tonic 0.14 status values are part of the published crates'
public API (`tokeira-proto`, the engine's in-process errors), so this ships as 0.3.0.

Compatibility authority: Temporal server v1.31.0 (`TEMPORAL_SERVER_COMPAT`) for every RPC
behaviour, which this feature leaves unchanged, and the vendored protos under
`proto/upstream/` for wire shape. Library facts are taken from the crate sources in the
pinned registry: tonic 0.14.6, tonic-prost 0.14.6, tonic-prost-build 0.14.6, prost 0.14.4,
prost-reflect 0.16.4, hyper 1.10 and later, protox 0.9.1. Sibling specs:
[embedded-engine-listener](../embedded-engine-listener/requirements.md) owns the host
listener and its reset-on-stop behaviour;
[grpc-edge-transport](../grpc-edge-transport/requirements.md) owns the edge's gRPC
service surface; [edge-http-api-gateway](../edge-http-api-gateway/requirements.md) and
[edge-nexus-http-dispatch](../edge-nexus-http-dispatch/requirements.md) own the two
transport adapters' semantics;
[temporal-functional-conformance](../temporal-functional-conformance/requirements.md)
owns the corpus and the wire-coverage layer this feature re-verifies against.

## Glossary

- **Legacy Stack:** tonic 0.11, tonic-web 0.11, tonic-reflection 0.11, tonic-build 0.11,
  prost 0.12, prost-reflect 0.12, hyper 0.14, http 0.2, http-body 0.4, h2 0.3, axum 0.6,
  hyper-timeout 0.4, tower-http 0.4.
- **Target Stack:** tonic 0.14, tonic-prost 0.14, tonic-web 0.14, tonic-reflection 0.14,
  tonic-prost-build 0.14, prost 0.14, prost-types 0.14, prost-reflect 0.16, hyper 1,
  hyper-util 0.1, http 1, http-body 1, http-body-util 0.1, h2 0.4, axum 0.8, tower 0.5,
  tower-http 0.6, protox 0.9.
- **Public Listener:** the TCP gRPC server `tokeirad` binds, assembled in
  `crates/tokeira-engine/src/lib.rs` from the Workflow, Operator, Admin, and reflection
  services plus the tower layers listed in Requirement 3.
- **Host Listener:** the listener `Engine::listen` attaches to an embedded engine
  (`crates/tokeira-engine/src/listener.rs`).
- **In-Process Endpoint:** `InProcessGrpcService` in `crates/tokeira-edge/src/in_process.rs`,
  which dispatches a raw protobuf call into the tonic `Routes` value without a socket.
- **Transport Adapters:** the two tower layers in the engine that serve non-gRPC
  requests on the Public Listener's socket: `HttpApiLayer`
  (`crates/tokeira-engine/src/http_api_transport.rs`) for Temporal's HTTP/JSON API and
  `NexusHttpLayer` (`crates/tokeira-engine/src/nexus_http_transport.rs`) for caller-facing
  Nexus HTTP.
- **Generated Bindings:** the checked-in Rust code under
  `crates/tokeira-proto/src/generated/{upstream,tokeira}` produced by the Codegen Tool.
- **Descriptor Sets:** the encoded `FileDescriptorSet` files the Codegen Tool writes next
  to the bindings (`tokeira_public_descriptor.bin`, `tokeira_internal_descriptor.bin`),
  consumed by reflection and by the HTTP API transcoder.
- **Codegen Tool:** `tools/proto-sync` (`proto-sync generate`), which regenerates the
  Generated Bindings and Descriptor Sets from `proto/`.
- **Binding Inventory:** the set of services, methods (with input type, output type, and
  streaming kind), messages, enums, and fields (full name and field number) declared by
  the Generated Bindings' descriptor sets.
- **Golden:** a fixture captured on the Legacy Stack before any dependency moves and
  reproduced byte for byte by the Target Stack.
- **Wire Parity:** identical observable gRPC and HTTP behaviour between the Legacy Stack
  and the Target Stack: status code, status message, `grpc-status-details-bin` bytes,
  metadata, trailers, message framing, compression, gRPC-Web framing, reflection listings,
  HTTP API rendering, and Nexus HTTP rendering.
- **Connector Exception:** while `aurora-dsql-sqlx-connector` is pinned below 0.2, the AWS
  SDK's legacy HTTPS client keeps hyper 0.14, h2 0.3, hyper-rustls 0.24, and rustls 0.21 in
  the lock as a client-only path to AWS endpoints. That edge is owned by the connector
  slice, not this feature.

## Target State

- The workspace resolves one HTTP stack: the Target Stack. The Legacy Stack is absent from
  `Cargo.lock` except for the Connector Exception, which a lock-invariant test names
  explicitly and which disappears with the connector slice.
- The Generated Bindings are produced by `protox` plus `tonic-prost-build` 0.14 with no
  `protoc` on the machine; the Descriptor Sets keep their paths; the Binding Inventory is
  unchanged.
- The Public Listener, the Host Listener, the In-Process Endpoint, and the Transport
  Adapters keep every setting and behaviour in force today. Reflection additionally
  serves the `v1` protocol next to `v1alpha`.
- The engine has one `tonic`: the `tonic-sdk` alias and the status translation between
  tonic versions are gone, and the `hyper-legacy` and `http-body-legacy` aliases are gone.
- Wire Parity is proven by Goldens, by the transport-neutral engine tests, by the SDK
  1.0.0 probes over both transports, and by the functional conformance corpus.
- Out of scope: any change to RPC semantics, validation, defaulting, error mapping, or
  lifecycle behaviour; the DSQL connector edge; the connect-rust controller and
  compatibility surfaces, which already sit on hyper 1 and keep their generators.

## Evidence From Current Code

- Workspace pins: `Cargo.toml` `[workspace.dependencies]` holds `tonic = "0.11"`
  (`transport`, `gzip`), `tonic-reflection = "0.11"`, `tonic-web = "0.11"`,
  `tower-http = "0.4"` (`cors`), `prost-reflect = "0.12"` (`serde`), while `hyper = "1"`
  (`http1`, `server`), `hyper-util = "0.1"`, and `http-body-util = "0.1"` are already on
  the Target Stack.
- The proto crate pins `prost = "0.12"`, `prost-types = "0.12"`, and
  `tonic = "0.11"` (`crates/tokeira-proto/Cargo.toml`); its public module
  re-exports (`pub use public::…`, `pub use internal::…`) expose those types.
- The Codegen Tool pins `tonic-build = "0.11"` and `connectrpc-build = "0.6"`
  (`tools/proto-sync/Cargo.toml`) and runs three `tonic_build::configure()…compile()`
  passes plus one `connectrpc_build` pass ([generate.rs](../../../tools/proto-sync/src/generate.rs));
  `compile` shells out to `protoc`, which `docs/development.md` lists as a prerequisite.
- The In-Process Endpoint imports `hyper_legacy::{Body, Request, Version}` and
  `tonic::transport::server::Routes`, builds a hyper 0.14 request with `content-type`
  and `te: trailers`, reads trailers through the legacy body, and copies headers between
  the two `http` crates in `copy_request_headers` and `copy_response_headers`
  ([in_process.rs](../../../crates/tokeira-edge/src/in_process.rs)).
- The Host Listener builds `tonic_reflection::server::Builder`, layers `ResetOnStopLayer`
  over `Request<Body>`/`Response<BoxBody>`, and serves with `Server::builder()`
  `.add_routes(routes)` `.add_service(reflection)`
  ([listener.rs](../../../crates/tokeira-engine/src/listener.rs)).
- The Public Listener is assembled twice, with and without the conformance wire-coverage
  layer: `Server::builder().accept_http1(true)` then the Nexus HTTP layer, the HTTP API
  layer, `CorsLayer::permissive()`, `GrpcWebLayer::new()`, optionally
  `WireCoverageLayer`, then the three services and reflection, served with
  `serve_with_incoming_shutdown` ([lib.rs](../../../crates/tokeira-engine/src/lib.rs)).
- The engine maps `tonic::Code` to the SDK's `tonic_sdk::Code` in `to_sdk_status`
  because the SDK seam (`tonic-sdk = { package = "tonic", version = "0.14" }`) and the
  edge use different tonic versions (`crates/tokeira-engine/Cargo.toml`, `lib.rs`).
- The Transport Adapters import `http_body_legacy::Body` and
  `hyper_legacy::{Body, Request, Response}` and produce `Response<BoxBody>`; the HTTP API
  transcoder uses `prost-reflect` over the Descriptor Sets
  (`crates/tokeira-edge/src/http_api/{route,transcode,response,json}.rs`).
- The wire-coverage layer used by the conformance harness lives in
  `crates/tokeira-edge/src/conformance/layer.rs` and is typed against tonic 0.11 bodies.
- The three edge services enable gzip in both directions
  (`accept_compressed`/`send_compressed` in `crates/tokeira-edge/src/grpc/{workflow,operator,admin}_service.rs`)
  and set no explicit message-size limit, so the Legacy Stack's default decode limit
  applies.
- Error mapping builds statuses with `Status::with_details_and_metadata`
  (`crates/tokeira-edge/src/grpc/errors.rs`); the listener tests already compare statuses
  and details across transports (`statuses_and_details_match_across_transports`) and
  exercise reflection (`listener_mounts_the_reflection_service`); the test transport
  uses `tonic::client::Grpc` with `tonic::codec::ProstCodec`
  (`crates/tokeira-engine/tests/listener_support/mod.rs`).
- Dependency graph today: `cargo tree -i h2@0.3.27` shows exactly two parents, hyper 0.14
  and tonic 0.11; `cargo tree -i hyper@0.14.32` shows tonic 0.11, tonic-web 0.11, axum
  0.6, hyper-timeout 0.4, the AWS legacy client, and the direct `hyper-legacy` uses in the
  edge, the engine, and `tokeirad`.
- The connect-rust stack is already modern and is not part of the h2 0.3 edge: the
  workspace carries connectrpc 0.6.1 and buffa 0.6.0 on the product surfaces and
  connectrpc 0.8.1 and buffa 0.8.1 on the conformance control surface; both connectrpc
  lines depend on hyper 1, http 1, and axum 0.8 and carry no tonic dependency, and buffa
  carries no prost. connectrpc 0.9.0 and buffa 0.9.2 exist and are not used.
- The Temporal Rust SDK compiles its protos with `protox` (`temporalio-common` 1.0.0,
  `vendored-protox` feature), and `tonic-prost-build` 0.14 accepts a prebuilt descriptor
  set through `Builder::compile_fds`.

## Dependency and Configuration Policy

Every Legacy Stack edge is accounted for below. "Where" lists the manifests that name the
crate today.

| Crate | Today | Target | Where | Action |
|---|---|---|---|---|
| `tonic` | 0.11 (`transport`, `gzip`) | 0.14 (`transport`, `gzip`, `server`, `router`) | workspace; proto, edge, engine, controller, tokeirad, tests | move; `tonic-sdk` alias removed |
| `tonic-prost` | absent | 0.14 | proto, tests | add: codec for the generated stubs |
| `tonic-reflection` | 0.11 | 0.14 | workspace; engine | move; serve v1 and v1alpha |
| `tonic-web` | 0.11 | 0.14 | workspace; engine | move |
| `tonic-build` | 0.11 | replaced by `tonic-prost-build` 0.14 | proto-sync | replace |
| `protox` | absent | 0.9 | proto-sync | add: replaces `protoc` |
| `prost`, `prost-types` | 0.12 | 0.14 | proto and every crate naming them | move |
| `prost-reflect` | 0.12 (`serde`) | 0.16 (`serde`) | workspace; edge | move |
| `hyper` alias `hyper-legacy` | 0.14 | removed | edge, engine, tokeirad | delete |
| `http-body` alias `http-body-legacy` | 0.4 | removed; `http-body-util` 0.1 for collection | engine | delete |
| `tower-http` | 0.4 (`cors`) | 0.6 (`cors`) | workspace; engine | move |
| `axum`, `hyper-timeout` | 0.6, 0.4 (transitive via tonic 0.11) | 0.8, 0.5 (transitive via tonic 0.14) | none | resolve away |
| `h2`, `http`, `http-body` | 0.3, 0.2, 0.4 (transitive) | 0.4, 1, 1 | none | resolve away, except the Connector Exception for h2 0.3 |
| `connectrpc`, `connectrpc-build`, `buffa` | 0.6 (product surfaces) and 0.8 (conformance control) | unchanged | proto, proto-sync, runtime, edge, controller, autoscaler, compatibility, conformance, tokeirad | unchanged: both lines are already on hyper 1; unifying them on the 0.9 line is a separate slice |

Every server setting in force today is preserved; the one implicit setting becomes explicit.

| Setting | Today | Target |
|---|---|---|
| HTTP/1.1 acceptance on the Public Listener | `accept_http1(true)` | same |
| Layer order on the Public Listener | Nexus HTTP, HTTP API, CORS (permissive), gRPC-Web, wire coverage (conformance only) | same order |
| Services | Workflow, Operator, Admin, reflection | same |
| Compression | gzip accepted and sent on the three services | same |
| Decode limit | Legacy Stack default (4 MiB), not stated | `max_decoding_message_size(4 MiB)` stated on each service |
| Encode limit | Legacy Stack default (none) | same, stated |
| Reflection protocols | `v1alpha` | `v1` and `v1alpha` |
| Host Listener | `Routes` clone plus reflection, `ResetOnStopLayer` | same |
| In-process framing | unary, `content-type: application/grpc`, `te: trailers` | same |
| HTTP API request bound | `MAX_HTTP_API_REQUEST_BYTES` (4 MiB) | same |
| Nexus HTTP request bound | `MAX_NEXUS_PAYLOAD_BYTES` (2 MiB) | same |

## Requirements

### Requirement 1: One HTTP stack in the workspace

**User Story:** As the engine owner, I want one gRPC and HTTP stack across the workspace, so
that the h2 0.3 line leaves the server surface and no code bridges between two `http`
crates.

#### Acceptance Criteria

1.1 WHEN the workspace lock resolves, THE workspace SHALL contain exactly one version each
of `tonic`, `prost`, `prost-types`, `http`, `http-body`, `hyper-util`, and `tower-http`,
every one on the Target Stack line.

1.2 WHEN the workspace lock resolves, THE workspace SHALL contain none of `tonic 0.11`,
`tonic-web 0.11`, `tonic-reflection 0.11`, `tonic-build 0.11`, `prost 0.12`,
`prost-types 0.12`, `prost-reflect 0.12`, `axum 0.6`, `tower-http 0.4`,
`hyper-timeout 0.4`, `http 0.2`, or `http-body 0.4`.

1.3 WHILE the Connector Exception applies, THE workspace lock MAY contain `hyper 0.14`,
`h2 0.3`, `hyper-rustls 0.24`, and `rustls 0.21` only as dependencies of the AWS SDK's
legacy HTTPS client; WHEN `aurora-dsql-sqlx-connector` is pinned at 0.2 or later, THE
workspace lock SHALL contain none of them.

1.4 THE manifests SHALL declare no `hyper-legacy`, `http-body-legacy`, or `tonic-sdk`
dependency alias.

1.5 THE engine SHALL hand the SDK seam the edge's `tonic::Status` values directly, with no
translation between tonic versions.

1.6 WHILE the Connector Exception applies, THE `deny.toml` ignore for RUSTSEC-2026-0258
SHALL name the AWS legacy HTTPS client as the only remaining source and the connector
slice as its exit; WHEN the Connector Exception no longer applies, THE ignore SHALL be
removed.

### Requirement 2: Generated bindings from the pinned generator, without protoc

**User Story:** As a maintainer regenerating bindings, I want the checked-in code produced
by one pinned generator from a pure-Rust proto compiler, so that regeneration is
reproducible on any machine and the bindings match the stack they are compiled with.

#### Acceptance Criteria

2.1 THE Codegen Tool SHALL compile the vendored protos with `protox` into a
`FileDescriptorSet` and SHALL hand that set to `tonic-prost-build` 0.14
(`Builder::compile_fds`); THE Codegen Tool SHALL NOT invoke `protoc`.

2.2 WHEN the Codegen Tool runs, THE Tool SHALL encode the compiled descriptor set itself
and write it to the Descriptor Set paths in use today, rather than relying on the
generator to do so.

2.3 THE regenerated Generated Bindings SHALL declare the same Binding Inventory as the
bindings generated by the Legacy Stack.

2.4 WHEN the Codegen Tool runs on a clean tree, THE run SHALL produce no diff, and
`tools/proto-sync/Cargo.toml` SHALL pin the exact generator versions the checked-in
bindings were produced with.

2.5 THE `tokeira-proto` crate SHALL expose prost 0.14 message types and tonic 0.14 client
and server stubs, with `tonic-prost` as the codec.

2.6 THE compute, controller (connect-rust), and compatibility surfaces SHALL be
regenerated in the same run on their current generators and SHALL be unchanged in content.

### Requirement 3: The Public Listener serves the same surface

**User Story:** As an operator or SDK user, I want `tokeirad`'s gRPC port to behave exactly
as before the migration, so that workers, clients, tooling, and browsers notice nothing.

#### Acceptance Criteria

3.1 THE Public Listener SHALL mount, in the order in force today, the Nexus HTTP layer,
the HTTP API layer, permissive CORS, gRPC-Web, and, only when conformance recording is
enabled, the wire-coverage layer, followed by the Workflow, Operator, Admin, and
reflection services.

3.2 THE Public Listener SHALL accept HTTP/1.1 connections for gRPC-Web, the HTTP API, and
Nexus HTTP.

3.3 THE Workflow, Operator, and Admin services SHALL accept and send gzip-compressed
messages.

3.4 THE Workflow, Operator, and Admin services SHALL state a 4 MiB decode limit and SHALL
reject a larger request message with the status code and message the Legacy Stack
returned for the same message (a Golden).

3.5 THE reflection service SHALL serve `grpc.reflection.v1` and
`grpc.reflection.v1alpha`, and its service listing SHALL equal the Legacy Stack's listing
plus the `v1` reflection service itself.

3.6 WHEN a gRPC-Web request arrives, THE response SHALL carry the same payload and status
as the native gRPC response, as today.

### Requirement 4: The Host Listener and the In-Process Endpoint keep their contracts

**User Story:** As an embedding host, I want `Engine::listen` and the in-process endpoint
unchanged in behaviour, so that the SDK seam and the raw-protobuf callers keep working.

#### Acceptance Criteria

4.1 `Engine::listen` SHALL serve a clone of the `Routes` value the In-Process Endpoint
dispatches into, plus reflection, and SHALL reset in-flight calls with `UNAVAILABLE` when
the listener stops.

4.2 THE In-Process Endpoint SHALL build requests and read responses with the Target
Stack's `http` types end to end, and the header-copying helpers between two `http` crates
SHALL be removed.

4.3 WHEN the In-Process Endpoint returns a response, THE `content-type`, `grpc-status`,
`grpc-message`, and `grpc-status-details-bin` keys SHALL be withheld from the
caller-visible headers and every other response header and trailer SHALL pass through
unchanged, as today.

4.4 THE In-Process Endpoint SHALL keep its admission limit, drain, abort-on-drop
cancellation, unary frame parsing, and trailers-only response handling unchanged.

4.5 THE In-Process Endpoint's status reporting to the SDK seam SHALL use the edge's
`tonic::Status` directly.

### Requirement 5: Transport adapters keep their bounds and rendering

**User Story:** As an HTTP API or Nexus caller, I want the same limits and the same
responses, so that the migration is invisible on the non-gRPC surfaces.

#### Acceptance Criteria

5.1 THE HTTP API layer SHALL bound request bodies at `MAX_HTTP_API_REQUEST_BYTES` and
render successes and errors exactly as today.

5.2 THE Nexus HTTP layer SHALL bound request bodies at `MAX_NEXUS_PAYLOAD_BYTES`, handle
only the two Nexus route prefixes, and delegate every other request to tonic unchanged.

5.3 THE HTTP API transcoder SHALL use `prost-reflect` on the Target Stack's prost line
over the same Descriptor Sets and SHALL render the same JSON.

### Requirement 6: Wire Parity is proven

**User Story:** As the engine owner, I want proof that the migration changed no observable
behaviour, so that the targeted-release contract stands untouched.

#### Acceptance Criteria

6.1 THE feature SHALL NOT change request validation, defaulting, error mapping, lifecycle
ordering, or any other behaviour the targeted release defines; every such decision stays
where it is.

6.2 BEFORE any dependency moves, THE feature SHALL capture Goldens on the Legacy Stack:
the status catalogue (for a fixed list of edge error values, the code, message,
`grpc-status-details-bin` bytes, and metadata over both transports), the decode-limit
behaviour at 4 MiB minus one byte, 4 MiB, and 4 MiB plus one byte, the compression
matrix, reflection's service listing, gRPC-Web samples, and the Binding Inventory.

6.3 WHEN the migration is complete, THE same probes SHALL reproduce every Golden byte for
byte, with reflection's listing extended only by the `v1` reflection service.

6.4 WHEN the functional conformance corpus tiers recorded as CLEAN in
`docs/readiness/conformance.md` run against the migrated `tokeirad`, THE outcomes SHALL be
unchanged and the ledger SHALL record the rerun.

6.5 THE SDK 1.0.0 probes in `spikes/temporal-rust-sdk-v0-7-embedded` and the bench SHALL
pass over the in-process seam and over a listener.

### Requirement 7: Clients and tooling move with the servers

**User Story:** As a maintainer, I want every client adapter and test transport on the
same stack as the servers, so that no second stack survives in tooling.

#### Acceptance Criteria

7.1 THE controller, `tokeirad`, the bench, and the test transports SHALL use tonic 0.14
channels and the `tonic-prost` codec.

7.2 THE documentation SHALL state that regeneration needs no `protoc`
(`docs/development.md`, `docs/crates/proto.md`) and SHALL describe the `protox` path.

### Requirement 8: Release

**User Story:** As a downstream consumer, I want the public type change announced as a
breaking release, so that my pins move deliberately.

#### Acceptance Criteria

8.1 THE change SHALL ship as 0.3.0 with `changed` release notes for the prost and tonic
type moves and a `removed` note for the `protoc` requirement.
