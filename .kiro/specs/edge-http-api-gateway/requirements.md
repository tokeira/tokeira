# Requirements Document: Edge HTTP API Gateway

## Introduction

This feature implements Temporal's public HTTP/JSON API at Tokeira's compatibility edge. It
transcodes the `google.api.http` bindings attached to WorkflowService and OperatorService methods
between REST-shaped HTTP requests and the same protobuf/gRPC service path already served by
`tokeirad`. It does not introduce a second workflow implementation, another authoritative service,
or any kernel/runtime state.

Observable behaviour targets Temporal server v1.31.0. The authoritative server loci are
`service/frontend/http_api_server.go`, `service/frontend/protojson_marshaler.go`,
`service/frontend/openapi_http_handler.go`, and `tests/http_api_test.go @ v1.31.0`. Route and message
shape comes from the vendored Temporal API descriptors under `proto/upstream/`, whose version is
independently pinned by `TEMPORAL_PROTO_VERSION`.

Temporal implements this surface with a generated Go gRPC-Gateway, an inline reflective gRPC client,
and a separate HTTP listener. Those are implementation details, not the compatibility contract.
Tokeira preserves its architecture: a same-listener Tower adapter recognizes HTTP API routes,
transcodes them using the vendored descriptors, and invokes the existing inner Tonic service
in-process. The request therefore passes through the same gRPC adapter, authentication,
authorization, validation, edge service, and runtime path as a native gRPC request. The gateway
owns transport translation only.

## Glossary

- **HTTP_API_Gateway**: The edge-owned HTTP/JSON compatibility adapter defined by this feature.
- **HTTP_Binding**: A primary or additional `google.api.http` rule attached to a unary
  WorkflowService or OperatorService method.
- **Descriptor_Pool**: The reflection view decoded from
  `tokeira_proto::public::FILE_DESCRIPTOR_SET` and used to discover methods, bindings, request
  messages, and response messages.
- **Canonical_Proto_JSON**: Standard protobuf JSON mapping: lower-camel field names, symbolic enum
  names, base64 bytes, string-form 64-bit integers, and omission of absent/default fields.
- **Payload_Shorthand**: Temporal's JSON representation in which eligible `Payloads` values are JSON
  arrays of their decoded values rather than canonical `{metadata,data}` payload objects.
- **Gateway_Policy**: Tokeira's static `[policy.http_api]` configuration: host allow-list and
  additional forwarded-header patterns.
- **Synthetic_gRPC_Request**: An in-process request assembled by the HTTP transport adapter with a
  gRPC method path, framed protobuf body, and filtered metadata, then submitted to the existing
  inner Tonic service without network I/O.
- **HTTP_Service_Metric**: One counter observation for an admitted HTTP binding, labelled by the
  full gRPC method and request namespace.

## Target State

- The existing public `tokeirad` address accepts native gRPC, gRPC-Web, caller-facing Nexus HTTP,
  and Temporal HTTP/JSON traffic on one listener.
- Every primary and additional `google.api.http` binding in the vendored WorkflowService and
  OperatorService descriptors is discovered rather than hand-maintained.
- HTTP path variables, query parameters, JSON bodies, and eligible headers are translated into the
  corresponding protobuf request; the existing Tonic service remains the only RPC implementation.
- Responses use canonical protobuf JSON or Temporal payload shorthand, with compact/pretty variants
  selected exactly as v1.31.0 selects them.
- Tonic status responses become the v1.31.0 HTTP status and `google.rpc.Status` JSON representation,
  including typed details.
- Host admission, forwarded-header policy, body limits, OpenAPI documents, and HTTP service metrics
  match the observable v1.31.0 contract.
- The conformance-only override bridge can change `frontend.httpAllowedHosts` live, and the
  conformance harness supplies its existing onebox-specific forwarded-header configuration without
  changing corpus test bodies.
- `tokeira-kernel`, `tokeira-runtime`, storage, history, and projections remain unchanged.

## Ground-Truth Decisions

| Behaviour | v1.31.0 source | Tokeira contract |
|---|---|---|
| HTTP routes | WorkflowService/OperatorService `google.api.http` annotations from the API module used by v1.31.0; generated handlers registered in `http_api_server.go` | Discover the equivalent bindings from the pinned vendored descriptor set; individual RPC support remains governed by the compatibility matrix |
| Listener topology | `NewHTTPAPIServer` derives a separate HTTP port from the frontend gRPC listener | Deliberate mechanism difference: multiplex on Tokeira's existing public listener |
| Four JSON modes | `newTemporalProtoMarshaler` in `protojson_marshaler.go @ v1.31.0` | Preserve all four media types and query overrides |
| Payload shorthand | `go.temporal.io/api/common/v1/payload_json.go` at the API version used by v1.31.0 | Apply the same eligible-encoding and whole-`Payloads` fallback rules |
| Host validation | `allowedHostsMiddleware` and `frontend.httpAllowedHosts` in `http_api_server.go` / `common/dynamicconfig/constants.go @ v1.31.0` | Static production policy plus a live conformance-only override; default allow-all |
| Header forwarding | `incomingHeaderMatcher` and `defaultForwardedHeaders` in `http_api_server.go @ v1.31.0` | Default gRPC-Gateway headers plus exact/prefix operator additions; inject HTTP client identity |
| Error mapping | `HTTPAPIServer.errorHandler` in `http_api_server.go @ v1.31.0` | Preserve gRPC code/message/details and use gRPC-Gateway's HTTP status mapping |
| Request bound | `rpc.MaxHTTPAPIRequestBytes` in `common/rpc/grpc.go @ v1.31.0` | Cap body reads at 4 MiB; a generated no-body handler does not read merely to trigger the cap |
| OpenAPI | `openapi_http_handler.go @ v1.31.0` serves API-module v2 JSON and v3 YAML assets | Serve artifacts aligned with `TEMPORAL_PROTO_VERSION` at the same root paths |
| Metric | `inlineClientConn.Invoke` records `metrics.HTTPServiceRequests` | Emit one native counter observation before inner gRPC invocation; bridge its name for the corpus |
| Route precedence | `fx.go` registers Nexus and OpenAPI routes before the gateway catch-all | Nexus and OpenAPI recognition precedes generic annotated bindings; unrelated paths continue to Tonic |

## Configuration Surface

The HTTP API is enabled whenever the public frontend listener is enabled. Tokeira does not add a
second port or an enable flag. The optional policy is:

```toml
[policy.http_api]
allowed_hosts = ["*"]
additional_forwarded_headers = [
  "x-operator-header",
  "x-operator-prefix-*",
]
```

| Field | Default | Semantics |
|---|---|---|
| `allowed_hosts` | `["*"]` | Full-host, case-sensitive wildcard patterns. `*` matches any substring; all other characters are literal. Any matching pattern admits the request. |
| `additional_forwarded_headers` | `[]` | Case-insensitive exact header names, or prefix rules ending in one `*`. Matching headers are forwarded as gRPC metadata in addition to the gateway defaults. |

Production configuration is immutable after startup. A `--features conformance` server additionally
consults `frontend.httpAllowedHosts` from the existing live override registry on every matched HTTP
request. The override value is a JSON string array and, while present, replaces the static
`allowed_hosts` value. This mirrors the global live property read by v1.31.0 without introducing a
production dynamic-config subsystem.

## Requirements

### Requirement 1: Same-listener protocol ownership

**User Story:** As an operator, I want one advertised frontend address to serve Temporal's supported
protocols, so that enabling HTTP compatibility does not create another service topology.

#### Acceptance Criteria

1. WHEN an HTTP/1 request matches a WorkflowService or OperatorService HTTP_Binding, THE frontend
   SHALL route it to the HTTP_API_Gateway.
2. WHEN an HTTP/1 request targets `/swagger.json` or `/openapi.yaml`, THE frontend SHALL route it to
   the OpenAPI handler before annotated-binding dispatch.
3. WHEN an HTTP/1 request matches a caller-facing Nexus route, THE frontend SHALL route it to the
   existing Nexus HTTP handler before annotated-binding dispatch.
4. IF a request matches neither Nexus, OpenAPI, nor an HTTP_Binding, THEN THE frontend SHALL delegate
   it unchanged to the existing Tonic service.
5. THE frontend SHALL serve HTTP_API_Gateway and native gRPC traffic on the same bound address.
6. THE HTTP_API_Gateway SHALL invoke the existing inner Tonic service in-process and SHALL NOT open a
   loopback network connection or instantiate a second WorkflowService/OperatorService.
7. THE existing Nexus recognizer SHALL reject ordinary `/namespaces/...` workflow HTTP routes rather
   than claiming the whole namespace prefix.
8. WHEN a path matches an HTTP_Binding registered for another HTTP verb but not the request verb,
   THE HTTP_API_Gateway SHALL return the v1.31.0 method-not-allowed response rather than delegating
   the request to Tonic.

### Requirement 2: Descriptor-derived route completeness

**User Story:** As a client author, I want the HTTP surface to track the pinned Temporal API, so that
route availability cannot drift from the protobuf contract.

#### Acceptance Criteria

1. AT startup, THE HTTP_API_Gateway SHALL decode
   `tokeira_proto::public::FILE_DESCRIPTOR_SET` into one Descriptor_Pool.
2. THE HTTP_API_Gateway SHALL discover every unary WorkflowService and OperatorService method carrying
   a `google.api.http` option.
3. FOR EACH discovered method, THE HTTP_API_Gateway SHALL register its primary HTTP_Binding and every
   direct `additional_bindings` entry; nested additional bindings are invalid under
   `google/api/http.proto` and SHALL fail startup validation.
4. EACH registered binding SHALL retain the HTTP verb, path template, body selector, full gRPC
   method name, input descriptor, and output descriptor.
5. IF two bindings are indistinguishable for the same HTTP verb and path specificity, THEN startup
   SHALL fail with a diagnostic naming both methods rather than choosing by iteration order.
6. A vendored API update that adds or removes an annotation SHALL update the registered route set
   without a hand-maintained route-table edit.
7. HTTP route exposure SHALL NOT change whether the underlying gRPC method is implemented; an
   unsupported method SHALL preserve the existing gRPC status through HTTP error translation.

### Requirement 3: HTTP path and query transcoding

**User Story:** As an HTTP client, I want route variables and query parameters mapped according to
the protobuf annotations, so that I can address the same resources as a gRPC client.

#### Acceptance Criteria

1. WHEN a path matches an HTTP_Binding, THE HTTP_API_Gateway SHALL percent-decode each captured
   variable according to the `google.api.http` path-template rules.
2. THE HTTP_API_Gateway SHALL support literal segments, `*`, `**`, and nested field selectors in path
   variables.
3. THE HTTP_API_Gateway SHALL assign path captures to their selected protobuf fields, including
   nested message fields.
4. THE HTTP_API_Gateway SHALL map query parameters to unbound protobuf fields using protobuf JSON
   field names, including nested fields, repeated scalar values, booleans, numbers, bytes, and enum
   names or numbers accepted by v1.31.0's gateway.
5. WHERE a field is bound by the path or request body, THE HTTP_API_Gateway SHALL NOT also consume it
   as an ordinary query field.
6. WHEN the same singular query field is supplied more than once, THE HTTP_API_Gateway SHALL preserve
   the v1.31.0 gRPC-Gateway result rather than silently inventing aggregation semantics.
7. IF a capture or query value cannot be converted to its target field type, THEN THE
   HTTP_API_Gateway SHALL return the v1.31.0 invalid-argument HTTP response and SHALL NOT invoke the
   inner Tonic service.
8. THE query flags `pretty` and `noPayloadShorthand` SHALL control response representation and SHALL
   NOT be assigned to protobuf request fields.

### Requirement 4: HTTP body transcoding

**User Story:** As an HTTP client, I want JSON request bodies decoded into the annotated request
message, so that the HTTP and gRPC transports carry equivalent protobuf values.

#### Acceptance Criteria

1. WHERE an HTTP_Binding declares `body: "*"`, THE HTTP_API_Gateway SHALL decode the request body as
   the complete input message before applying path bindings.
2. WHERE an HTTP_Binding selects a named body field, THE HTTP_API_Gateway SHALL decode the request
   body as that field's message/value and merge it into the complete input message.
3. WHERE an HTTP_Binding has no body selector, THE HTTP_API_Gateway SHALL discard any supplied body
   and build the request from path/query fields, matching the generated v1.31.0 gateway handlers.
4. THE HTTP_API_Gateway SHALL decode Canonical_Proto_JSON using the method's input descriptor and the
   shared Descriptor_Pool, including well-known types and `Any` values resolvable from that pool.
5. PATH-bound values SHALL take the same precedence over body values as v1.31.0's generated gateway.
6. IF JSON is malformed, contains an unknown field, or violates protobuf JSON typing, THEN THE
   HTTP_API_Gateway SHALL return an invalid-argument HTTP response and SHALL NOT invoke Tonic.
7. IF a binding with a body selector reads more than 4 MiB, THEN THE HTTP_API_Gateway SHALL reject
   it before full buffering or inner-service admission.
8. A binding without a body selector SHALL NOT read a discarded body solely to enforce the 4 MiB
   limit, matching the generated v1.31.0 handler behavior.

### Requirement 5: Temporal JSON media modes

**User Story:** As an HTTP client, I want Temporal's canonical and ergonomic JSON forms, so that
existing HTTP integrations can choose fidelity or convenience without changing API behavior.

#### Acceptance Criteria

1. WHEN the selected media type is `application/json`, THE HTTP_API_Gateway SHALL decode and encode
   Payload_Shorthand using compact JSON.
2. WHEN the selected media type is `application/json+pretty`, THE HTTP_API_Gateway SHALL decode and
   encode Payload_Shorthand using two-space-indented JSON.
3. WHEN the selected media type is `application/json+no-payload-shorthand`, THE HTTP_API_Gateway
   SHALL decode and encode Canonical_Proto_JSON using compact JSON.
4. WHEN the selected media type is `application/json+pretty+no-payload-shorthand`, THE
   HTTP_API_Gateway SHALL decode and encode Canonical_Proto_JSON using two-space-indented JSON.
5. WHERE the query contains `pretty`, THE HTTP_API_Gateway SHALL select the corresponding pretty
   representation regardless of the original Accept header.
6. WHERE the query contains `noPayloadShorthand`, THE HTTP_API_Gateway SHALL select the corresponding
   canonical representation regardless of the original Accept header.
7. WHEN both representation query flags are present, THE HTTP_API_Gateway SHALL select pretty
   Canonical_Proto_JSON.
8. THE HTTP_API_Gateway SHALL emit protobuf field names, enum values, bytes, integers, maps, and
   well-known types according to Canonical_Proto_JSON rather than Rust Serde struct conventions.
9. Compact success and error representations SHALL contain no formatting newline; pretty
   representations SHALL contain indentation/newlines where the message has fields.

### Requirement 6: Payload shorthand fidelity

**User Story:** As a Temporal HTTP client, I want payload values represented directly where safe,
so that I do not need to construct encoding metadata and base64 data manually.

#### Acceptance Criteria

1. WHEN shorthand input supplies a JSON scalar, array, or object for a `Payload`, THE
   HTTP_API_Gateway SHALL create a payload with `encoding=json/plain` and compact JSON data.
2. WHEN shorthand input supplies JSON `null` for a `Payload`, THE HTTP_API_Gateway SHALL create a
   payload with `encoding=binary/null` and empty data.
3. WHEN a shorthand object contains string `_protoMessageType`, THE HTTP_API_Gateway SHALL remove
   that member from its compact data and create `encoding=json/protobuf` plus matching
   `messageType` metadata.
4. WHEN a shorthand object is already a valid canonical Payload object, THE HTTP_API_Gateway SHALL
   preserve its decoded metadata and data instead of re-encoding it as `json/plain`.
5. WHEN a `Payloads` field is shorthand-enabled, THE HTTP_API_Gateway SHALL represent it as a JSON
   array and SHALL accept JSON `null` as an empty payload list.
6. WHEN emitting shorthand, THE HTTP_API_Gateway SHALL directly emit `binary/null`, eligible
   `json/plain`, and eligible `json/protobuf` payloads using the v1.31.0 rules.
7. IF a `json/plain` payload carries metadata beyond `encoding`, or a `json/protobuf` payload lacks
   exactly `encoding` plus non-empty `messageType`, THEN that payload SHALL NOT be shorthand-eligible.
8. IF any member of one `Payloads` value is not shorthand-eligible, THEN THE HTTP_API_Gateway SHALL
   emit the complete `Payloads` value canonically rather than mixing shorthand and canonical members.
9. IF shorthand emission encounters an unsupported encoding in a context where v1.31.0 returns a
   marshal error, THEN THE HTTP_API_Gateway SHALL surface the corresponding HTTP error rather than
   corrupt or omit the payload.

### Requirement 7: Header and caller-context forwarding

**User Story:** As an authenticated HTTP caller, I want eligible headers to reach the same edge
interceptors as gRPC metadata, so that transport choice does not bypass identity or request context.

#### Acceptance Criteria

1. THE HTTP_API_Gateway SHALL apply the default gRPC-Gateway incoming-header matcher used by
   v1.31.0.
2. THE HTTP_API_Gateway SHALL additionally forward `Authorization-Extras`, `X-Forwarded-For`,
   `client-name`, and `client-version` case-insensitively.
3. THE HTTP_API_Gateway SHALL forward headers matching any configured exact
   `additional_forwarded_headers` rule.
4. THE HTTP_API_Gateway SHALL forward headers matching any configured trailing-`*` prefix rule.
5. THE HTTP_API_Gateway SHALL NOT forward a non-default header that matches no configured exact or
   prefix rule.
6. IF the forwarded metadata lacks `client-name`, THEN THE HTTP_API_Gateway SHALL set it to
   `temporal-server-http`.
7. IF the forwarded metadata lacks `client-version`, THEN THE HTTP_API_Gateway SHALL set it to the
   pinned `TEMPORAL_SERVER_COMPAT` version string.
8. THE Synthetic_gRPC_Request SHALL carry forwarded values as ordinary incoming gRPC metadata so the
   existing authentication, authorization, and service interceptors observe the same request.
9. THE HTTP_API_Gateway SHALL NOT construct a parallel authentication or authorization decision.

### Requirement 8: Host admission policy

**User Story:** As an operator, I want HTTP Host validation before RPC admission, so that an
unexpected virtual host cannot reach Temporal APIs.

#### Acceptance Criteria

1. BEFORE request-body decoding or inner-service invocation, THE HTTP_API_Gateway SHALL compare the
   complete HTTP Host value against the effective allowed-host patterns.
2. A host wildcard pattern SHALL be full-string and case-sensitive; `*` SHALL match any substring,
   and every other character SHALL be treated literally.
3. IF any pattern matches, THEN THE HTTP_API_Gateway SHALL continue request admission.
4. IF no pattern matches, THEN THE HTTP_API_Gateway SHALL return HTTP `403` with exactly
   `{"code": 7, "message": "Host not allowed"}` and SHALL NOT invoke Tonic.
5. WHEN no static policy or live override narrows admission, THE effective policy SHALL allow every
   Host value, matching the v1.31.0 default.
6. IN a conformance build, THE HTTP_API_Gateway SHALL read `frontend.httpAllowedHosts` live for every
   matched request and SHALL use a present JSON string-list override in place of static patterns.
7. IF a present conformance override is malformed, THEN THE HTTP_API_Gateway SHALL log the invalid
   policy and fail closed for that override rather than silently restoring allow-all.

### Requirement 9: Existing gRPC behavior and HTTP response translation

**User Story:** As an HTTP client, I want the response to represent the existing gRPC result, so that
the gateway cannot diverge from Tokeira's authoritative API behavior.

#### Acceptance Criteria

1. WHEN request transcoding succeeds, THE HTTP_API_Gateway SHALL submit exactly one
   Synthetic_gRPC_Request to the full gRPC method path named by the HTTP_Binding.
2. THE Synthetic_gRPC_Request SHALL contain one standard unary gRPC frame carrying the encoded input
   message.
3. WHEN Tonic returns success, THE HTTP_API_Gateway SHALL decode the unary response using the
   method's output descriptor and emit the selected JSON representation with HTTP `200` unless the
   binding contract specifies another successful status.
4. WHEN Tonic returns a gRPC status, THE HTTP_API_Gateway SHALL preserve its numeric code, message,
   and typed `Any` details in a `google.rpc.Status` JSON response.
5. THE HTTP_API_Gateway SHALL map gRPC codes to HTTP status codes using the gRPC-Gateway version
   bundled with Temporal v1.31.0, including `ALREADY_EXISTS` to HTTP `409` and
   `PERMISSION_DENIED` to HTTP `403`.
6. Error responses SHALL use the selected canonical/pretty JSON formatting and content type.
7. A `WorkflowExecutionAlreadyStarted` detail SHALL retain its `runId` field in HTTP JSON.
8. IF the inner response contains no message or more than one unary message frame, THEN THE
   HTTP_API_Gateway SHALL return an internal error and SHALL NOT fabricate a partial success.
9. Canceling the inbound HTTP future SHALL cancel/drop the in-process inner-service future; the
   gateway SHALL NOT spawn detached RPC work.

### Requirement 10: OpenAPI documents

**User Story:** As an HTTP client author, I want machine-readable API documents, so that I can
inspect or generate clients for the pinned Temporal surface.

#### Acceptance Criteria

1. WHEN a caller sends `GET /swagger.json`, THE frontend SHALL return the OpenAPI v2 JSON document
   aligned with `TEMPORAL_PROTO_VERSION`.
2. WHEN a caller sends `GET /openapi.yaml`, THE frontend SHALL return the OpenAPI v3 YAML document
   aligned with `TEMPORAL_PROTO_VERSION`.
3. The v2 response SHALL use the observable v1.31.0 content type `text/plain; charset=utf-8` because
   its handler does not install the OAI media type passed to its route helper and `net/http` sniffs
   the JSON bytes as text.
4. The v3 response SHALL use the observable v1.31.0 content type `text/plain; charset=utf-8` because
   the same handler leaves YAML content-type selection to `net/http` sniffing.
5. Each served document SHALL parse successfully and SHALL describe the HTTP_Bindings derived from
   the same pinned API release.
6. OpenAPI route handling SHALL take precedence over the generic gateway catch-all and SHALL NOT
   invoke WorkflowService or OperatorService.

### Requirement 11: HTTP service metrics

**User Story:** As an operator, I want HTTP calls attributed to their underlying Temporal method, so
that HTTP and gRPC traffic remain distinguishable and observable.

#### Acceptance Criteria

1. WHEN a request has been successfully transcoded and is about to invoke Tonic, THE
   HTTP_API_Gateway SHALL increment `tokeira_edge_http_service_requests_total` exactly once.
2. THE counter SHALL carry `operation` equal to the full gRPC method path, for example
   `/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution`.
3. THE counter SHALL carry `namespace` equal to the decoded top-level request namespace when one is
   present, otherwise the same unknown-namespace label used by existing edge metrics.
4. A host rejection, route miss, OpenAPI request, or request-transcoding failure SHALL NOT increment
   the HTTP_Service_Metric.
5. An admitted request whose inner RPC returns an error SHALL retain its single counter observation.
6. THE Temporal conformance metrics bridge SHALL map the native counter to Temporal's
   `http_service_requests` name without altering corpus assertions.

### Requirement 12: Static and conformance configuration delivery

**User Story:** As a conformance operator, I want the harness to deliver real HTTP policy inputs, so
that tests exercise Tokeira behavior rather than in-process Temporal configuration.

#### Acceptance Criteria

1. THE config crate SHALL model `[policy.http_api]` with `deny_unknown_fields`, documented defaults,
   validation, serialization round-trip coverage, and no environment-variable production override.
2. A missing `[policy.http_api]` section SHALL enable the HTTP API with allow-all hosts and no
   additional non-default forwarded headers.
3. THE conformance override registry SHALL classify `frontend.httpAllowedHosts` as a wired JSON key
   only in the same change that adds the live gateway consult site.
4. THE Temporal fork's Shape-2 harness SHALL add the onebox's existing
   `this-header-forwarded` and `this-header-prefix-forwarded-*` rules to its generated temporary
   `tokeirad` TOML.
5. THE Temporal fork SHALL NOT edit `tests/http_api_test.go` or any other corpus test body.
6. Clearing the live host override between tests SHALL restore the static Gateway_Policy.

### Requirement 13: Architectural and regression invariants

**User Story:** As a Tokeira maintainer, I want HTTP compatibility isolated to translation, so that
adding another wire representation cannot change durable execution correctness.

#### Acceptance Criteria

1. THE feature SHALL make no change to `tokeira-kernel`, `tokeira-runtime`, storage schemas,
   transition encoding, history events, or projection state.
2. THE HTTP_API_Gateway SHALL contain no workflow lifecycle, defaulting, deduplication, retry,
   fencing, or ordering decision that is not already represented by request transcoding or the
   invoked gRPC method.
3. A protobuf request produced from an HTTP binding and the equivalent native gRPC request SHALL
   reach the same existing service implementation and produce equivalent protobuf responses or
   statuses.
4. Existing native gRPC and gRPC-Web focused tests SHALL remain green.
5. Existing caller-facing Nexus HTTP tests SHALL remain green, and ordinary workflow HTTP paths
   SHALL no longer be misclassified as Nexus operations.
6. The obsolete `/api/v1/{service}/{method}` `HttpProxy` placeholder SHALL be removed or explicitly
   superseded so Tokeira documents only the real annotated Temporal HTTP surface.
7. Two consecutive clean `TestHttpApiTestSuite` conformance runs SHALL pass all leaves before Tier
   9.43 is recorded green.
