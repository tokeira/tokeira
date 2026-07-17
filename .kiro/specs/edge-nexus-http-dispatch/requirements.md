# Requirements Document: Edge Nexus HTTP Dispatch

## Introduction

This feature adds Temporal's caller-facing Nexus HTTP API to Tokeira's compatibility edge. It
accepts StartOperation and CancelOperation requests on the same frontend address as WorkflowService,
resolves either a namespace/task-queue route or a registered endpoint route, dispatches the request
to a Nexus worker through `NexusTaskBroker`, and translates the worker outcome back to the Nexus HTTP
protocol.

Observable behaviour targets Temporal server v1.31.0. The HTTP protocol implementation is
ground-truthed to `service/frontend/nexus_http_handler.go`, `service/frontend/nexus_handler.go`, and
`common/nexus/nexusrpc @ v1.31.0`. Worker-facing token and correlation behaviour is owned by the
sibling `edge-nexus-task-transport` spec. Endpoint CRUD and the live endpoint registry are owned by
`api-conformance-nexus-admin`.

Authentication and authorization are owned by the approved `authorization-foundation` spec. This
feature supplies the Nexus-HTTP-specific call target and error translation after route resolution
and before broker publication. The Temporal corpus's process-local `Host().SetOnAuthorize` hook is
delivered through a conformance-only namespace-scoped callback bridge; it is absent from production
builds and does not weaken the configured JWT/AWS-IAM path.

## Glossary

- **Nexus_HTTP_Handler**: The edge-owned HTTP service that recognizes and processes Nexus operation
  routes on the frontend listener.
- **Namespace_Route**: Dispatch through a namespace and Nexus task queue supplied in the URL.
- **Endpoint_Route**: Dispatch through a Nexus endpoint ID resolved from the live endpoint registry.
- **Resolved_Target**: A canonical namespace ID/name, task queue, optional endpoint ID, and endpoint
  name produced by route preprocessing.
- **StartOperation**: A Nexus request that asks a worker to start an operation and waits for a sync,
  async, unsuccessful, handler-error, or timeout outcome.
- **CancelOperation**: A Nexus request that asks a worker to cancel a previously-started operation.
- **NexusTaskBroker**: The runtime broker shared with `edge-nexus-task-transport`; it makes work
  available to `PollNexusTaskQueue` and correlates one worker response with one waiting dispatch.
- **Preprocess_Error**: A failure before a valid namespace/operation context is established, such as
  malformed routing data, namespace length rejection, or endpoint lookup failure.
- **Handler_Error**: A Nexus protocol error carrying a type, message/cause, and retry behaviour.
- **Operation_Error**: A failed or canceled operation outcome, distinct from a handler failure.
- **Temporal_Failure_Support**: Compatibility mode selected by
  `Temporal-Nexus-Failure-Support: true`, causing modern Temporal failure envelopes to be preserved.
- **Failure_Source**: The response header `Temporal-Nexus-Failure-Source`, set to `worker` when the
  returned failure originated from a Nexus worker.

## Target State

- The frontend listener multiplexes Temporal gRPC/gRPC-Web traffic and both Nexus HTTP route
  families without requiring a second public port.
- Namespace and endpoint routes produce the same worker request and HTTP response semantics after
  target resolution.
- StartOperation and CancelOperation synchronously dispatch through `NexusTaskBroker`, using the
  protobuf task-token and broker-held correlation contract from `edge-nexus-task-transport`.
- Request bodies, content metadata, callback data, links, request/operation timeouts, headers, and
  failure-capability flags survive the HTTP-to-worker translation required by v1.31.0.
- Sync success, async acceptance, operation failure/cancellation, worker handler errors, and dispatch
  timeout map back to the v1.31.0 Nexus HTTP response contract.
- Nexus-specific and service telemetry metrics record one terminal outcome per admitted operation.
- Authentication and authorization use the shared configured foundation. Nexus denials and
  authorizer errors are translated to the Nexus HTTP protocol exactly as v1.31.0 does.
- Conformance builds can route `Host().SetOnAuthorize` through a namespace-scoped callback bridge,
  allowing parallel dedicated-cluster corpus leaves to exercise their exact Go closure without a
  process-global race.

## Evidence From Current Code

- **Routes:** `common/nexus/routes.go @ v1.31.0` defines
  `namespaces/{namespace}/task-queues/{task_queue}/nexus-services` and
  `nexus/endpoints/{endpoint}/services`.
- **Route preprocessing:** `service/frontend/nexus_http_handler.go:101-285 @ v1.31.0` percent-decodes
  variables, applies namespace-name validation, resolves endpoint IDs, limits request bodies, and
  records `nexus_request_preprocess_errors`.
- **Request protocol:** `common/nexus/nexusrpc/server.go` and `api.go @ v1.31.0` define POST routing,
  Start/Cancel suffixes, callback/query/header parsing, link parsing, timeout parsing, response status
  codes, failure JSON, and retry headers.
- **Start/Cancel dispatch behaviour:** `service/frontend/nexus_handler.go:355-709 @ v1.31.0`
  resolves namespace state, builds `temporal.api.nexus.v1.Request`, waits synchronously for the
  selected worker outcome, and maps it to Nexus HTTP. Tokeira preserves those observable invariants
  through its runtime delivery broker; the upstream service topology is not adopted.
- **Limits:** `common/rpc/grpc.go:34-37 @ v1.31.0` caps an HTTP request body at 2 MiB;
  `common/dynamicconfig/constants.go:316-320 @ v1.31.0` sets the payload error limit to 2 MiB.
- **Metrics:** `service/frontend/nexus_handler.go:92-111 @ v1.31.0` records `nexus_requests`,
  `nexus_latency`, and normal service telemetry; namespace-not-found is recorded with its explicit
  outcome before returning.
- **Current Tokeira gap:** `apps/tokeirad/src/lib.rs` enables HTTP/1 on Tonic but mounts only gRPC,
  gRPC-Web, and CORS services on the frontend listener. `crates/tokeira-edge` has an inbound
  completion-callback handler but no caller-facing Nexus Start/Cancel HTTP handler.
- **Existing dependencies:** `crates/tokeira-runtime/src/nexus.rs` provides the broker and live
  endpoint store; `crates/tokeira-edge/src/namespace_cache.rs` provides namespace lookup; the
  `edge-nexus-task-transport` correction defines protobuf tokens and outstanding correlation.

## HTTP Contract Policy

### Route Surface

| Surface | Target policy | Error if invalid | Side-effect impact |
|---|---|---|---|
| `/namespaces/{namespace}/task-queues/{task_queue}/nexus-services/{service}/{operation}` | Percent-decode all variables and dispatch directly to the named namespace/task queue | Invalid escaping → `400 BAD_REQUEST`; namespace too long → `400` `Namespace length exceeds limit.` | Preprocess rejection does not publish a task |
| `/nexus/endpoints/{endpoint}/services/{service}/{operation}` | Resolve endpoint ID through the live registry; only Worker targets dispatch locally | Missing endpoint → `404` `nexus endpoint not found`; invalid target → `400` `invalid endpoint target` | Preprocess rejection does not publish a task |
| `POST .../{service}/{operation}` | StartOperation | Wrong method or malformed suffix → Nexus handler failure | At most one broker dispatch |
| `POST .../{service}/{operation}/cancel` | CancelOperation; operation token comes from `Nexus-Operation-Token`, falling back to `?token=` | Missing token → `400` `missing operation token` | At most one broker dispatch |
| `POST .../{service}/{operation}/{operation_token}/cancel` | Accept the v1.31.0 deprecated token-in-path form | Invalid escaping → `400` | At most one broker dispatch |
| Unrelated frontend paths | Continue to the existing gRPC/gRPC-Web service | Existing service behaviour | No Nexus metric or broker effect |

### StartOperation Request

| Input | Target policy | Error if invalid | Worker request effect |
|---|---|---|---|
| Body + `Content-*` headers | Convert to one Temporal `Payload`, including content metadata | Invalid content → `400` `invalid input`; encoded payload over 2 MiB → `400` `input exceeds size limit` | Populates `StartOperationRequest.payload` |
| `Nexus-Request-Id` | Preserve verbatim | No additional edge rejection | Populates `request_id` |
| `?callback=` | Preserve callback URL verbatim | Protocol parser errors use `400` | Populates `callback` |
| `Nexus-Callback-*` headers | Strip prefix and preserve as callback header map | Multiple values use the first, matching v1.31.0 | Populates `callback_header` |
| `Nexus-Link` | Parse every encoded link and preserve URL/type | Invalid link → `400` `invalid "nexus-link" header` | Populates `links` |
| General HTTP headers | Lower-case and preserve first values, excluding `Content-*` and `Nexus-Callback-*` | None at this layer | Populates request `header` |
| `Temporal-Nexus-Failure-Support` | Set caller capability when the value is exactly `true`; do not forward it as a worker header | None | Populates `capabilities.temporal_failure_responses` |
| `Request-Timeout` | Parse Nexus duration and bound synchronous dispatch | Invalid duration → `400` `invalid request timeout header` | Worker receives a remaining/buffered timeout header |
| Request arrival time | Capture once during preprocessing | None | Populates `scheduled_time` |
| Endpoint route metadata | Include resolved endpoint name | None | Populates `Request.endpoint`; namespace route leaves it empty |

### CancelOperation Request

| Input | Target policy | Error if invalid | Worker request effect |
|---|---|---|---|
| Service and operation path variables | Percent-decode and preserve | Invalid escaping → `400` | Populates `service` and `operation` |
| Operation token | On the current `/cancel` route, prefer `Nexus-Operation-Token` then `?token=`. The deprecated token-in-path form is a separate route whose path token wins outright. | Missing token on the current route → `400` `missing operation token` | Populates both `operation_token` and the v1.31.0 compatibility `operation_id` |
| General headers | Lower-case and preserve first values | None at this layer | Populates request `header` |
| `Temporal-Nexus-Failure-Support` | Set caller capability when exactly `true` | None | Populates `capabilities.temporal_failure_responses` |
| `Request-Timeout` | Parse and bound synchronous dispatch | Invalid duration → `400` | Worker receives a remaining/buffered timeout header |

### HTTP Response Mapping

| Worker/edge outcome | HTTP status and body | Required headers |
|---|---|---|
| Start sync success | `200`; worker payload bytes + `Content-*` metadata | Encoded `Nexus-Link` values when supplied |
| Start async success | `201`; JSON `{token, state:"running"}` | `Content-Type: application/json`; encoded links |
| Start operation failed/canceled | `424`; JSON Nexus failure | `Nexus-Operation-State`; `Content-Type: application/json`; worker failure source |
| Cancel success | `202`; empty body | None beyond common headers |
| Worker handler error | JSON Nexus failure; `BAD_REQUEST`→400, `REQUEST_TIMEOUT`→408, `CONFLICT`→409, `UNAUTHENTICATED`→401, `UNAUTHORIZED`→403, `NOT_FOUND`→404, `RESOURCE_EXHAUSTED`→429, `INTERNAL`→500, `NOT_IMPLEMENTED`→501, `UNAVAILABLE`→503, `UPSTREAM_TIMEOUT`→520 | `Nexus-Request-Retryable` when specified; worker failure source |
| Dispatch/request timeout | `520`; JSON failure with `upstream timeout` | Worker failure source |
| Empty/malformed worker outcome | `500`; JSON internal HandlerError | Worker failure source |

## Requirements

### Requirement 1: Same-Listener Nexus Routing

**User Story:** As a Nexus caller, I want Nexus HTTP requests accepted at the Temporal frontend
address, so that one advertised endpoint supports both Temporal SDK and Nexus clients.

#### Acceptance Criteria

1. WHEN an HTTP/1 request matches a Namespace_Route, THE frontend service SHALL route it to the
   Nexus_HTTP_Handler.
2. WHEN an HTTP/1 request matches an Endpoint_Route, THE frontend service SHALL route it to the
   Nexus_HTTP_Handler.
3. IF an HTTP request does not match either Nexus route prefix, THEN THE frontend service SHALL
   preserve the existing gRPC/gRPC-Web routing result.
4. THE frontend service SHALL serve Nexus HTTP and Temporal gRPC traffic on the same bound address.

### Requirement 2: Route Preprocessing and Target Resolution

**User Story:** As an operator, I want every Nexus route resolved before task dispatch, so that
invalid namespaces and endpoints cannot enqueue work.

#### Acceptance Criteria

1. WHEN a Namespace_Route is received, THE Nexus_HTTP_Handler SHALL percent-decode its namespace and
   task-queue variables.
2. IF a Namespace_Route namespace exceeds the v1.31.0 maximum ID length, THEN THE
   Nexus_HTTP_Handler SHALL return `400` with `Namespace length exceeds limit.`
3. IF a Namespace_Route namespace is absent from the namespace cache, THEN THE Nexus_HTTP_Handler
   SHALL return `404` with `namespace not found: "<namespace>"`.
4. WHEN an Endpoint_Route is received, THE Nexus_HTTP_Handler SHALL percent-decode and resolve its
   endpoint ID through the live endpoint store.
5. IF the endpoint ID is absent, THEN THE Nexus_HTTP_Handler SHALL return `404` with
   `nexus endpoint not found`.
6. WHEN the endpoint target is Worker, THE Nexus_HTTP_Handler SHALL resolve its namespace ID to the
   current namespace name and task queue.
7. IF the endpoint target is not Worker, THEN THE Nexus_HTTP_Handler SHALL return `400` with
   `invalid endpoint target`.
8. IF preprocessing fails, THEN THE Nexus_HTTP_Handler SHALL NOT publish a Nexus task.

### Requirement 3: StartOperation HTTP Translation

**User Story:** As a Nexus caller, I want my HTTP request represented faithfully to the worker, so
that operation handlers receive the same inputs they would under Temporal v1.31.0.

#### Acceptance Criteria

1. WHEN a valid StartOperation HTTP request is admitted, THE Nexus_HTTP_Handler SHALL construct one
   `temporal.api.nexus.v1.Request` StartOperation variant.
2. THE constructed StartOperation request SHALL preserve service, operation, request ID, callback
   URL, callback headers, links, and payload.
3. THE constructed outer Nexus request SHALL preserve eligible general headers with lower-case keys.
4. THE constructed outer Nexus request SHALL record the preprocessing arrival time as
   `scheduled_time`.
5. WHERE dispatch used an Endpoint_Route, THE constructed outer Nexus request SHALL carry the
   resolved endpoint name.
6. WHERE dispatch used a Namespace_Route, THE constructed outer Nexus request SHALL carry an empty
   endpoint name.
7. IF the input cannot be converted to a Temporal Payload, THEN THE Nexus_HTTP_Handler SHALL return
   `400` with `invalid input`.
8. IF the converted payload exceeds 2 MiB, THEN THE Nexus_HTTP_Handler SHALL return `400` with
   `input exceeds size limit`.
9. IF a `Nexus-Link` value is invalid, THEN THE Nexus_HTTP_Handler SHALL return `400` with
   `invalid "nexus-link" header`.
10. IF `Request-Timeout` is invalid, THEN THE Nexus_HTTP_Handler SHALL return `400` with
    `invalid request timeout header`.

### Requirement 4: CancelOperation HTTP Translation

**User Story:** As a Nexus caller, I want cancellation requests delivered to the same worker target,
so that an asynchronously-started operation can be canceled.

#### Acceptance Criteria

1. WHEN a valid CancelOperation HTTP request is admitted, THE Nexus_HTTP_Handler SHALL construct one
   `temporal.api.nexus.v1.Request` CancelOperation variant.
2. THE constructed CancelOperation request SHALL preserve service, operation, operation token, and
   eligible general headers.
3. THE constructed CancelOperation request SHALL populate the compatibility `operation_id` with the
   operation-token value.
4. IF neither a header, query, nor deprecated path operation token is present, THEN THE
   Nexus_HTTP_Handler SHALL return `400` with `missing operation token`.
5. IF `Request-Timeout` is invalid, THEN THE Nexus_HTTP_Handler SHALL return `400` with
   `invalid request timeout header`.

### Requirement 5: Synchronous Worker Dispatch and Correlation

**User Story:** As a Nexus caller, I want the HTTP request to await the selected worker's response,
so that the HTTP result represents that exact task execution.

#### Acceptance Criteria

1. WHEN StartOperation or CancelOperation translation succeeds, THE Nexus_HTTP_Handler SHALL publish
   one task to `NexusTaskBroker` under the Resolved_Target namespace ID and task queue.
2. WHEN a task is published, THE Edge_Layer SHALL register its HTTP response waiter and THE
   NexusTaskBroker SHALL register only the waiter's opaque ID before the task becomes visible to
   pollers.
3. WHEN the worker completes or fails the task, THE NexusTaskBroker SHALL atomically return that
   opaque route and THE Edge_Layer SHALL deliver the public worker outcome to exactly one waiting
   HTTP request.
4. IF the HTTP request is canceled before a worker response, THEN THE Edge_Layer SHALL remove its
   waiter and THE NexusTaskBroker SHALL remove only that task's disposable delivery correlation
   without resolving another task.
5. IF the effective request deadline expires before a worker response, THEN THE Nexus_HTTP_Handler
   SHALL return a Nexus upstream-timeout HandlerError.
6. WHEN a timeout is propagated to the worker, THE Edge_Layer's poll translation SHALL expose the
   remaining request timeout after applying the v1.31.0 dispatch buffer.

### Requirement 6: Worker Outcome to HTTP Response

**User Story:** As a Nexus caller, I want worker outcomes translated to the Nexus HTTP protocol, so
that Nexus SDKs decode success and failure without Temporal-specific knowledge.

#### Acceptance Criteria

1. WHEN StartOperation returns sync success, THE Nexus_HTTP_Handler SHALL return `200` with the
   worker payload and content metadata.
2. WHEN StartOperation returns async success, THE Nexus_HTTP_Handler SHALL return `201` with running
   operation JSON containing the worker operation token.
3. WHEN StartOperation returns an operation failure, THE Nexus_HTTP_Handler SHALL return `424` with
   the operation state and serialized Nexus failure.
4. WHEN CancelOperation succeeds, THE Nexus_HTTP_Handler SHALL return `202` with an empty body.
5. WHEN a worker returns a HandlerError, THE Nexus_HTTP_Handler SHALL map its type to the v1.31.0
   HTTP status.
6. WHERE a worker HandlerError specifies retry behaviour, THE Nexus_HTTP_Handler SHALL emit
   `Nexus-Request-Retryable` with the matching boolean value.
7. WHEN a response failure originated from the worker, THE Nexus_HTTP_Handler SHALL emit
   `Temporal-Nexus-Failure-Source: worker`.
8. WHERE Temporal_Failure_Support is true, THE Nexus_HTTP_Handler SHALL preserve the modern Temporal
   failure envelope.
9. WHERE Temporal_Failure_Support is false, THE Nexus_HTTP_Handler SHALL emit the v1.31.0 legacy
   unwrapped failure shape.
10. WHEN a worker supplies valid response links, THE Nexus_HTTP_Handler SHALL emit equivalent
    `Nexus-Link` headers.
11. IF a worker outcome is empty or structurally invalid, THEN THE Nexus_HTTP_Handler SHALL return
    `500` as an internal HandlerError.

### Requirement 7: Nexus Metrics and Telemetry

**User Story:** As an operator, I want Nexus HTTP outcomes represented in standard metrics, so that
admission and worker failures are observable.

#### Acceptance Criteria

1. WHEN preprocessing rejects a request, THE Nexus_HTTP_Handler SHALL increment
   `nexus_request_preprocess_errors` once.
2. WHEN namespace resolution fails after a valid Namespace_Route is established, THE
   Nexus_HTTP_Handler SHALL increment `nexus_requests` once with outcome `namespace_not_found`.
3. WHEN an operation reaches a terminal HTTP outcome, THE Nexus_HTTP_Handler SHALL increment
   `nexus_requests` once with namespace, method, outcome, and endpoint tags.
4. WHEN an operation reaches a terminal HTTP outcome, THE Nexus_HTTP_Handler SHALL record
   `nexus_latency` once with the same operation dimensions.
5. WHEN an operation reaches a terminal HTTP outcome, THE Nexus_HTTP_Handler SHALL record normal
   service telemetry under `StartNexusOperation` or `CancelNexusOperation`.
6. WHERE dispatch used a Namespace_Route, THE Nexus metrics SHALL use `_unknown_` as the endpoint
   tag.
7. WHERE dispatch used an Endpoint_Route, THE Nexus metrics SHALL use the resolved endpoint name as
   the endpoint tag.

### Requirement 8: Nexus HTTP Authentication and Authorization

**User Story:** As the conformance owner, I want Nexus HTTP authorization to use the shared auth
foundation and preserve Temporal's Nexus error protocol, so that HTTP callers and the functional
corpus observe the same decisions as gRPC callers.

#### Acceptance Criteria

1. WHEN preprocessing produces a Resolved_Target, THE Nexus_HTTP_Handler SHALL authenticate the
   request and authorize it after target construction and before namespace-state validation or
   broker publication (`service/frontend/nexus_handler.go:156-181 @ v1.31.0`).
2. WHERE dispatch uses a Namespace_Route, THE authorization call target SHALL use API name
   `DispatchNexusTaskByNamespaceAndTaskQueue`, the resolved namespace name, and no endpoint name.
3. WHERE dispatch uses an Endpoint_Route, THE authorization call target SHALL use API name
   `DispatchNexusTaskByEndpoint`, the endpoint target's resolved namespace name, and the registered
   endpoint name (`nexus_handler.go:161-165 @ v1.31.0`).
4. IF the authorizer denies with a non-empty reason, THEN THE Nexus_HTTP_Handler SHALL return an
   `UNAUTHORIZED` HandlerError whose message is `permission denied: <reason>` and SHALL record
   outcome `unauthorized`.
5. IF the authorizer denies without a reason or returns an error while
   `frontend.exposeAuthorizerErrors` is false, THEN THE Nexus_HTTP_Handler SHALL return an
   `UNAUTHORIZED` HandlerError with message `permission denied` and SHALL record outcome
   `unauthorized`.
6. IF the authorizer returns an error while `frontend.exposeAuthorizerErrors` is true, THEN THE
   Nexus_HTTP_Handler SHALL convert that error to the corresponding Nexus HandlerError and SHALL
   record outcome `internal_auth_error` (`nexus_handler.go:168-178 @ v1.31.0`).
7. WHERE the `conformance` feature is enabled and the harness registers a scoped
   `Host().SetOnAuthorize` callback, THE server SHALL route Nexus HTTP authorization by resolved
   namespace to that callback and SHALL include API name, namespace, and endpoint name; callbacks
   for parallel namespaces SHALL NOT overwrite or observe one another.
8. WHERE no scoped conformance callback exists, THE Nexus_HTTP_Handler SHALL use the production
   authenticator/authorizer configured by `authorization-foundation`.
9. IN a build without the `conformance` feature, THE authorization callback bridge SHALL be absent.

## Iteration and Feedback Notes

- Tier 7.36 (`TestNexusAPIValidationTestSuite`) exercises route preprocessing, payload limits, exact
  errors, protobuf Respond tokens, metrics, and the namespace-scoped `Host().SetOnAuthorize` bridge.
- Tier 7.38 (`tests/nexus_api_test.go`) exercises the full Start/Cancel dispatch and response contract
  in both legacy and Temporal-failure modes. Implementing only Tier 7.36 rejection paths is not an
  acceptable substitute for this target state.
- Multi-cluster request forwarding is not introduced here. Tokeira's single-cluster topology has no
  namespace-not-active forwarding path; this is an internal topology difference until multi-cluster
  conformance is brought into scope.
