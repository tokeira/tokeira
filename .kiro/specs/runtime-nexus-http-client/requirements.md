# Requirements Document

## Introduction

tokeirad wires `NoopNexusHttpClient`, whose `start_operation` always errors, so every
External-target Nexus operation fails immediately in the live server — no operation ever starts or
completes over HTTP. This feature provides a real outbound Nexus HTTP client (the `NexusHttpClient`
trait has only a `Noop` and a test `Mock` implementation today) so External Nexus operations dispatch
to their endpoint URL and their sync/async/failure outcomes flow back through the existing
`NexusResolution` path. Ground-truthed to the Nexus HTTP wire format used by Temporal v1.31.0
(`common/nexus/nexusrpc/client.go` and `handle.go` @ v1.31.0). No Rust Nexus-protocol crate exists;
the workspace already depends on `reqwest`.

## Glossary

- **External endpoint** — a Nexus endpoint whose target is an HTTP URL (`EndpointTarget.External`).
- **Operation token** — handler-issued identifier for an async (running) operation.
- **Sync completion** — handler returns the result inline (HTTP 200).
- **Async start** — handler accepts and will complete later (HTTP 201, `OperationStateRunning`).
- **Links** — `Nexus-Link` header entries the handler returns, recorded on the operation's history
  events.
- **System endpoint** — the internal `__temporal_system` endpoint; handled in-process, not over HTTP.

## Requirements

### Requirement 1: Outbound StartOperation over HTTP

**User Story:** As a workflow scheduling a Nexus operation on an External endpoint, I want the server to
actually call the handler so the operation can start or complete.

#### Acceptance Criteria

1. WHEN an External Nexus operation is dispatched THEN the client SHALL `POST` to
   `{endpoint_url}/{service}/{operation}` with the input payload as the body.
2. WHEN the handler responds 200 THEN the client SHALL return a sync-completed result carrying the
   response body and any response links.
3. WHEN the handler responds 201 with `OperationStateRunning` and an operation token THEN the client
   SHALL return async-accepted carrying the operation token and any response links.
4. WHEN the handler responds with an operation-unsuccessful status THEN the client SHALL return a
   sync-failure carrying the failure.
5. WHEN the request carries a request id, operation-timeout, caller links, and a callback URL + token
   THEN the client SHALL send them as the corresponding Nexus headers/query, per the v1.31.0 wire
   format.

### Requirement 2: Outbound CancelOperation over HTTP

**User Story:** As a workflow cancelling a started Nexus operation, I want the server to call the
handler's cancel endpoint.

#### Acceptance Criteria

1. WHEN a started External Nexus operation is cancelled THEN the client SHALL `POST` to
   `{endpoint_url}/{service}/{operation}/cancel` with the operation token header.
2. WHEN the cancel responds success THEN the client SHALL report success; WHEN it responds failure THEN
   the client SHALL report failure (mapping unchanged from the current trait contract).

### Requirement 3: Outcomes flow through the existing resolution path

**User Story:** As the runtime, I want HTTP outcomes mapped onto the existing `NexusResolution` so the
kernel records the right events.

#### Acceptance Criteria

1. THE SYSTEM SHALL map sync-completed → `NexusResolution::Completed`, async-accepted →
   `NexusResolution::Started`, sync-failure and transport/parse error → `NexusResolution::Failed`,
   preserving the current mapping.
2. THE SYSTEM SHALL carry handler-returned links onto the `NexusOperationStarted` and
   `NexusOperationCompleted` history events.

### Requirement 4: Wire the real client into tokeirad

**User Story:** As an operator, I want the live server to use the real client.

#### Acceptance Criteria

1. THE SYSTEM SHALL replace `NoopNexusHttpClient` in tokeirad with the real client.
2. THE default unit/integration test suite SHALL NOT require a live external HTTP endpoint.

### Requirement 5: Scoped boundaries

**User Story:** As a maintainer, I want this feature bounded so deferred surfaces are explicit.

#### Acceptance Criteria

1. THE SYSTEM SHALL NOT route the `__temporal_system` endpoint through the HTTP client (it is internal,
   handled separately and tracked elsewhere).
2. THE SYSTEM SHALL send a callback URL + token on StartOperation for wire-faithfulness, but hosting the
   inbound completion-callback endpoint remains out of scope (tracked as the deferred Nexus
   completion-callback surface).
3. THE SYSTEM SHALL perform a single start attempt per dispatch; retry/backoff of failed attempts is
   tracked separately (`nexus-retry-policy`).
