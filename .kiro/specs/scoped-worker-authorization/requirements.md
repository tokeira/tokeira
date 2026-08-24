# Requirements Document: Scoped Worker Authorization

## Introduction

This spec defines a Tokeira-native credential scope for untrusted Temporal workers. A scoped
worker credential authorizes one namespace, a non-empty allow-list of normal task queues, and
one exact Worker Deployment Version `(deployment_name, build_id)`. It permits only the public
WorkflowService operations required to poll, execute, complete, heartbeat, shut down, and
describe those queues. It does not grant general namespace read or write access.

The feature closes the security dependency tracked by
[`tokeira/tokeira#29`](https://github.com/tokeira/tokeira/issues/29) and by the
[`worker-compute-controller`](../worker-compute-controller/requirements.md) spec. It allows a
compute provider such as the sibling worker-compute provider to launch an untrusted worker that uses an ordinary Temporal SDK
and bearer metadata without giving the guest a namespace-wide Writer role.

This is an extension of, not a replacement for, the
[`authorization-foundation`](../authorization-foundation/requirements.md):

- Temporal v1.31.0 remains the authority for the public RPC shapes, default role model,
  authorization classification, and error behavior (`AGENTS.md §8`).
- The existing unscoped JWT and AWS IAM paths retain their v1.31.0-compatible behavior.
- `WorkerScope` is a Tokeira-owned attenuation applied only when a verified identity carries or
  maps to the structured scope defined here.
- Tokeira does not become an identity provider. An operator's IdP signs JWTs, or an AWS client
  supplies a presigned STS identity; Tokeira verifies and maps that identity.

The relevant v1.31.0 ground truth is:

- `common/authorization/default_authorizer.go`, `roles.go`, and
  `common/api/metadata.go @ v1.31.0` — the stock numeric role model and RPC classifications.
  In particular, stock `Worker=1` alone grants no namespace access under the default authorizer.
- `common/authorization/interceptor.go @ v1.31.0` — authorization precedes handler dispatch,
  and cross-namespace commands are separately authorized.
- `service/frontend/workflow_handler.go @ v1.31.0` — worker poll, response, heartbeat, and
  shutdown request behavior.
- Vendored `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` — poll
  deployment options, task tokens, Worker heartbeat requests, shutdown fields, and
  `DescribeTaskQueue`.
- Vendored `proto/upstream/temporal/api/deployment/v1/message.proto` —
  `WorkerDeploymentOptions` and exact Deployment-Version identity.
- Vendored `proto/upstream/temporal/api/worker/v1/message.proto` — Worker heartbeat queue and
  Deployment-Version identity.

The implementation must preserve Tokeira's architecture. Authorization belongs to
`tokeira-auth` and the compatibility edge. Runtime participation is limited to read-only
resolution of authoritative task origin where the edge cannot decide from trusted data. This
feature introduces no kernel command, kernel state, history event, projection dependency,
delivery-queue correctness claim, or authorization I/O in the kernel.

## Glossary

- **Worker_Scope:** the structured attenuation
  `(namespace, task_queues, deployment_name, build_id)` carried by authenticated claims.
- **Scoped_Identity:** authenticated claims with one effective Worker_Scope.
- **Ordinary_Identity:** authenticated claims without a Worker_Scope; authorized by the existing
  v1.31.0-compatible role model.
- **Normal_Task_Queue:** the stable application task-queue name configured on the Worker.
- **Sticky_Task_Queue:** an ephemeral Worker-specific Workflow Task Queue whose public
  `TaskQueue.normal_name` identifies its Normal_Task_Queue.
- **Deployment_Version:** the exact, case-sensitive pair `(deployment_name, build_id)` from
  `WorkerDeploymentOptions` in `VERSIONED` mode.
- **Allowed_Queue:** a Normal_Task_Queue named in the Worker_Scope.
- **Worker_Operation:** one of the fixed RPCs in the operation matrix below.
- **Task_Origin:** the server-authoritative namespace, Normal_Task_Queue, task kind, and
  Deployment_Version from which a polled task was issued.
- **Own_Task:** a task whose Task_Origin matches the Scoped_Identity and whose server-issued
  task token remains valid under the existing fencing rules.
- **By_ID_Response:** an activity completion, failure, cancellation, or heartbeat addressed by
  namespace/workflow/activity identifiers instead of a server-issued task token.
- **Presence_Activated:** no global feature switch exists; ordinary behavior is unchanged until
  an authenticated identity supplies or maps to a Worker_Scope.
- **Scope_Source:** either the fixed JWT `tokeira_worker_scope` claim or a configured
  subject/ARN Worker-Scope rule.
- **Attenuation:** a maximum authority boundary. Ordinary roles never widen a Worker_Scope.

## Target State

A platform can issue a short-lived JWT containing:

```json
{
  "iss": "https://idp.example.test",
  "sub": "worker-instance-123",
  "aud": "tokeira",
  "exp": 1785149999,
  "tokeira_worker_scope": {
    "version": 1,
    "namespace": "payments",
    "task_queues": ["payments-worker"],
    "deployment_name": "payments",
    "build_id": "2026-07-28.1"
  }
}
```

Alternatively, a configured JWT subject rule or AWS IAM ARN rule maps the verified identity to
the same Worker_Scope. The Worker passes the resulting bearer through the standard Temporal SDK
authorization metadata supplier. No SDK fork, custom RPC, task payload field, or upstream proto
extension is required.

The credential can poll Workflow, Activity, and Nexus tasks for `payments-worker` only when the
poll declares the exact Deployment_Version. It can answer only tasks actually issued under that
scope, send scoped Worker heartbeats, shut down its own polls, and use
`DescribeTaskQueue` for readiness. It cannot start, signal, query, update, describe, list,
terminate, reset, or otherwise operate Workflow Executions; cannot use activity By-ID response
RPCs; cannot poll an unversioned or different-version queue; and cannot access another
namespace or task queue.

The absence of a Worker_Scope leaves the existing configured JWT/STS and permissive-default
behavior byte-for-byte unchanged. The presence of a Worker_Scope caps the entire identity even
when the same token also contains ordinary namespace roles.

## Evidence From Current Code

- `crates/tokeira-auth/src/lib.rs` models `Claims` with system and namespace roles, verifies JWT
  issuer/JWKS/audience, maps `permissions`, and maps JWT subjects and AWS IAM ARNs through
  namespace-role grants. It has no task-queue or Deployment-Version scope.
- `crates/tokeira-auth/src/lib.rs` passes only
  `(api_name, namespace, access_classification)` to `DefaultAuthorizer`. That target is
  sufficient for v1.31.0 parity but cannot decide a scoped Worker request.
- `crates/tokeira-config/src/lib.rs` exposes JWT issuer grants and AWS IAM grants only as
  `namespace:role` strings. It has no structured Worker-Scope configuration.
- `crates/tokeira-edge/src/interceptors.rs` faithfully classifies worker polls and responses as
  namespace Write and `DescribeTaskQueue` as ReadOnly, matching
  `common/api/metadata.go @ v1.31.0`. It authorizes before resolving task-queue or task-token
  targets.
- `crates/tokeira-edge/src/task_token.rs` wraps runtime fencing tokens and namespace identity.
  Workflow and Activity public task tokens do not currently carry trusted queue or
  Deployment-Version origin.
- `crates/tokeira-runtime/src/nexus.rs` includes namespace and task queue in a Nexus token while
  keeping version correlation private to the broker.
- `crates/tokeira-edge/src/grpc/workflow_service.rs` has Activity, Workflow, Nexus poll/response,
  Worker heartbeat, shutdown, and `DescribeTaskQueue` seams. Its CHASM standalone-activity poll
  fast path can currently claim a task before the shared inner poll admission.
- Vendored poll requests carry `WorkerDeploymentOptions`. Workflow and Activity sticky polling
  uses a dynamic sticky name plus `TaskQueue.normal_name`; requiring the dynamic name in static
  credentials would be unusable and would not identify the application queue.
- `RecordWorkerHeartbeatRequest` contains multiple Worker heartbeats. Each heartbeat names a task
  queue and Deployment Version. `PollNexusTaskQueueRequest` can piggyback multiple Worker
  heartbeats. `ShutdownWorkerRequest` names the normal/sticky queues and may carry a Worker
  heartbeat.
- Activity By-ID response RPCs are implemented and share namespace-Write classifications with
  token response RPCs. A namespace-only decision therefore grants more authority than Issue #29
  permits.
- The sibling worker-compute provider's contract requires exact namespace, fleet-version, task-queue, and
  operation grants for an untrusted Firecracker guest. Its readiness check uses
  `DescribeTaskQueue` and matches an exact `(deployment_name, build_id)` PollerInfo.

## Contract Policy

### Fixed JWT Claim

The claim name is a protocol constant, not a configuration knob:
`tokeira_worker_scope`.

| Field | Type | Policy |
|---|---|---|
| `version` | integer | Required and exactly `1` |
| `namespace` | string | Required, non-blank, exact and case-sensitive |
| `task_queues` | array of strings | Required, non-empty, exact Normal_Task_Queue names; duplicates invalid |
| `deployment_name` | string | Required, non-blank, exact and case-sensitive |
| `build_id` | string | Required, non-blank, exact and case-sensitive |

Unknown fields are rejected. Wildcards are not supported in any scope field. Scope validation
happens after JWT signature, issuer, audience, and lifetime validation but before authorization.

### Static Mapping

The existing authorization configuration gains two structured lists:

```toml
[[policy.authorization.jwt.issuers.worker_scopes]]
match_sub = "system:serviceaccount:workers:payments-*"
namespace = "payments"
task_queues = ["payments-worker"]
deployment_name = "payments"
build_id = "2026-07-28.1"

[[policy.authorization.aws_iam.worker_scopes]]
match_arn = "arn:aws:sts::123456789012:assumed-role/payments-worker-*"
namespace = "payments"
task_queues = ["payments-worker"]
deployment_name = "payments"
build_id = "2026-07-28.1"
```

The pattern grammar remains the full-string, case-sensitive, `*`-only grammar defined by
`authorization-foundation`. Structured scope rules do not grant an ordinary namespace role.

### Fixed Operation Matrix

| RPC | Scoped decision | Additional condition |
|---|---|---|
| `PollWorkflowTaskQueue` | Allow | Allowed_Queue or a Sticky_Task_Queue whose `normal_name` is allowed; exact versioned Deployment_Version |
| `PollActivityTaskQueue` | Allow | Allowed_Queue; exact versioned Deployment_Version; no standalone-activity claim |
| `PollNexusTaskQueue` | Allow | Allowed_Queue; exact versioned Deployment_Version |
| `RespondWorkflowTaskCompleted` | Allow | valid token for Own_Task |
| `RespondWorkflowTaskFailed` | Allow | valid token for Own_Task |
| `RespondQueryTaskCompleted` | Allow | valid token for query issued by an Own_Task poll |
| `RespondActivityTaskCompleted` | Allow | valid token for Own_Task |
| `RespondActivityTaskFailed` | Allow | valid token for Own_Task |
| `RespondActivityTaskCanceled` | Allow | valid token for Own_Task |
| `RecordActivityTaskHeartbeat` | Allow | valid token for Own_Task |
| `RespondNexusTaskCompleted` | Allow | valid token for Own_Task |
| `RespondNexusTaskFailed` | Allow | valid token for Own_Task |
| `RecordWorkerHeartbeat` | Allow | every heartbeat matches namespace, Allowed_Queue, and Deployment_Version |
| `ShutdownWorker` | Allow | normal queue, optional heartbeat, and non-empty sticky queue are within scope |
| `DescribeTaskQueue` | Allow | exact namespace and Allowed_Queue |
| `Health/Check`, `GetSystemInfo` | Allow | universal v1.31.0 health set; allowed before claims and not granted by Worker_Scope |
| Every other WorkflowService/OperatorService RPC | Deny | except the universal health set; ordinary roles cannot widen the scope |

The allow-list is fixed in code and documentation. Operators configure resource scope, not
arbitrary RPC permissions. The universal health set is inherited from
`default_authorizer.go:37-43 @ v1.31.0`; it is not Worker authority and remains callable by
anonymous clients.

## Requirements

### Requirement 1: Worker-Scope model and attenuation

**User Story:** As a security engineer, I want one explicit and non-composable Worker scope, so
that a guest credential cannot gain broader authority through role union or ambiguous mappings.

#### Acceptance Criteria

1. THE auth crate SHALL represent Worker_Scope as one namespace, one non-empty set of
   Normal_Task_Queue names, one non-blank deployment name, and one non-blank build ID.
2. THE auth crate SHALL attach at most one effective Worker_Scope to authenticated Claims.
3. WHEN Claims contain a Worker_Scope, THE authorizer SHALL cap the identity to the Fixed
   Operation Matrix plus the universal v1.31.0 health set regardless of any system or namespace
   roles on the same Claims.
4. WHEN Claims do not contain a Worker_Scope, THE authorizer SHALL preserve the existing
   v1.31.0-compatible role decision without a task-queue or Deployment-Version check.
5. THE Worker_Scope SHALL compare namespace, task queue, deployment name, and build ID exactly
   and case-sensitively.
6. THE Worker_Scope SHALL reject wildcard syntax and blank values in every resource field.
7. THE Worker_Scope SHALL reject an empty task-queue list.
8. THE Worker_Scope SHALL reject duplicate task-queue entries instead of silently changing the
   signed or configured grant.
9. THE Worker_Scope SHALL expose its task queues in deterministic lexical order after successful
   validation.
10. THE Worker_Scope SHALL be transport-independent, with no dependency on edge, runtime,
    storage, projection, or kernel crates.

### Requirement 2: Signed JWT Worker-Scope claim

**User Story:** As an identity-platform operator, I want my IdP to sign the Worker scope into a
standard bearer JWT, so that ephemeral workers do not require per-instance Tokeira config.

#### Acceptance Criteria

1. WHEN a verified JWT contains `tokeira_worker_scope`, THE JWT authenticator SHALL parse it
   according to the Fixed JWT Claim table.
2. WHEN `tokeira_worker_scope.version` is absent or not exactly integer `1`, THE JWT
   authenticator SHALL reject authentication.
3. WHEN the Worker-Scope claim contains an unknown field, THE JWT authenticator SHALL reject
   authentication.
4. WHEN the Worker-Scope claim has the wrong JSON type for any field, THE JWT authenticator SHALL
   reject authentication.
5. WHEN the Worker-Scope claim violates Requirement 1 validation, THE JWT authenticator SHALL
   reject authentication.
6. WHEN a JWT omits `tokeira_worker_scope`, THE JWT authenticator SHALL preserve its existing
   permissions-claim and subject-grant behavior.
7. THE JWT authenticator SHALL validate signature, issuer, audience, and lifetime before trusting
   `tokeira_worker_scope`.
8. THE JWT authenticator SHALL treat `tokeira_worker_scope` as a fixed claim name rather than
   adding a production configuration field.
9. WHEN a malformed Worker-Scope claim appears beside valid namespace roles, THE JWT
   authenticator SHALL reject the credential rather than fall back to those roles.
10. THE JWT authenticator SHALL avoid logging the bearer token or complete signed claim.

### Requirement 3: Configured JWT-subject and AWS-IAM scope mapping

**User Story:** As an operator, I want a verified JWT subject or AWS IAM ARN to map to a Worker
scope, so that identity providers that cannot emit custom claims can still launch constrained
workers.

#### Acceptance Criteria

1. THE config crate SHALL model `jwt.issuers[].worker_scopes[]` with `match_sub` plus the four
   structured Worker-Scope fields in the Static Mapping table.
2. THE config crate SHALL model `aws_iam.worker_scopes[]` with `match_arn` plus the four
   structured Worker-Scope fields in the Static Mapping table.
3. THE config crate SHALL apply the existing full-string, case-sensitive, `*`-only pattern
   grammar to `match_sub` and `match_arn`.
4. WHEN a configured Worker-Scope rule violates Requirement 1 validation, THE config crate SHALL
   fail startup and name the offending field.
5. WHEN no configured Worker-Scope rule matches an authenticated identity, THE authenticator
   SHALL preserve the identity's existing ordinary grants.
6. WHEN exactly one configured Worker-Scope rule matches, THE authenticator SHALL attach that
   rule's scope to Claims.
7. WHEN multiple matching configured rules produce byte-for-byte equivalent normalized scopes,
   THE authenticator SHALL collapse them to one effective Worker_Scope.
8. WHEN multiple matching configured rules produce different normalized scopes, THE
   authenticator SHALL deny authentication rather than union the scopes.
9. WHEN a JWT signed Worker-Scope claim and a configured subject rule produce equivalent
   normalized scopes, THE JWT authenticator SHALL attach that one effective Worker_Scope.
10. WHEN a JWT signed Worker-Scope claim and a configured subject rule produce different scopes,
    THE JWT authenticator SHALL deny authentication rather than choose or union them.
11. THE AWS IAM authenticator SHALL evaluate Worker-Scope rules only after successful presigned
    STS identity verification.
12. THE configured Worker-Scope mappings SHALL NOT require an accompanying
    `namespace:worker`, `namespace:read`, or `namespace:write` grant.

### Requirement 4: Request-aware fail-closed authorization

**User Story:** As a platform owner, I want scope checks to finish before handler side effects,
so that a mismatched worker cannot claim a task or mutate Worker state.

#### Acceptance Criteria

1. WHEN a Scoped_Identity calls a Worker_Operation, THE edge SHALL resolve every request target
   required by the Fixed Operation Matrix before invoking the effectful handler.
2. WHEN a request target does not match the Worker_Scope, THE edge SHALL return the configured
   generic authorization denial without invoking the effectful handler.
3. WHEN target resolution fails internally, THE edge SHALL fail closed rather than fall back to
   namespace-only authorization.
4. THE authorizer target SHALL distinguish Worker_Operations from every other API operation.
5. THE authorizer target SHALL carry only normalized resource identity, excluding request
   payloads, task payloads, and bearer material.
6. THE request admission path SHALL preserve the existing rule that authorization occurs before
   namespace-existence disclosure.
7. WHEN an Ordinary_Identity makes the same request, THE edge SHALL preserve the current
   namespace-classification behavior and error ordering.
8. THE authorization implementation SHALL NOT perform I/O in `tokeira-auth`.
9. THE authorization implementation SHALL NOT introduce authorization state or commands in
   `tokeira-kernel`.
10. WHEN read-only runtime resolution is required for a token-bound response, THE edge SHALL
    treat authoritative committed run state or broker correlation as the source of Task_Origin.

### Requirement 5: Versioned poll authorization

**User Story:** As a tenant, I want a scoped worker to receive tasks only for its declared queue
and exact code version, so that a compromised or stale guest cannot execute another workload.

#### Acceptance Criteria

1. WHEN a Scoped_Identity polls a normal Workflow Task Queue, THE edge SHALL require the request
   queue to be an Allowed_Queue.
2. WHEN a Scoped_Identity polls an Activity Task Queue, THE edge SHALL require the request queue
   to be an Allowed_Queue.
3. WHEN a Scoped_Identity polls a Nexus Task Queue, THE edge SHALL require the request queue to
   be an Allowed_Queue.
4. WHEN a Scoped_Identity polls any task kind, THE edge SHALL require
   `WorkerDeploymentOptions.worker_versioning_mode` to be `VERSIONED`.
5. WHEN a Scoped_Identity polls any task kind, THE edge SHALL require deployment name and build
   ID to equal the Worker_Scope Deployment_Version.
6. WHEN a Scoped_Identity supplies only deprecated build-ID capabilities, THE edge SHALL deny
   the poll because no exact deployment name can be proven.
7. WHEN a Scoped_Identity supplies a partial Deployment_Version, THE edge SHALL deny the poll
   before registering a poller or claiming a task.
8. WHEN a Scoped_Identity supplies an unversioned Deployment mode, THE edge SHALL deny the poll
   before registering a poller or claiming a task.
9. WHEN a Scoped_Identity polls a Sticky_Task_Queue, THE edge SHALL authorize its
   `TaskQueue.normal_name` against the Allowed_Queue list.
10. WHEN a scoped sticky poll omits `normal_name`, THE edge SHALL deny the poll before
    registering a poller or claiming a task.
11. WHEN a scoped sticky poll names an allowed normal queue, THE edge SHALL permit the
    Worker-specific sticky queue name without requiring it in static scope.
12. THE poll authorization decision SHALL ignore caller-supplied Worker identity as proof of
    queue or version authority.
13. WHEN a scoped Workflow or Activity poll names a non-empty
    `worker_control_task_queue`, THE edge SHALL require server evidence tying that queue to the
    same scoped worker rather than treating it as another Allowed_Queue.
14. THE poll authorization decision SHALL run before the poller registry, heartbeat store,
    broker waiter, or task-claim path changes state.
15. WHEN no task is available after a poll is authorized, THE edge SHALL preserve the existing
    long-poll and empty-response behavior.

### Requirement 6: Token-bound Own-Task responses

**User Story:** As a tenant, I want a guest to answer only tasks the server issued within its
scope, so that copied identifiers or forged request fields do not become completion authority.

#### Acceptance Criteria

1. WHEN a Scoped_Identity responds with a Workflow Task token, THE edge SHALL require the
   token's authoritative Task_Origin to match the Worker_Scope.
2. WHEN a Scoped_Identity responds with an Activity Task token, THE edge SHALL require the
   token's authoritative Task_Origin to match the Worker_Scope.
3. WHEN a Scoped_Identity responds with a Nexus Task token, THE edge SHALL require the token's
   authoritative Task_Origin to match the Worker_Scope.
4. WHEN a Scoped_Identity records an Activity heartbeat, THE edge SHALL require the token's
   authoritative Task_Origin to match the Worker_Scope.
5. THE Task_Origin check SHALL cover namespace, Normal_Task_Queue, task kind, deployment name,
   and build ID.
6. THE Task_Origin check SHALL derive from server-authored state or correlation rather than
   request-declared queue, deployment, build ID, identity, or headers.
7. WHEN a task token is valid but its Task_Origin is outside scope, THE edge SHALL deny the
   response before committing a transition or completing Nexus correlation.
8. WHEN a task token is malformed, stale, or fenced after scope admission, THE edge SHALL
   preserve the existing v1.31.0-compatible token error rather than report a fabricated
   authorization success.
9. WHEN a Scoped_Identity calls an Activity By_ID_Response RPC, THE edge SHALL deny the request
   even when the namespace and identifiers refer to an in-scope task.
10. WHEN a Scoped_Identity calls `RespondQueryTaskCompleted`, THE edge SHALL require the query
    token to originate from an authorized Workflow poll.
11. THE response path SHALL preserve existing task-token attempt, stamp, and shard-epoch fencing.
12. THE Task_Origin proof SHALL survive process-local handoff between poll and response for the
    lifetime of every valid task token.
13. WHEN Task_Origin cannot be proven after restart or failover, THE edge SHALL fail closed
    rather than infer it from caller input.

### Requirement 7: Workflow-task completion side effects

**User Story:** As a Workflow author, I want ordinary Workflow commands to retain their Temporal
semantics while the responding worker remains unable to escape its credential scope.

#### Acceptance Criteria

1. WHEN a Scoped_Identity completes an Own_Task Workflow Task, THE runtime SHALL apply valid
   same-namespace Workflow commands under the task's existing execution authority.
2. THE scoped authorizer SHALL NOT require every Activity or child-workflow target queue in a
   valid Workflow command to appear in the worker credential's poll allow-list.
3. WHEN a Workflow command addresses another namespace, THE edge SHALL apply the existing
   cross-namespace authorization rule to the Scoped_Identity.
4. WHEN a Scoped_Identity lacks authority for a cross-namespace command, THE edge SHALL reject
   the Workflow Task completion before committing any command in that completion.
5. WHEN a valid completion requests eager Activity execution for a task outside the
   Worker_Scope, THE server SHALL accept the Workflow Task completion without returning that
   eager Activity task to the caller.
6. WHEN a valid completion requests a new Workflow Task response outside the Worker_Scope, THE
   server SHALL accept the completion without returning that task inline.
7. WHEN an eager or inline task is returned to a Scoped_Identity, THE returned task SHALL have a
   Task_Origin that matches the Worker_Scope.
8. THE suppression of an optional eager or inline task SHALL preserve its normal durable
   dispatch path.

### Requirement 8: DescribeTaskQueue readiness access

**User Story:** As a worker provisioner, I want a scoped credential to verify that its exact
version is polling, so that readiness can be established without namespace-wide read access.

#### Acceptance Criteria

1. WHEN a Scoped_Identity calls `DescribeTaskQueue`, THE edge SHALL require the request namespace
   to equal the Worker_Scope namespace.
2. WHEN a Scoped_Identity calls `DescribeTaskQueue`, THE edge SHALL require the requested normal
   task queue to be an Allowed_Queue.
3. WHEN a scoped `DescribeTaskQueue` request matches an Allowed_Queue, THE edge SHALL preserve
   the existing BASIC or ENHANCED response semantics.
4. THE scoped `DescribeTaskQueue` response SHALL preserve PollerInfo deployment name and build ID
   so a provisioner can match the exact Worker_Scope Deployment_Version.
5. THE scoped `DescribeTaskQueue` response SHALL NOT grant access to `ListWorkers`,
   `DescribeWorker`, Workflow visibility, Workflow history, or namespace description APIs.
6. WHEN a scoped `DescribeTaskQueue` request uses report or version-selection fields, THE edge
   SHALL treat those fields only as response-shape selectors and not as wider resource authority.
7. WHEN a scoped `DescribeTaskQueue` request names a disallowed queue, THE edge SHALL deny it
   before reading poller history or backlog statistics.

### Requirement 9: Worker heartbeat and shutdown integrity

**User Story:** As an operator, I want lifecycle observations from a scoped worker to stay within
its assigned queue and version, so that it cannot poison readiness, worker inventory, or poll
cancellation state.

#### Acceptance Criteria

1. WHEN a Scoped_Identity calls `RecordWorkerHeartbeat`, THE edge SHALL validate every repeated
   WorkerHeartbeat before storing any heartbeat from that request.
2. WHEN a scoped WorkerHeartbeat names a task queue, THE edge SHALL require that queue to be an
   Allowed_Queue.
3. WHEN a scoped WorkerHeartbeat carries a Deployment Version, THE edge SHALL require its
   deployment name and build ID to equal the Worker_Scope Deployment_Version.
4. WHEN a scoped WorkerHeartbeat omits or partially specifies its Deployment Version, THE edge
   SHALL deny the enclosing request.
5. WHEN any heartbeat in a scoped multi-heartbeat request is outside scope, THE edge SHALL reject
   the whole request without a partial store update.
6. WHEN a scoped Nexus poll piggybacks Worker heartbeats, THE edge SHALL validate every heartbeat
   before registering the poller, storing a heartbeat, or claiming a Nexus task.
7. WHEN a Scoped_Identity calls `ShutdownWorker`, THE edge SHALL require a non-empty top-level
   normal task queue to be an Allowed_Queue.
8. WHEN scoped shutdown carries a WorkerHeartbeat, THE edge SHALL apply the same validation as
   `RecordWorkerHeartbeat`.
9. WHEN scoped shutdown names a non-empty Sticky_Task_Queue, THE edge SHALL require server
   evidence tying that sticky queue to the scoped worker and an Allowed_Queue.
10. WHEN scoped shutdown passes validation and the existing v1.31.0
    `frontend.enableCancelWorkerPollsOnShutdown` policy is enabled, THE edge SHALL cancel only
    outstanding polls for the authorized normal queue, requested task-queue types, and caller
    Worker identity; WHEN the policy is disabled, THE edge SHALL advertise that state and preserve
    the v1.31.0 client-side poll-termination behavior.
11. WHEN scoped shutdown fails validation, THE edge SHALL avoid heartbeat insertion, sticky
    denial, and outstanding-poll cancellation.
12. THE Worker-Scope allow-list SHALL NOT authorize `ListWorkers` or `DescribeWorker`.

### Requirement 10: Standalone-activity and alternate-path closure

**User Story:** As a security reviewer, I want every task-delivery path to pass the same scope
gate, so that a fast path cannot bypass queue/version enforcement.

#### Acceptance Criteria

1. THE gRPC Workflow poll path SHALL complete scoped admission before entering any broker or
   legacy fast path.
2. THE gRPC Activity poll path SHALL complete scoped admission before entering the CHASM
   standalone-activity bridge or workflow-activity broker.
3. THE gRPC Nexus poll path SHALL complete scoped admission before heartbeat insertion or Nexus
   broker registration.
4. WHEN a Scoped_Identity polls Activity tasks, THE standalone-activity bridge SHALL NOT return
   an unversioned standalone Activity.
5. THE Workflow, Activity, Query, and Nexus response fast paths SHALL complete Own_Task
   authorization before mutating their correlation store or authoritative state.
6. THE conformance and legacy-token compatibility paths SHALL NOT bypass Worker-Scope checks.
7. THE HTTP/gRPC-gateway path SHALL preserve the same Worker-Scope decision as direct gRPC when
   it transports one of the scoped RPCs.
8. THE scoped authorization decision SHALL be structurally shared across normal and fast paths
   rather than duplicated as best-effort handler checks.

### Requirement 11: Compatibility, errors, observability, and documentation

**User Story:** As an operator, I want this security extension to be explicit and diagnosable
without exposing tenant or credential data.

#### Acceptance Criteria

1. WHEN no Worker_Scope is present or configured, THE server SHALL preserve all
   authorization-foundation behavior and defaults.
2. THE feature SHALL remain inert without a scoped JWT claim or matching configured scope rule.
3. THE server SHALL use the authorization-foundation denial exposure policy for scoped denials.
4. THE server SHALL avoid revealing the allowed queue list, deployment name, build ID, or token
   content in the public generic denial.
5. THE server SHALL emit a bounded denial-reason metric distinguishing operation, namespace,
   queue, version, task-origin, heartbeat, shutdown, and ambiguous-mapping failures.
6. THE server SHALL log the authenticated subject and bounded denial reason without logging
   bearer tokens or task payloads.
7. THE Feature Catalog SHALL describe scoped Worker authorization as a Tokeira-native,
   presence-activated extension rather than Temporal v1.31.0 default behavior.
8. THE public Tokeira configuration guide SHALL document the fixed JWT claim and both static
   mapping forms.
9. THE public Tokeira configuration guide SHALL state that Worker_Scope attenuates ordinary
   roles instead of unioning with them.
10. THE public Tokeira configuration guide SHALL provide a standard Temporal SDK bearer-metadata
    example without embedding a real secret.
11. THE public Tokeira configuration guide SHALL state that the credential issuer owns token
    minting and rotation.
12. THE public Tokeira configuration guide SHALL warn that scoped polling requires VERSIONED
    deployment options with the exact deployment name and build ID.
13. THE implementation SHALL cite the relevant vendored proto or v1.31.0 source beside every
    non-obvious compatibility decision.

### Requirement 12: Verification and downstream readiness

**User Story:** As the owner, I want executable evidence at each security boundary, so that
provider readiness rests on fail-closed behavior rather than a happy-path demonstration.

#### Acceptance Criteria

1. THE auth tests SHALL property-test Worker-Scope normalization, exact matching, invalid fields,
   and deterministic queue ordering.
2. THE JWT tests SHALL cover valid, absent, malformed, unknown-version, unknown-field, and
   conflicting signed Worker-Scope claims.
3. THE static-mapping tests SHALL cover no match, one match, identical duplicate matches, and
   conflicting matches for JWT subjects and AWS IAM ARNs.
4. THE authorizer tests SHALL exhaustively cover every row of the Fixed Operation Matrix.
5. THE poll tests SHALL cover all three task kinds, wrong namespace, wrong queue, wrong
   deployment, wrong build, partial version, unversioned mode, deprecated capabilities, and
   sticky normal-name resolution.
6. THE task-response tests SHALL prove that a token from another queue or version is denied
   before transition or correlation mutation.
7. THE task-response tests SHALL prove that every Activity By_ID_Response is denied to a
   Scoped_Identity.
8. THE Workflow-completion tests SHALL prove that same-namespace commands retain ordinary
   semantics while cross-namespace commands remain denied.
9. THE Workflow-completion tests SHALL prove that out-of-scope eager and inline tasks are
   withheld without losing durable dispatch.
10. THE heartbeat tests SHALL prove atomic rejection of a mixed-scope repeated heartbeat request.
11. THE shutdown tests SHALL prove that a mismatch changes neither heartbeat state nor poll
    cancellation state.
12. THE alternate-path tests SHALL prove that standalone Activity, legacy token, Nexus
    piggyback-heartbeat, and HTTP gateway paths cannot bypass scoped admission.
13. THE integration tests SHALL demonstrate in-scope Workflow, Activity, and Nexus task
    completion by starting a Worker through a standard Temporal SDK auth metadata supplier.
14. THE integration tests SHALL demonstrate that the same credential cannot poll a second queue
    or version.
15. THE integration tests SHALL demonstrate that the same credential can call
    `DescribeTaskQueue` for readiness and cannot call namespace-wide read or write APIs.
16. THE cross-repository contract evidence SHALL record the exact tokeira commit and sibling-provider
    contract revision used for the end-to-end readiness proof.

## Out of Scope

- Issuing, refreshing, or distributing guest credentials inside Tokeira.
- Adding a token-minting RPC or modifying upstream Temporal protos.
- mTLS-derived Worker identity.
- Wildcard namespaces, wildcard task queues, multiple namespaces, multiple Deployment Versions,
  or arbitrary per-RPC operator grants in one Worker_Scope.
- Namespace-wide Worker inventory access (`ListWorkers`, `DescribeWorker`).
- Activity By-ID responses for scoped workers.
- Unversioned, deprecated-build-ID-only, or standalone-activity polling by scoped workers.
- Kernel, history, lane-routing, projection, or delivery-order changes.
- Provider-specific VM, container, Lambda, IAM-role, or Firecracker lifecycle.
- Changing Temporal v1.31.0's stock default authorizer, numeric roles, or ordinary identity
  behavior.

## Traceability

- Security issue: [`tokeira/tokeira#29`](https://github.com/tokeira/tokeira/issues/29).
- Existing identity and role foundation:
  [`authorization-foundation`](../authorization-foundation/requirements.md).
- Compute-provider dependency:
  [`worker-compute-controller`](../worker-compute-controller/requirements.md).
- Architecture: [`docs/architecture/000-overview.md`](../../../docs/architecture/000-overview.md).
- Public Tokeira configuration:
  [`docs/conformance/v1.31.0/tokeira-configuration.md`](../../../docs/conformance/v1.31.0/tokeira-configuration.md).
- Temporal configuration inventory:
  [`docs/conformance/v1.31.0/temporal-configuration.md`](../../../docs/conformance/v1.31.0/temporal-configuration.md).
