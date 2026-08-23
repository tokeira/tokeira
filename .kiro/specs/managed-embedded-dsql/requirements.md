# Managed Aurora DSQL for Embedded Tokeira — Requirements

## Introduction

Embedded Tokeira currently provides a Temporal-compatible engine in the host process,
without a Tokeira daemon or a listening socket. Its storage is forced to an in-memory
backend. This specification adds explicit embedded storage modes for a dedicated,
Tokeira-managed Amazon Aurora DSQL cluster and for an operator-supplied Aurora DSQL
cluster, while retaining the existing in-memory mode.

This document deliberately separates two kinds of statement:

- **Verified current behaviour** is supported by the repository or by current official
  AWS documentation, inspected on 2026-08-23.
- **Proposed required behaviour** is the target contract introduced by this feature.

The feature is a storage, startup-lifecycle, configuration, AWS integration, and
observability change. It does not change workflow semantics, the authoritative history
model, or the deterministic kernel. No requirement below implies a change to
`tokeira-kernel`. If a later design cannot satisfy these requirements without a kernel
change, work must stop and that conflict must be raised for a new product decision.

## Glossary

- **Embedded Engine**: A Tokeira engine running inside a host process and reached through
  the in-process Temporal `service_override`, without a Tokeira-owned TCP listener or
  daemon.
- **In-memory Mode**: The existing embedded storage mode whose durable state exists only
  in an optional host-provided snapshot.
- **Managed DSQL Mode**: An explicitly selected embedded mode in which Tokeira creates or
  recovers one dedicated, single-Region Aurora DSQL cluster and manages its schema.
- **Existing DSQL Mode**: An explicitly selected mode in which an operator supplies the
  canonical identity and connection locator for an Aurora DSQL cluster.
- **Distributed Mode**: Multi-process Tokeira operation with external coordination. It is
  not made single-process merely by pointing multiple engines at one managed cluster.
- **Cluster Descriptor**: Crash-safe state holding the selected AWS Region, DSQL cluster
  ID, cluster ARN, endpoint, and explicit `CreateCluster` client token. During creation,
  the descriptor may temporarily contain only the Region and client token.
- **Canonical Cluster Identity**: The pair of AWS DSQL cluster ID and ARN. Tags are
  metadata and an endpoint is only a connection locator.
- **Creation Client Token**: The explicit application-generated `clientToken` passed to
  `CreateCluster` and reused for all retries and crash recovery of one creation attempt.
- **Schema Version**: The monotonically increasing integer recorded by Tokeira's DSQL
  migration ledger.
- **Schema Compatibility Contract**: Release metadata containing the Tokeira release,
  minimum supported schema version, target schema version, maximum readable schema
  version, and migration-set digest.
- **Migration-set Digest**: A deterministic digest over the ordered, immutable migration
  identities and contents recognized by a Tokeira release.
- **Automatic Migration Policy**: Permission for startup to apply verified forward
  migrations up to the release's target schema version.
- **Validate-only Migration Policy**: Permission for startup to inspect compatibility but
  not change schema state.
- **Embedded Ownership Claim**: A renewable, time-bounded claim in the DSQL cluster that
  admits only one live embedded engine process.
- **Startup Report**: Structured, non-secret information describing the selected storage
  mode, resolved cluster, schema compatibility decision, migration outcome, and ownership
  result.
- **Host Observability**: The tracing subscriber, OpenTelemetry providers, metrics
  recorder/exporters, Logfire integration, and flush lifecycle installed and owned by the
  embedding application.
- **Stable Execution Identifiers**: Workflow ID, Run ID, Activity ID, Activity Type,
  attempt, task queue, and related durable identifiers that remain meaningful when a new
  trace is created after a crash or restart.

## Target State

| Area | Verified current behaviour | Proposed required behaviour |
|---|---|---|
| Embedded storage | `Engine::start_with_config` rewrites all embedded storage to in-memory. | The host explicitly selects in-memory, managed DSQL, or existing DSQL; selection never silently falls back. |
| Embedded transport | The in-process gRPC bridge reaches the same service router without binding configured listeners. | The transport remains in-process; AWS DSQL and host exporter network traffic remain allowed. |
| DSQL coordination | Production `DsqlStore::connect` constructs DynamoDB-backed connection-rate and slot coordination. | Managed embedded DSQL uses bounded process-local coordination and requires no DynamoDB. |
| Cluster lifecycle | The AWS IaC resource creates DSQL with deletion protection and persists ID, ARN, and endpoint, but does not send an explicit create client token. | Embedded startup persists an explicit token before create, retries with it, and recovers only through canonical identity. |
| Schema | Embedded migrations have per-migration checksums, but the engine does not apply them during DSQL startup and no release compatibility envelope exists. | Each release declares and enforces the complete schema compatibility contract. |
| Ownership | The engine can self-assign shards when DSQL has no controller, but it has no cross-process embedded-owner exclusion. | One renewable DSQL claim fences managed embedded use to a single live process. |
| Telemetry | Embedded construction does not install the daemon's process-global observability runtime; the code emits `tracing` and `metrics` events. | Embedded code remains globally inert, emits composable signals, and propagates context across all Tokeira-mediated boundaries. |
| Correlation | Durable execution identifiers exist, and some runtime spans record run-related fields, but coverage is not an end-to-end contract. | Relevant spans and metrics carry stable, non-sensitive execution identifiers across crashes and restarts. |

## Evidence from Current Code and Official APIs

### Verified Tokeira implementation

1. [`Engine::start_with_config`](../../../crates/tokeira-engine/src/lib.rs) calls
   `embedded_config`, which currently sets storage to `InMemory` and removes the
   controller endpoint. The adjacent tests verify that a configured DSQL backend is
   rewritten to in-memory.
2. The same engine module's embedded builder deliberately omits signals and process
   observability setup, while daemon construction calls the observability installer.
3. [`InProcessGrpcService`](../../../crates/tokeira-edge/src/in_process.rs) invokes the
   same tonic router, copies request metadata, and does not own a listening socket.
4. [`DsqlStore::connect`](../../../crates/tokeira-storage/src/dsql/mod.rs) currently
   requires a DynamoDB-backed `DistributedTokenBucket` and `SlotBlockManager` before
   constructing the connection director. Its local alternatives are test-only.
5. [`DsqlConnectionDirector`](../../../crates/tokeira-storage/src/dsql/connection.rs)
   already provides class-based concurrency budgets, leak tracking, and coordinated
   shutdown. The storage contract also requires `max_idle_conns == max_conns` and
   forbids ad hoc connections outside the director.
6. [`MigrationRunner`](../../../crates/tokeira-storage/src/dsql/migration.rs) embeds an
   ordered migration set and verifies the checksum of each previously applied migration.
   Its schema status is currently only the highest applied version.
7. [`DsqlCluster`](../../../crates/tokeira-aws/src/resources/dsql_cluster.rs) uses the
   returned cluster ID for `GetCluster`, enables deletion protection, persists ID, ARN,
   and endpoint, and implements explicit deletion. Its create request does not currently
   set `clientToken`.
8. [`DsqlInfraConfig`](../../../crates/tokeira-config/src/lib.rs) currently stores an
   endpoint and Region but has no embedded storage-mode choice, cluster descriptor,
   creation token, ownership configuration, or migration policy.
9. [`tkr schema`](../../../apps/tkr/src/commands/schema.rs) currently makes schema setup
   an explicit, confirmed operator action. This remains the baseline expectation for
   operator-supplied and distributed DSQL unless an explicit migration policy says
   otherwise.
10. [`tokeira-observability`](../../../crates/tokeira-observability/src/lib.rs) is a
    process-level installer for binaries. It can install global tracing and metrics state
    and can bind a metrics listener, so embedded mode must not call it.
11. The repository is still in the migration build phase described by
    [`crates/tokeira-storage/AGENTS.md`](../../../crates/tokeira-storage/AGENTS.md), in
    which existing migration files may be edited until the first durable release. A
    frozen baseline is therefore a prerequisite for a cross-release checksum contract.
12. No provider, MCP-tool, or handoff abstraction is present in this repository. Those
    boundaries are an external integration contract to be verified with the embedding
    host; Tokeira owns only the boundaries it mediates.

### Verified current AWS behaviour

The following are external service facts, not Tokeira proposals:

1. [`CreateCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_CreateCluster.html)
   accepts an idempotency `clientToken`; retrying with the same token after success
   returns the original result without another creation. The response supplies the
   cluster ID, ARN, endpoint, and status.
2. [`GetCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_GetCluster.html)
   addresses a cluster by its 26-character identifier and returns the ARN, endpoint,
   status, deletion-protection state, and tags.
3. [Aurora DSQL cluster lifecycle](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cluster-lifecycle.html)
   defines `IDLE` and `INACTIVE` as healthy scale-to-zero states. A connection wakes the
   cluster and the first connection can be slower.
4. [`UpdateCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_UpdateCluster.html)
   changes deletion protection by identifier, and
   [`DeleteCluster`](https://docs.aws.amazon.com/aurora-dsql/latest/APIReference/API_DeleteCluster.html)
   deletes by identifier. Deletion is blocked while deletion protection is enabled.
5. [Aurora DSQL quotas](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)
   currently include 20 single-Region clusters per account and Region by default, a
   connection-creation rate of 100 per second, a burst of 1,000, and a default 10,000
   concurrent connections. A small embedded pool is therefore a Tokeira product policy,
   not an AWS requirement.
6. [Aurora DSQL authentication](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/authentication-authorization.html)
   uses IAM authorization, and generated
   [authentication tokens](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/SECTION_authentication-token.html)
   expire by default after 15 minutes without terminating an established connection.

### Context-propagation investigation findings

| Boundary | Verified current evidence | Gap resolved by the proposed requirements |
|---|---|---|
| `service_override` | The in-process adapter copies request metadata, and the [gRPC tracing interceptor](../../../crates/tokeira-edge/src/grpc/tracing_interceptor.rs) extracts W3C Trace Context and sets the server span parent. | Existing tests cover the pieces but not parentage through an actual SDK `service_override` call. |
| Internal runtime channels | [`lane.rs`](../../../crates/tokeira-runtime/src/lane.rs) captures an origin trace and span ID and records them as fields on the receiving span. | Recording IDs as fields is correlation, not OpenTelemetry parentage or a span link; the intended relationship needs an integration test. |
| Workflows and activities | Workflow, Run, Activity, Activity Type, and attempt identifiers are present in protocol and runtime state. | There is no repository-wide contract that attaches every applicable stable identifier or verifies SDK-carried trace context end to end. |
| Providers, MCP tools, and handoffs | No such integration abstraction exists in this repository. | The embedding host owns these carriers; Tokeira must preserve or inject context only where it mediates a boundary, and host integration tests must cover the rest. |
| Restarts | Workflow and Run identity is durable; telemetry context is not part of authoritative history. | A restarted process can create a new trace, so correlation must use durable execution identifiers rather than a promise that one trace survives a crash. |
| Metrics export | Domain crates emit through `metrics`; the daemon installer owns the global Prometheus recorder and HTTP listener. | Embedded construction must remain recorder-neutral, and high-cardinality execution IDs must not become default metric labels. |
| Shutdown flushing | The daemon observability runtime can stop its background HTTP task, but it does not own a host's providers; embedded mode retains no exporter handle. | Tokeira must finish its own instrumented work, then let the host flush and shut down the providers it owns. |

## Scope and Policy Tables

### Embedded storage selection

| Mode | Cluster lifecycle owner | Default migration policy | Coordination | DynamoDB |
|---|---|---|---|---|
| `in_memory` | Host snapshot lifecycle | Not applicable | Process-local | Not required |
| `managed_dsql` | Embedded Tokeira startup plus explicit administrator destruction | `automatic` | Process-local and DSQL ownership claim | Forbidden |
| `existing_dsql` | Operator | No implicit default; host must select | Process-local for one embedded process, distributed coordination for multiple processes | Not required for one embedded process; distributed mode retains its own contract |

### Managed cluster descriptor

| Field | Required before `CreateCluster` | Required after successful create | Meaning |
|---|---:|---:|---|
| `region` | Yes | Yes | AWS Region used for the control plane and connection token |
| `creation_client_token` | Yes | Yes | Stable idempotency key for this creation attempt |
| `cluster_id` | No | Yes | Canonical AWS DSQL identifier |
| `cluster_arn` | No | Yes | Canonical AWS resource identity paired with the ID |
| `endpoint` | No | Yes | Refreshable connection locator, never identity |

The descriptor must not persist AWS credentials, DSQL authentication tokens, prompts,
tool data, or telemetry exporter credentials.

### Schema compatibility decisions

Let `C` be the cluster's current schema version, `MIN` the release's minimum supported
version, `TARGET` its target version, and `MAX` its maximum readable version. Checksum
and digest validation always occurs before the version decision.

| Condition | Automatic policy | Validate-only policy |
|---|---|---|
| Any known checksum differs, or applicable migration digest differs | Reject | Reject |
| `C < MIN` | Reject as unsupported; do not attempt a partial migration | Reject as unsupported |
| `MIN <= C < TARGET` | Apply the verified ordered migrations to `TARGET` | Reject with migration-required status |
| `C == TARGET` | Continue | Continue |
| `TARGET < C <= MAX` | Continue without a down-migration | Continue |
| `C > MAX` or an applied version is unknown to the readable set | Reject as a future incompatible schema | Reject as a future incompatible schema |

### Default embedded DSQL resource envelope

These are proposed safe defaults, not AWS limits. The design may expose lower host
overrides but must not permit an embedded configuration to exceed the bounded envelope
without explicitly selecting distributed mode.

| Resource | Proposed default | Embedded maximum |
|---|---:|---:|
| Physical database connections | 8 | 16 |
| Concurrent connection creations | 2 | 4 |
| Connection creations per second | 8 | 16 |
| Connection-creation burst | 2 | 4 |

## Requirements

### Requirement 1: Explicit Embedded Storage Selection

**User Story:** As an embedding host, I want storage selection to be explicit so that a
configuration error cannot silently replace durable storage with memory or create AWS
resources unexpectedly.

#### Acceptance Criteria

1.1 THE embedded engine configuration SHALL expose `in_memory`, `managed_dsql`, and
`existing_dsql` as distinct storage modes.

1.2 WHEN no embedded storage mode is supplied, THE embedded engine SHALL select
`in_memory` for backward compatibility.

1.3 WHEN `managed_dsql` is selected, THE embedded engine SHALL require an explicit
create-or-recover opt-in.

1.4 WHEN a selected storage mode is invalid or incomplete, THE embedded engine SHALL
fail startup without selecting another storage mode.

1.5 WHEN `managed_dsql` or `existing_dsql` is selected, THE embedded engine SHALL use
Aurora DSQL as the authoritative runtime store.

1.6 WHEN embedded startup completes, THE embedded engine SHALL expose the selected
storage mode in its startup report.

1.7 WHILE running in any embedded storage mode, THE embedded engine SHALL avoid binding
a Tokeira execution listener.

1.8 THE embedded execution transport SHALL remain compatible with the Temporal SDK
`service_override` path.

1.9 THE absence of a Tokeira execution listener SHALL NOT prohibit outbound AWS DSQL
traffic.

1.10 THE absence of a Tokeira execution listener SHALL NOT prohibit host-controlled
telemetry export traffic.

### Requirement 2: Crash-safe Managed Cluster Creation

**User Story:** As an embedding host, I want managed cluster creation to be idempotent
across retries and crashes so that startup cannot create duplicate billable clusters.

#### Acceptance Criteria

2.1 WHEN managed startup has no cluster descriptor, THE managed DSQL lifecycle SHALL
generate a new explicit `CreateCluster` client token.

2.2 BEFORE the first `CreateCluster` request, THE managed DSQL lifecycle SHALL durably
persist the selected Region and client token.

2.3 WHEN a persisted descriptor contains a client token but no cluster ID, THE managed
DSQL lifecycle SHALL reuse that token in the next `CreateCluster` request.

2.4 WHEN retrying a failed or indeterminate `CreateCluster` request, THE managed DSQL
lifecycle SHALL reuse the persisted client token.

2.5 THE managed DSQL lifecycle SHALL pass the client token explicitly rather than rely
on SDK-generated idempotency state.

2.6 THE managed DSQL lifecycle SHALL request a dedicated single-Region cluster.

2.7 THE managed DSQL lifecycle SHALL enable deletion protection in the create request.

2.8 WHEN `CreateCluster` returns identity and connection data, THE managed DSQL
lifecycle SHALL atomically persist the Region, client token, cluster ID, cluster ARN,
and endpoint.

2.9 IF descriptor persistence fails after a successful create response, THEN THE managed
DSQL lifecycle SHALL fail startup with the persisted client token left recoverable.

2.10 THE cluster descriptor persistence mechanism SHALL provide crash-safe replacement
and compare-and-swap protection.

2.11 THE managed DSQL lifecycle SHALL NOT store AWS credentials in the cluster
descriptor.

2.12 THE managed DSQL lifecycle SHALL NOT emit the creation client token in telemetry.

### Requirement 3: Canonical Cluster Recovery and Validation

**User Story:** As an operator, I want recovery to use immutable AWS identity so that
renamed tags or endpoint changes cannot attach Tokeira to the wrong cluster.

#### Acceptance Criteria

3.1 THE managed DSQL lifecycle SHALL treat the persisted cluster ID and ARN as the
canonical cluster identity.

3.2 THE managed DSQL lifecycle SHALL treat the endpoint only as a refreshable connection
locator.

3.3 THE managed DSQL lifecycle SHALL treat tags only as optional metadata.

3.4 THE managed DSQL lifecycle SHALL NOT discover or recover a cluster by tag.

3.5 WHEN a descriptor contains a cluster ID, THE managed DSQL lifecycle SHALL call
`GetCluster` with that ID.

3.6 WHEN `GetCluster` returns a different ID or ARN from the descriptor, THE managed DSQL
lifecycle SHALL fail startup as an identity mismatch.

3.7 WHEN `GetCluster` returns a current endpoint, THE managed DSQL lifecycle SHALL
refresh the descriptor's endpoint without changing canonical identity.

3.8 WHEN the descriptor Region conflicts with the cluster ARN Region, THE managed DSQL
lifecycle SHALL fail startup as an identity mismatch.

3.9 WHEN an existing DSQL configuration supplies only an endpoint, THE embedded engine
SHALL reject it as missing canonical identity.

3.10 WHEN a managed descriptor is lost, THE managed DSQL lifecycle SHALL require restored
canonical identity or a new explicit create decision.

3.11 WHEN a recovered cluster is `CREATING` or `UPDATING`, THE managed DSQL lifecycle
SHALL wait with a bounded deadline.

3.12 WHEN a recovered cluster is `IDLE` or `INACTIVE`, THE managed DSQL lifecycle SHALL
wake it through a database connection attempt.

3.13 WHEN a recovered cluster becomes `ACTIVE`, THE managed DSQL lifecycle SHALL proceed
to schema compatibility checks.

3.14 WHEN a recovered cluster is `FAILED`, `DELETING`, `DELETED`, `PENDING_SETUP`, or
`PENDING_DELETE`, THE managed DSQL lifecycle SHALL fail with the observed status.

3.15 WHEN an AWS response supplies `retryAfterSeconds`, THE managed DSQL lifecycle SHALL
respect it within the startup deadline.

3.16 WHEN AWS rejects creation because of a service quota, THE managed DSQL lifecycle
SHALL expose the service code, quota code, and remediation context in the startup error.

### Requirement 4: Release-bound Schema Compatibility Contract

**User Story:** As a Tokeira release operator, I want every binary to declare exactly
which DSQL schemas it can use so that upgrades, rollbacks, and corrupted migrations fail
safely.

#### Acceptance Criteria

4.1 THE Tokeira build metadata SHALL declare the Tokeira release associated with the
schema compatibility contract.

4.2 THE Tokeira build metadata SHALL declare a minimum supported schema version.

4.3 THE Tokeira build metadata SHALL declare a target schema version.

4.4 THE Tokeira build metadata SHALL declare a maximum readable schema version.

4.5 THE Tokeira build metadata SHALL declare a migration-set digest.

4.6 THE build process SHALL compute the migration-set digest deterministically from the
ordered recognized migration set.

4.7 THE build process SHALL fail when schema contract versions are not ordered as
`MIN <= TARGET <= MAX`.

4.8 BEFORE managed embedded DSQL is released as durable storage, THE storage migration
contract SHALL declare its initial immutable baseline.

4.9 AFTER the immutable baseline is declared, THE build process SHALL reject mutation of
a released migration identity or content.

4.10 THE DSQL schema ledger SHALL retain the version, identity, and checksum of every
applied migration.

4.11 WHEN a known applied migration has a different checksum, THE compatibility checker
SHALL reject the schema.

4.12 WHEN applicable persisted schema metadata has a different migration-set digest,
THE compatibility checker SHALL reject the schema.

4.13 WHEN the current schema version is below the minimum supported version, THE
compatibility checker SHALL reject the schema.

4.14 WHEN the current schema version exceeds the maximum readable version, THE
compatibility checker SHALL reject the schema.

4.15 WHEN an applied schema version is unknown to the release's readable migration set,
THE compatibility checker SHALL reject the schema.

4.16 WHEN the current schema is newer than the target but no newer than the maximum
readable version, THE compatibility checker SHALL avoid a down-migration.

4.17 WHEN schema compatibility is rejected, THE compatibility checker SHALL report the
observed version, supported interval, target version, and mismatch category.

### Requirement 5: Mode-specific Migration Policy

**User Story:** As an operator, I want migration authority to match cluster ownership so
that managed startup is convenient without silently mutating operator-controlled or
distributed databases.

#### Acceptance Criteria

5.1 WHEN managed DSQL mode omits a migration policy, THE embedded engine SHALL select the
automatic migration policy.

5.2 WHEN existing DSQL mode omits a migration policy, THE embedded engine SHALL reject
the configuration.

5.3 WHEN distributed DSQL mode omits a migration policy, THE Tokeira process SHALL
reject the configuration.

5.4 WHEN validate-only policy observes `MIN <= C < TARGET`, THE compatibility checker
SHALL return a migration-required failure without changing the schema.

5.5 WHEN automatic policy observes `MIN <= C < TARGET`, THE migration runner SHALL apply
the ordered verified migrations through `TARGET`.

5.6 BEFORE applying a migration, THE migration runner SHALL validate the checksums of all
previously applied known migrations.

5.7 WHEN multiple contenders request automatic migration, THE migration runner SHALL
serialize migration ownership in DSQL.

5.8 WHEN a migration loses its ownership fence, THE migration runner SHALL stop applying
new statements.

5.9 WHEN a migration fails, THE embedded engine SHALL fail startup without serving
workflow requests.

5.10 WHEN automatic migration reaches `TARGET`, THE migration runner SHALL persist the
release's applicable schema compatibility metadata.

5.11 WHEN no schema tables exist in a newly created managed cluster, THE migration runner
SHALL install the complete Tokeira schema automatically.

5.12 WHEN an existing or distributed cluster explicitly selects automatic migration,
THE migration runner SHALL apply the same compatibility and fencing checks as managed
mode.

5.13 WHEN an existing or distributed cluster selects validate-only migration, THE
migration runner SHALL leave schema state unchanged.

### Requirement 6: DynamoDB-free Embedded DSQL Admission Control

**User Story:** As an embedding host, I want DSQL-only durable execution with bounded
local resource use so that embedded Tokeira needs no DynamoDB and cannot create a
connection storm.

#### Acceptance Criteria

6.1 WHILE one embedded process uses DSQL, THE connection director SHALL use a
process-local connection-creation rate limiter.

6.2 WHILE one embedded process uses DSQL, THE connection director SHALL use
process-local concurrency admission.

6.3 THE embedded DSQL startup path SHALL NOT construct a DynamoDB client.

6.4 THE embedded DSQL startup path SHALL NOT require DynamoDB table names.

6.5 THE embedded DSQL profile SHALL default to eight physical database connections.

6.6 THE embedded DSQL profile SHALL reject a physical connection limit greater than
sixteen.

6.7 THE embedded DSQL profile SHALL default to two concurrent connection creations.

6.8 THE embedded DSQL profile SHALL reject a concurrent connection-creation limit
greater than four.

6.9 THE embedded DSQL profile SHALL default to eight connection creations per second.

6.10 THE embedded DSQL profile SHALL reject a connection-creation rate greater than
sixteen per second.

6.11 THE embedded DSQL profile SHALL default to a connection-creation burst of two.

6.12 THE embedded DSQL profile SHALL reject a connection-creation burst greater than
four.

6.13 THE connection director SHALL bound each connection class with explicit admission
permits.

6.14 WHEN no class permit or physical slot is available, THE connection director SHALL
wait without opening an ad hoc connection.

6.15 THE embedded pool SHALL keep its maximum idle connection count equal to its maximum
connection count.

6.16 THE connection director SHALL expose leaked-permit and leaked-connection diagnostics
without including credentials.

6.17 WHEN shutdown begins, THE connection director SHALL reject new admissions.

6.18 WHEN shutdown completes, THE connection director SHALL close its physical pool.

### Requirement 7: Exclusive Managed Embedded Ownership

**User Story:** As an embedding host, I want one live engine to own a managed embedded
cluster so that a second process cannot accidentally turn embedded mode into an
uncoordinated distributed deployment.

#### Acceptance Criteria

7.1 BEFORE serving requests in managed DSQL mode, THE embedded engine SHALL acquire an
exclusive ownership claim in the target DSQL cluster.

7.2 THE ownership claim SHALL identify the managed cluster by cluster ID and ARN.

7.3 THE ownership claim SHALL include a unique process incarnation identifier.

7.4 THE ownership claim SHALL expire unless renewed by its owning process.

7.5 WHILE serving requests, THE embedded engine SHALL renew its ownership claim before
expiry.

7.6 WHEN another unexpired ownership claim exists, THE embedded engine SHALL fail
startup with the current owner and expiry information.

7.7 WHEN the ownership claim is lost or fenced, THE embedded engine SHALL stop admitting
new workflow operations.

7.8 WHEN graceful shutdown reaches the storage-drain phase, THE embedded engine SHALL
release its ownership claim.

7.9 WHEN an owner crashes, THE ownership mechanism SHALL permit takeover only after the
prior claim expires.

7.10 THE managed embedded configuration SHALL reject any request for multi-process
operation.

7.11 THE Tokeira configuration SHALL direct intentional multi-process operation to
distributed mode.

### Requirement 8: Ordered and Inspectable Startup

**User Story:** As an embedding host, I want startup to complete lifecycle checks in a
safe order and return structured results so that the host never receives a half-ready
engine.

#### Acceptance Criteria

8.1 WHEN managed embedded startup begins, THE embedded engine SHALL load and validate the
cluster descriptor before an AWS mutation.

8.2 WHEN cluster creation or recovery succeeds, THE embedded engine SHALL establish a
bounded DSQL connection before schema work.

8.3 WHEN schema compatibility permits startup, THE embedded engine SHALL acquire the
exclusive ownership claim before serving requests.

8.4 BEFORE startup returns success, THE embedded engine SHALL restore any runtime state
required from authoritative DSQL history.

8.5 BEFORE startup returns success, THE embedded engine SHALL make the in-process service
available to the host.

8.6 IF any managed startup phase fails, THEN THE embedded engine SHALL return no usable
service handle.

8.7 IF any managed startup phase fails after pool creation, THEN THE embedded engine
SHALL close the pool before returning.

8.8 IF any managed startup phase fails after ownership acquisition, THEN THE embedded
engine SHALL attempt to release the ownership claim before returning.

8.9 WHEN startup succeeds, THE startup report SHALL include the cluster ID, ARN, Region,
and endpoint.

8.10 WHEN startup succeeds, THE startup report SHALL include the observed, target, and
maximum readable schema versions.

8.11 WHEN startup succeeds, THE startup report SHALL include the migration-set digest
identifier without migration SQL.

8.12 WHEN startup succeeds, THE startup report SHALL include the migration and ownership
outcomes.

8.13 THE startup report SHALL NOT include credentials, authentication tokens, or the
creation client token.

8.14 THE startup sequence SHALL enforce a host-configurable bounded deadline.

### Requirement 9: Safe Shutdown and Separate Destruction

**User Story:** As an operator, I want ordinary engine lifecycle operations to preserve
the managed database so that dropping an embedded handle cannot destroy durable state.

#### Acceptance Criteria

9.1 WHEN an embedded engine handle is dropped, THE managed DSQL lifecycle SHALL NOT call
`DeleteCluster`.

9.2 WHEN an embedded engine shuts down normally, THE managed DSQL lifecycle SHALL NOT
call `DeleteCluster`.

9.3 WHEN an embedded engine shuts down normally, THE managed DSQL lifecycle SHALL NOT
disable deletion protection.

9.4 THE managed DSQL lifecycle SHALL keep deletion protection enabled by default.

9.5 THE Tokeira administrative surface SHALL expose cluster destruction as a separate
explicit operation.

9.6 BEFORE cluster destruction changes AWS state, THE administrative surface SHALL
present a plan containing the canonical cluster ID and ARN.

9.7 BEFORE cluster destruction changes AWS state, THE administrative surface SHALL
require explicit confirmation.

9.8 WHEN confirmed destruction targets a protected cluster, THE administrative surface
SHALL disable deletion protection by canonical cluster ID.

9.9 AFTER deletion protection is disabled, THE administrative surface SHALL call
`DeleteCluster` by canonical cluster ID.

9.10 WHEN deletion is requested, THE administrative surface SHALL wait for an
unambiguous deleted or not-found result with a bounded deadline.

9.11 THE destruction operation SHALL NOT identify its target through tags or endpoint.

### Requirement 10: Host-owned, Composable Observability

**User Story:** As an embedding host, I want Tokeira telemetry to compose with my
existing OpenTelemetry or Logfire stack so that embedding the engine never mutates
process-global observability.

#### Acceptance Criteria

10.1 WHILE constructing an embedded engine, THE embedded engine SHALL NOT install a
global tracing subscriber.

10.2 WHILE constructing an embedded engine, THE embedded engine SHALL NOT install a
global OpenTelemetry tracer provider.

10.3 WHILE constructing an embedded engine, THE embedded engine SHALL NOT install a
global OpenTelemetry meter provider or metrics recorder.

10.4 WHILE constructing an embedded engine, THE embedded engine SHALL NOT replace the
host's text-map propagator.

10.5 WHILE constructing an embedded engine, THE embedded engine SHALL NOT start a
Tokeira-owned telemetry listener.

10.6 THE embedded engine SHALL emit tracing spans through composable library
instrumentation.

10.7 THE embedded engine SHALL emit metrics through composable library instrumentation.

10.8 THE embedded engine SHALL expose enough lifecycle information for the host to flush
its own providers after Tokeira shutdown completes.

10.9 WHEN Tokeira shutdown completes, THE embedded engine SHALL finish all spans it owns
before returning control to the host.

10.10 WHEN Tokeira shutdown completes, THE embedded engine SHALL NOT globally flush or
shut down host-owned providers.

10.11 THE embedded observability contract SHALL permit host exporters to use network
connections.

10.12 THE embedded engine SHALL use stable OpenTelemetry semantic conventions where an
applicable convention is stable in the host-pinned OpenTelemetry version.

10.13 WHEN no stable semantic convention applies, THE embedded engine SHALL use a
documented Tokeira attribute namespace.

### Requirement 11: Context Propagation and Durable Correlation

**User Story:** As an application operator, I want traces and metrics to correlate work
across service calls, activities, tools, handoffs, crashes, and restarts so that a
distributed durable execution can be diagnosed end to end.

#### Acceptance Criteria

11.1 WHEN `service_override` receives W3C Trace Context metadata, THE in-process server
span SHALL use the extracted remote context as its parent.

11.2 WHEN Tokeira initiates a Tokeira-mediated outbound call, THE call instrumentation
SHALL inject the current W3C Trace Context into the supported carrier.

11.3 WHEN Tokeira receives a Tokeira-mediated workflow task, THE workflow processing span
SHALL carry Workflow ID and Run ID.

11.4 WHEN Tokeira receives a Tokeira-mediated activity task, THE activity processing span
SHALL carry Workflow ID, Run ID, Activity ID, Activity Type, and attempt.

11.5 WHEN Tokeira enqueues or dispatches durable work, THE resulting telemetry SHALL
carry the stable identifiers available at that boundary.

11.6 WHEN Tokeira mediates a provider invocation, THE provider span SHALL inherit the
current trace context.

11.7 WHEN Tokeira mediates an MCP tool invocation, THE tool span SHALL inherit the
current trace context.

11.8 WHEN Tokeira mediates a handoff, THE handoff span SHALL link the sending and
receiving execution contexts.

11.9 WHEN a process restart creates a new trace, THE new telemetry SHALL retain the
stable execution identifiers of the resumed work.

11.10 THE embedded engine SHALL NOT persist transient OpenTelemetry context as
authoritative workflow history.

11.11 THE embedded engine SHALL NOT make correctness depend on successful telemetry
propagation or export.

11.12 THE embedded engine SHALL bound metric dimensions to avoid unbounded workflow,
run, activity, prompt, or tool identifiers as metric labels.

11.13 THE embedded engine SHALL make high-cardinality stable identifiers available on
traces and structured events rather than default metric labels.

11.14 WHEN trace context is absent or invalid, THE receiving boundary SHALL start a new
trace without rejecting the workflow operation.

### Requirement 12: Sensitive-data Exclusion

**User Story:** As a security-conscious host, I want useful operational telemetry without
capturing model or tool payloads and secrets by default.

#### Acceptance Criteria

12.1 BY DEFAULT THE embedded engine SHALL exclude prompt content from telemetry.

12.2 BY DEFAULT THE embedded engine SHALL exclude tool input content from telemetry.

12.3 BY DEFAULT THE embedded engine SHALL exclude tool output content from telemetry.

12.4 BY DEFAULT THE embedded engine SHALL exclude workflow and activity payload bodies
from telemetry.

12.5 THE embedded engine SHALL exclude AWS credentials from telemetry.

12.6 THE embedded engine SHALL exclude DSQL authentication tokens from telemetry.

12.7 THE embedded engine SHALL exclude the creation client token from telemetry.

12.8 WHEN an error contains a connection string, THE embedded engine SHALL redact its
credential-bearing components before emission.

12.9 WHEN the host explicitly enables content capture, THE observability boundary SHALL
apply host-provided redaction and size limits before emission.

12.10 WHEN the host does not provide an explicit content-capture policy, THE embedded
engine SHALL keep content capture disabled.

### Requirement 13: Verification Matrix and Failure Evidence

**User Story:** As a maintainer, I want the lifecycle and telemetry contracts verified at
their real boundaries so that unit-only success cannot hide restart, AWS, or context
propagation defects.

#### Acceptance Criteria

13.1 THE managed DSQL test suite SHALL verify that the client token is persisted before
the first mocked `CreateCluster` request.

13.2 THE managed DSQL test suite SHALL verify that a crash after `CreateCluster` reuses
the original token and resolves the original cluster.

13.3 THE managed DSQL test suite SHALL verify that recovery uses `GetCluster` by ID
without tag discovery.

13.4 THE managed DSQL test suite SHALL verify endpoint refresh without identity change.

13.5 THE managed DSQL test suite SHALL verify `IDLE` and `INACTIVE` wake-up behaviour.

13.6 THE managed DSQL test suite SHALL verify rejection of mismatched ID, ARN, and
Region.

13.7 THE schema test suite SHALL verify every row of the schema compatibility decision
table.

13.8 THE schema test suite SHALL verify rejection of a modified released migration.

13.9 THE schema test suite SHALL verify migration serialization and fencing.

13.10 THE embedded DSQL test suite SHALL verify operation with no DynamoDB client or
tables.

13.11 THE embedded DSQL test suite SHALL verify the physical pool and connection-creation
bounds under concurrent load.

13.12 THE ownership test suite SHALL verify that only one of two concurrent embedded
process incarnations is admitted.

13.13 THE ownership test suite SHALL verify takeover after crash expiry.

13.14 THE lifecycle test suite SHALL verify that engine drop and shutdown never invoke
cluster deletion or deletion-protection changes.

13.15 THE observability test suite SHALL verify that embedded startup leaves all
process-global telemetry state unchanged.

13.16 THE observability test suite SHALL verify parentage through `service_override`.

13.17 THE observability test suite SHALL verify context propagation through workflow and
activity boundaries.

13.18 THE host integration suite SHALL verify context propagation through providers,
MCP tools, and handoffs when those boundaries are host-mediated.

13.19 THE restart integration suite SHALL verify stable identifier correlation across a
process restart.

13.20 THE metrics integration suite SHALL verify bounded metric cardinality under many
workflow and activity identifiers.

13.21 THE shutdown integration suite SHALL verify that Tokeira-owned spans finish before
the host flushes its providers.

13.22 THE security test suite SHALL verify default exclusion of prompts, tool inputs,
tool outputs, payloads, credentials, and tokens.

13.23 BEFORE release, THE AWS integration suite SHALL verify create, crash recovery,
wake-up, schema installation, and explicit destruction against a real disposable Aurora
DSQL cluster.

13.24 WHEN a live AWS test cannot run in default CI, THE test documentation SHALL state
the required credentials, cost-bearing resources, and operator command.

### Requirement 14: Architectural Boundaries

**User Story:** As a Tokeira maintainer, I want managed embedded DSQL to fit existing
architectural boundaries so that storage convenience does not weaken deterministic
execution or history authority.

#### Acceptance Criteria

14.1 THE managed embedded DSQL implementation SHALL keep AWS API calls outside
`tokeira-kernel`.

14.2 THE managed embedded DSQL implementation SHALL keep DSQL I/O outside
`tokeira-kernel`.

14.3 THE managed embedded DSQL implementation SHALL keep ownership leases outside
`tokeira-kernel`.

14.4 THE managed embedded DSQL implementation SHALL keep telemetry propagation outside
`tokeira-kernel`.

14.5 THE managed embedded DSQL implementation SHALL preserve per-run history as the
authority for workflow state.

14.6 THE managed embedded DSQL implementation SHALL keep queues and telemetry outside the
workflow correctness path.

14.7 THE managed embedded DSQL implementation SHALL reuse the existing storage and
runtime contracts without adding a provider-specific kernel abstraction.

14.8 IF design work determines that a kernel change is necessary, THEN THE specification
workflow SHALL stop for an explicit product and architecture decision.

## Out of Scope

- Multi-Region Aurora DSQL cluster creation.
- Creating a general-purpose DSQL control plane or discovering clusters by tags.
- Treating one managed embedded cluster as a multi-process deployment.
- Changing Temporal API behaviour or the deterministic workflow transition model.
- Installing or configuring OpenTelemetry, Logfire, or another vendor exporter for the
  host.
- Defining provider, MCP, or handoff APIs that are owned by an external embedding
  framework.
- Capturing prompt, tool, workflow payload, or credential content by default.
- Deleting a managed cluster as a side effect of engine drop or shutdown.

## Requirements Gate

This document is the requirements phase only. `design.md` and `tasks.md` require
separate user approval under the repository's one-document consent gates.
