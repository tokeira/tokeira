# Requirements Document

## Introduction

This spec implements the OperatorService Nexus endpoint administration RPCs that are currently
stubbed (`get`/`create`/`update`/`delete`/`list_nexus_endpoint(s)` return `UNIMPLEMENTED` —
`crates/tokeira-edge/src/grpc/operator_service.rs:134-168`). It delivers the **endpoint admin
registry only**, ground-truthed to Temporal `v1.31.0` per `AGENTS §8`.

Every behaviour below is verified against the tagged source and cited inline. The authoritative
loci are:

- `service/frontend/nexus_endpoint_client.go @ v1.31.0` — request validation, API↔persistence
  spec translation, read-after-write reads, list/name-filter behaviour.
- `service/matching/nexus_endpoint_client.go @ v1.31.0` — the endpoint table owner: server-authored
  id, monotonic version, duplicate-name detection, version-conflict CAS.
- `common/dynamicconfig/constants.go @ v1.31.0` — the six limit knobs and their defaults.

A fix whose justification is "it makes the test pass" rather than "v1.31.0 does X, verified at
`<path>@v1.31.0`" is not acceptable (the Implementer mandate in
`temporal-functional-conformance/reference/FINDINGS.md`).

## Glossary

- **Nexus endpoint:** Operator-managed endpoint metadata used by Nexus task transport.
- **Endpoint version:** Monotonic conflict token authored by the endpoint-table owner; used for
  optimistic update/delete CAS.
- **Endpoint registry:** Durable store of Nexus endpoint definitions (the `NexusEndpointStore`).
- **RequestIssues:** v1.31.0's validation accumulator (`common/rpc`); multiple field issues are
  appended and returned **together** as a single `InvalidArgument` whose message concatenates them
  (`nexus_endpoint_client.go` uses `issues.GetError()`). Validation is not fail-fast on the first
  issue.

## Target State

`ImplementedSubset`: Nexus endpoint **admin registry only**. This spec does **not** claim
completion of Nexus task polling (`PollNexusTaskQueue`), operation execution / cancellation, the
Nexus HTTP handler, or worker task transport conformance — those are owned by
`edge-nexus-task-transport`, `kernel-nexus-operations`, `runtime-nexus-dispatch`,
`nexus-retry-policy`, and `nexus-multi-cluster`.

### Conformance scope (set expectations)

Executing this spec clears only the endpoint-admin suites:

- `TestNexusEndpointsFunctionalSuite` (15) — CRUD/list lifecycle.
- `TestNexusAPIValidationTestSuite` (2) — admission validation.

It does **not** clear `TestNexusApiTestSuiteWithTemporalFailures` (40),
`TestNexusApiTestSuiteWithLegacyErrorPaths` (40), or `TestNexusWorkflowTestSuite` (2), which are
Nexus operation-execution / task-transport tests. FINDINGS' C4 row is split accordingly (C4a admin
CRUD ≈ 17 tests here; C4b task execution elsewhere).

## Deliberate deviation from v1.31.0 internal topology (observable contract preserved)

In v1.31.0 the **frontend** OperatorService validates requests and forwards Create/Update/Delete to
the **matching** service, which owns the `nexus_endpoints` table; reads (Get/List) go straight to
persistence for read-after-write consistency
(`nexus_endpoint_client.go:30-34 @ v1.31.0` — "Create, Update, and Delete requests are forwarded to
matching service which owns the endpoints table. Read … requests are sent directly to persistence").

Tokeira does not split frontend/matching the same way; this spec collapses the table owner into a
single `NexusEndpointStore` reached behind OperatorService. This is an **internal-topology**
deviation, not an observable one: the **error codes, error messages, server-authored id/version
semantics, and read-after-write behaviour MUST match v1.31.0 regardless of where the table lives**.
Where v1.31.0 returns an error from the matching side (e.g. version mismatch), tokeira returns the
same gRPC code and message from the collapsed store.

## Endpoint Field Policy

| Field group | Current state | Target policy | Error if unsupported | Storage impact |
|---|---|---|---|---|
| Endpoint id | Stubbed | Server-authored UUID (`uuid` v4) | n/a (server-authored) | Registry key |
| Endpoint name | Stubbed | Non-empty, ≤ `NexusEndpointNameMaxLength` (200), matches `EndpointNameRegex`; unique | `INVALID_ARGUMENT` (validation), `ALREADY_EXISTS` (duplicate), `NOT_FOUND` (missing) | Unique name index |
| Target (Worker) | Stubbed | namespace set **and exists**, task queue ≤ `MaxIDLengthLimit` (1000) | `INVALID_ARGUMENT` (unset/empty/bad TQ), `FAILED_PRECONDITION` (namespace missing) | Registry value |
| Target (External) | Stubbed | non-empty URL ≤ `NexusEndpointExternalURLMaxLength` (4096), parseable, scheme ∈ {http, https} | `INVALID_ARGUMENT` | Registry value |
| Description | Stubbed | size ≤ `NexusEndpointDescriptionMaxSize` (20000 bytes) | `INVALID_ARGUMENT` | Registry value |
| Version token | Stubbed | Monotonic; required `> 0` on delete; CAS on update/delete | `FAILED_PRECONDITION` on mismatch (**not** `ABORTED`) | Registry CAS |
| Pagination | Stubbed | default `NexusEndpointListDefaultPageSize` (100), max `NexusEndpointListMaxPageSize` (1000); name-filter ignores page args | `INVALID_ARGUMENT` on bad page size | Registry scan |

## Configuration Surface (raise, never hardcode — Implementer mandate rule 3)

These six knobs govern v1.31.0 behaviour and MUST be modelled as tokeira config with the
v1.31.0-faithful defaults below, not inlined constants. All are global except the description size
(namespace-scoped upstream, but the endpoint client ignores namespace because endpoints are global
resources — `nexus_endpoint_client.go:60-62 @ v1.31.0`), so a global default is faithful.

| Knob | Upstream key | Default | Source |
|---|---|---|---|
| Endpoint name max length | `limit.endpointNameMaxLength` | 200 | `constants.go:544 @ v1.31.0` |
| External URL max length | `limit.endpointExternalURLMaxLength` | 4096 (`4*1024`) | `constants.go:549 @ v1.31.0` |
| Description max size (bytes) | `limit.endpointDescriptionMaxSize` | 20000 | `constants.go:554 @ v1.31.0` |
| Task-queue max length | `limit.maxIDLength` | 1000 | `constants.go:423 @ v1.31.0` |
| List default page size | `limit.endpointListDefaultPageSize` | 100 | `constants.go:559 @ v1.31.0` |
| List max page size | `limit.endpointListMaxPageSize` | 1000 | `constants.go:564 @ v1.31.0` |

## Requirements

### Requirement 1: Nexus Endpoint CRUD

**User Story:** As an operator, I want to manage Nexus endpoints, so that workflows can target named
Nexus services.

#### Acceptance Criteria

1. WHEN a `CreateNexusEndpoint` passes validation, THE registry SHALL create a new endpoint with a
   server-authored UUID id, an initial monotonic version, and create/update timestamps, and SHALL
   return the persisted endpoint in the external API shape
   (`nexus_endpoint_client.go:83-110 @ v1.31.0`).
2. WHEN `GetNexusEndpoint` is called with a valid id, THE registry SHALL return the endpoint read at
   current committed state (read-after-write), or `NOT_FOUND` if absent
   (`nexus_endpoint_client.go:165-188 @ v1.31.0`).
3. WHEN `UpdateNexusEndpoint` is called, THE registry SHALL validate the spec, then apply the
   mutation only if the supplied version equals the stored version, returning the new persisted
   endpoint (`nexus_endpoint_client.go:112-145 @ v1.31.0`,
   `service/matching/nexus_endpoint_client.go:112-160 @ v1.31.0`).
4. WHEN `DeleteNexusEndpoint` is called with a valid id and version `> 0`, THE registry SHALL remove
   the endpoint, returning `NOT_FOUND` if the id is absent
   (`nexus_endpoint_client.go:147-163 @ v1.31.0`,
   `service/matching/nexus_endpoint_client.go:200-220 @ v1.31.0`).
5. WHEN `ListNexusEndpoints` is called without a name filter, THE registry SHALL return endpoints in
   stable order with `(name, id)`-stable pagination bounded by the page-size knobs; WHEN called with
   a non-empty `name`, THE registry SHALL ignore `page_size`/`next_page_token`, scan, and return the
   single matching endpoint or an empty list (`nexus_endpoint_client.go:190-310 @ v1.31.0`).

### Requirement 2: Validation and Errors

**User Story:** As an operator, I want invalid endpoint mutations rejected before persistence with
the exact codes and messages v1.31.0 returns, so that bad endpoint definitions do not break runtime
dispatch and SDK/operator error handling matches.

All upsert (create/update) field validation accumulates via `RequestIssues` and returns a **single**
`INVALID_ARGUMENT` whose message concatenates the issues — not fail-fast
(`nexus_endpoint_client.go:validateUpsertSpec / getEndpointNameIssues @ v1.31.0`).

#### Acceptance Criteria

1. THE registry SHALL validate the endpoint **name**: non-empty ("endpoint name not set"), length
   ≤ `NexusEndpointNameMaxLength` ("endpoint name exceeds length limit of N"), and matching
   `EndpointNameRegex = ^[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9]$` ("endpoint name must match the regex:
   …") → `INVALID_ARGUMENT` (`nexus_endpoint_client.go:getEndpointNameIssues @ v1.31.0`).
2. THE registry SHALL validate the **target variant**: an unset variant → `INVALID_ARGUMENT`
   ("empty target variant" / "empty endpoint target");
   - **Worker** target: namespace unset → issue "target namespace not set"; namespace set but not
     found → **`FAILED_PRECONDITION`** "could not verify namespace referenced by target exists: …"
     (returned immediately, not accumulated); task queue invalid → issue "invalid target task queue:
     …" bounded by `MaxIDLengthLimit`.
   - **External** target: empty URL → "empty target URL"; length > `NexusEndpointExternalURLMaxLength`
     → "target URL length exceeds limit of N"; unparseable → "invalid target URL: …"; scheme not
     http/https → "invalid target URL scheme: …, expected http or https".
   (`nexus_endpoint_client.go:validateUpsertSpec @ v1.31.0`.)
3. THE registry SHALL validate the **description** size ≤ `NexusEndpointDescriptionMaxSize`
   ("description size exceeds limit of N") → `INVALID_ARGUMENT`.
4. THE registry SHALL validate **id-bearing** requests (Get/Delete): id non-empty ("endpoint ID not
   set") and a parseable UUID ("malformed endpoint ID: …"); Delete additionally requires version
   `> 0` ("endpoint version is non-positive") → `INVALID_ARGUMENT`
   (`getEndpointIDIssues`, `validateDeleteRequest`, `validateGetRequest @ v1.31.0`).
5. WHEN a create encounters a duplicate name, THE registry SHALL return `ALREADY_EXISTS` "error
   creating Nexus endpoint. Endpoint with name %v already registered"
   (`service/matching/nexus_endpoint_client.go:100 @ v1.31.0`).
6. WHEN an update/delete targets a missing id, THE registry SHALL return `NOT_FOUND` ("error
   updating Nexus endpoint. endpoint ID %v not found" / "error deleting nexus endpoint with ID: %v")
   (`service/matching/nexus_endpoint_client.go:152,218 @ v1.31.0`).
7. WHEN an update/delete supplies a version that does not equal the stored version, THE registry
   SHALL return **`FAILED_PRECONDITION`** "nexus endpoint version mismatch. received: %v expected
   %v" — **never `ABORTED`** (`service/matching/nexus_endpoint_client.go:155-156 @ v1.31.0`).
8. WHEN a list request supplies a `page_size` greater than `NexusEndpointListMaxPageSize`, THE
   registry SHALL return `INVALID_ARGUMENT` (`nexus_endpoint_client.go:validatePageSize @ v1.31.0`).

### Requirement 3: Runtime Integration

**User Story:** As a workflow author, I want runtime Nexus dispatch to use operator-managed
endpoints, so that endpoint admin changes affect future dispatches.

> **Prerequisite (verified 2026-06-18).** `runtime-nexus-dispatch` shipped a **static**
> `NexusEndpointRegistry` — an immutable `Arc<HashMap<String, NexusEndpointConfig>>` built once at
> `TokeiraRuntime::new` (`crates/tokeira-runtime/src/nexus.rs:72-86`), with no insert/remove and no
> trait seam, wired empty today (`NexusEndpointRegistry::default()`, `runtime/mod.rs:337-340`). The
> dispatch read-path (`resolve`) exists and is consumed by `publisher.rs`, but it cannot observe
> runtime CRUD. This requirement therefore includes making the registry **live-backed**; it is not a
> pre-existing seam this spec merely calls.

#### Acceptance Criteria

1. THE runtime endpoint registry SHALL become live-backed by the `NexusEndpointStore` (the source of
   truth), replacing the static construction-time `Arc<HashMap>`, so that `resolve` reflects current
   committed state.
2. WHEN an endpoint is created and committed, THE runtime Nexus dispatch lookup SHALL resolve it.
3. WHEN an endpoint is deleted, THE runtime Nexus dispatch lookup SHALL NOT resolve it for new
   dispatch.
4. Runtime reads SHALL use a neutral store trait and SHALL NOT import OperatorService implementation
   types (no edge↔runtime cycle).
5. THE `resolve` API SHALL return an **owned** `NexusEndpointConfig` (not `Option<&…>`), because a
   store/lock-backed registry cannot hand out a borrow; the `publisher.rs` call site SHALL be updated
   accordingly.
