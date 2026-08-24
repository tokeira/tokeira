# Implementation Plan: Scoped Worker Authorization

## Overview

Implement a default-inert, Tokeira-native Worker authorization capability on top of the
`authorization-foundation` seams. The work adds a normalized exact Worker scope, strict JWT and
static identity mappings, server-authored task-origin evidence, durable token provenance,
two-phase edge admission, atomic Worker heartbeat validation, session-bound shutdown, explicit
operator configuration, and end-to-end standard-SDK evidence. It does not change kernel state,
commands, transitions, history, lane routing, projection semantics, or delivery ordering.

## Tasks

- [x] 1. Add the shared task-origin model and pure Worker-scope decision engine
  - [x] 1.1 Add `WorkerTaskClass` and `WorkerTaskOrigin` to `tokeira-types`
    - Define the transport-neutral task-class enum and exact origin fields for namespace ID,
      normal task queue, task class, deployment, and build ID.
    - Add stable conversion helpers needed by storage without placing authorization policy in
      `tokeira-types`.
    - Add serde round-trip and exact numeric-mapping unit tests.
    - _Requirements: 4.8-4.10, 5.5-5.12, 6.1-6.6_
  - [x] 1.2 Add the normalized `WorkerScope` model to `tokeira-auth`
    - Add the internal `tokeira-types` dependency.
    - Implement strict construction, duplicate rejection before `BTreeSet` normalization,
      lexical queue ordering, non-blank exact values, and wildcard rejection.
    - Add bounded construction and denial error enums whose public use remains generic.
    - _Requirements: 1.1, 1.5-1.10, 11.4-11.6_
  - [x] 1.3 Add the fixed Worker operation and target types
    - Define the code-owned `WorkerOperation`, `WorkerTarget`, `WorkerCallTarget`, and fixed
      operation-to-target-shape rules.
    - Keep the operation matrix non-configurable and transport-independent.
    - _Requirements: 1.2-1.3, 4.2-4.5, 5.1-5.12, 8.1-8.2, 9.2-9.4, 9.7, 9.12_
  - [x] 1.4 Extend Claims, CallTarget, and DefaultAuthorizer with attenuation
    - Add `Claims.worker_scope` and `CallTarget.worker`.
    - Preserve the exact universal `Health/Check` and `GetSystemInfo` exception before claims.
    - Preserve the ordinary numeric-role path unchanged; when a Worker scope exists, ignore
      roles and fail closed for every non-health action without an allowed Worker target.
    - _Requirements: 1.2-1.4, 4.2-4.5, 11.1-11.3_
  - [x] 1.5 Property test: Property 1 — Worker-Scope normalization and validation
    - Implement a workspace-`proptest` reference-model test with at least 100 generated cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 1: Worker-Scope normalization and validation`
    - _Requirements: 1.1, 1.5-1.10, 12.1_
  - [x] 1.6 Property test: Property 4 — Scoped authorizer decision matrix
    - Generate claims, operations, namespaces, and Worker targets and compare the real
      authorizer with the fixed decision-matrix reference model for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 4: Scoped authorizer decision matrix`
    - _Requirements: 1.2-1.4, 4.2-4.5, 5.1-5.12, 8.1-8.2, 9.2-9.4, 9.7, 9.12, 11.1-11.3, 12.4_
  - [x] 1.7 Property test: Property 12 — Ordinary-identity preservation
    - Compare every generated ordinary Claims/CallTarget decision and principal with the
      authorization-foundation reference behavior for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 12: Ordinary-identity preservation`
    - _Requirements: 1.4, 4.7, 11.1-11.3_

- [x] 2. Implement strict signed and configured Worker-scope resolution
  - [x] 2.1 Parse the fixed versioned JWT claim after normal JWT verification
    - Parse only `tokeira_worker_scope`, require version 1, deny unknown fields and wrong types,
      and pass resource values through `WorkerScope::try_new`.
    - Reject a malformed present claim instead of falling back to ordinary roles; preserve
      current behavior when the claim is absent.
    - Ensure errors and diagnostics never render bearer material.
    - _Requirements: 2.1-2.10_
  - [x] 2.2 Add reusable configured `WorkerScopeRules`
    - Reuse `GlobPattern` for subject/ARN matching.
    - Resolve zero matches to no scope, one or repeated-equal matches to one normalized scope,
      and distinct matching scopes to an authentication conflict.
    - Keep ordinary grants independent; they cannot widen the resolved scope.
    - _Requirements: 3.3, 3.5-3.12_
  - [x] 2.3 Wire scope rules into JWT issuer and AWS STS authentication
    - Extend `JwtIssuerProfile` and `StsAuthenticator` with `WorkerScopeRules`.
    - Resolve signed and configured JWT scopes with the equality/conflict rule; resolve STS
      scopes from verified ARN rules only.
    - Leave multi-source issuer routing and ordinary principal derivation unchanged.
    - _Requirements: 2.7-2.9, 3.5-3.12, 4.7_
  - [x] 2.4 Add strict typed configuration and indexed validation
    - Add `jwt.issuers[].worker_scopes[]` and `aws_iam.worker_scopes[]` typed structures.
    - Validate patterns and Worker scopes with indexed `ValidationError::Field` paths.
    - Treat AWS IAM as non-empty when it contains an ordinary grant or Worker-scope rule, while
      retaining the existing error for a source with neither.
    - Do not add enablement, claim-name, operation-list, TTL, wildcard, or signing-secret knobs.
    - _Requirements: 3.1-3.4, 11.7-11.9_
  - [x] 2.5 Property test: Property 2 — Scope-source resolution is non-composable
    - Compare generated identities, rule sets, optional signed scopes, and role grants with a
      distinct-normalized-scope reference model for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 2: Scope-source resolution is non-composable`
    - _Requirements: 3.3, 3.5-3.12, 12.3_
  - [x] 2.6 Property test: Property 3 — Fixed JWT claim parsing is fail-closed
    - Generate absent, valid, malformed, unknown-version, unknown-field, and wrongly typed JSON
      claim values and compare parsing with a strict version-1 reference model for at least 100
      cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 3: Fixed JWT claim parsing is fail-closed`
    - _Requirements: 2.1-2.10, 12.2_
  - [x] 2.7 Property test: Property 11 — Configuration validation and round-trip
    - Generate valid and invalid authorization configurations; prove lossless TOML round-trip,
      indexed validation failures, and the AWS IAM source non-empty rule over at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 11: Configuration validation and round-trip`
    - _Requirements: 3.1-3.4_

- [x] 3. Checkpoint: shared types, auth, and config are green
  - Run formatting plus focused check, clippy, and tests for `tokeira-types`, `tokeira-auth`, and
    `tokeira-config`.
  - Verify ordinary authorization regressions and all Property 1-4, 11, and 12 tests are green.

- [x] 4. Add the durable Worker-task provenance registry
  - [x] 4.1 Define provenance records, digesting, and the storage trait
    - Add `WorkerTaskProvenance`, `ProvenancePut`, bounded storage errors, and
      `WorkerTaskProvenanceStore`.
    - Compute the key from the exact public token bytes with
      `tokeira-worker-task-provenance-v1\0` domain separation and workspace `sha2`.
    - Store no raw token, subject, bearer, role, task payload, Workflow ID, Activity ID, or Run
      ID.
    - _Requirements: 4.3, 4.8-4.10, 6.1-6.7, 6.12-6.13_
  - [x] 4.2 Implement provenance in `InMemoryStore`
    - Implement exact-record idempotent put, conflicting-digest corruption detection,
      non-authoritative expired reads, idempotent delete, and bounded expiry deletion.
    - Add deterministic clock-boundary and failure-injection unit tests.
    - _Requirements: 6.5-6.7, 6.12-6.13_
  - [x] 4.3 Add contiguous DSQL migrations for provenance and expiry
    - Re-read the migration tail at implementation time and allocate the next two contiguous
      versions.
    - Add one base-table `CREATE TABLE` statement and one `CREATE INDEX ASYNC` statement in
      separate forward-only migrations accepted by the DSQL DDL validator.
    - Keep all existing authoritative run-state migrations byte-for-byte unchanged.
    - _Requirements: 4.8-4.10, 6.5, 6.12-6.13_
  - [x] 4.4 Implement the DSQL provenance repository
    - Encode/decode every origin field explicitly, enforce exact duplicate semantics without
      overwrite, exclude expired rows on read, and delete expired rows in bounded batches.
    - Map connectivity failures separately from corruption/conflict so the edge can return
      `UNAVAILABLE` versus `INTERNAL`.
    - Add SQL-shape, row-codec, duplicate, expiry, and DDL tests through existing storage seams.
    - _Requirements: 4.3, 6.5-6.7, 6.12-6.13_
  - [x] 4.5 Property test: Property 6 — Provenance-store state machine
    - Run generated put/get/delete/expire sequences against the in-memory implementation and a
      pure DSQL record model, comparing both with a reference map for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 6: Provenance-store state machine`
    - _Requirements: 4.3, 4.10, 6.5-6.7, 6.12-6.13_

- [x] 5. Checkpoint: provenance storage is green
  - Run formatting plus focused check, clippy, and tests for `tokeira-storage`.
  - Run the migration-contiguity and DSQL DDL-validator tests and confirm Property 6 is green.

- [x] 6. Surface the server-authoritative origin of every started Worker task
  - [x] 6.1 Add origin to started Workflow and Activity task DTOs
    - Populate namespace ID and stable normal queue from authoritative run/activity state.
    - Populate deployment and build from the actual final offered/dispatch queue after routing,
      including sticky Workflow delivery.
    - Preserve all ordinary and unversioned task behavior; an unversioned origin is simply
      ineligible for scoped exposure.
    - _Requirements: 4.8-4.10, 5.5-5.12, 6.1-6.6_
  - [x] 6.2 Add origin to Query and Nexus task DTOs and correlations
    - Populate Query origin from the actual `QueryTask.queue`.
    - Populate Nexus origin from `NexusQueueKey`, retain it in correlation, and validate
      namespace/queue agreement with the public token before a response can use it.
    - _Requirements: 5.7, 6.1-6.6, 6.10-6.11, 10.5_
  - [x] 6.3 Add focused origin-construction tests
    - Cover normal and sticky Workflow queues, routed Activity queues, Query delivery, Nexus
      delivery, exact version fields, and rejection of incomplete origins for scoped use.
    - Prove sticky authorization uses the stable `normal_name` while provenance records the
      actual final versioned delivery target.
    - _Requirements: 5.5-5.12, 6.1-6.6, 12.5-12.6_

- [x] 7. Add two-phase request-aware edge authorization
  - [x] 7.1 Make `Action` classification explicit for the fixed Worker surface
    - Split all four Activity By-ID variants from token-bearing responses.
    - Add a total `Action::worker_operation()` mapping for every allowed matrix row and no
      wildcard fallback.
    - Retain existing v1.31.0 classifications for ordinary identities.
    - _Requirements: 4.4-4.5, 6.9, 11.1-11.3, 12.4, 12.7_
  - [x] 7.2 Implement preflight and final target authorization in `EdgeInterceptors`
    - Add `begin_worker_preflight` and `authorize_worker_target`.
    - Reuse the authenticated claims between phases; never repeat JWT/JWKS or STS verification.
    - Preserve explicit-versus-omitted namespace error precedence and expose no resource detail
      in generic authorization denials.
    - _Requirements: 4.1-4.7, 6.8, 10.6_
  - [x] 7.3 Retain and normalize every poll target field before any waiter or claim
    - Preserve normal/sticky queue data, `normal_name`, worker instance/control keys, and
      `WorkerDeploymentOptions`.
    - Admit scoped polls only in exact VERSIONED mode with both deployment and build matching;
      deny deprecated-only, partial, unversioned, missing-normal-name, and standalone forms.
    - _Requirements: 5.1-5.15, 10.1-10.4_
  - [x] 7.4 Add bounded denial metrics and safe public formatting
    - Map every scoped rejection to exactly one bounded label and the generic public
      `PERMISSION_DENIED` response.
    - Keep coordinates, bearer bytes, presigned STS URLs, and token bytes out of errors, logs,
      and metrics.
    - _Requirements: 2.10, 11.4-11.6, 11.10-11.11_
  - [x] 7.5 Property test: Property 5 — Poll-target normalization
    - Generate normal/sticky queue shapes and deployment options and compare normalization with
      the exact VERSIONED reference model for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 5: Poll-target normalization`
    - _Requirements: 5.1-5.15, 12.5_
  - [x] 7.6 Property test: Property 13 — Bounded denial classification
    - Generate every internal denial reason and arbitrary sensitive coordinates; prove one
      bounded metric label and secret-free public formatting for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 13: Bounded denial classification`
    - _Requirements: 11.4-11.6_

- [x] 8. Add scoped Worker sessions and atomic heartbeat admission
  - [x] 8.1 Implement `ScopedWorkerSessionRegistry`
    - Key sessions by stable namespace ID, subject, and worker instance key.
    - Fix scope, identity, normal queue, and non-empty control queue on first fully authorized
      poll; permit only monotonic addition of authorized sticky queues.
    - Use the existing Worker/poller-history horizon for bounded expiry without making session
      state durable or task-authoritative.
    - _Requirements: 5.13-5.14, 9.7-9.11_
  - [x] 8.2 Add atomic repeated-heartbeat storage
    - Extend `HeartbeatStore` with `insert_batch(Vec<HeartbeatObservation>)`.
    - Implement one-lock all-or-nothing mutation in memory and one-transaction semantics for
      durable implementations; preserve existing single-observation behavior through the batch
      path.
    - Add structural validation and failure-injection tests proving no partial write.
    - _Requirements: 9.1-9.6, 12.10_
  - [x] 8.3 Authorize Worker heartbeat, Nexus piggyback heartbeat, and shutdown before effects
    - Translate and validate the complete repeated heartbeat set before insertion.
    - Reuse the same validator for Nexus piggyback heartbeats.
    - Require an exact scoped session match before shutdown heartbeat, sticky denial, or poll
      cancellation; missing or conflicting sessions fail closed.
    - _Requirements: 9.1-9.12, 10.5, 12.10-12.12_
  - [x] 8.4 Property test: Property 8 — Heartbeat batch atomicity
    - Generate preexisting store state, repeated heartbeat batches, scope mismatches, and
      insertion failures; compare with an all-or-nothing reference model for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 8: Heartbeat batch atomicity`
    - _Requirements: 9.1-9.6, 12.10_
  - [x] 8.5 Property test: Property 9 — Scoped Worker-session monotonicity
    - Generate poll/shutdown observation sequences and compare registry state and shutdown
      eligibility with the monotonic-session reference model for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 9: Scoped Worker-session monotonicity`
    - _Requirements: 5.13-5.14, 9.7-9.11, 12.11_

- [x] 9. Checkpoint: origin, two-phase admission, sessions, and heartbeats are green
  - Run formatting plus focused check, clippy, and tests for `tokeira-runtime`,
    `tokeira-edge`, and affected auth/types crates.
  - Verify Properties 5, 8, 9, and 13 and ordinary Worker heartbeat/shutdown regressions are
    green.

- [x] 10. Bind every scoped task token to durable provenance before exposure
  - [x] 10.1 Inject provenance storage into WorkflowService and centralize token registration
    - Add helpers that serialize the final public token bytes, compute the digest, derive expiry
      from the existing task deadline, and insert the exact origin.
    - Distinguish insert unavailability (`UNAVAILABLE`) from conflicting-digest corruption
      (`INTERNAL`); expose no token after either failure.
    - Keep ordinary identities on the current path without provenance writes.
    - _Requirements: 4.3, 4.8-4.10, 6.1-6.7, 6.12-6.13_
  - [x] 10.2 Register provenance for all scoped poll and direct-return sites
    - Cover Workflow, Activity, Query-on-Workflow-poll, and Nexus poll responses.
    - Insert only after the authoritative task-start/correlation commit and before returning the
      public token.
    - On insertion failure, withhold the token and rely on existing task timeout/retry recovery;
      add no rollback command or queue repair effect.
    - _Requirements: 4.3, 5.13, 6.1-6.7, 10.3-10.5_
  - [x] 10.3 Register and filter inline/eager return sites
    - Cover returned Workflow tasks and eager Activity tasks from Workflow-task completion.
    - Check scope before claiming optional work; when out of scope, do not claim it and preserve
      ordinary durable dispatch.
    - When an in-scope task is claimed but provenance insertion fails, withhold it and preserve
      timeout/retry recovery.
    - _Requirements: 7.3-7.8, 12.9_
  - [x] 10.4 Authorize token responses from exact non-expired provenance
    - For explicit namespaces, preflight before token details; for omitted namespaces, preserve
      existing decode/backfill precedence.
    - Require stable namespace-ID agreement and authorize every exact origin coordinate before
      the existing runtime/correlation fence.
    - Return `PERMISSION_DENIED` for missing/expired/mismatched provenance and `UNAVAILABLE` for
      store lookup failure.
    - _Requirements: 4.1-4.7, 6.1-6.8, 6.10-6.13_
  - [x] 10.5 Implement terminal deletion, heartbeat retention, and bounded expiry maintenance
    - Delete provenance only after successful terminal Workflow, Activity, Query, or Nexus
      consumption; retain it for Activity heartbeat.
    - Treat post-success delete failure as cleanup debt, never as authority to repeat consumed
      work.
    - Run bounded expiry deletion through the existing service cancellation/lifecycle pattern;
      derive expiry from task deadlines and add no TTL configuration.
    - _Requirements: 6.5-6.7, 6.10-6.13_
  - [x] 10.6 Property test: Property 7 — Exact-token origin binding
    - Generate token bytes, origins, and scopes; mutate every byte and every coordinate and
      compare authorization with the exact-binding reference model for at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 7: Exact-token origin binding`
    - _Requirements: 6.1-6.7, 6.10-6.11, 12.6_

- [x] 11. Preserve Workflow-completion semantics while filtering returned work
  - [x] 11.1 Enforce cross-namespace command restrictions without queue overreach
    - Preserve every valid same-namespace Workflow-task command, including scheduling an
      Activity or child Workflow on a queue outside the poll allowlist.
    - Reject the complete Workflow-task completion before mutation when any command targets
      another namespace.
    - _Requirements: 7.1-7.2, 12.8_
  - [x] 11.2 Filter optional returned tasks by their actual origin
    - Expose inline Workflow and eager Activity tasks only when every actual origin coordinate
      matches scope and provenance insertion succeeds.
    - Withhold unauthorized optional returns without dropping or consuming their durable
      dispatch.
    - _Requirements: 7.3-7.8, 12.9_
  - [x] 11.3 Property test: Property 10 — Workflow-completion return filtering
    - Generate same- and cross-namespace commands plus inline/eager origins and compare
      completion acceptance, returned tasks, and preserved dispatch with a reference model for
      at least 100 cases.
    - Tag: `// Feature: scoped-worker-authorization, Property 10: Workflow-completion return filtering`
    - _Requirements: 7.1-7.8, 12.8-12.9_

- [x] 12. Close every alternate Worker path and prove denial is side-effect free
  - [x] 12.1 Add queue-only scoped `DescribeTaskQueue`
    - Authorize the stable normal queue declared by the request.
    - Treat report mode, selectors, task-queue type, and stats flags only as response-shape
      controls; preserve the complete PollerInfo result without credential-version filtering.
    - Deny Worker inventory and all namespace-wide reads/writes absent from the fixed matrix.
    - _Requirements: 8.1-8.7, 12.15_
  - [x] 12.2 Close CHASM, legacy-token, gateway, query, Nexus, and By-ID bypasses
    - Ensure scoped standalone Activity polls deny before entering the CHASM bridge.
    - Route direct gRPC and HTTP/gRPC-gateway requests through the same admission decision.
    - Require provenance and existing correlation/fencing on legacy query and Nexus response
      paths.
    - Prove every Activity By-ID response and every non-matrix API denies a scoped identity.
    - _Requirements: 6.9, 9.12, 10.1-10.8, 12.7, 12.12_
  - [x] 12.3 Add token-error-precedence and existing-status regression tests
    - Cover explicit and omitted namespaces, malformed tokens, namespace mismatch, stale/fenced
      tokens, missing/expired provenance, provenance-store outage, and digest conflict.
    - Assert the exact external codes and generic messages in the design's Error Handling table.
    - _Requirements: 4.6-4.7, 6.8, 10.6_
  - [x] 12.4 Add instrumented no-effect-before-authorization tests
    - Instrument broker waiters/claims, task starts, Query/Nexus correlations, heartbeat state,
      sessions, poll cancellation, CHASM bridge calls, and committed transitions.
    - For each mismatch path, prove every observation remains identical to pre-request state.
    - _Requirements: 4.1-4.3, 4.6, 5.13, 6.7, 9.5-9.11, 10.1-10.8, 12.12_
  - [x] 12.5 Prove provenance is never task authority
    - Exercise creation, retention, deletion, expiry, corruption, and loss while preserving the
      requirement for the existing runtime fence/correlation on every accepted response.
    - Prove provenance mutations alone cannot start, complete, fail, cancel, heartbeat, or
      dispatch any task.
    - _Requirements: 4.8-4.10, 6.11-6.13_
  - [x] 12.6 Add exhaustive fixed-deny-surface coverage
    - Enumerate every WorkflowService and OperatorService `Action`; assert exact health and fixed
      Worker matrix entries are the only scoped candidates.
    - Include `ResetStickyTaskQueue`, `ListWorkers`, `DescribeWorker`, all Activity By-ID
      variants, standalone Activity, and future/unclassified actions.
    - _Requirements: 6.9, 8.5, 9.12, 10.6, 12.7_

- [x] 13. Checkpoint: scoped edge paths are green
  - Run formatting plus focused check, clippy, and tests for `tokeira-edge`,
    `tokeira-runtime`, `tokeira-storage`, and `tokeira-auth`.
  - Verify Properties 7 and 10, all integration/structural invariants I1-I4, and the ordinary
    gRPC/gateway regression set are green.

- [x] 14. Wire production bootstrap and the public operator surface
  - [x] 14.1 Build configured scope rules in `tokeirad`
    - Construct JWT-subject and AWS-IAM-ARN `WorkerScopeRules` beside existing `GrantRules`.
    - Feed the same DefaultAuthorizer ordinary and scoped claims without changing issuer routing.
    - Fail startup with indexed field diagnostics for invalid patterns, scopes, or conflicting
      static definitions.
    - _Requirements: 3.1-3.12, 11.7-11.9_
  - [x] 14.2 Supply provenance, session, and cleanup dependencies
    - Inject the configured DSQL/in-memory provenance store and session registry into the edge
      service.
    - Start bounded expiry cleanup under existing cancellation and shutdown ownership.
    - Keep scoped authorization state outside kernel, history, lanes, projections, and delivery
      ordering.
    - _Requirements: 4.8-4.10, 6.5-6.7, 6.12-6.13, 10.8_
  - [x] 14.3 Register the Feature Catalog entry
    - Add `scoped-worker-authorization` as a Tokeira-native, implemented,
      presence-activated, default-inert capability dependent on JWT or AWS IAM verification.
    - Update generated compatibility/config inventories through their owning generators and
      add drift tests.
    - _Requirements: 11.7-11.9, 11.12-11.13_
  - [x] 14.4 Document the exact public configuration and SDK usage
    - Add secret-free `config.example.toml` examples for signed JWT, subject mapping, and AWS IAM
      mapping.
    - Update the public Tokeira configuration guide with attenuation, external credential
      ownership, the fixed claim, exact VERSIONED pair, the fixed allowed RPC surface, and a
      standard SDK auth metadata supplier example.
    - State explicitly that Activity By-ID, standalone Activity, unversioned/deprecated
      versioning, Worker inventory, and namespace-wide APIs are excluded for scoped identities.
    - _Requirements: 11.7-11.13_
  - [x] 14.5 Add structural confidentiality and operator-surface tests
    - Check config examples, generated catalogs, docs, metrics labels, error formatting, and
      provenance schema for forbidden credential/token/payload material.
    - Check the public catalog and guide enumerate every required activation and limitation.
    - _Requirements: 2.10, 11.4-11.13_
  - [x] 14.6 Add kernel/history isolation guards
    - Assert no dependency from `tokeira-kernel` to auth/storage additions, no new kernel
      command/state/transition/history field, no upstream proto modification, and no change to
      existing authoritative run-state migration contents.
    - Permit only the new standalone provenance migrations in the storage schema.
    - _Requirements: 4.8-4.10, 10.8_

- [ ] 15. Add end-to-end readiness and regression evidence
  - [x] 15.1 Test the complete authentication stack
    - Exercise locally signed JWTs with signed scopes, subject mappings, equal and conflicting
      combinations, AWS IAM verified-ARN fixtures, ordinary roles, and universal health.
    - Prove malformed/conflicting scope inputs fail closed while absent scope preserves current
      ordinary identity behavior.
    - _Requirements: 2.1-2.10, 3.5-3.12, 4.7, 11.1-11.6, 12.2-12.4_
  - [x] 15.2 Test a real standard-SDK Worker over gRPC
    - Use the SDK auth metadata supplier to poll and complete exact-version Workflow, Activity,
      and Nexus tasks, heartbeat an Activity, shut down its bound session under the stock
      cancellation-policy default, and use `DescribeTaskQueue` for readiness.
    - Assert the same credential cannot poll a second queue/version or call namespace-wide
      read/write APIs.
    - _Requirements: 5.1-5.15, 6.1-6.13, 8.1-8.7, 9.1-9.12, 12.13-12.15_
  - [x] 15.3 Test provenance lifecycle and edge reconstruction
    - Cover poll→record→heartbeat-retain→terminal-delete, expiry, insert/lookup outages, digest
      conflict, and a reconstructed edge service using the same durable store.
    - Prove task timeout/retry recovers work whose token was withheld after a provenance insert
      failure.
    - _Requirements: 4.3, 6.1-6.8, 6.10-6.13_
  - [x] 15.4 Test the complete negative scope and path-closure matrix
    - Cover wrong namespace/queue/deployment/build, partial/deprecated/unversioned mode,
      Activity By-ID, ListWorkers, DescribeWorker, visibility, Workflow start, CHASM,
      legacy-token, Query, Nexus piggyback heartbeat, and HTTP gateway.
    - Prove every denial occurs before its path-specific mutable effect.
    - _Requirements: 4.1-4.7, 5.1-5.15, 6.9, 9.12, 10.1-10.8, 12.5-12.7, 12.12_
  - [x] 15.5 Test Workflow-completion commands and optional returns end to end
    - Prove same-namespace commands retain ordinary semantics, cross-namespace commands reject
      atomically, and out-of-scope eager/inline tasks are withheld without durable work loss.
    - _Requirements: 7.1-7.8, 12.8-12.9_
  - [ ] 15.6 Record and verify the downstream sibling-provider contract evidence
    - Add versioned evidence that names the exact Tokeira commit and sibling worker-compute-provider contract
      revision used to launch a Firecracker guest and satisfy exact-version
      `DescribeTaskQueue` readiness.
    - Make the readiness test validate the evidence shape and reject stale/missing revision
      fields without committing credentials or provider secrets.
    - _Requirements: 11.10-11.11, 12.16_

- [x] 16. Final checkpoint: workspace and documentation bars are green
  - Run the complete AGENTS.md §10.4 bar:
    `cargo +nightly fmt --all`, `cargo lint --locked`,
    `cargo check --workspace --locked`, `cargo test --workspace --locked`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Run offline Markdown link validation and generated-config/catalog drift checks.
  - Confirm every Property 1-13 test runs at least 100 cases, every integration/structural
    invariant I1-I8 is covered, the working tree contains no generated drift, and no kernel,
    upstream-proto, or dependency-version change was introduced. Limit `Cargo.lock` movement to
    the mechanical addition of the approved internal `tokeira-auth` → `tokeira-types` edge.

## Task Dependency Graph

```text
1 -> 2
1,2 -> 3
1 -> 4
4 -> 5
1 -> 6
1,2,6 -> 7
2,6,7 -> 8
5,6,7,8 -> 9
4,6,7,8,9 -> 10
7,10 -> 11
7,8,10,11 -> 12
10,11,12 -> 13
2,4,7,8,10,12 -> 14
13,14 -> 15
15 -> 16
```

## Notes

- Re-verify every public behavior against Temporal server `v1.31.0` before changing an edge
  decision. The universal unauthenticated health set is exactly `Health/Check` and
  `GetSystemInfo`; the scoped Worker surface is otherwise a Tokeira-native attenuation.
- The kernel is not an implementation target. If any task appears to require a kernel command,
  state field, transition effect, history event, I/O, async operation, or side-effecting
  command, stop and return to the design.
- Re-read crate-local `AGENTS.md` before editing `tokeira-storage`, `tokeira-runtime`, or
  `tokeira-edge`. Re-read the migration tail immediately before allocating version numbers.
- Do not add or upgrade third-party dependencies. `sha2` and `proptest` are already present; the
  only planned dependency-graph change is the approved internal `tokeira-auth` →
  `tokeira-types` edge and its mechanical `Cargo.lock` package-dependency update.
- Provenance is authorization evidence only. Runtime fences and correlations remain mandatory;
  task deadlines recover tokens withheld after provenance insertion failure.
- Ordinary identities must retain their existing path, decisions, principals, status mapping,
  and lack of provenance writes.
- Every Property 1-13 task is mandatory, uses workspace `proptest`, runs at least 100 cases, and
  carries the exact `// Feature: scoped-worker-authorization, Property N: ...` tag.
- The Feature Catalog entry is default-inert and presence-activated. Tokeira does not issue or
  distribute the guest credential.
- The final sibling-provider evidence depends on an exact sibling contract revision and must never embed
  a bearer, presigned STS URL, private key, task token, payload, or tenant secret.
