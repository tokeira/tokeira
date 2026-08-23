# Implementation Plan

- [x] 1. Add explicit embedded-storage configuration
  - [x] 1.1 Add the embedded configuration model in `tokeira-config`
    - Implement `EmbeddedEngineConfig`, the closed `EmbeddedStorageConfig` enum,
      managed create-or-recover intent, existing-cluster identity, migration policy,
      startup deadline, and embedded DSQL resource limits with unknown-field rejection.
    - Preserve the existing `Engine::start*` configuration path by defaulting only that
      compatibility path to in-memory storage.
    - _Requirements: 1.1–1.6, 5.1–5.3, 6.5–6.12, 7.10–7.11, 8.14_
  - [x] 1.2 Extend listener-backed DSQL configuration with an explicit migration policy
    - Add the optional serialized field to `DsqlInfraConfig`; defer its required-value
      validation to DSQL startup so non-DSQL configurations remain unaffected.
    - Keep the configuration types independent of storage implementation types and map
      them exhaustively at the engine boundary.
    - _Requirements: 5.2–5.3, 5.12–5.13_
  - [x] 1.3 Add fixed configuration and serialization tests
    - Cover exact defaults and maxima, positive-value and cross-field validation,
      unknown fields, missing identity/intent/policy, lossless TOML round trips, and
      error messages that identify the invalid field without exposing secrets.
    - _Requirements: 1.1–1.6, 5.1–5.3, 6.5–6.12, 7.10–7.11_
  - [x] 1.4 Property test: Property 1 — embedded configuration is explicit and closed
    - Add a `proptest` model with at least 100 cases covering decoding, defaulting,
      validation, and rejection without storage-mode fallback.
    - Tag: `// Feature: managed-embedded-dsql, Property 1: embedded configuration is explicit and closed`
    - _Requirements: 1.1–1.6, 5.1–5.3, 6.5–6.12, 7.10–7.11_

- [x] 2. Checkpoint: embedded configuration is green
  - Run formatting plus focused `tokeira-config` check, clippy, and nextest commands
    with `--locked`; confirm serialization tests and Property 1 pass with no warnings.
  - _Requirements: 1.1–1.6, 5.1–5.3, 6.5–6.12, 7.10–7.11_

- [x] 3. Create the managed DSQL lifecycle crate and durable descriptor store
  - [x] 3.1 Add `tokeira-managed-dsql` as a documented workspace crate
    - Register the crate and workspace path dependency, inherit workspace lints, and add
      only the already-locked AWS DSQL and common workspace dependencies required by
      the approved design.
    - Define the control-plane trait, request/observation types, typed redacted errors,
      deadlines, retry policy, descriptor types, and crate-owned redacting client-token
      wrapper; do not depend on engine, storage, runtime, edge, observability, kernel,
      `tkr`, or `tkp`.
    - _Requirements: 2.1–2.12, 3.1–3.16, 9.4–9.11, 14.1–14.3_
  - [x] 3.2 Implement the local crash-safe `ClusterDescriptorStore`
    - Use a sidecar exclusive lock, monotonically increasing CAS revision, an owner-only
      same-directory temporary file, file `sync_all`, atomic rename, and parent-directory
      sync before releasing the lock.
    - Persist `PendingCreate`, `Ready`, and `Destroyed` states; reject corrupt or future
      formats and treat `Destroyed` as a tombstone rather than implicit create intent.
    - _Requirements: 2.1–2.4, 2.8–2.11, 3.8, 3.10, 9.10_
  - [x] 3.3 Add descriptor and identity example tests
    - Cover owner-only permissions where supported, fsync/rename failure injection,
      redacted `Debug`/errors, descriptor format rejection, ID/ARN/Region parsing, and
      every partial-identity rejection.
    - _Requirements: 2.2, 2.8–2.12, 3.1–3.2, 3.6, 3.8, 8.13, 12.7–12.8_
  - [x] 3.4 Property test: Property 2 — descriptor CAS admits one canonical history
    - Use generated descriptor histories and two same-revision writers against the
      descriptor-store model for at least 100 cases.
    - Tag: `// Feature: managed-embedded-dsql, Property 2: descriptor CAS admits one canonical history`
    - _Requirements: 2.2, 2.8–2.11, 3.8, 13.1, 13.6_

- [x] 4. Implement create, recovery, identity validation, and AWS adaptation
  - [x] 4.1 Implement the production `aws_sdk_dsql` control-plane adapter
    - Build create requests with the explicit persisted token, deletion protection, and
      configured tags; omit multi-Region, KMS, policy, and bypass fields exactly as the
      approved request-policy table specifies.
    - Map AWS status and errors into contract-shaped observations and typed retryable,
      access, validation, quota, conflict, and terminal errors without leaking SDK
      request debug output.
    - _Requirements: 2.5–2.7, 2.11–2.12, 3.1–3.5, 3.15–3.16, 9.11_
  - [x] 4.2 Implement the create-or-recover state machine
    - Persist the client token before the first create call; on CAS loss reload and use
      the winning token; on replay reuse the durable token; and persist returned Region,
      ID, ARN, and endpoint as one canonical ready identity.
    - Recover ready descriptors only with `GetCluster` by ID, refresh endpoint only,
      reject lost descriptors and canonical-identity disagreement, and never use tags or
      endpoint for discovery.
    - _Requirements: 2.1–2.10, 3.1–3.10, 13.1–13.4, 13.6_
  - [x] 4.3 Implement bounded status recovery and scale-to-zero wake handling
    - Follow the approved status table, use one injected startup deadline, honor
      `retryAfterSeconds` as a lower bound, and use deterministic fake time in tests.
    - Allow `IDLE`/`INACTIVE` to enter bounded pool warmup, proceed to schema only after
      `ACTIVE`, and reject failed, deleting, deleted, and multi-Region-only statuses.
    - _Requirements: 3.11–3.16, 8.14, 13.4–13.6_
  - [x] 4.4 Implement existing-cluster resolution without managed mutations
    - Validate configured Region, cluster ID, and ARN with `GetCluster` by ID; refresh
      only the endpoint; expose no create, protection-update, delete, or descriptor path.
    - _Requirements: 3.1–3.9, 5.2, 9.11_
  - [x] 4.5 Property test: Property 3 — creation is idempotent across every crash point
    - Drive the lifecycle with fake AWS, fake descriptor persistence, injected crashes,
      and deterministic replay for at least 100 cases.
    - Tag: `// Feature: managed-embedded-dsql, Property 3: creation is idempotent across every crash point`
    - _Requirements: 2.1–2.9, 2.12, 13.1–13.2_
  - [x] 4.6 Property test: Property 4 — AWS request construction is complete and identity-neutral
    - Generate valid managed configurations and all control-plane operation kinds for at
      least 100 cases, asserting field closure and canonical-ID targeting.
    - Tag: `// Feature: managed-embedded-dsql, Property 4: AWS request construction is complete and identity-neutral`
    - _Requirements: 2.5–2.7, 3.1–3.5, 3.9–3.10, 9.11, 13.3_
  - [x] 4.7 Property test: Property 5 — recovery follows the cluster-status reference model
    - Compare generated AWS observation/error sequences with a pure status/retry model
      using injected time for at least 100 cases and no sleeps.
    - Tag: `// Feature: managed-embedded-dsql, Property 5: recovery follows the cluster-status reference model`
    - _Requirements: 3.5–3.16, 8.14, 13.4–13.6_

- [x] 5. Checkpoint: managed lifecycle is green
  - Run formatting plus focused check, clippy, nextest, and doctests for
    `tokeira-managed-dsql`; confirm its fake-AWS suite and Properties 2–5 pass without
    credentials or network access.
  - _Requirements: 2.1–2.12, 3.1–3.16, 9.11, 13.1–13.6_

- [x] 6. Establish the release-bound schema contract and immutable baseline
  - [x] 6.1 Add storage-owned schema contract parsing and canonical digest generation
    - Implement the platform-independent ordered digest format, cumulative prefix
      digests, contiguous-version checks, and contract ordering validation in a helper
      shared by `build.rs` and storage tests.
    - _Requirements: 4.1–4.7, 4.10, 13.8_
  - [x] 6.2 Normalize and extend the DSQL migration set before cutting the baseline
    - Inventory the current migration head at implementation time; normalize existing
      table/index/seed migrations to the supported idempotent DSQL forms while the
      build-phase rule still permits it.
    - Add one-statement forward migrations for `schema_compatibility` and
      `tokeira_control_lease`, including the equivalent idempotent bootstrap DDL used by
      first-run automatic migration.
    - _Requirements: 4.8–4.10, 5.6–5.11, 7.1–7.9_
  - [x] 6.3 Check in and enforce `schema-contract.toml` and `schema-baseline.lock`
    - Select nonzero `MIN`, `TARGET`, `MAX`, digest, and immutable ceiling from the final
      contiguous migration set; record every immutable `(version, name, checksum)`.
    - Make the storage build fail on gaps, duplicates, ordering/release/digest mismatch,
      a changed locked entry, or an immutable ceiling below the readable ceiling; update
      the crate-local migration rule in the same baseline-cut change so future edits are
      forward-only.
    - _Requirements: 4.1–4.10, 13.8_
  - [x] 6.4 Expose the validated contract through `tokeira-build-info`
    - Add minimum, target, maximum readable, and migration-set digest fields without
      moving validation authority out of `tokeira-storage`.
    - _Requirements: 4.1–4.7_
  - [x] 6.5 Property test: Property 6 — the release schema contract is deterministic and immutable
    - Generate ordered and malformed migration sets, mutations, and contracts for at
      least 100 cases against a pure reference digest/baseline model.
    - Tag: `// Feature: managed-embedded-dsql, Property 6: the release schema contract is deterministic and immutable`
    - _Requirements: 4.1–4.10, 13.8_
  - [x] 6.6 Add schema-contract build-helper unit tests
    - Cover exact canonical bytes, fixed known digests, missing/duplicate versions,
      invalid version inequalities, release mismatch, and mutation at every baseline
      position.
    - _Requirements: 4.1–4.10, 13.8_

- [x] 7. Add fenced DSQL control leases and the shared admission gate
  - [x] 7.1 Implement `ControlLeaseRepository` and `ControlLeaseGuard`
    - Acquire with insert-on-conflict plus a fresh repeatable-read lock transaction,
      database-time expiry checks, exact cluster identity, monotonic fence increments,
      and bounded OCC retry.
    - Condition renew and release on claim name, owner incarnation, and fence token;
      treat zero affected rows as fencing.
    - _Requirements: 5.7–5.8, 7.1–7.9, 13.9, 13.12–13.13_
  - [x] 7.2 Implement owner renewal, quiescence, and `OwnershipAdmissionGate`
    - Share `Open`/`Closing`/`Fenced` state with edge and storage, close local admission
      before an unconfirmed renewal can pass database expiry, and distinguish clean from
      expired takeover using the approved quiescence rule.
    - Keep owner/fence data out of kernel commands and workflow history.
    - _Requirements: 7.1–7.9, 11.10, 14.2–14.5_
  - [x] 7.3 Add fixed lease/OCC/fencing tests
    - Cover busy claims, renewal and release fencing, database versus monotonic time,
      owner crash expiry, clean takeover, expired takeover, and redacted diagnostics with
      injected clocks and no sleeps.
    - _Requirements: 5.7–5.9, 7.1–7.9, 13.9, 13.12–13.13_
  - [x] 7.4 Property test: Property 11 — embedded ownership has at most one admitted owner
    - Compare generated multi-owner operation/time sequences against a lease reference
      model for at least 100 cases.
    - Tag: `// Feature: managed-embedded-dsql, Property 11: embedded ownership has at most one admitted owner`
    - _Requirements: 7.1–7.9, 13.12–13.13_

- [x] 8. Implement schema compatibility assessment and automatic migration
  - [x] 8.1 Add the compatibility record, contract, decisions, and pure assessment logic
    - Read catalog, migration ledger, and cumulative digest without DDL; tolerate absent
      metadata; validate known checksums/digests before version-policy decisions; and
      return typed incompatibility details.
    - Permit legacy compatibility backfill only after complete checksum validation and
      only under automatic policy.
    - _Requirements: 4.10–4.17, 5.1–5.6, 5.13_
  - [x] 8.2 Refactor `MigrationRunner` into assess/apply phases
    - Map configuration policy exhaustively at the engine boundary; initialize a new
      managed cluster automatically, but leave an uninitialized validate-only existing
      cluster unchanged with `MigrationRequired`.
    - Serialize apply with the schema-migration claim and revalidate the fence and all
      applied checksums before every migration step.
    - _Requirements: 5.1–5.9, 5.11–5.13_
  - [x] 8.3 Implement DSQL-safe, crash-recoverable migration steps
    - Execute one DDL or DML statement per transaction, wait for and validate asynchronous
      indexes, write the ledger only after the operation completes, then persist the
      cumulative digest in a separate compatible transaction.
    - Retry `40001` only for proven-idempotent steps; on lost job IDs inspect named index
      and job state; stop immediately on fencing, invalid index, or checksum drift.
    - _Requirements: 4.10–4.12, 5.5–5.12, 13.8–13.9_
  - [x] 8.4 Property test: Property 7 — schema compatibility matches the decision table
    - Generate valid contracts, ledgers, digests, versions, and policies and compare the
      pure function with the approved decision table for at least 100 cases, including
      no modeled mutation on rejection.
    - Tag: `// Feature: managed-embedded-dsql, Property 7: schema compatibility matches the decision table`
    - _Requirements: 4.11–4.17, 5.4–5.6, 13.7_
  - [x] 8.5 Property test: Property 8 — migration replay is serialized, fenced, and idempotent
    - Generate migration sequences, crash points, async-index states, OCC schedules, and
      competing owners against fake SQL and lease boundaries for at least 100 cases.
    - Tag: `// Feature: managed-embedded-dsql, Property 8: migration replay is serialized, fenced, and idempotent`
    - _Requirements: 5.5–5.12, 13.8–13.9_
  - [x] 8.6 Add fixed compatibility and migration tests
    - Cover every decision-table row, modified released migrations, bootstrap behavior,
      validate-only no-write behavior, exact `40001` classification, lost async-index
      jobs, invalid indexes, and actionable migration-required errors.
    - _Requirements: 4.11–4.17, 5.4–5.13, 13.7–13.9_

- [x] 9. Add an isolated DynamoDB-free embedded connection foundation
  - [x] 9.1 Preserve the distributed foundation and add embedded-only coordination
    - Keep `Reservoir`, `DistributedTokenBucket`, and `SlotBlockManager` on their
      pre-feature direct construction path without changing DynamoDB validation,
      ordering, rate, slot, refill, scheduling, or cancellation behavior.
    - Add a private embedded-only coordinator and preserve its director invariant:
      class/in-flight permit, then process-local physical slot, then process-local
      creation-rate token, then physical connection.
    - _Requirements: 6.1–6.4, 6.13–6.16, 14.2, 14.7_
  - [x] 9.2 Implement the process-local token bucket and atomic slot budget
    - Use an injected monotonic clock and `Notify`, initialize to the configured burst,
      cap replenishment and slots, and release exactly one slot on every failure,
      retirement, and bad-return path.
    - _Requirements: 6.1–6.2, 6.5–6.14, 13.10–13.11_
  - [x] 9.3 Add `DsqlStore::connect_embedded` and bounded shutdown
    - Construct no DynamoDB configuration/client/table names; use a separate embedded
      reservoir with the existing director, five `DbClass` budgets, leak diagnostics,
      authentication, and repository path with `max_idle_conns == max_conns`.
    - Make only the embedded reservoir's warmup and ready-channel waiting deadline/
      cancellation aware; close its admission, refillers, checked-out permits,
      coordinator, and physical pool during shutdown.
    - _Requirements: 1.5, 1.9, 6.1–6.18, 8.2, 13.10–13.11_
  - [x] 9.4 Property test: Property 9 — process-local creation limiting obeys rate and burst
    - Compare generated monotonic arrival/time sequences with a token-bucket reference
      model for at least 100 cases.
    - Tag: `// Feature: managed-embedded-dsql, Property 9: process-local creation limiting obeys rate and burst`
    - _Requirements: 6.1, 6.7–6.12, 13.10–13.11_
  - [x] 9.5 Property test: Property 10 — connection slot and class accounting is conserved
    - Generate embedded creation/check-out/return/expiry/leak/shutdown sequences for at
      least 100 cases, checking the physical-slot and class-budget conservation model.
    - Tag: `// Feature: managed-embedded-dsql, Property 10: connection slot and class accounting is conserved`
    - _Requirements: 6.2–6.6, 6.13–6.18, 13.10–13.11_
  - [x] 9.6 Add focused embedded-reservoir tests
    - Prove no DynamoDB object is constructed, all connection classes remain bounded,
      warmup uses the remaining startup deadline, bad connections release slots, leak
      diagnostics fire, and shutdown reaches zero resources without sleeps; retain the
      distributed reservoir, bucket, and slot-manager tests on their existing path.
    - _Requirements: 6.1–6.18, 13.10–13.11_

- [x] 10. Checkpoint: storage foundations are green
  - Run formatting plus focused check, clippy, nextest, doctests, and build-script tests
    for storage and build-info with `--locked`; confirm Properties 6–11 pass and the
    build rejects an intentionally altered baseline fixture.
  - _Requirements: 4.1–7.9, 13.7–13.13_

- [x] 11. Wire managed and existing DSQL into embedded engine startup
  - [x] 11.1 Add `Engine::start_with_embedded_config` and typed startup reports/errors
    - Keep old start methods in-memory compatible; make durable startup explicit; map
      configuration migration policy into storage policy exhaustively; and share one
      bounded deadline across lifecycle, warmup, schema, ownership, and stack phases.
    - _Requirements: 1.1–1.6, 5.1–5.3, 8.1–8.14_
  - [x] 11.2 Factor the current DSQL path into reusable transport-neutral construction
    - Preserve distributed mode's concrete token-bucket, slot-manager, and reservoir
      construction; make embedded mode pass its isolated process-local pool into the
      same repositories/runtime stack.
    - Ensure embedded DSQL returns `ConstructedStack::Embedded` and binds no gRPC, Nexus,
      callback, metrics, control, or other Tokeira listener.
    - _Requirements: 1.5, 1.7–1.10, 6.3–6.4, 10.5, 14.7_
  - [x] 11.3 Implement the ordered managed/existing startup phases
    - Resolve cluster, build/wake pool, assess/apply schema policy, acquire embedded owner,
      restore runtime state, self-assign existing shard leases, then open in-process
      admission; do not return a handle before every phase succeeds.
    - _Requirements: 3.5–3.16, 5.1–5.13, 7.1–7.7, 8.1–8.5_
  - [x] 11.4 Add failure-atomic rollback and the redacted startup report
    - Unwind completed resources in reverse order, conditionally release ownership, close
      the pool on later failure, and report storage/cluster/schema/ownership outcomes
      without paths, tokens, credentials, SQL, or payloads.
    - _Requirements: 8.6–8.14, 12.5–12.8_
  - [x] 11.5 Add engine startup and structural boundary unit tests
    - Inject a failure at every phase; verify exact ordering, no leaked endpoint, reverse
      unwind, report contents/redaction, no listener, no DynamoDB in embedded DSQL, and
      no new `tokeira-kernel` dependency or feature.
    - _Requirements: 1.6–1.10, 6.3–6.4, 8.1–8.14, 13.10, 14.1–14.8_
  - [x] 11.6 Property test: Property 12 — startup is prefix-safe and failure-atomic
    - Generate startup outcomes and one injected failure boundary for at least 100 cases
      against an ordered-phase/rollback reference model.
    - Tag: `// Feature: managed-embedded-dsql, Property 12: startup is prefix-safe and failure-atomic`
    - _Requirements: 8.1–8.14, 1.6_

- [x] 12. Implement in-process drain, runtime task ownership, and shutdown
  - [x] 12.1 Add admission and in-flight drain to `InProcessGrpcService`
    - Give every handler a cancellation-safe decrement guard; make `begin_shutdown`
      synchronous; drain with `Notify` and a deadline; return `UNAVAILABLE` after close
      or owner fencing.
    - _Requirements: 7.7–7.8, 8.5, 10.8–10.9, 13.21_
  - [x] 12.2 Consolidate runtime cancellation and join ownership
    - Add a non-kernel `RuntimeShutdownHandle`; track engine refresh, repair, renewal,
      cleanup, and scanner tasks with `TaskTracker`; close the tracker after startup and
      await it after cancellation.
    - _Requirements: 10.8–10.10, 11.10–11.11, 13.21, 14.2–14.6_
  - [x] 12.3 Add `EmbeddedShutdownCoordinator`, explicit shutdown, and safe `Drop`
    - Preserve the director after `DsqlStore::into_parts`; close admission, cancel, drain
      and join, finish owned telemetry, release shard leases, conditionally release owner,
      and close storage in the approved order while aggregating independent failures.
    - Make `Drop` only close admission and cancel synchronously; it must never disable
      deletion protection, delete the cluster, or block on AWS.
    - _Requirements: 6.17–6.18, 7.7–7.9, 9.1–9.4, 10.8–10.10, 13.14, 13.21_
  - [x] 12.4 Property test: Property 19 — shutdown establishes the host flush boundary
    - Generate admitted-call/task completion and shutdown interleavings for at least 100
      cases against an ordering model, including independent cleanup failures.
    - Tag: `// Feature: managed-embedded-dsql, Property 19: shutdown establishes the host flush boundary`
    - _Requirements: 6.17–6.18, 7.8, 10.8–10.10, 13.21_
  - [x] 12.5 Add fixed shutdown/drop tests
    - Prove cancellation-safe handler counts, bounded drain, all cleanup attempts after an
      earlier failure, conditional owner release, pool closure, zero AWS mutation from
      normal shutdown/drop, and continued usability of the host telemetry provider.
    - _Requirements: 6.17–6.18, 7.7–7.9, 9.1–9.4, 10.8–10.10, 13.14, 13.21_

- [x] 13. Add library-only explicit managed-cluster destruction
  - [x] 13.1 Implement `ManagedDsqlAdmin` plan, confirmation, and apply
    - Keep planning read-only and bind the plan digest to descriptor revision, canonical
      ID/ARN/Region, and observed protection; reject absent/mismatched confirmation or a
      stale descriptor before any AWS mutation.
    - On confirmed apply, revalidate with `GetCluster` by ID, disable protection, delete
      by ID, wait for deleted/not-found inside the deadline, and CAS-write `Destroyed`.
      Derive separate retry-stable operation tokens from plan digest and operation name.
    - Expose no `tkr` or `tkp` command/deployment adapter in this feature.
    - _Requirements: 9.4–9.11_
  - [x] 13.2 Property test: Property 13 — destruction is explicit, bound, and idempotent
    - Generate engine lifecycle sequences, stale/current plans, confirmation values, AWS
      retry schedules, and plan replays for at least 100 cases with fake AWS and storage.
    - Tag: `// Feature: managed-embedded-dsql, Property 13: destruction is explicit, bound, and idempotent`
    - _Requirements: 9.1–9.11, 13.14_
  - [x] 13.3 Add fixed administrative destruction tests
    - Cover plan content, confirmation binding, stale revision/identity, protection-before-
      delete ordering, separate idempotency tokens, retries, deleted/not-found success,
      tombstone persistence, and redacted output/errors.
    - _Requirements: 9.5–9.11_

- [x] 14. Checkpoint: embedded lifecycle is green
  - Run formatting plus focused check, clippy, nextest, and doctests for managed DSQL,
    storage, engine, runtime, and edge; confirm Properties 12, 13, and 19 pass and all
    engine lifecycle tests make zero destructive AWS calls.
  - _Requirements: 1.1–10.10, 13.10–13.14, 13.21, 14.1–14.8_

- [ ] 15. Implement composable trace-context propagation and durable correlation
  - [ ] 15.1 Extend `ChannelTraceContext` to complete serializable W3C span context
    - Capture trace ID, span ID, flags, and tracestate from the current context and rebuild
      an OpenTelemetry remote parent; carry data rather than span handles and keep it out
      of authoritative history.
    - _Requirements: 11.2, 11.5, 11.10–11.11_
  - [ ] 15.2 Extract W3C parentage at the in-process `service_override` boundary
    - Parse copied gRPC `traceparent`/`tracestate`, set a valid remote parent on the server
      span, and start a root for absent/invalid input without changing Temporal results.
    - _Requirements: 1.8, 11.1, 11.14, 13.16_
  - [ ] 15.3 Thread context through runtime channels and task processing
    - Propagate parent or link relationships through direct dispatch, fanout/handoff,
      workflow-task and activity-task processing, and Tokeira-owned outbound calls while
      preserving opaque Temporal headers.
    - Attach stable Workflow, Run, Activity, task/attempt, and operation identifiers to
      spans/events so a restarted process may start a new trace without losing durable
      correlation.
    - _Requirements: 11.2–11.11, 13.17–13.19_
  - [ ] 15.4 Add host-carrier integration fixtures without defining host APIs
    - Exercise provider, MCP-tool, and handoff context carriers through a host-owned test
      fixture; Tokeira supplies only stable identifiers and context at boundaries it
      actually mediates.
    - _Requirements: 11.6–11.9, 13.18–13.19_
  - [ ] 15.5 Property test: Property 15 — `service_override` preserves W3C parentage
    - Generate valid, absent, and malformed W3C contexts for at least 100 cases and
      compare recorded span relationships and unchanged service results.
    - Tag: `// Feature: managed-embedded-dsql, Property 15: service_override preserves W3C parentage`
    - _Requirements: 11.1, 11.14, 13.16_
  - [ ] 15.6 Property test: Property 16 — transient context and durable identifiers compose
    - Generate boundary chains, relationship kinds, execution identifiers, and restart
      points for at least 100 cases against a parent/link/correlation reference model.
    - Tag: `// Feature: managed-embedded-dsql, Property 16: transient context and durable identifiers compose`
    - _Requirements: 11.2–11.11, 13.17–13.19_

- [ ] 16. Make embedded telemetry host-owned, bounded, redacted, and observational
  - [ ] 16.1 Separate library instrumentation from process-level installation
    - Ensure no embedded construction path calls the process installer, mutates a global
      dispatcher/recorder/propagator/provider, starts a metrics listener, owns an
      exporter, or globally flushes/shuts down host telemetry.
    - Keep existing composable `tracing` and `metrics` emission and use pinned stable
      semantic conventions plus documented `tokeira.*` attributes where needed.
    - _Requirements: 10.1–10.13, 13.15_
  - [ ] 16.2 Extend the metric manifest with bounded lifecycle dimensions
    - Add storage mode, cluster status, schema/ownership outcome, database class,
      operation kind, and error class as bounded dimensions; reject workflow/run/trace/
      request/activity identifiers and prompt/tool/credential/token fields as labels.
    - _Requirements: 10.6–10.7, 10.12–10.13, 11.12–11.13, 13.20_
  - [ ] 16.3 Apply default sensitive-data exclusion and redacted formatting
    - Keep prompts, tool inputs/outputs, workflow/activity payloads, AWS credentials,
      DSQL auth tokens, creation tokens, connection passwords, and secret-bearing errors
      out of spans, events, metrics, reports, and `Debug`/`Display` output.
    - Leave deliberate host content capture and redaction entirely host-owned; add no
      Tokeira content-capture switch.
    - _Requirements: 2.12, 8.13, 12.1–12.10, 13.22_
  - [ ] 16.4 Property test: Property 14 — embedded construction is transport- and global-state-neutral
    - Generate host instrumentation setups and embedded storage modes for at least 100
      cases, recording listener attempts, globals, and locally emitted instrumentation.
    - Tag: `// Feature: managed-embedded-dsql, Property 14: embedded construction is transport- and global-state-neutral`
    - _Requirements: 1.7–1.10, 10.1–10.7, 10.11, 13.15_
  - [ ] 16.5 Property test: Property 17 — metric dimensions stay bounded
    - Generate arbitrary counts and contents of high-cardinality identifiers and content
      for at least 100 cases; assert emitted labels remain in the bounded manifest.
    - Tag: `// Feature: managed-embedded-dsql, Property 17: metric dimensions stay bounded`
    - _Requirements: 10.6–10.7, 10.12–10.13, 11.12–11.13, 13.20_
  - [ ] 16.6 Property test: Property 18 — sensitive content is absent by default
    - Generate unique canaries for every sensitive source, nested error chain, report,
      formatting path, and host-redactor result for at least 100 cases.
    - Tag: `// Feature: managed-embedded-dsql, Property 18: sensitive content is absent by default`
    - _Requirements: 2.12, 8.13, 12.1–12.10, 13.22_
  - [ ] 16.7 Property test: Property 20 — telemetry is observational only
    - Replay generated request sequences with no subscriber, a recorder, and a dropping/
      failing exporter for at least 100 cases; compare decisions and committed transition
      bytes exactly.
    - Tag: `// Feature: managed-embedded-dsql, Property 20: telemetry is observational only`
    - _Requirements: 11.10–11.11, 14.4–14.6_
  - [ ] 16.8 Add fixed observability and security tests
    - Cover every forbidden metric label, semantic-attribute names, process-global state
      preservation, no-subscriber behavior, failed exporter behavior, and canary absence
      from each fixed output surface.
    - _Requirements: 10.1–12.10, 13.15, 13.20, 13.22, 14.4–14.6_

- [ ] 17. Checkpoint: context and observability are green
  - Run formatting plus focused check, clippy, nextest, and doctests for observability,
    edge, runtime, and engine; confirm Properties 14–18 and 20 pass under isolated test
    processes and no test installs shared process-global state across cases.
  - _Requirements: 10.1–12.10, 13.15–13.22, 14.4–14.6_

- [ ] 18. Add end-to-end embedded DSQL verification
  - [ ] 18.1 Add the full embedded storage integration test
    - Build `StackTransport::Embedded` over the DSQL repository path, invoke Temporal SDK
      calls through `service_override`, and assert no listener and no DynamoDB
      client/table/config access.
    - _Requirements: 1.5, 1.7–1.10, 6.1–6.18, 8.5, 13.10–13.11, 14.7_
  - [ ] 18.2 Add restart and ownership integration tests
    - Recreate the engine from the same descriptor/database state; verify canonical
      cluster identity, stable workflow/run correlation with a permitted new trace, one
      admitted owner, immediate clean takeover, quiesced expired takeover, and old
      endpoint fencing.
    - _Requirements: 2.3–2.9, 3.1–3.13, 7.1–7.9, 11.9, 13.12–13.13, 13.19_
  - [ ] 18.3 Add telemetry, host-carrier, shutdown-flush, and canary integrations
    - Install local host-owned test instrumentation before startup; exercise RPC,
      workflow, activity, outbound, provider, MCP-tool, handoff, restart, and shutdown
      paths; flush only after engine shutdown; assert stable correlation, bounded metrics,
      and complete canary exclusion.
    - _Requirements: 10.1–12.10, 13.15–13.22_
  - [ ] 18.4 Add the non-default live AWS lifecycle test and runbook
    - Behind the existing DSQL integration mechanism and an explicit credentialed
      invocation, create one disposable single-Region cluster, inject post-create crash,
      recover with the durable token, exercise wake/schema/ownership, obtain and confirm
      a library destroy plan, and explicitly destroy it.
    - Document permissions, Region, cost, timeout, descriptor path, cleanup/recovery, and
      why default CI skips it; do not add a `tkr` or `tkp` adapter.
    - _Requirements: 2.1–3.16, 5.5–5.12, 7.1–7.9, 9.5–9.11, 13.23–13.24_
  - [ ] 18.5 Add cross-crate architecture and default-suite assertions
    - Prove the default workspace tests need neither AWS credentials nor Docker, DSQL SQL
      tests stay feature-gated, all async tests synchronize without sleeps, and kernel
      dependencies/features/source remain unchanged.
    - _Requirements: 13.23–13.24, 14.1–14.8_

- [ ] 19. Final checkpoint: workspace quality bar is green
  - Run `cargo +nightly fmt --all`, `cargo lint --locked`,
    `cargo check --workspace --locked`, `cargo nextest run --workspace --locked`,
    `cargo test --workspace --doc --locked`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Run the repository's offline Markdown-link check and confirm the build/test commands
    leave no generated diff; report the live-AWS test as an explicit non-default release
    gate when credentials were not supplied.
  - _Requirements: 13.1–13.24, 14.1–14.8_

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["2"],
  "4": ["3"],
  "5": ["4"],
  "6": ["2"],
  "7": ["6"],
  "8": ["6", "7"],
  "9": ["2"],
  "10": ["5", "8", "9"],
  "11": ["10"],
  "12": ["11"],
  "13": ["5", "12"],
  "14": ["13"],
  "15": ["12"],
  "16": ["12", "15"],
  "17": ["16"],
  "18": ["14", "17"],
  "19": ["18"]
}
```

Subtasks execute in numeric order within their parent. Tasks 3–5, 6–8, and 9 may be
implemented as independent slices after the configuration checkpoint; Task 10 is their
join before engine wiring.

## Notes

- This plan contains no `tkr` or `tkp` implementation. The separate operator-tooling
  activity may consume the library-only destruction API after its contract is stable.
- No `tokeira-kernel` change is intended. If any implementation step appears to require
  kernel I/O, async, storage, telemetry, nondeterminism, a new command, or a transition
  shape change, stop and raise it for a spec amendment before proceeding.
- The new `tokeira-managed-dsql` crate is the approved architectural addition. Reuse the
  already-locked AWS DSQL dependencies; any further new third-party dependency requires
  separate architectural approval.
- A shared-reservoir refactor or hardening campaign is deferred until after the initial
  public launch and is not part of this spec. It requires a separate consent-gated spec
  covering DSQL service-quota behavior, DynamoDB conditional-write and hot-key capacity,
  concurrency accounting, cold starts, cancellation, throttling, and scale-to-zero.
- Select schema contract versions and cut the immutable baseline against the current
  contiguous migration head at implementation time so concurrent migration work is not
  overwritten. The baseline cut and the crate-local forward-only instruction change are
  one atomic review unit.
- Every property task is required, uses workspace `proptest` with at least 100 cases,
  and uses injected clocks, deterministic fakes, channels, or `Notify` rather than
  sleeps. Example tests supplement rather than replace those properties.
- The live AWS test is explicit, credentialed, cost-bearing, and excluded from the
  default suite. All other tasks must remain verifiable without AWS credentials or
  Docker.
