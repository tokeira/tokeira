# Implementation Plan

- [x] 1. Establish the worker-compute feature foundations
  - [x] 1.1 Add strict worker-compute policy configuration
    - Add `policy.worker_compute.enabled`, default it to `false`, reject unknown
      fields, and include it in lossless configuration round trips and generated
      configuration metadata.
    - Define the fixed controller intervals, capacities, leases, retry bounds, and
      namespace instance limit from the approved design without adding dynamic
      configuration knobs.
    - _Requirements: 1.1, 1.4, 1.6, 1.8; 7.2, 7.3; 8.1; 10.12_
  - [x] 1.2 Define the provider-neutral compute protobuf contract
    - Add `proto/tokeira/compute/v1/provider.proto` with the fixed
      `tokeira.worker.compute.v1.ComputeProvider` service name, `invoke-worker`
      operation, task-queue and reason enums, and the approved request/response field
      numbers.
    - Wire generation and protobuf round-trip tests without changing upstream
      Temporal protos.
    - _Requirements: 11.1, 11.2, 11.3, 11.4_
  - [x] 1.3 Introduce the runtime worker-compute domain and ports
    - Add the `tokeira-runtime::worker_compute` module with provider-neutral
      controller, group, observation, metrics, decision, action, health, clock,
      namespace-catalog, and repository types.
    - Keep all controller state outside the kernel and expose only bounded,
      non-blocking ports to existing delivery components.
    - _Requirements: 1.2, 1.5; 5.10, 5.12; 8.8; 10.11_
  - [x] 1.4 Add foundation unit tests
    - Cover absent/false/true configuration, strict unknown-field rejection, generated
      metadata, protobuf field compatibility, and domain serialization round trips.
    - _Requirements: 1.1, 1.4, 1.6, 1.8; 11.1, 11.4_

- [x] 2. Implement ComputeConfig eligibility, scaler decoding, and fingerprints
  - [x] 2.1 Decode and validate `no-sync` scaler details
    - Accept a missing details payload as defaults; decode a string-keyed object;
      reject unknown keys; accept JSON numbers and numeric strings with the pinned
      integer truncation and minimum checks; and enforce the cooloff/poll-interval
      relationship.
    - Preserve accepted scaler bytes through the existing Worker Deployment registry
      round trip.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_
  - [x] 2.2 Classify providers and partition scaling groups
    - Treat any non-empty implementation type with a non-empty Nexus endpoint as a
      remote provider, apply the `no-sync` default, classify `rate-based` and direct
      providers as stored-but-controller-ineligible, and deterministically resolve
      explicit versus catch-all task types.
    - Perform pure validation before the existing registry CAS so invalid mutations
      leave bytes and conflict tokens unchanged; defer endpoint existence to
      invocation time.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11_
  - [x] 2.3 Compute the canonical Configuration Fingerprint
    - Implement the domain-separated BLAKE3 canonical encoding over group identity,
      effective task types, provider type/details, endpoint, scaler type/details, and
      all behavior-affecting fields.
    - Add fixed vectors proving field boundaries, ordering, and mutation sensitivity.
    - _Requirements: 4.2; 10.2, 10.7; 11.6_
  - [x] 2.4 Property test: Property 2 — eligibility is deterministic and mutation-atomic
    - Generate ComputeConfigs and prior registry states; compare the effective
      partition/error to an independent model and verify error leaves the prior bytes
      and conflict token unchanged, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 2: eligibility is deterministic and mutation-atomic`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.11_
  - [x] 2.5 Property test: Property 3 — `no-sync` decoding is total and preserving
    - Generate scaler payloads and prove deterministic acceptance/rejection plus
      byte-for-byte registry preservation for every accepted payload, for at least 100
      cases.
    - Tag: `// Feature: worker-compute-controller, Property 3: no-sync decoding is total and preserving`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_

- [x] 3. Checkpoint: foundations and validation are green
  - Run formatting, focused clippy/checks for config, proto, runtime, and Worker
    Deployment registry crates, plus the focused unit and property tests from Tasks 1
    and 2.

- [x] 4. Add the durable controller, sample, and action repositories
  - [x] 4.1 Add forward-only DSQL migrations
    - Add one-statement contiguous migrations for controller slots, controller state,
      action outbox, queue samples, and the separate asynchronous due-action index.
    - Use DSQL-compatible keys and types, including namespace slots `0..99`, action
      due buckets, no foreign keys, no unbounded indexed values, and no `ALTER`.
    - _Requirements: 10.1, 10.2, 10.12; 12.1, 12.8_
  - [x] 4.2 Define one behavioral repository contract and in-memory implementation
    - Implement controller admission, fenced claim, atomic decision commit, queue
      sample put/list, namespace-scoped due-action claim, begin-attempt,
      finalize-action, and deterministic health listing.
    - Model revision and lease epochs explicitly; store controller/action payloads as
      versioned records.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.10; 12.1, 12.8, 12.13_
  - [x] 4.3 Implement DSQL controller admission and fencing
    - Admit controller instances through the fixed 100-slot namespace table in one
      optimistic transaction rather than a count-then-insert sequence.
    - Implement claim, renewal, revision compare-and-swap, deletion/inactivation, and
      restart reconstruction with stale-owner rejection.
    - _Requirements: 10.3, 10.5, 10.6, 10.9, 10.10, 10.12, 10.13_
  - [x] 4.4 Implement DSQL queue samples and durable action outbox
    - Persist same-writer sequence-fenced samples; scan only one namespace for due
      action buckets; claim with epochs and leases; record attempt start before I/O;
      and finalize only the current claim.
    - Revalidate the current Configuration Fingerprint before first provider I/O and
      supersede stale pending actions in bounded transactions.
    - _Requirements: 8.1, 8.7; 10.7, 10.8; 12.1, 12.2, 12.8, 12.13_
  - [x] 4.5 Share repository conformance tests between memory and DSQL SQL-shape models
    - Cover concurrent namespace admission, revision/lease fencing, atomic state plus
      action commit, sample sequence suppression, fingerprint supersession, claim
      expiry, stable reload bytes, and deterministic health order.
    - Validate migration contiguity, one-statement files, bind ordering, and
      DSQL-supported SQL without requiring a live cluster.
    - _Requirements: 8.1, 8.7; 10.1, 10.3, 10.4, 10.5, 10.6, 10.12, 10.13; 12.1, 12.8_
  - [x] 4.6 Property test: Property 10 — concurrent decision commit creates at most one action
    - Generate candidate decisions racing on one revision and prove that at most one
      state advance succeeds and that any action row is atomic with its next scaler
      state, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 10: concurrent decision commit creates at most one action`
    - _Requirements: 10.3, 10.4, 10.6; 15.3_
  - [x] 4.7 Property test: Property 11 — restart, capacity, and fingerprint fences survive
    - Generate restarts, lease transfers, configuration mutations, deletions, and
      action states against the repository model; prove durable scaler recovery,
      stale-owner rejection, the 100-instance bound, stale-action suppression, and
      retained audit records, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 11: restart, capacity, and fingerprint fences survive`
    - _Requirements: 10.1, 10.2, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11, 10.12, 10.13_

- [x] 5. Checkpoint: storage contracts and migrations are green
  - Run formatting, focused storage/runtime clippy and checks, repository conformance
    tests, migration/SQL-shape tests, and Properties 10 and 11.

- [x] 6. Implement pure batching and the pinned `no-sync` scaler
  - [x] 6.1 Implement the per-version observation batch state machine
    - Batch independently by namespace and exact Deployment Version, retain saturating
      sync/no-sync counts, route observations to effective groups, and schedule the
      fixed 500 ms no-sync or 60 second sync-only deadline using an explicit clock.
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9_
  - [x] 6.2 Implement the pure `no-sync` evaluator
    - Reproduce the pinned decision behavior for no-sync demand, strict backlog
      thresholds, cooloff, refresh, disabled refresh, epsilon suppression, missing
      prior rate, action reason precedence, count `1`, last-scale-up time, and prior
      per-type rates.
    - Keep time and all inputs explicit and do not call delivery, storage, Nexus, or
      kernel code from the evaluator.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 9.9, 9.10, 9.11, 9.12, 9.13_
  - [x] 6.3 Property test: Property 7 — batch eligibility matches the reference clock
    - Generate observation/time sequences and compare due times, independent version
      state, and exact saturating counts with a reference clock model, for at least 100
      cases.
    - Tag: `// Feature: worker-compute-controller, Property 7: batch eligibility matches the reference clock`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9_
  - [x] 6.4 Property test: Property 9 — `no-sync` decisions match the pinned reference model
    - Implement an independent model from `no_sync_match.go @ edd947d743d2`; generate
      valid configs, state, demand/metric inputs, effective types, and times; compare
      action, reason, timestamps, and prior rates for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 9: no-sync decisions match the pinned reference model`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 9.9, 9.10, 9.11, 9.12, 9.13_

- [x] 7. Checkpoint: pure controller policy is green
  - Run formatting, focused runtime clippy/checks, table tests, and Properties 7 and 9.

- [x] 8. Emit demand observations from the delivery plane
  - [x] 8.1 Add the bounded non-blocking observation sink
    - Record only a best-effort `try_send` after delivery dedupe and matching, with
      explicit disabled/full/closed outcomes that never alter publication success or
      ordering.
    - _Requirements: 5.9, 5.10, 5.11, 5.12; 12.10_
  - [x] 8.2 Observe unique versioned workflow publications
    - Emit one normal-queue observation with namespace, queue, exact Deployment
      Version, Workflow type, and actual sync/no-sync result; exclude unversioned and
      still-sticky work; emit only if sticky fallback reaches the normal queue.
    - _Requirements: 5.1, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9_
  - [x] 8.3 Observe unique versioned activity publications
    - Emit one post-dedupe observation with the same exact-version and sync/no-sync
      semantics while preserving existing activity delivery order and retry behavior.
    - _Requirements: 5.2, 5.4, 5.7, 5.8, 5.9; 15.6_
  - [x] 8.4 Add workflow/activity broker regressions for sink isolation
    - Prove enabled, disabled, full, closed, and blocked controller sinks do not change
      dedupe, sticky fallback, priority/fairness, task ordering, or publication
      outcomes.
    - _Requirements: 5.10, 5.11, 5.12; 12.10; 15.6_

- [x] 9. Complete exact-version Nexus polling and observation
  - [x] 9.1 Preserve Nexus poll Deployment options at the edge
    - Validate VERSIONED deployment options, preserve Deployment name and Build ID
      through the runtime adapter, and leave unversioned requests unchanged.
    - _Requirements: 6.1, 6.2, 6.5_
  - [x] 9.2 Make Nexus broker identity version-aware
    - Introduce `NexusQueueKey` with namespace, queue, and optional exact Deployment
      Version; key waiters/tasks by exact identity; register a versioned poll as Nexus
      membership before the long wait, including timeout.
    - _Requirements: 6.3, 6.4, 6.5, 6.6_
  - [x] 9.3 Stamp workflow-origin Nexus tasks from authoritative runtime state
    - Use the already-loaded workflow state's effective Deployment Version when
      publishing a Nexus task, without adding kernel state, commands, or I/O.
    - Preserve task tokens, private task IDs, and workflow response correlation.
    - _Requirements: 5.3, 5.4, 5.9; 6.4, 6.7_
  - [x] 9.4 Observe unique version-routed Nexus publications
    - Emit one post-dedupe observation with actual exact-key sync/no-sync matching
      result and retain the same non-blocking sink isolation as workflow/activity
      publication.
    - _Requirements: 5.3, 5.4, 5.7, 5.8, 5.9, 5.10, 5.11, 5.12_
  - [x] 9.5 Property test: Property 5 — observation is post-dedupe and non-blocking
    - Generate workflow, activity, and Nexus publications plus ready/full/closed/
      disabled sinks; prove exact observation cardinality and publication independence
      for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 5: observation is post-dedupe and non-blocking`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11, 5.12_
  - [x] 9.6 Property test: Property 6 — Nexus version isolation preserves response identity
    - Generate mixed versioned/unversioned waiters and tasks; prove exact-key delivery
      and invariant token/private-ID/workflow-correlation bytes for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 6: Nexus version isolation preserves response identity`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_
  - [x] 9.7 Extend Nexus regressions
    - Retain existing HTTP, task transport, async completion, timeout, token, and
      workflow-resolution behavior for unversioned and exact-version paths.
    - _Requirements: 6.5, 6.7; 15.7_

- [x] 10. Checkpoint: broker observation and Nexus identity are green
  - Run formatting, focused edge/runtime clippy and checks, all affected broker/Nexus
    tests, and Properties 5 and 6.

- [x] 11. Produce durable queue-home metrics snapshots
  - [x] 11.1 Publish bounded exact-version queue samples
    - Sample every ten seconds by namespace, exact Deployment Version, task type, and
      task queue; persist same-writer sequence numbers and expire samples after two
      minutes.
    - Keep the sample write off per-task publication paths.
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.7, 8.8_
  - [x] 11.2 Derive workflow and activity queue metrics
    - Combine existing durable backlog and broker stats into per-type backlog and
      dispatch-rate samples without changing take order or delivery state.
    - _Requirements: 8.2, 8.3, 8.4, 8.7, 8.8_
  - [x] 11.3 Derive Nexus queue metrics from authoritative pending operations
    - Use broker stats while live and reconstruct a conservative backlog from pending
      Nexus operations after broker-memory loss, tolerating inclusion of already
      in-flight work rather than putting correctness on broker memory.
    - _Requirements: 8.2, 8.3, 8.4, 8.7, 8.8_
  - [x] 11.4 Aggregate version and effective-group snapshots
    - Sum only non-expired samples separately for Workflow, Activity, and Nexus; route
      each type to exactly its effective group; emit zeros for absent types/queues.
    - _Requirements: 7.7, 7.8, 7.9; 8.2, 8.3, 8.4, 8.5, 8.6_
  - [x] 11.5 Property test: Property 8 — metrics aggregate by version, type, and effective group
    - Generate exact-version queue samples, expiries, and group partitions; compare
      sums and zero behavior with an independent aggregation model and prove delivery
      state is unchanged, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 8: metrics aggregate by version, type, and effective group`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8_

- [x] 12. Reconcile deployments and commit activation/scale decisions
  - [x] 12.1 Implement the namespace catalog adapter and reconcile notifications
    - Present namespace IDs/names and eligible Worker Deployment versions to the
      runtime through a provider-neutral catalog interface; emit non-blocking
      reconcile hints after successful ComputeConfig mutations.
    - Preserve mutation latency and existing registry read/write semantics if the
      controller channel is full or unavailable.
    - _Requirements: 1.3; 4.3, 4.5; 15.8_
  - [x] 12.2 Reconcile one durable controller per eligible Deployment Version
    - Run startup and fixed 60-second catalog reconciliation, admit up to 100 active
      instances per namespace, expose capacity-limited health, inactivate deleted or
      ineligible versions, and retain their records.
    - _Requirements: 1.4; 4.3, 4.7, 4.8; 10.9, 10.10, 10.12, 10.13_
  - [x] 12.3 Reconcile groups and activation fingerprints
    - Create one activation action per group fingerprint, include empty queue bindings
      before observations exist, avoid advancing the no-sync last-scale-up timestamp,
      preserve scaler state across eligible config updates, and reset the group
      incarnation only after removal or ineligible replacement.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.6, 4.7, 4.8; 9.11, 9.13_
  - [x] 12.4 Evaluate due batches and metrics under a fenced controller claim
    - Feed only eligible groups to the pure scaler, atomically commit next state plus
      any action, retain prior terminal failures without blocking future decisions,
      and schedule the next controller poll.
    - _Requirements: 7.4, 7.5, 7.7, 7.8, 7.9; 9.1, 9.2, 9.3, 9.11, 9.12; 10.3, 10.4; 12.7_
  - [x] 12.5 Property test: Property 4 — one activation per group fingerprint
    - Generate controller records, eligible configurations, and reordered duplicate
      reconcile hints; prove exactly one activation per fingerprint and one new
      activation after a fingerprint change, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 4: one activation per group fingerprint`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_
  - [x] 12.6 Add reconciliation and scaler integration tests
    - Cover pre-existing startup config, prompt update hints, empty activation,
      observation and periodic recovery, restart cooloff/rates, config change,
      deletion/re-add, and 100-instance capacity promotion.
    - _Requirements: 4.1, 4.2, 4.3, 4.5, 4.6, 4.7, 4.8; 7.9; 10.5, 10.9, 10.12, 10.13_

- [x] 13. Checkpoint: metrics and reconciliation are green
  - Run formatting, focused edge/runtime/storage clippy and checks, queue-sample and
    reconciliation integration tests, and Properties 4 and 8.

- [x] 14. Build and validate the provider action contract
  - [x] 14.1 Canonically construct `InvokeWorkerRequest`
    - Use the action ID as request ID; include exact namespace, Deployment Version,
      group, count `1`, provider type/details, fingerprint, reason, and unique
      lexicographically sorted queue bindings.
    - Exclude task payloads, workflow/run identity, credentials, tokens, and
      authorization grants; apply the existing Nexus payload-size validator before
      committing an action.
    - _Requirements: 11.5, 11.6, 11.7, 11.8, 11.9, 11.10; 15.12_
  - [x] 14.2 Encode the Nexus protobuf payload and fixed operation identity
    - Emit exactly one `binary/protobuf` payload with the approved message type,
      fixed service, fixed operation, action request ID, and the action deadline needed
      by existing Nexus timeout headers.
    - _Requirements: 11.2, 11.3, 11.4, 11.5; 13.7_
  - [x] 14.3 Validate provider completion exactly
    - Accept only synchronous success with exactly one decodable response payload and
      matching request ID; classify asynchronous acceptance, missing/multiple/
      malformed payloads, mismatched IDs, unsuccessful operations, and handler errors
      into the approved retryable or terminal outcomes.
    - _Requirements: 11.11, 11.12, 11.13, 11.14; 12.4, 12.5_
  - [x] 14.4 Property test: Property 12 — provider request encoding is canonical and secret-free
    - Generate valid action inputs; prove deterministic encoding, exact decoded fields,
      unique sorted queues, count one, and absence of forbidden identity/credential
      material for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 12: provider request encoding is canonical and secret-free`
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 11.10; 15.12_
  - [x] 14.5 Property test: Property 13 — provider completion validation is exact
    - Generate Nexus success/error/async/malformed response shapes and prove only the
      exact matching synchronous response reaches Delivered, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 13: provider completion validation is exact`
    - _Requirements: 11.11, 11.12, 11.13, 11.14_

- [x] 15. Reuse Nexus External and Worker targets for provider delivery
  - [x] 15.1 Add a provider-neutral Nexus delivery outcome
    - Extend the Nexus HTTP handler error shape with bounded retryability while
      preserving existing callers and request-size, timeout, endpoint, header, and
      error behavior.
    - Keep retry policy and controller state transitions in worker-compute runtime
      code, not at the edge or in Nexus transport.
    - _Requirements: 12.3, 12.4, 12.5; 13.7_
  - [x] 15.2 Deliver External endpoint actions through the existing Nexus HTTP client
    - Resolve the endpoint at every attempt, build a
      `StartOperation` request with an empty callback and action deadline, and avoid
      provider-specific HTTP routes or clients.
    - _Requirements: 13.1, 13.3, 13.4, 13.5, 13.6, 13.7_
  - [x] 15.3 Deliver Worker endpoint actions through the Nexus task broker
    - Add `NexusTaskCorrelation::WorkerCompute { action_id, claim_epoch }`, publish
      through the existing Worker target, and maintain runtime-owned bounded waiters
      for the current delivery attempt.
    - _Requirements: 13.2, 13.3, 13.4, 13.5, 13.6, 13.7_
  - [x] 15.4 Translate Worker target completion and failure without owning policy
    - Correlate public Nexus completion/failure RPCs to the current worker-compute
      waiter, map late/duplicate responses to existing `NOT_FOUND`, and return a
      provider-neutral outcome to runtime.
    - Preserve all existing workflow-origin Nexus correlation and response behavior.
    - _Requirements: 11.11, 11.12, 11.13; 13.2, 13.7; 15.7_
  - [x] 15.5 Property test: Property 15 — endpoint re-resolution changes only transport
    - Generate endpoint create/update/target-kind/delete sequences; prove each attempt
      uses the current endpoint while durable payload bytes remain identical and only
      the Nexus adapter selects transport, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 15: endpoint re-resolution changes only transport`
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7_
  - [x] 15.6 Add provider-neutral External/Worker adapter tests
    - Use fake endpoint records and broker completions for success, timeout, retryable
      and terminal handler failures, async acceptance, mismatched IDs, endpoint
      deletion/recreation, duplicate/late responses, and workflow-Nexus regression.
    - _Requirements: 11.11, 11.12, 11.13; 13.1, 13.2, 13.5, 13.6, 13.7; 15.1, 15.7_

- [x] 16. Checkpoint: provider contract and Nexus adapters are green
  - Run formatting, focused proto/edge/runtime clippy and checks, Nexus transport and
    action-contract tests, and Properties 12, 13, and 15.

- [x] 17. Deliver actions through the durable outbox
  - [x] 17.1 Implement namespace-scoped action claim and shutdown behavior
    - Claim only due actions in owned namespace buckets, stop claiming on shutdown,
      let interrupted claims expire, and never hold workflow/activity/Nexus delivery
      resources across provider I/O.
    - _Requirements: 12.8, 12.10, 12.12, 12.13_
  - [x] 17.2 Begin attempts with a current-fingerprint fence
    - Increment attempt state and persist attempt start before I/O; supersede stale
      pending actions; allow already in-flight old-fingerprint delivery to finish as
      audit-only; and prevent stale claims from finalizing newer attempts.
    - _Requirements: 10.7, 10.8; 12.2, 12.11, 12.13_
  - [x] 17.3 Implement bounded retry and terminal failure state
    - Reuse immutable action ID/request bytes across exponential retry (1 second,
      multiplier two, cap one hour), recover after restart/claim expiry, classify
      terminal failures, retain bounded redacted health, and permit later scaler
      decisions after failure.
    - _Requirements: 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.11, 12.13_
  - [x] 17.4 Preserve at-least-once provider semantics explicitly
    - Ensure every retry uses Action_Request_ID as the provider idempotency key and the
      provider-neutral test double deduplicates repeated accepted IDs without claiming
      exactly-once transport.
    - _Requirements: 11.14; 12.2, 12.9_
  - [x] 17.5 Property test: Property 14 — retry preserves action identity and isolates delivery
    - Generate transient failures, restarts, claim expiry, stale finalizers, eventual
      success, and terminal failure; prove stable ID/bytes, claim fencing, and unchanged
      task publication state/order for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 14: retry preserves action identity and isolates delivery`
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9, 12.10, 12.11, 12.12, 12.13; 15.4, 15.6_
  - [x] 17.6 Add outbox recovery and isolation integration tests
    - Cover persisted pending action restart, claim loss, slow/unavailable provider,
      config change while Pending and Claimed, endpoint mutation, terminal failure
      followed by another decision, and clean shutdown recovery.
    - _Requirements: 10.5, 10.7, 10.8; 12.3, 12.6, 12.7, 12.8, 12.10, 12.11, 12.12, 12.13_

- [x] 18. Bootstrap and supervise the controller service
  - [x] 18.1 Build the process-local controller service
    - Supervise catalog reconciliation, observation batching, queue sampling,
      controller evaluation, and action delivery as bounded child loops with explicit
      shutdown and failure behavior.
    - Start exactly one service per `tokeirad` process only when the policy is enabled.
    - _Requirements: 1.2, 1.4; 4.3; 8.1; 12.12_
  - [x] 18.2 Wire the app namespace catalog and repositories
    - Adapt the edge namespace cache and existing runtime/storage construction into the
      provider-neutral ports; keep endpoint resolution in the existing Nexus registry
      and retain normal server startup when the feature is disabled.
    - _Requirements: 1.2, 1.3, 1.4, 1.5; 2.10; 13.1, 13.2_
  - [x] 18.3 Add a deny-by-construction provider-neutral integration harness
    - Provide an in-process Nexus provider that records/deduplicates requests and can
      return all approved outcomes, with ports that cannot open cloud credentials,
      Docker, Yadori, or sibling-repository processes.
    - _Requirements: 15.1, 15.2, 15.5, 15.9, 15.10_
  - [x] 18.4 Property test: Property 1 — disabled configuration is inert
    - Generate stored registries, publications, polls, mutations, and time advances;
      prove existing ComputeConfig results and an empty worker-compute action set when
      disabled, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 1: disabled configuration is inert`
    - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6_
  - [x] 18.5 Property test: Property 17 — provider-neutral tests require no Yadori or cloud state
    - Generate controller scenarios against the in-process provider and prove decision/
      retry equivalence while the default harness cannot access live provider, cloud
      credential, Docker, or sibling-process ports, for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 17: provider-neutral tests do not require Yadori or cloud state`
    - _Requirements: 15.1, 15.2, 15.5, 15.8, 15.9, 15.10, 15.11_

- [x] 19. Add bounded observability, diagnostics, and operator truth
  - [x] 19.1 Instrument controller observations, decisions, and delivery
    - Add bounded counters and latency metrics, omitting task queue, action ID,
      Deployment, Build ID, and group from labels; add structured action logs with
      required identity/reason fields and no provider details or credentials.
    - _Requirements: 14.1, 14.2, 14.4, 14.5, 14.6_
  - [x] 19.2 Implement read-only worker-compute diagnostics
    - Add `tkr diagnostics worker-compute --namespace <name>` with stable text and JSON
      output over namespace-scoped durable health, deterministic ordering, bounded
      failure categories, and contextual non-zero storage errors.
    - _Requirements: 10.10; 12.6; 14.3, 14.6_
  - [x] 19.3 Update generated feature and configuration truth
    - Classify `worker-compute-controller` as experimental/default-disabled; state that
      only remote Nexus plus `no-sync` is controller-eligible; mark `rate-based` and
      direct providers accepted-but-ineligible; document exact enablement, capacity
      launch cost, at-least-once/idempotency responsibility, and provider readiness
      meaning.
    - Correct stale architecture text that presents remote provider invocation as
      already active while preserving the architecture decision record.
    - _Requirements: 1.7, 1.8; 14.7, 14.8, 14.9, 14.10_
  - [x] 19.4 Property test: Property 16 — diagnostics and telemetry remain bounded and truthful
    - Generate controller/action health and observability manifests; prove exact
      durable health, bounded categories/label sets, deterministic serialization, and
      absence of provider details/credentials for at least 100 cases.
    - Tag: `// Feature: worker-compute-controller, Property 16: diagnostics and telemetry remain bounded and truthful`
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6_

- [x] 20. Complete integration and regression coverage
  - [x] 20.1 Run the complete provider-neutral lifecycle scenarios
    - Cover startup/update activation, workflow/activity/Nexus no-sync demand, periodic
      recovery, refresh/epsilon behavior, persisted restart, endpoint change/delete/
      recreate, Pending/Claimed fingerprint changes, capacity promotion, provider
      isolation, and clean cancellation.
    - _Requirements: 4.1, 4.2, 4.3, 4.5; 7.9; 9.4, 9.5, 9.7; 10.5, 10.7, 10.8, 10.12, 10.13; 12.8, 12.10, 12.13; 15.2_
  - [x] 20.2 Preserve Worker Deployment and broker/Nexus regression suites
    - Keep ComputeConfig update-mask, request-ID dedupe, conflict-token, validation,
      and round-trip behavior; keep workflow/activity ordering, fairness, sticky,
      dedupe, and poll behavior; keep Nexus HTTP/task/async/token/workflow behavior.
    - _Requirements: 1.3; 15.6, 15.7, 15.8_
  - [x] 20.3 Enforce the kernel and authorization boundaries
    - Add dependency/diff assertions proving there are no worker-compute kernel
      commands, fields, serialized-state changes, or I/O, and that this feature grants
      no provider or guest-worker workflow/task access.
    - Leave all JWT/STS guest-worker claim and permission work to the separate
      `scoped-worker-authorization` specification.
    - _Requirements: 1.5; 15.11, 15.12_
  - [x] 20.4 Keep cross-repository validation opt-in
    - Add only the Tokeira-side contract fixture or ignored test entry needed for a
      separately invoked Yadori integration; ensure the default workspace bar neither
      starts nor requires the sibling repository.
    - _Requirements: 15.9, 15.10_

- [x] 21. Final checkpoint: full implementation bar is green
  - Run the repository completion bar:
    - `cargo +nightly fmt --all`
    - `cargo lint --locked`
    - `cargo check --workspace --locked`
    - `cargo test --workspace --locked`
    - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`
  - Confirm every Property 1–17 PBT runs at least 100 cases, all affected Worker
    Deployment/broker/Nexus regressions are green, migration/link checks pass, the tree
    remains clean after validation, and no live Yadori/cloud/Docker dependency enters
    the default bar.

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1", "2"],
  "4": ["3"],
  "5": ["4"],
  "6": ["2", "5"],
  "7": ["6"],
  "8": ["3"],
  "9": ["8"],
  "10": ["9"],
  "11": ["5", "10"],
  "12": ["6", "11"],
  "13": ["12"],
  "14": ["2", "5"],
  "15": ["10", "14"],
  "16": ["15"],
  "17": ["12", "16"],
  "18": ["13", "17"],
  "19": ["18"],
  "20": ["18", "19"],
  "21": ["20"]
}
```

## Notes

- This plan implements the approved first slice only: remote Nexus providers plus the
  `no-sync` scaler. `rate-based` and direct cloud providers remain accepted and stored
  but controller-ineligible.
- The kernel is deliberately absent from the implementation graph. Workflow-origin
  Nexus version identity comes from authoritative state already loaded by the runtime
  publisher; observations, policy, storage, provider I/O, and diagnostics remain in
  their owning planes.
- No new crate or third-party dependency is planned. Adding either is an architectural
  change and requires a new owner decision before implementation.
- DSQL migrations use the next contiguous versions at implementation time. They remain
  forward-only, one statement per migration, and use a separate asynchronous index
  migration.
- The sibling Yadori repository is a provider implementation and optional integration
  target, not a build or test dependency. Guest-worker JWT/STS authorization remains
  owned by the separate `scoped-worker-authorization` specification and Issue #29.
- A checkpoint is a stop-and-fix gate: do not advance while its focused compilation,
  lint, or tests are red.
