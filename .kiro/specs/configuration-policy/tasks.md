# Implementation Plan

- [x] 1. Build the source-aware Temporal configuration denominator
  - [x] 1.1 Add the Go-AST extraction tool
    - Create `tools/temporal-config-audit/` using only the Go standard library.
    - Read the requested Temporal tag through `git archive`, parse production
      non-test Go files, and extract literal-key `New*Setting` calls with constructor,
      scope, value kind, default expression, and repository-relative source anchor.
    - Reject duplicate keys, non-literal setting keys, and unrecognized setting
      constructors without writing partial output.
    - Add focused Go tests for constructor recognition, test-file exclusion, source
      anchors, and deterministic ordering.
    - _Requirements: 2.1, 2.5–2.7, 2.13–2.14_
  - [x] 1.2 Generate and verify the v1.31.0 setting snapshot
    - Run the extractor against the local Temporal reference checkout at tag
      `v1.31.0`.
    - Commit the sorted snapshot at
      `crates/tokeira-compatibility/data/temporal-v1.31.0-settings.json`.
    - Assert the verified snapshot contains 613 unique production declarations,
      including the declarations outside `common/dynamicconfig/constants.go`.
    - Add fixed examples for `matching.useNewMatcher`,
      `matching.enableFairness`, `matching.priorityLevels`, and
      `activity.enableStandalone`.
    - _Requirements: 2.1, 2.5–2.7, 2.13–2.14_
  - [x] 1.3 Add the structured classification ledger
    - Add
      `crates/tokeira-compatibility/data/temporal-v1.31.0-classification.json`.
    - Classify every dynamic setting and each relevant static configuration group
      with exactly one disposition, treatment, owner, conformance-override state,
      and repository-relative evidence.
    - Preserve source facts and owner classification in separate files so extraction
      cannot overwrite a decision.
    - Complete the initial classification with no unresolved disposition.
    - _Requirements: 2.2–2.4, 2.8, 2.12_
  - [x] 1.4 Implement the pure ledger verifier and conformance cross-check
    - Add `crates/tokeira-compatibility/src/configuration.rs`.
    - Enforce exact source/classification key-set equality, uniqueness, complete
      metadata, deterministic joins, and disposition counts.
    - Cross-check classification against
      `tokeira-conformance::KEY_CLASSIFICATION` in tests/tooling without adding a
      production dependency on the conformance crate.
    - _Requirements: 2.3–2.12, 7.7–7.8_
  - [x] 1.5 Property test: Property 3 — source denominator determinism
    - Implement a `proptest` property with at least 100 cases over permutations and
      invalid declaration mutations.
    - Tag:
      `// Feature: configuration-policy, Property 3: source denominator determinism`
    - _Requirements: 2.1, 2.5–2.7, 2.9–2.10, 2.13–2.14_
  - [x] 1.6 Property test: Property 4 — classification-ledger exactness
    - Implement a reference-verifier PBT with at least 100 deletion, insertion,
      duplication, missing-owner/evidence, and conformance-disposition mutations.
    - Tag:
      `// Feature: configuration-policy, Property 4: classification-ledger exactness`
    - _Requirements: 2.2–2.12, 7.7–7.8, 12.3_

- [x] 2. Enrich and audit the canonical Feature Catalog
  - [x] 2.1 Extend `FeatureEntry` with operator-facing catalog metadata
    - Add origin, conformance disposition, Temporal maturity, Temporal/Tokeira
      defaults, enablement, scopes, mutability, guidance, and prerequisites.
    - Preserve `FeatureState` and `dynamic_config_key` behavior for compatibility
      dispatch.
    - Extend serialization, compatibility CLI/service projections, and the feature
      digest without introducing runtime reflection.
    - _Requirements: 10.4–10.10, 10.14_
  - [x] 2.2 Reconcile every existing matrix record with current evidence
    - Verify each record against v1.31.0 behavior and the current Tokeira
      implementation before changing its state.
    - Correct stale state, note, evidence, maturity, and default claims, including
      surfaces landed during the functional-conformance campaign.
    - Do not promote a feature solely because one corpus leaf passed; record partial
      boundaries explicitly.
    - _Requirements: 10.5–10.10, 10.13–10.15_
  - [x] 2.3 Complete non-RPC and Tokeira-native feature coverage
    - Inventory public response/history/capability/behavioral surfaces already
      represented outside RPC ownership.
    - Add Tokeira-owned compatibility-service RPCs, production feature-bearing
      configuration, and explicit Tokeira-native extensions such as AWS IAM bearer
      authorization.
    - Attach manual-review evidence where the denominator is conceptual rather than
      machine-discoverable.
    - _Requirements: 10.3–10.5, 10.10, 10.14_
  - [x] 2.4 Strengthen catalog validation while preserving existing RPC totality
    - Retain exact equality between matrix RPC ownership and vendored
      WorkflowService/OperatorService definitions.
    - Assert the 121 v1.31.0 target / 8 newer-wire partition, unique feature ids,
      unique machine-denominated surface ownership, and coherent catalog records.
    - Reject available default-disabled features without exact production
      enablement guidance.
    - _Requirements: 10.1–10.2, 10.5–10.13, 12.11–12.12_
  - [x] 2.5 Property test: Property 5 — feature-catalog surface ownership
    - Implement a reference-set PBT with at least 100 permutations and
      remove/duplicate/invent mutations.
    - Tag:
      `// Feature: configuration-policy, Property 5: feature-catalog surface ownership`
    - _Requirements: 10.1–10.2, 10.11–10.13, 12.11_
  - [x] 2.6 Property test: Property 6 — feature availability and guidance coherence
    - Generate valid and invalid combinations of state, origin, disposition,
      defaults, enablement, scope, mutability, evidence, and guidance for at least
      100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 6: feature availability and guidance coherence`
    - _Requirements: 10.3–10.10, 10.14–10.15, 12.12_

- [x] 3. Checkpoint: compatibility metadata and audits are green
  - Run Go formatting/tests for `tools/temporal-config-audit`.
  - Run formatting, check, clippy, and focused tests for
    `tokeira-compatibility`, `tokeira-compatibility-service`, and the compatibility
    CLI projections.
  - Confirm rerunning the extraction produces no diff.
  - Confirm the repository remains clean outside this feature's intended files.

- [x] 4. Add the typed production task-queue deployment policy
  - [x] 4.1 Add `TaskQueuePolicyConfig`
    - Add the strict `[policy.task_queues]` section to `PolicyConfig`.
    - Add `enable_fairness: bool` with an absent-section default of `false`.
    - Include the resolved value in effective-config output and preserve unrelated
      configuration values.
    - _Requirements: 4.1–4.3, 4.7–4.10, 9.1–9.4_
  - [x] 4.2 Add the production configuration-field catalog
    - Add `crates/tokeira-config/src/documentation.rs`.
    - Inventory every strict production field with class, default, requirement,
      restart behavior, owning feature, and operator guidance.
    - Add a complete annotated fixture and uniqueness/default coverage tests.
    - _Requirements: 11.7–11.21_
  - [x] 4.3 Replace the stock-only delivery provider with a typed provider
    - Add `StaticDeliveryPolicy` and `ConfiguredDeliveryModeProvider`.
    - Pass only typed startup policy from `tokeirad`; retain priority enabled,
      five bands, default key 3, and auto-enable disabled.
    - Keep the existing conformance adapter as the only raw-key mapping path and
      preserve its scope-generation reset.
    - _Requirements: 3.1–3.5, 4.4–4.8, 5.1–5.9, 7.1–7.6_
  - [x] 4.4 Preserve architectural isolation
    - Keep policy resolution in config/runtime/server construction.
    - Add or extend dependency tests proving the kernel has no configuration or
      conformance dependency and the lane router does not consult delivery policy.
    - _Requirements: 8.1–8.9_
  - [x] 4.5 Property test: Property 1 — effective-policy precedence
    - Implement a pure reference-model PBT over all valid source combinations with
      at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 1: effective-policy precedence`
    - _Requirements: 1.1–1.8, 3.1–3.10, 7.1–7.9_
  - [x] 4.6 Property test: Property 2 — configuration defaulting and round-trip
    preservation
    - Generate valid `TokeiraConfig` values and old-shape TOML omitting
      `policy.task_queues`; run at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 2: configuration defaulting and round-trip preservation`
    - _Requirements: 4.1–4.3, 4.7–4.11, 9.1–9.4, 12.2_
  - [x] 4.7 Property test: Property 12 — delivery-mode composition
    - Generate queue kinds, sticky state, metadata, deployment policy, and
      conformance overlays against a pure reference model for at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 12: delivery-mode composition`
    - _Requirements: 4.4–4.6, 5.1–5.10, 8.6, 8.8, 12.8–12.10_

- [x] 5. Implement durable task-queue configuration storage
  - [x] 5.1 Add storage-domain records and repository interface
    - Add stored task-queue kind, key, metadata, complete record, revision, and CAS
      result types to `tokeira-storage`.
    - Add async load, conditional put, and list-all repository methods.
    - Keep the record outside run state, workflow history, dispatch rows, and
      projection state.
    - _Requirements: 6.1–6.3, 6.6–6.9, 6.12–6.15_
  - [x] 5.2 Add the next contiguous DSQL migration
    - Create the `task_queue_config` table using the build-phase no-`ALTER` rule,
      DSQL-supported types, a composite namespace/queue/kind primary key, revision,
      encoded record, and update time.
    - Update migration validation/checksum expectations without editing unrelated
      migrations.
    - _Requirements: 6.1, 6.6–6.8, 9.5–9.7_
  - [x] 5.3 Add the task-queue configuration codec
    - Encode and decode every record field with checked revision/kind conversions.
    - Add malformed-kind, malformed-revision, and truncated-record example tests.
    - _Requirements: 6.6, 6.9–6.11_
  - [x] 5.4 Implement the in-memory repository
    - Provide the same conditional-create/update revision behavior as DSQL.
    - Return deterministic list ordering for hydration and tests.
    - _Requirements: 6.7–6.11, 9.5, 9.7_
  - [x] 5.5 Implement the DSQL repository
    - Follow the existing connection director and transaction/CAS patterns.
    - Normalize DSQL serialization failures to CAS conflict and preserve other
      storage failures.
    - Keep control-plane writes low-volume and independent from run transition
      commits.
    - _Requirements: 6.1–6.3, 6.7–6.9, 6.11, 9.6_
  - [x] 5.6 Property test: Property 9 — task-queue CAS concurrency
    - Exercise generated same-revision competing candidates and serialized retry
      schedules against the in-memory repository for at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 9: task-queue CAS concurrency`
    - _Requirements: 6.7–6.8, 12.5_
  - [x] 5.7 Property test: Property 10 — task-queue codec equivalence
    - Generate complete valid records, including both optional metadata records and
      arbitrary valid fairness maps, for at least 100 round trips.
    - Tag:
      `// Feature: configuration-policy, Property 10: task-queue codec equivalence`
    - _Requirements: 6.6, 6.9–6.11, 12.6_

- [x] 6. Checkpoint: configuration and storage foundations are green
  - Run formatting, check, clippy, and focused tests for `tokeira-config`,
    `tokeira-storage`, and `tokeira-runtime` consumers affected by type changes.
  - Run migration validation and the DSQL integration tests available in the
    current environment.
  - Confirm no lockfile dependency movement occurred beyond adding the internal
    documentation tool package.

- [x] 7. Replace the volatile runtime store with the repository-backed facade
  - [x] 7.1 Make storage-dependent task-queue store operations async
    - Change `get`, `apply`, and `list` to return explicit runtime store errors.
    - Update the in-memory implementation and focused test doubles.
    - Preserve the process-local `changed` notification contract.
    - _Requirements: 3.9–3.10, 6.1–6.5, 6.10, 6.14–6.15_
  - [x] 7.2 Implement pure patching and repository-backed CAS retries
    - Separate validation and candidate construction from persistence.
    - Commit before cache publication or success response.
    - Reload and reapply after CAS conflict; reject without mutation on validation
      failure.
    - _Requirements: 6.1–6.9, 6.14_
  - [x] 7.3 Implement hydration, cache refresh, and wake propagation
    - Hydrate all stored records before serving polls.
    - Refresh remote revisions on a bounded internal interval and notify only keys
      whose effective record changed.
    - Treat the cache as disposable and reconstructible.
    - _Requirements: 6.3–6.5, 6.10–6.15_
  - [x] 7.4 Refactor broker consult sites safely
    - Await policy lookup before acquiring workflow, activity, or Nexus ready-queue
      locks.
    - Preserve queue ordering, rate-limit state, sticky behavior, and long-poll wake
      semantics.
    - Add a focused assertion/review guard that no storage await occurs while a
      broker lock is held.
    - _Requirements: 4.4–4.6, 5.7, 5.10, 8.6–8.9_
  - [x] 7.5 Property test: Property 8 — task-queue patch state machine
    - Implement a generated operation-sequence PBT against a pure atomic reference
      model with at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 8: task-queue patch state machine`
    - _Requirements: 6.1–6.7, 6.9–6.10, 6.12–6.15, 12.4_
  - [x] 7.6 Property test: Property 11 — restart recovery
    - Generate finite successful patch histories, discard the runtime cache, hydrate
      a fresh store, and compare edge/broker views for at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 11: restart recovery`
    - _Requirements: 6.2–6.5, 11.3, 12.7_

- [x] 8. Wire the durable store through edge and server construction
  - [x] 8.1 Update the gRPC handler
    - Await the store mutation and project only the committed record.
    - Preserve exact validation messages and `INVALID_ARGUMENT` mapping.
    - Map repository/CAS-exhaustion failures to explicit `UNAVAILABLE`.
    - _Requirements: 6.1–6.2, 6.14, 11.3_
  - [x] 8.2 Share one repository-backed store with all consumers
    - Add the repository bound to `build_and_serve_with_storage`.
    - Wire in-memory and DSQL repository implementations.
    - Hydrate before accepting traffic and share the store with edge, workflow,
      activity, and Nexus brokers.
    - _Requirements: 6.3–6.5, 6.10–6.11, 9.5–9.7_
  - [x] 8.3 Add edge/server integration coverage
    - Test read-after-commit, invalid-patch preservation, storage unavailability,
      same-name task-kind isolation, and restart recovery.
    - Verify subsequent delivery consumes committed rates and weight overrides
      without another update.
    - _Requirements: 6.2–6.6, 6.14–6.15, 12.7_

- [x] 9. Checkpoint: runtime, edge, and server integration are green
  - Run formatting, check, clippy, and focused tests for `tokeira-runtime`,
    `tokeira-edge`, `tokeira-storage`, and `tokeirad`.
  - Run the relevant v1.31.0 task-queue config leaves and the active
    `TestPrioritySuite`, `TestFairnessSuite`, and `TestFairnessAutoEnableSuite`
    leaves twice using the functional-conformance runner.
  - Confirm production-default and configured-fairness runs remain independent.

- [x] 10. Generate the public configuration and feature documentation
  - [x] 10.1 Add `tools/compatibility-docs`
    - Add the internal Rust workspace tool with `check` and `write` modes.
    - Consume only verified configuration data, `FEATURE_MATRIX`, and
      `CONFIG_FIELD_CATALOG`.
    - Restrict write mode to its three owned artifacts and make check mode
      byte-for-byte deterministic.
    - _Requirements: 2.9–2.11, 10.6–10.10, 11.5–11.22_
  - [x] 10.2 Generate `temporal-configuration.md`
    - Rename the existing configuration document and replace the incidental
      564-key claim with the verified source-aware denominator.
    - Include every dynamic declaration, relevant static group, extraction
      methodology, disposition counts, and Tokeira treatment.
    - _Requirements: 2.1–2.14, 11.5–11.6, 11.22_
  - [x] 10.3 Generate `tokeira-configuration.md`
    - Publish the empty-configuration guarantee, exhaustive feature availability,
      Temporal/Tokeira defaults, exact enablement actions, policy scope/mutability,
      prerequisites, and every production config field.
    - Clearly distinguish unavailable, excluded, partial, experimental,
      default-disabled, and conformance-only behavior.
    - Include Standalone Activities, User Fairness, authorization, Nexus callbacks,
      public task-queue policy, and Tokeira-native extensions.
    - _Requirements: 10.3–10.10, 11.1–11.13, 11.19–11.22_
  - [x] 10.4 Generate the canonical `config.example.toml`
    - Produce a strict, parseable, annotated example.
    - Show optional sensitive/enforcing sections without enabling them by default.
    - Include exact JWT `iss`, Nexus callback reachability, and User Fairness
      warnings.
    - _Requirements: 11.14–11.21_
  - [x] 10.5 Property test: Property 7 — deterministic documentation projection
    - Generate permutations of verified input catalogs and assert byte-identical
      output plus source-equal availability/default/enablement cells for at least
      100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 7: deterministic documentation projection`
    - _Requirements: 2.9–2.11, 10.6–10.10, 11.5–11.13, 11.22_
  - [x] 10.6 Property test: Property 13 — annotated configuration example coverage
    - Generate valid scalar substitutions and assert strict parsing plus exact
      field-catalog coverage for at least 100 cases.
    - Tag:
      `// Feature: configuration-policy, Property 13: annotated configuration example coverage`
    - _Requirements: 9.3–9.4, 11.7–11.21_

- [x] 11. Curate the public conformance documentation set
  - [x] 11.1 Move deliberation records to owning specs
    - Move the configuration proposal, authorization decision, and Worker
      Versioning decision with `git mv` to the approved `reference/` destinations.
    - Preserve every substantive research, review, and owner-decision paragraph.
    - _Requirements: 11.25–11.27, 11.31_
  - [x] 11.2 Retire the empty decisions page
    - Fold concise resolved outcomes into `README.md`, `supported.md`, or
      `excluded.md`.
    - Remove `decisions.md` only after verifying no decision remains open.
    - Establish owning specs as the starting point for future deliberation.
    - _Requirements: 11.23–11.24, 11.28–11.30_
  - [x] 11.3 Reconcile authoritative public records
    - Update `README.md`, `supported.md`, and `excluded.md` to reflect the final
      feature audit, durable `UpdateTaskQueueConfig`, defaults, and evidence links.
    - Keep `docs/readiness/conformance.md` an evidence/status ledger rather than a
      competing feature catalog.
    - _Requirements: 1.1–1.4, 7.9, 10.13–10.15, 11.1–11.4, 11.23–11.24, 11.29–11.30_
  - [x] 11.4 Repair and validate every moved-document link
    - Update code comments, specs, readiness documents, and public conformance pages.
    - Run offline lychee over all Markdown and leave no old-path references.
    - _Requirements: 11.29–11.32_

- [x] 12. Checkpoint: generated and curated documentation is authoritative
  - Run `compatibility-docs check` and confirm write mode followed by check mode
    produces no diff.
  - Parse and validate `config.example.toml`.
  - Run focused compatibility/configuration tests and offline lychee over every
    Markdown file.
  - Confirm `docs/conformance/v1.31.0/` contains only the approved authoritative
    core plus any published conformance report.

- [ ] 13. Final integration and regression bar
  - [x] 13.1 Run the enforced workspace bar
    - Run `cargo +nightly fmt --all`.
    - Run `cargo lint --locked`.
    - Run `cargo check --workspace --locked`.
    - Run `cargo test --workspace --locked`.
    - Run
      `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
    - Confirm builds and generators leave the worktree clean except for intended
      source changes.
  - [ ] 13.2 Run configuration-specific regression checks
    - Verify an empty TOML document and representative existing configurations
      preserve prior behavior.
    - Verify production builds contain no conformance registry and accept no raw
      Temporal key input.
    - Verify a DSQL-backed restart preserves public task-queue policy.
    - Verify the kernel dependency/purity checks remain green.
    - Local validation on 2026-07-26 completed every check except the live DSQL
      body: `TOKEIRA_DSQL_TEST_DATABASE_URL` was unavailable. The environment-gated
      DSQL repository-recreation test is compiled and ready for that endpoint.
    - _Requirements: 1.3–1.8, 7.1–7.9, 8.1–8.9, 9.1–9.7_
  - [x] 13.3 Run final functional conformance evidence
    - Use the current harness invocation documented in root `AGENTS.md`.
    - Run two clean consecutive passes of the active priority/fairness and
      task-queue configuration coverage.
    - Record only measured outcomes in `docs/readiness/conformance.md`; do not
      infer feature-catalog completeness from the suite result.
    - _Requirements: 5.1–5.10, 6.2–6.5, 7.9, 10.15_

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1", "2"],
  "4": ["3"],
  "5": [],
  "6": ["4", "5"],
  "7": ["4", "5", "6"],
  "8": ["7"],
  "9": ["8"],
  "10": ["3", "4", "8"],
  "11": ["10"],
  "12": ["10", "11"],
  "13": ["3", "6", "9", "12"]
}
```

## Notes

- No implementation task changes `tokeira-kernel`; dependency checks make this an
  explicit invariant rather than an assumption.
- The Temporal source checkout and Go extractor are maintenance inputs only. Normal
  Rust builds, tests, server startup, and documentation checks consume the checked
  snapshot.
- `FEATURE_MATRIX` remains the single canonical feature catalog. The renderer and
  public documents are projections, not parallel sources of truth.
- Task-queue policy is durable control-plane data but not workflow authority. It never
  enters per-run history or a kernel transition.
- DSQL migration numbering must be rechecked immediately before implementation; use
  the next contiguous version and follow the storage crate's build-phase rules.
- Every PBT is required, uses at least 100 cases, and carries the exact feature/property
  tag stated above.
- If implementation evidence contradicts a requirement or property, stop and
  ground-truth the behavior before amending the approved spec.
