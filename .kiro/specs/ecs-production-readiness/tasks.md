# Implementation Plan: ECS Production Readiness

## Overview

Close the ECS production-readiness gaps without replacing `definition.tkd` as desired-state authority or
forking shared provisioner mechanics. Work proceeds from shared operation identity and selection contracts,
through ECS/AWS realization, into bundle admission and the explicitly invoked live-AWS qualification
harness. Every correctness property in [`design.md`](./design.md) has a mandatory property-based test task.

## Tasks

- [ ] 1. Add operation identity and dependency-correct module selection
  - [ ] 1.1 Persist the deployment UUID through implicit provisioner inception
    - Add the backward-decodable optional `deployment_uuid` field to `DeploymentStateEnvelope` and its
      migration/round-trip paths.
    - Pass `DeploymentMetadata.id` into the provisioner's implicit inception path and persist it before any
      non-state resource can be created; do not restore an explicit `tkp init` command.
    - Construct normal `ProvisionOperation` values from the envelope, reject missing UUIDs for ECS managed
      naming, and never infer identity from the deployment directory or editable `definition.tkd`.
    - _Requirements: 5.1, 5.2_

  - [ ] 1.2 Add shared requested/effective module-selection contracts
    - Add `RequestedModules`, `SelectionPurpose`, `SelectionTrace`, and `Scoped<T>` to the shared
      provisioner CLI layer with serialized, deterministic report shapes.
    - Add repeatable `--module <NAME>` parsing and transparent forwarding for infra plan/apply/destroy and
      deploy plan/apply; distinguish absent selection from a present empty selection.
    - Extend `ProvisionerPlatform` operation inputs/results without changing exact resource-id rollback
      deletion.
    - _Requirements: 1.1, 1.2, 1.5, 1.6, 1.7_

  - [ ] 1.3 Implement the pure ECS module-graph resolver
    - Build `ModuleGraph` from the interpreted deployment rather than a second hard-coded module list.
    - Validate unique names, known references, a non-empty known-name request, and acyclicity before AWS or
      state access.
    - Resolve prerequisite closure for plan/apply and dependent closure for destroy, then order results by
      definition index and convert them to `ModuleSelection`.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.8_

  - [ ] 1.4 Wire selection through ECS infra and deploy operations
    - Pass the same resolved scope into composition, plan, apply, and destroy while keeping the complete
      interpreted graph as `known_modules` and only the effective set active.
    - Preserve unrelated state, the whole persisted definition/configuration revision, the binding gate,
      operation lock, topological ordering, and fail-closed deletion.
    - Emit requested/effective sets in deterministic order in human and JSON reports, with actionable
      errors that never substitute `ModuleSelection::All`.
    - _Requirements: 1.5, 1.6, 1.7, 1.8, 1.9_

  - [ ] 1.5 Property test: Property 1 — module closure equals the graph reference model
    - Implement a `proptest` over generated acyclic graphs and absent/non-empty known-name requests, with at
      least 128 cases, comparing both closure directions and definition ordering to an independent model.
    - Tag: `// Feature: ecs-production-readiness, Property 1`
    - _Requirements: 1.1, 1.3, 1.4, 1.5_

  - [ ] 1.6 Property test: Property 2 — invalid or unforwardable selection is mutation-free
    - Implement a `proptest` with at least 128 cases over empty, unknown, cyclic, malformed, and unsupported
      scopes using state/provider spies; prove refusal occurs before access and never widens to `All`.
    - Tag: `// Feature: ecs-production-readiness, Property 2`
    - _Requirements: 1.2, 1.6, 1.8_

  - [ ] 1.7 Property test: Property 3 — scoped Delta isolation
    - Implement a `proptest` with at least 128 cases over generated compositions, states, scopes, and
      Deltas; prove only effective modules change, unrelated state remains, infra/deploy aliases agree, and
      the definition is not rewritten.
    - Tag: `// Feature: ecs-production-readiness, Property 3`
    - _Requirements: 1.7, 1.8, 1.9_

- [ ] 2. Checkpoint: operation identity and scoped selection are green
  - Run nightly formatting, targeted checks/clippy, and focused envelope, selection, platform-spy, and ECS
    graph tests with `--locked`; confirm the tree is not dirtied by validation.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 5.1, 5.2_

- [ ] 3. Implement one authoritative private operator-endpoint inventory
  - [ ] 3.1 Define the typed ECS endpoint inventory
    - Add the six canonical service, remote-port, capacity-provider, and `SsmAccess` entries under
      `platforms/ecs/src/operator_endpoints.rs`.
    - Extend the generic `PortMapping` access representation so published, SSM direct-instance, and SSM
      remote-host endpoints are serializable without ECS-specific hard-coded maps in `tkr`.
    - _Requirements: 2.1, 2.2, 2.3_

  - [ ] 3.2 Project generic mappings and SSM requests from the same entry
    - Implement ECS `Ops::port_mappings` by projecting the canonical inventory.
    - Refactor `tkr port-forward` to resolve remote port, capacity-provider target, SSM document, and
      Service Connect host from that projection; apply local-port overrides only after resolution.
    - Remove the duplicate ECS service/port match table.
    - _Requirements: 2.2, 2.3, 2.4_

  - [ ] 3.3 Implement private target resolution and shared operator errors
    - Resolve active instances in the endpoint's dedicated capacity provider and build direct-instance or
      remote-host SSM requests as declared by the inventory.
    - Return one actionable unknown-service error and supported-name list from mapping and forwarding; keep
      all paths private and require no public address, listener, subnet, or workstation CIDR rule.
    - _Requirements: 2.5, 2.6, 2.7, 2.8_

  - [ ] 3.4 Property test: Property 4 — endpoint projections cannot disagree
    - Implement a `proptest` with at least 128 cases over inventory entries, valid local-port overrides, and
      unsupported names, comparing generic mapping and SSM request projections field by field.
    - Tag: `// Feature: ecs-production-readiness, Property 4`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

  - [ ] 3.5 Property test: Property 5 — operator access remains private
    - Implement a `proptest` with at least 128 cases over endpoint entries and generated private instance
      observations; reject any generated plan containing a public address/listener/subnet requirement or
      workstation CIDR rule.
    - Tag: `// Feature: ecs-production-readiness, Property 5`
    - _Requirements: 2.8_

- [ ] 4. Isolate ECS capacity-provider hosts with typed network resources
  - [ ] 4.1 Separate security-group shells from typed rule resources
    - Extend `crates/tokeira-aws/src/resources/security_group.rs` with `SecurityPeer`, `RuleDirection`, and
      `SecurityGroupRuleResource` using canonical rule identity and explicit dependencies.
    - Revoke AWS default allow-all egress when creating a managed group; make rule resources own provider
      authorization/revocation and idempotent describe/delete behavior.
    - _Requirements: 3.3, 3.4_

  - [ ] 4.2 Build the declarative ECS network policy
    - Create the eight distinct host-group logical identities and the explicit group, prefix-list, and DNS
      resolver edges for ECS/ECR/SSM/AWS control traffic, ALB targets, service communication, and operator
      forwarding.
    - Model S3 access with the gateway prefix-list route edge used for ECR layers and state objects; reject
      a VPC-wide CIDR when a group or prefix-list peer can express the edge.
    - _Requirements: 3.1, 3.3, 3.4, 3.5_

  - [ ] 4.3 Attach one host group to each capacity-provider launch template
    - Update ECS networking/cluster module construction so each launch template depends on exactly its
      plane's host group instead of `sg-runtime`.
    - Preserve task-ENI and ALB groups as separate identities, and expose capacity provider, launch
      template, host group, and allowed edges in human/JSON plans.
    - _Requirements: 3.1, 3.2, 3.8_

  - [ ] 4.4 Implement safe host-group migration semantics
    - Detect host-group assignment changes as replacement/rolling effects before apply.
    - Preserve configured healthy capacity during replacement and retain an old group until no dependent
      instance remains; use synchronization/provider waiters rather than fixed sleeps.
    - _Requirements: 3.6, 3.7_

  - [ ] 4.5 Add fixed network-policy and migration tests
    - Cover the exact eight plane/group assignments, default-deny group creation, canonical rule identity,
      S3 gateway prefix-list handling, forbidden broad CIDRs, audit rendering, and old-group retention.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

  - [ ] 4.6 Property test: Property 6 — capacity planes and host groups are bijective
    - Implement a `proptest` with at least 128 cases over valid ECS configurations, proving exactly eight
      distinct plane/group identities, the matching single launch-template dependency, and equivalent
      audit output.
    - Tag: `// Feature: ecs-production-readiness, Property 6`
    - _Requirements: 3.1, 3.2, 3.8_

  - [ ] 4.7 Property test: Property 7 — network rules equal the declarative policy
    - Implement a `proptest` with at least 128 cases over endpoint/service configurations, comparing
      realized rule identities to a reference edge set and proving default-deny, constrained SSM edges,
      and peer specificity.
    - Tag: `// Feature: ecs-production-readiness, Property 7`
    - _Requirements: 3.3, 3.4, 3.5_

  - [ ] 4.8 Property test: Property 8 — host-group migration preserves capacity and dependencies
    - Implement a `proptest` with at least 128 cases over generated old/new assignments and valid capacity
      bounds, comparing the migration state machine to a reference model for reported effects, minimum
      healthy capacity, and dependency-safe group deletion.
    - Tag: `// Feature: ecs-production-readiness, Property 8`
    - _Requirements: 3.6, 3.7_

- [ ] 5. Checkpoint: operator access and network isolation are green
  - Run nightly formatting, targeted checks/clippy, and focused endpoint, SSM request, AWS security-group,
    ECS networking, cluster, and migration tests with `--locked`.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

- [ ] 6. Admit preexisting DSQL roles from live IAM evidence
  - [ ] 6.1 Complete preexisting DSQL identity admission
    - Add `cluster_arn` to `DsqlConfig`/`definition.tkd`, require it in preexisting mode, and validate the
      cluster and both role ARNs against the resolved account/region before mutation.
    - Record the adopted cluster ARN so generated server configuration, IAM proof, and ownership evidence
      use the same exact identity.
    - _Requirements: 4.1, 4.4, 4.5_

  - [ ] 6.2 Define one runtime/admin DSQL permission profile
    - Build the exact trust principal and required action/resource pairs for DSQL, DynamoDB, SSM parameters,
      and SSM messages from resolved deployment resources.
    - Render managed role trust/permission documents and adopted-role simulation requests from the same
      pure `DsqlPermissionProfile` model.
    - _Requirements: 4.2, 4.4, 4.5, 4.11_

  - [ ] 6.3 Collect complete, immutable IAM admission evidence
    - Implement the async IAM reader for `GetRole`, decoded trust, every inline policy, every attached
      managed policy default version, and an optional permissions boundary.
    - Fail closed on unreadable/malformed/unsupported policy documents, versions, conditions, variables,
      or boundaries; retain only identifiers and digests, never raw sensitive policy/session data.
    - _Requirements: 4.1, 4.2, 4.3, 4.6, 4.8_

  - [ ] 6.4 Evaluate every exact permission pair and classify findings
    - Call `iam:SimulatePrincipalPolicy` for the exact required pairs and require `allowed` for each;
      explicit deny, implicit deny, missing context, or indeterminate results are admission failures.
    - Separate extra/broad-grant least-privilege findings from required-coverage failures and include
      actionable evidence in plan output.
    - _Requirements: 4.4, 4.5, 4.6, 4.7, 4.8, 4.9_

  - [ ] 6.5 Wire IAM admission before IaC mutation and preserve adopted roles
    - Run evidence collection and reduction before plan can enter a mutating engine path, insert immutable
      evidence into `ProvisionContext`, and consume it from adopted role resources.
    - Ensure successful apply/destroy never calls IAM mutation APIs for adopted roles while managed roles
      continue to use the shared profile.
    - _Requirements: 4.1, 4.10, 4.11_

  - [ ] 6.6 Add IAM reader, reducer, and no-mutation integration tests
    - Use fake IAM clients to cover trust variants, managed-policy versions, boundaries, explicit/implicit
      denial, indeterminate simulation, extra grants, exact evidence digests, and immutable adoption.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11_

  - [ ] 6.7 Property test: Property 9 — IAM admission equals the permission-pair reference model
    - Implement a `proptest` with at least 128 cases over generated trust documents, policy sets,
      boundaries, and simulation decisions; compare admission and evidence coverage to an independent
      effective-pair model.
    - Tag: `// Feature: ecs-production-readiness, Property 9`
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [ ] 6.8 Property test: Property 10 — managed and adopted DSQL roles share one profile
    - Implement a `proptest` with at least 128 cases over naming identities and exact cluster/resource
      ARNs, comparing managed-policy output with adopted-role simulation pairs and proving findings do not
      mutate adopted roles.
    - Tag: `// Feature: ecs-production-readiness, Property 10`
    - _Requirements: 4.9, 4.10, 4.11_

- [ ] 7. Make physical naming collision-safe and resource-local
  - [ ] 7.1 Extend operation/resource context with canonical naming identity
    - Carry project name, recorded deployment UUID, environment, STS account id, and region as immutable
      `NamingIdentity` in `ResourceContext`.
    - Implement only the domain-separated, length-prefixed SHA-256 digest and fixed 80-bit lowercase-base32
      suffix as shared context behavior; fail before candidate generation on missing UUID or account/region
      retargeting.
    - Do not add `naming.rs`, an AWS resource-kind enum, a provider-policy match, or a name registry.
    - _Requirements: 5.1, 5.2, 5.3, 5.10_

  - [ ] 7.2 Move provider-facing name derivation into each concrete resource
    - In every AWS resource used by the ECS definition that has a managed provider-facing name, implement
      its stable role token, configured/project alias handling, provider syntax, reserved forms, length
      budget, readable-prefix truncation, final candidate, and typed local error beside its constructor and
      provider calls.
    - Preserve the complete resource-local role and digest suffix, expose full digest/truncation evidence,
      and treat configured names as prefixes/aliases rather than exact managed names.
    - Cover, as applicable, ECS cluster/capacity resources, launch templates, ASGs, ALB/target groups, IAM
      roles/profiles, security groups, DynamoDB tables, S3 buckets, SSM paths, Cloud Map namespaces, private
      DNS zones, repositories, services, and other named resources emitted by the interpreted definition.
    - _Requirements: 5.3, 5.4, 5.6, 5.7, 5.11_

  - [ ] 7.3 Make collision and adoption decisions resource-local
    - Update each named resource's describe/preflight path to classify `Available`, `Owned`, `Adopted`, or
      `Collision` using its provider response and typed adoption contract.
    - Require recorded physical id plus naming identity for ownership, preserve exact names only for typed
      preexisting adoption, and remove create-time adoption based solely on an existing name or tags.
    - _Requirements: 5.5, 5.8, 5.9_

  - [ ] 7.4 Render resource-local naming evidence and errors
    - Include configured/project prefix, logical id, final candidate, digest inputs, full digest,
      truncation, and ownership/adoption verdict in human and JSON plans.
    - Map resource-local invalid-name, insufficient-budget, collision, missing-identity, and retarget errors
      to stable operator-actionable reason codes without centralizing provider policy.
    - _Requirements: 5.7, 5.8, 5.10, 5.11_

  - [ ] 7.5 Property test: Property 11 — resource-local physical naming is deterministic and bounded
    - Co-locate required `proptest` coverage with every affected resource module; run at least 128 cases per
      resource policy over generated prefixes and identities, using one digest reference model while each
      resource proves its own role, provider bounds, reserved forms, truncation, stability, and report
      evidence.
    - Tag every implementation: `// Feature: ecs-production-readiness, Property 11`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.6, 5.7, 5.11_

  - [ ] 7.6 Property test: Property 12 — resource-local admission follows ownership precedence
    - Co-locate required `proptest` coverage with every affected resource module; run at least 128 cases per
      admission policy over generated observations and recorded identities, proving typed adoption,
      recorded ownership, collision refusal, retarget refusal, and rejection of name/tag-only ownership.
    - Tag every implementation: `// Feature: ecs-production-readiness, Property 12`
    - _Requirements: 5.5, 5.8, 5.9, 5.10_

- [ ] 8. Checkpoint: IAM admission and resource-local naming are green
  - Run nightly formatting, targeted checks/clippy, and focused ECS IAM-admission plus all affected
    `tokeira-aws` resource tests with `--locked`; verify no central naming registry or name-only adoption
    remains in the ECS resource set.
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11_

- [ ] 9. Register ECS with the shared provisioner-bundle pipeline
  - [ ] 9.1 Add the ECS bundle target and remove the create refusal
    - Register package `tokeira-ecs-deployment`, source binary `tkp-ecs`, source-closure seed, and placed
      name `tkp` in the existing generic platform bundle-target registry.
    - Accept `tkr deployment create --platform ecs --bundle` and route it through generic snapshot,
      identity, resolve/build, admission, retention, and placement code without an ECS-specific format or
      trust branch.
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ] 9.2 Preserve admission-before-inception and atomic publication
    - Require checksum/identity/test/authority/revocation verification and retained rollback bytes before
      atomically placing the ECS provisioner and sidecar.
    - Enter implicit self-verifying inception only after successful placement/retention, write Day-0 state
      before non-state resources, and clean incomplete staging without publishing committed metadata.
    - _Requirements: 6.5, 6.6, 6.7, 6.8_

  - [ ] 9.3 Keep configuration revisions orthogonal to bundle identity
    - Ensure bundle identity includes frozen source/lock/toolchain/container/features/profile/target inputs
      but excludes `definition.tkd` content.
    - Reuse admitted cached bundles only after full reverification and advance configuration revision—not
      engine identity—when only the persisted definition changes.
    - _Requirements: 6.2, 6.3, 6.4, 6.9_

  - [ ] 9.4 Add ECS bundle failure-ordering and placement integration tests
    - Inject snapshot, cache, build, test, checksum, authority, revocation, retention, placement,
      self-verification, and implicit-inception failures; prove no forbidden Day-0/deployment/AWS side
      effect and no partial publication.
    - Cover trusted-CI production floors and the existing explicit local-development path.
    - _Requirements: 6.3, 6.4, 6.5, 6.6, 6.7, 6.8_

  - [ ] 9.5 Property test: Property 13 — bundle acquisition is an admission state machine
    - Extend the shared bundle suites with a `proptest` of at least 128 cases over generated identities,
      cache states, bytes, authority floors, evidence, and revocation sets; compare ECS outcomes and
      side-effect order to the shared reference state machine.
    - Tag: `// Feature: ecs-production-readiness, Property 13`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8_

  - [ ] 9.6 Property test: Property 14 — definition revisions are orthogonal to bundle identity
    - Implement a `proptest` with at least 128 cases over generated valid `definition.tkd` edit sequences,
      proving engine identity/provisioner bytes remain fixed while successful applies advance and retain
      configuration revisions.
    - Tag: `// Feature: ecs-production-readiness, Property 14`
    - _Requirements: 6.9_

- [ ] 10. Checkpoint: ECS bundle creation is green
  - Run nightly formatting, targeted checks/clippy, and focused snapshot, bundle-store, admission,
    placement, implicit-inception, envelope, and ECS create-flow tests with `--locked`.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9_

- [ ] 11. Implement the explicit live-AWS production-qualification target
  - [ ] 11.1 Define versioned qualification evidence and its total fold
    - Add redacted run identity, engine/definition/bundle digests, event, resource, health, permission,
      recovery, cleanup, and outcome models under `platforms/ecs/tests/support/evidence.rs`.
    - Make `Qualified` reachable only when every required scenario has valid matching evidence and cleanup
      reports no residual managed resource; reject missing, malformed, duplicate/conflicting, or
      secret-bearing evidence.
    - _Requirements: 7.2, 7.12, 7.13_

  - [ ] 11.2 Build the ignored, credentialed live-AWS harness boundary
    - Add `platforms/ecs/tests/live_aws.rs` plus reusable support for explicit account/region allow-list,
      unique run identity, admitted bundle, artifact directory, deadlines, cleanup policy, real `tkr`/`tkp`
      child execution, and observation-only AWS clients.
    - Compile the target in normal validation but require explicit feature/ignored invocation; default
      workspace tests must not need AWS credentials, Docker, or network access.
    - _Requirements: 7.1, 7.2, 7.14_

  - [ ] 11.3 Implement lifecycle, convergence, and selective-reconciliation scenarios
    - Automate full create/apply verification, configuration writeback checks, unchanged no-op plan/apply,
      selected prerequisite/dependent closure, ordering, and proof that unrelated modules remain unchanged.
    - Record account, region, deployment/run ids, identities/digests, command timeline, selections, resource
      ids, and outcomes for every scenario.
    - _Requirements: 7.1, 7.3, 7.4, 7.5, 7.13_

  - [ ] 11.4 Implement replacement, health, and DSQL-boundary scenarios
    - Inject task/container-instance loss and observe recovery without manual state edits.
    - Verify ALB target registration/health and use real assumed-role sessions to prove runtime/admin
      separation plus denial of unrelated cluster/table/parameter access.
    - Use AWS waiters/status APIs with deadlines and synchronization events, never fixed test sleeps.
    - _Requirements: 7.6, 7.7, 7.8_

  - [ ] 11.5 Implement interruption, rollback, destroy, and residual discovery scenarios
    - Terminate child operations only after a structured durable-phase event, rerun the same verb, and prove
      mutual exclusion, idempotent recovery, and converged state.
    - Verify both rollback provisioner checksums, B-only deletion, atomic A re-pin, and A forward
      reconciliation; then destroy in reverse order and query live APIs for residual run-owned resources.
    - Make cleanup failures non-qualified while retaining redacted remediation inventory.
    - _Requirements: 7.9, 7.10, 7.11, 7.12_

  - [ ] 11.6 Add fixed harness/evidence tests without live credentials
    - Test account mismatch refusal, deadline/cancellation handling, child-event synchronization, redaction,
      required-scenario completeness, residual-resource failure, artifact serialization, and cleanup-guard
      execution using fakes.
    - _Requirements: 7.2, 7.9, 7.12, 7.13, 7.14_

  - [ ] 11.7 Property test: Property 15 — qualification evidence cannot overstate success
    - Implement a `proptest` with at least 128 cases over generated event streams, identifiers, outcomes,
      residual inventories, and secret-shaped values; compare the fold to a reference
      completeness/redaction model.
    - Tag: `// Feature: ecs-production-readiness, Property 15`
    - _Requirements: 7.2, 7.12, 7.13_

- [ ] 12. Complete cross-cutting reports, errors, and hermetic integration coverage
  - [ ] 12.1 Wire stable reason codes and operator remediation
    - Map selection, endpoint, network-policy, resource-local naming, IAM admission, bundle, and
      qualification failures to the approved stable reason codes in human and JSON output.
    - Ensure every error says what happened, why it was refused, and the next corrective action without
      leaking credentials or raw sensitive policy data.
    - _Requirements: 1.2, 1.6, 2.7, 3.4, 4.8, 5.7, 5.8, 5.10, 6.5, 7.12, 7.13_

  - [ ] 12.2 Add end-to-end fake-boundary integration tests
    - Exercise `tkr` to fake `tkp` selection forwarding, ECS fake-provider scoped reconciliation, endpoint
      mapping/SSM requests, host-group replacement, IAM no-mutation adoption, resource-local collision
      refusal, bundle-before-inception ordering, and configuration revision retention.
    - Assert state/report schemas round-trip and builds/tests leave the working tree unchanged.
    - _Requirements: 1.5, 1.7, 1.8, 1.9, 2.2, 2.3, 3.6, 3.7, 4.10, 5.8, 5.9, 6.5, 6.9_

  - [ ] 12.3 Document public contracts and correctness-critical invariants in code
    - Add module/public-item documentation and focused WHY comments for scope closure, mutation gates,
      host-group migration, IAM fail-closed reduction, resource-local ownership, implicit inception, and
      qualification cleanup/redaction; do not narrate obvious control flow.
    - _Requirements: 1.8, 3.7, 4.7, 4.8, 5.9, 6.5, 7.12, 7.13_

- [ ] 13. Final checkpoint: workspace completion bar and spec traceability are green
  - Run `cargo +nightly fmt --all`.
  - Run `cargo lint --locked`.
  - Run `cargo check --workspace --locked`.
  - Run `cargo test --workspace --locked`.
  - Run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Verify all 15 tagged property suites execute with at least 128 cases, the ignored live-AWS target
    compiles but does not execute by default, no build dirties the tree, and no dependency/lockfile change
    was introduced unless separately approved.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10, 7.11, 7.12, 7.13, 7.14_

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["2"],
  "4": ["2"],
  "5": ["3", "4"],
  "6": ["5"],
  "7": ["1", "6"],
  "8": ["7"],
  "9": ["8"],
  "10": ["9"],
  "11": ["10"],
  "12": ["11"],
  "13": ["12"]
}
```

## Notes

- Property-test tasks are mandatory. None is optional or deferrable; each uses `proptest`, runs at least
  128 cases, and carries the exact feature/property tag shown in its task.
- `definition.tkd` remains the sole desired-state authority. Selection scopes an operation and never
  rewrites the definition or mutates replicas/resources out of band.
- Provisioner inception is implicit in deployment creation. Do not add or document a `tkp init` command.
- Shared naming code is limited to immutable identity and canonical digest generation. Provider-facing
  naming and collision/adoption behavior belong to each concrete AWS resource; do not create a naming
  registry or centralized resource-kind policy.
- The S3 gateway endpoint is retained for same-VPC private S3/ECR-layer access. It is represented by the
  S3 managed prefix-list edge rather than an interface-endpoint security group.
- The live-AWS harness is code delivered by this plan but remains an ignored, explicit, credentialed
  release target. The default workspace suite stays hermetic.
- No dependency addition/removal/upgrade or workspace lockfile movement is authorized by this plan; use
  existing workspace facilities and `--locked` validation.
