# Requirements Document

## Introduction

The ECS platform has an authoritative interpreted `definition.tkd`, a platform-bound `tkp-ecs`, remote
state, a complete module/resource topology, private networking, DSQL integration, capacity providers,
services, and day-2 operator commands. The base topology is specified by
[`ecs-deployment`](../ecs-deployment/requirements.md), and the already-corrected rollout, execution-role,
environment, resource-arithmetic, and endpoint-inventory behaviours are specified by
[`ecs-deployment-correctness`](../ecs-deployment-correctness/bugfix.md).

This feature closes the remaining gaps between that functional foundation and a production-qualified ECS
platform. It adds selective module reconciliation, one authoritative private-operator endpoint inventory,
capacity-provider host isolation, live admission of preexisting DSQL IAM roles, collision-safe AWS
physical names, hermetic/versioned ECS provisioner bundles, and a repeatable live-AWS qualification gate.

This spec does not replace the existing definition or provisioner contracts. The persisted
`definition.tkd` remains the sole desired-state authority under
[`platform-config-dsl`](../platform-config-dsl/requirements.md), while engine identity, configuration
revisions, bundle integrity, rollback, and operation locking remain owned by
[`platform-provisioner-binary`](../platform-provisioner-binary/requirements.md).

### Authority for correctness

No Temporal public API behaviour is introduced here, so Temporal server ground truth is not applicable.
The authorities for this feature are, in order:

1. The current ECS deployment contract and interpreted definition in `platforms/ecs/definition.tkd`.
2. The shared IaC/provisioner contracts named above, including fail-closed deletion, dependency-ordered
   reconciliation, bundle admission, and resumable rollback.
3. Live AWS observations captured by the qualification scenarios in this document.

## Current Implementation Evidence

| Gap | Current evidence | Required closure |
|---|---|---|
| Module selection is discarded | `platforms/ecs/src/provisioner.rs` composes, plans, applies, and destroys with `ModuleSelection::All` | Forward an operator selection end to end and compute only its specified dependency closure |
| Generic endpoint inventory is absent | `platforms/ecs/src/lib.rs` returns `"ECS port mappings are not implemented yet"`; `apps/tkr/src/commands/port_forward.rs` carries a separate hard-coded ECS list | Make one platform-owned inventory drive `Ops::port_mappings` and SSM forwarding |
| EC2 hosts share one security group | Every `LaunchTemplateResource` in `platforms/ecs/src/modules/cluster.rs` depends on `ResourceId("sg-runtime")` | Give each capacity-provider isolation plane a distinct host security-group identity and least-privilege rules |
| Preexisting DSQL roles are trusted by ARN alone | `DsqlIamRoleResource::preexisting` in `platforms/ecs/src/modules/dsql.rs` records the ARN without querying trust, attached policies, inline policies, or permissions boundaries | Admit a role only after live IAM evidence proves the required trust and permission coverage |
| Explicit and derived names can collide | `definition.tkd` defaults `cluster.name` to `tokeira` and `alb.name` to `tokeira-internal`; several resource names use only `project_name` | Derive or validate names against the deployment's account/region identity and provider limits |
| ECS bundle creation is refused | `apps/tkr/src/commands/deployment.rs` rejects `--bundle` for ECS | Route ECS through the shared snapshot, build, admission, retention, and placement pipeline |
| No live production gate exists | `docs/platforms/ecs/README.md` records live-AWS lifecycle, recovery, health, and permission-boundary work as outstanding | Produce repeatable qualification evidence and prove cleanup against real AWS resources |

## Glossary

- **Requested selection** — the optional module-name set explicitly supplied for one lifecycle command.
  Absence means the full definition; a present but empty set is invalid.
- **Effective selection** — the requested selection plus the exact dependency closure required by the
  operation policy in this document.
- **Prerequisite closure** — each requested module and all modules it transitively depends on.
- **Dependent closure** — each requested module and all modules that transitively depend on it.
- **Operator endpoint** — a private service/port pair an operator may reach through the supported SSM
  forwarding path; it does not imply public ingress.
- **Host security group** — the group attached to EC2 instances launched for one ECS capacity provider;
  it is distinct from task-ENI groups and ALB groups.
- **Preexisting DSQL role** — an operator-supplied IAM role adopted by ARN rather than created by Tokeira.
- **IAM coverage proof** — evaluation of the role trust policy, inline policies, attached managed-policy
  default versions, explicit denies, and permissions boundary against the required action/resource set.
- **Deployment identity** — the stable combination of Tokeira deployment id/name, environment, AWS
  account id, and AWS region used to scope physical names.
- **Physical-name candidate** — the final provider-facing name after explicit-name validation or
  deterministic derivation, including any bounded hash suffix.
- **Provisioner bundle** — the shared `ProvisionerBundle` containing the ECS `tkp` artifact, integrity
  manifest, engine identity, build authority, source-snapshot evidence, and test evidence.
- **Qualification run** — an explicitly invoked, credentialed live-AWS campaign that records auditable
  evidence and cleans up its managed resources; it is not part of the default hermetic workspace suite.

## Target State

- Every ECS lifecycle command either reconciles the whole interpreted module graph or an explicit,
  visible, dependency-correct subset; no layer silently widens a selection to `All`.
- `definition.tkd` remains the sole desired-state authority. Selection scopes an operation but never
  rewrites replica intent, module structure, or configuration out of band.
- One ECS-owned endpoint inventory describes generic port mappings and the concrete SSM transport used by
  `tkr port-forward`.
- The eight capacity-provider planes have distinct host security groups, and every permitted network edge
  is justified by workload or operator-access policy.
- Preexisting DSQL roles are admitted from live IAM evidence, not operator assertion, and managed roles
  satisfy the same permission profile.
- AWS physical names are deterministic, provider-valid, account/region-scoped where applicable, and
  fail closed on unowned collisions.
- `tkr deployment create --platform ecs --bundle` obtains and admits the ECS provisioner through the
  existing hermetic bundle pipeline before deployment or AWS mutation.
- A versioned live-AWS campaign proves lifecycle, no-op convergence, update, replacement, recovery,
  rollback, ALB health, DSQL permission boundaries, and complete destruction.

## Contract Policy Tables

### Module-selection policy

The module graph comes from the persisted `definition.tkd`:

`remote-state`; `images`; `networking -> remote-state`; `dsql -> networking`; `cluster -> dsql`;
`observability -> cluster, images`; `services -> observability, images`.

| Operation | Selector absent | Named selector | Effective selection | Order |
|---|---|---|---|---|
| `infra plan` / `infra apply` | All modules | One or more definition module names | Prerequisite closure | Forward topological |
| `deploy plan` / `deploy apply` | All modules | One or more definition module names | Same prerequisite closure as the ECS infrastructure graph | Forward topological |
| `infra destroy` | All modules | One or more definition module names | Dependent closure; prerequisites not in that closure are retained | Reverse topological |
| Upgrade/rollback delete-only pass | Not module-selected | Not module-selected | Exact resource-id set recorded by the shared rollback protocol | Shared fail-closed ID-set ordering |

Unknown names, a present empty selector, a dependency cycle, or a selection that cannot be represented by
the interpreted graph is an admission error. Derived closure is the only permitted widening.

### Operator-access inventory

All entries use TCP and remain private. A local-port override changes only the workstation listener.

| Service | Remote port | SSM transport | Target |
|---|---:|---|---|
| `grafana` | 3000 | `AWS-StartPortForwardingSession` | EC2 instance in `cp-grafana` |
| `mimir` | 9009 | `AWS-StartPortForwardingSession` | EC2 instance in `cp-mimir` |
| `loki` | 3100 | `AWS-StartPortForwardingSession` | EC2 instance in `cp-loki` |
| `edge-api` | 7233 | `AWS-StartPortForwardingSessionToRemoteHost` | `edge-api.<service-connect-namespace>` via an instance in `cp-edge-api` |
| `edge-poll` | 7234 | `AWS-StartPortForwardingSessionToRemoteHost` | `edge-poll.<service-connect-namespace>` via an instance in `cp-edge-poll` |
| `controller` | 7240 | `AWS-StartPortForwardingSessionToRemoteHost` | `controller.<service-connect-namespace>` via an instance in `cp-control` |

### Capacity-provider host-isolation policy

| Plane | Capacity provider | Required host-group identity |
|---|---|---|
| Public API edge | `cp-edge-api` | `host-edge-api` |
| Poll edge | `cp-edge-poll` | `host-edge-poll` |
| Runtime | `cp-runtime` | `host-runtime` |
| Projection | `cp-projection` | `host-projection` |
| Control services | `cp-control` | `host-control` |
| Mimir | `cp-mimir` | `host-mimir` |
| Loki | `cp-loki` | `host-loki` |
| Grafana | `cp-grafana` | `host-grafana` |

Host groups default to no inbound rules. Required ECS/SSM/AWS control-plane access is outbound, and any
host-bound or task-to-task data path is represented by an explicit source-group, destination-group,
protocol, and port rule. A VPC-wide CIDR rule is not equivalent to a declared group-to-group edge.

### DSQL role admission policy

Both roles require an `Allow` trust statement for principal `ecs-tasks.amazonaws.com` and action
`sts:AssumeRole`, with no condition that prevents the ECS task assumption path.

| Profile | Required actions | Required resources |
|---|---|---|
| Runtime | `dsql:DbConnect` | Exact configured/adopted DSQL cluster ARN |
| Admin | `dsql:DbConnectAdmin` | Exact configured/adopted DSQL cluster ARN |
| Both | `dynamodb:DescribeTable`, `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:UpdateItem` | Exact rate-limiter and connection-lease table ARNs in the resolved account/region |
| Both | `ssm:GetParameter` | Exact server-config parameter ARN and the deployment's Alloy sidecar parameter subtree |
| Both | `ssmmessages:CreateControlChannel`, `ssmmessages:CreateDataChannel`, `ssmmessages:OpenControlChannel`, `ssmmessages:OpenDataChannel` | `*`, as required by the service API |

Coverage may be supplied by the union of inline and attached managed policies, but an explicit deny or a
permissions boundary that excludes a required action/resource invalidates the proof. Additional grants are
reported as least-privilege findings; they do not compensate for missing required coverage.

### AWS physical-name policy

Every Tokeira-managed physical name follows the conceptual form
`<sanitized-project-name>-<resource-role>-<short-stable-digest>`, normalized to the target provider's
syntax. The digest covers the canonical tuple `(deployment UUID, environment, AWS account id, AWS
region, logical resource id)`. It is the uniqueness component; `project_name` and the resource role are
the human-readable components. Operator-configured names are readable prefixes/aliases, not exact managed
physical names. An exact provider-facing name is accepted only through a supported preexisting-resource
adoption contract.

| Name class | Examples | Policy |
|---|---|---|
| Operator-configured managed prefix | ECS cluster, ALB | Sanitize the configured/project prefix, add the resource role and deployment-identity digest, validate provider constraints, and show configured plus final names in plan |
| Operator-configured managed DNS prefix | Service Connect namespace, private DNS zone | Normalize the configured prefix as DNS labels, add a bounded deployment-identity digest, and reject an invalid or conflicting final namespace/zone |
| Fully derived account/region resources | IAM roles/profiles, launch templates, ASGs, capacity providers, DynamoDB tables, SSM paths | Use `project_name`, resource role, and the deployment-identity digest; preserve the digest when truncation is needed |
| Globally named resources | S3 remote-state bucket | Use the same scheme with account and region in the digest input; validate global availability before mutation |
| Adopted preexisting resources | Operator-supplied cluster, role, endpoint, namespace, or zone identity | Preserve the exact configured physical name only through an explicit adoption contract and verify ownership/existence without renaming |
| Provider-assigned ids | VPC ids, endpoint ids, task-definition revisions | Record returned ids in state; never guess or reuse them as ownership evidence |

### ECS provisioner-bundle policy

| Stage | Required inputs/evidence | Failure side effects |
|---|---|---|
| Snapshot | Immutable ECS provisioner source closure, lock closure, source tree oid, audit commit | No deployment state or AWS mutation |
| Identity | Toolchain, digest-pinned build container, feature set, profile, target, source/lock digests | No deployment state or AWS mutation |
| Build/resolve | Requested build authority and passing test evidence; verified CAS hit or one hermetic build | Temporary build artifacts only |
| Admission | Artifact checksum, identity match, sufficient authority, non-revocation | No placement, Day-0 stamp, or AWS mutation |
| Retention/placement | Admitted bundle retained for the deployment; platform source binary placed as `<deployment>/tkp`; manifest sidecar retained | Atomic local publication or cleanup of an incomplete staging result |
| Inception | Self-verifying `tkp` writes the Day-0 binding and integrity manifest | Begins only after all preceding stages succeed |

### Live-AWS qualification matrix

| Scenario | Required observation |
|---|---|
| Inception | Versioned ECS bundle admitted; Day-0 state stamped before non-state resources |
| Full create/apply | All definition modules converge in dependency order and writeback produces usable runtime config |
| No-op | A second unmodified plan/apply reports no material infrastructure change |
| Selective reconciliation | Requested/effective module sets match the module-selection policy and unrelated modules remain unchanged |
| Config update | A definition-only revision changes the intended resources without changing engine identity |
| Task replacement | Forced task/instance loss is replaced; desired count/daemon coverage recovers without manual state edits |
| ALB health | Expected targets register and become healthy on the configured health-check path before success is recorded |
| DSQL boundaries | Runtime credentials connect but cannot perform admin-only access; admin credentials perform the required admin path; unrelated cluster/table/parameter access is denied |
| Interrupted operation | Re-running an interrupted apply/upgrade/rollback resumes from durable state without a second writer or silent widening |
| Engine rollback | Both binaries verify; B removes only B-created resources; A re-pins and forward-reconciles its retained definition |
| Destroy | Reverse dependency teardown completes and an inventory query finds no live resource still owned by the qualification deployment |

Each run records the account, region, deployment id, run id, engine identity, definition digest, bundle
manifest digest, command/outcome timeline, requested/effective selection, relevant AWS resource ids and
health observations, rollback/recovery markers, and cleanup inventory. Evidence never contains secrets,
session credentials, or policy-unredacted sensitive values.

## Requirements

### Requirement 1: Dependency-correct selective module reconciliation

**User Story:** As an operator, I want to scope an ECS lifecycle command to named modules while preserving
the definition's dependency invariants, so that I can reconcile or retire part of the platform without an
implicit full-environment operation.

#### Acceptance Criteria

1. WHEN no module selector is supplied, THE ECS provisioner SHALL use all modules interpreted from the persisted `definition.tkd`.
2. WHEN a module selector is supplied, THE ECS provisioner SHALL reject a present empty set or any name absent from the interpreted definition before provider access.
3. WHEN a named `plan` or `apply` is admitted, THE ECS provisioner SHALL compute the effective selection as exactly the requested modules plus their transitive prerequisites.
4. WHEN a named `destroy` is admitted, THE ECS provisioner SHALL compute the effective selection as exactly the requested modules plus their transitive dependents.
5. WHEN the effective selection is computed, THE command output and machine-readable result SHALL report both requested and effective module sets in deterministic definition order.
6. IF any layer cannot represent or forward the requested selection, THEN the command SHALL refuse rather than substitute `ModuleSelection::All`.
7. WHEN `deploy plan` or `deploy apply` is used for ECS, THE provisioner SHALL apply the same selection semantics to the definition's IaC-backed service modules as the corresponding infrastructure verb.
8. WHEN a selected operation executes, THE engine SHALL preserve the shared composition validation, forward/reverse topological ordering, fail-closed deletion, binding gate, and remote operation lock.
9. WHEN selection scopes an operation, THE persisted `definition.tkd` SHALL remain unchanged unless the operator separately edits it as a configuration revision.

### Requirement 2: One authoritative private operator-access inventory

**User Story:** As an operator, I want generic endpoint discovery and supported private tunnels to agree,
so that tooling can describe and reach ECS services without duplicated hard-coded port knowledge or public
exposure.

#### Acceptance Criteria

1. THE ECS platform SHALL expose the six service/port/transport entries in the operator-access policy as one canonical inventory.
2. WHEN `Ops::port_mappings(service, config)` is called for a supported ECS service, THEN it SHALL return the canonical TCP endpoint information instead of an unimplemented error.
3. WHEN `tkr port-forward` resolves an ECS service, THE command SHALL derive its remote port, capacity-provider target, and SSM document from the same canonical inventory used by `Ops::port_mappings`.
4. WHEN a local-port override is supplied, THE command SHALL change only the workstation listener while retaining the inventory's remote service port.
5. WHEN a service uses direct-instance transport, THE command SHALL select an active container instance from that service's dedicated capacity provider before starting the SSM session.
6. WHEN a service uses remote-host transport, THE command SHALL target its Service Connect DNS name through an active instance in the mapped capacity provider.
7. IF a service is absent from the canonical inventory, THEN generic mapping and forwarding SHALL return the same operator-actionable unknown-service error with the supported names.
8. WHILE an operator tunnel is active, THE platform SHALL require no public subnet, public task address, public listener, or inbound workstation CIDR rule.

### Requirement 3: Capacity-provider host security-group isolation

**User Story:** As a security operator, I want each ECS capacity-provider plane isolated at the EC2-host
boundary, so that compromise or a permissive rule in one workload plane does not silently expose every
other host pool.

#### Acceptance Criteria

1. WHEN the networking and cluster modules are realized, THE platform SHALL create and attach the eight distinct host-group identities listed in the host-isolation policy.
2. WHEN a launch template is realized, THE platform SHALL depend on the host group assigned to its own capacity-provider plane rather than the shared `sg-runtime` identity.
3. THE host groups SHALL default to no inbound rules and only the explicit egress or group-to-group edges required by ECS, SSM, AWS endpoints, DNS, health, and declared service communication.
4. IF an equivalent rule uses the whole VPC CIDR where a security-group reference can express the source, THEN admission SHALL reject it as broader than the host-isolation policy.
5. WHEN SSM operator forwarding is enabled, THE security-group graph SHALL permit the selected instance to reach only the inventory target/protocol/port required by that forwarding mode.
6. WHEN the security-group assignment of a launch template changes, THE reconciliation plan SHALL expose the replacement or rolling-instance effect before apply.
7. WHEN a host-group migration is applied, THE platform SHALL preserve configured healthy capacity while replacing instances and not detach the old group until its dependent instances are gone.
8. WHEN the infrastructure plan is rendered as JSON, THE output SHALL identify each capacity provider, launch template, host group, and allowed rule edge for audit.

### Requirement 4: Live IAM admission for preexisting DSQL roles

**User Story:** As an operator adopting existing DSQL infrastructure, I want Tokeira to verify the supplied
roles against the actual IAM control plane, so that an ARN that exists but cannot safely run the workload
fails before deployment.

#### Acceptance Criteria

1. WHEN `dsql.mode` is `preexisting`, THE ECS plan SHALL query each configured role ARN and reject a missing role, malformed ARN, or account mismatch before mutation.
2. WHEN a preexisting role is inspected, THE admission check SHALL evaluate its decoded assume-role policy for the required ECS task principal and `sts:AssumeRole` path.
3. WHEN permission coverage is evaluated, THE admission check SHALL include every inline policy and the default version of every attached managed policy.
4. WHEN the runtime role is admitted, THE evidence SHALL prove all runtime-profile actions against the exact resources in the DSQL role policy table.
5. WHEN the admin role is admitted, THE evidence SHALL prove all admin-profile actions against the exact resources in the DSQL role policy table.
6. WHERE a role has a permissions boundary, THE admission check SHALL prove that the boundary does not exclude any required action/resource pair.
7. IF an applicable policy or boundary explicitly denies a required action/resource pair, THEN the admission check SHALL fail even when another statement allows it.
8. IF effective coverage cannot be proved because a policy document, version, condition, variable, or boundary cannot be resolved safely, THEN admission SHALL fail closed with the unresolved item and remediation.
9. WHEN extra grants are found, THE plan SHALL report least-privilege findings separately from required-coverage failures without treating those grants as substitutes.
10. WHEN preexisting-role admission succeeds, THE apply and destroy paths SHALL leave the adopted roles and their policies unchanged.
11. WHEN Tokeira creates managed DSQL roles, THE generated trust and permission documents SHALL satisfy the same role profiles used to admit preexisting roles.

### Requirement 5: Deterministic and collision-safe AWS physical names

**User Story:** As an operator running multiple deployments, accounts, and regions, I want every
Tokeira-managed name derived from a readable project prefix and the stable deployment identity, so that a
default such as `tokeira` cannot bind to or collide with another environment's resources.

#### Acceptance Criteria

1. WHEN ECS configuration is admitted, THE provisioner SHALL resolve and record `project_name`, deployment UUID, environment, AWS account id, and AWS region as the deployment's naming identity.
2. WHEN uniqueness input is assembled, THE provisioner SHALL read the deployment UUID from recorded deployment identity rather than operator-editable definition data.
3. WHEN a managed physical name is produced, THE naming function SHALL combine sanitized `project_name`, the logical resource role, and a stable digest of `(deployment UUID, environment, AWS account id, AWS region, logical resource id)`.
4. WHEN an operator configures a cluster, ALB, namespace, or private-zone name, THE naming function SHALL treat that value as a readable prefix/alias and append the managed resource role and deployment-identity digest.
5. WHEN an exact provider-facing name is required, THE platform SHALL permit it only through a supported preexisting-resource adoption contract rather than for a Tokeira-managed resource.
6. WHEN provider length limits require truncation, THE naming function SHALL retain the complete collision-resistant suffix and produce the same result for the same naming identity.
7. WHEN any physical-name candidate is produced, THE provisioner SHALL validate the provider's character, prefix, suffix, reserved-name, and length constraints before provider mutation.
8. IF a candidate already exists but is absent from this deployment's recorded state and is not declared through a supported preexisting-resource contract, THEN the operation SHALL fail closed with the conflicting identity.
9. WHEN an existing candidate is treated as owned, THE provisioner SHALL corroborate ownership from recorded physical id plus deployment identity rather than name or tags alone.
10. IF ambient AWS account or region differs from the recorded naming identity, THEN the provisioner SHALL report a retarget and refuse silent renaming or cross-account reconciliation.
11. WHEN a plan is emitted, THE output SHALL show the configured/project prefix, logical resource id, final physical-name candidate, digest scope inputs, truncation decision, and ownership/adoption verdict in human and JSON views.

### Requirement 6: Hermetic and versioned ECS provisioner bundles

**User Story:** As a production operator, I want ECS creation to use the same verified provisioner-bundle
contract as other platforms, so that the exact manager married to a deployment is reproducible, retained,
and recoverable rather than an ambient local build.

#### Acceptance Criteria

1. WHEN `tkr deployment create --platform ecs --bundle` is requested, THE CLI SHALL accept the option and invoke the shared provisioner-bundle pipeline with the ECS platform source closure.
2. WHEN the bundle identity is computed, THE pipeline SHALL include the immutable source snapshot, lock closure, toolchain, digest-pinned build container, feature set, profile, and target while excluding `definition.tkd` configuration content.
3. WHEN an admissible cached ECS bundle exists, THE pipeline SHALL re-verify identity, bytes, authority, test evidence, and revocation status before reuse.
4. WHEN no admissible bundle exists, THE pipeline SHALL build and test `tkp-ecs` from the frozen snapshot in the hermetic build environment before publishing it to the authority-partitioned CAS.
5. IF snapshot, resolution, build, test, checksum, authority, revocation, retention, or self-verification fails, THEN ECS creation SHALL stop before Day-0 state, deployment metadata, provisioner placement, or AWS mutation becomes visible as committed state.
6. WHEN admission succeeds, THE create flow SHALL retain the bundle for rollback, atomically place its platform provisioner as `<deployment>/tkp`, and retain the corresponding manifest/build-evidence sidecar.
7. WHEN the placed ECS provisioner performs inception, THE binary SHALL verify itself against the admitted manifest before writing the Day-0 binding and integrity record.
8. WHEN a production-policy deployment requests a bundle, THE admission gate SHALL require the configured trusted build authority rather than silently accepting a local-developer artifact.
9. WHEN the operator edits only the persisted ECS `definition.tkd`, THE provisioner SHALL record a configuration revision without rebuilding or re-keying the admitted engine bundle.

### Requirement 7: Repeatable live-AWS production qualification

**User Story:** As a release owner, I want an auditable live-AWS qualification campaign for ECS, so that
production readiness is demonstrated by lifecycle, health, recovery, security-boundary, and cleanup
evidence rather than compilation alone.

#### Acceptance Criteria

1. WHEN an ECS engine identity is proposed for production readiness, THE release process SHALL run every scenario in the live-AWS qualification matrix with a versioned admitted bundle.
2. WHEN a qualification run begins, THE harness SHALL allocate a unique run identity and collision-safe deployment namespace in an explicitly selected non-production AWS account and region.
3. WHEN full create and apply complete, THE harness SHALL verify every expected module/resource, server-config writeback, ECS service, capacity-provider association, and remote-state record from live APIs.
4. WHEN the unchanged deployment is planned and applied again, THE harness SHALL verify no material provider mutation and no unexpected configuration-revision or engine-identity transition.
5. WHEN selective scenarios run, THE harness SHALL prove requested/effective closure, ordering, and non-mutation of unrelated modules from plan, state, and live-provider evidence.
6. WHEN task or container-instance failure is injected, THE harness SHALL verify replacement and recovery to desired replica or daemon coverage without manual state repair.
7. WHEN the edge API is deployed behind the ALB, THE harness SHALL verify expected target registration and healthy status on the configured health-check path before declaring the scenario successful.
8. WHEN DSQL role scenarios run, THE harness SHALL prove the runtime/admin capability separation and denial of unrelated cluster, table, and parameter access using real assumed-role sessions.
9. WHEN an operation is interrupted at a durable phase boundary, THE harness SHALL verify same-verb recovery, mutual exclusion, idempotent progress, and a converged final state.
10. WHEN an engine rollback scenario runs, THE harness SHALL verify both provisioner checksums, B-only deletion, atomic re-pin to A, and A's forward reconciliation of its retained definition.
11. WHEN qualification teardown runs, THE harness SHALL destroy in reverse dependency order and query live APIs for residual resources owned by the run.
12. IF teardown leaves a managed resource, THEN the qualification SHALL fail and preserve enough redacted evidence for operator cleanup without marking the engine identity production-ready.
13. WHEN a qualification run finishes, THE harness SHALL emit the complete redacted evidence record defined by this document in a versioned machine-readable format plus a concise human summary.
14. WHILE the default workspace test suite runs without live AWS credentials, THE qualification harness SHALL remain explicitly invoked and not make the hermetic suite depend on AWS or Docker.

## Non-Goals

- Replacing, compiling, or introducing a second desired-state source beside the persisted ECS
  `definition.tkd`.
- Restoring direct `tkr scale` mutations for ECS; replica and capacity-provider intent remains a reviewed
  definition edit followed by plan/apply.
- Reimplementing provenance, config-revision history, fail-closed deletion, operation locking, upgrade,
  rollback, source snapshotting, or bundle CAS mechanics already owned by `platform-provisioner-binary`.
- Changing rollback's delete-only phase from its exact resource-id set to module selection.
- Exposing Grafana, Mimir, Loki, control services, polls, or runtime APIs publicly; the supported operator
  path remains private SSM forwarding.
- Mutating preexisting IAM roles to make them pass admission; the operator changes external IAM, then
  reruns plan.
- Treating tags, matching names, or an operator assertion as sufficient proof of AWS resource ownership.
- Making live-AWS qualification part of `cargo test --workspace`; it is a credentialed release gate with
  separate invocation, evidence retention, budget, and cleanup controls.
- Claiming Temporal API behaviour or changing the compatibility target.
