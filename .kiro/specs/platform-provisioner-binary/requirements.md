# Requirements Document

## Introduction

Tokeira provisions infrastructure through an extensible IaC framework (`tokeira-iac`,
`tokeira-deploy-engine`, `tokeira-state`, `tokeira-orchestrator`, `tokeira-aws`) on which the platforms
are built. Today nothing binds a provisioned deployment to the exact code that produced it: a change to a
resource implementation can silently re-interpret existing state and drift live infrastructure on the
next apply.

This spec adopts a **complete platform-provisioner binary married to the deployment**. The provisioner is
a standalone, optimized binary that owns the IaC engine, the platforms, and the AWS resource
implementations. Each deployment's remote state records the provisioner version that may manage it; a
mismatch is gated, never silently applied; the bound binary's identity is recorded tamper-evidently so it
can be verified; and version changes happen only at a deliberate upgrade/migration boundary.

Scope here is the **minimal foundational set**: provenance, binding, integrity, the migration boundary,
and optional binary retention for S3 remote state. Heavier mechanisms (automated self-update with atomic
swap and rollback, release signing infrastructure and key management, and the single-shared-binary vs
provisioner-as-SDK multi-consumer decision) are explicit non-goals here and are deferred to follow-on
specs.

## Glossary

- **Provisioner** — the standalone optimized binary containing the IaC engine, the platforms, and the AWS
  resource implementations; the only artifact that mutates a deployment's infrastructure.
- **Deployment** — a provisioned set of resources tracked by remote state.
- **Provenance stamp** — the provisioner version (semver + git SHA) recorded in a state document.
- **Binding** — the association between a deployment's state and the provisioner version permitted to
  manage it.
- **Integrity manifest** — the CAS-guarded record of provisioner version plus per-target content
  checksums and an optional retrieval reference.
- **Migration boundary** — the deliberate version-transition point at which state is migrated forward.
- **Target** — an (operating system, architecture) pair a provisioner binary is built for.
- **Remote state** — the persisted deployment state (`tokeira-state`: CAS store, or S3 store).

## Requirements

### Requirement 1: Provisioner provenance in state

**User Story:** As an operator, I want every state document to record which provisioner version produced
it, so that I can always tell what is managing a deployment and detect code drift.

#### Acceptance Criteria

1. WHEN the provisioner writes or updates a state document, THEN it SHALL record its own version (semver +
   git SHA) in that document.
2. WHEN remote state is initialized for a new deployment, THEN the provisioner SHALL write the provenance
   stamp before any resource is created.
3. WHERE a state document predates provenance stamping, THE provisioner SHALL treat a missing stamp as an
   explicit unknown-provenance value rather than assuming it matches the running version.

### Requirement 2: Deployment binding and mismatch gate

**User Story:** As an operator, I want the provisioner to refuse to silently mutate a deployment created
by a different version, so that implementation changes never drift infrastructure without my
acknowledgement.

#### Acceptance Criteria

1. WHEN a mutating operation (plan, apply, destroy, scale) begins, THEN the provisioner SHALL compare its
   version to the deployment's recorded provenance.
2. IF the running version differs from the recorded version, THEN the provisioner SHALL surface the
   mismatch and SHALL NOT apply mutations without explicit operator acknowledgement or an upgrade.
3. WHEN the running version matches the recorded version, THEN the operation SHALL proceed under the
   normal plan-confirm-apply flow.

### Requirement 3: Integrity manifest

**User Story:** As an operator, I want the bound provisioner's identity and checksum recorded in
tamper-evident state, so that a binary retrieved to manage the deployment can be verified before
execution.

#### Acceptance Criteria

1. WHEN the provisioner stamps provenance, THEN it SHALL record its version and a content checksum per
   built target in the CAS-guarded manifest.
2. WHERE a retrieval reference for the binary is known, THE provisioner SHALL record it in the manifest.
3. WHEN a provisioner binary is obtained to manage a deployment, THEN its checksum SHALL be verified
   against the manifest before execution, AND a mismatch SHALL abort the operation.

### Requirement 4: Upgrade and migration boundary

**User Story:** As an operator, I want version changes to happen only at a deliberate upgrade step that
migrates state forward, so that upgrades are controlled rather than implicit.

#### Acceptance Criteria

1. WHEN an operator performs a provisioner upgrade for a deployment, THEN the new version SHALL run the
   registered migration from the recorded version forward before any mutation.
2. IF the running version is older than the deployment's recorded version, THEN the provisioner SHALL
   refuse to operate and SHALL surface the downgrade.
3. WHERE no migration is registered between two versions AND the state format is unchanged, THE
   provisioner SHALL treat the transition as identity; otherwise it SHALL refuse and surface the gap.

### Requirement 5: Binary retention for S3 remote state

**User Story:** As an operator using S3 remote state, I want the bound provisioner binary retained with
the deployment, so that I can manage or destroy it for its full lifetime without depending on an external
release channel.

#### Acceptance Criteria

1. WHERE remote state is S3, THE provisioner MAY persist the binary artifact for its target alongside the
   state documents.
2. WHEN a binary artifact is persisted to remote state, THEN its trust SHALL derive from the manifest
   checksum (Requirement 3), not from the stored blob.
3. WHEN a persisted binary is retrieved, THEN it SHALL be verified against the manifest checksum before
   execution.
