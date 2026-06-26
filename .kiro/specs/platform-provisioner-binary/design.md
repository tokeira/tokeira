# Design

## Overview

The provisioner is a single optimized binary that owns the IaC engine, the platforms, and the AWS
resource implementations. A deployment's remote state carries a **manifest-level provenance stamp** (one
per state snapshot — deliberately coarse, not per-resource) plus an **integrity manifest** binding the
deployment to a provisioner version and its per-target checksums. Mutating operations gate on a
version match; version changes occur only at a deliberate upgrade/migration boundary; and, for S3 remote
state, the bound binary may be retained alongside the state so the deployment carries its own manager.

The coarseness is intentional and is what the binary model buys: because code changes become deliberate,
gated upgrade events (not silent rebuilds), migrations run once per version transition rather than
per-resource on every apply. This avoids a per-`ResourceState` schema-version subsystem in the minimal
scope.

## Architecture

The provisioner is one binary spanning the existing engine-decoupled crates: `tokeira-iac` (engine),
`tokeira-deploy-engine`, `tokeira-orchestrator` (operation entry points), `tokeira-aws` (resources), the
platform crates, and `tokeira-state` (remote state). This spec adds a thin **provenance/binding layer** at
two seams and leaves the engine otherwise unchanged:

- **State seam (`tokeira-state`):** the manifest gains the provenance stamp + integrity manifest,
  inheriting CAS protection. The S3 store gains an optional binary-blob object keyed by version+target.
- **Operation seam (`tokeira-orchestrator` / `tokeira-iac`):** every mutating entry point first resolves
  the recorded provenance, runs the binding gate, and (on upgrade) the migration boundary, before the
  existing plan-confirm-apply flow proceeds.

No engine-core crate is touched; the layer sits entirely within the IaC framework and its state store.

## Components and Interfaces

- **Provenance reader/writer** (`tokeira-state`): stamps the running version into the manifest on write;
  reads it back as `ProvenanceStamp` (concrete or `Unknown`).
- **Binding gate** (`tokeira-orchestrator`): `check_binding(running, recorded)` yields
  `Match | Mismatch | Downgrade | Unknown`; mutating ops consult it and refuse to apply without
  acknowledgement or upgrade.
- **Integrity verifier**: computes/compares per-target `sha256` against the manifest descriptor; gates
  execution of any retrieved binary.
- **Migration registry + boundary** (`tokeira-iac`): resolves and runs forward migrations at the
  deliberate upgrade step; refuses downgrade and unbridged transitions.
- **S3 binary store** (`tokeira-state` S3 backend): optional persist/retrieve of the blob, verified via
  the integrity verifier.

## Data Models

- **ProvenanceStamp** — `{ version: String (semver), git_sha: String, recorded_at: String }`. Written
  into the state manifest. A missing stamp is represented as an explicit `Unknown` value, never coerced
  to a concrete version.
- **Target** — `{ os: String, arch: String }` (e.g. `linux/aarch64`, `macos/aarch64`).
- **BinaryArtifactDescriptor** — `{ version: String, target: Target, sha256: String,
  retrieval_ref: Option<String>, size_bytes: u64 }`.
- **IntegrityManifest** — `{ provisioner_version: String, artifacts: Vec<BinaryArtifactDescriptor> }`.
  Lives in the CAS-guarded state manifest, so its contents inherit optimistic-concurrency protection and
  cannot be silently rewritten.
- **MigrationRegistry** — an ordered set of `Migration { from: String, to: String, apply: fn(state) }`.
  May be empty initially; absence between two versions is handled per Requirement 4.3.

The provenance + integrity records attach to the existing `tokeira-state` manifest (alongside its
`schema_version`, which remains the *state-format* version — distinct from the *provisioner* version).
`ResourceState` is unchanged in this scope.

## Binary artifact: size and storage

**Estimated size.** A complete provisioner links ~14 AWS SDK service clients (`ec2`, `ecs`, `eks`, `iam`,
`s3`, `autoscaling`, `elasticloadbalancingv2`, `dynamodb`, `dsql`, `ecr`, `secretsmanager`,
`servicediscovery`, `ssm`, `sts`) plus tokio/rustls/hyper/serde. The AWS SDK generated code dominates,
with `aws-sdk-ec2` by far the largest. With the current release profile (`lto = "fat"`,
`codegen-units = 1`, `strip = "symbols"`, `panic = "abort"`) a realistic estimate is **~50–80 MB**.
Levers: trimming linked service clients to only those a build's platforms use is the largest reduction;
`opt-level = "z"` trades a little speed for size; optional UPX compression yields roughly **~20–35 MB**
on disk at a cold-start decompression cost (and a heavier scanner/trust profile for a cloud-privileged
binary, so it is not recommended by default). The exact figure SHALL be measured
(`cargo build --release` + `ls -la`) and recorded; the estimate is for planning only.

**Storage.** The integrity metadata (version + per-target `sha256` + optional `retrieval_ref`) is
**always** recorded in the CAS-guarded manifest — this is the trust anchor regardless of where the blob
lives. For **S3 remote state**, the binary blob MAY additionally be co-located with the state documents
(keyed by `version` + `target`), making the deployment self-contained and retainable without an external
release channel. Trust never flows from the stored blob: a retrieved binary (co-located or fetched via
`retrieval_ref`) is verified against the manifest `sha256` before execution. Heterogeneous operator
platforms are handled by keying artifacts per `Target`; the manifest records checksums for all built
targets even when only one blob is co-located.

## Correctness Properties

### Property 1: Provenance round-trips

*For any* state manifest the provisioner writes, reading it back SHALL yield a parseable `ProvenanceStamp`
carrying the writing provisioner's version and git SHA.

**Validates: Requirements 1.1, 1.2**

### Property 2: A version mismatch is never silently mutated

*For any* running version that differs from the deployment's recorded provenance, a mutating operation
SHALL NOT apply changes without explicit acknowledgement or an upgrade.

**Validates: Requirements 2.1, 2.2**

### Property 3: Checksum gate before execution

*For any* provisioner binary obtained to manage a deployment, IF its `sha256` does not equal the manifest
descriptor for its target, THEN it SHALL NOT be executed and the operation SHALL abort.

**Validates: Requirements 3.3, 5.3**

### Property 4: No downgrade

*For any* running version older than the deployment's recorded version, the provisioner SHALL refuse to
operate.

**Validates: Requirements 4.2**

### Property 5: Missing provenance is unknown, not a match

*For any* state lacking a provenance stamp, the comparison against a concrete running version SHALL
evaluate to unknown/mismatch, never to equal.

**Validates: Requirements 1.3, 2.1**

## Error Handling

| Condition | Handling |
|-----------|----------|
| Missing provenance stamp on existing state | Treat as `Unknown`; gate as a mismatch (Req 1.3, 2.2); operator acknowledges or upgrades. |
| Running version ≠ recorded version (newer) | Refuse mutation; instruct upgrade/migration (Req 2.2, 4.1). |
| Running version < recorded version | Refuse outright; surface downgrade (Req 4.2). |
| Retrieved binary checksum ≠ manifest | Abort before execution; do not run (Req 3.3, 5.3). |
| No migration registered, state format changed | Refuse; surface the migration gap (Req 4.3). |
| CAS conflict writing the manifest | Re-read and retry per existing `tokeira-state` CAS semantics; never force-overwrite. |

## Testing Strategy

- **Unit:** provenance stamp serialization round-trip; manifest descriptor encode/decode; version
  comparison including the `Unknown` case; checksum verify pass/fail.
- **Property (proptest):** Properties 1–5 above, tagged to their requirements. Property 2 and 5 exercise
  arbitrary version pairs (including absent provenance) and assert the gate decision.
- **Integration (no live AWS):** against the in-memory CAS store, drive write-stamp → reopen-with-changed
  version → assert gate; persist + retrieve a binary blob against a checksum and assert verify/abort.
- No tests require live AWS credentials or network.
