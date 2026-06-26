# Implementation Plan: Platform Provisioner Binary

## Overview

Implement the minimal foundational set: provenance stamping, the binding/mismatch gate, the integrity
manifest with checksum verification, the upgrade/migration boundary (registry may start empty), and
optional binary retention for S3 remote state. Heavier mechanisms (automated self-update, signing
infrastructure, the one-binary-vs-SDK multi-consumer decision) are out of scope.

## Tasks

- [ ] 1. Data models in `tokeira-state`
  - [ ] 1.1 Add `ProvenanceStamp`, `Target`, `BinaryArtifactDescriptor`, `IntegrityManifest` to the state
        manifest model; `ProvenanceStamp` distinguishes `Unknown` from a concrete version.
  - [ ] 1.2 Wire them into the existing manifest write/read path, preserving the distinct state-format
        `schema_version`. Property: provenance round-trips (Property 1).

- [ ] 2. Provenance stamping
  - [ ] 2.1 On every state-document write, stamp the running provisioner version (semver + git SHA from
        `tokeira-build-info`).
  - [ ] 2.2 On remote-state init, write the stamp before any resource create.

- [ ] 3. Binding and mismatch gate
  - [ ] 3.1 At the start of every mutating op (plan/apply/destroy/scale) in `tokeira-orchestrator` /
        `tokeira-iac`, compare running version to recorded provenance.
  - [ ] 3.2 On mismatch (or `Unknown`), surface it and require explicit acknowledgement or upgrade; never
        apply silently. Properties 2, 5.

- [ ] 4. Integrity manifest + verification
  - [ ] 4.1 Record version + per-target `sha256` (+ optional `retrieval_ref`) in the CAS-guarded manifest
        at stamp time.
  - [ ] 4.2 Verify a retrieved binary's checksum against the manifest before execution; abort on
        mismatch. Property 3.

- [ ] 5. Upgrade/migration boundary
  - [ ] 5.1 Add the `MigrationRegistry` and the version-transition entry point; run forward migration
        before mutation on upgrade.
  - [ ] 5.2 Refuse downgrade; refuse on a missing migration when the state format changed. Property 4.

- [ ] 6. S3 binary retention (optional path)
  - [ ] 6.1 In the S3 state store, optionally persist the binary blob keyed by `version`+`target`
        alongside state documents.
  - [ ] 6.2 Retrieve + checksum-verify before execution (reuses 4.2). Property 3 (5.3).

- [ ] 7. Measure and record the optimized binary size
  - [ ] 7.1 `cargo build --release` the provisioner; record the stripped size and the linked AWS SDK
        client set; note the trim/`opt-level`/UPX levers in the design's size section.

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1", "1.1", "1.2"] },
    { "wave": 2, "tasks": ["2", "2.1", "2.2", "4", "4.1"] },
    { "wave": 3, "tasks": ["3", "3.1", "3.2", "4.2"] },
    { "wave": 4, "tasks": ["5", "5.1", "5.2"] },
    { "wave": 5, "tasks": ["6", "6.1", "6.2"] },
    { "wave": 6, "tasks": ["7", "7.1"] }
  ]
}
```

## Notes

- Provenance is recorded at the manifest/snapshot level (coarse, one stamp per snapshot), not per
  `ResourceState`. This is the simplification the binary model enables: migrations run once per version
  transition at the upgrade boundary, not per-resource on every apply.
- The state-format `schema_version` (serialization shape) and the CAS generation (concurrency token) are
  distinct from the provisioner provenance version; do not conflate the three.
- Trust always flows from the CAS-guarded manifest checksum, never from a stored or fetched binary blob.
- Out of scope (follow-on specs): automated self-update (download, atomic swap, rollback); release
  signing and key management; the single-shared-binary vs provisioner-as-SDK decision for multi-consumer
  reuse (e.g. Odori); per-target build/distribution matrix beyond recording checksums.
