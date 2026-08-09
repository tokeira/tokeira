# spike-tuf-platform-definition

Prototype of the complete platform definition represented as a [TUF](https://theupdateframework.io)
repository, built and consumed with [`tough`](https://github.com/awslabs/tough) (0.24), stored in
S3 behind a custom `tough::Transport`, signed by keys that AWS KMS can hold via `tough-kms`.

Standalone by contract: excluded from the tokeira workspace, no tokeira crate dependencies,
mirrored product shapes with citations (`src/set.rs`). The spike exists to answer one question —
*can a signed TUF repository carry a multi-document definition set, with the product's
`sha256-set-v1` identity and served-part order intact, over S3, and hand verified bytes to the
product's existing seams?* Answer: **yes**, with the constraints recorded below.

## What it proves

- **The mapping is natural.** Targets are the set's sibling file names (`deployment.tkd`,
  `platform.tkd`, `observability.tkd` — the real Compose set is the fixture); the *set claim*
  (format, root name, served-part order, `sha256-set-v1` identity) rides as `custom` metadata on
  the root document's target, inside the signed targets role. The consumer refetches, recomputes
  the identity over the claimed order, and refuses any mismatch (`tests/roundtrip.rs`).
- **The verified set stands behind the product seams.** `FetchedSet` is exactly the
  `DefinitionSeed` shape (`apps/tkr/src/deployment_dir.rs:61` — root bytes + named part bytes),
  and `VerifiedPartSources` implements the mirrored `SourceResolver`
  (`crates/tokeira-platform/src/definition.rs:64`).
- **`S3Transport` is small and real.** `Transport` is one async method; the implementation is
  `GetObject` plus two load-bearing mappings: `NoSuchKey`/404 → `FileNotFound` (TUF's
  root-rotation walk probes `N+1.root.json` until absent), and a pass-through body stream —
  integrity stays `tough`'s job. The whole verification chain runs over the real `aws-sdk-s3`
  request path against an in-memory bucket in tests (`tests/s3.rs`); `tuf-spike upload` /
  `verify-s3` run the same code against real S3.
- **Consistent snapshots give the create-only S3 layout for free.** Every metadata version and
  every target is version-/digest-named; only `timestamp.json` (and the convenience `root.json`
  trust-anchor copy) mutate. The uploader writes immutables with `If-None-Match: *` and
  byte-verifies on collision — the platform-source-set spec's write policy, produced by TUF
  rather than hand-specified.
- **The attacks the spec's envelope cannot see are refused** (`tests/attacks.rs`): tampered
  metadata (signature), tampered target bytes (streaming hash), served-back older publication
  (rollback via the client datastore), frozen repository (timestamp expiry fails closed;
  `ExpirationEnforcement::Unsafe` is the deliberate break-glass).
- **Key rotation works in place.** Root v2 signed by the same root key rotates the online
  (targets/snapshot/timestamp) keys; a client pinned to root v1 walks `1.root.json → 2.root.json`
  and accepts the new chain. No client re-pinning, no bucket rewrite.
- **KMS slots in without touching the publisher.** A role key is a `KeySource`; `kms_role_key`
  builds `tough-kms`'s `KmsKeySource`. Constraint: **RSA only, `RSASSA_PSS_SHA_256`**
  (`KmsSigningAlgorithm` has one variant; `RSA_2048/3072/4096` key specs). KMS holds no Ed25519,
  and `tough-kms` does not use KMS ECC keys. Mixed repositories are fine: KMS-held online roles
  appear as `rsa` keys beside a file-held `ed25519` root.

## Usage

```bash
cargo run -q -- keygen  --keys /tmp/keys
cargo run -q -- publish --keys /tmp/keys --set fixtures/compose-set \
                        --root-doc deployment.tkd --out /tmp/repo
cargo run -q -- verify  --repo /tmp/repo
# live, with ambient AWS credentials:
cargo run -q -- upload    --repo /tmp/repo --bucket <b> --prefix deployments/compose
cargo run -q -- verify-s3 --trusted-root /tmp/repo/metadata/root.json \
                          --bucket <b> --prefix deployments/compose
# KMS-held online roles (root stays on the local file):
cargo run -q -- publish ... --kms-key-id arn:aws:kms:...:key/... [--profile <p>]
```

## Against the platform-source-set spec

The spec (`.kiro/specs/platform-source-set/requirements.md`) hand-rolls what TUF standardizes.
Mapping, for the rewrite to consume:

| Spec concept | TUF realization |
|---|---|
| Source Inventory entry (path, byte_length, content_digest) | `targets.json` entry (name, `length`, `hashes.sha256`) |
| Tree Digest / Revision Descriptor | signed `targets.json` + its version; the set claim carries `sha256-set-v1` |
| Release Evidence Envelope (scheme, key_id, signature, verification material) | TUF signature envelope on every role; keys + thresholds declared in `root.json` |
| Release-Authority Policy (trust roots, key states) | `root.json` role/key declarations; rotation by root-version walk |
| Blob store `blobs/sha256/<digest>`, create-only | consistent-snapshot targets `<sha256>.<name>`, create-only |
| Revision descriptor `revisions/<digest>.json`, create-only | `N.targets.json` / `N.snapshot.json`, create-only |
| *(no analog)* | `timestamp.json` freshness — rollback/freeze protection the envelope model lacks |

Stays product-owned, outside TUF: the `sha256-set-v1` semantics and served-order (carried as an
opaque signed claim), retention/`config_history`, the deployment envelope CAS, apply markers,
operation leases. TUF replaces the *evidence* machinery, not the state machine.

## Findings against tough 0.24 / tough-kms 0.16

- Programmatic root authoring needs no `tuftool`: build `schema::Root`, add keys
  (`Key::key_id()`), self-sign via `SignedRole::<Root>::new` with `KeyHolder::Root`.
- `RepositoryEditor` requires an on-disk root.json path and real target files (`Target::from_path`
  hashes files); `Target.custom` is fully writable before `add_target`.
- Signed files are written pretty-printed; signatures cover the *canonical* (olpc-cjson) bytes of
  the `signed` value — formatting-level tampering is irrelevant, structural tampering fails.
- Metadata republished at the same version is **not** byte-identical (fresh signatures and
  expirations), so version-named metadata is the immutable unit, not content-named metadata;
  target objects *are* content-named and republish byte-identically (`tests/roundtrip.rs`).
- Cross-load rollback protection exists only with a persistent `datastore` directory — a fresh
  consumer accepts any validly-signed unexpired publication. The deployment dir must own this
  directory (it is the TUF analog of `state/`).
- `jiff` timestamps: role expirations add `Span`s with absolute units only (hours, not days).
- `TargetName` re-exports at the crate root only; `tough::schema::TargetName` is private.
- `KeySource` is not clonable and APIs want owned `Box<dyn KeySource>` slices — the `Arc` shim
  (`SharedKeySource`) is the ergonomic fix, and is also how one KMS key serves several roles.
- Ed25519 signing keys are raw pkcs8 DER files (no PEM wrapper) for `LocalKeySource`;
  `aws-lc-rs` generates them, so tests never ship key material.
- `tough` claims TUF 1.0 minus repository consensus; delegated targets exist but are not needed
  here (one publisher per deployment definition).

## Operational shape (costs to accept)

- **Freshness has a price.** `timestamp.json` must be re-signed within its lifetime (spike
  default 14 days) by an online key — a scheduled re-sign job, or accept `Unsafe` expiration
  handling for definition repositories and lose freeze detection. This is the one genuinely new
  operational obligation TUF brings.
- **Publishers must serialize per repository.** Metadata versions are monotonic; concurrent
  publishers need the create-only collision refusal (proven here) plus the spec's operation
  lease. TUF does not solve multi-writer; it makes the conflict loud.
- **Trust-anchor distribution is the deployment-origin problem.** The pinned root.json bytes are
  what `PublishedProvisionerLocator.definition_seed_ref`-adjacent metadata must carry (or a
  digest of them); rotation after that is in-band.

## Adoption path

1. **Seam:** a TUF fetch drops into `DefinitionSeed` at `tkr` create
   (`begin_create` already stages root + parts atomically); `definition_seed_ref` in
   `PublishedProvisionerLocator` (`crates/tokeira-provisioner/src/catalog.rs:47`) is the
   documented home for the repository locator + trusted-root digest.
2. **Crate shape:** a `tokeira-definition-repo` (or `tokeira-state` neighbor) owning publish,
   fetch, `S3Transport`, and the datastore; frontends and `tkp` stay unaware — they keep seeing
   `SourceResolver`/`DefinitionSeed`.
3. **Spec:** the platform-source-set rewrite consumes the table above — TUF metadata replaces
   Tree Digest/Revision Descriptor/Release Evidence Envelope machinery; retention, envelope CAS,
   and apply markers stay as specified.
4. **KMS:** RSA keys for any KMS-held role; decide per environment whether root is offline-file
   or a separately-guarded KMS key. Threshold >1 on root is configuration, not new code.

## Pinning

- `tough 0.24.0`, `tough-kms 0.16.0` (2026-07-10 releases; RSA-only KMS signing).
- `aws-sdk-s3 1.x` — conditional writes (`If-None-Match: *`, 412 on collision) are GA S3
  behaviour and the mocked endpoint mirrors them.
- `aws-lc-rs 1.x` — the same crypto backend `tough` uses; keygen only.
