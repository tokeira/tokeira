# Proposal 005 — Provisioner bundles: build once per engine identity, bind at deployment create

- **Status:** **Draft** — for review. No decisions ratified; the open questions are collected in §Open decisions.
- **Refines:** Req 1 (provenance from Day 0), Req 4 (integrity verification), Req 5 (binary retention), Req 6.5 (the commands that write the stamp), Req 7 (surface separation); tasks 6 (`BinaryStore`) and 7 (binary size).
- **Builds on:** platform-config-dsl [Proposal 004 — `.tkd` syn interpreter](../../platform-config-dsl/proposals/004-tkd-syn-interpreter.md). The interpreted-definition work is the *enabler*: because a `.tkd` is data the bound `tkp` reads at runtime, `tkp` is no longer source-specialised per deployment, so two deployments on the same engine can share the exact same `tkp` bytes.
- **Requires requirements changes (proposed, not yet adopted):** Req 1.2, Req 4.x, Req 5.x, Req 6.5 wording (§Requirements changes).
- **Owner area:** `apps/tkr` (deployment-create orchestration), `apps/tkp` (independent identity + provenance `build.rs`), a new Dagger **provisioner-build** module (sibling of `tokeira-build`'s image pipeline), `crates/tokeira-provisioner` (`EngineIdentity`, `BuildAuthority`, manifest re-key), `tokeira-state` (`BinaryStore` re-key + per-deployment retention).

## Thesis

`tkr deployment create` establishes the deployment's **provisioner binding** — it does **not** compile a
unique binary per deployment. The canonical operation is:

> **resolve `EngineIdentity` → obtain (reuse | download | build) a verified `ProvisionerBundle` → retain
> and bind it to the deployment → initialise stamped state → execute the bound `tkp` to realise the
> definition.**

Two deployments that share an engine identity share one bundle:

```
deployment A ─┐
deployment B ─┼─  EngineIdentity X  →  one verified ProvisionerBundle
deployment C ─┘
```

Producing the bytes is a **Dagger** function that runs identically on a laptop or a Buildkite agent.
Deciding whether a given set of bytes is **admissible** for a given deployment is Tokeira's, and it is
governed by the existing integrity model, not by the cache. The most important correction this proposal
makes to earlier framing:

> The provisioner **binding** is created at deployment-create time — not a binary. `tkr` obtains the
> required `tkp` artifact through the canonical Dagger build (locally or through trusted CI) and retains
> it with the deployment. **Caching accelerates production; it never grants admission.**

## The reframe that must sit at the centre

The provisioner spec's root invariant is *verify the output bytes* against a CAS-guarded manifest before
executing (Property 3; `IntegrityManifest::verify_artifact`). Every caching mechanism below —
Dagger layer/volume/function caches, a shared cloud cache, an S3 bundle cache — is an *input→output*
memoisation. Memoisation is a performance tool sitting **below** the trust boundary; it must never
replace the byte check above it. Concretely, every cache **hit** re-verifies at bind:

1. **re-hash the retained bytes** and match the bundle's integrity manifest (the build result's checksum
   is a *claim*; the bytes are the truth);
2. **authority ≥ the deployment's policy** (a hit from a lower-trust tier is a *miss* for a higher-trust
   deployment — see §The two gates);
3. **not revoked** (§Caching, guardrail 4).

A cache hit is a performance event, never a trust decision.

## Verified facts this design rests on

- **Config is data; engine is code.** A `.tkd` change is a `config_revision` (Req 13, tasks 14.x); an
  engine change moves identity. This split is what lets the bundle key **exclude** the definition digest.
- **`tkr` and `tkp` are decoupled** — `tkr` is the operator cockpit on its own cadence; `tkp` is the
  deployment-married provisioner. `tkr` *invokes* `tkp` (the Wave 7 launcher); there is no shared-provenance
  binding by design. **The implementation does not yet honour this:** both `apps/tkr` and `apps/tkp` are
  `version.workspace = true` (both stamp the same `0.1.0`), and `tkp init` stamps via
  `ProvenanceStamp::current()` → `tokeira_build_info::{TOKEIRA_VERSION, SOURCE_TREE_HASH}`, where
  `SOURCE_TREE_HASH` is a **whole-workspace** digest (it hashes `tkr` and every other crate). So today
  `tkp`'s recorded provenance is literally the workspace's / `tkr`'s. Closing that is Phase 1.
- **The `.tkd` author vocabulary is engine identity** (design.md — the interpreter, builder vocabulary,
  and kind library are compiled into `tkp`). A definition is therefore only guaranteed to interpret under
  a `tkp` of the *same* identity (see §Definitions are portable within an identity).
- **Dagger is already the build boundary** in `tokeira-build` (`build_tokeirad_image` runs through
  `DefaultDaggerClient`). This proposal adds a *sibling* provisioner-build function, not a new boundary.
- **Retention already exists** as `tokeira_provisioner::BinaryStore`, keyed by `version + target`. This
  proposal widens the key to `EngineIdentity + target`.
- **The Dagger export path is a known, unsolved prerequisite.** `tkr image build` this cycle reached the
  Dagger `.export` step and hung to the 600s `reqwest` timeout in `dagger-client` (and separately panicked
  in `tkr`'s async context — the panic is patched; the export slowness is not). Making Dagger the canonical
  `tkp` build path inherits that failure mode until it is solved.

## Data model

Illustrative, not final — the field set and canonicalisation are open decisions.

```rust
/// What makes two tkp binaries interchangeable. A content-addressed digest over
/// tkp's DEPENDENCY CLOSURE — not the whole workspace (see Risks: over-invalidation).
struct EngineIdentity {
    source_hash: Sha256Digest,        // tkp's own source subtree, over the immutable snapshot (§Source snapshotting)
    cargo_lock_hash: Sha256Digest,    // the LOCKED versions reachable from apps/tkp, not the whole lock
    kind_library_version: Version,    // compose-syn / kind library
    builder_vocabulary_version: Version,
    rust_toolchain: ToolchainId,      // exact rustc + std
    build_container_digest: Sha256Digest, // the build image by digest, NOT a floating tag
    rustflags_and_link: Sha256Digest, // RUSTFLAGS, linker, target sysroot config
    feature_set: BTreeSet<Feature>,
    build_profile: BuildProfile,      // release | debug
    // Deliberately NOT: the deployment definition digest.
}

/// Orthogonal to Dev|Versioned. Who built the bytes and under what controls.
enum BuildAuthority {
    LocalDeveloper,
    TrustedCi { provider: CiProvider, build_id: String, source_commit: GitSha },
    Released { channel: String },
}

struct ProvisionerBundle {
    identity: EngineIdentity,
    authority: BuildAuthority,
    artifacts: Vec<BinaryArtifactDescriptor>, // per target: { target, sha256, size, retrieval_ref }
    tests: TestEvidence,                       // bound to the exact bundle bytes, not just the identity
    build_manifest: BuildManifest,             // provenance of the build itself
}
```

Envelope additions at Day-0 (extends `DeploymentStateEnvelope`): `definition_digest`, `definition_ref`,
and the **required authority** for this deployment (so later `upgrade`s re-check admission, not just
create). The bundle's `EngineIdentity` and integrity manifest are recorded as the binding.

## The two gates

`BuildMode` (Dev|Versioned) governs the *binding* gate — the running binary vs the recorded one
(Requirement 2, already built). `BuildAuthority` introduces a **second, distinct gate** at create/upgrade:
a **provenance-admission gate** — *does this artifact's build authority satisfy this deployment's policy?*

- `EngineIdentity` says the bytes are **interchangeable**. `BuildAuthority` says they are **trusted**.
- Because Rust builds are not bit-reproducible, a `LocalDeveloper` build and a `TrustedCi` build of the
  *same* `EngineIdentity` are **not the same bytes**. So a same-identity, lower-authority bundle must not
  satisfy a higher-authority deployment. The required authority is recorded in the envelope and re-checked
  on every subsequent identity-advancing operation.

Illustrative policy: a production deployment requires `authority = TrustedCi`, an immutable/reachable
source commit, a working-tree digest matching the request, tests passed, and the artifact manifest
retained. Initial ECS/local work is `authority = LocalDeveloper`, `mode = Dev`, and stays ergonomic.

## Definitions are portable within an identity, not across it

`definition_digest` is excluded from the bundle key — the same `tkp` interprets many definitions. **True
within one engine identity.** But because the author vocabulary *is* engine identity, a definition is only
guaranteed to interpret under a `tkp` of the same identity; crossing an identity boundary (an `upgrade`)
can require the definition to change. This is exactly Req 9's rollback caveat ("a config revision that does
not compile under the prior binary"). So: `definition ⟂ binary-identity` for **reuse**, but
`definition ⊑ vocabulary` for **validity**. Both hold; neither is weakened by the other.

## Source snapshotting (fidelity under concurrent work)

`EngineIdentity` is a function of source, so "source" must be an **immutable snapshot**, not a live tree.
This is not hypothetical: the development environment runs several AI agents, each in its own git worktree,
all mutating source concurrently. If `create` derived identity from a live working tree, the recorded
identity could disagree with the bytes built (an edit lands between "compute identity" and "run build"),
and two concurrent creates could see different content. For a system whose whole point is *the recorded
identity **is** the bytes*, that is corrupting.

**Invariant: snapshot → derive → build, atomic w.r.t. source.** `create`'s first act freezes the source
closure into an immutable, content-addressed reference; `EngineIdentity` is computed over the snapshot,
and the build consumes **the snapshot**, never the host's live tree (so even a hermetic container build
reads frozen bytes). The snapshot ref + digest are recorded in the build request and the envelope.

The snapshot's *nature* follows `BuildAuthority`:

- **`LocalDeveloper` (dirty tree — the common agent case).** Snapshot the working tree — staged *and*
  unstaged — without disturbing it. The precise primitive is a **temporary-index `write-tree`**: seed a
  throwaway index (`GIT_INDEX_FILE`, or an in-memory `gix` index) from the current one, stage the
  provisioner closure into it, and `write-tree` a content-addressed `tree` — leaving the working tree, the
  real index, and every ref (including `refs/stash`) untouched. The porcelain `git stash` is unusable here
  (it reverts the tree and mutates `refs/stash` — hostile to concurrent worktrees); `git stash create` is
  the nearest one-shot intuition but writes a dual-parent WIP commit and, crucially, **omits untracked
  files**. Untracked `.rs` in `tkp`'s closure are a real build input, so **`create` refuses by default and
  lists them** (decision 9), staging them only under an explicit `--include-untracked`.
- **`TrustedCi` (production).** No dirty snapshot: the admission gate requires an **immutable, reachable,
  protected commit**, and the build request pins that commit ("commit serially, build from protected
  commits," enforced).

Because each agent already works in its own worktree, a `create` from within a worktree snapshots *that*
worktree's state; concurrent creates from different worktrees freeze independently — **worktree isolation
plus the create-time snapshot together give per-agent fidelity.**

## Creation flow — a staged bootstrap

**Phase 1 — resolve the creation inputs.** `tkr` gathers: deployment name + stable id, state backend, the
platform's definition (selected by `--platform`) + the operator's `config()` value overrides + the
resulting digest, recorded identity values (region/account), the requested build authority (`--ci-build`
⇒ `TrustedCi`, else `LocalDeveloper`), and an immutable **source snapshot** (§Source snapshotting) that
feeds both identity and build. It **validates the `.tkd`**
with a compatible
local interpreter — *advisory only*; the bound `tkp` interprets it authoritatively (same pattern as
computing `EngineIdentity`: `tkr`'s pre-flight is advisory; the trusted build stamps the true identity,
and bind re-verifies it). Validate before triggering an expensive remote build.

**Phase 2 — obtain the bundle.** `tkr` resolves `EngineIdentity`, then:

```
resolve EngineIdentity
  → S3 CAS: verified bundle for (identity × required authority)?
      hit  → re-verify (bytes + authority + not-revoked) → reuse
      miss → produce via Dagger:
               local:  tkr → local Dagger Engine → ProvisionerBundle
               remote: tkr → Buildkite → same Dagger function → ProvisionerBundle
             → publish to S3 CAS
```

The Dagger function is the **sole** implementation of building, testing, hashing, and packaging `tkp`:

```
BuildProvisionerBundle(source, ProvisionerBuildRequest) -> ProvisionerBundle
  validate source closure → compute EngineIdentity → build tkp for requested targets
  → run tests → inspect/measure artifacts → checksum artifacts → produce build metadata → export bundle
```

**Phase 3 — bootstrap the state authority.** The remote-state resource may have to exist before a remote
envelope can be written (design.md Day-0 bootstrap):

1. create only the state-bootstrap resource, if necessary;
2. commit the stamped empty `DeploymentStateEnvelope` (provenance, integrity, `definition_digest`,
   `definition_ref`, required authority, `config_revision: 0`, empty heads);
3. retain/reference the verified bundle in the deployment's artifact prefix;
4. record the definition + digest;
5. execute the bound `tkp` for the first real plan/apply (`config_revision: 1`).

The first `tkp` execution may run from the freshly-obtained bundle; retention into the deployment prefix
is for *future* launches and self-contained rollback (Proposal 002), so it need not block the first apply.

## Build boundary — Dagger, local + CI parity; Buildkite for trust; GitHub thin

- **Dagger module** — the only place `tkp` is built/tested/hashed/packaged. Runs identically locally and
  on a Buildkite agent (Dagger's defining property).
- **Buildkite** — trusted remote execution, caching, artifact publication. `tkr` may trigger it directly
  for an operator-requested build; normal commit/release builds trigger from GitHub → Buildkite directly.
- **GitHub Actions** — *optional* thin dispatch/approval front-end (`workflow_dispatch` UX, environment
  approvals). It is **not** the build implementation and **not** the artifact channel. Making a GitHub
  Action a mandatory middleman for every `tkr deployment create` adds latency and a credential boundary
  without adding much.

`ProvisionerBuildRequest` carries `{ request_id, source_repository, source_commit, expected_source_hash,
targets, profile, definition_digest (correlation only), requested_by, created_at }`; the result carries the
`EngineIdentity`, per-target descriptors, test evidence, and the CI build id + source facts. `tkr` verifies
request id, source hash, commit, target presence, **re-hashed** artifact checksums, and authority-vs-policy
before binding.

## Caching

Four layers, all *below* the trust boundary:

1. **Layer cache (automatic)** — OCI/build-graph caching of `cargo fetch` / `cargo-chef` / `cargo build` /
   `cargo test` container steps.
2. **Persistent cache volumes** — mounted `cargo registry`, `cargo git db`, `target`, `sccache`, keyed by
   `toolchain + Cargo.lock + target triple + profile`. Survive across sessions.
3. **Function caching** — memoise `BuildProvisionerBundle()` / `GenerateKindLibrary()` / `RunTests()` when
   inputs are identical; `PublishProvisioner()` is `cache: never`.
4. **Engine/cloud cache** — Dagger's newer single-cache engine + optional Cloud Engines share layers,
   volumes, and function results without a bespoke distributed cache.

Above all four sits the **Tokeira-controlled S3 CAS** as the *authoritative* store of "does a verified
bundle exist for this identity × authority" (Dagger's caches are accelerators beneath it, not peers).

**Guardrails (the part that makes caching a provisioner safe):**

1. **Key = `EngineIdentity × BuildAuthority`; shared-cache *writes* are authority-gated.** If the shared
   cache is keyed by identity alone, the *first writer defines the canonical provisioner for everyone* with
   that identity — a laptop could populate bytes a production create later binds, and the integrity manifest
   would self-consistently pass. Partition the cache by authority tier and write-restrict the trusted tiers
   to CI. A prod deployment cache-*misses* on a laptop bundle of the same identity and correctly triggers a
   Buildkite build.
2. **Complete identity inputs.** A cache is only as correct as its key. `EngineIdentity` must include the
   **build-container image digest** and **`RUSTFLAGS`/link config** (omitted from the naïve list); a missed
   input serves the wrong provisioner on a hit.
3. **Hermetic for the canonical bundle; incremental for the dev loop.** A build that mounts an incremental
   `target/` is **not pure** w.r.t. its declared inputs, so the trusted, cache-by-identity bundle must be a
   **cold hermetic** build (pure → safely memoisable). Engine-developer iteration uses native `cargo`
   incremental, which is *faster* than a Dagger cold build and needs no bundle cache. Map to authority:
   `LocalDeveloper`/dev-candidate → native `cargo build -p tkp`; `Versioned*` → hermetic Dagger bundle.
   The two build worlds do not share a compile cache and don't need to: the native dev build is
   accelerated by the host's `kache` `RUSTC_WRAPPER` (per-worktree `target/`, one content-addressed
   store), while the hermetic Dagger build runs in a container with its own layer/volume/function caches —
   the hermetic boundary keeps them cleanly separate, and `EngineIdentity` (over source) is orthogonal to
   both (they only change *how fast* the same logical bytes are produced).
4. **Revocation.** "Cached forever" for a security artifact needs an escape hatch: a **deny-list of revoked
   identities / bundle digests checked at bind**, plus a forced cache-bust (bump `builder_version`, or an
   explicit revoke honoured by *both* S3 and Dagger's caches).
5. **Test evidence binds to bytes.** A memoised "tests passed" must be evidence about the exact bundle bytes,
   not merely the identity — or a cached pass could vouch for bytes served from a different entry.

## Artifact storage & retention

Global, content-addressed, Tokeira-controlled — **not** GitHub Actions artifacts:

```
s3://<artifact-bucket>/provisioners/<authority>/<engine-identity>/
    manifest.json
    aarch64-apple-darwin/tkp
    aarch64-unknown-linux-gnu/tkp
    x86_64-unknown-linux-gnu/tkp
```

Each deployment additionally retains a **logical copy** under its own state prefix
(`deployments/<id>/artifacts/provisioners/<identity>/<target>/tkp`) for self-contained rollback (Proposal
002); the global CAS avoids rebuilding and transfer. Copy-vs-reference-plus-checksum is an open decision;
copying is stronger for the self-contained goal.

**Target matrix (initial):** `aarch64-apple-darwin` (M-series operator), `x86_64-unknown-linux-gnu`
(CI/operator Linux), `aarch64-unknown-linux-gnu` (Graviton). `x86_64-apple-darwin` if retained. A fully
static `musl` target is attractive but the AWS SDK / native-TLS / native-dep story needs testing before it
is authoritative. The manifest records every successfully built target; a target can be added later without
changing identity **iff** built from the exact same source/build identity.

## Command UX (illustrative)

The **platform selects the definition** — there is no `--definition`. `--platform` names the platform
whose canonical `.tkd` the deployment interprets; the operator overrides *config values* within that
definition's `config()` surface (Proposal 004), never the structure. A **local build is the default**;
`--ci-build` opts into a trusted Buildkite build.

```
# Local build — the default; no build flag
tkr deployment create compose-dev  --platform compose

# Trusted remote build via Buildkite
tkr deployment create staging      --platform compose  --ci-build

# Dry-run: resolve identity, report cache hit/miss + the first-apply plan, create nothing
tkr deployment create staging      --platform compose  --ci-build  --plan
```

`--ci-build` selects `BuildAuthority::TrustedCi` (Buildkite); its absence is `LocalDeveloper`. Pinning a
create to a specific pre-built bundle (a controlled release rollout — orthogonal to *how* it is built) is
**under consideration** (§Open decisions); the common paths are `--platform` (local) and
`--platform … --ci-build` (trusted).

## Requirements changes (proposed)

- **Req 1.2 / Req 6.5 wording** — from "creation always *stamps*" to "creation always *binds an obtained,
  verified artifact* and stamps its provenance + integrity (Day-0)." The set of stamp-writing commands is
  unchanged (`create`, `upgrade`, `rollback`, the advisory dev re-stamp); `create` gains the resolve→obtain→
  verify→bind→retain responsibility.
- **Req 4** — verification is re-run against the **retained bytes** at bind, not only trusted from a build
  result; the manifest records the `EngineIdentity`, not just `version + target`.
- **Req 5** — retention is a global content-addressed CAS keyed by `EngineIdentity × BuildAuthority × target`,
  with a per-deployment logical copy; a revocation/deny mechanism is added.
- **New concept — `BuildAuthority`** — orthogonal to `BuildMode`; drives the provenance-admission gate. The
  three "versioned-ish" authorities may collapse into `Versioned` for the binding gate while retaining
  authority metadata for audit and admission.

## Risks and honest limits

- **Over-invalidation (make-or-break).** If `cargo_lock_hash`/`source_hash` are whole-workspace rather than
  `tkp`-closure, a `tkr`-only dep bump re-keys every `tkp` identity → mass rebuild + forced re-bind (an
  upgrade event per deployment). The closure scoping (Phase 1) is not optional.
- **Reproducibility is not assumed.** "Same identity → same bytes" means *cache-by-identity + reuse the
  first verified bundle's exact bytes*, **not** that any rebuild is bit-identical. The invariant is "one
  identity → one canonical artifact set," established at first verified build. Do not wire a reproducibility
  check that would fail on the second build.
- **Bimodal UX.** "Most creates = zero compilation" assumes a warm cache. The *first* create of a new engine
  identity — i.e. exactly when an engine developer is iterating — pays the full build (or the Buildkite
  round-trip + export). Native cargo for the dev loop mitigates this.
- **Cache poisoning** if authority is not gated (guardrail 1) — a supply-chain hole dressed as a cache hit.
- **Two-cache coherence** — S3 CAS and Dagger's caches must agree on revocation; S3 is authoritative.
- **Export/timeout prerequisite** — the Dagger export path must be robust before it is load-bearing.

## Phased landing (workspace green at each step)

- **Phase 0 — native-cargo dev binding (unblocks ECS/local now).** `tkr deployment create` resolves the
  co-located dev `tkp` (or `cargo build -p tkp`), stamps + retains + first-applies; `authority =
  LocalDeveloper`, `mode = Dev`. No Dagger, no CAS. This alone realises "create binds the provisioner."
- **Phase 1 — `tkp` independent identity.** Give `tkp` its own version (drop `version.workspace = true`) and
  a `tkp`-scoped provenance `build.rs` computing the closure-scoped `source_hash`/`cargo_lock_hash`; the
  manifest/`BinaryStore` re-key to `EngineIdentity`.
- **Phase 2 — hermetic Dagger `BuildProvisionerBundle()` + S3 CAS reuse.** The canonical bundle; identity-
  keyed reuse; bind re-verifies bytes.
- **Phase 3 — Buildkite + admission gate.** `ProvisionerBuildRequest/Result` verification; the provenance-
  admission gate; authority-partitioned CAS writes; revocation.
- **Phase 4 — thin GitHub Actions dispatch** (optional).

## Open decisions

1. The exact `EngineIdentity` field set and its canonical serialization/digest.
2. Where the deployment's required authority/policy lives (per-deployment, per-backend, org default) and how
   it is edited.
3. Per-deployment artifact **copy** vs **reference + checksum** (copy is stronger for self-contained
   rollback; reference is cheaper).
4. The revocation mechanism (deny-list location, who writes it, how Dagger's caches honour it).
5. `musl` viability for a single static target vs the per-OS/arch matrix.
6. Whether Phase 0 *resolves* a co-located `tkp` or *builds* one — and whether the dev loop ever goes
   through Dagger at all.
7. **`--provisioner-bundle` (pin a create to a pre-built bundle, e.g. `tkp:1.3.0`)** — is an explicit
   operator flag needed, or is "create a released deployment from an already-published engine version"
   better expressed by policy (a released deployment resolves the released bundle) or by pinning the
   *source revision* under `--ci-build`? It pins by **artifact**, whereas `--platform` + source pins by
   **definition + source** — the question is whether that distinction warrants operator-facing surface.
8. **Platform naming.** `--platform compose` selecting the **compose-syn** definition presumes the legacy
   `ComposeDeployment` is retired/renamed. Reconcile `CliPlatformKind` (currently `Local|Compose|Ecs`,
   where `Compose` is legacy) so the operator word "compose" means compose-syn.
9. **Untracked source in a `LocalDeveloper` snapshot.** *(DECIDED — refuse by default.)* The temp-index
   `write-tree` mechanism *can* stage untracked closure files (unlike `git stash create`, which omits them),
   so this is policy, not mechanism. **Default: `create` refuses and lists any untracked `.rs` in the
   closure**; `--include-untracked` opts them in. Inclusion is an explicit operator act, so nothing that
   determines identity is ever silently swept in or silently omitted.
10. **Snapshot retention: pin-a-ref vs record-oid-only.** *(DECIDED — record oid only; pin under
    `TrustedCi`.)* `EngineIdentity` keys on the `write-tree` **tree** oid; a `commit-tree` wrapper (fixed
    synthetic identity + fixed timestamps for determinism; parentless fallback on an unborn `HEAD`) gives a
    reachable audit handle but is a *dangling* commit, prunable by `git gc`. Since the built bytes live in
    the bundle, **the default records the commit oid only**; `refs/tokeira/snapshots/<engine-identity>` pins
    it **only under `TrustedCi`**, where durable audit matters.
