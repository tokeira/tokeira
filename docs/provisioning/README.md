# Provisioning

Tokeira's platform contract is a matched language-and-engine pair: every
supported platform defines its own definition vocabulary and delivers the
platform-specific `tkp` engine that gives definitions in that vocabulary their
meaning. A definition document is therefore not portable desired state
interpreted by whichever provisioner happens to be available. It is desired
source for the exact platform engine bound to its deployment.

The three operator-visible surfaces are:

- **The definition set** — desired deployment data in one platform's admitted
  vocabulary, written in `.tkd` (checked Rust syntax) or `.tkdp` (Python on a
  pinned interpreter). The platform names its own documents — Compose's root
  is `deployment.tkd`, with companion parts like `platform.tkd` beside it —
  and a deployment keeps its chosen format for life.
- **`tkp`** — the platform-owned, versioned engine that interprets that
  vocabulary and realizes the deployment.
- **`tkr`** — the construction, admission, placement, selection, and launch
  authority around deployment-local engines.

Shared crates do not make `tkp` a generic engine. `tokeira-tkp` supplies the
common lifecycle shell, `tokeira-platform` the declaration seam,
`tokeira-platform-definition` the two definition frontends, and
`tokeira-deployment` the deployment domain (envelope, repository, admission,
binding) — but the generated composition root links exactly one platform and
one frontend, and the compiler assembles one concrete engine without runtime
plugins or reflection.

## How a deployment is born

<p align="center">
  <img src="../diagrams/deployment-creation.svg" width="900"
       alt="Deployment creation: tkr resolves the platform from the workspace catalog and stages an invisible directory; the engine is obtained content-addressed — a CAS hit is fully re-admitted, a miss triggers a hermetic Dagger build whose bundle is published back; the engine is bound to the staging directory and verified through the placed bytes during creation; a TUF repository is provisioned; then one atomic rename commits the deployment and a birth publication records it — publication failure is pending, never fatal.">
</p>

`tkr deployment create` resolves the platform and definition frontend from the
workspace catalog (Cargo metadata, never hard-coded), stages the platform's
definition and content in a directory that stays invisible until the final
atomic rename, and obtains the engine content-addressed:

- **The hermetic bundle is the default.** `--build-image <image>@sha256:<digest>`
  pins the build container; a floating tag is refused. `--dev-engine` is the
  stated development path — a native workspace build whose synthesized sidecar
  honestly records `tests.passed: false` and never enters the CAS.
- **A cache hit is only a performance result.** CAS resolution re-runs the
  full admission gate — authority floor, revocation list, byte re-hash — and a
  tampered or inadmissible entry is an error, never a quiet rebuild.
- **Creation verifies through the placed bytes.** `tkr` itself drives the
  staged `tkp` read-only — the definition check and the config seed are
  internal steps, never operator commands — then independently re-hashes the
  placed binary against its manifest sidecar. A sidecar that does not
  describe the placed bytes is a creation refusal.
- **`ecs` and `eks` are refused at create** while those platforms are
  experimental; the creatable set is `local` and `compose`.
- **Birth publication is non-fatal.** A failed publication leaves the
  deployment created with its publication pending, completed later by
  `tkr deployment publish`.

## Provenance is the language boundary

A definition edit can combine kinds and values already compiled into the
bound engine. It cannot add a provider call, filesystem read, dependency,
kind, or resource implementation. Those changes alter the engine closure and
therefore its identity.

A versioned `tkp` identity is deliberately stronger than a semantic version or
source commit label. `EngineIdentity` combines:

- the content-addressed tree of the frozen source closure — the union of the
  generic shell, the selected platform, and the selected frontend, reached
  through the generated composition root;
- the canonical locked third-party dependency closure;
- the exact Rust toolchain;
- the digest-pinned build container;
- enabled Cargo features; and
- the build profile.

The definition digest is absent because a definition is data interpreted by
the engine. The human-facing semver is also absent because it does not prove
executable equivalence. `BuildAuthority` is orthogonal to identity: identical
engine inputs can be built by a local developer or trusted CI, while
deployment policy can require one authority tier. The CAS address is
partitioned by authority tier and keyed by engine identity and target, so
local bytes cannot satisfy a trusted-CI floor. How the engine is composed,
frozen, identified, built, and married is drawn in
[How a tkp is built](../diagrams/tkp-construction.svg) and explained in
[`tkr` and `tkp`](tkr-and-tkp.md).

## One deployment, two processes

<p align="center">
  <img src="../diagrams/tkr-tkp-lifecycle.svg" width="900"
       alt="Steady state: the operator's definition edits, service manifest edits, and operational commands enter tkr, which routes by the deployment's recorded metadata, verifies the bound binary before a mutation, and spawns the deployment's own tkp with inherited stdio. Inside tkp every mutation walks one gate spine: admission, the operation lease, the marker gate, the binding gate, the retarget gate refusing changed create-time fields, the destructive gate requiring --yes, probe then mutate, writeback, the envelope CAS commit that advances the config revision, post-commit publication, and explanation retention. Read-only verbs bypass the lock and gates.">
</p>

`tkr` routes by the deployment's recorded metadata, not file presence: a
definition-backed deployment forwards to its own `tkp`, and can never silently
fall through to a legacy handler. The launcher resolves only the
`<deployment>/tkp` bound at creation, re-hashes it against the envelope's
integrity manifest before a mutation, and spawns it with inherited stdio —
the `tkp` exit status propagates verbatim.

The two edit paths meet the same spine:

- **A definition edit** is made in place — the definition set in the
  deployment directory is the desired source. `tkr infra plan` previews it
  read-only; `tkr infra apply` walks the gates and commits it as config
  revision N+1, with the post-writeback source retained under
  `state/config-revisions/`. The retarget gate compares the retained prior
  revision against the live source and refuses any changed create-time field
  absolutely.
- **A service manifest edit** flows through `tkr deploy plan` / `deploy apply`
  onto the deploy engine over the definition's service plane. It walks the
  same marker, binding, and retarget gates, counts service deletes as
  destructive, and advances the revision — but carries no writeback,
  publication, or explanation model.

`infra apply`, `revert`, and `upgrade` publish their committed transition to
the deployment repository; a publication failure is pending, never an unwind.
`revert --to N` restores a retained revision and commits it as a new forward
revision — history only moves forward. Upgrade and rollback are two-binary
orchestrations in which the retained prior artifact is verified and restored
by identity, never found by name.

## The deployment repository

Every named deployment is backed by a TUF repository: a signed, pinned trust
anchor under `state/repository/`, a publisher configuration, and a publication
lineage of claims — the definition root, companion parts, the config tree,
the engine manifest, and one engine binary per target. Residency at create is
a local repository beside the deployments root; S3 residency is reached
through the repository remotes (`tkr deployment fetch`, `publish`, `refresh`,
`inspect`, and `list --repositories`). Fetching is the second way a deployment
comes into existence: a staged, verified materialization that re-runs creation-time
realization at the fetched revision.

## Responsibility map

| Surface | Primary owner | Owns | Does not own |
|---|---|---|---|
| `tkr` | `apps/tkr` | Deployment registry and selection; platform catalog discovery; engine construction and the obtain flow; artifact retention and placement; launch classification and byte verification; upgrade and rollback orchestration; repository setup and remotes | Definition evaluation or provider realization for forwarded deployments |
| Platform `tkp` | Platform package | The concrete engine executable: the platform declaration, its kinds and provider integrations, and the reachable shared engine closure | Deployment registry or selection |
| Lifecycle shell | `tokeira-tkp` | Commands, reports, admission, the operation lease, the gate spine, writeback persistence, post-commit publication, upgrade and rollback transitions | Platform kinds, clients, or resource construction |
| Deployment domain | `tokeira-deployment` | Engine identity and bundles, integrity and admission, binding verdicts, the state envelope, config revisions, markers, locks, the repository | CLI parsing or provider operations |
| Definition frontends | `tokeira-platform-definition` | Parsing, reject-by-default subset checking, evaluation, create-time admission, for `.tkd` and `.tkdp` | Filesystem, network, provider calls, or concrete platform kinds |
| The definition set | Operator | Desired values and structure within the bound platform engine's admitted vocabulary | Executable behavior or arbitrary code |

## Platform coverage

| Platform | Route | Definition-backed platform package | Creatable today |
|---|---|---:|---|
| Local | Legacy in-process (`deployment.toml`, `tkr` handlers) | No | Yes |
| Compose | Definition-backed, forwarded to its own `tkp` | Yes | Yes |
| ECS | Definition-backed platform package | Yes | No — refused at create as experimental |
| EKS | Definition-backed platform package | Yes | No — refused at create as experimental |

## Read next

- [Deployment definition programming guide](deployment-definitions.md) explains the
  platform-neutral language, program shape, admitted constructs, annotations, and
  interpretation passes.
- [Definition patterns and current practice](deployment-definition-patterns.md) shows
  source-backed authoring idioms and the complete platform chain.
- [The provisioner](provisioner.md) explains the lifecycle shell, platform seam,
  envelope, gates, locks, revisions, upgrade, and rollback.
- [`tkr` and `tkp`](tkr-and-tkp.md) follows engine construction, bundle admission,
  placement, creation-time verification, launch verification, upgrade, and rollback.
- [Deployment configuration](deployment-configuration.md) documents named deployment
  directories, current in-process and forwarded layouts, and the `tkr` command surface.
- [IaC framework guide](../iac/README.md) covers the convergence engines and
  provider seams beneath provisioning;
  [120-iac-framework](../architecture/120-iac-framework.md) is the
  architectural overview.
- [Platform support](../platforms/README.md) summarizes current target environments.
