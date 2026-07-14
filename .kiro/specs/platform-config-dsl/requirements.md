# Requirements Document

## Introduction

A tokeira deployment's platform — its modules, resources, services, their typed parameters, and their
wiring — is authored as a **deployment definition** written in a small, fixed subset of Rust and stored
as a **`.tkd`** file. The bound provisioner (`tkp`) **interprets** the `.tkd` at plan/apply time into the
in-memory deployment the IaC and runtime engines already consume; it is never compiled into the binary.
Because the definition is *interpreted data*, both value changes and structural changes are an ordinary
`apply` — no rebuild, no per-deployment source snapshot.

A definition has two halves, both ordinary Rust: `config()` returns the operator surface (a value of the
config `struct`/`enum` types the definition declares), and `deployment(cfg, cx)` returns the structure
(it reads the config and injected context and calls a fixed builder vocabulary). The interpreter is the
platform-agnostic **`tokeira-tkd`** crate; each platform binds it by implementing one **`HostBridge`**.
`platforms/compose-syn` (`ComposeBridge`) and `platforms/eks` (`EksBridge`) already share it.

The engine identity the provisioner binds against is the compiled Rust — the `tokeira-tkd` interpreter,
each platform's builder vocabulary, kinds, and bridge; the `.tkd` is the deployment-married configuration
revision it records and retains. The interpreted subset (reject-by-default) is what guarantees a `.tkd`
edit can only *name* the versioned vocabulary and can never silently become an engine-identity change.

> **History.** An earlier design specified a bespoke `logos`/`chumsky`/`ariadne` compiler, a `Composition`
> IR, a `KindLibrary`/`Realizer`/`DslPlatform` framework, multi-file `.platform` files, and an
> `inputs.toml`. None shipped in that form; see [`proposals/HISTORY.md`](./proposals/HISTORY.md). Two of
> its ideas — multi-file composition and declared context providers — are retained as **Roadmap** items
> at the end of this document.

### Authority for "correct"

This DSL matches no external system. Its ground-truth authority is **the current implementation** —
`crates/tokeira-tkd` (the interpreter + `HostBridge` seam) and `platforms/compose-syn` (the reference
bridge, kinds, builder vocabulary, and `definition.tkd`), with `platforms/eks` as the second bridge — and
**the engine's consumption contract** (`tokeira_orchestrator::Deployment`/`Ops`, `tokeira_iac::Resource`,
the deploy-engine service set) into which a definition realizes without loss.

## Glossary

- **Deployment definition (`.tkd`)** — the single-file, interpreted Rust-subset artifact describing one
  deployment: `config()` (the operator surface) + `deployment(cfg, cx)` (the structure). The
  deployment-married configuration the provisioner records, digests, and retains.
- **`config()`** — the function returning the platform's config value; the operator's editable surface.
  Overriding a default is editing this literal. Must be host-free (data only).
- **`deployment(cfg, cx)`** — the function returning the structure; reads the config and context and calls
  the builder vocabulary.
- **Builder vocabulary** — the fixed verbs a definition may call: `Deployment::new`, `d.module`,
  `d.resource`, `d.service`, `d.writeback`, `r.output`, and the `cx.*` accessors. Author-owned Rust.
- **Kind** — a typed struct the operator names (`DsqlCluster`, `DynamoDbTable`, `ObservabilityConfigFiles`,
  `LocalStateDir`, `Service`, …) that realizes directly to a concrete engine resource. The DSL references
  kinds; it does not define their behaviour.
- **`tokeira-tkd`** — the platform-agnostic `syn` interpreter crate: parse → subset check → eval → admit,
  generic over a `HostBridge`.
- **`HostBridge`** — the platform-supplied seam the interpreter dispatches through (construct kinds, call
  builder verbs, read context). The only place platform types are named.
- **Interpreted subset** — the reject-by-default allow-list of Rust constructs the interpreter evaluates;
  everything else is a spanned diagnostic and is never run.
- **`#[create]`** — a config-field attribute marking a create-time-immutable value; changing it after
  create is a *retarget* the provisioner refuses.
- **`#[require(expr)]`** — a constraint on a config type, evaluated against the resolved config before
  `deployment()` runs.
- **Cx (runtime context)** — the closed record `tkp` injects at interpretation: `project_name` and
  `region` are readable by the definition; `deployment_dir` is host-supplied, used author-side, and never
  surfaced to the `.tkd` or persisted.
- **Output reference** — `r.output(name)`: a deferred handle to a resource's provisioned output, resolved
  from `InfraState` after apply.
- **Writeback** — the definition's declaration of which `TokeiraConfig` keys `tkp` writes from post-apply
  infra outputs (the DSQL endpoint/region and coordination-table names).
- **Configuration revision** — the deployment's desired state as data (the `.tkd`); editing it is a plan,
  recorded as a monotonic `config_revision`. Distinct from **engine identity** (the compiled
  interpreter + vocabulary + kinds + bridge, keyed by the provisioner's `source_tree_hash`).
- **Platform author** — the engineer who writes a platform's engine identity (bridge, kinds, builder
  realizers) and ships its default `definition.tkd`.
- **Operator** — the user who instantiates a deployment by selecting a platform and editing its `.tkd`
  `config()` (and optionally structure); never rebuilds a binary.

## Target State

- A deployment's platform is a **`.tkd`** deployment definition (`config()` + `deployment(cfg, cx)`),
  authored by the platform author and shipped with the crate; the operator selects the platform and edits
  the persisted `.tkd`.
- `tkp` is **generic**: the `tokeira-tkd` interpreter + a fixed per-platform bridge/kinds/vocabulary. It is
  not deployment-specific by source.
- The interpreter **enforces the subset before evaluation**, evaluates `config()` (host-free), runs
  admission (`#[require]` + the `#[create]` retarget check), then evaluates `deployment()` into the
  platform's deployment with no partial output on error.
- **Structural and value changes are both `.tkd` edits** → an ordinary `apply` on the same `tkp`.
- The DSL powers `compose-syn` and `eks` on the shared interpreter; the compiled platforms
  (`compose`, `ecs`, `local`) are migration targets.
- The DSL **stays a configuration language**: total, side-effect-free, deterministic, and incapable of
  defining resource behaviour.

## Requirements

### Requirement 1: Platform composition defined by a `.tkd` deployment definition

**User Story:** As an operator, I want my deployment's platform expressed as an interpreted `.tkd` rather
than compiled Rust, so that I can evolve its values and structure without rebuilding a binary.

#### Acceptance Criteria

1. THE deployment definition SHALL express a deployment's full infra+services — modules, resources,
   services, their typed parameters, and their wiring — such that no part of a supported platform's
   structure requires compiled Rust beyond the author's kinds and builder vocabulary.
2. THE definition SHALL consist of two functions: `config()` returning the platform's config value, and
   `deployment(cfg, cx)` returning the structure via the builder vocabulary.
3. WHEN a deployment is created, THEN its platform SHALL be represented by exactly one persisted
   `definition.tkd`, AND every subsequent `plan`/`apply` SHALL interpret that persisted copy, never the
   live platform-crate file.
4. THE `.tkd` SHALL NOT carry the running server's runtime configuration (`TokeiraConfig`); that remains
   `tokeirad.toml`, a separate concern.

### Requirement 2: Kinds are a fixed, typed, compiled vocabulary

**User Story:** As a platform author, I want the `.tkd` to instantiate a fixed set of typed kinds rather
than describe resource behaviour, so that provider correctness stays in reviewed Rust while composition
stays editable.

#### Acceptance Criteria

1. THE `.tkd` SHALL reference kinds and builder verbs **by name** from the platform's bridge/vocabulary
   compiled into `tkp`; it SHALL NOT define or alter a kind's realization.
2. WHEN a `.tkd` names a kind, method, or associated function the platform's `HostBridge` does not expose,
   THEN the subset check SHALL emit a spanned diagnostic and the definition SHALL NOT be evaluated.
3. WHEN a `.tkd` supplies a field a kind does not declare, or omits a required one, THEN construction
   SHALL fail with a located error (the kind constructor consumes the field map and rejects leftovers).
4. A `d.service(...)` SHALL realize to **both** a `tokeira_iac::Resource` and a
   `tokeira_deploy_engine::Service`; no engine object SHALL be produced except by a kind's `realize`.

### Requirement 3: Total, deterministic, hermetic interpretation

**User Story:** As an operator, I want interpretation to be a pure, terminating, deterministic step, so
that the plan derived from a `.tkd` is reproducible and safe to run inside the provisioner.

#### Acceptance Criteria

1. THE interpreter SHALL enforce the interpreted subset (a reject-by-default allow-list) **before** any
   evaluation; a construct outside the subset (loops, arbitrary calls, closures, `std::env`/`std::fs`,
   non-whitelisted macros) SHALL be a spanned diagnostic and SHALL NOT be evaluated.
2. Interpretation SHALL perform no I/O into the language — no OS environment, network, clock, or arbitrary
   filesystem read — during subset check or evaluation.
3. WHEN the same `.tkd` is interpreted with the same `Cx`, THEN it SHALL yield the same deployment — the
   same resources, services, ids, dependency edges, module ownership, and writeback entries — every time.
4. IF interpretation fails at any stage, THEN no partial deployment SHALL be passed to the engine.

### Requirement 4: Static admission precedes realization

**User Story:** As an operator, I want the config validated before anything is realized, so that a
malformed or illegal config is caught as an error rather than mis-provisioned.

#### Acceptance Criteria

1. THE `config()` value SHALL be **host-free** (data only — structs/enums/scalars); IF it contains an
   author kind or builder handle, THEN `interpret` SHALL reject it before admission.
2. WHERE a config type carries a `#[require(expr)]` constraint, THE interpreter SHALL evaluate it against
   the resolved config before `deployment()` runs, AND a false result SHALL abort with the constraint's
   span.
3. WHERE a `#[create]` field's value differs from the recorded baseline on a re-apply, THE provisioner
   SHALL treat it as a **retarget** and refuse it, not reconcile it; a non-`#[create]` field change SHALL
   reconcile.

### Requirement 5: Resource output references

**User Story:** As a platform author, I want a definition to reference a resource's provisioned output (a
DSQL endpoint, a table name) so that dependent wiring resolves after provisioning, not at author time.

#### Acceptance Criteria

1. THE builder vocabulary SHALL provide `r.output(name)` returning a deferred `Output` handle bound to the
   resource's id.
2. WHEN a definition uses an `Output`, THEN interpretation SHALL record the deferred handle and SHALL NOT
   require its value; the value SHALL be resolved by the adapter during apply from the referenced
   resource's provisioned `InfraState`.
3. WHERE an `Output` names a resource absent from the deployment, THE adapter SHALL emit no value for that
   entry (it cannot resolve). Determinism (Req 3.3) applies to the deployment structure and the set of
   references, not to provider-assigned output values.

### Requirement 6: Server-config writeback

**User Story:** As an operator, I want the DSQL cluster identity discovered at apply to reach the server
config, so that `tokeirad` connects to the right cluster and coordination tables.

#### Acceptance Criteria

1. THE definition SHALL declare writeback entries via `d.writeback(key, value)`, where `value` is a
   literal or a resource `Output`.
2. WHEN infra apply completes, THEN `tkp` SHALL resolve each writeback entry (literals directly, `Output`s
   from `InfraState`) and write exactly those keys into `tokeirad.toml`; it SHALL NOT write any other
   `TokeiraConfig` key.
3. THE writeback SHALL be a mechanic specialized for `TokeiraConfig` (the DSQL endpoint/region and the two
   coordination-table names), not a general-purpose config-editing facility.

### Requirement 7: The definition is the retained, digested configuration revision

**User Story:** As an operator, I want the deployment to carry its `.tkd` (and a content digest) so that
the provisioner records, retains, and can roll back to it, making the deployment self-contained without a
buildable source tree.

#### Acceptance Criteria

1. WHEN the provisioner records or retains deployment configuration, THEN it SHALL record/retain
   `definition.tkd` and a content digest over it, sufficient to re-interpret deterministically.
2. WHEN the provisioner captures a rollback checkpoint (per `platform-provisioner-binary`), THEN the prior
   `definition.tkd` SHALL be part of that checkpoint, so a prior configuration is restorable.
3. THE retained `definition.tkd` SHALL be interpretable by the `tkp` whose engine identity is recorded
   alongside it; a checkpoint definition SHALL be paired with the engine identity that can interpret it.

### Requirement 8: Create → edit → apply lifecycle and persisted representation

**User Story:** As an operator, I want a clear, minimal on-disk representation of my deployment's config,
so that I can read, diff, and reason about what defines it.

#### Acceptance Criteria

1. WHEN `tkr deployment create` runs for a `.tkd` platform (compose), THEN it SHALL write the platform's
   shipped `definition.tkd`, write `metadata.json` (the CLI registry: `name`, `id`, `platform`, `storage`,
   `status`, `created_at`, `updated_at`), seed a **prototypical `tokeirad.toml`** (from
   `prototypical_server_config(storage, region)`) so the operator can edit server-config defaults **before**
   the first apply, and create an empty `state/` — and produce no `docker-compose.yml` or other engine
   artifacts yet. *(Current gap: the compose branch of `create` does not yet seed `tokeirad.toml` — only
   `local`/`ecs` do — and does not bake `--storage`/`--region` into the seeded `.tkd`; both are tracked in
   Roadmap R5 / tasks.md task 4. `deployment.toml` is a legacy-platform artifact the `.tkd` model does not
   use.)*
2. THE deployment definition — the configuration revision the interpreter reads — SHALL be exactly the
   `definition.tkd` (digested), interpreted against the injected `Cx` under the engine identity compiled
   into `tkp`. `metadata.json` SHALL be a CLI registry, not an input to interpretation (the interpreter
   never reads it). THERE SHALL be no separate `inputs.toml`; operator value choices live in the `.tkd`'s
   `config()`.
3. WHEN a mutating `apply` runs, THEN `tkp` SHALL interpret `definition.tkd` against the injected `Cx`,
   realize it, generate the derived artifacts (`docker-compose.yml`, `config/`, engine `state/`), perform
   the writeback into `tokeirad.toml`, and record a monotonically increasing `config_revision`.
4. THE generated artifacts (`docker-compose.yml`, `config/`, realized engine resources) SHALL be derived
   and reproducible — regenerated each apply — and SHALL NOT be retained as configuration source.
5. THE persistence, versioning, retention, state envelope, and `config_revision` mechanics SHALL be owned
   by the `platform-provisioner-binary` (`tkp`) spec; this spec owns the artifact shape, the interpreter,
   and the create-time hand-off.
6. THE `.tkd`'s `config()` SHALL be the source of truth for the `storage` (and `region`) choice, since it
   is the interpreter's only input. *(Current gap: `create` records `--storage` in `metadata.json` but
   seeds the `.tkd` verbatim, so the two can diverge, and `--region` is dropped for compose. Reconciling
   this — patching the seeded `.tkd`, or deriving `metadata.json.storage` from the `.tkd` — is Roadmap
   R5.)*

### Requirement 9: Multi-platform via the shared interpreter

**User Story:** As a platform author, I want to add a platform by implementing one bridge over the shared
interpreter, so that platforms share the language and the interpreter is written once.

#### Acceptance Criteria

1. THE interpreter (`tokeira-tkd`) SHALL be platform-agnostic and generic over a `HostBridge`; a platform
   SHALL be expressible as a `HostBridge` impl plus its kinds, builder realizers, and a `definition.tkd`,
   with no change to the interpreter.
2. THE interpreter SHALL support at least two live platforms — `platforms/compose-syn` (`ComposeBridge`)
   and `platforms/eks` (`EksBridge`) — sharing the one crate.
3. WHEN the compose `definition.tkd` is interpreted, THEN the realized deployment SHALL match the compiled
   reference (`definition.rs` → the engine `ComposeDeployment`) for both storage modes: the same workload
   shape, namespaces, per-module resource identity (id/type/module/deps), and writeback keys/values.

### Requirement 10: Accessible to platform author and operator

**User Story:** As either a platform author or an operator, I want the definition to be legible and
role-appropriate, so that authors reason about the engine surface and operators about their config.

#### Acceptance Criteria

1. THE `.tkd` SHALL be written in a subset of Rust syntax (parsed by `syn`), so authors and operators read
   familiar constructs (`struct`/`enum`, `if let`/`match`, struct literals) with editor highlighting and
   `rustfmt`, without a bespoke grammar to learn.
2. THE operator-editable surface (`config()`) SHALL be separated from the structure (`deployment()`), and
   `#[create]` SHALL mark the fields an operator may set only at create.
3. THE spec and the shipped `definition.tkd` SHALL make clear which parts are author-owned engine identity
   (kinds, builder verbs, the bridge — changed only by a `tkp` rebuild) and which are operator-owned
   configuration (the `.tkd` — changed by an ordinary `apply`).

### Requirement 11: Engine identity vs configuration revision

**User Story:** As an operator, I want the engine identity derived from the `tkp` that interprets my
`.tkd` — never declared in the `.tkd` — so that version stamping is reliable and a `.tkd` edit can never
silently change engine behaviour.

#### Acceptance Criteria

1. THE running `tkp` SHALL expose the engine identity it provides (the interpreter + vocabulary + kinds +
   bridge, keyed by `source_tree_hash`); the provisioner's binding SHALL derive from it, never from a
   deployment's `.tkd`.
2. THE `.tkd` SHALL NOT declare or pin an engine/language version; a reference to a kind, field, or
   construct the running `tkp` does not provide SHALL be rejected per Requirements 2 and 3 (subset), not
   via a program-declared version.
3. WHEN the interpreter, builder vocabulary, kinds, or a bridge changes, THEN it SHALL constitute an
   engine-identity change handled by the provisioner's `upgrade` path, distinct from a `.tkd` edit (which
   is an ordinary `apply`, recorded as a new `config_revision`).

### Requirement 12: Closed runtime context and security posture

**User Story:** As an operator, I want the `.tkd` confined to a closed context and the author's vocabulary
with no ambient authority, so that a definition can never read host secrets, reach the OS/network/
arbitrary filesystem, or execute host code.

#### Acceptance Criteria

1. THE `.tkd` SHALL access no OS environment, network, clock, or arbitrary filesystem; the only external
   data it may read at interpretation is the closed `Cx`. THERE SHALL be no environment-variable or
   key-based lookup construct in the language.
2. WHERE a workload requires secret-bearing material (cloud credentials), THE `.tkd` SHALL only *declare
   the need* through a typed field (`aws: Some(region)`), AND `tkp`/the realizer SHALL perform the
   injection at materialization, so secret values never enter the `.tkd`, its evaluation, or its output.
   *(Documented deviation: the AWS edge is resolved author-side at realize time, reading live `HOME`/`AWS_*`,
   so a realized DSQL manifest is not hermetic — the `.tkd` authoring is; the realizer boundary is the
   sanctioned exception.)*
3. THE interpreter SHALL surface every failure as an `EvalError`/diagnostic and SHALL NOT panic on any
   operator-reachable path (asserted by a no-panic fuzz test).
4. THE `.tkd` SHALL exercise no authority beyond composing the author's kinds/vocabulary over the closed
   `Cx`; host-code execution from a definition SHALL be impossible (reject-by-default subset).

### Requirement 13: Runtime context (implicit `Cx`) and recorded-identity precedence

**User Story:** As an operator, I want the definition to read the small set of context values it needs
(project name, region) while identity-bearing values stay authoritative, so a deployment wires host data
without the DSL gaining ambient authority.

#### Acceptance Criteria

1. THE `Cx` injected at interpretation SHALL expose `project_name` and `region` to `deployment(cfg, cx)`;
   `deployment_dir` SHALL be host-supplied, used author-side (in `Cx` helpers and the realizer), and
   SHALL NOT be surfaced to the `.tkd`.
2. WHERE an identity-bearing value is **recorded** with the deployment (`region`, and any future
   `account`), THE recorded value SHALL be authoritative; IF an ambient host source supplies a differing
   value, THE provisioner SHALL surface it as a retarget and require explicit operator confirmation, never
   silently overriding the recorded one. *(Current gap: `region` is not persisted in `metadata.json` and
   is carried only in the `.tkd`'s `config()` for DSQL; `Cx.region` (and `Cx.project_name`, from the
   deployment name) sourcing is unfinished — part of Roadmap R5.)*
3. WHERE a context value is **machine-local ambient** (`deployment_dir`), THE host SHALL supply it without
   confirmation, AND it SHALL NOT be persisted — it is re-derived per host (the ambient-never-retained
   rule); rollback restores the definition, never the context.

## Roadmap (retained, not part of the current contract)

These were part of the original design and remain desirable, but are **not implemented** and are **not**
current requirements. They are recorded here so the direction is not lost. Each would be an
engine-identity change (a `tkp` upgrade), never a `.tkd`-only edit.

### R1: Multi-file `.tkd` composition with import containment

A deployment definition MAY grow beyond a single file, composed by a fail-closed relative import: no
`..`, no absolute path, canonicalised within the deployment root, folder depth ≤ 1, acyclic, composed in
a stable path-sorted order, with the content digest taken over the sorted `(relative_path, sha256)` set.
The interpreted subset currently has **no import construct**; the definition is a single
`definition.tkd`. Adding composition requires an interpreter change (a new subset item + the resolver) and
would move the digest from a single file to the file set.

### R2: Declared context providers (`context {}` / `env` / `env.secret`)

Beyond the implicit `Cx` (`project_name`, `region`), a definition MAY declare additional context values
bound to a canonical, version-fixed provider catalog — at minimum `env "NAME"` (→ `String?`) and
`env.secret "NAME"` (→ `Secret<String>?`, subject to taint rules: never in a diagnostic, only into
secret-accepting parameters). New providers would arrive only by engine upgrade, never declared by an
operator. The current implementation exposes no declared-provider block; the compose AWS edge is handled
author-side by the `Service { aws: Some(region) }` flag and the realizer, not a declared `env` provider.

### R3: Migrate the remaining compiled platforms to `.tkd`

`platforms/{compose,ecs,local}` are still compiled platforms. Each SHOULD be re-expressed as a
`HostBridge` + kinds + builder realizers + a shipped `definition.tkd`, at parity with its current
compiled definition, reusing the shared `tokeira-tkd` interpreter (as `compose-syn` and `eks` do). ECS in
particular exercises richer output-reference wiring (IAM policy needing a cluster ARN, ALB listener target
groups, secrets-by-reference) that the compose surface does not.

### R4: Typed writeback closure

The shipped writeback uses dotted-key strings (`d.writeback("infrastructure.dsql.endpoint", …)`), which
hardcode the `TokeiraConfig` schema as strings. The intended form is a typed closure
`d.writeback(|t: &mut TokeiraConfig| { t.infrastructure.dsql.endpoint = cluster.output("…"); … })`,
accepted by the interpreter as a structural special-form and lowered to the same entries — removing the
magic strings. This is a correctness enhancement (typed paths), not a convenience, and is deferred.

### R5: Reconcile `storage`/`region` between `metadata.json` and the `.tkd`

`storage` currently lives in **two** places: `metadata.json.storage` (the CLI registry, set from
`--storage` at create — `apps/tkr/src/metadata.rs`) and the `.tkd`'s `config().storage` (the interpreter's
only input). `tkr deployment create` seeds the `.tkd` verbatim, so `--storage dsql` is recorded in
`metadata.json` but **not** reflected in the seeded `.tkd`, and `--region` is dropped for compose
entirely. The reconciliation SHOULD make the `.tkd`'s `config()` the single source of truth for
`storage`/`region` — either `create` patches the seeded `.tkd` from `--storage`/`--region`, or
`metadata.json` carries only a *derived* copy for `tkr list`. The `Cx.project_name`/`Cx.region` wiring
(from the deployment name and the recorded configuration) and the `#[create]` retarget check against the
recorded prior config value land with the same work — the tkr/tkp lifecycle wiring (tasks.md task 4).
