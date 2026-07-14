# Design Document

## Overview

A tokeira deployment's platform — its modules, resources, services, their typed parameters, and their
wiring — is authored as a **deployment definition** written in a small, fixed subset of Rust and stored
as a **`.tkd`** file. The bound provisioner (`tkp`) **interprets** the `.tkd` at plan/apply time (it is
never compiled into the binary) and turns it into the in-memory deployment the IaC and runtime engines
already consume. Because the definition is *interpreted data*, editing it — a value or the structure — is
an ordinary `apply`, never a rebuild.

A definition has two halves, both ordinary Rust:

- **`config()`** returns the **operator surface**: a value of the platform's config type (`struct`/`enum`
  types the definition itself declares). Overriding a default *is* editing this literal — flipping
  `storage: Storage::InMemory` to `Storage::Dsql { .. }` is the whole edit.
- **`deployment(cfg, cx)`** returns the **structure**: it reads the resolved config and the injected
  context and calls a fixed builder vocabulary (`d.module`, `d.resource`, `d.service`, `d.writeback`,
  `r.output`) to describe the deployment.

The interpreter is the platform-agnostic **`tokeira-tkd`** crate. It parses the `.tkd` with
[`syn`](https://docs.rs/syn), enforces the **interpreted subset** (a reject-by-default allow-list), and
walks it into the platform's deployment type. It is generic over a platform-supplied **`HostBridge`**:
the interpreter holds host values opaquely and routes every host operation (construct a kind, call a
builder verb, read a context field) through the bridge, so it names no concrete kind and needs no
`Box<dyn Any>`. Each platform implements one bridge; `platforms/compose-syn` (`ComposeBridge`) and
`platforms/eks` (`EksBridge`) share the one interpreter.

This split is the whole point:

- **Engine identity** — the interpreter (`tokeira-tkd`), each platform's builder vocabulary + kinds +
  bridge — is compiled Rust, covered by the provisioner's `source_tree_hash`. Changing it mints a `tkp`
  version and gates an `upgrade`.
- **Configuration revision** — the `.tkd` — is data the bound `tkp` reads. Editing it is a plan, recorded
  as a monotonic `config_revision`. It can never become an engine-identity change, *because* the
  interpreted subset only lets a `.tkd` **name** the versioned vocabulary — no new kind, no I/O, no
  apply-logic (see [Security posture](#security-posture)).

**Scope of this document.** `platforms/compose-syn` is the reference platform and the source of every
ground-truth citation here. `platforms/eks` is the second platform on the same interpreter. The
remaining compiled platforms (`platforms/{compose,ecs,local}`) are migration targets, not yet on the
`.tkd` model (see [Multi-platform status](#multi-platform-status)).

> **Historical note.** An earlier design used a bespoke `logos`/`chumsky`/`ariadne` compiler, a
> `Composition` IR, a `KindLibrary`/`Realizer`/`DslPlatform` framework, multi-file `.platform` files, and
> an `inputs.toml`. None of that shipped in that form; it is preserved in
> [`proposals/HISTORY.md`](./proposals/HISTORY.md). The decision trail is Proposals 001–004.

## Audience: platform author vs operator

The DSL serves two roles, and the design keeps their surfaces distinct (Requirement 10).

| | **Platform author** | **Operator** |
|---|---|---|
| Who | Engineer building a platform (e.g. `compose-syn`, `eks`) | User instantiating a deployment |
| Writes | The Rust *engine identity*: the builder vocabulary, the kinds, the `HostBridge`, and the shipped default `definition.tkd` | The `.tkd`'s `config()` values, and optionally the `deployment()` structure |
| Owns | What kinds exist, what they realize to, what the operator *may* express | The deployment's configuration revision |
| Changes via | A `tkp` rebuild (engine-identity change → `upgrade`) | An ordinary `apply` (no rebuild) |
| Constrained by | Rust, review, the engine's traits | The interpreted subset (may only name the author's vocabulary) |

The author ships a starting `definition.tkd` with the platform crate (exposed as `DEFAULT_TKD`);
`tkr deployment create` persists a copy into the deployment. From then on the operator owns that copy.
`#[create]` fields mark the parts an operator may set only once.

## The deployment definition (`.tkd`)

The definition is Rust syntax, but the interpreter accepts only a fixed subset. Two halves.

### The config surface — `config()`

The config type is `struct`/`enum` types the definition declares, and `config()` returns the default
value. This *is* the operator's editable surface.

```rust
enum DsqlMode { Managed, Preexisting }

enum Storage {
    InMemory,
    Dsql { region: String, mode: DsqlMode, endpoint: Option<String>, arn: Option<String> },
}

struct Compose {
    #[create]                 // create-time-immutable; editing it later is a retarget tkp refuses
    storage: Storage,
    tokeirad: Tokeirad,
    observability: Observability,
}

fn config() -> Compose {
    Compose {
        storage: Storage::InMemory,   // flip to Dsql { .. } for persistence — that is the whole edit
        tokeirad: Tokeirad { image: "tokeirad:latest".into(), replicas: 1, grpc_port: 7233, metrics_port: 9090 },
        observability: /* … */
    }
}
```

- **`#[create]`** marks a create-time-immutable field. On apply, the value in `config()` is diffed against
  the recorded baseline; a changed `#[create]` field is a **retarget** the provisioner refuses (it would
  rename or replace live resources), not a reconcile. Every other field reconciles freely.
- **`#[require(<expr>)]`** attaches a constraint to a config type, evaluated against the resolved config
  before `deployment()` runs (e.g. "`Preexisting` needs an endpoint"). A false result aborts the apply
  with the constraint's span.

`config()` must be **host-free** — it may contain only data (structs/enums/scalars), never a kind or
builder handle. The interpreter enforces this (`tokeira-tkd::interpret`), because the `#[create]` diff
compares config values structurally and a host handle is not comparable.

### The structure — `deployment(cfg, cx)`

`deployment` reads the resolved config `cfg` and the injected context `cx`, and calls the builder
vocabulary. Every call records a piece of the deployment; nothing executes an effect.

| Builder verb | Produces | Notes |
|---|---|---|
| `Deployment::new(&["default", …])` | the deployment + its namespaces | |
| `d.module(name, &[needs…])` → `ModuleRef` | an IaC module (resource grouping) + module-level deps | |
| `d.resource(&m, id, Kind { … })` → `ResourceRef` | an IaC resource of a fixed kind | kind from the author's vocabulary |
| `d.service(&m, name, Service { … })` | a workload — realizes to **both** an infra resource and a deploy-engine service | member of module `m`; `needs` are deploy-ordering deps |
| `r.output(name)` → `Output` | a deferred reference to a resource's provisioned output | resolved from `InfraState` post-apply |
| `d.writeback(key, value)` | a server-config writeback entry | value is a literal or an `Output` |

Context is read as `cx.project_name` / `cx.region`. The operator's `.tkd` never names a host path: volume
anchors are the path-free `cx.state(sub, at)`, `cx.config(sub, at)`, and `cx.docker_sock()`
(`platforms/compose-syn/src/context.rs`). The realizer resolves them to concrete host paths at apply.

The kinds an author exposes (compose: `platforms/compose-syn/src/kinds.rs`) are `LocalStateDir`,
`DsqlCluster`, `DynamoDbTable`, `ObservabilityConfigFiles`, and `Service`. Each is a typed struct
implementing `Kind` (`fn realize(&self, cx) -> Box<dyn iac::Resource>`), building its `tokeira-compose` /
`tokeira-aws` engine resource directly. A `Service` additionally carries author-mechanic flags
(`server_config: bool`, `aws: Option<String>`) that the operator only *declares*; the realizer performs
the `tokeirad.toml` mount and the AWS credential edge (see [Security posture](#security-posture)).

The canonical worked example is the shipped compose definition,
[`platforms/compose-syn/definition.tkd`](../../../platforms/compose-syn/definition.tkd): `config()` plus a
`deployment()` that declares `local_state`, a conditional `dsql` module (under
`if let Storage::Dsql { .. }`), the `observability` module (config-files resource + mimir/loki/grafana/
alloy services), the `runtime` module (`tokeirad`), and the DSQL writeback.

### The interpreted subset (the boundary)

`syn` parses all of Rust; the interpreter walks only a fixed allow-list and **rejects everything else**
before evaluation. Reject-by-default *is* the security model (`tokeira-tkd::subset`).

- **Allowed items:** `struct`/`enum` definitions (the config schema), the `config()` and
  `deployment(...)` functions, and pure helper `fn`s; `impl` blocks only as `#[require]` carriers.
- **Allowed attributes:** `#[create]`, `#[require(<expr>)]`, `#[derive(..)]` (ignored), doc comments.
- **Allowed expressions:** struct/enum literals (with `..Kind::EMPTY` spread for author kinds), array/
  tuple literals, field access, `let`, `if`/`if let`/`match` (value-producing), method calls **only** on
  the builder/handles/`cx` (validated against the bridge's method set), `format!`/`vec!`/`matches!`, and
  `&`/`.clone()`/`.into()`/`.to_string()` (identity during lowering).
- **Allowed patterns:** enum-variant binding (`Storage::Dsql { region, .. }`), tuple, ident, wildcard.
- **Rejected (unit-tested as such):** `for`/`while`/`loop`, `unsafe`, `async`/`.await`, `?`, closures
  (except the future writeback closure), arbitrary function calls, any `std::env`/`std::fs`/`std::path`/
  `.exists()`/`.join()`, and any macro outside the three whitelisted.

## The interpreter (`tokeira-tkd`)

The interpreter is platform-agnostic. `interpret(src, bridge, cx)` runs the pipeline
(`crates/tokeira-tkd/src/lib.rs`):

1. **Parse** — `syn::parse_file(src)`.
2. **Collect schema** — `schema::collect` builds the type table and fn table and records `#[create]`/
   `#[require]`.
3. **Subset check** — `subset::check` runs the reject-by-default allow-list **before any evaluation**; an
   out-of-subset definition is rejected, never run.
4. **Eval `config()`** — walk to the resolved config `Value`; reject if it contains a host handle.
5. **Admission** — evaluate `#[require]` constraints against the config (and, on re-apply, the `#[create]`
   retarget diff — `retarget_check`).
6. **Eval `deployment(cfg, cx)`** — walk the body, dispatching every host operation through the bridge,
   and `HostBridge::finish` unwraps the return into the platform's `Deployment`.

The one runtime value type is `tokeira_tkd::Value<H>` — scalars, `Vec`/`Tuple`/`Opt`, `Struct`/`Enum`
(config types modelled generically from the `.tkd`'s own AST), and `Host(H)` (the platform's opaque
handles). New kinds and config types are new bridge entries and new structs — **zero** new `Value`
variants — which is what makes the interpreter reusable across platforms.

### The `HostBridge` seam

A platform implements `tokeira_tkd::HostBridge`, the only place platform types are named. Its surface
(`crates/tokeira-tkd/src/bridge.rs`; compose impl `platforms/compose-syn/src/bridge.rs`):

- `type Host` — the platform's closed handle enum (compose `HostObj`: `Deployment`, `Module`, `Resource`,
  `Output`, `Kind`, `Vol`, `Cx`). Dispatch keys on the handle's tag, so a receiver-type error is
  structural, never a downcast.
- `type Cx` / `type Output` — the platform's context and deployment types.
- `is_kind` / `knows_method` / `knows_assoc` — the allow-list the subset check consults, so an unknown
  kind or method is a *spanned reject*, not a runtime panic.
- `construct_kind` / `kind_defaults` — build a kind from a resolved field map (the per-kind "reflection"
  Rust lacks, written once per kind); `kind_defaults` supplies the `..Service::EMPTY` overlay image.
- `assoc` / `call_method` / `host_field` — `Deployment::new`, the builder verbs, and `cx.<field>` reads.
- `cx_host` / `finish` — inject the context handle and unwrap the final `Deployment`.

The interpreter has **no operator-reachable panic**: post-subset the receiver kind is proven, so the only
`unreachable!`s are the proven receiver matches; every other path returns `EvalError`.

## Realization: from `.tkd` to the engine

The builder vocabulary and kinds (`platforms/compose-syn/src/{builder,kinds}.rs`) realize the recorded
deployment **directly** to engine types — there is no intermediate IR:

- A **kind** realizes to a `Box<dyn tokeira_iac::Resource>` via `Kind::realize(&self, cx)`
  (`DsqlCluster` → `tokeira_aws::DsqlCluster`, `DynamoDbTable` → `tokeira_aws::DynamoDbTable`,
  `ObservabilityConfigFiles` → the config-files resource, `LocalStateDir` → the state-dir resource).
- A **service** realizes two ways: as an infra `iac::Resource` and as a deploy-engine `Service` workload
  (`Service::to_compose_service` → `tokeira_compose::ComposeService`). `to_compose_service` is the sole
  owner of the relocated author mechanics (host-path volume resolution, the conditional `tokeirad.toml`
  mount, the DSQL AWS edge), kept byte-identical to the compiled compose platform.

The provisioner consumes the interpreted deployment through a thin adapter
(`platforms/compose-syn/src/adapter.rs`): `TkdDeployment` implements `tokeira_orchestrator::Deployment`
and `Ops`, projecting `remote_state_module`/`infra_modules` from `realize_module`, `services` from
`realize_workloads`, `required_namespaces` from `namespaces`, and `collect_writeback` from
`writeback_entries` (resolving each deferred `Output` handle against the post-apply `InfraState`).
`prototypical_config` returns `DEFAULT_TKD`. Day-2 verbs (`scale`/`logs`/`ports`) that need the live
platform are driven through `tkp`, not tkr's facade.

## Deployment directory, lifecycle, and configuration (authoritative)

This section is the authoritative account of **where a deployment's pieces live on disk, who writes each
one and when, what precisely defines its configuration, and how that configuration is represented once
persisted.** It is ground-truthed against `platforms/compose-syn` (`definition.tkd`, `src/context.rs`
`Cx`, `src/lib.rs` `DEFAULT_TKD`, `src/adapter.rs`).

### Config taxonomy — four distinct things, four homes

1. **Definition (structure + operator config)** — modules, resources, services, wiring, writeback, **and**
   the operator's chosen input values. All of it lives in one interpreted **`definition.tkd`**:
   `deployment(cfg, cx)` is the structure; `config()` is the operator surface. The platform author ships
   the file (`DEFAULT_TKD`); `tkr deployment create` persists a copy into the deployment; every
   `plan`/`apply` interprets that **persisted copy**, never the crate file. It is the deployment's
   **configuration revision** — data, not compiled code.
2. **Deployment registry (`metadata.json`)** — the CLI-side record `tkr` keeps per deployment
   (`apps/tkr/src/metadata.rs` `DeploymentMetadata`): `name`, `id`, `platform`, **`storage`**
   (`in-memory` | `dsql`), **`status`** (`created` | `running` | `stopped`), and `created_at`/`updated_at`.
   The engine never reads or writes it — only the operator CLI does. `storage` is set once from the
   `--storage` flag at create; `status` starts `Created` and is advanced by `update_status` (deploy apply /
   scale-down; local also reconciles against `tokeirad.pid`). This record names *who* the deployment is and
   *what state the CLI thinks it is in* — it is **not** the deployment definition, and it does **not**
   currently hold `project_name` or `region` (see [Where storage, region, and status
   live](#where-storage-region-and-status-live)).
3. **Ambient context** — `deployment_dir` and the host paths derived from it. Supplied by the host every
   invocation and **never persisted, never surfaced to the `.tkd`**: `Cx` exposes only `project_name` and
   `region` to `deployment(cfg, cx)`; path math lives author-side in `Cx` helpers and the service
   realizer. Rollback restores the definition, never the context.
4. **Server runtime config** — `tokeirad.toml` (`TokeiraConfig`). **Not** the DSL's, but a first-class
   **create-time artifact**: `create` seeds a **prototypical `tokeirad.toml`** (from
   `prototypical_server_config(storage, region)` — for DSQL it sets `infrastructure.storage`, a placeholder
   `dsql.endpoint`, and the region) so the operator can edit server-config defaults **before** the first
   apply. Apply then **writes back** the discovered values (`infrastructure.dsql.endpoint`, the
   coordination-table names — the keys the `.tkd`'s `writeback` names) on top of the operator's edits.
   *(Current gap: the compose branch of `create` does not yet seed `tokeirad.toml` — only `local`/`ecs` do
   — `apps/tkr/src/deployment_dir.rs`; the machinery (`prototypical_server_config`) exists. Tracked in
   `tasks.md` task 4.)*

Everything else on disk is **derived or runtime state**, not config.

### Layout

```
<tkr-state-root>/<name>/
  definition.tkd        # (1) the authored, interpreted definition — written verbatim at create
  metadata.json         # (2) CLI registry: name, id, platform, storage, status, created_at, updated_at
  state/                # WRITTEN at create (empty); engine CAS state fills it at apply
  tokeirad.toml         # (4) SERVER config — seeded (prototypical) at create; writeback-updated at apply
  docker-compose.yml    #     GENERATED at apply (compose provider artifact)
  config/               #     GENERATED at apply (observability config files + dashboards)
  .tokeira-state/       #     container runtime data volumes (mimir / loki / grafana)
  .latest               #     (in the parent) name of the most-recently-selected deployment
```

At `tkr deployment create` the intended artifacts are `definition.tkd`, `metadata.json`, a prototypical
`tokeirad.toml`, and an empty `state/`; `docker-compose.yml`, `config/`, and `.tokeira-state/` are produced
by `apply`. *(Current gap: the compose branch of `create` — `apps/tkr/src/deployment_dir.rs` — writes only
`definition.tkd` + `metadata.json` + `state/`, not yet the prototypical `tokeirad.toml`; the legacy
`local`/`ecs` platforms seed `tokeirad.toml` plus a `deployment.toml`, the latter unused by the `.tkd`
model.)* `ctx.deployment_dir` is `<name>/`. `definition.tkd` sits **directly at the deployment root as a
single file** — the interpreted subset has no `use`/import construct, so there is no containment
subdirectory (multi-file composition is a roadmap item; see `requirements.md`).

### Lifecycle — who writes what, when

- **`tkr deployment create`** seeds the config the operator will edit before the first apply: the shipped
  `definition.tkd`, `metadata.json` (identity + `storage` + `status = Created`), a **prototypical
  `tokeirad.toml`** (server-config defaults, from `prototypical_server_config(storage, region)`), and an
  empty `state/`. No `docker-compose.yml` and no engine artifacts yet — those are apply outputs.
  **Current gaps** (`apps/tkr/src/deployment_dir.rs` `create`): the compose branch (a) does not yet seed
  `tokeirad.toml`, and (b) writes the `.tkd` **verbatim**, so `--storage`/`--region` are recorded in
  `metadata.json` but not baked into the seeded `config()` (which keeps `storage: Storage::InMemory`).
  Both are tracked as the create-completeness work in `tasks.md` task 4 / Roadmap R5. See [Where storage,
  region, and status live](#where-storage-region-and-status-live).
- **Operator edit** — the operator edits `definition.tkd` (a `config()` value, or the `deployment()`
  structure). Data, not a rebuild. A `#[create]` change is refused as a retarget; every other edit
  reconciles.
- **`tkr infra|deploy plan|apply`** (forwarded to the bound `tkp`) — `tkp` interprets `definition.tkd`
  against the injected `Cx` → a deployment; the realizer *generates* `docker-compose.yml` and `config/`
  and writes engine `state/`; the writeback updates `tokeirad.toml` from post-apply infra outputs. Apply
  records a monotonically increasing `config_revision` in the deployment state envelope. Generated
  artifacts are **derived and reproducible**, never retained as config source.

### What defines the config, precisely

A deployment's configuration is **exactly**:

```
config  ==  definition.tkd (digested)                                   # structure + config() values (incl. storage) + #[create] + writeback
          + the injected Cx (project_name, region)                       # identity/ambient supplied at interpret time
          + (tokeira-tkd interpreter, builder vocabulary, kinds, bridge)  # the engine identity, compiled into tkp
```

Given those three, `interpret(definition.tkd, bridge, cx)` → the platform deployment → every generated
artifact is reproducible. So **only the first is retained and digested as the configuration revision**;
the third is the **engine identity** the provisioner binds (`source_tree_hash`), never the `.tkd`. The
content digest is over `definition.tkd`. `metadata.json` is a **CLI registry**, not an input to
interpretation — the interpreter never reads it.

### Configuration when persisted

The persisted representation is deliberately minimal and legible:

- **`definition.tkd`** — the operator-editable config revision (including the `config().storage` choice),
  digested by the provisioner. A `git diff` of it is a complete, reviewable change to the deployment.
- **`metadata.json`** — the CLI registry (`name`, `id`, `platform`, `storage`, `status`, timestamps).
  Written and read only by `tkr`; changing its field set is a breaking change for on-disk deployments
  (prefer additive optional fields — `apps/tkr/src/metadata.rs`).
- **`config_revision`** (in the deployment state envelope, owned by `platform-provisioner-binary`) —
  monotonic; bumped on each mutating apply. An edit that changes only `config()` values or `deployment()`
  structure is a new `config_revision`, never an engine-identity change. *(Not yet wired for compose — the
  interpret→apply path is a remaining task.)*

There is **no** separate `inputs.toml`: operator value choices live in the `.tkd`'s `config()`.

### Where storage, region, and status live

Because these three are the parts most likely to be looked for, and because two of them currently live in
more than one place, they are called out explicitly:

- **`status`** — only in `metadata.json` (`DeploymentStatus`: `created`/`running`/`stopped`). Purely a
  CLI-observed lifecycle state; the engine and the `.tkd` never carry it.
- **`storage`** — in **two** places that are **not yet reconciled**: `metadata.json.storage` (the CLI's
  record, set from `--storage` at create) and the `.tkd`'s `config().storage` (what the interpreter
  actually reads). `create` seeds the `.tkd` verbatim, so `--storage dsql` is recorded in `metadata.json`
  but **not** reflected in the seeded `.tkd`. The interpreter reads only the `.tkd`, so the `.tkd` is the
  effective source of truth for realization; `metadata.json.storage` is a CLI convenience that can drift.
  The choice also parameterizes the prototypical `tokeirad.toml` (`infrastructure.storage` + the DSQL
  region/endpoint placeholder) that `create` seeds — so all three must agree.
- **`region`** — currently **only** in the `.tkd` (`Storage::Dsql { region }`); it is **not** persisted in
  `metadata.json`, and the `--region` flag is dropped for compose at create. The injected `Cx` also
  carries `region` at interpret time, but the `tkr`/`tkp` wiring that populates `Cx.region` (and
  `Cx.project_name` from the deployment `name`) is unfinished.

Reconciling this — either patching the seeded `.tkd` from `--storage`/`--region` at create, or making the
`.tkd` the sole source and deriving `metadata.json.storage` from it — is a tracked item (`requirements.md`
→ Roadmap R5). The intended end state: the `.tkd`'s `config()` is authoritative for `storage`/`region`,
`metadata.json` carries at most a derived copy for `tkr list`, and a changed `#[create]` field
(`storage`) is caught by the `retarget_check` against the recorded prior config value.

### Boundary with `tkp`

This spec defines the **shape** of the config artifact (`definition.tkd`) and how it is interpreted. The
**versioning, provenance stamping, integrity manifest, retention, the deployment state envelope, and the
monotonic `config_revision`** are the provisioner's (`platform-provisioner-binary`). This spec writes the
create-time artifacts and interprets them; it does not stamp, version, or retain them.

## Security posture

The definition is untrusted input; the interpreter is the trust boundary (Requirement 12).

1. **No ambient authority.** Interpretation reads only the closed `Cx` and the platform's kinds/vocabulary
   through the bridge. The subset admits no `std::env`/`std::fs`/network/clock/OS construct, and no
   env/key lookup exists in the language.
2. **Reject-by-default subset.** The allow-list runs before evaluation; anything outside it is a spanned
   diagnostic and never runs. This is what makes a `.tkd` edit *structurally* a config revision and never
   an engine-identity change.
3. **`config()` is host-free.** A kind/builder handle can never appear in the config surface, so the
   `#[create]` diff always compares comparable data.
4. **Secrets are declared, never read.** A workload's credential need is a typed flag (`aws: Some(region)`
   → the `~/.aws` mount + `AWS_*` forwarding); the secret values never enter the `.tkd`, its evaluation,
   or its output. **Documented deviation:** the AWS edge is resolved *author-side at realize time*
   (`Service::to_compose_service` reads live `HOME`/`AWS_*`), so the realized DSQL manifest is not
   hermetic — the `.tkd` authoring is hermetic, the sanctioned realizer boundary is not. No consumer
   should expect a deterministic realized artifact under DSQL.
5. **No operator-reachable panic.** The interpreter returns `EvalError` on every reachable path; a fuzz
   test asserts malformed input yields diagnostics, never a panic.

## Multi-platform status

The DSL is already multi-platform on one interpreter (Requirement 9):

| Platform | Crate | State |
|---|---|---|
| Compose | `platforms/compose-syn` (`ComposeBridge`) | Reference platform; fidelity-proven against the compiled definition |
| EKS | `platforms/eks` (`EksBridge`) | Second bridge on `tokeira-tkd`; its own kinds + config structs (`ServiceManifest`, `IngressRule`) |
| Compose (legacy), ECS, Local | `platforms/{compose,ecs,local}` | Compiled platforms, **not** yet on the `.tkd` model — migration targets (roadmap) |

A new platform is: a `HostBridge` impl, its kinds + builder realizers, and a shipped `definition.tkd`.
The interpreter is not touched — which is exactly why `tokeira-tkd` was extracted out of `compose-syn`.

## Correctness properties

Property tests are tagged `// Feature: platform-config-dsl, Property N`.

### Property 1: Interpretation is deterministic given the context

*For any* `.tkd` and *any* `Cx`, repeated `interpret` SHALL yield an identical deployment — the same
resources, services, ids, dependency edges, module ownership, and writeback entries.

**Validates: Requirement 3**

### Property 2: Out-of-subset definitions are rejected before evaluation

*For any* `.tkd` containing a construct outside the interpreted subset (a loop, a non-whitelisted call,
`std::env`/`std::fs`, an unknown macro, an unknown kind or method), `subset::check` SHALL return a spanned
diagnostic and the definition SHALL NOT be evaluated.

**Validates: Requirements 3, 12**

### Property 3: `config()` is host-free

*For any* `.tkd` whose `config()` evaluates to a value containing an author kind/handle, `interpret` SHALL
reject it before admission.

**Validates: Requirement 4**

### Property 4: `#[create]` change is a retarget; other edits reconcile

*For any* two config values differing in a `#[create]` field, `retarget_check` SHALL report a retarget;
*for any* two differing only in non-`#[create]` fields, it SHALL report none.

**Validates: Requirements 4, 11**

### Property 5: `#[require]` gates admission

*For any* config violating a `#[require]` constraint, `interpret` SHALL abort with the constraint's span
before `deployment()` runs.

**Validates: Requirement 4**

### Property 6: A service lowers to both an infra resource and a deploy-engine workload

*For any* `d.service(...)`, realization SHALL produce both a `tokeira_iac::Resource` and a
`tokeira_deploy_engine::Service`, and no engine object SHALL be produced except by a kind's `realize`.

**Validates: Requirement 2**

### Property 7: Output references resolve at apply, not at interpretation

*For any* `r.output(name)` used in a writeback, interpretation SHALL record a deferred handle and SHALL
NOT require the value; the adapter SHALL resolve it from the referenced resource's post-apply `InfraState`.
An output naming a resource absent from the deployment SHALL yield no writeback value.

**Validates: Requirement 5**

### Property 8: Fidelity — interpreted `.tkd` equals the compiled reference

*For any* config mode (InMemory, DSQL), the deployment realized from `definition.tkd` SHALL match the
compiled reference (`definition.rs` → the engine `ComposeDeployment`): same workload shape, namespaces,
per-module resource identity (id/type/module/deps), and writeback keys/values.

**Validates: Requirement 9**

### Property 9: Writeback projects only declared keys

*For any* `.tkd`, `collect_writeback` SHALL emit exactly the keys the definition declares, resolving
literal values directly and `Output` values from `InfraState`; it SHALL NOT write any other key of
`TokeiraConfig`.

**Validates: Requirement 6**

### Property 10: No ambient authority; interpretation never panics

*For any* `.tkd` (well-formed or adversarial), interpretation SHALL read no OS environment, network,
clock, or filesystem path, and SHALL return a value or an `EvalError`/diagnostic — never a panic.

**Validates: Requirement 12**

### Property 11: The digest defines the config revision

*For any* `definition.tkd`, its content digest SHALL be stable across reads and SHALL change iff the file
changes; recomputing the deployment from the retained `definition.tkd` under the recorded engine identity
SHALL reproduce the same deployment (with the same `Cx`).

**Validates: Requirements 7, 8**

## Error handling

| Condition | Handling |
|-----------|----------|
| Out-of-subset construct (loop, closure, arbitrary call, `std::env`/`std::fs`, unknown macro) | `subset::check` spanned diagnostic; never evaluated (Property 2) |
| Unknown kind / unknown method / unknown associated fn | subset diagnostic (bridge `is_kind`/`knows_method`/`knows_assoc`) (Property 2) |
| Unknown / missing / misspelled kind field | `EvalError` at construction — `construct_kind` consumes the field map and rejects leftovers (Property 6) |
| `config()` contains an author kind/handle | `interpret` rejects before admission (Property 3) |
| `#[create]` field changed vs recorded baseline | `retarget_check` → retarget refused, not reconciled (Property 4) |
| `#[require]` constraint false | admission abort with the constraint's span (Property 5) |
| Output reference to a resource absent from the deployment | no writeback value emitted for that key (Property 7) |
| Malformed / adversarial `.tkd` | `EvalError` or diagnostics; never a panic (Property 10) |
| Realize-time effect failure (AWS edge, filesystem) | surfaced by `tkp` at apply, outside the language |

## Testing strategy

- **Fidelity oracle (the spine).** `platforms/compose-syn/tests` proves the three-way lock: the
  interpreted `definition.tkd` equals the compiled `definition.rs` equals the engine `ComposeDeployment`,
  for both InMemory and DSQL — workload shape, namespaces, per-module resource identity (id/type/module/
  deps), and writeback keys/values (Property 8). The compiled `definition.rs` is retained as the
  differential oracle.
- **Per-kind round-trip.** Each kind is built from a fully-populated field map and asserted equal to the
  compiled-literal construction, field for field (the highest-risk surface — e.g. the nine-field
  `ObservabilityConfigFiles`), backstopping the synchronous shape oracle.
- **Subset reject fixtures.** Targeted `.tkd` snippets (a `for` loop, a non-writeback closure,
  `std::env`, `.exists()`, an unknown macro/method/kind, an un-placed-kind `let`) each assert a spanned
  reject (Property 2).
- **No-panic fuzz.** Malformed token input asserts clean diagnostics, never a panic (Property 10).
- **Admission.** `#[create]` retarget and `#[require]` fixtures (Properties 4, 5); a `replicas` edit
  reconciles.
- No tests require live AWS credentials, network, or Docker.

## Future directions

These are captured as the roadmap in `requirements.md` and are **not** part of the current contract:
multi-file `.tkd` composition with `use` import containment; declared `context {}` providers
(`env`/`env.secret` with `Secret<T>` taint) beyond the implicit `Cx`; the typed `|t: &mut TokeiraConfig|`
writeback closure (replacing the dotted-key form); and migrating the remaining compiled platforms
(`compose`, `ecs`, `local`) onto the `.tkd` interpreter.
