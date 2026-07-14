# HISTORY — the superseded bespoke-DSL design

This file preserves the **first design** of the platform configuration DSL, which the current
`requirements.md`/`design.md` no longer describe. It is kept for provenance: so a reader who finds a
reference to a bespoke compiler, a `.platform` file, or a `tokeira-platform-dsl` crate can see what was
meant and why it is gone. Nothing here is current. The live design is in `../design.md`; the decision
trail is Proposals [001](./001-platform-framework-and-realizer.md),
[002](./002-operator-configuration-surface.md), [003](./003-rust-via-syn-deployment-definition.md), and
[004](./004-tkd-syn-interpreter.md).

## Why it was superseded

The original design invented a **bespoke configuration language** — a hand-written `logos` lexer,
`chumsky` parser, `ariadne` diagnostics, a resolver/type-checker, and an evaluator lowering to a neutral
`Composition` IR — plus a `tokeira-platform` framework crate (the `Realizer` seam, `DslPlatform<R>`,
`ConfigurationRevision`, `KindLibrary`/`KindSchema`) and per-platform crates `compose-dsl` / `ecs-dsl`.
A deployment was authored as **one or more `.platform` files** under an import-contained deployment
root, with operator value choices recorded in a separate `inputs.toml`.

Proposal 003 retired the bespoke grammar: every construct had become a syntax debate for needs Rust
already covers (`enum`/`match` for sum types, `#[create]` for create-time-immutability, `if let` for
conditional inclusion, `format!` for path/string building). The decision was to author the definition in
a **small subset of Rust, parsed by `syn`, interpreted at runtime** — the `.tkd` model. Proposal 004
built that interpreter for compose and proved it byte-identical to the compiled definition, then it was
**extracted into the shared `tokeira-tkd` crate** (generic over a platform `HostBridge`), which
`platforms/compose-syn` and `platforms/eks` now share.

**None of the bespoke crates exist in the repository.** There is no `tokeira-platform-dsl`,
`tokeira-platform`, `compose-dsl`, or `ecs-dsl`. Any surviving reference to them is historical.

## What the bespoke design contained (for the record)

### The compiler pipeline (retired)

A deployment definition (the `.platform` file set) was processed in two phases:

- **Compile (pure, total):** resolve `use` imports within the deployment root → lex (`logos`) → parse
  (`chumsky`, with error recovery) → resolve names → type-check against kind schemas → lower to a typed
  executable **Program** IR. Diagnostics rendered with `ariadne`.
- **Execute:** evaluate the Program against an injected `RuntimeContext` to produce a **Composition**
  (`InfraComposition` + the deploy-engine `Service`/`Image` sets + writeback targets).

The current model replaces this whole pipeline with `syn::parse_file` + a reject-by-default subset walk
+ a tree-walking evaluator, all in `tokeira-tkd`. There is no separate IR: the tree walk calls the
platform's builder vocabulary directly and returns the platform's own `Deployment` type.

### The `.platform` worked example (retired)

The compose platform was authored as a modular `.platform` set under the deployment root, depth ≤ 1,
composed by relative `use`:

```
<deployment root>/
  compose.platform        # root: platform decl, inputs, shared lets, use, namespaces, writeback
  infra.platform          # module local_state; module dsql (conditional)
  runtime.platform        # module runtime
  observability.platform  # module observability + observability_config resource
  images.platform         # image declarations
```

with a bespoke grammar, e.g.:

```
platform compose {
  use "infra.platform"
  input storage: Storage = InMemory
  let state_dir = ctx.deployment_dir / ".tokeira-state"
  namespaces [ "default" ]
  writeback when storage is Dsql {
    "infrastructure.dsql.endpoint" = dsql.cluster.cluster_endpoint,
  }
}

module dsql when storage is Dsql {
  depends_on [ local_state ]
  resource cluster = DsqlCluster { mode: d.mode, region: d.region }
}
```

The current model authors the same deployment as a single `definition.tkd` in Rust syntax — `config()`
returns the operator surface and `deployment(cfg, cx)` builds the structure. See
`platforms/compose-syn/definition.tkd`.

### The framework and realizer seam (retired)

Proposal 001 defined a generic `tokeira-platform` crate: a `Realizer` trait (`realize_resource` /
`realize_service` / `realize_image`), `RealizeContext`, `DslPlatform<R>` implementing the orchestrator
`Deployment`/`Ops`, a generic `ConfigurationRevision` config type, and compile-time `FieldSpec` defaults.
Kinds were `KindSchema` implementations registered in a `KindLibrary`, paired to a realizer arm by name.

The current model has **no** `Composition` IR, `KindLibrary`, or `Realizer` trait. A kind is a typed Rust
struct implementing a small `Kind` trait (`fn realize(&self, cx) -> Box<dyn iac::Resource>`) that builds
its engine resource directly (`platforms/compose-syn/src/kinds.rs`, `builder.rs`). The platform binds the
shared interpreter through a `tokeira_tkd::HostBridge` (`bridge.rs`) and exposes the result to the engine
through a thin `tokeira_orchestrator::Deployment` adapter (`adapter.rs`).

### The RuntimeContext provider model (moved to roadmap)

The bespoke design defined a `RuntimeContext` with an **implicit** part (`deployment_dir`, `home`) and an
operator-**declared** part: a `context { }` block binding fields to a canonical provider catalog
(`env "NAME"` / `env.secret "NAME"`), with `Secret<T>` taint rules. The current implementation exposes
only an implicit `Cx { project_name, region }` (with `deployment_dir` used author-side, never surfaced to
the `.tkd`); there is no declared-provider block. Because this is a genuinely desirable future
capability, it is **retained as a roadmap item** in `../requirements.md`, not buried here.

### Multi-file `use` import containment (moved to roadmap)

The bespoke design required a multi-file definition composed by fail-closed relative `use` (no `..`, no
absolute, depth ≤ 1, acyclic, path-sorted, symlink-canonicalised within the deployment root), with the
content digest taken over the sorted `(relative_path, sha256)` set. The current `.tkd` is a **single
file** — the interpreted subset has no `use` construct. Multi-file `.tkd` composition is **retained as a
roadmap item** in `../requirements.md`.

### On-disk layout (retired: `platform/` + `inputs.toml`)

The bespoke layout put the authored `.platform` set under a `platform/` containment subdirectory and
recorded operator value choices in a separate `inputs.toml` (with recorded identity). The current layout
is a single `definition.tkd` at the deployment root; operator values live in the `.tkd`'s `config()`, and
identity lives in `manifest.json`. See the authoritative layout section in `../design.md`.
