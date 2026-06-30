# Proposal 003 — The rust-via-`syn` Deployment Definition

- **Status:** Proposed (language decision + subset sketch; no code moved yet)
- **Companion to:** [001 — framework & realizer](./001-platform-framework-and-realizer.md) (HOW it compiles/realizes) · [002 — operator configuration surface](./002-operator-configuration-surface.md) (WHAT an operator sets)
- **Supersedes:** the bespoke `.tkd` grammar prototyped in `platforms/compose-dsl/platform/`. Every *semantic* decision from that prototype carries over; only the syntax host changes.
- **Scope:** the language a deployment definition is written in — for compose first, ECS next.
- **Audience:** whoever builds the `tkp` interpreter + builder vocabulary, and the platform authors / operators who write definitions.

## 1. The decision

A deployment definition is written in a **small subset of Rust**, parsed by the [`syn`](https://docs.rs/syn) crate, and **interpreted by `tkp` at runtime** into a `Composition`. It is **not compiled into the `tkp` binary** — it stays authored, persisted, digested data (the config revision of 001/002), which is exactly what lets an operator edit-and-`apply` without changing engine identity.

**Why rust-via-`syn`, after a bespoke DSL got most of the way:** the bespoke grammar worked, but every construct became a syntax debate (`create` vs `sealed`, `when` vs `if`) — a signal that the cost was *inventing a language* for needs that are genuinely minimal. Rust has already decided all of it:

| We were inventing | Rust already has |
|---|---|
| `when X is Dsql(d)` | `if let Storage::Dsql(d) = …` / `match` |
| `create <field>` | `#[create]` attribute |
| `++` / path-join | `format!` |
| sum type + payload binding | `enum` + pattern binding |
| `require … => …` | `#[require(…)]` attribute / a guard |
| `module` / `service` / `resource` | builder method calls + struct literals |

So we **stop designing syntax** and inherit Rust's — plus `syn` as a battle-tested parser (no lexer/grammar to maintain) and editor highlighting / `rustfmt` for free.

**Rejected alternatives.** `starlark-rust` (hermetic, no interpreter to build, but Python-flavoured and dynamically typed — loses the `enum`/`match` elegance that made the definition feel natural in the first place); Monty / full Python (the maximal-surface, dynamic version of the same idea). Compiled Rust (definitions as real crate code) is rejected by construction: editing a compiled definition is recompiling the binary — an engine-identity change, not an `apply`.

## 2. The capability map (the spine)

The language is a **typed projection of the engine's capability surface** — finite and complete over what the orchestration + engine can do, and nothing more. This table *is* the language; every later section elaborates one row.

| Engine / orchestration capability | rust-via-`syn` construct | Owner |
|---|---|---|
| Operator inputs (typed model + defaults) | `struct` / `enum` types + a default value literal | author/operator |
| Create-time-immutable vs editable input | `#[create]` field attribute | author/operator |
| Input constraints / validation | `#[require(<expr>)]` attribute | author/operator |
| Conditional inclusion | `if let` / `match` on the config enum | author/operator |
| Deployment identity | `cx.project_name` (the injected `Context`) | tkp / manifest |
| IaC module (resource grouping + deps) | `d.module(name, [needs…])` | author/operator |
| IaC resource (kind + lifecycle) | `.resource(name, Kind { … })` | author/operator |
| Resource output (from `InfraState`) | the `ResourceRef` a `.resource(…)` returns: `r.output("…")` | author/operator |
| Service workload (deploy engine) | `d.service(name, module, Service { … })` | author/operator |
| Image reference (resolve, not build) | `image: cfg.…` (a `String` ref) | author/operator |
| Namespaces | `Deployment::new([namespaces…])` | author/operator |
| Writeback (`collect_writeback`) | `d.writeback(|t: &mut TokeiraConfig| { … })` | **mechanic** |
| Ops (scale / logs / ports) | *derived* from the `service` set | free |
| Versioned engine identity, retention, retarget | — owned by `tkp` (interpreter + builder + kind library) | tkp |

**Ownership.** Nearly everything is **author/operator**: the author *seeds* the definition, the operator *owns it thereafter* and may evolve structure (add a service, rewire `needs`) — an ordinary `apply` (Req 16). The two exceptions sit on opposite sides: `writeback` is a **mechanic** (the engine's projection of discovered infra outputs into `TokeiraConfig`; editing it breaks the server-config bridge); identity and the **versioned engine** (the interpreter, the builder vocabulary, the kind library — all Rust *in* `tkp`) are **tkp's**, deliberately outside the definition. That outside-ness is precisely why operator edits are safe.

## 3. Anatomy of a definition

A definition is two halves, both real Rust:

**The config** — the operator surface (002), as `struct`/`enum` *types* plus a *default value*:

```rust
enum DsqlMode { Managed, Preexisting }

enum Storage {
    InMemory,
    Dsql { region: String, mode: DsqlMode, endpoint: Option<String>, arn: Option<String> },
}

struct Compose {
    #[create]                       // set once at create; editing it is a retarget tkp refuses
    storage: Storage,
    tokeirad: Tokeirad,
    observability: Observability,
}

// the defaults are a plain struct literal — this is what the operator edits
fn config() -> Compose {
    Compose {
        storage: Storage::InMemory,
        tokeirad: Tokeirad { image: "tokeirad:latest".into(), replicas: 1, grpc_port: 7233, metrics_port: 9090 },
        observability: observability_defaults(),
    }
}
```

The operator's config *is* the `config()` literal — overriding a default is editing a value (`storage: Storage::Dsql { region: "eu-west-2".into(), .. }`). `#[create]` fields are locked after create.

**The structure** — a single function `tkp` interprets, mapping config → engine primitives:

```rust
fn deployment(cfg: &Compose, cx: &Context) -> Deployment { … }
```

`cfg` is the resolved config; `cx` is the injected `Context` (`project_name`, `tokeira_config`). The body uses `if let`/`match`, `format!`, struct literals, and the builder vocabulary (§5). That is the whole surface.

## 4. The interpreted subset (the boundary)

`tkp` parses the file with `syn` (which accepts all of Rust), then **walks the AST and evaluates only the subset below, rejecting everything else**. Reject-by-default is the security model — the subset is an *allow-list*.

**Allowed:**
- **Items:** `struct` and `enum` definitions (the config schema); the `config()` and `deployment(...)` functions; small helper `fn`s that return values (e.g. `observability_defaults()`).
- **Attributes:** `#[create]`, `#[require(<expr>)]` (and a short fixed set TBD).
- **Expressions:** struct literals, enum construction, array/slice literals, field access, `let` bindings, `if`/`if let`/`match`, method calls *on the builder/handles only*, the `format!` macro, `&`/`.clone()`/`.into()` (or elided), comparison/boolean operators (for `match` guards and `#[require]`).
- **Patterns:** enum-variant binding (`Storage::Dsql(d)` / `Storage::Dsql { region, .. }`).

**Rejected (anything not above), notably:**
- arbitrary function *calls* (only builder/handle methods + `format!` + whitelisted helpers);
- loops (`for`/`while`), `unsafe`, `async`, closures except the `writeback` block, traits/`impl`/generics beyond the config types;
- any I/O, `std::fs`, environment, time, or randomness — **hermetic by enforcement** (Req 12.1: no filesystem reads, deterministic);
- macros other than the whitelisted few.

The result: `syn` gives us a free, correct front-end; the interpreter is small (a typed AST-walk over ~a dozen node kinds); and the definition is provably side-effect-free and deterministic — the properties a versioned, retargetable config revision needs.

## 5. The builder vocabulary

The `deployment(...)` body calls a fixed builder API — the *only* functions in scope. Each call is one row of the capability map; `tkp` records it into the `Composition` rather than executing anything.

```rust
Deployment::new([&str])                       -> Deployment      // namespaces
d.module(name: &str, needs: [&str])           -> ModuleRef       // an iac::Module + deps
  m.resource(name: &str, kind: Kind { … })    -> ResourceRef     // an iac::Resource (kind from the library)
d.service(name, module, Service { … })        -> ServiceRef      // a deploy-engine workload
d.writeback(|t: &mut TokeiraConfig| { … })                       // collect_writeback (mechanic)
r.output(name: &str)                          -> Output          // a typed reference into InfraState
```

- **Kinds** (`LocalStateDir`, `DsqlCluster`, `DynamoDbTable`, `Service`, …) come from the **kind library** — the versioned `tkp` surface (001). A definition can only name kinds the library exposes; the thin realizer maps each to an engine resource.
- **Outputs** are addressed through the **`ResourceRef` a `.resource()` returns** — `cluster.output("endpoint")` — not a magic `"dsql.cluster.endpoint"` string. This resolves the addressing gap noted during the prototype: the handle *is* the binding to the resource's `ResourceId`; the kind library declares which outputs exist and how they map to state properties (`endpoint → cluster_endpoint`).

## 6. Worked example — the full compose deployment

Faithful to `platforms/compose/src` (config.rs, compose.rs, modules.rs, lib.rs):

```rust
// ── Config (the operator surface, 002) ─────────────────────────────────────
enum DsqlMode { Managed, Preexisting }

enum Storage {
    InMemory,
    Dsql { region: String, mode: DsqlMode, endpoint: Option<String>, arn: Option<String> },
}

struct Tokeirad { image: String, replicas: u32, grpc_port: u16, metrics_port: u16 }
struct Grafana  { image: String, replicas: u32, port: u16, admin_password: String }
struct Backend  { image: String, replicas: u32 }              // mimir / loki / alloy
struct Observability { grafana: Grafana, mimir: Backend, loki: Backend, alloy: Backend }

struct Compose {
    #[create] storage: Storage,                              // create-time; editing = retarget
    tokeirad: Tokeirad,
    observability: Observability,
}

#[require(matches!(mode, DsqlMode::Preexisting).implies(endpoint.is_some()))] // preexisting needs endpoint
impl Storage {}                                              // (constraint attached to the Dsql variant)

fn config() -> Compose {
    Compose {
        storage: Storage::InMemory,                          // the default; flip to Dsql for persistence
        tokeirad: Tokeirad { image: "tokeirad:latest".into(), replicas: 1, grpc_port: 7233, metrics_port: 9090 },
        observability: Observability {
            grafana: Grafana { image: "grafana/grafana-oss:12.4.3".into(), replicas: 1, port: 3000, admin_password: "admin".into() },
            mimir:   Backend { image: "grafana/mimir:3.0.6".into(),  replicas: 1 },
            loki:    Backend { image: "grafana/loki:3.7.1".into(),   replicas: 1 },
            alloy:   Backend { image: "grafana/alloy:v1.16.0".into(), replicas: 1 },
        },
    }
}

// ── Structure (modules, services, writeback) ───────────────────────────────
fn deployment(cfg: &Compose, cx: &Context) -> Deployment {
    let mut d = Deployment::new(["default"]);                              // namespaces

    // bootstrap state
    d.module("local_state", []).resource("dir", LocalStateDir);

    // persistent storage — only under DSQL
    if let Storage::Dsql(dsql) = &cfg.storage {
        let m = d.module("dsql", ["local_state"]);
        let cluster = m.resource("cluster", DsqlCluster {
            identity: format!("{}-compose", cx.project_name),
            region:   dsql.region.clone(),
            mode:     dsql.mode,
            endpoint: dsql.endpoint.clone(),
            arn:      dsql.arn.clone(),
        });
        let rate_limiter = m.resource("rate_limiter", DynamoDbTable {
            table: format!("{}-dsql-rate-limiter", cx.project_name),
            hash_key: "pk", ttl: "ttl_epoch", billing: BillingMode::OnDemand,
        });
        let conn_lease = m.resource("conn_lease", DynamoDbTable {
            table: format!("{}-dsql-conn-lease", cx.project_name),
            hash_key: "pk", ttl: "ttl_epoch", billing: BillingMode::OnDemand,
        });

        // writeback (mechanic) — typed against TokeiraConfig; outputs via handles
        d.writeback(|t: &mut TokeiraConfig| {
            t.infrastructure.storage              = StorageKind::Dsql;
            t.infrastructure.dsql.endpoint        = cluster.output("endpoint");
            t.infrastructure.dsql.region          = dsql.region.clone();
            t.infrastructure.dsql.rate_limiter_table = rate_limiter.output("name");
            t.infrastructure.dsql.conn_lease_table   = conn_lease.output("name");
        });
    }

    // services — flat; `module` = membership (infra grouping), `needs` = deploy ordering
    let o = &cfg.observability;
    d.service("mimir", "observability", Service {
        image: o.mimir.image.clone(), replicas: o.mimir.replicas,
        publish: [9009], command: ["--config.file=/etc/mimir/mimir.yaml"], ..Service::EMPTY
    });
    d.service("loki", "observability", Service {
        image: o.loki.image.clone(), replicas: o.loki.replicas,
        publish: [3100], command: ["--config.file=/etc/loki/loki.yaml"], ..Service::EMPTY
    });
    d.service("tokeirad", "runtime", Service {
        image: cfg.tokeirad.image.clone(), replicas: cfg.tokeirad.replicas,
        publish: [cfg.tokeirad.grpc_port, cfg.tokeirad.metrics_port],
        consumes: [cx.tokeira_config], ..Service::EMPTY      // server config delivered by the Engine
    });
    d.service("grafana", "observability", Service {
        image: o.grafana.image.clone(), replicas: o.grafana.replicas, publish: [o.grafana.port],
        env: [("GF_SECURITY_ADMIN_USER", "admin".into()),
              ("GF_SECURITY_ADMIN_PASSWORD", o.grafana.admin_password.clone()),
              ("GF_METRICS_ENABLED", "true".into())],
        needs: ["mimir", "loki"], ..Service::EMPTY
    });
    d.service("alloy", "observability", Service {
        image: o.alloy.image.clone(), replicas: o.alloy.replicas, publish: [4317, 4318],
        command: ["run", "/etc/alloy/config.alloy"],
        needs: ["tokeirad", "mimir", "loki"], ..Service::EMPTY   // crosses modules — fine, deploy graph is flat
    });

    d
}
```

Every line is something a Rust developer reads at a glance, and the bespoke-DSL semantics survive intact: the storage sum type, create-time immutability, the conditional DSQL module, the flat service list with membership + deploy `needs`, and the typed writeback — now expressed in `enum`/`if let`/`format!`/struct-literals.

## 7. Lifecycle, ownership & engine identity

- **Edit-and-apply, no recompile.** The definition is data: `tkp` parses + interprets it each run. An operator edits the `.tkd`, runs `tkp apply`, and the *same versioned* `tkp` realizes the new desired state. Nothing is compiled into a binary.
- **`#[create]` enforces the lifecycle axis (002 §2).** On re-apply, `tkp` compares create-time fields against the recorded `inputs`/manifest; a changed `#[create]` field is a **retarget**, refused (not reconciled). Plain fields reconcile.
- **The versioned surface is the interpreter + builder vocabulary + kind library + realizer** — all Rust *in* `tkp`. Changing any of them is an engine-identity change (gated through `tkp upgrade`, per 001). The `.tkd` is *not* part of engine identity; it is the desired state the versioned engine consumes. This is the clean separation the whole effort was for: operator flexibility **through** a versioned provisioner.

## 8. File extension

Keep a **role-signalling extension** (`.tkd`), **not** `.rs`. The content is Rust *syntax*, but the file is an *interpreted definition bound to the versioned engine*, not crate source. Naming it `.rs` invites cargo + rust-analyzer to compile/analyse it and redline every line (the builder vocabulary and `#[create]` aren't a real compilable crate in the operator's tree). Rust tooling is still available where it helps — a one-line editor association (`"*.tkd": "rust"`) for highlighting, `rustfmt` on demand — without the compile-trap. The only cost is non-zero-config highlighting, which is cheap next to a repo full of redlined definitions.

## 9. Open design decisions

| Decision | Options | Lean |
|---|---|---|
| **Constraint syntax** (`#[require]`) | attribute on the type vs a checked expression vs a `validate()` fn | attribute, evaluated in the subset |
| **Defaults** | a `config()` literal (shown) vs `#[default(…)]` attributes vs `Default` impl | `config()` literal — most explicit, most editable |
| **`Service` field elision** | `..Service::EMPTY` spread vs all fields required vs `service()` builder methods | spread, so a service is only its non-defaults |
| **Output addressing** | handle `r.output("endpoint")` (shown) vs typed accessor `r.endpoint()` from the kind schema | start with `output(name)`; generate typed accessors from the kind library later |
| **Subset edges** | allow small helper `fn`s? `let` in `config()`? comprehension-free only | allow pure helper `fn`s + `let`; no loops/closures (except `writeback`) |
| **Where the interpreter lives** | replace `tokeira-platform-dsl` (the bespoke compiler) with a `syn`-based one; keep `tokeira-platform` (framework) and the kind library | yes — swap the front-end, keep the realizer/framework |

## 10. Relationship to 001 / 002

- **001** stands, with one substitution: the bespoke *compiler* (`tokeira-platform-dsl`: lex → parse → typeck → eval) becomes a **`syn`-based interpreter** producing the same `Composition`. The framework (`tokeira-platform`), the `Realizer`, the kind library, and `DslPlatform<R>` are unchanged — they consume a `Composition`, however it was produced.
- **002** stands entirely: it defines the *config types* this language declares (the `struct`/`enum` of §3/§6) and their lifecycle. 002's surface is the `Compose`/`Storage` types here.
- **003** is the missing top-down piece: the *language* those two were always implicitly assuming — now pinned to Rust-via-`syn`, with the subset as its only real new surface.

---

*002 says what an operator may set; 001 says how it's realized; 003 says what they write it in — a typed projection of the engine's capabilities, in the language the team already speaks.*
