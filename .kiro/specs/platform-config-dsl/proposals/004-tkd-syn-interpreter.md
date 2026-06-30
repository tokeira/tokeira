# Proposal 004 — The `tkd` `syn`-Interpreter for the Compose Deployment Definition

- **Status:** IMPLEMENTED (2026-06-30) — all phases built in `platforms/compose-syn`, fidelity proven three-way (interpreted `.tkd` == compiled `definition.rs` == engine `ComposeDeployment`), adversarially reviewed (11 correctness findings fixed + regression-locked), 54 tests green, clippy clean. Implements Proposal 003 for compose first. See §18 for the as-built notes.
- **Companion to:** 001 (framework/realizer), 002 (operator config surface), 003 (the rust-via-`syn` language decision + subset §4 + builder vocab §5 + worked example §6).
- **Scope:** the runtime interpreter (`tkp`) that turns the operator's compose deployment definition from *compiled Rust* (`platforms/compose-syn/src/definition.rs`) into an *interpreted* `.tkd` file (Rust syntax, parsed by `syn` 2.0, walked at runtime), producing a `builder::Deployment` **byte-identical** to today's compiled `deployment()` — re-proven by the fidelity harness against `tokeira_compose_deployment::ComposeDeployment`.

## 1. Overview

The interpreter is a small typed AST-walk over ~a dozen `syn` node kinds, living as a module tree **inside the existing `tokeira-compose-syn` crate** (`src/interp/`), not a sibling crate. It exposes one entry point:

```rust
pub fn interpret(src: &str, cx: &Cx) -> Result<(builder::Deployment, ConfigValue), Diagnostics>;
```

It produces a real `builder::Deployment` by calling **exactly the same author constructors** the compiled `deployment()` calls today, so the output is byte-identical by construction — then proven, not asserted, by an extended fidelity oracle.

Two intertwined problems define the work:

1. **Hermetic refactor (the crux).** The faithful `deployment()` today uses four non-hermetic mechanics forbidden by 003 §4: a `vol` closure over `cx.deployment_dir.join(...)`; `std::env::var("HOME")`; a `tokeirad.toml.exists()` probe; and an `AWS_*` `for` loop. These relocate **out of the `.tkd` and into author Rust** (the `Service` kind realizer + `Cx` helpers + a typed `Vol` vocabulary), supplied at realize-time. The `.tkd` becomes clean per 003; the manifests stay byte-identical because relocation *moves the identical expressions across the author/interpreter seam* rather than rewriting them. This is landed against the **compiled** definition first (Phase 1), so byte-identity is proven before any interpreter exists.

2. **Kind-registry / host-bridge.** The `.tkd` names two type categories: (a) **config types** defined *in* the `.tkd` (`Compose`/`Storage`/`DsqlMode`/`Tokeirad`/`Observability`/…) — pure data the interpreter models generically from the `.tkd`'s own struct/enum AST; (b) **author types** named but not defined in the `.tkd` (`Deployment`, `Service`, `DsqlCluster`, `DynamoDbTable`, `LocalStateDir`, `ObservabilityConfigFiles`, `ModuleRef`, `ResourceRef`, `Output`) — real Rust the interpreter bridges through a **name-keyed registry** of hand-written constructors and method shims (the reflection Rust lacks, written once per kind, contained to one file).

Four hardening grafts from review: a closed typed `HostObj` enum (no `Rc<dyn Any>`) so receiver dispatch is structural; an author-side `Deployment::resource_dyn(Box<dyn Kind>)` shim (the one bridge-correctness hole); an explicit no-panic eval invariant with a fuzz test; and an in-crate module-boundary rule (only `registry.rs` may `use crate::kinds`/`crate::builder`) so the future `tokeira-tkd` extraction for ECS stays mechanical.

## 2. The value model

One runtime enum (`interp/value.rs`) covers config values and the values crossing into the host registry. A **closed, tagged `HostObj`** replaces `Rc<dyn Any>` so receiver dispatch is structural and unknown-kind errors are impossible by construction:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Unit,
    Bool(bool),
    Int(i128),                       // u16 ports / u32 replicas / u64 share one ladder; range-checked at the host boundary
    Str(String),                     // String / &str / "x".into() / "x".to_string()
    Vec(Vec<Value>),                 // [..], &[..], vec![..]  (always — including empty)
    Tuple(Vec<Value>),               // ("K","V") env pairs, the DsqlMode flatten tuple
    Opt(Option<Box<Value>>),         // Some(x) / None
    // CONFIG types DEFINED in the .tkd — modelled generically from the .tkd's own struct/enum AST:
    Struct { ty: String, fields: BTreeMap<String, Value> },
    Enum   { path: EnumPath, variant: String, body: VariantBody },
    // AUTHOR types NAMED but not defined in the .tkd — opaque, never decomposed by the interpreter:
    Host(HostObj),
}

#[derive(Clone, Debug, PartialEq)]
pub enum VariantBody { Unit, Tuple(Vec<Value>), Struct(BTreeMap<String, Value>) }

/// The *path* the enum literal used, not just the leaf — disambiguates a config
/// `DsqlMode` from any future host enum sharing the leaf name (review #14).
#[derive(Clone, Debug, PartialEq)]
pub struct EnumPath { pub ty: String, pub segments: Vec<String> } // e.g. {ty:"DsqlMode", segments:["DsqlMode"]} or ["Storage","Dsql"]

#[derive(Clone)]
pub enum HostObj {                   // closed set — exactly the host types compose has
    Deployment(Rc<RefCell<builder::Deployment>>), // the one mutable cell; all d.* borrow it
    Module(builder::ModuleRef),
    Resource(builder::ResourceRef),
    Output(builder::Output),
    Kind(Rc<RefCell<Option<HostKindVal>>>),       // a constructed-but-unplaced kind; .take()-once
}

pub enum HostKindVal { Boxed(Box<dyn builder::Kind>), Service(kinds::Service) } // service is concrete-typed
```

`HostKind` is the discriminant tag (`Deployment`/`Module`/`Resource`/`Output`/`Kind`) used as the method-table key.

**`PartialEq` safety (review fix, issue #16).** `Value: PartialEq` is the same impl the `#[create]` retarget diff relies on. Host handles are **not value-comparable**: `HostObj`'s `PartialEq` is a `debug_assert!(false, "Host compared")` returning `false`, and the `#[create]`/`#[require]` diff entry asserts `debug_assert!(!value.contains_host())`. Config values being diffed are only `Struct`/`Enum`/scalars (host handles only ever appear in `deployment()`, never `config()`), so the assertion never fires in practice and tag-equality can never be silently wrong.

**The config-vs-author discriminator** is the whole dispatch spine: when the evaluator meets a struct-literal `Foo { .. }` or a bare path `Foo`, it asks "is `Foo` in the `.tkd`'s own collected `TypeTable`?" — yes ⇒ generic `Value::Struct`/`Value::Enum`; no ⇒ route to the kind registry, producing `Value::Host`.

This model generalizes to ECS with **zero new Value variants** — new kinds are new registry entries and new config structs (free).

## 3. The interpreter pipeline

`interpret(src, cx)` runs:

1. **PARSE** — `syn::parse_file(src) -> syn::File`. Scan `file.items`: collect `ItemStruct`/`ItemEnum` into a `TypeTable` (field/variant names + per-field `#[create]` flags + per-type `#[require]` clauses), and `ItemFn`s into a `FnTable` keyed by `sig.ident` (`config`, `deployment`, whitelisted pure helpers). Param binding names for `deployment` are read from `sig.inputs` (the `PatType.pat` idents), never hardcoded `cfg`/`cx`.
2. **VALIDATE** (subset reject pass) — `subset::check(&file, &registry, &type_table) -> Result<(), Diagnostics>`, reject-by-default allow-list (§6). Runs **before any evaluation**; method names are validated against the registry table here so an unknown method is a spanned reject, not a runtime panic. Aborts on any diagnostic.
3. **REGISTER TYPES** — TypeTable already built; nothing executes.
4. **EVAL `config()`** — `eval_fn("config", &[])` walks the body (optional pure `let`s + a tail `Compose { .. }` literal) to a `Value::Struct{ty:"Compose",..}` = the `ConfigValue` the operator edited. No host calls occur in `config()`.
5. **ADMISSION** — `#[create]` retarget check + `#[require]` constraint eval against the `ConfigValue` (§8). On failure, abort before `deployment()`.
6. **EVAL `deployment()`** — `eval_fn("deployment", &[cfg_value, cx_value])`. `cx_value` is a fixed `Value::Struct{ty:"Cx",..}` seeded from the real `Cx` exposing **only** `project_name` and `region` as readable fields (`deployment_dir` is *not* surfaced — the warts that used it moved author-side). Walk statements: `let mut d = Deployment::new(&["default"])` ⇒ `HostObj::Deployment(Rc<RefCell<..>>)`; `d.module/.resource/.service/.writeback` ⇒ method dispatch borrowing the cell; `if let Storage::Dsql{..}`, inner `match`, `format!`, `r.output(..)`. The tail `d` is unwrapped (`Rc::try_unwrap`) back to the real `builder::Deployment`.
7. **RETURN** the real `builder::Deployment` (+ `ConfigValue`), consumed by `realize_workloads()`/`namespaces()`/`writeback_entries()`/`realize_module()` exactly as the compiled path's output is.

Every constructor on the path is the same author code the compiled definition calls, so the object is byte-identical by construction — and proven by the extended oracle (§10).

## 4. The kind registry (host bridge)

`interp/registry.rs` is the **only** module permitted to `use crate::kinds`/`crate::builder`. Three name-keyed tables, built once by author Rust:

```rust
type FieldMap = BTreeMap<String, Value>;
type Ctor     = fn(FieldMap, &Cx) -> Result<HostObj, EvalError>;        // struct/unit literal -> Host(Kind)
type Defaults = fn() -> FieldMap;                                       // the interpreter image of `<Ty>::EMPTY`
type Method   = fn(&HostObj, Vec<Value>, &Cx) -> Result<Value, EvalError>;
type Assoc    = fn(Vec<Value>, &Cx) -> Result<HostObj, EvalError>;

pub struct Registry {
    kinds:    HashMap<&'static str, Ctor>,     // DsqlCluster, DynamoDbTable, LocalStateDir, ObservabilityConfigFiles, Service
    defaults: HashMap<&'static str, Defaults>, // Service (the ..Service::EMPTY image); others have no spread today
    methods:  HashMap<(HostKind, &'static str), Method>, // (Deployment, module|resource|service|writeback), (Resource, output)
    assoc:    HashMap<&'static str, Assoc>,    // "Deployment::new"
    method_names: HashSet<&'static str>,       // for subset check-time validation
}
```

### 4.1 Struct-literal ⇒ real kind (with spread, review #12/#13)

When eval meets `Service { image: .., ..Service::EMPTY }` for an **author** type, it:
1. starts from `registry.defaults(ty)()` — the interpreter-visible image of the EMPTY const (`{server_config: Bool(false), aws: Opt(None), publish: Vec([]), volumes: Vec([]), env: Vec([]), command: Vec([]), needs: Vec([])}`);
2. validates `rest: Some(path)` is **exactly** `<Ty>::EMPTY` (else subset reject);
3. overlays the explicitly-written `FieldValue`s (with shorthand handled, §4.4);
4. calls `registry.kinds(ty)(field_map, cx)`.

This makes the ctor **total** (every field present) and makes `..Service::EMPTY` a map-merge, not reflection — the reflection problem (`kinds::Service::EMPTY` is a real const the interpreter cannot read) is sidestepped by registering the defaults map.

### 4.2 The ctors (the per-kind reflection, hardened)

Each ctor unpacks by name into the real kind. Every ctor **consumes** the `FieldMap` and asserts it is empty at the end, so an unknown/misspelled `.tkd` field is a spanned reject *and* a stale ctor that forgot a field is caught by the leftover-key check — total-coverage enforcement without reflection (review #17):

```rust
fn ctor_dsql_cluster(mut f: FieldMap, _cx: &Cx) -> Result<HostObj, EvalError> {
    let region = take_str(&mut f, "region")?;
    // config DsqlMode crosses here (path-checked); the ctor flattens config->kind enum AND
    // lifts the Preexisting payload into the flat cluster fields (review #14/#15 — option (b)):
    let mode_v = take(&mut f, "mode")?;
    let (mode, endpoint, arn) = flatten_dsql_mode(mode_v)?;   // see §4.3
    let endpoint = take_opt_str_or(&mut f, "endpoint", endpoint)?; // explicit field wins if present
    let arn      = take_opt_str_or(&mut f, "arn", arn)?;
    f.expect_empty()?;                                        // leftover-key = unknown_field reject
    Ok(host_kind(kinds::DsqlCluster { region, mode, endpoint, arn }))
}
```

`ctor_local_state_dir` ignores an empty map (unit kind, §4.4). `ctor_observability_config_files` unpacks all nine fields with `take_str`/`take_u16`/`take_u32` then `expect_empty()`. `ctor_service` defaults every absent field via the defaults overlay, then unpacks.

### 4.3 Enum crossing + payload flatten — config-type alignment decided

**Decision (review #14/#15, option b): align the `.tkd` config `Storage`/`DsqlMode` to the 003 §6 flat shape.** Today's `definition.rs` nests `DsqlMode::Preexisting { endpoint, arn }`; 003 §6 carries `Storage::Dsql { region, mode, endpoint: Option<String>, arn: Option<String> }` with a *unit* `DsqlMode`. We adopt the 003 §6 shape:

```rust
enum DsqlMode { Managed, Preexisting }
enum Storage {
    InMemory,
    Dsql { region: String, mode: DsqlMode, endpoint: Option<String>, arn: Option<String> },
}
```

This is a **prerequisite of Phase 1** because the fidelity harness imports `definition::DsqlMode` and `definition::Storage` directly (fidelity.rs:17,93). With flat fields, the `.tkd` `match mode { .. }` tuple-flatten **disappears entirely** — no `Pat::Tuple`, no in-`.tkd` payload surgery. The `DsqlCluster` literal becomes `mode: dsql.mode, endpoint: dsql.endpoint.clone(), arn: dsql.arn.clone()`, and `flatten_dsql_mode` (host-side) maps the unit config variant to `kinds::DsqlMode`. Only **one** `DsqlMode` (the config one) ever exists as a Value, eliminating the leaf-name collision at the source.

`Value::Enum` is keyed by `EnumPath` (full path the literal used). `take_enum`/`flatten_dsql_mode` assert the expected `EnumPath.ty`, so a genuine future collision is a spanned reject, not a silent cross-map.

The structural-validity invariant 003 §6 wants ("preexisting needs endpoint") is now a `#[require]`, not a payload nest (§8); the live compose `.tkd` still ships with zero `#[require]`s because both modes realize identically and endpoint/arn are `Option` regardless — the require machinery is exercised by one synthetic fixture for ECS.

### 4.4 Path resolution precedence (review #9)

`Expr::Path` resolution order is **explicit and total**: (1) local binding in the current `Env`; (2) unit enum variant of a TypeTable enum; (3) `<Ty>::EMPTY` associated-const sentinel (consumed by the struct-spread, never evaluated standalone); (4) **zero-field author kind** (e.g. bare `LocalStateDir`) ⇒ `registry.kinds(name)(FieldMap::new(), cx)`. So `d.resource(&local_state, "dir", LocalStateDir)` routes the bare path to the kind registry with an empty map. Anything else is an unbound-path reject.

**Field-init shorthand (review #8).** In `ExprStruct` handling, a `FieldValue` with `colon_token.is_none()` is shorthand: synthesize the value by evaluating an `Expr::Path` of the member ident against the current `Env`.

### 4.5 The `Deployment::new` assoc (review #10)

Keyed exactly `"Deployment::new"`. Its body unwraps the transparent `&` then calls `as_str_vec` on `args[0]` (identical to `d.module`), accepting empty arrays:

```rust
assoc["Deployment::new"] = |args, _| {
    let ns = args[0].as_str_vec()?;                 // &["default"] -> Vec<&str>; &[] -> []
    Ok(HostObj::Deployment(Rc::new(RefCell::new(builder::Deployment::new(&ns)))))
};
```

## 5. The builder bridge

Each builder verb is one `Method` shim keyed `(HostKind::of(recv), method_name)`, downcasting the closed `HostObj` (no `Any`) and forwarding to the real `builder::Deployment`. Post-subset, the receiver kind is proven, so the `let HostObj::X = recv else { unreachable!() }` arms are the only sanctioned `unreachable!`s:

```rust
methods[(Deployment,"module")]   = |recv, args, _| {                 // d.module("dsql", &["local_state"])
    let HostObj::Deployment(d) = recv else { unreachable!() };
    let name  = args[0].as_str()?;
    let needs = args[1].as_str_vec()?;
    Ok(Value::Host(HostObj::Module(d.borrow_mut().module(name, &needs))))
};
methods[(Deployment,"resource")] = |recv, mut args, _| {             // d.resource(&m, "cluster", DsqlCluster{..})
    let HostObj::Deployment(d) = recv else { unreachable!() };
    let m    = args[0].as_host_module()?;                            // & is transparent
    let id   = args[1].as_str()?;
    let kind = args[2].take_host_boxed_kind()?;                      // Box<dyn Kind> moved out of the cell
    Ok(Value::Host(HostObj::Resource(d.borrow_mut().resource_dyn(&m, id, kind))))
};
methods[(Deployment,"service")]  = |recv, mut args, cx| {            // d.service(&m, "tokeirad", Service{..})
    let HostObj::Deployment(d) = recv else { unreachable!() };
    let m    = args[0].as_host_module()?;
    let name = args[1].as_str()?;
    let svc  = args[2].take_host_service()?;                         // kinds::Service moved out
    d.borrow_mut().service(&m, name, svc);                          // realize-time path-math lives in to_compose_service(name, cx)
    Ok(Value::Unit)
};
methods[(Resource,"output")]     = |recv, args, _| {                 // cluster.output("cluster_endpoint")
    let HostObj::Resource(r) = recv else { unreachable!() };
    Ok(Value::Host(HostObj::Output(r.output(args[0].as_str()?))))
};
methods[(Deployment,"writeback")] = /* §9 */;
```

**The one author-side change (the Arch-1 bridge-correctness graft):** `builder::Deployment::resource` is generic (`kind: impl Kind + 'static`), which `Box<dyn Kind>` cannot satisfy. Add:

```rust
pub fn resource_dyn(&mut self, module: &ModuleRef, id: &str, kind: Box<dyn Kind>) -> ResourceRef { /* existing body, pushes the already-boxed kind */ }
pub fn resource(&mut self, module: &ModuleRef, id: &str, kind: impl Kind + 'static) -> ResourceRef {
    self.resource_dyn(module, id, Box::new(kind))
}
```

`service` already takes a concrete `kinds::Service`, so the `Service` ctor builds the real value and the shim passes it straight through — no change. `&`/`.clone()`/`.into()`/`.to_string()` are recognized as identity during lowering, so `o.mimir.image.clone()` evaluates as a plain field-access chain. The interpreter never reflects over real structs; it only calls these six author shims.

**Kind handles are single-use-inline (review #16).** `ResourceRef`/`Output`/`ModuleRef` derive `Clone` and carry only strings — freely re-referenceable from the `Env`. But `HostObj::Kind` is `.take()`-once. To remove the move-vs-clone hazard, subset.rs **rejects binding an un-placed kind to a `let`**: a kind literal must appear directly as the `.resource()`/`.service()` argument, never via an intermediate binding. This matches every real usage in `definition.rs` (kinds are always inline args).

## 6. The interpreted subset (the reject pass)

`interp/subset.rs` — a hand-written reject-by-default match-walk over `syn` (the allow-list *is* the set of match arms; `_ => reject(span)` for the rest). Runs **before** eval; returns **all** violations as `Diagnostic{span, msg}` with `proc_macro2::Span`→line/col; a non-empty set aborts.

**Allowed items:** `ItemStruct`, `ItemEnum` (→ TypeTable); `ItemFn` named `config`/`deployment`/whitelisted pure helper; `ItemImpl` **only** as the `#[require]` carrier (no method bodies executed).

**Allowed attrs:** `#[create]`, `#[require(<expr>)]`, `#[derive(..)]` (ignored), doc comments — any other attr rejects.

**Allowed exprs** (~the dozen node kinds):
- `ExprStruct` (+ `rest` validated as `<Ty>::EMPTY` for author types);
- `ExprCall` **only** for enum-variant construction or the exact `Deployment::new` (a two-element allow-set);
- `ExprMethodCall` **only** where the method name ∈ `registry.method_names` ∪ `{into, to_string, clone, as_deref, is_some, is_none}` — any other method name is a **check-time** reject;
- `ExprMacro` **only** `format!`/`vec!`/`matches!`;
- `ExprField`, `ExprPath`, `ExprLit`, `ExprArray` (incl. empty), `ExprTuple` (review #7), `ExprReference`/`ExprParen`/`ExprGroup` (transparent);
- `ExprLet`/`Local` (with the un-placed-kind-binding rejection of §5);
- `ExprIf` (+ if-let via `Expr::Let` cond), `ExprMatch` (value-producing, §7);
- `ExprBinary`/`ExprUnary` (`== != && || ! < >` — only in `#[require]`/guards);
- `ExprClosure` **only** as the sole arg to `d.writeback(...)` (§9, Phase 6).

**Allowed patterns:** `PatStruct`/`PatTupleStruct` (variant binding, with `..` rest), `PatTuple` (review #6/#7), `PatIdent`, `PatWild`, `PatPath` (unit variant).

**Rejected and unit-tested-as-rejected:** `for`/`while`/`loop`, any non-writeback `Closure`, `unsafe`, `async`/`.await`, `?`, free `Call` not whitelisted, binding an un-placed kind to a `let`, any path/method touching `std::env`/`std::fs`/`std::path`/`.exists()`/`.join()`/`.display()`/`var`, macros other than the three.

**The no-panic security invariant.** `eval` returns `Result<Value, EvalError>` with **no** `unreachable!`/`panic!` on any operator-reachable path; the only `unreachable!`s are post-subset receiver-kind matches (proven by the check pass). A fuzz/property test feeds malformed `.tkd` snippets and asserts clean `Diagnostics`, never a panic.

**The sharp negative test.** Because the four warts moved out of the `.tkd`, none of `std::env`/`.exists()`/`.join()`/the `for` loop/the `vol` closure appears, so the reject pass is satisfiable with no exceptions. A test that `subset::check` **rejects the current non-hermetic `definition.rs`** (its `for`, `std::env::var`, `.exists()`, `.join()`, the `vol` closure) with the expected node-kind diagnostics proves the allow-list bites before the clean `.tkd` is written.

## 7. Tuples and value-producing match (review #6/#7/#11)

`Expr::Tuple` evaluates each element to `Value::Tuple(Vec<Value>)` — load-bearing for the grafana env (`vec![("GF_SECURITY_ADMIN_USER".into(), "admin".into()), ..]`). `Pat::Tuple` destructures `Value::Tuple` positionally in `eval_let_pattern`/`eval_match`, mirroring the Vec arms.

`eval_match` is **value-producing** (unified with if-let-with-else): select the first arm whose pattern matches the scrutinee Value, bind pattern idents into a child `Env`, evaluate that arm's body expr, and **return it as the match's Value**. This handles `aws: match &cfg.storage { Storage::Dsql { region, .. } => Some(region.clone()), _ => None }` in field position. (With §4.3's flat config shape, the `let (..) = match mode` tuple-match is gone, so this is the only match in the live `.tkd`; tuple destructuring is retained for the grafana env and for ECS readiness.)

## 8. `#[create]` and `#[require]`

Both are recorded during the parse/validate pre-pass into side-tables on the TypeTable, never as runtime Values:

```rust
struct FieldMeta { create: bool }
struct RequireClause { scope: String, expr: syn::Expr }
struct Schema { fields: HashMap<(String,String), FieldMeta>, requires: Vec<RequireClause> }
```

**`#[create]`** (enforced by `tkp` at apply-time, outside `deployment()`): after `config()` yields the `ConfigValue`, for each `create:true` field path (compose: `Compose::storage`) extract that sub-Value and compare via structural `Value: PartialEq` against the value recorded in the prior `manifest.json`/`inputs`. A mismatch ⇒ `EvalError::Retarget{field}` — refused, not reconciled (003 §7). Plain fields reconcile. The diff domain is config-only; `debug_assert!(!sub.contains_host())` guards the by-tag Host equality (review #16).

**`#[require(expr)]`** (evaluated immediately after `config()` resolves, before `deployment()`): the recorded `syn::Expr` is evaluated by the same `eval` over the `ConfigValue`, in an `Env` seeded with the scope's bound fields. The require subset adds `matches!(x, Pat)` (structural pattern test → `Bool`), `.is_some()`/`.is_none()` on `Value::Opt`, `== != && || !`, and `.implies(b)` desugared to `!a || b`. Result must be `Value::Bool(true)` or apply aborts with the require's source span. Both attributes are inert to `deployment()`; they gate config admission only.

## 9. Writeback

Writeback is a **mechanic, narrowly specialized for `TokeiraConfig`**: its sole job is projecting the DSQL *identity* — the adopted/created cluster `endpoint` + `region` and the two coordination DynamoDB table names — into the server config (`tokeirad.toml` / `TokeiraConfig`), so `tokeirad` connects to the right cluster and tables. The motivating case is **operator adoption of a pre-existing DSQL cluster at create**: `Storage::Dsql { mode: Preexisting, endpoint, arn, .. }` hands in the cluster identity, and writeback is how that (and the two coordination tables) reaches the server config. It is **not** a general-purpose config-editing facility.

**Shipped form — the original compose-platform `collect_writeback` mirror (retained by decision).** `d.writeback("infrastructure.storage", "dsql")` / `d.writeback("infrastructure.dsql.endpoint", cluster.output("cluster_endpoint"))` dispatch through `methods[(Deployment,"writeback")]` — arg2 is `Str → WbValue::Const` or `HostObj::Output → WbValue::Output`. This is byte-for-byte the engine's `ComposeDeployment::collect_writeback` (5 dotted keys; the `region` writeback sources from the same `Storage::Dsql.region` binding as the AWS edge, so they can never disagree — review #5). It is the fidelity reference, kept as-is by decision.

**Deferred: strong typing (the `TokeiraConfig`-typed closure).** The shipped dotted-key form *hardcodes the server-config schema as magic strings* — the very flaw flagged against the original `compose.platform`. The proper form is `d.writeback(|t: &mut TokeiraConfig| { t.infrastructure.dsql.endpoint = cluster.output("…"); .. })`, where the LHS are **typed paths into `TokeiraConfig`**, not strings. The interpreter would accept this single `ExprClosure` as a structural special-form (param type ends `TokeiraConfig`, body = `t.<path> = <rhs>` assignments), lower each typed path to its dotted key, evaluate the RHS to a `WbValue`, and call `Deployment::writeback(key, wb)` — same `Vec<(String, WbValue)>`, same fidelity. **Deferred by decision** (retain the compose-platform form now; address strong typing later); not a convenience — it is the correctness fix for the schema-hardcoding.

## 10. The hermetic refactor (the crux)

All four warts move from the operator body into author Rust; the `.tkd` services become clean; manifests stay byte-identical because relocation **moves the identical expressions** across the seam.

**New author `Vol` vocabulary (builder.rs)** — logical anchors, no host paths:

```rust
pub enum Vol {
    State  { sub: String, at: String },   // -> <dir>/.tokeira-state/<sub>:<at>
    Config { sub: String, at: String },   // -> <dir>/config/<sub>:<at>
    Raw(String),                          // vetted constant only (docker.sock)
}
```

**New on `Cx` (context.rs):** `state_dir()` = `deployment_dir.join(".tokeira-state")`, `config_dir()` = `deployment_dir.join("config")` (the only surviving `deployment_dir.join`); `state(sub, at) -> Vol::State`, `config(sub, at) -> Vol::Config`, and a vetted `docker_sock() -> Vol::Raw("/var/run/docker.sock:/var/run/docker.sock")` (preferred over a generic `host(raw)` so the operator surface stays fully path-free).

**`kinds::Service` changes:** `volumes: Vec<String>` → `Vec<Vol>`; add `server_config: bool` and `aws: Option<String>` (the region); add a `Service::EMPTY` associated const for `..Service::EMPTY` elision (its interpreter image is the `defaults` map of §4.1); `to_compose_service(&self, name)` → `to_compose_service(&self, name, cx: &Cx)`, now the **sole owner** of host-path joins, the toml mount, and the AWS edge. `builder.rs::realize_module` already has `cx`; `realize_workloads` gains a `&Cx` param (threaded; fidelity passes `playground_cx`).

**Volume + env build order in `to_compose_service` (review #2/#4) — pinned exactly:**

```rust
let mut volumes: Vec<String> = self.volumes.iter().map(|v| realize_vol(v, cx)).collect(); // base FIRST
let mut env: HashMap<String,String> = self.env.iter().cloned().collect();                 // base FIRST
// WART 2 — tokeirad.toml mount (server_config), BEFORE aws (matches compose.rs L56):
if self.server_config {
    let toml = cx.deployment_dir.join("tokeirad.toml");
    if toml.exists() {
        volumes.push(format!("{}:/etc/tokeira/tokeirad.toml:ro", toml.display()));
        env.insert("TOKEIRA_CONFIG".into(), "/etc/tokeira/tokeirad.toml".into());
    }
}
// WART 3 — DSQL AWS edge, AFTER server_config (matches compose.rs L65):
if let Some(region) = &self.aws {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/root".into());
    volumes.push(format!("{home}/.aws:/home/nonroot/.aws:ro"));
    env.insert("HOME".into(), "/home/nonroot".into());
    env.insert("AWS_REGION".into(), region.clone());
    for key in ["AWS_PROFILE","AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY","AWS_SESSION_TOKEN","AWS_ROLE_ARN"] {
        if let Ok(v) = std::env::var(key) { env.insert(key.into(), v); }
    }
}
```

- **WART 1 — volume math** is `realize_vol`: `Vol::State{sub,at} => format!("{}:{}", cx.state_dir().join(sub).display(), at)`, `Vol::Config{..}` likewise, `Vol::Raw(s) => s.clone()` — byte-for-byte the old `vol(state_dir.join("mimir"),"/data")`.
- **Base-before-edge order is load-bearing** (`to_manifest` serializes `volumes` positionally): base `.tkd` volumes first, then `server_config`, then `aws`. Today tokeirad's base is empty so order is `[toml?, aws?]` — correct. Locked by a positional fidelity assertion on the **full** tokeirad `volumes` Vec (base + toml + aws), plus a test with a non-empty tokeirad base volume to prove base-before-edge ordering.
- **Env merge rule (review #4):** base `self.env` collected first, then realizer `.insert`s (`TOKEIRA_CONFIG`/`HOME`/`AWS_REGION`/`AWS_*`) so realizer values override — matching the engine where only the realizer writes these keys. A guard/test asserts no `.tkd` service sets a reserved key (`HOME`/`AWS_*`/`TOKEIRA_CONFIG`).

**Clean `.tkd` tokeirad:**

```rust
let runtime = d.module("runtime", &["local_state"]);
d.service(&runtime, "tokeirad", Service {
    image: cfg.tokeirad.image.clone(), replicas: cfg.tokeirad.replicas,
    publish: vec![cfg.tokeirad.grpc_port, cfg.tokeirad.metrics_port],
    server_config: true,
    aws: match &cfg.storage { Storage::Dsql { region, .. } => Some(region.clone()), _ => None },
    ..Service::EMPTY
});
```

**Clean `.tkd` mimir** (representative observability service):

```rust
d.service(&observability, "mimir", Service {
    image: o.mimir.image.clone(), replicas: o.mimir.replicas, publish: vec![9009],
    volumes: vec![ cx.state("mimir","/data"),
                   cx.config("mimir.yaml","/etc/mimir/mimir.yaml"),
                   cx.config("mimir/rules","/data/mimir/rules") ],
    command: vec!["--config.file=/etc/mimir/mimir.yaml".into()],
    ..Service::EMPTY
});
```

alloy's docker.sock becomes `cx.docker_sock()`. The `.tkd` now contains **zero** `PathBuf`/`.join`/`.display`, **zero** `std::env`, **zero** `.exists()`, **zero** `for` — only struct literals, `vec!`, `cx.*` calls, one `match` on `Storage`, and the writeback.

**Region channels — documented, not unified (review #3).** Three region sources feed the manifest and the design states this explicitly: (a) `AWS_REGION` (workload env) flows from `Storage::Dsql.region`; (b) the DSQL/DynamoDB **resource** regions flow from `Cx.region` (`kinds.rs:89` `cx.region.as_deref().unwrap_or("us-east-1")`); (c) the writeback region Const flows from the same `Storage::Dsql.region` binding as (a). Fidelity holds only when `manifest.json`'s recorded region == the `.tkd` `Storage::Dsql.region` == the engine `dsql.region` == `Cx.region`. To prevent the silent `unwrap_or("us-east-1")` fallback diverging, **thread the single region from `Storage::Dsql` into `Cx.region` at `deployment()` entry is NOT possible (cx is injected, read-only)** — instead, a fidelity case with a **non-`us-east-1`** region is added to expose the coupling, and the divergence is documented as an operator invariant (the `.tkd` region and the recorded `Cx.region` must agree, which `tkp` guarantees because both derive from the same recorded inputs).

**Hermeticity is a property of `.tkd` authoring, not of the realized manifest.** The AWS edge genuinely reads live process env at realize via the sanctioned `Cx`/realizer boundary, so the realized DSQL manifest is not hermetic at realize-time. This is acceptable (the realizer, like `compose_services`, is versioned author Rust) and documented; no consumer expects a deterministic realized artifact under DSQL.

## 11. File layout

A module tree **inside** `tokeira-compose-syn` (not a sibling crate yet — compose-first; the registry references `crate::builder`/`crate::kinds` directly; fidelity stays a one-command gate). **Module-boundary discipline:** only `registry.rs` may `use crate::kinds`/`crate::builder`.

- `src/interp/mod.rs` — `pub fn interpret(src, cx) -> Result<(builder::Deployment, ConfigValue), Diagnostics>`; orchestration (parse → validate → eval config → admission → eval deployment).
- `src/interp/value.rs` — `Value`, `VariantBody`, `EnumPath`, `HostObj`, `HostKind`, `HostKindVal`; unpack helpers (`take_str`/`take_enum`/`take_opt_str`/`take_vec_str`/`take_vec_u16`/`take_u16`/`take_u32`/`as_str`/`as_str_vec`/`as_host_module`/`take_host_boxed_kind`/`take_host_service`/`expect_empty`).
- `src/interp/subset.rs` — the reject-by-default allow-list pass + `Diagnostic`/`Diagnostics`.
- `src/interp/schema.rs` — `TypeTable` + `FnTable` + `#[create]`/`#[require]` `Schema` extraction.
- `src/interp/eval.rs` — the AST-walk: `eval_fn`/`eval_expr`/`eval_stmt`/`eval_let_pattern`/`eval_match` + `format!`/`vec!`/`matches!` handlers + the no-panic invariant.
- `src/interp/registry.rs` — kind ctors + defaults + method/assoc shims (the host bridge; the only `use crate::kinds`/`crate::builder`).
- `src/interp/require.rs` — `#[create]` retarget diff + `#[require]` boolean sub-evaluator.
- `platforms/compose-syn/definition.tkd` — the operator definition as data (role-signalling `.tkd` per 003 §8), sibling to `src/definition.rs`. The compiled `definition.rs` stays as the differential oracle during bring-up, then is deleted in Phase 6.
- `platforms/compose-syn/tests/fidelity_interp.rs` — mirrors `tests/fidelity.rs` but sources the Deployment from `interpret(include_str!("../definition.tkd"), &cx)`.

Author-surface edits (compiled Rust, engine-identity): `src/builder.rs` (`Vol`, `resource_dyn`, `realize_workloads(&Cx)`), `src/kinds.rs` (`Service.volumes: Vec<Vol>`, `server_config`, `aws`, `Service::EMPTY`, `to_compose_service(name, cx)`), `src/context.rs` (`state_dir`/`config_dir`/`state`/`config`/`docker_sock`), `src/lib.rs` (`pub mod interp;`), `src/definition.rs` (the §4.3 flat `Storage`/`DsqlMode` shape, decided before Phase 1).

## 12. Dependencies

Verified: `Cargo.lock` already resolves `syn 2.0.117` and `proc-macro2 1.0.106` transitively (via a proc-macro dep, not linkable from our own code), so adding them as **direct** deps reuses the locked versions with no new fetch/version bump. Hand-edit `platforms/compose-syn/Cargo.toml` `[dependencies]` (do **not** `cargo add`/`remove` — workspace-dep prune gotcha):

```toml
syn = { version = "2.0", features = ["full", "extra-traits"] }
proc-macro2 = "1.0"
```

`full` is **mandatory** (default features are derive-input-only and lack `ItemFn`/`Block`/`Stmt`/`Expr`-statement nodes, so `parse_file` would not yield usable `config()`/`deployment()` bodies). `extra-traits` gives `Debug`/`PartialEq` on syn nodes for tests (droppable later). `quote` is **not** needed — we parse + walk, never emit tokens. `proc-macro2` is needed directly for `Span` (diagnostics) and `syn::parse2` (re-parsing `#[require]`/macro token streams). Crate-local pin is sufficient; optionally promote to root `[workspace.dependencies]` when `tokeira-tkd` is extracted for ECS.

## 13. Fidelity strategy (extended oracle — review #1)

The existing `tests/fidelity.rs` compares only `shape(d.realize_workloads())` (the 5 ComposeService workloads), `d.namespaces()`, and writeback keys/Const values. It never compares the **infra-resource** manifests (`DsqlCluster`, `DynamoDbTable`, `ObservabilityConfigFiles`, `LocalStateDir`) — yet the registry ctors construct exactly those. A field transposed in `ctor_dsql_cluster` or a mis-mapped `DsqlMode` would pass green. So the oracle **must** be extended; "byte-identical by construction" is otherwise unproven for kinds.

**Constraint discovered in ground truth:** `iac::Resource` exposes `resource_type()`, `resource_id()`, `dependencies()`, `module()` **synchronously**, but `ResourceState.properties` is only produced by **async** `describe()`/`create()` — and `create()` does filesystem I/O (LocalStateDir) or AWS calls (DSQL), not viable in a unit test. So the extended oracle compares the **synchronously-projectable identity** for every module, which catches the high-risk ctor errors (wrong kind, wrong id, wrong module, wrong deps) without materializing live state:

```rust
type ResourceShape = BTreeSet<(String /*resource_id*/, String /*resource_type*/, String /*module*/, Vec<String> /*deps*/)>;
```

The oracle asserts, for **InMemory and DSQL**:
- `shape(interpret(..).realize_workloads(&cx)) == shape(ComposeDeployment.services(&ref))` (the existing workload check);
- `interpret(..).namespaces() == ComposeDeployment.required_namespaces(&ref)`;
- for every module, the `ResourceShape` of `interpret(..).realize_module(name, &cx)` equals the `ResourceShape` of the matching `ComposeDeployment.infra_modules(&ref, All)` module's `resources(&ModuleContext)` — covering `DsqlCluster`/`DynamoDbTable`/`ObservabilityConfigFiles`/`LocalStateDir` resource_type+id+module+deps;
- the DSQL writeback (5 keys in order, 2 Const values matching `collect_writeback`).

Where `properties` parity is genuinely needed (e.g. `ObservabilityConfigFiles`' nine fields are the highest-risk surface and only surface in `properties`), a **per-kind round-trip unit test** (review #17) builds the kind from a fully-populated `FieldMap` and asserts its constructed `kinds::ObservabilityConfigFiles`/`DsqlCluster`/… equals the compiled-literal construction field-for-field — localizing a field regression to one test rather than the whole deployment. These per-kind tests are the property-coverage backstop the synchronous shape-oracle cannot reach.

**Two stages isolate byte-identity proof from interpreter risk:**

1. **Phase 1** lands the hermetic refactor against the **compiled** `definition.rs` and re-greens the existing `fidelity.rs` (only the `to_compose_service(name)`→`(name, cx)` and `realize_workloads(&cx)` call-sites change), plus the full positional tokeirad-volumes assertion and a `server_config`/toml-present test (the harness runs in a tempdir with no toml, so the present=true branch needs its own test). This proves the four warts relocated faithfully **before any interpreter exists**.
2. **Phase 5** adds `fidelity_interp.rs` mirroring `fidelity.rs` exactly (same `shape()` projection, same `reference_config`/`playground_cx` anchored to one tempdir so `deployment_dir` is equal on both sides, same `ComposeDeployment` reference) but sources the Deployment from `interpret(include_str!("../definition.tkd"), &cx)`, **plus the extended resource-shape oracle above**. Result: compiled `definition.rs` == interpreted `definition.tkd` == engine `ComposeDeployment` — a three-way lock.

After Phase 6 deletes the compiled `definition.rs`, `fidelity_interp.rs` is the sole standing fidelity gate.

## 14. Residual caveats (documented, accepted)

- **Unconditional writeback.** The `.tkd` emits the full writeback key set unconditionally inside the Dsql branch (matching the state-prepopulated fidelity test), but the engine's `collect_writeback` pushes `rate_limiter_table`/`conn_lease_table` only when `InfraState` has the property. A future no-state apply path could diverge; we keep the unconditional emission (matches the test today) and document it. Modeling the conditional is deferred until a no-state apply path exists.
- **Realize-time non-hermeticity** under DSQL (the AWS edge reads live env) — accepted; `Cx`/realizer is the sanctioned boundary.
- **ECS readiness.** The `Vol` State/Config/Raw model and `server_config`/`aws` flags are compose-bind-mount-shaped; the `Cx` helper set will be platform-scoped (compose `Cx` vs ecs `Cx`) when `tokeira-tkd` is extracted, so the Value model is not hardcoded to bind-mount semantics. New kinds/config-types are new registry entries and structs — zero new Value variants.


## 15. Build order (the phased plan)
0. Phase 0 — syn/proc-macro2 direct deps (hand-edit Cargo.toml) + empty interp/{mod,value,subset,schema,eval,registry,require}.rs + `pub mod interp;`. VERIFY: `cargo test -p tokeira-compose-syn` builds with syn linked; a smoke test parses a 3-line fixture via syn::parse_file; the SHARP NEGATIVE test asserts subset::check on the CURRENT non-hermetic definition.rs returns diagnostics naming the for-loop, std::env::var, .exists(), .join(), and the vol closure with their spans.
1. Phase 1 — hermetic refactor of the COMPILED definition (no interpreter). Decide+apply the §4.3 flat Storage/DsqlMode shape FIRST (fidelity.rs imports these). Add Vol + resource_dyn + realize_workloads(&Cx) (builder.rs); Service.volumes: Vec<Vol> + server_config + aws + Service::EMPTY + to_compose_service(name, cx) with the pinned base→toml→aws order (kinds.rs); state_dir/config_dir/state/config/docker_sock (context.rs). Rewrite definition.rs to the clean 003 form. VERIFY: existing fidelity.rs (in_memory+dsql+writeback) byte-identical, PLUS a positional assertion on the FULL tokeirad volumes Vec, PLUS a server_config/toml-present test, PLUS a non-empty-base-volume ordering test, PLUS a non-us-east-1 region case.
2. Phase 2 — Value model + unpack helpers + registry shims. value.rs (Value/EnumPath/HostObj/HostKindVal + take_* with range-checked narrowing + expect_empty); registry.rs (5 kind ctors with leftover-key reject, Service defaults map, Deployment::new assoc, the 6 method shims over closed HostObj using resource_dyn). VERIFY: per-kind round-trip unit tests (build kind from full FieldMap == compiled-literal construction, field-for-field, incl. the 9-field ObservabilityConfigFiles); ..Service::EMPTY merge test (only image/replicas set == compiled Service{..Service::EMPTY}); empty-array fixture for Deployment::new/&[]; wrong-kind/wrong-arg returns EvalError not panic; resource shim round-trips Box<dyn Kind> through resource_dyn.
3. Phase 3 — subset validate pass (full allow-list). subset.rs: TypeTable/FnTable/Schema extraction + the reject-by-default walk with spanned Diagnostics; method names validated against registry.method_names at check time; reject un-placed-kind let-binding. VERIFY: clean definition.tkd (copied verbatim from cleaned definition.rs body) passes with zero diagnostics; targeted reject fixtures (for-loop, non-writeback closure, std::env::var, .exists(), unknown macro, unknown method name, free call, un-placed-kind let) each assert a reject with the right span; fuzz/property test feeds malformed token input and asserts clean diagnostics, never a panic.
4. Phase 4 — evaluator for config() + #[create]/#[require] admission. eval.rs config path (struct/enum/tuple/format!/vec!/.into-identity/field-access/shorthand) + require.rs. interpret evaluates config() to ConfigValue and runs admission. VERIFY: tests/interp_config.rs asserts interpreted config() Value structurally equals compiled definition::config() projected to Value (InMemory default + Dsql-edited variant); #[create] storage change vs recorded manifest yields Retarget; synthetic #[require] fixture (matches!/.is_some/implies) passes and fails as expected; a replicas edit reconciles; debug_assert no-Host-in-diff holds.
5. Phase 5 — evaluator for deployment() + EXPLICIT writeback + extended fidelity oracle. eval.rs deployment path: let-bindings, d.* dispatch, if-let Storage::Dsql, value-producing match (aws: match ...), format!, r.output(..), explicit two-arg d.writeback. interpret returns the real builder::Deployment. VERIFY: tests/fidelity_interp.rs mirrors fidelity.rs sourcing from interpret(include_str!("../definition.tkd"), &cx) — workload shape + namespaces + writeback keys/Const byte-identical to ComposeDeployment for InMemory AND DSQL, PLUS the per-module ResourceShape (id+type+module+deps) oracle over realize_module vs infra_modules for both storage modes.
6. Phase 6 — writeback closure special-form + retire compiled definition. Add the |t: &mut TokeiraConfig| {..} desugaring (the TokeiraConfig field-path→dotted-key table) accepted by methods[(Deployment,writeback)]; switch definition.tkd's writeback to the closure form. Delete src/definition.rs once definition.tkd is sole source. Enforce the module-boundary rule. VERIFY: fidelity_interp.rs re-runs keys/values green with the closure form; full `cargo test -p tokeira-compose-syn` + `cargo clippy` clean; a grep/CI check confirms no interp module except registry.rs names crate::kinds/crate::builder.

## 16. Phase 1 — start here (verbatim)

PHASE 1 is where an engineer starts (Phase 0 is trivial dep-scaffold). Phase 1 lands the hermetic refactor against the COMPILED definition.rs with NO interpreter — it proves byte-identity before any new machinery exists. Order of operations:

PRE-STEP (decided, do it first): Change the config types in /Users/iw/Projects/tokeira/tokeira/platforms/compose-syn/src/definition.rs to the flat 003 §6 shape, because fidelity.rs:17,93 import `definition::DsqlMode` and `definition::Storage` directly and the flatten must leave the .tkd surface:
  enum DsqlMode { Managed, Preexisting }
  enum Storage { InMemory, Dsql { region: String, mode: DsqlMode, endpoint: Option<String>, arn: Option<String> } }
Update fidelity.rs:93 `Storage::Dsql { region: \"us-east-1\".into(), mode: DsqlMode::Managed }` to include `endpoint: None, arn: None`.

FILE 1 — src/context.rs: add methods on Cx:
  pub fn state_dir(&self) -> PathBuf { self.deployment_dir.join(\".tokeira-state\") }
  pub fn config_dir(&self) -> PathBuf { self.deployment_dir.join(\"config\") }
  pub fn state(&self, sub: &str, at: &str) -> Vol { Vol::State { sub: sub.into(), at: at.into() } }
  pub fn config(&self, sub: &str, at: &str) -> Vol { Vol::Config { sub: sub.into(), at: at.into() } }
  pub fn docker_sock(&self) -> Vol { Vol::Raw(\"/var/run/docker.sock:/var/run/docker.sock\".into()) }
(import builder::Vol.)

FILE 2 — src/builder.rs: add `pub enum Vol { State { sub: String, at: String }, Config { sub: String, at: String }, Raw(String) }`. Split resource(): add `pub fn resource_dyn(&mut self, module: &ModuleRef, id: &str, kind: Box<dyn Kind>) -> ResourceRef` containing the existing body but pushing the already-boxed kind; make `resource()` delegate `self.resource_dyn(module, id, Box::new(kind))`. Change `realize_workloads(&self)` -> `realize_workloads(&self, cx: &Cx)` and pass cx into `s.svc.to_compose_service(&s.name, cx)` (line 212 and 222 both gain cx).

FILE 3 — src/kinds.rs: change Service.volumes to `Vec<Vol>`; add `pub server_config: bool` and `pub aws: Option<String>`; add `impl Service { pub const EMPTY: Service = Service { image: String::new(), replicas: 0, publish: Vec::new(), volumes: Vec::new(), env: Vec::new(), command: Vec::new(), needs: Vec::new(), server_config: false, aws: None }; }` (note: const requires the String::new()/Vec::new() const-fns — fine). Change `to_compose_service(&self, name: &str)` -> `to_compose_service(&self, name: &str, cx: &Cx)` and make it the SOLE owner of path math + toml + aws, exactly per the design §10 snippet: build volumes from self.volumes via realize_vol(cx) FIRST, then the server_config push, then the aws push; build env from self.env FIRST then realizer .insert overrides. Add a private `fn realize_vol(v: &Vol, cx: &Cx) -> String`.

FILE 4 — src/definition.rs deployment(): delete the `state_dir`/`config_dir`/`vol` lets (lines 97-99), the whole tokeirad volumes/env/toml/for block (241-264). Each service's volumes become cx.state(..)/cx.config(..)/cx.docker_sock() calls; tokeirad gets `server_config: true, aws: match &cfg.storage { Storage::Dsql { region, .. } => Some(region.clone()), _ => None }, ..Service::EMPTY`. Use `..Service::EMPTY` to elide empty Vecs. The DsqlCluster literal becomes `DsqlCluster { region: region.clone(), mode: kind_mode_from(mode), endpoint: endpoint.clone(), arn: arn.clone() }` sourced from the flat Storage::Dsql fields (no tuple match).

FIRST TEST TO MAKE GREEN: the existing /Users/iw/Projects/tokeira/tokeira/platforms/compose-syn/tests/fidelity.rs — update its two call sites (`d.realize_workloads()` -> `d.realize_workloads(&playground_cx(dir))`; the Storage::Dsql literal gains endpoint/arn). Run `cargo test -p tokeira-compose-syn`. `in_memory_services_match_compose_deployment` and `dsql_services_match_compose_deployment` and `dsql_writeback_keys_match_compose_deployment` must stay byte-identical to ComposeDeployment. THEN add the new positional assertion: in dsql_services test, assert the tokeirad service's manifest `volumes` array equals the engine tokeirad's volumes array element-for-element (not a set) — this locks base→toml→aws order. THEN add a toml-present test: create `<dir>/tokeirad.toml`, build the deployment, assert the toml mount string and TOKEIRA_CONFIG env appear (the existing harness tempdir has no toml so this branch is otherwise untested). THEN add a non-empty-base-volume ordering test and a non-us-east-1 region case per §10/§13.

## 17. Residual risks (accepted / tracked)

- Region channel divergence is documented, not unified: AWS_REGION env (Storage::Dsql.region), DSQL/DynamoDB resource region (Cx.region with unwrap_or("us-east-1") fallback in kinds.rs:89), and writeback region Const (Storage::Dsql.region) are three sources. Fidelity holds only because all are pinned equal. A non-us-east-1 fidelity case exposes the coupling, but tkp must guarantee the recorded Cx.region and the .tkd Storage::Dsql.region agree (both derive from recorded inputs) — if a future apply path lets them drift, the DynamoDB resource region silently falls back to us-east-1.
- The extended fidelity oracle compares synchronously-projectable resource identity (id+type+module+deps) but NOT ResourceState.properties for DsqlCluster/DynamoDbTable/ObservabilityConfigFiles, because properties require async describe()/create() which does I/O. Per-kind round-trip unit tests backstop the 9-field ObservabilityConfigFiles and DsqlCluster property surface, but a field-value regression in a kind whose properties are never unit-tested could pass the deployment-level oracle. Mitigation: the per-kind tests must cover every kind, not just the deployment.
- Unconditional writeback emission inside the Dsql branch matches the state-prepopulated fidelity test, but diverges from the engine's conditional collect_writeback (which pushes rate_limiter/conn_lease only when InfraState has the property). A real no-state apply path would diverge; deferred until that path exists.
- ..Service::EMPTY relies on kinds::Service::EMPTY being a const fn-constructible associated const AND its interpreter-image defaults map in registry.rs staying in lockstep with the real const. Adding a Service field requires updating three places (the struct, the EMPTY const, the defaults map) or the leftover-key/expect_empty assertion fires — contained to one file but not compile-checked across the const↔map seam.
- Realize-time non-hermeticity under DSQL: the relocated to_compose_service still reads live std::env (HOME, AWS_*) at realize. .tkd authoring is hermetic; the realized DSQL manifest is not. Accepted as the sanctioned Cx/realizer boundary, but any consumer expecting a deterministic realized artifact under DSQL would be surprised.
- Writeback strong-typing is DEFERRED by decision: the shipped form mirrors the original compose-platform `collect_writeback` (dotted-key strings that hardcode the server-config schema — the flaw originally flagged). The eventual `TokeiraConfig`-typed closure is the correctness fix (typed LHS, no magic strings), not a convenience; until then the dotted keys are the fidelity reference and the schema coupling is implicit. See §9.

## 18. As built (2026-06-30)

Implemented in `platforms/compose-syn` (`src/interp/`): `value.rs` (Value/HostObj/EnumPath + FieldMapExt), `registry.rs` (5 kind ctors, Service `..EMPTY` defaults, closed method/assoc dispatch, `cx` field/method accessors), `eval.rs` (the AST-walk), `subset.rs` (reject-by-default allow-list), `schema.rs` (TypeTable/FnTable), `admission.rs` (`#[create]` retarget + `#[require]`), `mod.rs` (`interpret`/`validate`/`retarget_check`). Operator definition: `definition.tkd` (and the compiled `definition.rs` retained as the differential oracle). Tests: `fidelity.rs`, `fidelity_interp.rs`, `subset.rs`, `admission.rs`, `interp_edges.rs` + unit tests — 54 green, clippy clean.

**Deviations from the design as written:** (a) HostObj gained `Vol` and `Cx` variants — the clean `.tkd` calls `cx.state/config/docker_sock` (methods → `Vol`) and reads `cx.project_name` (field), so `cx` is a host object, not a plain Struct. (b) The boundary discipline is "only `value.rs` + `registry.rs` name `crate::builder`/`crate::kinds`" (the design's "only registry.rs" is impossible since HostObj wraps builder types); `eval/subset/schema/admission` are engine-agnostic except naming `crate::context::Cx`. (c) DSQL config uses string-substitution of the `storage:` line in tests to exercise the edited-config path (simulates an operator edit) rather than a hand-built Value. (d) Writeback ships the original compose-platform `collect_writeback` mirror (dotted-key `d.writeback(key, value)`), retained by decision as the fidelity reference; the `TokeiraConfig`-typed closure that removes the schema-hardcoded strings is a deferred *correctness* enhancement, not a convenience (§9). The writeback is a mechanic specialized for `TokeiraConfig`: it projects the adopted/created DSQL cluster endpoint+region and the two coordination table names into the server config, motivated by operator adoption of a pre-existing cluster at create (cluster adopted explicitly via `mode/endpoint/arn`; the two DynamoDB tables are adopt-if-exists by derived name in `DynamoDbTable::create` — there is no explicit operator table-name input in `ComposeDsqlConfig`). (e) The compiled `definition.rs` is kept (not retired) as a permanent differential oracle.

**Adversarial review (4-lens, independently verified) found and fixed 11 latent correctness bugs** — all subset-valid operator *edits* that diverged from compiled-Rust semantics, none affecting the shipped `definition.tkd` (fidelity stayed green throughout). Each is regression-locked in `tests/interp_edges.rs`:
1. `.to_string()` on Int/Bool was an identity no-op → now stringifies via `value_to_display`; `as_str`/`as_deref` restricted to string/Option receivers.
2. Variant patterns matched on leaf name only → now check the value's `EnumPath.ty` against the pattern's named enum (a wrong-enum same-leaf pattern errors).
3. `..` rest in tuple/tuple-struct patterns was treated as a positional element → `match_seq` now splits head/rest/tail.
4. Config struct literals were unvalidated → now exact-set checked against declared fields (no missing, no unknown, no `..`).
5. `Expr::Block` passed subset but had no eval arm (block-bodied match arms / `let x = {..}`) → added.
6. `==`/`!=` on host handles panicked (debug) / wrong (release) → `eval_binary` rejects host comparison with an EvalError.
7. A kind used as a config struct-field value leaked a host into config → `interpret` rejects `config().contains_host()`; `check_retarget` host-guard downgraded from `debug_assert` to a real error (no panic).
8. `#[require(expr)]` bodies bypassed the subset → `subset::check` now walks struct/enum `#[require]` exprs before admission evaluates them.
9. Typo'd enum tuple/struct variants silently built phantom values (failed open) → `eval` rejects unknown variants via `TypeTable::enum_has_variant` (fails closed).
10. `#[create]` diff didn't recurse through enum/Option/Vec/Tuple → now mirrors the structural reach of `collect_struct_fields`.
11. `#[require]` checked only the first instance of the scope type → now checks every instance.

**Confirmed-correct by the review (no change needed):** `Rc::try_unwrap` cannot be tripped spuriously; `Env::child()` scoping; integer host-edge narrowing (clean errors, no wrap/panic); `format!` Int/Bool display; take-once kind handles; the Service EMPTY defaults map matches `kinds::Service::EMPTY`; the DsqlMode config→kinds crossing; the module boundary; and no operator-reachable panic remains on the probed paths.

## 19. Relationship to the provisioner binary (`tkp`)

This interpreter is the concrete realization of the **platform-provisioner-binary** spec's central lever — its *"Engine identity vs configuration revision"* split. That spec hedges: *"This works to the extent platform definition is **configuration (data) rather than compiled code**."* The `.tkd` interpreter is what makes platform **structure** data:

- **Engine identity** (`source_tree_hash`; compiled into `tkp`; changing it mints a version and gates `upgrade`): the interpreter (`interp/`), the builder vocabulary (`builder.rs`), and the kind library (`kinds.rs` + each kind's `realize()` → engine resource). The versioned engine surface.
- **Configuration revision** (data the bound `tkp` reads; editing = ordinary `apply`, `config_revision++`): the `.tkd` — the deployment's modules / services / wiring / knobs.

**Flow.** `tkr … use <name>` selects the deployment and *forwards* the lifecycle verb to its bound `tkp`; `tkp` loads + interprets the deployment's `.tkd` (`interpret(src, cx)`), adapts the result to `tokeira_orchestrator::Deployment`, and drives the existing Delta engine — the provisioner spec's `apply` path, with the structure as config rather than compiled code.

**The deepest alignment: the subset boundary (§6) *enforces* the binding invariant.** A `.tkd` can only *name* the versioned author vocabulary; it cannot define a new resource kind, perform I/O, or alter apply-logic. So a `.tkd` edit is *structurally guaranteed* to remain a config revision — it can never silently become an engine-identity change. The binding gate's purpose (stop engine code from silently reinterpreting state) is thereby made unbreakable at the language level, not by policy. `#[create]` (§8) is a config-revision-internal constraint (which fields may change in an `apply` vs require a retarget), orthogonal to the engine binding.

**Adapter (the seam `tkp` consumes).** `interpret()` → `tokeira_orchestrator::Deployment`: `infra_modules()` / `services()` / `required_namespaces()` / `collect_writeback()` project from the interpreted `builder::Deployment`; `collect_writeback` resolves the deferred `WbValue::Output` handles against the post-apply `InfraState`. Built in the `adapter` module and proven through the engine trait surface (`tests/adapter.rs`): the adapted `.tkd` drives the engine to the **same infra resource set + deploy workloads + namespaces + writeback** as the hand-written `ComposeDeployment`. Remaining infra-fidelity decision: the engine's storage-dependent **module dependency graph** (the `runtime`↔`observability` inversion) and the `observability-config-files`→service resource dependency are engine *ordering* mechanics not yet expressed in the `.tkd` — author-in-`.tkd` vs relocate-as-mechanic is an open call (the inversion's intent was already flagged for review).
