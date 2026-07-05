# Implementation Plan

Tasks are coding tasks only, dependency-ordered. Each cites the requirements it implements. Every
correctness property from the design (Properties 1–14) is a required property-based test (PBT) task, tagged
`// Feature: platform-eks, Property N`. Checkpoints mark where build/lint/test must be green before
proceeding. The enforced commands are `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`,
`cargo check --workspace`, `cargo test --workspace`, `cargo doc --workspace --no-deps`.

## Block 0 — Prerequisite: extract `crates/tokeira-tkd` and refactor `compose-syn` onto it

_This block lands and is green before any `platforms/eks` interpreter work (Requirement 14.5)._

- [x] 0.1 Create `crates/tokeira-tkd` skeleton: `Cargo.toml` (hand-pinned `syn = { version = "2", features = ["full","extra-traits"] }`, `proc-macro2 = "1"`, `serde_json`, `thiserror`), `lib.rs` with empty module tree (`value`, `schema`, `subset`, `eval`, `admission`, `bridge`, `mod`); register in the workspace members.
  _Requirements: 14.1_
- [x] 0.2 Move the platform-agnostic value model into `tokeira-tkd`, generic over the host type `H`: `Value<H>`, `EnumPath`, `VariantBody<H>`, `EvalError` (spanned), `Diagnostics`, `FieldMap<H>`, and the host-free `FieldMapExt` takers (`take_str`/`take_bool`/`take_u16`/`take_u32`/`take_opt_str`/`take_vec_str`/`expect_empty`). `PartialEq`/`contains_host` treat `Host(_)` opaquely.
  _Requirements: 14.1, 14.2_
- [x] 0.3 Define the `HostBridge` trait (`type Host`, `type Cx`, `type Output`; `is_kind`, `kind_defaults`, `construct_kind`, `assoc`, `call_method`, `knows_method`, `cx_field`, `cx_host`, `finish`). The core routes every host operation through this trait and names no concrete kind.
  _Requirements: 14.2_
- [x] 0.4 Move `schema` (collect `TypeTable`/`FnTable`, per-field `#[create]`, per-type `#[require]`) — non-generic; behaviour unchanged from compose-syn.
  _Requirements: 14.1_
- [x] 0.5 Move `subset` (reject-by-default allow-list) — validates method names via `bridge.knows_method`; behaviour unchanged.
  _Requirements: 14.1, 14.6_
- [x] 0.6 Move `eval` generic over `B: HostBridge` (the AST walk: struct/enum/tuple/`format!`/`vec!`/`matches!`, if-let/match, field/shorthand, method dispatch → `bridge.call_method`, kind construction → `bridge.construct_kind`, `cx` field/method).
  _Requirements: 14.1_
- [x] 0.7 Move `admission` (retarget diff over `#[create]` fields + `#[require]` evaluation) operating on host-free config `Value<()>`.
  _Requirements: 14.1_
- [x] 0.8 Implement the orchestration entry points: `interpret<B>(src, &B, &B::Cx) -> Result<(B::Output, Value<B::Host>), Diagnostics>`, `validate<B>`, `retarget_check`. Enforce `config()` is host-free.
  _Requirements: 14.1, 14.2_
- [x] 0.9 Refactor `platforms/compose-syn` onto `tokeira-tkd`: add `ComposeBridge` implementing `HostBridge` (wrapping today's `Registry` tables, the `HostObj`/`HostKindVal` enums, the host-typed coercions, `Cx`, `Deployment`); delete `src/interp/`; point `adapter.rs`/`platform.rs` at `tokeira_tkd::interpret(&ComposeBridge, &cx)`.
  _Requirements: 14.3, 14.4_
- [x] 0.10 [PBT] `// Feature: platform-eks, Property 3` — compose-syn regression: after the refactor, its realized deployment equals the pre-refactor result and interpreted `.tkd` == compiled `definition.rs`; the existing `fidelity`, `fidelity_interp`, `subset`, `admission`, `interp_edges` suites stay green.
  _Requirements: 14.3, 14.4_
- [x] 0.11 [PBT] `// Feature: platform-eks, Property 2` — feed malformed/random `.tkd` input to `interpret`/`validate`; assert `Ok` or `Diagnostics`, never a panic on an operator-reachable path.
  _Requirements: 1.2, 14.6_
- [x] 0.12 [PBT] `// Feature: platform-eks, Property 4` — CI/grep boundary check: no source under `crates/tokeira-tkd` names a platform's concrete kind or builder type.
  _Requirements: 14.2_
- [x] 0.13 **Checkpoint** — `cargo build`/`cargo lint`/`cargo test` green for `tokeira-tkd` and `compose-syn`; `cargo doc` clean.

## Block 1 — `crates/tokeira-k8s` (live Kubernetes platform)

- [x] 1.1 Create `crates/tokeira-k8s` skeleton: `Cargo.toml` (`kube`, `k8s-openapi` feature `v1_33` only, `serde_json`, `async-trait`, `thiserror`, `tokeira-iac`); register in the workspace.
  _Requirements: 3.1, 3.2_
- [x] 1.2 Implement `KubePlatform::connect`/`ensure_reachable` and server-side apply/`get`/`delete`/`wait_ready` over `kube::Client` (field-manager `tkp`).
  _Requirements: 3.1, 3.3_
- [x] 1.3 Implement day-2 ops on `KubePlatform`: `scale` (patch replicas), `logs` (pod logs / Loki), `port_forward`.
  _Requirements: 3.1, 10.1, 10.2, 10.3_
- [x] 1.4 Implement shared manifest helpers: `standard_labels(service, project)` and `build_node_pool(node_families)` (`karpenter.sh/v1` NodePool → `eks.amazonaws.com` default NodeClass, arm64/on-demand).
  _Requirements: 6.3, 6.6_
- [x] 1.5 Implement `NamespaceResource` (`iac::Resource` whose `create/update/delete/describe` call the context's `KubePlatform`, dep on the EKS cluster).
  _Requirements: 3.3, 6.5_
- [x] 1.6 **Checkpoint** — `tokeira-k8s` builds/lints/tests green; `build_node_pool` shape unit test passes.

## Block 2 — `platforms/eks` skeleton, context, builder

_The config surface is **not** a `config.rs`: per Proposal 003 §3 the DSL is the config, so the `EksConfig`
types + `config()` defaults + `#[create]`/`#[require]` are authored in `definition.tkd` (Block 4, task 4.3)
and validated by the interpreter (`#[require]` + the reject-by-default subset), not by serde/TOML. Block 2
builds only the crate skeleton, the `Cx`, and the builder vocabulary. The `config()` round-trip + unknown-
field rejection (Property 8) is proven in Block 4 (task 4.8), once the interpreter path exists._

- [x] 2.1 Create `platforms/eks` skeleton: `Cargo.toml` (deps `tokeira-tkd`, `tokeira-k8s`, `tokeira-aws`, `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-orchestrator`, `tokeira-state`, `tokeira-config`, `k8s-openapi` `v1_33`; the `syn` interpreter arrives via `tokeira-tkd`, so there is **no direct `syn`/`proc-macro2` pin** — mirroring the refactored `platforms/compose-syn`); register in the workspace.
  _Requirements: 1.1, 1.4_
- [x] 2.2 Implement `context.rs` — `Cx { project_name, region, account_id, deployment_dir }` (no bind-mount `Vol` helpers; K8s manifests are built in kinds, not from host paths).
  _Requirements: 1.1_
- [x] 2.3 Implement `builder.rs` — `Deployment`/`ModuleRef`/`ResourceRef`/`Output`/`WbValue` and the `Kind` trait (`realize(&self, cx: &Cx) -> Box<dyn iac::Resource>`), same shape as compose-syn's builder **minus** the compose `Service`/`Vol` workload machinery (K8s objects are `Box<dyn Kind>` resources; there is no deploy-engine workload path for EKS).
  _Requirements: 1.1, 5.1_
- [x] 2.4 **Checkpoint** — `platforms/eks` builds/lints green; builder/context unit tests pass.

## Block 3 — `platforms/eks` kinds (AWS resources + Kubernetes manifests)

- [x] 3.1 Implement the AWS kinds realizing to existing `tokeira-aws` resources: `Vpc`, `VpcEndpoint`, `SecurityGroup`, `IamRole`, `EksCluster`, `PodIdentityAssociation`, `DsqlCluster`, `DsqlConnectionEndpoint`, `DynamoDbTable`, `S3Bucket`, `SecretsManagerSecret`, `EcrRepository`. The tokeira-task role carries DSQL + DynamoDB access; DSQL mode is inferred from endpoint/arn presence.
  _Requirements: 5.1, 5.3, 5.4, 11.4_
- [ ] 3.2 [PBT] `// Feature: platform-eks, Property 10` — for any `EksConfig`, the realized plan has no public subnet, no internet gateway, a private EKS API, no `0.0.0.0/0` ingress rule, no `Ingress`/`LoadBalancer`; each service pod has a Pod-Identity association.
  _Requirements: 11.1, 11.2, 11.3, 11.4_
  _Block 3 covers the manifest-level facets (Services are ClusterIP-only; no `LoadBalancer`/`Ingress` builder exists — `manifests` tests). The full plan sweep over a realized `EksConfig` (no public subnet/IGW, private EKS API, per-service Pod-Identity) realizes with the Block 5 adapter, alongside Property 5's module DAG._
- [x] 3.3 Implement the K8s manifest builders (`k8s-openapi` typed structs → `serde_json::Value`): per-service `Deployment` (main container + Alloy native sidecar init-container `restartPolicy: Always`, arm64 node affinity, pod anti-affinity, Pod-Identity `ServiceAccount`, downward-API `POD_IP`/`TOKEIRA_NODE_HOST` env, a mounted config ConfigMap located by `TOKEIRA_CONFIG`/`--config` — DSQL arrives in `tokeirad.toml` via writeback, not env), ClusterIP `Service` (gRPC + metrics), `ServiceAccount`, config `ConfigMap`; the `NodePool` kind (via `tokeira-k8s::build_node_pool`). Grounded topology: `tokeirad` (monolith) / `tokeira-controller` / `tokeira-autoscaler` + observability (Req 7), not per-role services.
  _Requirements: 6.1, 6.2, 6.3, 6.4, 7.3_
- [x] 3.4 [PBT] `// Feature: platform-eks, Property 13` — the generated NodePool is `karpenter.sh/v1` referencing the `eks.amazonaws.com` default NodeClass; node families are Graviton4 `m8g/c8g/r8g`; the Alloy sidecar is a native (init + `restartPolicy: Always`) sidecar.
  _Requirements: 5.5, 6.6_
- [x] 3.5 [PBT] `// Feature: platform-eks, Property 9` — every generated K8s manifest round-trips through `serde_json` losslessly; the service dependency graph is acyclic.
  _Requirements: 13.4_
  _Round-trip proven in Block 3 (`manifests::tests::manifests_round_trip_through_serde_json`). The acyclic service-dependency graph is proven with the Block 5 topology (`valid_services` + deps), alongside Property 5._
- [x] 3.6 Wrap the K8s manifests as `iac::Resource` kinds whose `create/update/delete/describe` apply via the context's `KubePlatform` (mirroring how compose containers apply via `ComposePlatform`).
  _Requirements: 3.3, 6.5_
- [x] 3.7 **Checkpoint** — kinds build/lint green; per-kind round-trip and manifest tests pass.

## Block 4 — `platforms/eks` bridge + definition (interpreter-backed)

- [x] 4.1 Implement `HostObj` (closed enum: `Deployment`/`Module`/`Resource`/`Output`/`Kind`/`Cx`), the host-typed coercions, and `EksBridge` implementing `tokeira_tkd::HostBridge` (`Host = HostObj`, `Cx = Cx`, `Output = builder::Deployment`): the kind ctors (15 kinds; `ServiceManifest`/`IngressRule` are config structs decomposed here, not kinds), no `..EMPTY` defaults, method (`module`/`resource`/`writeback`/`output`) / assoc (`Deployment::new`) tables, `cx_field` (`project_name`/`region`/`account_id`), `finish`. No `Vol`/`Service` variant.
  _Requirements: 1.4, 14.2_
- [ ] 4.2 Author `definition.tkd` **config half**: the `EksConfig` types (project/state/vpc/eks/dsql/services/observability/debug), `#[create]` on the DSQL storage identity (mode/endpoint/arn) and any `#[require]` (canonical ports, `max_idle_conns == max_conns`), plus `config()` returning the defaults (EKS `1.36`, Graviton4 `[m8g,c8g,r8g]`). There is **no compiled `definition.rs` oracle** for EKS (user directive): the `.tkd` is the sole authored form.
  _Requirements: 1.3, 4.1, 4.3, 4.4, 7.1, 9.1_
- [ ] 4.3 Author `definition.tkd` **deployment half**: `deployment(cfg, cx)` building `remote_state → foundation → cluster` with the grounded tokeira topology and the DSQL writeback (endpoint/arn/region/table names); hermetic (no paths/env/I/O; config content built via `format!` from `cfg`/`cx`, DSQL endpoint arriving via `hydrate_config`).
  _Requirements: 1.2, 1.3_
- [ ] 4.4 [PBT] `// Feature: platform-eks, Property 1` — fidelity by **direct structural assertion** (no compiled `.rs` twin): for default and DSQL configs, `interpret(definition.tkd, EksBridge, cx)` yields the expected module DAG (`remote_state → foundation → cluster`), per-module resource shape (`resource_id`+`type`+`module`+deps), required namespaces, and writeback keys/values, via `tests/fidelity_interp.rs`.
  _Requirements: 1.3, 13.1_
- [ ] 4.5 [PBT] `// Feature: platform-eks, Property 7` — a config pair differing in a `#[create]` field (DSQL mode/identity) is refused by `retarget_check`; a non-`#[create]` change reconciles.
  _Requirements: 13.3_
- [ ] 4.6 [PBT] `// Feature: platform-eks, Property 6` — `collect_writeback` over an `InfraState` fixture emits exactly the DSQL endpoint (connection-endpoint `private_hostname` preferred, cluster endpoint fallback), ARN, region, and the two coordination table names, each `Output` resolved to its state property; no other key.
  _Requirements: 8, 9.1, 9.2, 9.3_
- [ ] 4.7 [PBT] `// Feature: platform-eks, Property 12` — for any `EksConfig`, exactly one DSQL cluster is provisioned and it backs both persistence and projection; no separate visibility/search datastore appears.
  _Requirements: 8.1, 8.2_
- [ ] 4.8 [PBT] `// Feature: platform-eks, Property 8` — interpreting the `.tkd` `config()` yields a config value equal to the authored literal (interpreter round-trip is identity), and a config struct-literal with an unknown field is rejected by the subset (the `syn`-model analog of serde `deny_unknown_fields`; no platform-config TOML exists).
  _Requirements: 13.2_
- [ ] 4.9 **Checkpoint** — fidelity, retarget, writeback, single-datastore, and config round-trip/unknown-field tests pass; `cargo doc` clean.

## Block 5 — orchestrator adapter (`EksDeployment`)

- [ ] 5.1 Implement `adapter.rs` `EksDeployment: orchestrator::Deployment` — `remote_state_module` (the S3-bucket bootstrap), `infra_modules` (from `module_names`, excluding bootstrap), `required_namespaces`, `collect_writeback` (resolving `WbValue::Output` vs `InfraState`), `create_infra_store`/`create_deploy_store` (`S3StateStore`, keyed by project+environment; `MissingAwsClientsBackend` when clients absent), `register_infra_extensions` (AWS clients), `hydrate_config`.
  _Requirements: 2.6, 4.1, 5.1, 9.2, 12.1, 12.2_
- [ ] 5.2 [PBT] `// Feature: platform-eks, Property 5` — for any `EksConfig`, module names and resource ids are unique, `desired ⊆ known`, dependencies present, and the module graph is a DAG (`remote_state → foundation → cluster`).
  _Requirements: 4.1, 4.5_
- [ ] 5.3 Implement `EksDeployment: orchestrator::Ops` — `valid_services` (tokeira topology), `desired_replicas` (from `service_replicas`), `scale_up`/`scale_down` (via `KubePlatform::scale` in tokeira startup order), `logs`, `port_mappings` (via `KubePlatform`).
  _Requirements: 7.1, 7.4, 10.1, 10.2, 10.3, 10.4_
- [ ] 5.4 Implement `EksDeployment: orchestrator::PlatformConfig` — `prototypical_config` (default `.tkd`) and `prototypical_server_config` (default `tokeirad.toml`, DSQL variant pre-filling endpoint/region placeholders).
  _Requirements: 12.3_
- [ ] 5.5 **Checkpoint** — adapter builds/lints/tests green; module-DAG test passes.

## Block 6 — `tkp` / `tkr` integration

- [ ] 6.1 Add `Eks` to `tokeira_orchestrator::PlatformKind`.
  _Requirements: 2.1_
- [ ] 6.2 Add the `PlatformKind::Eks` arms to `apps/tkr/src/prototypical.rs` (`EksDeployment::prototypical_config`/`prototypical_server_config`), including the DSQL region patch.
  _Requirements: 2.1, 12.3_
- [ ] 6.3 Extend `apps/tkp/src/platform.rs`: add `Platform::Eks`; make `detect` read `DeploymentMetadata.platform` from `metadata.json` (falling back to the `.tkd`/local heuristic only when metadata is absent); implement `open_eks_engine` building `InfraEngine<EksDeployment>` and registering a reachable `KubePlatform` on the provision context (omit for read-only `plan` when unreachable; require it for apply/destroy).
  _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_
- [ ] 6.4 [PBT] `// Feature: platform-eks, Property 11` — `apply` routes every K8s-object mutation through the registered `KubePlatform` (no manifest-only path); `plan` with no reachable cluster still yields Creates.
  _Requirements: 2.4, 2.5, 3.3_
- [ ] 6.5 [PBT] `// Feature: platform-eks, Property 14` — `tkp::platform::detect` returns `Eks` iff `metadata.platform == Eks`, independent of `.tkd` presence.
  _Requirements: 2.2_
- [ ] 6.6 **Checkpoint** — full enforced stack green: `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`, `cargo check --workspace`, `cargo test --workspace`, `cargo doc --workspace --no-deps`.

## Block 7 — integration

- [ ] 7.1 In-memory integration test: drive one turn through `tkp` (`init` then `plan`) against the EKS `definition.tkd`; assert the module/resource shape, the required namespaces, and the DSQL writeback — without live AWS or a live cluster.
  _Requirements: 2.6, 4.1, 9.1_
- [ ] 7.2 Live `KubePlatform` integration test behind a feature/`#[ignore]` gate (server-side apply + scale + logs against a real cluster); excluded from the default suite.
  _Requirements: 3.1, 10.1, 10.2, 10.3_

## Task Dependency Graph

```
0 (tokeira-tkd + compose-syn refactor)
├─ 0.1 → 0.2 → 0.3 → 0.4 → 0.5 → 0.6 → 0.7 → 0.8 → 0.9 → {0.10, 0.11, 0.12} → 0.13
1 (tokeira-k8s)              depends on: none (parallel with 0)
├─ 1.1 → {1.2, 1.3, 1.4, 1.5} → 1.6
2 (eks skeleton/context/builder) depends on: 0.13, 1.6
├─ 2.1 → 2.2 → 2.3 → 2.4
3 (eks kinds)                depends on: 2.4
├─ 3.1 → 3.2, 3.1 → 3.3 → {3.4, 3.5} → 3.6 → 3.7
4 (bridge + definition)      depends on: 3.7, 0.8
├─ 4.1 → 4.2 → 4.3 → 4.4 → {4.5, 4.6, 4.7, 4.8} → 4.9
5 (adapter)                  depends on: 4.9
├─ 5.1 → 5.2 → 5.3 → 5.4 → 5.5
6 (tkp/tkr wiring)           depends on: 5.5
├─ 6.1 → 6.2 → 6.3 → {6.4, 6.5} → 6.6
7 (integration)              depends on: 6.6
├─ 7.1 → 7.2
```

Blocks 0 and 1 are independent and may proceed in parallel; Block 2 needs both. Everything after is linear.

## Notes

- **The DSL is the config (Proposal 003 §3).** There is no `config.rs` and no platform-config TOML: the
  `EksConfig` types, their `config()` defaults, and their `#[create]`/`#[require]` attributes are authored in
  `definition.tkd`, and the interpreter models them from the `.tkd`'s own AST. Validation is `#[require]`;
  unknown fields are rejected by the reject-by-default subset. This is why Block 2 is skeleton + `context.rs`
  + `builder.rs` only, and the config work lands in Block 4 (task 4.3).
- **No compiled `definition.rs` oracle for EKS (applied in tasks 4.2/4.4).** Per the user directive,
  `platforms/eks` authors only `definition.tkd`; there is no compiled `definition.rs` differential twin
  (unlike `compose-syn`, which keeps one). Fidelity (Property 1) is proven by asserting the interpreted
  deployment's structure directly — module DAG, per-module resource shape, namespaces, writeback
  keys/values. The design's Property 1 and Req 1.3/13.1 still carry the older "byte-identical to compiled
  `definition.rs`" wording; read them as "structurally as specified" for EKS.
- **Block 0 is the gate.** Per Requirement 14.5 the `tokeira-tkd` extraction and the `compose-syn` refactor
  land and stay green before any interpreter-backed `platforms/eks` work (Blocks 4+). Property 3 protects
  the refactor.
- **Single apply path.** Blocks 3.6 and 6.3 keep every mutation on `tkp`'s `InfraEngine` via the registered
  `KubePlatform`, mirroring compose-syn's `ComposePlatform` — no `DeployEngine` path for EKS.
- **AWS resources are reused, not ported.** Block 3.1 wires existing `tokeira-aws` resources; any missing
  capability is fixed in `tokeira-aws` (a shared-crate change), never worked around in `platforms/eks`
  (Requirement 5.2).
- **No live credentials in the default suite.** All PBTs and unit tests operate on desired-state /
  manifest generation; the only live-AWS/live-cluster path (7.2) is feature/`#[ignore]`-gated.
- **Currency.** The EKS version default (`1.36`) and the NodePool/sidecar conventions were verified
  2026-07 (requirements "Topology currency"); Property 13 locks the conventions.
