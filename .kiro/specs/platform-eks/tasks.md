# Implementation Plan

## Completed groundwork (DONE)

- [x] G.1 `crates/tokeira-k8s` — `KubePlatform` (server-side apply, readiness wait, scale, logs,
  port-forward over `kube::Client`), `standard_labels`, `build_node_pool` (`karpenter.sh/v1` → EKS Auto
  Mode default NodeClass, arm64/on-demand), `NamespaceResource`. In-tree and green.
- [x] G.2 `platforms/eks` package skeleton + the migrated kind set (`kinds.rs`: the twelve AWS kinds
  realizing to `tokeira-aws` resources unchanged, plus `Namespace` / `NodePool` / `ServiceDeployment`),
  `context.rs`, `builder.rs`, `manifests.rs`, `bridge.rs`. Compiles against the current framework.

## 1. Realization surfaces against the current framework

- [x] 1.1 Re-home the kind set behind a declared `Namespace` (advertised TYPE names, decode via
  `kind::decode_resource`/`decode_service`, own TYPE consts pinned by test) and retire the
  bridge/builder machinery the retired frontend required. Kinds keep realizing to `tokeira-aws`
  resources unchanged (Req 4.1).
- [x] 1.2 Implement `EksExecution` (probe: AWS credential resolvability = `Ok(None)`; no
  deployment-wide cluster probe — document why operation-local errors are authoritative) and
  `EksIntegration` (registers the live `KubePlatform` extension during applying verbs; never
  constructs a duplicate AWS client bundle) (Req 2.2).
- [x] 1.3 `platform() -> PlatformDeclaration` — pure construction; namespaces = EKS kinds +
  `tokeira_deployment::server_config` + observability content + AWS; ops per task block 4;
  catalog descriptor (`[package.metadata.tokeira.platform]`, id `eks`, engine = workspace version,
  seeds for both formats) (Req 1.1, 1.2).
- [x] 1.4 Pod-shape manifest builders trued to Requirement 5: Alloy native sidecar, arm64 affinity,
  anti-affinity, Pod-Identity ServiceAccount, downward-API broadcast env, ConfigMap projection of the
  `ServerConfig` node at the `TOKEIRA_CONFIG` path.
- [x] 1.5 [PBT] `// Feature: platform-eks, Property 9` — NodePool/pod-shape/version topology currency.
- [x] 1.6 **Checkpoint** — crate green under the workspace bar; kinds decode/realize round-trips pass.

  **DONE:** 1.1–1.6. The platform declares the current namespaces and catalog seeds, and focused
  tests pin kind decoding, EKS 1.36, Auto Mode family selection, and the workload pod shape.
  `EksIntegration` registers one shared, lazy `KubePlatform`: registration performs no Kubernetes
  access, and failed first-use initialization is retryable after the ordered cluster module. The
  complete workspace bar is green on the XL devbox.

## 2. The definition sets

- [x] 2.1 Author the `.tkd` set: `platform.tkd` (config model per the policy table; `Dsql` as a shaped
  `#[create]` enum), `helpers.tkd`, and the seven module parts
  (`remote_state` · `images` · `networking` · `dsql` · `cluster` · `observability` · `services`),
  wired by `deployment.tkd` (root = the wiring diagram; cross-part handles for VPC/SGs; `config()`
  defaults; the five writeback declarations). Every part documented for a cold operator (Req 1.3, 3,
  8).
- [x] 2.2 Author the `.tkdp` peer set, semantically identical (Req 1.3).
- [x] 2.3 Definition tests: shipped sets evaluate against the real declaration — module order, resource
  census per module, writeback keys, config-equals-authored-defaults — for default + managed +
  preexisting DSQL (Req 12.2).
- [x] 2.4 [PBT] `// Feature: platform-eks, Property 1` — dual-frontend parity (config, graph,
  writebacks, infrastructure desired manifests) across all three configurations.
- [x] 2.5 [PBT] `// Feature: platform-eks, Property 2` — module composition DAG; one bootstrap;
  backward dependencies.
- [x] 2.6 [PBT] `// Feature: platform-eks, Property 10` — config round-trip identity + unknown-field
  refusal through each frontend's admission.
- [x] 2.7 [PBT] `// Feature: platform-eks, Property 4` — `#[create]` DSQL identity change refused as a
  retarget; non-`#[create]` change reconciles.
- [x] 2.8 [PBT] `// Feature: platform-eks, Property 6` — private-only/least-privilege plan invariants.
- [x] 2.9 [PBT] `// Feature: platform-eks, Property 7` — single DSQL datastore.
- [x] 2.10 **Checkpoint** — both sets evaluate green; parity holds; `cargo doc` clean.

  **DONE:** 2.1–2.10. Both modular source sets evaluate to the same 51-node graph and realized
  manifests for managed and adopted DSQL, with deployment identity derived only from
  `cx.project_name`. TKD and TKDP both refuse DSQL identity retargets while admitting reconcilable
  changes across generated configurations. Every workload has Pod Identity; dependency-backed IAM
  policies resolve provider-assigned DSQL, DynamoDB, S3, and Secrets Manager ARNs exactly, and
  property tests reject public placement or wildcard resources.

## 3. Writeback, state, and creation

- [x] 3.1 Wire the five writebacks through the standard platform-side machinery; hydration fills empty
  DSQL identity from applied state before assembly (Req 8).
- [x] 3.2 [PBT] `// Feature: platform-eks, Property 3` — writeback exactness over `InfraState`
  fixtures.
- [ ] 3.3 S3-native state store selection keyed by the deployment name alone; loud failure without AWS
  clients (Req 11.1).
- [x] 3.4 `tkr … create` stages the shipped set for the chosen format — root + all parts + content —
  verified by a create-then-plan test (Req 11.2; the companion-content staging lesson).
- [x] 3.5 **Checkpoint** — created deployment plans clean in both formats without further staging.

  **DONE:** 3.1–3.2 and 3.4–3.5. The standard writeback resolver produces exactly the five fields
  admitted by `TokeiraConfig`; service assembly consumes the applied private DSQL endpoint and
  refuses an unresolved output. A framework-independent `tkr create` regression byte-checks every
  shipped root, part, and content companion, then evaluates and realizes the created deployment into
  verified plan inputs for both frontends. S3 store selection remains a framework-owned gap: the
  provisioner currently selects local CAS stores before platform execution and exposes no platform
  store-selection seam.

## 4. Operations and live apply

- [x] 4.1 `Ops` over `KubePlatform`: scale in startup order (reverse down; zero admissible for
  `tokeirad` + observability; controller-zero refused while runtimes remain), logs, port-forward; every
  verb loud and actionable when the cluster/credentials are unreachable (Req 9).
- [x] 4.2 [PBT] `// Feature: platform-eks, Property 11` — scale-order computation with zero targets.
- [x] 4.3 [PBT] `// Feature: platform-eks, Property 8` — apply routes through the registered
  `KubePlatform`; plan-without-cluster yields Creates; missing-handle lifecycle error.
- [x] 4.4 [PBT] `// Feature: platform-eks, Property 5` — manifest serde round-trip + acyclic service
  graph in startup order.
- [ ] 4.5 Gated (non-default) live integration tests for the `KubePlatform` paths.
- [ ] 4.6 Operator access (Req 13): the `operator_access` config enum in both definition sets; under
  `Ssm`, the relay (keyless arm64 nano, SSM-parameter AMI resolution, ingress-free SG, SSM-core
  profile) + the three SSM interface endpoints in the networking module; the shared relay-anchored
  connection mechanism (lazy first-use connect, TLS server-name handling) used by live apply and
  every day-2 verb; the named-condition errors (plugin absent, relay unregistered, External-mode
  route assumption).
- [ ] 4.7 [PBT] `// Feature: platform-eks, Property 12` — access-mode exactness across both modes.
- [ ] 4.8 **Checkpoint** — full §10.4 bar green; provider-contract checklist satisfied item-by-item in
  the PR.

  **DONE:** 4.1–4.4. `Ops` re-evaluates the admitted revision to recover its authored namespace and
  derives the cluster name from deployment identity; logs, live pod port-forward, and readiness-
  awaited scale all run through `KubePlatform`. Scale reductions run in reverse startup order,
  increases run forward, zero is admitted, and controller-zero is refused while runtimes remain;
  Property 11 covers generated mixed target sets. Registered handles connect lazily on first use
  (Req 13.3), plans remain provider-pure, desired-field comparison checks live drift, and the
  seven-service graph is acyclic. Live integration task 4.5 and the remaining operator-access slice
  in tasks 4.6–4.8 remain operator-gated follow-ups.

## 5. Live acceptance (operator-driven)

- [ ] 5.1 Live-AWS acceptance in the E1/E2 mold — operator at the wheel: provision (managed DSQL),
  deploy, workload round-trip, scale (including to zero and back), logs, port-forward, destroy;
  adopted-DSQL leg; findings routed as fix slices. Green tests necessary, not sufficient.
