# Implementation Plan

## Completed groundwork (DONE)

- [x] G.1 `crates/tokeira-k8s` — `KubePlatform` (server-side apply, readiness wait, scale, logs,
  port-forward over `kube::Client`), `standard_labels`, `build_node_pool` (`karpenter.sh/v1` → EKS Auto
  Mode default NodeClass, arm64/on-demand), `NamespaceResource`. In-tree and green.
- [x] G.2 `platforms/eks` package skeleton + the migrated kind set (`kinds.rs`: the twelve AWS kinds
  realizing to `tokeira-aws` resources unchanged, plus `Namespace` / `NodePool` / `ServiceDeployment`),
  `context.rs`, `builder.rs`, `manifests.rs`, `bridge.rs`. Compiles against the current framework.

## 1. Realization surfaces against the current framework

- [ ] 1.1 Re-home the kind set behind a declared `Namespace` (advertised TYPE names, decode via
  `kind::decode_resource`/`decode_service`, own TYPE consts pinned by test) and retire the
  bridge/builder machinery the retired frontend required. Kinds keep realizing to `tokeira-aws`
  resources unchanged (Req 4.1).
- [ ] 1.2 Implement `EksExecution` (probe: AWS credential resolvability = `Ok(None)`; no
  deployment-wide cluster probe — document why operation-local errors are authoritative) and
  `EksIntegration` (registers the live `KubePlatform` extension during applying verbs; never
  constructs a duplicate AWS client bundle) (Req 2.2).
- [ ] 1.3 `platform() -> PlatformDeclaration` — pure construction; namespaces = EKS kinds +
  `tokeira_deployment::server_config` + observability content + AWS; ops per task block 4;
  catalog descriptor (`[package.metadata.tokeira.platform]`, id `eks`, engine = workspace version,
  seeds for both formats) (Req 1.1, 1.2).
- [ ] 1.4 Pod-shape manifest builders trued to Requirement 5: Alloy native sidecar, arm64 affinity,
  anti-affinity, Pod-Identity ServiceAccount, downward-API broadcast env, ConfigMap projection of the
  `ServerConfig` node at the `TOKEIRA_CONFIG` path.
- [ ] 1.5 [PBT] `// Feature: platform-eks, Property 9` — NodePool/pod-shape/version topology currency.
- [ ] 1.6 **Checkpoint** — crate green under the workspace bar; kinds decode/realize round-trips pass.

## 2. The definition sets

- [ ] 2.1 Author the `.tkd` set: `platform.tkd` (config model per the policy table; `Dsql` as a shaped
  `#[create]` enum), `helpers.tkd`, and the seven module parts
  (`remote_state` · `images` · `networking` · `dsql` · `cluster` · `observability` · `services`),
  wired by `deployment.tkd` (root = the wiring diagram; cross-part handles for VPC/SGs; `config()`
  defaults; the five writeback declarations). Every part documented for a cold operator (Req 1.3, 3,
  8).
- [ ] 2.2 Author the `.tkdp` peer set, semantically identical (Req 1.3).
- [ ] 2.3 Definition tests: shipped sets evaluate against the real declaration — module order, resource
  census per module, writeback keys, config-equals-authored-defaults — for default + managed +
  preexisting DSQL (Req 12.2).
- [ ] 2.4 [PBT] `// Feature: platform-eks, Property 1` — dual-frontend parity (config, graph,
  writebacks, infrastructure desired manifests) across all three configurations.
- [ ] 2.5 [PBT] `// Feature: platform-eks, Property 2` — module composition DAG; one bootstrap;
  backward dependencies.
- [ ] 2.6 [PBT] `// Feature: platform-eks, Property 10` — config round-trip identity + unknown-field
  refusal through each frontend's admission.
- [ ] 2.7 [PBT] `// Feature: platform-eks, Property 4` — `#[create]` DSQL identity change refused as a
  retarget; non-`#[create]` change reconciles.
- [ ] 2.8 [PBT] `// Feature: platform-eks, Property 6` — private-only/least-privilege plan invariants.
- [ ] 2.9 [PBT] `// Feature: platform-eks, Property 7` — single DSQL datastore.
- [ ] 2.10 **Checkpoint** — both sets evaluate green; parity holds; `cargo doc` clean.

## 3. Writeback, state, and creation

- [ ] 3.1 Wire the five writebacks through the standard platform-side machinery; hydration fills empty
  DSQL identity from applied state before assembly (Req 8).
- [ ] 3.2 [PBT] `// Feature: platform-eks, Property 3` — writeback exactness over `InfraState`
  fixtures.
- [ ] 3.3 S3-native state store selection keyed by the deployment name alone; loud failure without AWS
  clients (Req 11.1).
- [ ] 3.4 `tkr … create` stages the shipped set for the chosen format — root + all parts + content —
  verified by a create-then-plan test (Req 11.2; the companion-content staging lesson).
- [ ] 3.5 **Checkpoint** — created deployment plans clean in both formats without further staging.

## 4. Operations and live apply

- [ ] 4.1 `Ops` over `KubePlatform`: scale in startup order (reverse down; zero admissible for
  `tokeirad` + observability; controller-zero refused while runtimes remain), logs, port-forward; every
  verb loud and actionable when the cluster/credentials are unreachable (Req 9).
- [ ] 4.2 [PBT] `// Feature: platform-eks, Property 11` — scale-order computation with zero targets.
- [ ] 4.3 [PBT] `// Feature: platform-eks, Property 8` — apply routes through the registered
  `KubePlatform`; plan-without-cluster yields Creates; missing-handle lifecycle error.
- [ ] 4.4 [PBT] `// Feature: platform-eks, Property 5` — manifest serde round-trip + acyclic service
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

## 5. Live acceptance (operator-driven)

- [ ] 5.1 Live-AWS acceptance in the E1/E2 mold — operator at the wheel: provision (managed DSQL),
  deploy, workload round-trip, scale (including to zero and back), logs, port-forward, destroy;
  adopted-DSQL leg; findings routed as fix slices. Green tests necessary, not sufficient.
