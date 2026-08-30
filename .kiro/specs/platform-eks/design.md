# Design Document

## Overview

`platforms/eks` completes as a definition-backed platform package at the same structural standard as
`platforms/ecs`: a pure `platform()` declaration, modular `.tkd` + `.tkdp` definition sets whose parity
is enforced by test, a catalog descriptor for discovery, and execution/integration objects that own the
Kubernetes substrate. The AWS layer is entirely `tokeira-aws`; the Kubernetes layer is entirely
`tokeira-k8s`; this package contributes vocabulary, definitions, manifest builders, and the adapter
seams — nothing else.

The design divides into four planes:

1. **Authoring** — the definition sets and the config model they carry (the definition is the config).
2. **Realization** — kinds decoding through declared namespaces into `tokeira-aws` resources, K8s
   `iac::Resource`s, and deploy-engine services.
3. **Substrate** — `KubePlatform` (live server-side apply, readiness, scale, logs, port-forward)
   registered onto the provision context during applying operations.
4. **Operations** — the declaration's probe and `Ops`, in provider terms.

## Dependencies and Non-Goals

Depends on (consumed, not modified): `tokeira-platform` (declaration model),
`tokeira-platform-definition` (both frontends), `tokeira-deployment` (the `ServerConfig` node),
`tokeira-iac` / `tokeira-deploy-engine` / `tokeira-orchestrator` / `tokeira-state` (engines and
state), `tokeira-aws` (all AWS resources), `tokeira-k8s` (the Kubernetes substrate).

Non-goals: frontend semantics, provisioner lifecycle mechanics, any public ingress, automatic
scale-to-zero wiring (manual zero is in scope; HPA/KEDA is operator territory), the benchmark service
set.

## Architecture

### The definition sets

Ten-document `.tkd` set mirroring the ECS decomposition — a root wiring diagram plus focused parts —
and a peer `.tkdp` projection:

- `deployment.tkd` — the root: module wiring
  (`remote_state → images → networking → dsql → cluster → observability → services`), cross-part
  resource handles (VPC, security groups), `config()` defaults, writeback declarations.
- `platform.tkd` — the config model: region/tags (no authored environment — deployment identity is
  `cx.project_name` alone), `Networking`, `Eks` (version 1.36
  default, Graviton4 families), `Dsql` as a shaped enum (`Managed` / `Preexisting { endpoint, arn }`,
  `#[create]`), `Services`, `Observability`, `Debug`.
- `helpers.tkd` — shared assemblies callable from the root.
- `remote_state.tkd` · `images.tkd` · `networking.tkd` · `dsql.tkd` · `cluster.tkd` ·
  `observability.tkd` · `services.tkd` — one part per module.
- `definition.tkdp` + part peers — the Python projection, semantically identical.

Every part carries operator-grade documentation: what it owns, what it authors vs derives, its
dependencies and why.

### The declaration

```
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration {
        namespaces: vec![
            eks_kinds_namespace(),                       // the 12 AWS + 3 K8s kinds
            tokeira_deployment::server_config::namespace(),
            observability_content_namespace(),           // rendered content kinds
            aws_namespace(),                             // AwsClients registration trigger
        ],
        ops: Some(eks_ops()),
        execution: Box::new(EksExecution),
        implementation: Arc::new(EksIntegration),
    }
}
```

Construction is pure. `EksExecution::probe` answers the platform's reachability precondition honestly:
AWS credential resolution is the meaningful precondition (`Ok(None)` when resolvable); cluster
reachability is deliberately *not* probed deployment-wide (a fresh deployment has no cluster yet) —
operation-local errors are authoritative there, documented per the provider contract's honest-probe
item.

### Kinds and realization

The migrated kind set realizes to `tokeira-aws` resources unchanged (kind policy table in
requirements). The three Kubernetes kinds realize to `tokeira-k8s` constructs: `Namespace` →
`NamespaceResource`; `NodePool` → the Karpenter manifest resource; `ServiceDeployment` → the
deploy-engine service whose manifests are the proven pod shape (Alloy native sidecar, arm64 affinity,
anti-affinity, Pod-Identity ServiceAccount, downward-API broadcast env, config volume).

The `ServerConfig` node supplies the `tokeirad.toml` graph identity; the EKS service kinds consume its
dependency identity and deliver the rendered document as a ConfigMap mounted at the `TOKEIRA_CONFIG`
path. Delivery mechanism is platform-owned per `tokeira-deployment`'s contract.

### Substrate registration

During applying verbs, the integration registers a live `KubePlatform` onto the provision context's
extension bag (the one sanctioned `Box<dyn Any>` seam). K8s resource lifecycle methods fetch it and
fail with a remediation-bearing error when absent during apply; `describe` with no reachable cluster
reports absent, so plan yields Creates without a cluster — mirroring compose-without-Docker.

### Operator access (SSM-first)

A private-only cluster is only as usable as its access story, and the access story is owned here,
not left as a prerequisite. Under the default `operator_access = Ssm`: the Auto Mode node role
carries the AWS-managed SSM core policy (Bottlerocket ships the agent), the three SSM interface
endpoints join the networking module (the ECS deployment's required-endpoint precedent), and every
node is a tunnel anchor. One shared connection mechanism serves live apply and every day-2 verb: an
SSM port-forwarding session anchored on a node, layered to the private EKS API endpoint, with the
`kube::Client` speaking through it. `External` mode provisions none of it and says so on connection
failure. No bastion or relay instance exists in either mode; sessions are agent-initiated and
outbound-only, so Requirement 10's no-ingress posture is untouched.

`Ops` recovers its coordinates (admitted namespace, derived cluster name) by reading the admitted
revision from the deployment directory `DeploymentRef` carries — the sanctioned path for a
definition-backed platform's operations.

### Day-2 operations

`Ops` in provider terms over `KubePlatform`: scale patches replicas and awaits readiness in startup
order (reverse on the way down; zero admissible per Requirement 9.3, controller-last constraint
enforced); logs via the live cluster; port-forward via the `kube` client. Every verb fails loudly and
actionably when the cluster or credentials are unreachable.

## Data Models

- **Config model** — authored in the definition (both frontends), shaped enums over optional-field
  inference, `#[create]` on the DSQL storage identity. No serde platform-config file exists.
- **Writebacks** — the five-key DSQL set (requirements policy table), resolved from applied
  `InfraState` outputs and persisted into `TokeiraConfig` by the standard platform-side machinery.
- **Manifests** — `k8s-openapi` values built by pure functions; serde_json round-trip lossless.
- **State** — S3-native store keyed by the deployment name alone; loud failure when AWS clients are
  unregistered.

## Correctness Properties

- **Property 1 — Dual-frontend parity.** *For any* shipped configuration (default, managed DSQL,
  preexisting DSQL), evaluating the `.tkd` and `.tkdp` sets yields identical config, graph (modules,
  resources, dependencies), writebacks, and infrastructure desired manifests.
  **Validates: Requirements 1.3, 12.1.**
- **Property 2 — Module composition is a valid DAG.** *For any* admitted config, module names and
  resource ids are unique, dependencies present and backward-pointing, exactly one bootstrap module,
  and the graph acyclic in the staged order. **Validates: Requirements 3.1, 3.3.**
- **Property 3 — Writeback resolves to DSQL identity only.** *For any* applied `InfraState`,
  writeback emits exactly the five policy keys (endpoint preferred from the connection endpoint's
  `private_hostname`, cluster endpoint fallback), each output resolved to its state property; no other
  key. **Validates: Requirements 8.1, 8.2.**
- **Property 4 — `#[create]` retarget refusal.** *For any* config pair differing in the DSQL storage
  identity, admission refuses the re-apply as a retarget; differing only in non-`#[create]` fields
  reconciles. **Validates: Requirement 12.3.**
- **Property 5 — Manifest round-trip + acyclic services.** *For any* generated manifest, serde_json
  round-trip is lossless; the service dependency graph is acyclic and respects the startup order.
  **Validates: Requirements 6.3, 12.4.**
- **Property 6 — Private-only, least-privilege.** *For any* admitted config, the realized plan contains
  no public subnet, no internet gateway, a private EKS API, no `0.0.0.0/0` ingress rule, no
  `Ingress`/`LoadBalancer`; every service pod's AWS access is a Pod-Identity association.
  **Validates: Requirement 10.**
- **Property 7 — Single DSQL datastore.** *For any* admitted config, exactly one DSQL cluster is
  provisioned and it backs both persistence and projection; no separate visibility/search datastore
  appears. **Validates: Requirement 7.**
- **Property 8 — Live apply, plan-without-cluster.** Apply routes every K8s object mutation through the
  registered `KubePlatform` (no manifest-only path); plan with no reachable cluster yields Creates;
  lifecycle methods without the registered handle fail with the remediation error.
  **Validates: Requirements 2.2, 2.3, 2.4.**
- **Property 9 — Topology currency.** The generated NodePool is `karpenter.sh/v1` referencing the
  `eks.amazonaws.com` default NodeClass; node families default to Graviton4 `m8g/c8g/r8g`; the Alloy
  sidecar is a native (init + `restartPolicy: Always`) sidecar; the EKS version defaults to 1.36.
  **Validates: Requirements 4.5, 5.3.**
- **Property 10 — Config equals its authored defaults.** Evaluating the shipped set yields a config
  value equal to the authored `config()` literal, and an unknown config field is refused by frontend
  admission. **Validates: Requirement 12.2.**
- **Property 11 — Scale ordering with zero admissible.** *For any* scale target including zero,
  scale-up applies in startup order and scale-down in reverse; zero for `tokeira-controller` is
  refused while any `tokeirad` replica remains. **Validates: Requirements 9.1, 9.3.**
- **Property 12 — Access mode is exact.** *For any* admitted config: `Ssm` yields the three SSM
  interface endpoints and the SSM core policy on the node role, and no bastion/relay instance;
  `External` yields none of them; in both modes no ingress rule or public surface is added.
  **Validates: Requirements 13.1, 13.4, 13.5.**

## Error Handling

| Condition | Surface |
|---|---|
| Definition outside the frontend's admitted surface | Frontend diagnostics at load; `tkp` refuses with line/col |
| `#[create]` field changed on re-apply | Retarget refusal (admission), never a reconcile |
| K8s apply conflict | Server-side apply field management; retry with backoff |
| Cluster/credentials unreachable on apply or day-2 | Clear remediation ("check VPC access / EKS auth"); loud failure |
| AWS permission error | Fail fast naming the API + resource |
| S3 state conflict | CAS re-read + re-plan, never force |
| AWS clients unregistered at state-store creation | Loud failure (mirror ecs) |
| Scale-to-zero requested for the controller with live runtimes | Refusal naming the constraint |
| Session Manager plugin absent / no SSM-registered node | Named condition + remedy; never a silent direct-connect timeout |
| Connection failure under `External` access | States the operator-provided-route assumption |

## Testing Strategy

- **Property-based (proptest)** — Properties 1–11 become PBT tasks; config generators over the admitted
  model; `InfraState` fixtures for writeback.
- **Definition tests** — the shipped sets evaluated against the real declaration (module order,
  resource census, writeback keys, config defaults) for default + both DSQL modes; the parity assertion
  across frontends (`platforms/ecs/tests/definition.rs` is the exemplar harness).
- **Unit tests** — per-kind decode/realize round-trips, NodePool shape, pod-shape manifest content,
  private-only invariants, scale-order computation.
- **Out of the default suite** — no live AWS credentials and no live cluster: `KubePlatform` live paths
  are exercised behind gated integration tests; the default suite covers admission, realization, plan
  shape, and parity only. Live acceptance is operator-driven (the E1/E2 mold).
