# Proposal 002 — The Operator Configuration Surface

- **Status:** Proposed (config-surface definition; no code moved yet)
- **Companion to:** [Proposal 001 — `tokeira-platform` framework and the `Realizer` seam](./001-platform-framework-and-realizer.md)
- **Scope:** the `compose` and `ecs` platforms
- **Audience:** whoever authors the `.tkd` kind libraries and `.platform` definitions for compose / ecs (Proposal 001, Wave 8) and decides what an operator may set
- **Owner area:** `platforms/compose`, `platforms/ecs` (source of truth being modelled); `platforms/compose-dsl`, future `platforms/ecs-dsl` (consumers)

## 1. Purpose & scope

This document defines **WHAT configuration the platform `.tkd` should expose** to an operator for the compose and ECS platforms. Proposal 001 defines **HOW** that configuration is compiled and realized (the `tokeira-platform` framework, the `Realizer` trait, the kind-library/realizer split). The two are complementary: 001 is the machinery; 002 is the surface that machinery presents.

The principle: **success is a supremely clear operator configuration surface.** The DSL is then in service of that clarity — its job is to make *editing the surface* concise and simple, not to invent the surface. Before we can make the config terse, we must know exactly which values are genuine operator inputs, which are realization mechanics that must never leak into the operator's hands, and which are server-config that flows in from elsewhere.

The headline, stated up front so it frames everything below:

> **The operator surface is SMALL.** The large majority of `platforms/compose/src/config.rs` and `platforms/ecs/src/config.rs` is **defaults + mechanics**: derived names, validation-locked wiring ports, writeback targets, and internal plumbing. A real operator touches a handful of fields. The minimal compose config that stands up a real (DSQL-backed) deployment is **four lines** (Section 6) — and note compose is the *dev* platform; ECS is production.

A note on method: this surface was assembled by a source sweep and then sharpened by two adversarial passes (an *operator-advocate* lens and a *completeness* lens). Where the adversarial findings were verified against source, they **override the raw catalog**. The three findings that changed classifications are flagged inline and consolidated in Section 5. Verified findings are weighted over the raw catalog throughout.

## 2. Classification framework

Every value in a platform config falls into exactly one **category**, and separately sits somewhere on the **exposed-vs-hardcoded axis**.

### Categories

| Category | Meaning | Operator-facing? |
|----------|---------|------------------|
| **operator-knob** | A genuine deployment input an operator legitimately sets. Sub-tiered 1/2/3 by how often a real operator touches it. | Yes |
| **identity** | Fixed-at-create name (`project_name`, ECS `environment`/`region`) that seeds every derived resource id. Set once; not a tuning knob. | Set once |
| **mechanics** | Realization detail: wiring ports, derived paths, service-connect URLs, build/mirror remaps, SDK pins. Changing it breaks the deployment. | No |
| **structure** | Deployment *shape*: the service roster, capacity-provider roster, module dependency wiring. Belongs in the `.tkd` definition, not as a value knob. | Author-only (`.platform`) |
| **server-config** | Belongs to `TokeiraConfig` (`tokeirad.toml`), what the *running server* reads. Reaches the server via writeback, not as a deployment knob. | No (different file/owner) |

### Operator-knob tiers

| Tier | Meaning | Test |
|------|---------|------|
| **Tier 1** | Essential to stand up a real deployment. The operator must consciously choose. | "Can't ship without deciding this." |
| **Tier 2** | Commonly tuned per deployment. Has a sane default but operators routinely override. | "Reach for it on a normal day." |
| **Tier 3** | Advanced / rare. Air-gap mirroring, poll cadences, advanced sizing. Default is almost always fine. | "Most operators never touch it." |

### The exposed-vs-hardcoded axis

Orthogonal to category: is the value **exposed** as a config field today, or **hardcoded** in Rust?

- **exposed** — already a config field.
- **hardcoded → PROMOTE** — currently baked into Rust but *should* be an operator-knob.
- **exposed → DEMOTE** — currently a config field but is really mechanics/server-config and should not sit in the operator surface.
- **exposed but DEAD** — a config field with **zero consumers**; it does nothing. Worse than hardcoded, because it lies to the operator.

The DEAD subcategory is the sharpest adversarial finding and recurs below.

### Lifecycle: create-time-immutable vs editable

A third axis, orthogonal to category and exposure, governs *when* a value may be set — and it is decisive for the apply model:

- **create-time-immutable** — chosen once when the deployment is created, then recorded (`inputs.toml` / manifest). It either seeds derived resource names or chooses the backing store, so changing it later would rename or replace live resources. Editing it is a **retarget** the provisioner must refuse, not reconcile. Members: `project_name`, `region`, ECS `environment`, and **`storage`** — flipping `InMemory ↔ DSQL` is a destructive re-platform, not a config edit.
- **editable** — changed freely via a config edit + `apply` (the Requirement 16 evolution envelope): images, replicas, ports, sizing, retention.

**The test:** *does this value seed a derived resource name, or choose the backing store?* If yes, it is create-time-immutable — even when it is a genuine operator choice (`storage` is an `operator-knob` that is nonetheless immutable after create).

## 3. The compose surface

**Compose is the dev-oriented platform** — five services on one host via `docker compose`, for local development and testing (including testing persistence against DSQL). It is not a production target; ECS (Section 4) is. The dev framing matters for tiering: concerns that are urgent in production — Grafana credential secrecy, retention policy, replica sizing — are routine non-issues on a developer's machine, so they sit lower here than a naïve read of the fields suggests. The genuinely *editable* compose surface is essentially one knob — `tokeirad.image` (which build to test) — plus host-port escapes for collisions; the storage/DSQL decisions are create-time-immutable. Ports are **host-published** (symmetric `host:container`), which is why several stay operator-knobs here that demote to mechanics on ECS.

### 3.1 Tier 1 — must decide at create

Tier-1 splits by **lifecycle**: the storage/DSQL decisions are **create-time-immutable** (chosen once, recorded, never edited — flipping them is a re-platform, not an apply); only the image is an **editable** knob.

| Knob | Default | State | Source | Rationale |
|------|---------|-------|--------|-----------|
| `storage` | `InMemory` | **create-time** · exposed | `config.rs:94` (default `:56`) | The backing-store choice: in-memory dev vs DSQL persistence. Gates the DSQL module, AWS clients, writeback. **Immutable after create** — operators may *not* flip `InMemory ↔ DSQL`; it is a destructive re-platform. |
| `dsql.mode` | `managed` | **create-time** · exposed | `config.rs:18` | `managed` provisions a cluster; `preexisting` adopts one (then `dsql.endpoint` required, `modules.rs:166`). |
| `dsql.region` | `us-east-1` | **create-time** · exposed | `config.rs:24` (default `:52`) | Where the cluster lives; seeds the cluster identity. Moving it post-create is a retarget. |
| `dsql.endpoint` | `None` | **create-time** · exposed | `config.rs:21` | Operator-supplied in `preexisting`; written back in `managed`. |
| `tokeirad.image` | `tokeirad:latest` | **editable** · exposed | `config.rs:62,113` | The developer's primary knob — which tokeirad build to run. Local build vs pinned tag; also the local-build gate sentinel (`gates.rs`). |

### 3.2 Operator-knobs — Tier 2 (commonly tuned)

| Knob | Default | State | Source | Rationale |
|------|---------|-------|--------|-----------|
| `tokeirad.grpc_port` | `7233` | exposed | `config.rs:63,114` | Host-published gRPC port; operators remap to avoid host collisions. |
| `tokeirad.replicas` | `1` | exposed | `config.rs:65,116` | Replica count; drives desired scale. Common capacity tuning. |
| `observability.grafana_port` | `3000` | exposed | `config.rs:86,129` | Host-published Grafana UI port; commonly remapped for collisions. |
| **`observability.loki_retention_hours`** | `168` (7d) | **hardcoded → PROMOTE** | `observability_config.rs:112` | Routine cost/compliance knob; 7 days is short. Template plumbing already exists (`retention_hours → loki.yaml`; proptest `1..=720` at `:705`), so promotion is low-risk. **NB (verified):** unlike ECS, compose *applies* its retention — see Section 5. |
| **Grafana admin `user`/`password`** | `admin`/`admin` | **hardcoded → PROMOTE** | `compose.rs:139-140` | Both hardcoded plaintext. **On a dev stack `admin/admin` is acceptable** — low urgency here; the real driver is converging on the ECS generated-secret model for any shared/non-local use (§5.1). |

### 3.3 Operator-knobs — Tier 3 (advanced / rare)

| Knob | Default | State | Source | Rationale |
|------|---------|-------|--------|-----------|
| `tokeirad.metrics_port` | `9090` | exposed | `config.rs:64,115` | Host-published Prometheus scrape port; feeds `metrics_target_port`. Same publish mechanism as `grpc_port` — see the tiering note below. |
| `observability.{mimir,loki,grafana,alloy}_image` | pinned upstream refs | exposed | `config.rs:71,77,73,79` | Version-pin / private-mirror (air-gap) knobs. Defaults are pinned. Rare. |
| `observability.{mimir,loki,grafana,alloy}_replicas` | `1` | exposed | `config.rs:72,78,75,81` | Single-host compose rarely scales the monitoring stack. |

> **Tiering note (`metrics_port`, verified):** `metrics_port` is host-published symmetrically (`compose.rs:115`), identically to `grpc_port`, and `observability_config.rs:105` wires `metrics_target_port` to it so Alloy follows any remap. The catalog tiered `grpc=2` / `metrics=3`. The asymmetry is justified only if the rationale is *operators hit gRPC directly but rarely the scrape port*; on raw mechanism they are equivalent. Recorded as a minor open call (Section 8).

### 3.4 Not operator config (compose)

| Value | Category | Source | Why not a knob |
|-------|----------|--------|----------------|
| `project_name` | identity | `config.rs:91,109` | Seeds `dsql-<name>-compose`, dynamodb tables, compose project name, obs labels. Set once. |
| `dsql.arn` | operator-knob (tier-3, advisory) | `config.rs:24` | Metadata recorded for adopted clusters only; rare. |
| **`state_dir` / `LocalStateModule.state_dir`** | **mechanics / derived** | `lib.rs:160`; `modules.rs:24,41` | **RECLASSIFIED (verified).** The catalog called this a tier-3 config-field knob. There is **no `ComposeConfig` field** backing it; `lib.rs:160` sets it unconditionally to `deployment_dir.join("state")`. An operator cannot relocate it via `deployment.toml`. Derived, not exposed. |
| `deployment_dir` | mechanics / derived | `config.rs:103` | `serde(skip)`; CLI-populated base path. |
| service roster + order (`mimir, loki, tokeirad, grafana, alloy`) | structure | `lib.rs:77-100` | The 5-service shape; `valid_services` rejects anything else. Per-service replica *count* is the knob. |
| module roster + storage-driven dependency inversion | structure | `modules.rs:298-302` | Deployment-shape rule derived from `storage` kind. |
| DSQL resource-id formulas (`-compose` suffix, coordination tables) | mechanics | `modules.rs:201,206,237` | Deterministic naming from `project_name` + fixed suffixes. |
| DynamoDB coordination-table schema (pk/Hash, ttl, OnDemand, `ManagedBy=tkr`) | mechanics | `modules.rs:178,225` | Fixed schema the coordination code depends on. |
| `GF_METRICS_ENABLED=true` | mechanics | `compose.rs:141` | Observability wiring, not tuning. |
| `namespace "default"` (required_namespaces) | mechanics | `lib.rs:199` | Single seeded bootstrap namespace; operators create more via API. |
| internal obs URLs/ports (mimir 9009, loki 3100, push/datasource URLs) | mechanics | `observability_config.rs:108-111` | Compose-network wiring; changing breaks services. |
| published upstream-stack ports (mimir 9009, loki 3100, alloy 4317/4318, `docker.sock`) | mechanics | `compose.rs:90,100,150` | Canonical Grafana-stack/OTLP ports + log-discovery socket. |
| derived host volume/mount paths (`.tokeira-state/*`, `config/*`, `/etc/tokeira/tokeirad.toml`) | mechanics / derived | `compose.rs:22-62` | Layout derived from `deployment_dir`; container paths fixed. |
| DSQL AWS credential pass-through (`~/.aws`, `AWS_PROFILE/…` forwarded) | mechanics / derived | `compose.rs:67-83` | Ambient shell credentials for IAM; secrets in env, never in config. |
| writeback dotted keys → `tokeirad.toml` (`infrastructure.*`) | server-config | `lib.rs:288-319` | Targets belong to `TokeiraConfig`; the bridge is mechanics. |
| `prototypical_server_config` seeds (`replace-with-dsql-endpoint`, `us-east-1`, `Dsql`) | server-config | `lib.rs:142-144` | Placeholders in the generated `tokeirad.toml`; writeback replaces them. |
| build-source repo/tag + mirror remap (`tokeira/tokeirad:latest`, mirror suffixes) | mechanics | `images/tokeirad.rs:16-32` | Build/mirror realization; writeback lands in the `*_image` knobs. |
| dashboards/alerts artifacts (10 dashboards + alerts yaml) | structure | `observability_config.rs:18-49,243` | Baked authored content + rendered config tree. |
| local-build gate sentinel + remediation (`tokeirad:latest` ×3) | mechanics | `gates.rs:30-38` | Magic-value convention tied to `tokeirad.image`. The triple literal is a maintenance smell — extract to a const. |
| compose file name / state subdir / `BehaviorVersion::latest()` | mechanics | `lib.rs:208,213`; `compose.rs:22` | Fixed file/dir names + SDK pin. |
| **`observability.{aws_cli,busybox}_image`** | **mechanics → DEMOTE** | `config.rs:83,85` | Referenced by **no** compose service (mirror-only). The compose config generator even emits the comment `# populated by tkr image mirror for ECS deployments` (`lib.rs:125-130`) — a **cross-platform leak**: the compose surface advertises ECS-only fields. |

## 4. The ECS surface

ECS is the production-scale platform: a real VPC, an ECS-on-EC2 cluster with per-plane Auto Scaling Groups, an internal ALB, and DSQL. It adds whole **knob families compose lacks** — networking, capacity/ASG sizing, ALB/TLS — and, critically, it **validation-locks** many values that look like knobs (canonical ports, CPU/mem pairs). The lock is the point: in ECS, security groups, Service Connect, and wait-for all assume the canonical wiring, so exposing those ports would be a footgun.

### 4.1 Tier 1 — must decide at create

Some Tier-1 values are **create-time-immutable** (they seed derived resource names — editing post-create is a retarget): `environment`, `region`, and the DSQL ownership/identity decisions. The capacity/replica knobs are editable.

| Knob | Default | State | Source | Rationale |
|------|---------|-------|--------|-----------|
| `environment` | `dev` | **create-time** · exposed | `config.rs:52,324` | **Create-time identity.** A free-form **string label** (default `"dev"` — there is **no** `dev`/`staging`/`prod` enum; the field accepts any value). Seeds the DSQL cluster name `{project}-{env}` (`modules/dsql.rs:56`), the `{project}/{env}` secret/state path (`lib.rs:338`), and telemetry labels — so editing it post-create renames live resources. |
| `region` | `eu-west-2` | **create-time** · exposed | `config.rs:53,325` | **Create-time.** Drives AWS client region, VPC endpoint DNS, state bucket suffix; seeds derived names. Required, fixed at create. |
| `capacity_providers.*.{min,desired,max}_capacity` | runtime 1/3/16, edge 1/2/8, projection 1/1/8, control 1/1/3, obs 1/1/1 | exposed | `config.rs:108-119,362` | ASG scaling bounds per plane; the core sizing knob. Validated `min<=max`, `desired<=max`. |
| `services.*.desired_count` | edge 2, projection/controller/autoscaler 1, admin 0 | exposed | `config.rs:139,399` | Per-service replica counts (runtime is daemon). Admin defaults 0 (on-demand run-task). |
| `dsql.mode` | `managed` | exposed | `config.rs:185,467` | `managed` provisions DSQL; `preexisting` requires the 5 identity fields below (`require_preexisting`). |
| `dsql.{endpoint, management_endpoint_id, connection_endpoint_id, runtime_role_arn, admin_role_arn}` | `None` | exposed | `config.rs:186-190,468` | In `preexisting`, tier-1 operator-supplied identities of an existing cluster (validated). In `managed`, hydrated from infra state + written back. Distinct from `TokeiraConfig.infrastructure.dsql` (server-config). |

### 4.2 Operator-knobs — Tier 2 (commonly tuned)

| Knob | Default | State | Source | Rationale |
|------|---------|-------|--------|-----------|
| `tags` | `{}` (operator-**additional**) | exposed | `config.rs:54,326` | Operator's **extra** free-form tags (cost-center/owner), layered on top of a baseline. The empty default does **not** mean untagged: the `tokeira-aws` resource layer stamps a baseline `Name` / `Project` (=`project_name`) / `ManagedBy=tokeira-cli` on every resource (`dynamodb_table.rs:98-100`, `iam_role.rs:574-576`, `security_group.rs:528-530`) and merges these on top. Commonly extended, not required. |
| `networking.vpc_cidr` | `10.0.0.0/16` | exposed | `config.rs:75,351` | Operators with existing networks/peering must avoid overlap. Validated non-empty. |
| `networking.availability_zones` | `[eu-west-2a, eu-west-2b]` | exposed | `config.rs:76,352` | AZ spread; tuned per region/HA. Validated `>=1`. |
| `capacity_providers.*.instance_type` | `c8g.large` / `m8g.large` / `c8g.medium` | exposed | `config.rs:107,362` | Per-plane EC2 class; cost/perf tuning. |
| `services.*.cpu` / `memory_mb` | replica 512/1024, runtime 1024/2048 | exposed | `config.rs:140-141,420` | Task sizing. Constrained by `validate_cpu_memory` (Fargate valid-pair table) + `validate_resource_sufficiency` (must exceed alloy sidecar + init + N×wait_for). |
| `observability.*_cpu` / `*_memory_mb` | mimir/loki 1024/2048, grafana 512/1024, alloy 128/256 | exposed | `config.rs:204-218,481` | Obs task sizing. **Coupled knob:** `alloy_cpu/memory` is added as per-task sidecar overhead to *every* service in `validate_resource_sufficiency`, so bumping it raises the minimum task size everywhere. |
| `alb.listener_protocol` | `http2` | exposed | `config.rs:169,456` | `http2` (plaintext h2c internal) vs `https` (TLS). `https` requires `certificate_arn` (validated). |
| `alb.certificate_arn` | `None` | exposed | `config.rs:170,457` | REQUIRED when `listener_protocol=https` (`MissingCertificateArn`). |
| `observability.retention_days` | `30` | **exposed but DEAD → see §5** | `config.rs:223,494` | **RECLASSIFIED (verified).** Intended as the metrics/logs retention knob, but read **nowhere** outside its definition; mimir/loki get `&[]` command args (`services.rs:189,201`) and ECS renders no mimir/loki yaml, so containers run image-default retention. The field has **zero effect**. |

### 4.3 Operator-knobs — Tier 3 (advanced / rare)

| Knob | Default | State | Source | Rationale |
|------|---------|-------|--------|-----------|
| `cluster.name` | `tokeira` | exposed | `config.rs:68,342` | ECS cluster name; rarely changed from project default. |
| `networking.optional_endpoints.{sts,kms,secrets_manager,cloudwatch_logs,ec2}` | all `false` | exposed | `config.rs:83-88` | Extra interface VPC endpoints; needed only when private subnets must reach those AWS services privately. |
| `autoscaler.polling_interval_secs` | `15` | exposed | `config.rs:162,447` | Poll cadence (reactivity vs API load). |
| `alb.health_check_interval_secs` | `15` | exposed | `config.rs:172,459` | Health-check cadence (failover sensitivity vs noise). |
| `observability.{mimir,loki,grafana,alloy}_image` | pinned upstream refs | exposed | `config.rs:203-218,480` | Mirrored into ECR by `tkr image mirror`. Version-pin / air-gap knob. |

### 4.4 Not operator config (ECS)

| Value | Category | Source | Why not a knob |
|-------|----------|--------|----------------|
| `project_name` | identity | `config.rs:51,323` | Seeds every derived name (`{project}-state-{region}`, `tokeira-*`, `{project}-{env}`, `{project}/grafana/admin`, SSM `/{project}/alloy/*`). |
| **`cluster.service_connect_namespace`** | **DEAD → remove** | `config.rs:69,343` | **RECLASSIFIED (verified).** Consumed at **zero** call sites; `networking.private_dns_zone` is what's actually read (7 sites). Stronger than the catalog's "duplicate" — it's a pure dead field. |
| `networking.private_dns_zone` | mechanics | `config.rs:77,353` | The Service-Connect zone actually consumed (alloy `mimir.<zone>:9009`). Wiring. |
| `required_vpc_endpoints` roster | mechanics | `config.rs:500-522` | Mandatory AWS endpoints; interpolated with region. |
| `capacity_providers` roster (8) | structure | `config.rs:93-102` | The capacity-provider roster *is* the deployment shape. |
| `capacity_providers.runtime.scale_in_protection` | mechanics (safety invariant) | `config.rs:120,369` | Required by the drain loop for the DAEMON runtime fleet; operator should not disable. |
| observability capacity `1/1/1` invariant | mechanics | `config.rs:373-375`; `lib.rs:387` | Single-host invariant `tkr port-forward` relies on; `desired=max=1` enforced. (`instance_type`/`min` stay knobs.) |
| `services` roster (7: 6 replica + runtime daemon) | structure | `config.rs:125-133` | Roster + daemon-vs-replica distinction *is* the topology. |
| **`services.*.image` / `autoscaler.image`** | **mechanics → DEMOTE** | `config.rs:138,150,161` | Populated by `tkr image push` from the resolved ECR ref (`annotate_image_lifecycle_fields`, `lib.rs:61`). Realization **output**, not a hand-authored knob. |
| **`services.*.grpc_port`** | **mechanics → DEMOTE (split)** | `config.rs:142,153` | **SPLIT (verified).** `expect_port` enforces canonical for exactly **4** services (`config.rs:599-617` → edge-api 7233, edge-poll 7234, controller 7240, runtime 7241). projection/autoscaler/admin (7242/7243/7244) are **unguarded defaults** — editable, accepted, yet consumed as fixed wiring → silent breakage. See §5. |
| **`services.*.metrics_port`** | **mechanics → DEMOTE** | `config.rs:143,623` | `expect_metrics` forces 9090 for all 7 (`config.rs:623-629`); alloy scrape + obs env assume it. |
| **`services.*.http_port`** | **mechanics → DEMOTE (vestigial)** | `config.rs:144,155` | Never read in module/service construction. Carries nothing. |
| `alb.name` | mechanics | `config.rs:168,455` | Internal ALB resource name; drives `alb-*` ids. |
| **`alb.health_check_path`** | **mechanics → DEMOTE-adjacent** | `config.rs:171,458` | `/healthz` is fixed by the tokeirad binary; not tunable without changing the server. |
| **`observability.{aws_cli,busybox}_image`** | **mechanics → DEMOTE-adjacent** | `config.rs:219-222` | Utility init/sidecar images; overridable only for air-gap mirroring. |
| **`observability.loki_query_url`** | **mechanics → DEMOTE** | `config.rs:226` | Client-side endpoint for `tkr logs` assuming a `tkr port-forward loki` tunnel. CLI wiring, not a deployment value. |
| grafana admin username | **hardcoded → PROMOTE (mild, tier-3)** | `modules/observability.rs:72` | Generated-password secret pins `admin`; operators often want a non-default user. (Password is generated — no plaintext issue.) |
| grafana password_length | **hardcoded → PROMOTE (mild, tier-3)** | `modules/observability.rs:73` | Generated length `32`; a plausible policy knob (password never persisted to config). |
| grafana secret recovery_window_days | mechanics | `modules/observability.rs:75` | Secrets Manager deletion window `7`; AWS convention. |
| grafana admin secret env wiring (`GRAFANA_ADMIN_PASSWORD ← {project}/grafana/admin`) | mechanics / derived | `services.rs:342` | Env-from-secret; ECS resolves at task start. |
| alloy `scrape_interval` `15s` | mechanics | `modules/observability.rs:330` | Baked into rendered alloy config. |
| alloy runtime flags (listen `0.0.0.0:12345`, `generally-available`, `/tmp/alloy-<port>`) | mechanics | `services.rs:576-579` | Realization flags. |
| `TOKEIRA_OBSERVABILITY_LOG_FORMAT="json"` | server-config / mechanics (**record a decision**) | `services.rs:494` | **MISSED (verified).** Hardcoded literal on every ECS task; no config field. Faint tier-3 claim (logfmt for some pipelines) but configures the running server's telemetry. ECS-only (compose tokeirad reads only `tokeirad.toml`). Record a decision rather than silently bake `json`. See §5/§8. |
| `TOKEIRA_OBSERVABILITY_{SERVICE,CLUSTER,DEPLOYMENT,METRICS_ADDR}` | server-config / derived | `services.rs:478-493` | **MISSED (verified).** Four derived env vars (identity + canonical port) injected per task. Correctly mechanics; they form the **identity→telemetry env bridge** (distinct from the DSQL writeback bridge). |
| `WAIT_FOR_CPU/MEMORY` + `ALLOY_CONFIG_INIT_CPU/MEMORY` | mechanics | `config.rs:43-46` | Fixed sidecar/init reservations feeding `validate_resource_sufficiency`. |
| per-service `wait_for_count` topology | mechanics | `config.rs:633-729` | Init-container counts encoding each service's dependency-wait; constrains the cpu/mem knobs. |
| `validate_cpu_memory` valid-pair table + capacity range + preexisting-required rules | mechanics (invariants) | `config.rs:544-596,232` | Pin the *legal ranges* of the cpu/mem/capacity/dsql knobs. Not values themselves. |
| derived resource ids + service-connect URLs + S3/roles + SSM paths + SG ports | mechanics / derived | `lib.rs:333-339`; `modules/*` | All computed from identity + canonical ports. |
| observability fixed ports (mimir 9009, loki 3100, grafana 3000) | mechanics | `services.rs:187-213` | Canonical container/discovery ports. |
| S3 versioning + IAM managed policies + `ManagedBy` tags | mechanics | `modules/observability.rs:245`; `cluster.rs:55` | Always-on versioning + required AWS managed policies for ECS-on-EC2 + SSM. |
| dashboards/alerts artifacts (10) + alloy fan-out roster (10) | structure | `modules/observability.rs:160-240` | Fixed roster shape. |
| image repository naming + writeback targets | mechanics / derived | `images/mod.rs:50-145` | ECR repo names from project; writeback targets are the image fields. |
| EC2 AMI / root EBS / SSH key / DSQL deletion-protection | mechanics (`tokeira_aws`-owned) | `modules/cluster.rs:78-96`; `modules/dsql.rs:55-66` | **Boundary confirmation (verified):** these never appear as platform-config knobs because they are encapsulated in `tokeira_aws` resources (`LaunchTemplateResource` carries only name/instance_type/workload/profile/sg; `DsqlClusterConfig` only mode/endpoint/arn). The canonical examples of "realization detail deliberately not exposed." |

## 5. Cross-cutting findings

### 5.1 PROMOTE — hardcoded values that should become knobs

| Value | Platform | Tier | Source | Action |
|-------|----------|------|--------|--------|
| Grafana `user`/`password` (`admin`/`admin`, plaintext) | compose | 2 (dev: low) | `compose.rs:139-140` | Both hardcoded plaintext. Acceptable on a dev laptop; for shared/non-local use, converge on the ECS generated-secret model (expose username; generate or BYO-secret the password). |
| `loki_retention_hours` (`168`) | compose | 2 | `observability_config.rs:112` | Promote to an `ObservabilityConfig` field; template plumbing exists (proptest `1..=720`). |
| grafana admin username (`admin`) | ECS | 3 | `modules/observability.rs:72` | Mild promote; password already generated. |
| grafana `password_length` (`32`) | ECS | 3 | `modules/observability.rs:73` | Mild promote; password never persisted to config. |

**The Grafana-admin asymmetry (a real gap for non-dev use; low urgency on dev compose).** Compose hardcodes **both** `GF_SECURITY_ADMIN_USER=admin` and `GF_SECURITY_ADMIN_PASSWORD=admin` as **plaintext container env** (`compose.rs:139-140`). ECS does it correctly: a 32-char password generated into Secrets Manager and injected via `GRAFANA_ADMIN_PASSWORD` env-from-secret; only the username and length are hardcoded. **The `.tkd`/realizer should converge both platforms on the ECS model** (generated secret), exposing username and optionally a bring-your-own-password secret.

**The retention correction (verified — the catalog's biggest substantive miss).** The raw catalog framed compose's hardcoded `loki_retention_hours` as "just copy ECS, which already exposes `retention_days`." **That framing is false.** Source confirms ECS `observability.retention_days` (`config.rs:223,494`) has **zero consumers**: mimir and loki receive `&[]` command args (`services.rs:189,201`) and ECS renders no mimir/loki yaml, so they run image-default retention. So the true finding is: **neither platform reliably applies operator-chosen retention to the running store** — compose hardcodes 168h (at least *applied*); ECS exposes a **no-op** field (arguably worse, because it lies). The realizer must **actually wire retention to mimir/loki config on both platforms**, not merely expose a field.

### 5.2 DEMOTE — config fields that are really mechanics / dead

| Value | Platform | State | Source | Why |
|-------|----------|-------|--------|-----|
| `services.*.image` / `autoscaler.image` | ECS | machine-populated | `config.rs:138,150,161`; `lib.rs:61` | Writeback output from `tkr image push`, not a hand-authored input. |
| `services.*.metrics_port` | ECS | validation-locked | `config.rs:143,623` | `expect_metrics` forces 9090 for all 7. |
| `services.*.grpc_port` | ECS | **split** | `config.rs:142,599-617` | 4 services canonical-enforced; projection/autoscaler/admin **unguarded** (hazard below). |
| `services.*.http_port` | ECS | **vestigial / dead** | `config.rs:144,155` | Never read. |
| `cluster.service_connect_namespace` | ECS | **fully dead** | `config.rs:69` | Zero consumers; `private_dns_zone` is the real one. |
| `alb.health_check_path` | ECS | binary-fixed | `config.rs:171` | `/healthz` fixed by tokeirad. |
| `observability.loki_query_url` | ECS | CLI wiring | `config.rs:226` | `tkr logs` localhost tunnel endpoint. |
| `observability.{aws_cli,busybox}_image` | ECS | air-gap-only utility | `config.rs:219-222` | Init/sidecar helper images. |
| `observability.retention_days` | ECS | **dead (no-op)** | `config.rs:223` | Zero consumers — *but the fix is to WIRE it, not delete it* (§5.1). |
| `observability.{aws_cli,busybox}_image` | compose | mirror-only, unreferenced | `config.rs:83,85` | No compose service references them; config leaks an ECS-only comment (`lib.rs:125`). |
| `state_dir` | compose | derived, no config backing | `lib.rs:160` | Not relocatable via `deployment.toml`. |

**Unguarded-knob hazard (verified).** ECS `projection`/`autoscaler`/`admin` `grpc_port` (defaults 7242/7243/7244 at `config.rs:402-405`) are **editable** in `deployment.toml`, accept any value, pass validation (`expect_port` covers only the other 4, `config.rs:599-617`), yet are still consumed as fixed wiring (security groups, Service Connect assume the defaults). An operator who edits one gets a **silently broken deployment with no validation error** — strictly worse than the guarded four. The `.tkd` work should either (a) treat all seven as mechanics with **no operator-facing field**, or (b) extend `expect_port` to cover all seven. Option (a) is preferred — they are wiring, not knobs.

### 5.3 The server-config vs deployment-config boundary

Two distinct files with distinct owners:

1. **Deployment config** (`ComposeConfig` / `EcsConfig` in `deployment.toml`) — the **shape and inputs** of the infra to provision and the services to run. This is the surface this document defines.
2. **Server config** (`TokeiraConfig` in `tokeirad.toml`, `crates/tokeira-config`) — what the **running server** reads.

Two bridges carry values from deployment → server, and **neither side may model the other's fields as deployment knobs**:

- **DSQL writeback bridge.** `tkr infra apply` reads provisioned `InfraState` and writes `infrastructure.storage` / `infrastructure.dsql.{endpoint,region,rate_limiter_table,conn_lease_table,*_role_arn}` into `tokeirad.toml`. Those `TokeiraConfig` fields are **server-config / derived** — they must **not** be modeled as deployment-config knobs. The deployment-side `dsql.*` block (compose `ComposeDsqlConfig`, ECS `dsql.*`) is a **separate target** from `TokeiraConfig.infrastructure.dsql.*`: same logical value, different file/owner.
- **Identity→telemetry env bridge (ECS, verified).** `TOKEIRA_OBSERVABILITY_{SERVICE,CLUSTER,DEPLOYMENT,METRICS_ADDR}` (`services.rs:478-493`) flow deployment-identity → server-telemetry env, distinct from the DSQL writeback. `TOKEIRA_OBSERVABILITY_LOG_FORMAT="json"` (`services.rs:494`) rides the same bridge as a hardcoded literal — record a decision (§8).

**Identity & create-time recap.** `project_name` (both platforms), ECS `environment`+`region`, and **`storage`** are all fixed-at-create and recorded — they seed derived ids or choose the backing store, so editing any of them is a *retarget*, not an apply (the lifecycle axis, §2). `environment` is a free-form **string label** (no `dev`/`staging`/`prod` enum), defaulting to `"dev"`.

**Local platform (for completeness).** `LocalConfig` carries only `project_name` (identity); a single fixed tokeirad process, no modules/services/images; rejects scale/logs/port-mappings. **No operator knobs.**

## 6. The minimal config

The smallest real config an operator writes for a **DSQL-backed compose deployment** (compose is dev-oriented; this is the persistence-testing case) — note these are all **create-time** values; everything else defaults away:

```toml
[platform]
project_name = "acme"          # identity, fixed at create
storage = "dsql"               # create-time: DSQL persistence vs default in-memory (immutable after create)

[dsql]
# mode defaults to "managed" (tokeira provisions the cluster)
region = "eu-west-2"           # create-time: required outside the us-east-1 default
# endpoint/arn left unset: managed mode writes endpoint back after `tkr infra apply`
# all of the above are create-time-immutable; editing them post-create is a retarget, not an apply
```

Everything else defaults:

- `tokeirad.image = "tokeirad:latest"` (built by `tkr image build`)
- `tokeirad.grpc_port = 7233`, `metrics_port = 9090`, `replicas = 1`
- `observability.*` images pinned, `replicas = 1`, `grafana_port = 3000`

Preexisting DSQL instead is still tiny:

```toml
[dsql]
mode = "preexisting"
region = "eu-west-2"
endpoint = "my-cluster.dsql.eu-west-2.on.aws"
```

> Today, Grafana `admin/admin` and Loki retention `168h` are **not** settable — that is the gap the PROMOTE list (§5.1) closes. Once promoted, the secure-by-default minimal config still stays short, because the realizer generates the Grafana secret rather than asking the operator to supply it.

**The takeaway for the DSL.** This four-line surface is the proof of the headline: the operator surface is small. The DSL's job (Proposal 001) is to make *this* concise and obvious, and to make the long tail of Tier-2/3 knobs discoverable but invisible until reached for.

## 7. Implications for the DSL

Proposal 001 supplies the compile/realize machinery (kind library + realizer + authored `.platform`). This document's surface dictates how that machinery should *feel* to an operator:

1. **Make Tier-1 trivial — and split create-time from editable.** The create-time-immutable inputs (`storage`, `dsql.region`/`mode`/`endpoint`, ECS `environment`/`region`) are chosen once at create and recorded; the DSL/provisioner must *refuse to edit* them (retarget, not reconcile). The editable Tier-1/2 knobs (capacity, `desired_count`, image, `alb` TLS) reconcile on `apply`. Both must be front-and-centre, with the cross-field constraints (`preexisting` ⇒ endpoint required; `https` ⇒ `certificate_arn`) expressed as kind-library constraints (per 001), not realizer logic.
2. **Defaults hide Tiers 2–3.** Every Tier-2/3 knob has a sane default and should be **absent** from the minimal config. The DSL must let an operator omit them entirely and surface them only when overridden. Pinned image refs, replica counts, poll cadences, sizing — all default-away.
3. **Mechanics are not in the surface at all.** Wiring ports, derived names, service-connect URLs, build/mirror remaps, and the dead/vestigial fields (`http_port`, `service_connect_namespace`, `loki_query_url`, compose `aws_cli/busybox_image`) must **not** be operator-settable kinds. The clean-up actions: delete the dead fields; treat all ECS `grpc_port`/`metrics_port` as mechanics (close the unguarded-port hazard); extract the triple `tokeirad:latest` sentinel to a const.
4. **Secrets are generated, not authored.** Converge both platforms on the ECS generated-secret model for the Grafana admin password; expose only username (+ optional BYO secret reference). The DSL needs a **secret-typed** field that never round-trips a plaintext default into a config file.
5. **Retention must reach the store.** Expose retention on both platforms **and** wire it through the realizer to mimir/loki config — exposing a no-op field (current ECS `retention_days`) is worse than hardcoding.
6. **Expressiveness in service of simplicity.** The DSL's expressiveness (per 001) exists to keep this small surface concise and the constraints declarative — not to widen the surface. If a feature does not make the four-line minimal config clearer or a Tier-1 decision safer, it does not belong.

## 8. Open product judgments

These are the user's calls, not source facts:

| Question | Context | Default recommendation |
|----------|---------|------------------------|
| **Grafana password story** | Compose hardcodes plaintext `admin`; ECS generates into Secrets Manager. | Generate on both (ECS model). Expose username; optional BYO-secret reference. The strongest single recommendation. |
| **Retention tiering & wiring** | Both platforms fail to apply operator retention (compose hardcodes 168h; ECS field is a no-op). | Expose `retention_days` on both at Tier-2 **and wire it through the realizer**. Non-negotiable that it actually reaches the store. |
| **Observability image-version tiering** | `*_image` fields are version-pin / air-gap knobs (Tier-3) today. | Keep Tier-3, exposed but defaulted-away. Decide whether air-gap mirror overrides deserve a dedicated `[mirror]` block vs per-image fields. |
| **`compose tokeirad.metrics_port` tier** | Same host-publish mechanism as `grpc_port` (Tier-2) but tiered 3. | Either align both to Tier-2 (collision avoidance) or document the rationale (gRPC hit directly, scrape port rarely). Minor. |
| **`TOKEIRA_OBSERVABILITY_LOG_FORMAT`** | Hardcoded `"json"` on every ECS task; faint Tier-3 logfmt claim; ECS-only. | Record an explicit decision (keep server-config/mechanics, or expose Tier-3) rather than silently baking `json`. |
| **ECS scope for the first `.tkd`** | ECS adds networking/capacity/ALB families and many validation-locked values. | Decide whether the first ECS `.tkd` ships the full Tier-1+2 surface or starts with a managed-DSQL/default-capacity subset and grows. |
| **Unguarded ECS grpc ports** | projection/autoscaler/admin grpc are editable but unvalidated wiring. | Treat all seven as mechanics (no operator field). Cheapest correct fix; closes the silent-breakage hazard. |

---

*This document defines the operator configuration surface. Proposal 001 defines the framework that compiles and realizes it. Together: **define/compile** (`tokeira-platform-dsl`) → **realize** (`tokeira-platform`), presenting the small, clear surface above.*
