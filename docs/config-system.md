# Tokeira Configuration System — As Implemented

This document describes the configuration system as it currently exists in the codebase, not as the spec envisions it. Use this to understand what's actually running.

## Overview

There are **three separate config systems** in play, each with its own model, loader, and purpose:

```
┌─────────────────────────────────────────────────────────────────┐
│                        tkr CLI                                   │
│                                                                  │
│  deployment create --platform local --storage in-memory          │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────────┐    ┌──────────────────┐                    │
│  │ deployment.toml  │    │  tokeirad.toml    │                   │
│  │ (platform config)│    │  (server config)  │                   │
│  │                  │    │                   │                    │
│  │ LocalConfig      │    │ TokeiraConfig     │                   │
│  │ ComposeConfig    │    │                   │                    │
│  └────────┬─────────┘    └────────┬──────────┘                   │
│           │                       │                              │
│           ▼                       ▼                              │
│  Orchestrator framework     tokeirad --config <path>             │
│  (infra/deploy engines)     (server binary startup)              │
└─────────────────────────────────────────────────────────────────┘
```

## Config 1: TokeiraConfig (Server Runtime)

**Owner:** `crates/tokeira-config/src/lib.rs`

**Loaded by:** `tokeirad --config <path>` or `TOKEIRA_CONFIG` env var or defaults

**Purpose:** Controls how the `tokeirad` server process behaves at runtime.

### Model

```
TokeiraConfig
├── infrastructure
│   ├── cluster_name: String          ("tokeira-local")
│   ├── region: String                ("us-east-1")
│   ├── dsql
│   │   └── endpoint: Option<String>  (None — metadata only, not wired to storage)
│   ├── network
│   │   ├── grpc_addr: String         ("[::1]:7233")
│   │   └── metrics_addr: String      ("0.0.0.0:9090")
│   └── observability
│       ├── metrics_enabled: bool     (true)
│       ├── otlp_enabled: bool        (false)
│       ├── otlp_endpoint: String     ("http://localhost:4317")
│       ├── otlp_protocol: grpc|http  (grpc)
│       ├── trace_sample_rate: f64    (1.0)
│       ├── log_format: text|json     (text)
│       └── log_filter: String        ("info")
├── policy
│   ├── default_retention_days: u32   (30)
│   ├── namespace_creation: open|controlled (open)
│   └── quotas
│       ├── max_workflow_timeout_seconds: u64 (315_360_000)
│       └── max_signal_payload_bytes: u32    (4_194_304)
├── capacity
│   ├── performance
│   │   ├── target_workflow_starts_per_second: u32 (1000)
│   │   └── target_p99_wft_latency_ms: u32        (50)
│   └── dsql
│       ├── max_connections: u32              (10_000)
│       ├── connection_rate_per_second: u32   (100)
│       └── burst_capacity: u32              (1_000)
└── emergency
    ├── disable_stickiness: bool      (false)
    ├── freeze_projection: bool       (false)
    └── cap_poll_admission: Option<u32> (None)
```

### Loading Flow

```
1. tokeirad starts
2. Parse CLI: --config <path>, --dump-config
3. TokeiraConfig::resolve(cli.config):
   a. If --config provided → load from file
   b. Else if TOKEIRA_CONFIG env set → load from env path
   c. Else → TokeiraConfig::default()
4. Validate (collect all errors):
   - retention_days ∈ [1, 36500]
   - performance targets > 0
   - trace_sample_rate ∈ [0.0, 1.0]
   - grpc_addr/metrics_addr parseable as SocketAddr
5. If --dump-config → print TOML, exit
6. Log emergency warnings
7. Construct ObservabilityConfig from infrastructure.observability + network.metrics_addr
8. Construct RuntimeConfig::default() (not from TOML — mechanical settings)
9. Read grpc_addr from infrastructure.network
10. Start server
```

### Endpoints

- `GET /config` — returns `to_redacted_json()` (endpoint/arn fields redacted, _warnings for emergency overrides)
- `PUT /loglevel` — runtime log filter change

### Key Facts

- `serde(deny_unknown_fields)` on all structs — typos are caught
- `dsql.endpoint` is metadata only — no storage wiring in this spec
- `RuntimeConfig` is always `Default` — not configurable from TOML
- Full config API (load, resolve, validate, to_toml, to_redacted_json) lives in `tokeira-config` crate

---

## Config 2: LocalConfig (Local Platform)

**Owner:** `platforms/local/src/lib.rs`

**Loaded by:** `tkr` CLI via `tokeira_config_loader::load_config` from `deployment.toml`

**Purpose:** Minimal config for bare-process local execution. No compose, no observability stack.

### Model

```
LocalConfig
├── project_name: String    ("tokeira")
└── state_dir: PathBuf      (".tokeira-state")
```

That's it. Two fields. The local platform spawns tokeirad as a child process — it doesn't need images, ports, replicas, or compose files.

### Generated by

`LocalDeployment::prototypical_config(storage)` → serializes `LocalConfig::default()` to TOML.

---

## Config 3: ComposeConfig (Compose Platform)

**Owner:** `platforms/compose/src/lib.rs`

**Loaded by:** `tkr` CLI via `tokeira_config_loader::load_config` from `deployment.toml`

**Purpose:** Full Docker Compose stack config with observability services.

### Model

```
ComposeConfig
├── project_name: String              ("tokeira")
├── state_dir: PathBuf                (".tokeira-state")
├── compose_file: PathBuf             (".tokeira-state/docker-compose.yml")
├── tokeirad
│   ├── image: String                 ("tokeirad:local")
│   ├── grpc_port: u16                (7233)
│   ├── metrics_port: u16             (9090)
│   └── replicas: u32                 (1)
└── observability
    ├── mimir_image: String           ("grafana/mimir:3.0.6")
    ├── mimir_replicas: u32           (1)
    ├── grafana_image: String         ("grafana/grafana-oss:12.4.3")
    ├── grafana_replicas: u32         (1)
    ├── loki_image: String            ("grafana/loki:3.7.1")
    ├── loki_replicas: u32            (1)
    ├── alloy_image: String           ("grafana/alloy:v1.16.0")
    ├── alloy_replicas: u32           (1)
    └── grafana_port: u16             (3000)
```

### Generated by

`ComposeDeployment::prototypical_config(storage)` → serializes `ComposeConfig::default()` to TOML.

---

## Config 4: Generic Loader (in tokeira-config)

**Owner:** `crates/tokeira-config/src/loader.rs`

**Purpose:** Generic TOML loading with profile merge and variable substitution. Used by `tkr` to load platform configs.

### API

```rust
// Generic: load any T from a TOML file, optionally merging a profile overlay
load_config<T: DeserializeOwned>(base_path: &Path, profile_path: Option<&Path>) -> Result<T>

// Serialize any T to TOML
write_config_toml<T: Serialize>(config: &T) -> Result<String>

// Deep merge and variable substitution helpers
deep_merge(base: &mut toml::Value, override_val: toml::Value)
substitute_project_vars(value: &mut toml::Value, project_name: &str)
```

The `tokeira-config-loader` crate still exists as a thin re-export shim for backward compatibility.

---

## Deployment Directory Layout

```
$XDG_STATE_HOME/tokeira/tkr/          # ~/Library/Application Support/tokeira/tkr/
├── .latest                            # "dev"
├── dev/
│   ├── deployment.toml                # Platform config (LocalConfig or ComposeConfig)
│   ├── tokeirad.toml                  # Server config (TokeiraConfig)
│   ├── metadata.json                  # { name, id, platform, storage, status, timestamps }
│   ├── state/                         # Local backend state store
│   └── docker-compose.yml             # Compose platform only
```

### metadata.json

```json
{
  "name": "dev",
  "id": "uuid",
  "platform": "local",
  "storage": "in-memory",
  "status": "created",
  "created_at": "...",
  "updated_at": "..."
}
```

---

## Flow: `tkr deployment create --platform local --storage in-memory --name dev`

```
1. DeploymentResolver resolves XDG path → ~/Library/Application Support/tokeira/tkr/
2. Validate platform × storage (reject ecs + in-memory)
3. Normalize name → "dev"
4. Create directory: .../tkr/dev/
5. Call LocalDeployment::prototypical_config(InMemory) → write deployment.toml
6. Call LocalDeployment::prototypical_server_config(InMemory) → write tokeirad.toml
7. Write metadata.json
8. Create state/ subdirectory
9. Write .latest → "dev"
```

## Flow: `tkr deploy apply --yes`

```
1. Resolve deployment name from .latest → "dev"
2. Load metadata.json → platform=Local, storage=InMemory
3. Load deployment.toml as LocalConfig
4. Branch on platform:
   Local → spawn tokeirad as blocking child process
     - Find tokeirad binary (PATH or cargo run fallback)
     - Pass --config <path-to-tokeirad.toml>
     - Inherit stdio
     - Forward SIGINT
     - Write PID file
   Compose → use orchestrator DeployEngine + bollard
5. Update metadata status
```

---

## Completed Refinements (Steps 1-2)

1. ✅ **`tokeira-server-config` renamed to `tokeira-config`** — proper crate with full config API, validation, redaction, tests. No more `include!` hack.

2. ✅ **`tokeira-config-loader` consolidated** — generic loader (`load_config<T>`, `write_config_toml<T>`, `deep_merge`, `substitute_project_vars`) moved into `tokeira-config/src/loader.rs`. The `tokeira-config-loader` crate is now a thin re-export shim.

3. ✅ **Deploy-eks artifacts removed** — `ProjectConfig`, `load_project_config`, `load_base_config`, `write_config_values`, `load_dynamic_config` all gone.

4. ✅ **`ObservabilityTomlConfig` renamed to `ObservabilityConfig`** everywhere.

5. ✅ **Deployment directory constants** (`DEPLOYMENT_TOML`, `TOKEIRAD_TOML`, `METADATA_JSON`, `LATEST_FILE`) moved from `main.rs` to `deployment_dir.rs`.

---

## Pending: Steps 3-5 — Config Reshape

### Problem

`TokeiraConfig` is a flat struct. Every field exists regardless of platform or storage choice. A local/in-memory deployment carries `region`, `dsql.endpoint`, and `capacity.dsql.*` fields that have no meaning. An ECS/DSQL deployment would need fields that don't exist yet. The config model doesn't reflect the deployment's actual shape.

### Design: `TokeiraConfig<P, I, S>`

The server config becomes generic over three axes:

```rust
pub struct TokeiraConfig<P, I, S> {
    pub platform: P,
    pub infrastructure: InfrastructureConfig<I>,
    pub policy: PolicyConfig,
    pub capacity: CapacityConfig<S>,
    pub emergency: EmergencyConfig,
}
```

Each type parameter is a concrete config struct chosen by the platform crate:

| Axis | Trait | Local/InMemory | Local/DSQL | Compose/InMemory | Compose/DSQL |
|------|-------|----------------|------------|------------------|--------------|
| `P` | `PlatformConfigSection` | `LocalPlatformSection` | `LocalPlatformSection` | `ComposePlatformSection` | `ComposePlatformSection` |
| `I` | `InfraSection` | `NoInfra` | `DsqlInfra` | `NoInfra` | `DsqlInfra` |
| `S` | `StorageSection` | `NoStorageCapacity` | `DsqlCapacity` | `NoStorageCapacity` | `DsqlCapacity` |

### Traits

```rust
/// Marker trait for platform-specific config sections.
pub trait PlatformConfigSection: Serialize + DeserializeOwned + Clone + Debug + Default {}

/// Marker trait for infrastructure config sections.
pub trait InfraSection: Serialize + DeserializeOwned + Clone + Debug + Default {}

/// Marker trait for storage capacity config sections.
pub trait StorageSection: Serialize + DeserializeOwned + Clone + Debug + Default {}
```

These are marker traits — they exist for bounds, not for method dispatch. Each concrete type carries its own fields.

### Concrete Types

```rust
// --- Platform sections ---

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LocalPlatformSection {} // empty — local has no platform-specific server config

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ComposePlatformSection {} // empty for now — compose-specific server knobs go here later

// --- Infra sections ---

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NoInfra {} // in-memory: no infra config needed

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DsqlInfra {
    pub endpoint: String, // required, not optional
}

// --- Storage capacity sections ---

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NoStorageCapacity {} // in-memory: no capacity knobs

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DsqlCapacity {
    pub max_connections: u32,
    pub connection_rate_per_second: u32,
    pub burst_capacity: u32,
}
```

### Reshaped InfrastructureConfig and CapacityConfig

```rust
pub struct InfrastructureConfig<I> {
    pub cluster_name: String,
    pub network: NetworkConfig,
    pub observability: ObservabilityConfig,
    pub storage: I, // NoInfra or DsqlInfra
}

pub struct CapacityConfig<S> {
    pub performance: PerformanceConfig,
    pub storage: S, // NoStorageCapacity or DsqlCapacity
}
```

`region` moves into `DsqlInfra` — it's only meaningful when DSQL is the storage backend.

### Two-Phase Config Loading in `tkr`

```
1. Read metadata.json → { platform: "local", storage: "in-memory" }
2. Match (platform, storage) to concrete type alias:
     (Local, InMemory)  → TokeiraConfig<LocalPlatformSection, NoInfra, NoStorageCapacity>
     (Local, Dsql)      → TokeiraConfig<LocalPlatformSection, DsqlInfra, DsqlCapacity>
     (Compose, InMemory) → TokeiraConfig<ComposePlatformSection, NoInfra, NoStorageCapacity>
     (Compose, Dsql)     → TokeiraConfig<ComposePlatformSection, DsqlInfra, DsqlCapacity>
3. Deserialize tokeirad.toml into the resolved concrete type
4. Validate
```

### `config show` Through State Backend

Currently `config show` reads files directly from the deployment directory. It should go through the deployment's state backend so it works for both local and remote state.

### Type Aliases

```rust
// Convenient aliases for common combinations
pub type LocalInMemoryConfig = TokeiraConfig<LocalPlatformSection, NoInfra, NoStorageCapacity>;
pub type LocalDsqlConfig = TokeiraConfig<LocalPlatformSection, DsqlInfra, DsqlCapacity>;
pub type ComposeInMemoryConfig = TokeiraConfig<ComposePlatformSection, NoInfra, NoStorageCapacity>;
pub type ComposeDsqlConfig = TokeiraConfig<ComposePlatformSection, DsqlInfra, DsqlCapacity>;
```

### `tokeirad` Impact

`tokeirad` currently uses `TokeiraConfig::default()` or loads from file. After reshape:
- For in-memory mode: uses `LocalInMemoryConfig`
- The binary needs to know its concrete type at compile time (it's always local/in-memory for now)
- Future: `tokeirad` in a container would use the compose or ECS variant

### Migration Path

1. Add the generic struct and traits alongside the current flat struct
2. Implement `From<FlatTokeiraConfig>` for migration
3. Update platform crates to emit the typed config
4. Update `tkr` two-phase loading
5. Update `tokeirad` to use the concrete alias
6. Remove the flat struct

### Known Constraints

- `dsql.endpoint` becomes required (not `Option`) in `DsqlInfra` — if you chose DSQL, you must provide the endpoint
- `region` only exists in `DsqlInfra` — local/in-memory has no region
- `serde(deny_unknown_fields)` stays on all structs — typos are still caught
- `RuntimeConfig` remains `Default`-only — not part of TOML schema
