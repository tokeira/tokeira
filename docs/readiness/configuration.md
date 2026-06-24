# Tokeira configuration — readiness

> Where tokeira's own configuration lives today, the actual surface, and what is needed before the
> initial release. This is the **"after"** to the **"before"** captured in
> [`../conformance/v1.31.0/configuration.md`](../conformance/v1.31.0/configuration.md) (Temporal's 564
> dynamic-config keys + static YAML). Close-to-zero configuration is a headline claim for tokeira; this
> doc is where we make it real and releasable.

**Last updated:** 2026-06-24 · status surface — see [`delivery.md`](./delivery.md) for the workstream index.

## The claim, and the number

tokeira's entire server config is **~70 fields across 4 sections**, and **a valid `tokeirad.toml` is the
empty file** — every field has a default (test: `empty_toml_uses_defaults`). The only fields with no
sensible default are the DSQL endpoint/region when `storage = "dsql"` (rejected by `validate()` if
missing). Contrast: Temporal exposes **564 dynamic-config keys** plus the static YAML topology surface.
That gap *is* the product claim — this doc must let an operator see and trust it.

## The surface (grounded in `crates/tokeira-config/src/lib.rs`)

**`tokeirad.toml` = `TokeiraConfig`**, four sections, all `#[serde(deny_unknown_fields)]` (typos fail at
parse time):

- **`[infrastructure]`** — identity + topology + wiring.
  - `cluster_name`, `region`, `storage` (`in-memory` | `dsql`).
  - `[infrastructure.dsql]` — `endpoint`, `region`, `admin_role_arn`, `runtime_role_arn`,
    `readonly_role_arn`, `rate_limiter_table`, `conn_lease_table` (all optional; written back by
    `tkr infra apply --module dsql`).
  - `[infrastructure.placement]` — `controller_endpoint`, heartbeat/reconnect timings, `node_host`/`node_port`,
    `shard_count`, `bundle_count`, `partition_count`, `hash_version`, `routing_max_retries`.
  - `[infrastructure.network]` — `grpc_addr`, `metrics_addr`.
  - `[infrastructure.observability]` — `metrics_enabled` (default on), `otlp_enabled`, `otlp_endpoint`,
    `otlp_protocol`, `trace_sample_rate`, `log_format`, `log_filter`, `[…otlp_metrics]` (deferred push),
    `leak_detection_deadline_ms`, `[…alert_thresholds]`, `dashboard_provisioning_enabled`,
    `runbook_base_url`, `smoke_test_timeout_ms`.
- **`[policy]`** — behavioural policy.
  - `default_retention_days`, `namespace_creation` (`open` | `controlled`).
  - `[policy.quotas]` — `max_workflow_timeout_seconds`, `max_signal_payload_bytes`.
  - `[policy.compatibility]` — `enable_standalone_activities` (off = v1.31.0 baseline; on = declared deviation).
  - `[policy.nexus_endpoint_limits]` — the six endpoint admin limits (v1.31.0-faithful defaults).
  - `[policy.nexus_completion]` — `http_addr` (`0.0.0.0:7253`), `system_callback_url`
    (`http://127.0.0.1:7253`), retry policy (1s / 1h / 2.0 / unbounded).
- **`[capacity]`** — `[capacity.performance]` (`target_workflow_starts_per_second`, `target_p99_wft_latency_ms`),
  `[capacity.dsql]` (`max_connections`, `connection_rate_per_second`, `burst_capacity`).
- **`[emergency]`** — break-glass: `disable_stickiness`, `freeze_projection`, `cap_poll_admission`
  (logged as warnings when set).

**Not in `tokeirad.toml`:** `RuntimeConfig` is always `Default` — mechanical settings (broker, lanes,
sweepers, reservoir) are auto-tuned, never knobs. This is the deliberate counterpart to Temporal's
`history.*` tuning explosion.

**`deployment.toml` = platform config** (`LocalConfig` | `ComposeConfig`) — platform-specific deploy
intent; compose DSQL carries `storage = "dsql"` + `[dsql]` mode/endpoint/arn/region, written back to
`tokeirad.toml`.

**Resolution:** `--config <path>` → `TOKEIRA_CONFIG` env → built-in defaults. No auto-discovery of a bare
`tokeirad.toml`. No other env vars on invocation.

## Where it is documented today (the finding)

There is **no single configuration reference**. It is scattered:

| Source | What it gives | Role |
|--------|---------------|------|
| `AGENTS.md` → `## Configuration` | The philosophy (close-to-zero, `RuntimeConfig` Default, no env vars, `deny_unknown_fields`) + the four-section shape | Narrative summary |
| `crates/tokeira-config/src/lib.rs` + platform `config.rs` | Field-level contract via `///` docs + defaults | **Authoritative**, but it's code |
| `README.md` | Usage snippets (DSQL endpoint, compose lifecycle) | Task-oriented |
| Per-feature specs (`compose-dsql`, `nexus-async-completion/design.md`, `scenarios/standalone-activities/README.md`) | Fields where introduced; one minimal runnable `tokeirad.toml` | Fragmentary |

**Gaps:** no `config.example.toml` / `tokeirad.toml.example`; no operator-facing reference doc; the
surface is only fully visible by reading Rust structs. (The sibling `temporal-dsql-deploy-eks` treats
`config.example.toml` as its canonical reference — tokeira has no equivalent.)

## Readiness for initial release

- [ ] **Canonical reference.** A single annotated `config.example.toml` (or generated reference) that
      enumerates every field, its default, and whether it is required — the one place operators look.
- [ ] **The minimal configs, shown.** Empty file (in-memory); the 3-field DSQL minimum
      (`storage` + `dsql.endpoint` + `dsql.region`). Make the close-to-zero claim concrete.
- [ ] **The genuine deployment knobs, surfaced.** Distinguish the handful that *must* be set/correct for
      a real deployment — DSQL endpoint/region/roles, `network.grpc_addr`, `nexus_completion.system_callback_url`
      reachability (the one operational footgun), `default_retention_days` — from the rest (defaulted).
- [ ] **Emergency overrides documented as break-glass** with their warning semantics.
- [ ] **The contrast made explicit** — link the 564-key Temporal surface as the "before" so the claim is
      evidenced, not asserted.
- [ ] **Decide the home.** Whether the canonical reference lives in `docs/`, as `config.example.toml` at
      root, or both; and whether `AGENTS.md`'s summary points at it.

## Related

- [`../conformance/v1.31.0/configuration.md`](../conformance/v1.31.0/configuration.md) — Temporal v1.31.0's
  full config surface (the "before"; the denominator the close-to-zero claim answers).
