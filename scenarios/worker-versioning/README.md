# Scenario: Worker Versioning

Exercises Tokeira's **Worker Deployment v2 routing** end-to-end against a running
`tokeirad`: two versioned workers register, the driver sets a current version, ramps a
fraction of traffic to a new version, observes the split, then promotes the new version
and confirms traffic follows. This is the canonical scenario for the machinery owned by
the `worker-deployments` spec (`.kiro/specs/worker-deployments/`).

> **Server dependency.** Routing is owned by the `worker-deployments` spec (runtime
> registry + dispatch routing + edge handlers). Storage has landed; the routing path is
> still in progress. Until it does, the workers run and register but the driver's
> routing assertions will not pass. The scenario is written against the **real**, shipping
> Temporal Rust SDK surface — there is nothing stubbed on the client side.

## Design (what the SDK actually exposes)

Two facts about the pinned Temporal Rust SDK shape this scenario, and are worth stating
because they are easy to get wrong:

1. **Versions register by polling, not by an explicit RPC.** The SDK client exposes
   `describe_worker_deployment`, `describe_worker_deployment_version`,
   `set_worker_deployment_current_version`, and `set_worker_deployment_ramping_version`
   on `WorkflowService` — but **no** `create_worker_deployment_version`. A
   `(deployment, build_id)` comes into existence when a worker advertising it starts
   polling. So the scenario **starts the workers first**, then the driver waits for both
   versions to appear via Describe before routing.
2. **Versioning behaviour is per-worker, not per-workflow.** The high-level SDK sends
   `VersioningBehavior::Unspecified` on workflow-task completion, which falls back to the
   worker's `default_versioning_behavior`. There is no per-`#[workflow]` behaviour
   attribute. So a "pinned" vs "auto-upgrade" workflow is expressed by the **worker** it
   runs on (`WorkerOptions::deployment_options(... default_versioning_behavior ...)`).

## Pieces

| File | Role |
|------|------|
| `src/workflows.rs` | `VersionProbeWorkflow` + `VersionActivities`. The activity returns the build id baked into the worker that ran it, so each run's result *is* the observed routing decision. |
| `src/worker.rs` | A versioned worker. `--build-id <id> [--behavior pinned\|auto-upgrade]`. Builds `WorkerDeploymentOptions { version: { deployment_name, build_id }, use_worker_versioning: true, default_versioning_behavior }` and polls the shared task queue. |
| `src/starter.rs` | The driver. Waits for both versions, then set-current → ramp → promote, starting a batch of workflows at each stage and asserting which build executed them. |
| `src/config.rs` | Shared deployment name / task queue / address / namespace, from env with defaults. |

## Flow and observable assertions

The driver asserts an **observable** outcome at each stage (the build id a run reported),
not an internal structure:

1. **Register** — start `worker --build-id 1.0` and `worker --build-id 2.0`; the driver
   waits until `DescribeWorkerDeployment` lists both (Requirements 1.x, version
   registration by polling).
2. **Set current = v1** — `set_worker_deployment_current_version(v1)`; a batch of
   auto-upgrade workflows must **all** run on `1.0` (Requirement 3.1, 9.1).
3. **Ramp v2 @ 50%** — `set_worker_deployment_ramping_version(v2, 50)`; a fresh batch
   must split across `1.0` and `2.0` (Requirements 4.1, 9.4 — deterministic bucketing by
   workflow id).
4. **Promote v2** — `set_worker_deployment_current_version(v2)` (auto-unsets ramping per
   Requirement 3.3); a fresh batch must **all** run on `2.0`.

The driver exits non-zero if any stage's observed distribution does not match.

## Running

Three terminals, against a local `tokeirad` (defaults to `http://[::1]:7233`; override
with `TEMPORAL_ADDRESS`):

```bash
# 1. server
cargo run -p tokeirad

# 2. the two versioned workers (same task queue, different build ids)
cargo run --manifest-path scenarios/worker-versioning/Cargo.toml --bin worker -- \
  --build-id 1.0 --behavior auto-upgrade
cargo run --manifest-path scenarios/worker-versioning/Cargo.toml --bin worker -- \
  --build-id 2.0 --behavior auto-upgrade

# 3. the driver
cargo run --manifest-path scenarios/worker-versioning/Cargo.toml --bin starter
```

Environment overrides (shared by worker and starter): `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE`, `SCENARIO_DEPLOYMENT` (default `orders`), `SCENARIO_TASK_QUEUE`
(default `orders`).

## Ground truth

Behaviour matches Temporal server v1.31.0 (the `TEMPORAL_SERVER_COMPAT` target), as
captured and source-anchored in `.kiro/specs/worker-deployments/{requirements,design}.md`.
Routing precedence, ramp bucketing, and promotion semantics all trace to that spec;
consult it (not this README) for the authoritative contract.
