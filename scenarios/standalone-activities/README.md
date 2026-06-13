# Scenario: Standalone Activities

Exercises Tokeira's **standalone-activity** support (the first CHASM component)
end-to-end against a running `tokeirad`, over the real gRPC wire. One driver plays
both roles a real deployment splits across a client and a worker, and asserts the
**observable** outcome a `DescribeActivityExecution` returns at each stage:

1. **completed** — `StartActivityExecution` → `PollActivityTaskQueue` (as the worker)
   → `RespondActivityTaskCompleted` → `Describe` reports `COMPLETED`.
2. **failed** — start → poll → `RespondActivityTaskFailed` → `Describe` reports
   `FAILED`.
3. **terminated** — start → `TerminateActivityExecution` → `Describe` reports
   `TERMINATED` (no worker pickup needed).

The driver exits non-zero if any stage's observed status does not match.

## Why this scenario uses Tokeira's proto client, not the published SDK

The other scenario (`worker-versioning`) builds against the published Temporal Rust
SDK the way a downstream consumer would. This one **cannot**: the standalone-activity
RPCs (`StartActivityExecution`, `PollActivityExecution`, the SA worker path) do not
exist in any published SDK yet — even the SDK's raw gRPC `WorkflowService` trait
predates the surface. So there is nothing to build against downstream today.

Instead the driver depends on Tokeira's own `tokeira-proto` (vendored Temporal API
`v1.62.11`), whose generated tonic `WorkflowServiceClient` carries the SA RPCs. It is
still a genuine over-the-wire gRPC consumer — it connects to a `tokeirad` endpoint
and makes real calls; it is simply sourced from the proto crate rather than a
published SDK that has not caught up. Shipping first-class SA *ergonomics* in the
Rust SDK is a separate, larger effort and is not required to demonstrate the server.

## Server prerequisite

Standalone activities are **off by default** (they are ahead of the `v1.31.0`
baseline behavioural claim, so an unconfigured server answers `UNIMPLEMENTED`, which
matches the conformance matrix). Enable them with a config file passed on the
command line — `tokeirad` resolves config from `--config <path>`, then the
`TOKEIRA_CONFIG` env var, then built-in defaults; it does **not** auto-discover a
bare `tokeirad.toml`. A minimal enabling file:

```toml
# standalone-activities.toml
[policy.compatibility]
enable_standalone_activities = true
```

(All other sections take their defaults, including in-memory storage.) With the gate
off, the driver's first `StartActivityExecution` returns `UNIMPLEMENTED` and the
scenario fails fast with that message. A ready-to-use copy of this file ships next to
the scenario (`scenarios/standalone-activities/standalone-activities.toml`). Confirm
the resolved value any time with
`cargo run -p tokeirad -- --config scenarios/standalone-activities/standalone-activities.toml --dump-config`.

## Running

Two terminals, against a local `tokeirad` (defaults to `http://[::1]:7233`; override
with `TEMPORAL_ADDRESS`). Run from the repository root:

```bash
# 1. server, with standalone activities enabled via the shipped --config
cargo run -p tokeirad -- --config scenarios/standalone-activities/standalone-activities.toml

# 2. the scenario driver
cargo run --manifest-path scenarios/standalone-activities/Cargo.toml --bin driver
```

Environment overrides: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` (default `default`),
`SCENARIO_TASK_QUEUE` (default `standalone-activities`), `SCENARIO_IDENTITY`.

## Ground truth

Activity lifecycle and status semantics match Temporal server v1.31.0 (the
`TEMPORAL_SERVER_COMPAT` target), as captured in the `chasm-foundation` spec and the
`chasm/lib/activity` source at that tag. The status projection (`COMPLETED` /
`FAILED` / `TERMINATED`) is the public `ActivityExecutionStatus`
(`enums/v1/activity.proto`).
