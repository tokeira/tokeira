# Hello World

A basic workflow that calls a single activity and returns
the result. Adapted from the
[sdk-core hello_world example](https://github.com/temporalio/sdk-core/tree/master/crates/sdk/examples/hello_world)
using the [Temporal Rust SDK v0.2.0](https://github.com/temporalio/sdk-core/releases/tag/v0.2.0).

## What it demonstrates

- A workflow (`HelloWorldWorkflow`) that schedules one
  activity and returns the result
- An activity (`GreetingActivities::greet`) that formats
  a greeting string
- A worker that registers the workflow and activity on
  the `hello-world` task queue
- A starter that kicks off the workflow and waits for
  the result

## Running

1. Start the Tokeira server:

```bash
cargo run -p tokeirad
```

2. In another terminal, build and start the worker:

```bash
cd examples/hello-world
cargo run --bin worker
```

3. In another terminal, run the workflow:

```bash
cd examples/hello-world
cargo run --bin starter
```

The starter should print:

```
Started workflow, run_id: <uuid>
Workflow result: Hello, Temporal!
```

## Configuration

By default the SDK connects to `localhost:7233`, which
matches the default `tokeirad` address. To override:

```bash
export TEMPORAL_ADDRESS="[::1]:7233"
```

## File overview

| File | Purpose |
|------|---------|
| `Cargo.toml` | Dependencies (Temporal Rust SDK v0.2) |
| `workflows.rs` | Workflow and activity definitions |
| `worker.rs` | Worker binary |
| `starter.rs` | Starter binary |
