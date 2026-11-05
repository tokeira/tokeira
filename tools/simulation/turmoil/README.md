# Turmoil spike

Runs Tokeira's real engine code on [turmoil](https://docs.rs/turmoil/0.7.2/turmoil/)'s
simulated hosts and network, in simulated time, from a seed.

The other simulators here, [`broker`](../broker/) and [`placement`](../placement/), and the
TLA+ models in [`spec/tla/`](../../../spec/tla/) check models of Tokeira's design. This crate
checks the code itself. It sits outside the workspace with its own `Cargo.lock`, so the
turmoil dependency never touches the workspace lockfile.

## What step one does

One simulated host, `node`, starts the embedded in-memory engine and serves its Temporal
endpoint over turmoil's TCP. A second host, `worker`, runs a scripted worker through the
public gRPC API:

1. Start a workflow.
2. Take the first workflow task and schedule one activity.
3. Take the activity task and complete it.
4. Take the second workflow task and complete the workflow.
5. Read the workflow's history.

The run returns a trace of what the worker saw and when, in simulated milliseconds. The
tests check that the workflow completes and that one seed gives the same trace twice.

Tokeira's production code is unchanged. A small bridge frames gRPC by hand and passes each
call to the engine's in-process endpoint, the one the engine's integration tests use.

## Running it

From this directory:

```bash
cargo test
```

```bash
cargo run -- 42
```

The second command prints the trace for seed 42.

## Limits of step one

- **No timers, retries or backoffs.** The engine reads the wall clock when it decides
  whether something is due, and turmoil moves only tokio's clock. A workflow that waits
  on a timer would wait on real time. Step two needs a clock the simulation can drive.
- **One node.** Two nodes need the placement controller, plus seams in shared code for the
  node's listener, calls between nodes and calls to the controller.
- **Restarts lose state.** The store lives inside the node's host, so a crash or bounce
  starts the engine afresh.
