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

Step one changes no production code. A small bridge frames gRPC by hand and passes each
call to the engine's in-process endpoint, the one the engine's integration tests use.

## What step two does

Five hosts run a cluster on turmoil's network ([`src/cluster.rs`](src/cluster.rs)):

- `controller` runs the placement controller, built from `tokeira-controller`'s public
  parts.
- `node-a` and `node-b` each run a full listener-backed node. Each node's listener, its
  calls to the other node and its calls to the controller all cross turmoil's network.
- `worker` reads placement with the edge's own routing code. It picks a task queue and a
  workflow id that put the work where the scenario needs it, then runs the step-one
  script. Like a client behind a load balancer, it tries each call on one node and, if
  that node refuses, on the other.
- `previous-generation` runs nothing. It stands for an older cluster that holds every
  bundle until both nodes have registered.

The nodes share one in-memory store, which stands in for the DSQL database.

Two tests ([`tests/two_nodes.rs`](tests/two_nodes.rs)) run the same one-activity workflow.
In both, the workflow's home and its task queue's home are on different nodes:

- **The edge and the runtime agree on the activity queue's home.** The workflow completes.
  Its start, its tasks and its replies cross between the nodes.
- **They disagree** (the brief's defect 1). This test is `#[ignore]`d and fails on current
  code. Both nodes refuse the activity poll. On the node the edge picks, the runtime says
  the queue's home is not active there. On the runtime's home, the edge routes the poll to
  the other node and refuses it with "forwarding is not wired yet". The worker gives up
  after two simulated minutes.

### The seams step two added

To reach a node's connections, step two added three seams to production code:

- `TokeiradHandle::start_in_memory_with_transport` in `tokeira-engine` accepts a
  `NodeTransport`. It carries the node's incoming connections as in-memory pipes, a
  connector for peer channels, and a connection to the controller.
- Every edge type that opens peer channels has a `with_peer_connector` builder.
- `tokeira-runtime`'s membership client takes an optional `ControllerConnection`, and so
  does the edge's routing subscription.

Production behaviour is unchanged. Without a transport, the node binds its TCP listener and
dials as it always has. No dependency was added to a production crate.

### Why the cluster forms as a rolling replacement

The controller offers a registering node every bundle nobody owns, and it never moves a
bundle that has an owner (`compute_desired_placement` in `tokeira-controller`). If both
nodes start together, the first to register takes all four bundles. The second node's
offer then overlaps, its lease acquisition is rejected, and it ends up with nothing. Every
run tried before the harness staged startup ended with one node owning all four.

So the previous generation holds every lease until both nodes have registered, then hands
them back. The controller's next placement splits the bundles two and two.

## Running it

From this directory:

```bash
cargo test
```

```bash
cargo test --test two_nodes -- --ignored
```

The second command runs the defect-1 test and prints its trace when it fails.

```bash
cargo run -- 42
```

This one prints the step-one trace for seed 42.

## Limits

- **No timers, retries or backoffs.** The engine reads the wall clock to decide whether
  something is due, and turmoil moves only tokio's clock. A workflow that waits on a timer
  would wait on real time. Fixing that needs a clock seam the simulation can drive.
- **Heartbeat faults wait on the same seam.** The controller times node heartbeats with
  `std::time::Instant`, which turmoil does not move.
- **Two-node runs are not yet replayable.** Node incarnation ids are random UUIDs, and the
  controller orders nodes by id. A seed always gives a two-and-two split, but which
  bundles land on which node can differ between runs, and so can the queue and workflow the
  worker picks.
- **Step one's restarts lose state.** Its store lives inside the node's host, so a crash or
  bounce starts the engine afresh. Step two's store is shared and outlives any one host.
