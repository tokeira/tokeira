# Turmoil spike

Runs Tokeira's real engine code on [turmoil](https://docs.rs/turmoil/0.7.2/turmoil/)'s
simulated hosts and network, in simulated time, from a seed.

The other simulators here, [`broker`](../broker/) and [`placement`](../placement/), and the
TLA+ models in [`spec/tla/`](../../../spec/tla/) check models of Tokeira's design. This crate
checks the code itself, against the same properties. It sits outside the workspace with its
own `Cargo.lock`, so the turmoil dependency never touches the workspace lockfile.

## How a scenario is built

A scenario has four parts, each in its own module:

- **Topology** ([`src/topology.rs`](src/topology.rs)): who runs. `single_node` is one engine
  node, `node`, that owns its single bundle. `cluster` is a placement controller and two
  engine nodes, `node-a` and `node-b`. Every node is a full listener-backed node: its gRPC
  listener, its calls to the other node and its calls to the controller all cross turmoil's
  network, and it reads the time from the simulation. The store is created outside the
  hosts and stands in for the DSQL database, so it outlives a crashed node.
- **Workload** ([`src/workload.rs`](src/workload.rs)): what the `worker` host does, through
  the public gRPC API. Each workload is a scripted worker for one kind of workflow: one
  activity, a timer, an activity whose completion arrives late, a parent and a child. Like
  a client behind a load balancer, the worker offers each call to one node and, if that
  node refuses, to the other.
- **Faults** ([`src/faults.rs`](src/faults.rs)): what goes wrong, and when. A fault
  partitions or holds the traffic between two hosts, repairs or releases it, or crashes or
  bounces a host. It is applied at a simulated time, or after the worker records a given
  line. The second kind keeps landing at the right moment when the code's timing changes.
- **Oracle** ([`src/oracle.rs`](src/oracle.rs)): what must hold. Some checks run on every
  tick, reading the shared store from an observer host that no fault cuts off. Others run
  after the run, over the histories the worker recorded.

A scenario puts the four together. This one holds the worker's completion of an activity
on the network until the activity has timed out:

```rust
let mut run = topology::single_node(SEED);
run.worker(workload::late_completion);
let trace = run
    .drive([
        Fault::when_seen(FIRST_ATTEMPT_RECEIVED, Action::Hold(WORKER, NODE)),
        Fault::after_seen(
            FIRST_ATTEMPT_RECEIVED,
            ACTIVITY_TIMEOUT + Duration::from_secs(10),
            Action::Release(WORKER, NODE),
        ),
    ])
    .expect("the simulation runs to the end");
oracle::exactly_once(trace.history("workflow"), EventType::ActivityTaskStarted)
    .unwrap_or_else(|error| panic!("{error}:\n{trace}"));
```

The scenarios share one test binary, [`tests/scenarios/`](tests/scenarios/), with one module
per area of the engine. One binary means the engine is linked once. Nextest still runs each
test in its own process. Each module's first lines name the property it checks and the
TLA+ model the property comes from.

## The scenarios

### `harness`: the simulation itself

- **A one-activity workflow completes on one node.**
- **A timer fires in simulated time.** A 60-second timer fires no sooner than 60 seconds
  after it started. The node stamps the firing no later than the moment the worker
  receives the task it caused, which shows the node's clock is the simulation's.
- **The same seed replays the same trace.** Two timer runs of one seed give the same
  trace, event times included.

### `routing`: task routing and queue homes across two nodes

Property: every scheduled task is started or times out (`EventuallyResolved`,
[`40_dispatch_handoff`](../../../spec/tla/40_dispatch_handoff.tla)).

- **A workflow completes when its tasks start across nodes.** The workflow's home and its
  task queue's home are on different nodes. Its start, its tasks and its replies cross
  between them.
- **An activity poll is served on the node the edge sends it to.** This test is
  `#[ignore]`d and fails on current code (the brief's defect 1). The edge sends an activity
  poll to the owner of a bundle it computes from the namespace name. The runtime serves the
  poll only at a home it computes from the namespace id. When the two bundles have
  different owners, both nodes refuse the poll, and the worker gives up after two
  simulated minutes.

### `leases`: who owns each bundle, and at which epoch

Property: at most one node owns a bundle at an epoch, and a bundle's epoch never goes back
(`OneOwnerPerEpoch`, [`30_bundle_lease`](../../../spec/tla/30_bundle_lease.tla)).

- **Bundles keep one owner per epoch while the cluster forms.** An observer reads every
  lease from the shared store every simulated millisecond while each bundle passes from
  the previous generation, through no owner, to `node-a` or `node-b`. A one-activity
  workflow runs across the nodes meanwhile.

### `dispatch`: activity attempts, timeouts, retries and completions

Property: each attempt starts at most once, and only the live attempt can complete the
activity (`AtMostOneStartPerIncarnation`,
[`40_dispatch_handoff`](../../../spec/tla/40_dispatch_handoff.tla)).

- **A completion held past its timeout is refused, and the retry completes.** The
  activity has a 10-second start-to-close timeout. The worker's completion of the first
  attempt is held on the network for 20 seconds. The node refuses it as not found once it
  arrives, and the second attempt completes the activity. The history has one scheduled,
  one started and one completed event. The started event, which records the attempt that
  completed, is at least 10 seconds after the scheduled one.

### `outbound`: work one run starts on another run's home

Property: each child starts once, and its result reaches its parent once.

- **A child homed on the other node starts once and reports back once.** The parent's home
  is on one node and the child's on the other. The parent's history has one initiated, one
  started and one completed child event. The child's history has one start.

## Reading a trace

`Run::drive` returns the run's trace: each line the worker, the observer or the fault
schedule recorded, with the simulated time it was recorded. These lines are from the
timer scenario for seed 42:

```text
    116 ms  PollWorkflowTaskQueue served by node
    148 ms  started a 60-second timer
  60202 ms  PollWorkflowTaskQueue served by node
  60267 ms  workflow event 5 stamped 123 ms: EVENT_TYPE_TIMER_STARTED
  60267 ms  workflow event 6 stamped 60200 ms: EVENT_TYPE_TIMER_FIRED
```

The left column is when the line was recorded. "Stamped" is the time the node wrote on the
event. Both count simulated milliseconds from a fixed epoch, 2026-01-01T00:00:00Z. The
timer was due at 60 123 ms. The timer scanner, which runs every 200 ms, fired it at
60 200 ms, and the worker's waiting poll received the task 2 ms later.

Faults appear in the trace as they are applied, for example
`fault: hold traffic between worker and node`. Owner changes appear as the observer sees
them, for example `bundle 2 at epoch 3 is owned by node-a`.

## What the spike added to production code

Two sets of seams, neither of which changes production behaviour:

- **The node's connections.** `TokeiradHandle::start_in_memory_with_transport` in
  `tokeira-engine` accepts a `NodeTransport`. It carries the node's incoming connections as
  in-memory pipes, a connector for peer channels, and a connection to the controller.
  Every edge type that opens peer channels has a `with_peer_connector` builder.
  `tokeira-runtime`'s membership client takes an optional `ControllerConnection`, and so
  does the edge's routing subscription. Without a transport, a node binds its TCP listener
  and dials as it always has.
- **The node's clock.** A new crate, `tokeira-clock`, gives the engine one clock to read.
  A node passes it to its runtime, its edge and its handle on the store. Deadlines are
  stamped and judged on that clock, so timers, timeouts, retries and backoffs come due on
  it. Production passes the host's clock. The spike passes `SimClock`, which reads
  turmoil's simulated time from the fixed epoch, so that one seed stamps the same times on
  every run.

## Why the cluster forms as a rolling replacement

The controller offers a registering node every bundle nobody owns, and it never moves a
bundle that has an owner (`compute_desired_placement` in `tokeira-controller`). If both
nodes start together, the first to register takes all four bundles and the second gets
none. So a fifth host, `previous-generation`, holds every lease until both nodes have
registered, then hands them back. The controller's next placement splits the bundles two
and two.

## Running it

From this directory, all the scenarios:

```bash
cargo test --test scenarios
```

One area:

```bash
cargo nextest run --test scenarios -E 'test(/^leases::/)'
```

The defect-1 scenario, which prints its trace when it fails:

```bash
cargo test --test scenarios -- --ignored
```

A passing scenario's trace:

```bash
RUST_LOG=turmoil_sim::trace=debug cargo test --test scenarios a_timer_fires -- --nocapture
```

The trace of one run with a seed of your choice, with no test around it. The workload is
`one-activity` or `timer`:

```bash
cargo run -- 42 timer
```

## Limits

Three scenarios wait on changes to production code:

- **A lease that lapses.** The in-memory store grants leases with no expiry unless it is
  given a lease duration, and it times expiry with tokio `Instant`s, which each host counts
  from its own start. A node that crashes, or loses its store, therefore keeps its bundles.
  Timing lease expiry on the engine's clock would let a scenario check that another node
  takes the bundles over at a higher epoch, and that the old owner's writes are fenced.
- **A node cut off from its store.** The store is shared memory, not network, so no
  partition reaches it. A fault at the store's boundary for one node would let a scenario
  partition a node from the database while it can still reach its peers.
- **Replayable cluster runs.** Node incarnation ids are random UUIDs, not drawn from
  turmoil's seeded random numbers, and the controller orders nodes by id. A seed always
  gives a two-and-two split, but which bundles land on which node can differ between runs,
  and so can the queue and workflow the worker picks. Seeded incarnation ids would make a
  cluster run replay exactly.

Other limits:

- **Controller heartbeat faults are not reachable yet.** The controller times node
  heartbeats with `std::time::Instant`, which turmoil does not move.
- **Some times stay on the host's clock.** These include fairness snapshots, membership
  budgets, peer tokens and health reports. Each is stamped and judged on host clocks only,
  or never judged at all. The full list is in
  [the clock crate's page](../../../docs/crates/clock.md#what-stays-on-the-hosts-clock).
- **Calls are not forwarded between nodes.** A call that reaches a node that does not own
  its work is refused, and the worker offers it to the other node.
