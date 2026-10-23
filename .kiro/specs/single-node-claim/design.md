# Single-Node Claim — Bugfix Design

## Overview

A listener-backed distributed node without a controller takes a cluster-wide claim before it assigns itself every
home, the way the embedded engine already does:
- **The claim.** After the schema check passes, the node acquires the `tokeirad-owner` row of `tokeira_control_lease`
  through the control-lease repository the embedded engine uses. The row records an owner and a fence token, and the
  hold's expiry on the database's clock.
- **A held claim.** While another owner's hold is live, the node tries again for up to 80 s. A claim still held then
  belongs to a running node, and this node refuses to start, leaving nothing started.
- **An expired claim.** The previous owner may still have a transaction open, so the node waits out DSQL's longest
  transaction, renewing its claim, before it assigns itself any home.
- **Holding it.** The node renews the claim every 20 s. Its DSQL connection director admits work only while the claim
  is confirmed: if a renewal finds another owner, or none is confirmed in time, the director admits nothing new and the
  node drains and exits.
- **Letting go.** A clean shutdown closes the director, lets admitted work finish, and releases the claim, so the next
  node starts at once.

## Glossary

- **Node without a controller:** `tokeirad` with `infrastructure.storage = "dsql"` and no
  `infrastructure.placement.controller_endpoint`, built by `build_and_serve` (`crates/tokeira-engine/src/lib.rs`). It
  assigns itself every home (`self_assign_dsql_shards`).
- **Control-lease repository:** `ConnectionControlLeaseRepository` (`crates/tokeira-storage/src/dsql/control_lease.rs`).
  It acquires, renews and releases a named claim on a caller's connection. Acquisition reads the row `FOR UPDATE` in a
  repeatable-read transaction, compares its expiry with the database's `now()`, advances the fence token and commits.
  Renewal and release condition on the owner and fence token, and treat no matching row as fencing (`Fenced`).
- **Hold:** the time until the claim's `expires_at`, 60 s from its last acquisition or renewal (`OWNER_LEASE_DURATION`).
- **Admission gate:** `OwnershipAdmissionGate`. Installed on a DSQL connection director, it admits a checkout only while
  it is open. It closes when the local admission deadline passes without a confirmed renewal, 40 s after the last one
  (`OWNER_LEASE_DURATION` less `OWNER_ADMISSION_MARGIN`), and it is fenced for good when a renewal finds another owner.
- **Quiescence:** the 5 minutes an owner waits after it takes over an expired claim (`EXPIRED_TAKEOVER_QUIESCENCE`).
  DSQL ends any transaction older than 5 minutes, so by then no transaction the previous owner started can still commit.

## Bug Details

### Bug Condition

The bug condition holds when a node without a controller starts on a DSQL cluster where another node without a
controller is serving, or could still commit a transaction it started.

### Examples

- **A second node.** An operator starts a second node to add capacity. Today it relinquishes the first node's leases,
  takes them, and both serve, writing the same runs unfenced. After the fix, it tries for 80 s and refuses, naming the
  first node's claim.
- **An upgrade done out of order.** The operator starts a node of the new release before stopping the old one. When
  the old node is of this release or later, the new node refuses as above. When it is older, nothing changes (Out of
  Scope).
- **A restart after a crash.** The node's process dies without releasing its claim. Its replacement finds the claim
  held for up to 60 s more, then takes it over, waits 5 minutes, and serves. Today the replacement serves at once.
- **A clean restart.** The node stops on SIGTERM and releases its claim. Its replacement acquires the claim at once and
  serves. Today it also serves at once.
- **A partitioned node.** The node can't reach DSQL for more than 40 s. Its director stops admitting work, and the
  node drains and exits. Its replacement takes over as after a crash.
- **ECS.** An ECS deployment runs `tokeirad` with one DSQL configuration in several services, and sets no controller
  endpoint (`platforms/ecs/deployment.tkd`). Today each of its nodes relinquishes the others' leases when it starts, and
  all of them write unfenced. After the fix, the first node to take the claim serves, and the others refuse.

## Expected Behavior

### Preservation Requirements

- A node with a controller starts as before, without the claim (3.1).
- The embedded engine keeps its own claim and its own startup (3.2).
- An in-memory listener-backed node starts as before (3.3).
- A node that holds the claim checks the schema first, then assigns itself every home, sweeps each one and writes
  unfenced, exactly as before (3.4).
- No migration is added and the schema contract is unchanged (3.5).

## Root Cause

- **An assumption, not a check.** `self_assign_dsql_shards` relinquishes every lease because a node without a
  controller is meant to be the only one. Nothing makes it so. The node's commits rely on the same assumption: they are
  unfenced because nothing should contend with them.
- **Leases can't answer the question.** A node without a controller never renews or releases its self-assigned leases,
  so 30 s after it starts they look expired, though it still serves. A lease records its holder's incarnation and
  endpoint, not its release. So a refusal based on live shard leases would let a second node start once the first had
  run for 30 s.
- **The guard exists on one path only.** The embedded engine takes its `embedded-owner` claim in
  `acquire_embedded_ownership`, before its own call to `self_assign_dsql_shards`. `build_and_serve` goes from the schema
  check straight to `build_dsql_stack`.

## Correctness Properties

Property 1: A start follows the claim

_For any_ state of the claim when a node starts, the node SHALL:
- go on at once, when the claim has no owner or its owner released it;
- go on after the quiescence, renewing meanwhile, when it takes over an owner's expired hold;
- refuse to start, when another owner's hold is live at every attempt for 80 s.

**Validates: Requirements 2.1, 2.2, 2.4**

Property 2: One node admits work at a time

_For any_ schedule of two nodes' starts, renewals, crashes, partitions and clean shutdowns on one claim, in which each
renewal's round trip and the drift between clocks stay within the admission margin, the intervals in which each node's
admission gate is open SHALL NOT overlap. After a takeover of an expired hold, the new owner's gate SHALL open no
earlier than the quiescence after the previous owner's gate last could have admitted work.

**Validates: Requirements 2.1, 2.4, 2.5, 2.6**

Property 3: A lost claim stops the node

_For any_ sequence of renewal outcomes, once a renewal is fenced, or the local admission deadline passes without a
confirmed renewal, the node's gate SHALL admit no further checkout other than the claim's own, and the node SHALL drain
and exit with an error.

**Validates: Requirements 2.5, 2.6**

Property 4: A release never shortens a wait

_For any_ shutdown or failed start, the node SHALL release the claim only after its gate has closed and its admitted
work has drained, and never while the quiescence of an expired takeover is still running.

**Validates: Requirements 2.7, 2.8**

Property 5: A refused start leaves nothing started

_For any_ refused start, the node SHALL have bound no listener, taken no shard lease and changed no row of any table,
and SHALL have closed its DSQL connections and released its DynamoDB connection-slot blocks.

**Validates: Requirements 2.3**

## Fix Implementation

### Engine

- **Where.** `build_and_serve`'s DSQL branch acquires the claim after `check_distributed_schema` and before
  `build_dsql_stack`, when no controller endpoint is configured. The claim's logic lives in a module of its own in
  `tokeira-engine`, with its decisions as pure functions over a clock and the repository's outcomes, so Properties 1 to
  4 run offline.
- **The claim.** Name `tokeirad-owner`. Owner `tokeirad-<incarnation>`, a new `IncarnationId` per process, as the
  embedded engine's owner is `engine-<incarnation>`. Hold, renewal interval and admission margin are the embedded
  engine's constants: `OWNER_LEASE_DURATION` (60 s), `OWNER_RENEW_INTERVAL` (20 s) and `OWNER_ADMISSION_MARGIN` (20 s).
  The quiescence is `EXPIRED_TAKEOVER_QUIESCENCE` (5 minutes).
- **Its identity.** The repository checks the cluster identity stored in the row before anything else. A node knows its
  endpoint, which can change, and not its cluster's ARN, and the row lives in the cluster's own database. So the claim
  stores the constant `tokeirad` as both `cluster_id` and `cluster_arn`.
- **Acquiring.** The node takes one control-class connection from the store's director and calls the repository's
  `acquire`. It runs no bootstrap DDL: the schema check has already found V067 applied.
  - `Busy`: the node waits 5 s and tries again, until 80 s have passed since its first attempt. Then it refuses with an
    error naming the holder's owner and the hold's expiry, and saying to stop that node first. The refusal shuts the
    store down, as the schema check's refusal does.
  - `Clean`: the node goes on.
  - `ExpiredTakeover`: the node starts renewing at once, then waits until the guard's quiescence deadline.
- **The gate.** The node builds the gate from its guard, closed during a quiescence and open otherwise, and installs it
  on the store's director before `build_dsql_stack`. Today `install_ownership_gate` accepts only an embedded director;
  it accepts the coordinated one too. The claim's own renewals and release check out past the gate, through the seam
  the embedded engine's release uses (`acquire_shutdown_control`). Renewals must, because the gate is closed during a
  quiescence. The embedded engine's renewals go through its gate, and start only after its wait.
- **Renewing.** A task renews every 20 s from acquisition onwards. A `Fenced` renewal fences the gate; any other failed
  renewal leaves the claim to the local admission deadline, which closes the gate when it passes. When the gate is no
  longer open, the task starts the node's drain, and the node exits with an error saying it lost the claim.
- **Releasing.** The task owns the release. When the node's background work is cancelled, which `run_from_cli` does
  after the server has drained, the task closes the gate and waits up to 30 s for admitted checkouts to return, renewing
  meanwhile. Then it releases the claim, or, if checkouts are still out, stops renewing and leaves the claim to expire.
  `run_from_cli` waits for the task before the process exits.
- **A failed start.** If anything fails after the claim is acquired, the node closes the gate and releases the claim the
  same way, unless the quiescence of an expired takeover hasn't finished. Then it stops renewing and leaves the claim
  to expire, so the next node waits again.
- **Readiness.** While it tries for the claim or waits out a quiescence, the node has not built its stack: `/healthz`
  answers and `/readyz` reports not ready.

### Storage

- `DsqlConnectionDirector::install_ownership_gate` and `acquire_shutdown_control` accept the coordinated reservoir's
  director as well as the embedded one, and its checkout path honours the gate the same way on both.

### Platform paths

| Path | Today | After this change |
|---|---|---|
| Local or Compose with DSQL, one node | Serves | Serves; a second node refuses |
| ECS | Each `tokeirad` service relinquishes the others' leases at start, and all write unfenced | The first node to take the claim serves; the others refuse |
| ECS `tkr admin` task | Its `tokeirad` relinquishes the serving node's leases when it starts | Refuses while a node serves |
| Any node with a controller | Takes leases as the controller directs | Unchanged |
| Embedded managed or existing DSQL | Takes `embedded-owner` | Unchanged |

### Specs this changes

- [dsql-performance-fixes](../dsql-performance-fixes/requirements.md): criteria 1.1 and 1.2 apply once the node holds
  the single-node claim, and Requirement 1 links this spec.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, failing there:
- **live:** two nodes without a controller started on one cluster, the second while the first serves. The second
  refuses to start. On the code before the fix, both serve, and the second holds every lease.

### Property-Based Tests

- Property 1 runs offline over generated claim states (no row, released, live hold, expired hold) and attempt times,
  with a fake clock and a fake repository.
- Property 2 runs offline over generated schedules of two nodes on one fake claim with a shared fake clock: starts,
  renewals that succeed, fail or are delayed, crashes, partitions and clean shutdowns. It checks the open intervals of
  both gates.
- Property 3 runs offline over generated renewal outcomes and delays.
- Property 4 runs offline over generated shutdowns and failed starts, with and without a running quiescence.
- Property 5 is checked live, after the refusal.

### Unit Tests

- The refusal's error names the holder, the hold's expiry and what to do.
- The claim's name, identity and owner prefix are as this design gives them.

### Live Test

One test, on an ephemeral cluster with two ephemeral DynamoDB tables for the coordination:
1. **A second node.** Start node A, then node B. B refuses within 80 s, and nothing of B is started (Property 5). A
   still serves.
2. **A clean handover.** Stop A cleanly. B starts at once and serves. Stop B cleanly.
3. **A crash.** Start A, then abort it without a release. Start B. B serves only after A's hold has expired and 5
   minutes have passed.
4. **A lost claim.** While B serves, give the claim to another owner with SQL. B's next renewal is fenced, and B stops
   serving and returns an error.

After the refusal, the test checks that nothing was started:
- B's gRPC and Nexus completion addresses can be bound again;
- the shard leases and the claim are as A left them;
- no item of B's in the connection-lease table has an owner.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test
that covers it:
- **offline:** a busy claim treated as acquired;
- **offline:** an expired takeover that doesn't wait out the quiescence;
- **offline:** a release while a quiescence is running;
- **offline:** a failed renewal that leaves the gate open past the deadline;
- **live:** the claim skipped;
- **live:** a refusal that leaves its listener bound;
- **live:** a refusal that leaves the store running.

### Preservation Checking

The distributed schema check's live tests, the embedded ownership tests, the engine's embedded DSQL tests and the
in-memory listener tests stay green.
