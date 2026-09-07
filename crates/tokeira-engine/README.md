# tokeira-engine

`tokeira-engine` embeds Tokeira's Temporal-compatible runtime in a Rust process.
It provides an in-process RPC endpoint suitable for
`temporalio-client::ConnectionOptions::service_override`, without opening a TCP
listener, while `tokeirad` uses the same construction path with network transports.

The crate is under active development and currently targets Temporal Server
v1.31.0 behaviour.

## Connecting the Temporal Rust SDK

```rust,no_run
use temporalio_client::{Connection, ConnectionOptions};
use tokeira_engine::Engine;

# async fn connect() -> anyhow::Result<()> {
let engine = Engine::start().await?;
let options = ConnectionOptions::new(
    "http://tokeira-engine.invalid:7233".parse()?,
)
.service_override(engine.service_override())
.dns_load_balancing(None)
.build();
let connection = Connection::connect(options).await?;

drop(connection);
engine.shutdown().await?;
# Ok(())
# }
```

The URL is only an SDK configuration value when `service_override` is present;
the engine performs no DNS lookup and opens no listener. DNS load balancing must
be disabled because the callback service is the complete transport.

## Attaching a listener

Workers in other processes cannot reach the in-process endpoint. A running
engine can serve the same Temporal gRPC services on a TCP address the host
chooses; the listener serves the engine's own routes, so in-process and network
clients see one engine, one runtime, and one storage owner.

```rust,no_run
use tokeira_engine::Engine;

# async fn listen() -> anyhow::Result<()> {
let engine = Engine::start().await?;

// Port 0 asks for an ephemeral port; `bound_addr` reports the real one. An
// unspecified address (`0.0.0.0`) is reported unspecified: the host decides
// which interface address to advertise to other processes.
let listener = engine.listen("127.0.0.1:0".parse()?).await?;
let address = listener.bound_addr();
// Point a Temporal SDK worker at `http://{address}` ...

// Stop the listener on its own; the engine keeps serving in-process.
listener.shutdown().await?;
// Or shut the engine down: any attached listener is stopped and drained
// before the engine releases its leases, ownership, and storage.
engine.shutdown().await?;
# Ok(())
# }
```

Every `Engine::start*` path still binds nothing. A failed bind returns an error
and leaves the engine untouched. Stopping a listener resets its in-flight calls
with `UNAVAILABLE`, the same outcome a worker sees when a connection resets, so
a parked long poll never holds shutdown for its timeout.

## Existing Aurora DSQL connections

Use `Engine::start_with_embedded_config` with `EmbeddedStorageConfig::ExistingDsql`
for a durable embedded engine. Supply canonical Region, cluster ID and ARN, an
explicit migration policy, and the database hostname in
`ExistingEmbeddedDsqlConfig.endpoint`.

The engine validates cluster identity and status through AWS `GetCluster`. The
configured endpoint selects the database connection path for the entire engine
generation, including scale-to-zero wake, fresh IAM tokens, TLS hostname
verification, migrations, ownership, and runtime connections. AWS readiness and
post-wake observations do not replace it. Change the configuration at the next
startup to select a different locator. `server.infrastructure.dsql.endpoint` does
not override this embedded setting.

For [Aurora DSQL PrivateLink](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/privatelink-managing-clusters.html),
supply the `clusterVpcEndpoint` returned by `GetVpcEndpointServiceName` for the
canonical cluster. The host provisions the database and management connectivity.
The endpoint grants no authority to create, replace, delete, or change protection
on a cluster.

`engine.startup_report().cluster.as_ref().map(|cluster| &cluster.endpoint)` reports
the locator supplied to the database connection factory. Managed mode continues
to use the AWS endpoint observed before pool warmup; subsequent wake observations
do not change that report field or retarget the pool. No configuration fields or
dependencies are added by this correction. Consumers using a different intended
locator must update the existing field before restarting.

## Optional snapshots

The embedded store remains ephemeral unless snapshot policy is present. A
configured engine restores an existing snapshot before runtime recovery, replaces
the file atomically on the configured interval, and writes one final snapshot from
`Engine::shutdown`.

```rust,no_run
use std::path::PathBuf;

use tokeira_engine::{Engine, SnapshotPolicyConfig, TokeiraConfig};

# async fn start() -> anyhow::Result<()> {
let mut config = TokeiraConfig::default();
config.policy.snapshot = Some(SnapshotPolicyConfig {
    location: PathBuf::from("state/tokeira.snapshot"),
    interval_ms: 30_000,
});
let engine = Engine::start_with_config(config).await?;

// Graceful shutdown reports any final snapshot failure to the caller.
engine.shutdown().await?;
# Ok(())
# }
```

A missing file starts a fresh store. Malformed snapshots and unsupported format
versions fail startup rather than silently discarding state. The format is an
unstable dev/embedded convenience, not a cross-version compatibility surface.
