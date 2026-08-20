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
