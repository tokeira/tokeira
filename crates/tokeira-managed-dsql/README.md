# tokeira-managed-dsql

Crash-safe ownership of a dedicated Aurora DSQL cluster for embedded use: idempotent creation anchored on a durable client token, canonical cluster identity, adoption of existing endpoints, and never a silent fallback.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
