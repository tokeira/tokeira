# tokeira-chasm-activity

Standalone activities as a durable component: scheduling, timeout arming, retry state, and completion, executed on the shared state-machine substrate rather than as a workflow side effect.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
