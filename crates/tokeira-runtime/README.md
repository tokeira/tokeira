# tokeira-runtime

Where durable semantics meet scheduling: lane-local execution serializes each run's transitions, dispatch queues stay disposable, and every state change lands as a fenced durable transition before its effects are delivered.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
