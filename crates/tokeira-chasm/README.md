# tokeira-chasm

The durable state-machine framework the engine is built on. Workflows are not a special case: every durable construct — workflows, standalone activities, future components — is a state machine on this substrate, with persistence and dispatch handled uniformly underneath.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
