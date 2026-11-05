# tokeira-clock

The wall clock the engine reads the current time from: a `Clock` trait and the production `SystemClock`. Storage, the runtime and the edge stamp deadlines and judge them through one shared clock, so a harness can run the engine in simulated time by supplying its own. The kernel never depends on it.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
