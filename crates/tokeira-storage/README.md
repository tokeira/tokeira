# tokeira-storage

Semantic persistence traits (`RunRepository`, lease and queue stores) with the Aurora DSQL backend and an in-memory development store underneath. Runtime code depends on the traits, never on a driver.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
