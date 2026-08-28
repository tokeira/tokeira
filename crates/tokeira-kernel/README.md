# tokeira-kernel

The pure transition core: given durable state and a command, produce the next state and its effects. No I/O, no async, no clocks, no randomness — correctness lives here, everything operational lives above.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
