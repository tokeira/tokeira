# tokeira-auth

Transport-independent identity and authorization primitives: Temporal-compatible claims, roles, call classification, grant matching, and authorization decisions. Deliberately free of HTTP, gRPC, and storage concerns — the serving layer plugs these primitives into its interceptors.

Part of [Tokeira](https://github.com/tokeira/tokeira) — Temporal-compatible durable execution in Rust, built on Aurora DSQL. Run it as a server, or embed it in your process. Most users should depend on [`tokeira-engine`](https://crates.io/crates/tokeira-engine); this crate is one of its components, published so the engine can be consumed from crates.io.

## License

Apache-2.0
