# Temporal Rust SDK 0.7 embedded-engine spike

This standalone spike runs the published Temporal Rust SDK `0.7.0` against
`tokeira-engine` in one process. It starts an SDK worker, executes a workflow
that calls an activity, waits for the typed result, and shuts both worker and
engine down cleanly.

The client and worker use `ConnectionOptions::service_override`; no Temporal
TCP server or DNS lookup is involved. The program keeps the configured gRPC
and Nexus listener addresses occupied for its entire run, so an accidental
network-listener fallback fails deterministically.

Run it from the repository root:

```console
cargo run --manifest-path spikes/temporal-rust-sdk-v0-7-embedded/Cargo.toml --locked
```

Successful output ends with:

```text
Temporal Rust SDK: 0.7.0
Transport: temporalio-client::service_override (no TCP listener)
Workflow run_id: <generated run id>
Workflow result: Hello, embedded Tokeira!
```

The spike is excluded from the main workspace so its exact SDK `0.7.0` pins
and standalone lockfile do not expand or constrain the product workspace.
