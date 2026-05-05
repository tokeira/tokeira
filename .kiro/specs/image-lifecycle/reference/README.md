# Reference implementation: `dagger-client`

This folder contains the complete `dagger-client` crate from the `temporal-dsql-deploy-eks` project, provided so agents implementing Task 1 do not need to read upstream source or make copying choices.

## Contents

- `Cargo.toml` — crate manifest (3 runtime deps, 2 dev-deps; pin to workspace versions when porting)
- `lib.rs` — 421-line thin Dagger GraphQL client: `Client`, `Container`, `Directory`, `File`, typed IDs, `quote` helper
- `quote_tests.rs` — proptest that round-trips any input string through `quote` and JSON parsing

## How to use this during Task 1

1. Create `crates/dagger-client/` in the Tokeira workspace.
2. Copy `Cargo.toml` to `crates/dagger-client/Cargo.toml`.
3. Copy `lib.rs` to `crates/dagger-client/src/lib.rs`.
4. Copy `quote_tests.rs` to `crates/dagger-client/tests/quote_tests.rs`.
5. Add `"crates/dagger-client"` to the workspace `Cargo.toml` members list.
6. Replace `dagger-client` in `Cargo.toml` dependency declarations with workspace-pinned versions if Tokeira uses a different `[workspace.dependencies]` policy for `base64`, `eyre`, `reqwest`, `serde`, `serde_json`, or `proptest`.
7. If Tokeira's AGENTS.md mandates `thiserror` for library crates (which it does), plan to migrate the `eyre::Result` public surface to `thiserror`-based error types in a follow-up — do this only if the workspace policy does not permit `eyre` in library crates. The EKS reference uses `eyre` throughout; a straight port is acceptable for the initial task and the migration can land in a later checkpoint.

## What NOT to change during the port

- **The GraphQL query strings.** Every `format!(r#"{{ ... }}"#)` block in `lib.rs` encodes an exact Dagger GraphQL query that has been tested against a live Dagger session. Modifying them risks breaking the image build pipeline.
- **The `quote` helper.** It uses `serde_json::to_string` to JSON-escape strings for embedding in GraphQL. This is the correct escaping for Dagger's GraphQL dialect. Do not hand-roll quoting.
- **The macro `container_op!`.** It encodes the `loadContainerFromID(id) { <method>(<args>) { id } }` resume pattern. All `with_*` methods on `Container` use it.
- **The `export_image` docker-load flow.** Dagger does not have a direct `docker load` sink, so the client writes an OCI tarball via `export(path:)` then shells out to `docker load` + `docker tag`. This is the only path that makes a locally-built image available for `docker compose up` to find.
- **Timeouts.** The HTTP client sets a 600-second timeout on requests. Some Dagger operations (notably a cold `cargo build --release`) can exceed the default timeout; 600s has been tuned for Temporal-scale builds and should be sufficient for `tokeirad`.

## What the port MUST change

- The doc-comment example in `lib.rs` refers to `cargo run --package dsqld-build -- temporal`. Update this to `cargo run --package tokeira-build -- build` in Tokeira's copy.
- The `Cargo.toml` `description` field is free to reword; keeping the existing wording is fine.

## Why a line-for-line port

The EKS reference has been exercised against Dagger 0.20.x over multiple releases and handles a number of Dagger-specific edge cases (ID-based object resumption across queries, GraphQL error propagation, registry-auth secret binding). Re-deriving the client from the Dagger docs would reproduce the same edge cases for zero incremental value. The dependency surface is small (`base64`, `eyre`, `reqwest`, `serde`, `serde_json`) and the crate is self-contained.

## Relationship to the `DaggerClient` trait in `tokeira-build`

The `DaggerClient` trait defined in this spec's design document (§DaggerClient Trait) is a thin wrapper over the types in this crate. The trait boundary exists so unit tests can substitute a mock; it is NOT a replacement for this client. Task 1 produces this crate; Task 2.4 wraps it behind the trait.
