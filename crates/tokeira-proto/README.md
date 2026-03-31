# tokeira-proto

Generated protobuf and gRPC bindings for the Tokeira workspace.

This crate sits at the **wire boundary** of the system:

- it owns the generated public Temporal-compatible API bindings;
- it owns the generated Tokeira-internal API bindings;
- it centralizes small conversion helpers between wire objects and `tokeira-types`.

## Why this crate exists

The rest of the Tokeira workspace should not need to know about:

- `prost`-generated struct shapes,
- protobuf package paths,
- gRPC service trait names,
- or the exact oneof/map encoding details chosen by protobuf.

Those are transport concerns.

The edge layer, admin surfaces, and any compatibility tools should depend on `tokeira-proto`,
while the kernel/runtime/storage/projection layers should prefer `tokeira-types` and their own
domain-native structs.

## What is in this starter crate

This starter crate contains:

- a **trimmed Temporal-compatible snapshot** of the public API surface that Tokeira is most likely
  to exercise early:
  - `WorkflowService`
  - `OperatorService`
  - common payload and search-attribute messages
  - execution status and indexed-value enums
- a **small Tokeira-internal proto surface** for runtime/admin coordination:
  - runtime command envelopes
  - dispatch records
  - projection records
  - admin service scaffolding
- conversion helpers for:
  - payloads, memo, headers, search attributes
  - execution status and count/list visibility rows
  - search-attribute type registry wiring

## Important note about upstream compatibility

This crate is intentionally a **starter**. It does **not** yet vendor the full upstream
Temporal API repo.

In the mature repository layout, the recommended move is:

- keep `proto/` at the workspace root,
- sync `proto/upstream/temporal/...` from upstream Temporal,
- point this crate's `build.rs` at that root-level `proto/` tree.

This artifact keeps the proto tree **inside the crate** so that it is easy to download, inspect,
and evolve in isolation.

## Design principles

### Keep generated code centralized
Only one crate should call `tonic_build` for the shared API surface.

### Keep domain types separate
If a type expresses the business/runtime meaning of something, it probably belongs in
`tokeira-types`, not here.

### Keep conversions explicit
Public API compatibility is important. Explicit conversion code makes it easier to see where
Tokeira is preserving Temporal-compatible shapes and where it intentionally diverges internally.

## Expected next step

Once this crate is in your private repo, the most sensible next evolution is to:

1. switch `tokeira-edge` to depend on `tokeira-proto`,
2. move some request/response translation out of `tokeira-edge` into these conversions,
3. eventually replace the trimmed public proto snapshot with vendored upstream Temporal protos.
