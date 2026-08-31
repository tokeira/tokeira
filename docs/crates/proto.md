# tokeira-proto

Ahead-of-time generated protobuf and RPC bindings for the public
Temporal-compatible API and Tokeira-owned control-plane contracts.

## Where it sits

The crate is part of the compatibility edge. It owns wire shapes and service
metadata, while `tokeira-edge` owns request handling and `tokeira-types` owns
transport-neutral values.

## Surface map

| Module | Contract |
|---|---|
| `public` | Generated `temporal.api.*` packages, public service names, the descriptor set, and embedded OpenAPI documents |
| `conversions` | Explicit conversions for shared payloads, headers, memo, search attributes, task queues, tokens, timestamps, and durations |
| `connect` | Preferred Connect, gRPC, and gRPC-Web controller surface using buffa and connect-rust |
| `internal` | Legacy tonic/prost Tokeira runtime, admin, and controller packages |
| `compute::v1` | Provider-neutral Worker Compute request and response messages |
| `compute` constants | Fixed Nexus service, operation, and message-type identifiers for Worker Compute |

## Generation contract

Bindings are checked in under `src/generated/`. A normal crate build does not
run `protoc` or require the vendored proto tree. `tools/proto-sync` regenerates
the checked-in output after an intentional proto or code-generator change.

The vendored files under `proto/upstream/`, not generated build output, are the
authority for Temporal wire shape. The compatibility target for observable
server behaviour is tracked separately by `tokeira-build-info`.

## Key invariants

- Upstream Temporal messages are not extended with Tokeira-owned fields.
- Wire/domain conversion is explicit; domain crates do not depend on protobuf
  messages.
- The public descriptor set drives descriptor-derived HTTP/JSON routing.
- Controller callers should prefer `connect`; `internal` remains for code that
  still uses the tonic/prost surface.
- Worker Compute messages remain provider-neutral; provider execution belongs
  outside this crate.

## It does not own

This crate does not validate Temporal behaviour, dispatch RPCs, authorize
callers, run workflows, or persist state. Generated bindings describe the wire;
the edge and compatibility crates decide how supported calls are admitted.

## Pointers

- [Crate root](../../crates/tokeira-proto/src/lib.rs)
- [Vendored upstream protos](../../proto/upstream/)
- [Compatibility metadata](compatibility.md)
- [Compatibility edge](edge.md)
