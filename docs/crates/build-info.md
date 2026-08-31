# tokeira-build-info

Compile-time, immutable build and compatibility provenance for binaries, logs,
version output, and compatibility RPCs.

## Where it sits

This is a cross-cutting leaf crate. It has no runtime dependencies and performs
no I/O. Its build script resolves provenance and embeds it as constants.

## Surface map

| Area | Representative values |
|---|---|
| Product identity | `TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`, `SERVER_VERSION`, `BUILD_MODE` |
| Compatibility pins | `TEMPORAL_SERVER_COMPAT`, `TEMPORAL_PROTO_VERSION` |
| Build inputs | `RUST_TOOLCHAIN`, `SOURCE_TREE_HASH` |
| Policy identity | `FEATURE_MATRIX_DIGEST`, `SDK_MATRIX_DIGEST` |
| Schema compatibility | minimum supported, target, and maximum readable schema versions; migration-set digest |
| Aggregate | `BuildInfo` and the const `summary()` function |

The `pinned` module carries package-safe copies of release pins needed when a
crate is built outside the workspace.

## Contracts

- Metadata is fixed when the crate is compiled; reading it cannot inspect the
  filesystem, invoke Git, or contact another service.
- `SERVER_VERSION` is the SDK-visible server version and includes source
  identity in SemVer build metadata.
- The Temporal server compatibility target and vendored proto version are
  independent values.
- Matrix digests identify the exact feature and SDK policy compiled into the
  build.
- Schema bounds and the migration-set digest identify the storage compatibility
  contract carried by the release.
- Packaged fallback copies are parity-tested against the workspace toolchain and
  storage schema contract.

## It does not own

The crate does not decide feature support, generate protobufs, run migrations,
or evaluate schema compatibility at startup. It reports values owned by the
compatibility, proto, toolchain, and storage layers.

## Pointers

- [Crate root](../../crates/tokeira-build-info/src/lib.rs)
- [Pinned constants](../../crates/tokeira-build-info/src/pinned.rs)
- [Compatibility metadata](compatibility.md)
- [Storage](storage.md)
