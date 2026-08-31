# tokeira-compatibility

Canonical metadata for Temporal feature compatibility, SDK support,
configuration classification, conformance expectations, and RPC dispatch.

## Where it sits

This is a policy crate in the compatibility edge. It gives binaries, edge
adapters, conformance tools, and build provenance one shared answer without
depending on the edge, runtime, or kernel crates.

## Surface map

| Module | Contract |
|---|---|
| `feature` and `matrix` | Feature catalog types, `FEATURE_MATRIX`, newer-vendored-wire ledger |
| `sdk` | `SDK_MATRIX`, version compatibility entries, verification state |
| `configuration` | Static and runtime configuration classification, verified ledgers, conformance override disposition |
| `coverage` | RPC classification, feature lookup, expected conformance outcome, wire-path normalization |
| `dispatch` | `dispatch_rpc`, enablement decisions, disabled reasons, dynamic-config seam |
| `digest` | Stable feature- and SDK-matrix digests for build provenance |

## Contracts

- The feature and SDK matrices are the checked-in source of truth for published
  compatibility claims.
- Vendored RPCs newer than the targeted server behaviour are classified
  separately from the baseline claim.
- Catalog and configuration verification reject duplicate, inconsistent, or
  unclassified entries before they can drive dispatch.
- Dispatch combines a call's feature classification with its enablement policy;
  it does not execute the call.
- Conformance expectations derive from the same feature catalog rather than a
  separate hand-maintained list.
- Matrix digests let build metadata identify the exact compatibility policy
  compiled into a binary.

## It does not own

The crate performs no I/O, reads no live server configuration by itself, and
does not implement RPC handlers or workflow semantics. The edge enforces
dispatch outcomes; `tokeira-build-info` embeds the resulting digests.

## Pointers

- [Crate root](../../crates/tokeira-compatibility/src/lib.rs)
- [Compatibility edge](edge.md)
- [Build provenance](build-info.md)
- [Compatibility architecture](../architecture/005-decisions-and-boundaries.md)
- [Conformance readiness](../readiness/conformance.md)
