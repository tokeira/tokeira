//! Compatibility pins reviewed through the Temporal compatibility spec.
//!
//! These constants are the human-maintained compatibility claims. Updating
//! either value requires a spec-backed bump and matrix/evidence review.
//!
//! `TEMPORAL_PROTO_VERSION` mirrors the vendored upstream Temporal protobuf
//! tree (`proto/UPSTREAM_VERSION`). `TEMPORAL_SERVER_COMPAT` is the highest
//! Temporal server release whose SDK-visible behaviour Tokeira claims to
//! match, established through the api-conformance tracker.
//!
//! Tracked-ahead note: the vendored proto surface (`v1.62.11`) is ahead of the
//! API version that Temporal Server `1.31.0` ships (`v1.62.8`). The extra RPCs
//! present only in `v1.62.11` (Nexus operation execution) are not part of the
//! `1.31.0` compatibility claim and are tracked separately in the
//! api-conformance tracker.

pub const TEMPORAL_PROTO_VERSION: &str = "v1.62.11";
pub const TEMPORAL_SERVER_COMPAT: &str = "1.31.0";

/// The workspace's pinned toolchain channel, mirrored from
/// `rust-toolchain.toml` for builds of the published crate, which carry no
/// workspace and no toolchain pin. A parity test fails the workspace build
/// when the two drift.
pub const PINNED_RUST_TOOLCHAIN: &str = "1.97.1";
