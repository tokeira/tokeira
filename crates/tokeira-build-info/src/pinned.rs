//! Compatibility pins reviewed through the Temporal compatibility spec.
//!
//! These constants are the human-maintained compatibility pins. Updating
//! a pin requires a spec-backed bump and matrix/evidence review.
//!
//! `TEMPORAL_PROTO_VERSION` mirrors the vendored upstream Temporal protobuf
//! tree (`proto/UPSTREAM_VERSION`). `TEMPORAL_SERVER_COMPAT` is the highest
//! Temporal server release whose SDK-visible behaviour Tokeira claims to
//! match, established through the api-conformance tracker.
//!
//! The vendored proto surface (`v1.63.5`) equals the API version Temporal
//! server `1.32.0` ships (`go.mod @ v1.32.0`); it is no longer tracked ahead
//! of the campaign release. The advertised compatibility claim remains
//! `1.31.0` until the campaign supplies conformance evidence. Newer wire
//! surfaces alone do not expand that claim.

/// Vendored Temporal API tag; mirrors `proto/UPSTREAM_VERSION` exactly.
pub const TEMPORAL_PROTO_VERSION: &str = "v1.63.5";
/// Highest Temporal server release backed by the compatibility claim.
pub const TEMPORAL_SERVER_COMPAT: &str = "1.31.0";

/// The Temporal server release under compatibility campaign. Equals
/// [`TEMPORAL_SERVER_COMPAT`] when no campaign is running. Read by the
/// conformance pin gate and documentation tooling; never advertised as the claim.
/// The release identity is verified in `common/headers/version_checker.go @ v1.32.0`.
pub const TEMPORAL_SERVER_TARGET: &str = "1.32.0";

/// The workspace's pinned toolchain channel, mirrored from
/// `rust-toolchain.toml` for builds of the published crate, which carry no
/// workspace and no toolchain pin. A parity test fails the workspace build
/// when the two drift.
pub const PINNED_RUST_TOOLCHAIN: &str = "1.97.1";
