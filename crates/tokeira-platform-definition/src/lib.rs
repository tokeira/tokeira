//! The definition frontends: every shipped format, one crate.
//!
//! A platform definition is authored in one of the formats this crate
//! carries — `tkd` (Rust syntax, interpreted) or `tkdp` (Python, executed by
//! Monty) — and each frontend lives here as a feature-gated module with its
//! public surface unchanged from its former standalone crate. Composition
//! selects exactly one format feature per bound `tkp` (the generated
//! manifest names it), so a `tkd`-only build never compiles the Monty/ruff
//! dependency train; workspace consumers name the features they need.
//!
//! Frontend contracts are format-owned: see the `tkd` and `tkdp` modules for
//! each format's own invariants and entry points. (Named in prose, not
//! intra-doc links — a single-feature build documents only the enabled
//! module, and a link to the absent one would break the doc build.)

#[cfg(feature = "tkd")]
pub mod tkd;

#[cfg(feature = "tkdp")]
pub mod tkdp;
