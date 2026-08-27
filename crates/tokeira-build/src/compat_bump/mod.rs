//! Compatibility-bump governance primitives shared by CI and the future bump engine.
//!
//! The CI pipeline owns trailer validation now. The mutation phases remain a later
//! release-process slice, but they must render through the same typed format or the
//! protocol could accept a commit the producer cannot reproduce.

mod trailer;

pub use trailer::{BumpTrailer, BumpTrailerError, BumpTrigger, CompatibilityVersion};
