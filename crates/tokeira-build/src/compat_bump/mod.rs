//! Compatibility-bump governance primitives for the future bump engine.
//!
//! The `ci` Dagger module's bump-trailer check validates trailers now. The mutation
//! phases remain a later release-process slice, but they must render through the same
//! typed format or the protocol could accept a commit the producer cannot reproduce.

mod trailer;

pub use trailer::{BumpTrailer, BumpTrailerError, BumpTrigger, CompatibilityVersion};
