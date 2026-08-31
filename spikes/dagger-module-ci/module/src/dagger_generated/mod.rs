//! PRE-GENERATION STUB — `dagger generate` (via the re-init loop in
//! ../../HANDOFF-rust4-rerun.md) replaces this module with the real generated
//! tree. It exists so the authored crate satisfies both consumers of the
//! `dagger_generated` module before generation has run: the macro-emitted
//! bridge impls (`crate::dagger_generated::__private::…`) and the authoring
//! source walker, which follows `mod` declarations and fails the whole
//! authoring compilation on a declared module with no source document.

#[doc(hidden)]
pub mod __private {
    pub use dagger_sdk::__private::*;
}

mod module_context;
pub use module_context::{ModuleContext, ModuleQuery};
