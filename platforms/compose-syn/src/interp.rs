//! Facade preserving the `compose_syn::interp` public path over the shared
//! [`tokeira_tkd`] interpreter and the compose [`ComposeBridge`] (Requirement 14).
//!
//! The interpreter itself now lives in `tokeira-tkd`, generic over a platform
//! [`tokeira_tkd::HostBridge`]; this module binds it to compose's bridge and
//! re-exports the same entry points (`interpret`/`validate`/`retarget_check`) the
//! adapter, `tkp`, and the fidelity tests already call — so the extraction is
//! transparent to every consumer.

use crate::{bridge::ComposeBridge, builder::Deployment, context::Cx};

pub use tokeira_tkd::EvalError;

/// The compose runtime value type — the shared [`tokeira_tkd::Value`] over the
/// compose host handle. This is the config value the operator authored, returned
/// alongside the realized deployment (used by the retarget/admission checks).
pub type Value = tokeira_tkd::Value<crate::bridge::HostObj>;

/// Parse + interpret a compose `.tkd` into a realized [`Deployment`] and the
/// resolved config value. Byte-identical to the compiled `definition.rs`.
pub fn interpret(src: &str, cx: &Cx) -> Result<(Deployment, Value), EvalError> {
    tokeira_tkd::interpret(src, &ComposeBridge, cx)
}

/// Validate a compose `.tkd` against the interpreted subset (reject-by-default).
pub fn validate(src: &str) -> Result<(), Vec<String>> {
    tokeira_tkd::validate(src, &ComposeBridge)
}

/// `#[create]` retarget check between a recorded and a new config value.
pub fn retarget_check(src: &str, old: &Value, new: &Value) -> Result<(), Vec<String>> {
    tokeira_tkd::retarget_check(src, old, new)
}
