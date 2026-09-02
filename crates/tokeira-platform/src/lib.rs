//! Minimal language-neutral boundary for deployment-definition frontends.
//!
//! This crate owns transient located values, structural graph validation,
//! provider-kind placement, source diagnostics, content identity, and safe
//! inspection publication. Concrete platforms own services, images, provider
//! calls, operations, and inspection rendering. The orchestration layer retains
//! state-store lifecycle and its existing concrete-store selection seam.

pub mod author;
pub mod content;
pub mod context;
pub mod declaration;
pub mod definition;
pub mod error;
pub mod graph;
pub mod inspection;
pub mod kind;
pub mod ops;

#[cfg(test)]
mod tests;
