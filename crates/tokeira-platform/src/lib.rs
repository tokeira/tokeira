//! Minimal language-neutral boundary for deployment-definition frontends.
//!
//! This crate owns transient located values, structural graph validation,
//! provider-kind placement, source diagnostics, content identity, and safe
//! inspection publication. Concrete platforms own services, images, provider
//! calls, state-store construction, operations, and inspection rendering.

pub mod author;
pub mod config;
pub mod content;
pub mod context;
pub mod definition;
pub mod error;
pub mod graph;
pub mod inspection;
pub mod kind;

#[cfg(test)]
mod tests;
