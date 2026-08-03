//! Definition-language-neutral platform authoring and projection framework.
//!
//! This crate owns the common in-memory contract driven by definition
//! frontends and selected by first-party platform bindings. It deliberately
//! contains no parser, provider implementation, platform inventory, CLI, or
//! provisioner entrypoint. Frontends translate their runtime values into
//! [`author::LocatedValue`]; provider crates supply typed registrations through
//! [`catalog`]; platform crates assemble immutable values through [`binding`].

pub mod artifact;
pub mod author;
pub mod binding;
pub mod catalog;
pub mod config;
pub mod context;
pub mod definition;
pub mod error;
pub mod graph;
pub mod ops;
pub mod projection;
pub mod selection;

#[cfg(test)]
mod tests;
