//! The deployment repository: every Deployment's durable, authenticated
//! lineage as a TUF repository (deployment-repository spec).
//!
//! One repository per named Deployment; residency follows the state choice
//! made at create — a local filesystem directory for a local deployment, an
//! S3 location for remote state — with one verification path over both,
//! differing only in transport. Publications are monotonic versions written
//! at create and after committed lifecycle transitions; the envelope stays
//! the sole commit authority and a publication is a derived, signed
//! projection of committed state.
//!
//! Module map (design §Components):
//! - [`locator`] — where one repository lives; [`config`] — the publisher's
//!   `deny_unknown_fields` configuration; [`keys`] — role keys as sources
//!   (local Ed25519 files or KMS).
//! - [`claim`] — the Deployment Claim binding both halves of a publication.
//! - [`transport`] — `tough::Transport` over S3 `GetObject`; [`writer`] —
//!   the create-only/mutable-head write policy in both homes.
//! - [`publish`] — root authoring and `publish_transition`; [`open`] —
//!   verified load and claim enforcement; [`fetch`] — the materialize plan;
//!   [`list`] — deployment enumeration.
//! - [`error`] — typed refusals with stable names.

pub mod claim;
pub mod config;
pub mod error;
pub mod fetch;
pub mod keys;
pub mod list;
pub mod locator;
pub mod open;
pub mod publish;
pub mod transport;
pub mod writer;

#[cfg(test)]
pub(crate) mod testkit;

#[cfg(test)]
mod tests;
