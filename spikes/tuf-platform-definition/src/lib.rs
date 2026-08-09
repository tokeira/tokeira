//! Spike: TUF (via `tough`) as the representation of a complete platform
//! definition.
//!
//! Standalone by contract — excluded from the tokeira workspace, no tokeira
//! crate dependencies; product shapes are mirrored in `set` with citations.
//! The question under test: *can a signed TUF repository carry a
//! multi-document platform definition set — with the product's
//! `sha256-set-v1` identity and served-part order intact — over S3, signed
//! by KMS-holdable keys, and hand the verified bytes to the product's
//! existing seams (`SourceResolver`, `DefinitionSeed`)?*
//!
//! Module map:
//! - [`set`] — mirrored product shapes (resolver seam, set identity).
//! - [`keys`] — generated Ed25519 role keys behind `LocalKeySource`.
//! - [`publish`] — root.json authoring + `RepositoryEditor` publication.
//! - [`consume`] — verified load, set-claim enforcement, seed extraction.
//! - [`s3`] — `S3Transport` (`tough::Transport` over `GetObject`) and the
//!   create-only uploader.
//! - [`kms`] — KMS-held role keys through `tough-kms`.

pub mod consume;
pub mod keys;
pub mod kms;
pub mod publish;
pub mod s3;
pub mod set;
