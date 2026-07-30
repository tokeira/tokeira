//! `tokeira-eks-deployment` — the `syn`-authored EKS platform for tokeira.
//!
//! This is the Kubernetes sibling of `platforms/ecs` and the AWS sibling of
//! `platforms/compose`: it provisions tokeira on AWS EKS with Aurora DSQL,
//! authored in the `syn` deployment DSL (Proposals 003/004) and driven by `tkp`.
//! It sits in the platform plane — it owns *what* to provision (the config, the
//! kinds, the builder vocabulary, the `definition.tkd`, the orchestrator
//! adapter), while the interpreter (`tokeira-tkd`), the live Kubernetes apply
//! (`tokeira-k8s`), and the AWS resource implementations (`tokeira-aws`) are
//! shared crates it consumes unchanged.
//!
//! **The DSL is the config (Proposal 003 §3).** Unlike the classic
//! `platforms/ecs`, there is no serde `EksConfig`/TOML config file: the config
//! types, their `config()` defaults, and their `#[create]`/`#[require]`
//! attributes are authored in `definition.tkd` and read by the interpreter's
//! `TypeTable`. This crate therefore has no `config.rs`; validation is
//! `#[require]` and unknown-field rejection is the interpreter subset. The only
//! TOML in play is the *server* config (`tokeirad.toml`) that writeback fills.
//!
//! Module map (built incrementally per the platform-eks spec's task blocks):
//! - [`context`] — the engine-injected [`context::Cx`] the `.tkd` reads.
//! - [`builder`] — the author's builder vocabulary ([`builder::Deployment`] and
//!   the [`builder::Kind`] trait) that a realized `.tkd` records into.
//!
//! Later blocks add `definition.tkd` and `adapter` (the
//! `tokeira_orchestrator::Deployment`/`Ops` seam).

pub mod bridge;
pub mod builder;
pub mod context;
pub mod k8s_resource;
pub mod kinds;
pub mod manifests;
