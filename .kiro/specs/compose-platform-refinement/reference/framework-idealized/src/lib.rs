//! The bound framework: engine + platform-agnostic + platform-specific.
//!
//! A bound `tkp` is three layers, each owning a different question:
//!
//! | Layer | Value | Owns |
//! |---|---|---|
//! | platform-specific | `PlatformDeclaration` (from `platforms/compose`) | what the platform IS: definitions, content, providers, kind selections |
//! | platform-agnostic | [`platform::BoundPlatform`] | what platform a deployment is, and whether THIS binary may operate it: identity, admission, capabilities |
//! | engine | [`engine::Engine`] | changing it: evaluate, verify, realize, infra plan/apply/destroy, deploy plan/apply, state, selection, writeback |
//!
//! The shell (the CLI verbs) sits above all three and is concrete: it
//! admits the deployment once per command, drives [`engine::Engine`]'s
//! inherent methods for lifecycle with the admitted value, reads
//! [`platform::BoundPlatform`] directly for identity and capabilities, and
//! calls the platform's ops surface directly for live substrate questions.
//! There is no platform trait for verbs to implement and no wrapper that
//! answers "not applicable" — a verb that depends on a capability checks
//! its presence on the platform and renders the refusal itself.
//!
//! Composition, whole — the generated root names the two factories and the
//! identity pair; the framework does everything else:
//!
//! ```rust,ignore
//! tokeira_provisioner_cli::bound_provisioner_main! {
//!     expected_platform: "compose",
//!     platform: tokeira_compose_deployment::platform,
//!     expected_format: "tkd",
//!     frontend: tokeira_tkd::frontend,
//! }
//! // expands to:
//! fn main() -> std::process::ExitCode {
//!     run_bound_provisioner("compose", "tkd", platform(), frontend())
//! }
//! // which is:
//! //   let platform = BoundPlatform::bind(expected_platform, expected_format, declaration)?;
//! //   let engine = Engine::new(platform, frontend)?;
//! //   cli::run(engine)
//! ```
//!
//! Binding happens once, at process start: vocabulary composition (a
//! kind-name collision between declared providers refuses the binary,
//! naming both) and the platform/format identity the binary was built as.
//! Admission happens per deployment: the platform decides whether the
//! deployment on disk is one of its own before the shell takes a lock or
//! reads any state.

//! Provider attributes ride the same layers: the definition authors them at
//! resource and deployment level, the framework transports the declared
//! names' values, and the provider resolves them by its own precedence rule
//! ([`attributes`]).

pub mod attributes;
pub mod engine;
pub mod instantiation;
pub mod platform;
