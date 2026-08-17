//! The Compose platform.
//!
//! A platform describes its infrastructure and its services. `tkp` invokes
//! the platform and owns everything about changing it: definition
//! evaluation and verification, planning, confirmation, apply and destroy
//! ordering, recorded state, `--module` selection (prerequisites on apply,
//! dependants on destroy), writeback resolution and persistence, inspection
//! projections, retarget refusal, and progress. Nothing in this crate
//! manages change.
//!
//! Compose is five things:
//!
//! 1. **The definitions** — the `.tkd` set (`deployment.tkd` wiring the
//!    modules, `platform.tkd` carrying the configuration model,
//!    `observability.tkd` declaring that module's services) and its
//!    `definition.tkdp` peer: the programs that describe the deployment's
//!    infrastructure and services in terms of the kinds wired below, and
//!    author the configuration shape (`struct Compose` and friends) exactly
//!    once. This
//!    crate defines no config types and validates no config values: each
//!    kind validates its own input where it is authored, and the
//!    definition's shapes make invalid states unrepresentable.
//! 2. **The companion content** — `observability/` beside the definitions:
//!    the dashboards, alert rules, and backend config templates, staged
//!    with the definition and resolved against the definition source's own
//!    directory, so a retained revision renders its own content and a
//!    dashboard edit is a visible, plannable change to this deployment.
//! 3. **The Compose resources** — `tokeira-compose` supplies its frontend
//!    namespace, concrete resource/service models, Docker runtime platform,
//!    reachability, and live operations. This platform definition selects
//!    and integrates those capabilities; the resource crate does not export
//!    a preassembled platform.
//! 4. **The platform's own kinds** — [`observability`]: the
//!    `ObservabilityConfiguration` bundle that renders the companion
//!    content. Tokeira-opinionated description and its machinery, owned
//!    here and contributed through the platform's own namespace; the
//!    Compose resource crate keeps only the fencing contract
//!    (`config_content_resource_id`) its `Service` consumers key on.
//!    The AWS resource namespace joins alongside it.
//! 5. **The entry point** — [`platform`]: the one exported declaration
//!    `tkp` invokes. There is no binary here; `tkp` is the binary and
//!    includes the framework for invoking the platform.
//!
//! The contract types the entry point speaks (`PlatformDeclaration` and
//! what it carries) are `tkp`'s: the framework defines what it consumes.

pub mod observability;

use std::sync::Arc;

use tokeira_platform::{declaration::PlatformDeclaration, definition::Namespace};

/// The platform entry point: the one declaration `tkp` invokes.
///
/// The declaration exposes three authoring namespaces: Compose resources,
/// this platform's observability resource, and the AWS resources used by the
/// optional DSQL configuration. Operational capabilities are integrated here
/// rather than exported wholesale by `tokeira-compose`.
///
/// Construction is pure: no filesystem, no network, no Docker. Connections
/// happen when the framework runs an operation, never when the platform is
/// declared.
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration {
        namespaces: vec![
            tokeira_compose::namespace(),
            observability::namespace(),
            Namespace {
                name: tokeira_aws::kinds::NAMESPACE,
                kinds: tokeira_aws::kinds::KINDS,
                defaults: None,
                decode: tokeira_aws::kinds::decode,
            },
        ],
        ops: Some(Box::new(tokeira_compose::ops::DockerOps)),
        execution: Box::new(tokeira_compose::execution::ComposeExecution),
        implementation: Arc::new(tokeira_compose::execution::ComposeIntegration),
    }
}

// How this entry point reaches an operator.
//
// Build-time, `tkp` is assembled from three parts: this platform, one
// definition frontend selected by format, and the framework. The platform
// is discovered through its catalog descriptor — Cargo metadata on this
// package (`[package.metadata.tokeira.platform]`, declaring `id` and
// `default-format`); the frontend through its own descriptor, which also
// names the definition's conventional relative path, co-locating
// the declared root in this package. The generated binary's `main` is the
// framework's macro, which hands both declarations, with the identity pair
// the binary is built as, to the framework:
//
//     tokeira_tkp::bound_provisioner_main! {
//         expected_platform: "compose",
//         platform: tokeira_compose_deployment::platform,
//         expected_format: "tkd",
//         frontend: tokeira_platform_definition::tkd::frontend,
//     }
//
// The framework marries platform and frontend; the platform never receives
// the frontend. This function is the whole of what the macro names here.
//
// Deployment-time, `tkr deployment create` stages the co-located definition
// and its companion content into the deployment directory as configuration
// data — never compiled into `tkp` — and records `{ platform, format, path }`
// in metadata.json. The bound `tkp` refuses a deployment whose recorded pair
// differs from the pair it was built as; that verification is the durable
// association between the external source and the engine interpreting it.
