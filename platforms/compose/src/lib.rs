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
//! 3. **The provider** — `tokeira_compose::provider()`. Its export brings
//!    everything "running on Compose" means: the kind library (`Service`,
//!    `LocalStateDir`, `ServerConfig`), the Docker mechanics behind those
//!    kinds, the runtime reads (logs, port mappings) that `tkp` surfaces as
//!    verbs by presence, the execution installer the framework calls at
//!    operation start, and the workload export — the projection that
//!    recognizes the deployment's services among realized entries and the
//!    applier that deploys their manifests, splitting the service plane
//!    from the substrate. Being the compose platform is using the compose
//!    provider; its vocabulary needs no separate wiring act.
//! 4. **The platform's own kinds** — [`observability`]: the
//!    `ObservabilityConfiguration` bundle that renders the companion
//!    content. Tokeira-opinionated description and its machinery, owned
//!    here and contributed to the vocabulary as the platform's own
//!    selection; the provider keeps only the fencing contract
//!    (`config_content_resource_id`) its `Service` consumers key on.
//!    Auxiliary vocabulary joins the same way — AWS kinds, selected from
//!    `tokeira_aws::kinds`.
//! 5. **The entry point** — [`platform`]: the one exported declaration
//!    `tkp` invokes. There is no binary here; `tkp` is the binary and
//!    includes the framework for invoking the platform.
//!
//! The contract types the entry point speaks (`PlatformDeclaration` and
//! what it carries) are `tkp`'s: the framework defines what it consumes.

pub mod observability;

use tokeira_aws::kinds::{DsqlCluster, DynamoDbTable};
use tokeira_platform::declaration::{PlatformDeclaration, kind};

/// The platform entry point: the one declaration `tkp` invokes.
///
/// The provider brings its vocabulary, runtime reads, and execution
/// installer; the platform's own observability kind and the auxiliary AWS
/// kinds are selected explicitly. The authoring vocabulary is exactly the
/// union of the three — a definition naming any other kind fails at
/// `definition check` with an unknown-kind error located at the authoring
/// site, and two selections exporting the same kind name fail composition,
/// naming both.
///
/// Construction is pure: no filesystem, no network, no Docker. Connections
/// happen when the framework runs an operation, never when the platform is
/// declared.
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration::on(tokeira_compose::provider())
        // The platform's own kind: the observability configuration bundle
        // rendering the companion content shipped beside the definitions.
        .kinds(observability::kind_set())
        // Exactly the AWS kinds the definitions require, selected by type:
        // the vocabulary states its intent and grows only on purpose — a
        // definition adopting a new AWS kind names its type here in the
        // same change, and a typo is a compile error. The provider-tracking
        // alternative (`kinds::all()`) would widen this platform's
        // vocabulary with every kind the AWS export gains.
        .kinds(tokeira_aws::kinds::select(vec![
            kind::<DsqlCluster>(),
            kind::<DynamoDbTable>(),
        ]))
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
//     tokeira_provisioner_cli::bound_provisioner_main! {
//         expected_platform: "compose",
//         platform: tokeira_compose_deployment::platform,
//         expected_format: "tkd",
//         frontend: tokeira_tkd::frontend,
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
