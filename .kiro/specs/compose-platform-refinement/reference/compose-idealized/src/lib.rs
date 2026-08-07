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
//! Compose is four things:
//!
//! 1. **The definition** — `definition.tkd`, the program that describes the
//!    deployment's infrastructure and services in terms of the kinds wired
//!    below, and authors the configuration shape (`struct Compose` and
//!    friends) exactly once. This crate defines no config types and
//!    validates no config values: each kind validates its own input where
//!    it is authored, and the definition's shapes make invalid states
//!    unrepresentable. Companion content travels with the definition —
//!    `observability/` carries the dashboards, alert rules, and backend
//!    config templates as desired-source companions: recorded with the
//!    definition, resolved against the definition's own directory, and
//!    digested into the content identity that fences every consumer, so a
//!    dashboard edit is a visible, plannable change to this deployment.
//!    The substrate's kind library renders that content; which content,
//!    only this platform knows.
//! 2. **The substrate** — `tokeira_compose::substrate()`. Its export brings
//!    everything "running on Compose" means: the kind library (`Service`,
//!    `LocalStateDir`, `ServerConfig`, `ObservabilityConfiguration`), the
//!    Docker mechanics behind those kinds, the runtime reads (logs, port
//!    mappings) that `tkp` surfaces as verbs by presence, and the
//!    execution-extension constructor the framework calls at operation
//!    start. Being the compose platform is using the compose substrate; its
//!    vocabulary needs no separate wiring act.
//! 3. **The auxiliary vocabulary** — AWS kinds, selected from
//!    `tokeira_aws::kinds` at the entry point.
//! 4. **The entry point** — [`platform`]: the one exported declaration
//!    `tkp` invokes. There is no binary here; `tkp` is the binary and
//!    includes the framework for invoking the platform.
//!
//! The contract types the entry point speaks (`PlatformDeclaration` and
//! what it carries) are `tkp`'s: the framework defines what it consumes.

use tokeira_provisioner_cli::PlatformDeclaration;

/// The platform entry point: the one declaration `tkp` invokes.
///
/// The substrate brings its vocabulary, runtime reads, and execution
/// extensions; the auxiliary kinds are selected explicitly. The authoring
/// vocabulary is exactly the union of the two — a definition naming any
/// other kind fails at `definition check` with an unknown-kind error located
/// at the authoring site, and two providers exporting the same kind name
/// fail composition, naming both.
///
/// Construction is pure: no filesystem, no network, no Docker. Connections
/// happen when the framework runs an operation, never when the platform is
/// declared.
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration::on(tokeira_compose::substrate())
        // Everything tokeira-aws exports. Selecting only the required kinds
        // (`kinds::select(["DsqlCluster", "DynamoDbTable"])`) is the tighter
        // alternative: a vocabulary that states its intent exactly and
        // grows only on purpose, at the cost of an edit here when the
        // definition adopts a new AWS kind.
        .kinds(tokeira_aws::kinds::all())
}

// How this entry point reaches an operator.
//
// Build-time, `tkp` is assembled from three parts: this platform, one
// definition frontend selected by format, and the framework. The platform
// is discovered through its catalog descriptor — Cargo metadata on this
// package (`[package.metadata.tokeira.platform]`, declaring `id` and
// `default-format`); the frontend through its own descriptor, which also
// names the definition's conventional relative path, co-locating
// `definition.tkd` in this package. The generated binary's `main` is the
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
