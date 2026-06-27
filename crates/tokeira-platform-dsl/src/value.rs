//! Runtime values and the neutral composition IR produced by evaluation.
//!
//! Evaluation (the "execute" phase) is a pure function of `(Program,
//! RuntimeContext)` → [`Composition`] (design §Overview). The result is
//! **provider-neutral**: this crate never depends on `tokeira-iac`. A platform
//! crate maps the [`Composition`] onto concrete engine resources/services/images
//! via its kind library. Keeping the IR neutral is what lets the compiler stay
//! generic across platforms.
//!
//! Some leaves are **deferred**: an [`OutputRef`] is not resolved here — the
//! engine resolves it at apply from the referenced resource's provisioned state
//! (Requirement 15). So a [`Composition`] is deterministic in its *structure*
//! and its set of references given the [`RuntimeContext`], even though some
//! concrete values arrive only at apply (Property 2).

use std::collections::BTreeMap;

/// The closed, typed runtime context the host (`tkp`) injects at execution.
///
/// This is the *only* external data the program may read; there is no OS
/// environment, network, clock, or arbitrary-filesystem access (Requirement
/// 12.1). It is ambient and never retained — rollback restores the definition,
/// not the context. Operator-declared providers (the `env` provider) extend
/// this in a later slice; the implicit fields below are what the platforms
/// actually consume today.
#[derive(Debug, Clone)]
pub struct RuntimeContext {
    /// The deployment's on-disk root (volume/config/state paths derive from it).
    pub deployment_dir: String,
    /// The host user's home directory (compose's `~/.aws` mount).
    pub home: String,
    /// The deployment region.
    pub region: String,
}

/// A fully-evaluated DSL value, or a deferred reference resolved at apply.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A string.
    Str(String),
    /// An integer.
    Int(i64),
    /// A boolean.
    Bool(bool),
    /// A filesystem path (string-built via `/`, never touched on disk here).
    Path(String),
    /// An ordered list.
    List(Vec<Value>),
    /// A record (sorted keys for deterministic output, Property 2).
    Record(BTreeMap<String, Value>),
    /// A sum-type value: a variant name with an optional payload.
    Variant {
        /// The variant name (e.g. `Dsql`).
        name: String,
        /// The variant payload, if the variant carries one.
        payload: Option<Box<Value>>,
    },
    /// A deferred reference to another resource's provisioned output
    /// (Requirement 15); resolved by the engine at apply, not here.
    Output(OutputRef),
    /// A secret-tainted value; never rendered into a diagnostic (Property 16).
    Secret(Box<Value>),
    /// An absent optional value (e.g. dropped by `present_only`).
    Absent,
}

/// A deferred reference to a resource output: `<resource>.<output>` or
/// `<module>.<resource>.<output>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRef {
    /// The owning module, for a qualified `<module>.<resource>.<output>` ref.
    pub module: Option<String>,
    /// The referenced resource/service id.
    pub resource: String,
    /// The referenced output name.
    pub output: String,
}

/// Whether a lowered item is an infra resource or a service.
///
/// A `ComposeService` declaration is a [`ItemRole::Service`]; the platform crate
/// lowers it into **both** an infra resource and a deploy-engine service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemRole {
    /// An infra resource.
    Resource,
    /// A service (→ infra resource + deploy-engine service downstream).
    Service,
}

/// The neutral composition: the evaluation result the platform crate consumes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Composition {
    /// Present modules (those whose `when` guard held), in declaration order.
    pub modules: Vec<LoweredModule>,
    /// Declared images.
    pub images: Vec<LoweredImage>,
    /// Required namespaces.
    pub namespaces: Vec<String>,
    /// Writeback targets whose `when` guard held.
    pub writeback: Vec<LoweredWriteback>,
}

/// A present module with its resolved items and dependency edges.
#[derive(Debug, Clone, PartialEq)]
pub struct LoweredModule {
    /// Module name (ownership for its items).
    pub name: String,
    /// Module-level dependency edges (other module names).
    pub depends_on: Vec<String>,
    /// Resources and services contributed by the module.
    pub items: Vec<LoweredItem>,
}

/// A resolved resource or service instance.
#[derive(Debug, Clone, PartialEq)]
pub struct LoweredItem {
    /// Stable id (the declaration name; becomes a `ResourceId`).
    pub id: String,
    /// The kind name (resolved against the kind library downstream).
    pub kind: String,
    /// Whether this is a resource or a service.
    pub role: ItemRole,
    /// Resolved field values (the `depends_on` field is lifted out into
    /// [`LoweredItem::depends_on`], not kept here).
    pub fields: BTreeMap<String, Value>,
    /// Dependency edges (other resource/service ids) from the `depends_on`
    /// field.
    pub depends_on: Vec<String>,
}

/// A resolved image declaration.
#[derive(Debug, Clone, PartialEq)]
pub struct LoweredImage {
    /// Image name.
    pub name: String,
    /// Image kind (`Build` | `Mirror`).
    pub kind: String,
    /// Resolved field values.
    pub fields: BTreeMap<String, Value>,
}

/// A resolved writeback target: a dotted config key sourced from a resource
/// output that `tkp` resolves and writes after apply.
#[derive(Debug, Clone, PartialEq)]
pub struct LoweredWriteback {
    /// Dotted config key (e.g. `infrastructure.dsql.endpoint`).
    pub key: String,
    /// The output reference supplying the value.
    pub source: OutputRef,
}
