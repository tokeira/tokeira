//! What a platform is to the framework: one declaration.
//!
//! A platform describes its infrastructure and services; the framework owns
//! change management. This module is the boundary between them — the value a
//! platform's entry point returns ([`PlatformDeclaration`]) and everything it
//! carries: the export of the provider the platform runs on, explicit
//! auxiliary kind selections, and the composed authoring vocabulary.
//!
//! Wiring is by construction: a provider's kinds are authorable because its
//! [`KindSet`] is in the declaration, and its mechanics run because they
//! arrived in the same export. There is no global kind inventory, no
//! "known but unwired" state, and no separate registration of provider
//! clients — each selection carries its own extension constructor
//! ([`InfraConstructor`]), run by the deployment's registration seam.

use std::{collections::BTreeMap, fmt, path::PathBuf, pin::Pin, sync::Arc};

use futures_util::Stream;

use crate::{author::LocatedValue, error::KindError, kind::ProviderKind};

/// Live host/container port mapping shape shared with the operator surface.
pub use tokeira_orchestrator::PortMapping;

/// Everything the framework needs to operate one platform.
///
/// Constructed by the platform's entry point; construction is pure — no
/// filesystem, no network, no provider clients. Connections happen when the
/// framework runs an operation, never when the platform is declared.
pub struct PlatformDeclaration {
    /// The provider the platform runs on.
    pub provider: ProviderExport,
    /// Explicit auxiliary kind selections, in declaration order.
    pub auxiliary: Vec<KindSet>,
}

impl PlatformDeclaration {
    /// Declare the provider the platform runs on. Its kind library, runtime
    /// reads, and execution extensions arrive with it; no separate wiring
    /// act exists.
    pub fn on(provider: ProviderExport) -> Self {
        Self {
            provider,
            auxiliary: Vec::new(),
        }
    }

    /// Add an auxiliary kind selection. May be called once per provider.
    #[must_use]
    pub fn kinds(mut self, selection: KindSet) -> Self {
        self.auxiliary.push(selection);
        self
    }

    /// Compose the authoring vocabulary: the primary provider's kinds united
    /// with every auxiliary selection. Fails if two providers export the
    /// same kind name — name-routed decoding is only sound when names are
    /// unique within the declared set, so the collision is refused at
    /// composition, naming both providers.
    pub fn vocabulary(&self) -> Result<Vocabulary, CompositionError> {
        Vocabulary::of(
            std::iter::once(&self.provider.kinds)
                .chain(self.auxiliary.iter())
                .cloned()
                .collect(),
        )
    }
}

impl fmt::Debug for PlatformDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlatformDeclaration")
            .field("provider", &self.provider)
            .field("auxiliary", &self.auxiliary)
            .finish()
    }
}

/// One provider's complete export: what running on it means.
///
/// The kind library makes the provider's resources authorable; the ops
/// surface (when present) backs the framework's live verbs; the execution
/// seam answers reachability; the infra constructor (when present) is the
/// registration ingredient the deployment runs at operation start.
pub struct ProviderExport {
    /// The provider's complete kind library.
    pub kinds: KindSet,
    /// The provider's ops surface over a running deployment, or `None` —
    /// the framework surfaces the corresponding verbs by presence.
    pub ops: Option<Box<dyn Ops>>,
    /// Answers whether the provider's endpoint is reachable for a
    /// deployment.
    pub execution: Box<dyn ProviderExecution>,
    /// The provider's infra-phase extension constructor, when it has
    /// context handles to register.
    pub infra: Option<Arc<dyn InfraConstructor>>,
}

impl fmt::Debug for ProviderExport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderExport")
            .field("kinds", &self.kinds)
            .field("ops", &self.ops.is_some())
            .field("infra", &self.infra.is_some())
            .finish_non_exhaustive()
    }
}

/// An authorable kind: the contract a kind type implements so it can be
/// selected by type at a platform entry point.
///
/// The name, defaults, and decoder all derive from the type itself — the
/// entry point writes `kind::<DsqlCluster>()`, a typo is a compile error,
/// and no string routing exists anywhere.
pub trait AuthorableKind: ProviderKind + serde::de::DeserializeOwned + Sized + 'static {
    /// Stable author-visible kind name.
    const NAME: &'static str;

    /// Provider-owned defaults for `<Kind>::EMPTY`; `None` when the kind
    /// declares no defaults.
    fn defaults() -> Option<LocatedValue> {
        None
    }
}

/// One selected kind: name, defaults, and decoder, derived from the kind
/// type by [`kind`]. Plain data a platform entry point composes into sets.
#[derive(Debug, Clone)]
pub struct KindEntry {
    /// Stable author-visible kind name.
    pub name: &'static str,
    /// Provider-owned defaults for `<Kind>::EMPTY`.
    pub defaults: fn() -> Option<LocatedValue>,
    /// Decode this kind's authored input into a realizable provider kind.
    pub decode: fn(LocatedValue) -> Result<Box<dyn ProviderKind>, KindError>,
}

/// The typed selection unit: the entry for one kind type.
pub fn kind<K: AuthorableKind>() -> KindEntry {
    KindEntry {
        name: K::NAME,
        defaults: K::defaults,
        decode: |value| {
            let range = value.range;
            crate::author::from_located_value::<K>(value)
                .map(|kind| Box::new(kind) as Box<dyn ProviderKind>)
                .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
        },
    }
}

/// One provider's selected kind entries, carried by value.
#[derive(Debug, Clone)]
pub struct KindSet {
    /// Operator-facing provider identifier, used in composition errors.
    pub provider: &'static str,
    /// The selected entries, in declaration order.
    pub entries: Vec<KindEntry>,
    /// The selection's infra-phase extension constructor, when its kinds
    /// read shared context handles.
    pub infra: Option<Arc<dyn InfraConstructor>>,
}

impl KindSet {
    /// A provider-labelled selection of typed kind entries.
    pub fn new(provider: &'static str, entries: Vec<KindEntry>) -> Self {
        Self {
            provider,
            entries,
            infra: None,
        }
    }

    /// Attach the selection's infra-phase extension constructor.
    #[must_use]
    pub fn infra(mut self, constructor: impl InfraConstructor + 'static) -> Self {
        self.infra = Some(Arc::new(constructor));
        self
    }
}

/// The composed authoring vocabulary: every kind a definition may name.
#[derive(Debug, Clone)]
pub struct Vocabulary {
    sets: Vec<KindSet>,
}

impl Vocabulary {
    /// Compose a vocabulary directly from kind sets, refusing duplicate kind
    /// names across them. [`PlatformDeclaration::vocabulary`] is this over
    /// the declaration's sets; frontends and tests compose theirs directly.
    pub fn of(sets: Vec<KindSet>) -> Result<Self, CompositionError> {
        let mut owners: BTreeMap<&'static str, &'static str> = BTreeMap::new();
        for set in &sets {
            for entry in &set.entries {
                if let Some(first) = owners.insert(entry.name, set.provider) {
                    return Err(CompositionError::DuplicateKindName {
                        name: entry.name,
                        first,
                        second: set.provider,
                    });
                }
            }
        }
        Ok(Self { sets })
    }

    /// Complete author-visible kind names, in declaration order.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.sets
            .iter()
            .flat_map(|set| set.entries.iter().map(|entry| entry.name))
    }

    /// Whether an author-visible name belongs to the vocabulary.
    pub fn contains(&self, name: &str) -> bool {
        self.entry_of(name).is_some()
    }

    /// Provider-owned defaults for `<Kind>::EMPTY`.
    pub fn defaults(&self, name: &str) -> Option<LocatedValue> {
        self.entry_of(name).and_then(|entry| (entry.defaults)())
    }

    /// Decode one named kind. An unknown name is a located authoring error —
    /// there is no other category of refusal here.
    pub fn decode(
        &self,
        name: &str,
        value: LocatedValue,
    ) -> Result<Box<dyn ProviderKind>, KindError> {
        match self.entry_of(name) {
            Some(entry) => (entry.decode)(value),
            None => Err(KindError::new(format!("unknown kind `{name}`"))),
        }
    }

    fn entry_of(&self, name: &str) -> Option<&KindEntry> {
        self.sets
            .iter()
            .flat_map(|set| set.entries.iter())
            .find(|entry| entry.name == name)
    }
}

/// A declaration that cannot compose.
#[derive(Debug, thiserror::Error)]
pub enum CompositionError {
    /// Two providers export the same author-visible kind name; name-routed
    /// decoding would be ambiguous.
    #[error(
        "kind name `{name}` is exported by both `{first}` and `{second}`; \
         kind names must be unique across the declared providers"
    )]
    DuplicateKindName {
        /// The colliding author-visible name.
        name: &'static str,
        /// Provider declared earlier.
        first: &'static str,
        /// Provider declared later.
        second: &'static str,
    },
}

/// Deployment coordinates the framework supplies to provider behaviour:
/// identity, never state.
#[derive(Debug, Clone)]
pub struct DeploymentRef {
    /// Deployment name — the project scope provider resources carry.
    pub name: String,
    /// Deployment directory, for providers whose artifacts are path-scoped.
    pub dir: PathBuf,
}

/// Incremental log output for one service.
pub type LogStream = Pin<Box<dyn Stream<Item = anyhow::Result<String>> + Send>>;

/// A provider's ops surface over a running deployment: the live operator
/// questions answered from the substrate itself.
///
/// Implemented by providers that have one; the framework mounts the
/// corresponding operator verbs by presence and passes the operator's
/// service name through — an implementation answers for the services it
/// finds on the substrate, and an unknown name surfaces as the provider's
/// own error.
#[async_trait::async_trait]
pub trait Ops: Send + Sync + fmt::Debug {
    /// Open incremental logs for one declared service.
    async fn log_stream(
        &self,
        deployment: &DeploymentRef,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> anyhow::Result<LogStream>;

    /// Resolve the live host/container port mappings for one declared
    /// service.
    async fn port_mappings(
        &self,
        deployment: &DeploymentRef,
        service: &str,
    ) -> anyhow::Result<Vec<PortMapping>>;

    /// Change workload capacity (`<dim>=<n>` specs), returning the change
    /// count. Required, deliberately undefaulted: an ops surface answers
    /// every one of its verbs in its own words — a provider without a scale
    /// dimension states its own refusal as the error.
    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> anyhow::Result<usize>;
}

/// The provider's execution seam, invoked by the framework: reachability
/// stated as data. Platforms never call it. Registration is not this
/// seam's question — each selection's [`InfraConstructor`] carries it.
#[async_trait::async_trait]
pub trait ProviderExecution: Send + Sync + fmt::Debug {
    /// Is the provider's substrate reachable for this deployment?
    ///
    /// `Ok(None)` — reachable. `Ok(Some(issue))` — the degradable answer,
    /// stated as data: the engine's plan blocks (it plans nothing and the
    /// issue is the outcome's only content — describing the live substrate
    /// is a precondition of comparing against the record); apply and
    /// destroy refuse on it. `Err` — a non-provider
    /// failure.
    ///
    /// A point-in-time answer, not a guarantee: the substrate can go down
    /// the moment after a passing probe. Failures after a passing probe
    /// surface through the operation's own error path, carrying the same
    /// platform-issue evidence — nothing may be "optimized" on the
    /// assumption that a probe protects the operation behind it.
    async fn probe(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>>;
}

/// A selection's infra-phase extension constructor: the registration
/// ingredient the deployment runs inside `register_infra_extensions`.
///
/// Registration happens through the deployment's unchanged seam and nowhere
/// else: the deployment runs each declared selection's constructor, the
/// constructor puts handles into the context, and resources read them via
/// `ctx.extension::<T>()` at the mechanics moment. `attributes` is the
/// selection's namespace block from the evaluated configuration (e.g. the
/// `aws` block), transported by the framework and interpreted only here —
/// the provider owns its precedence rule over it.
///
/// Failures are errors — real ones, and equally the unreachable class
/// arriving after a passing probe; the provider keeps that class typed at
/// the error root so it renders as the same platform issue, never a
/// blended message.
#[async_trait::async_trait]
pub trait InfraConstructor: Send + Sync + fmt::Debug {
    /// Register this selection's extensions on the provision context.
    async fn construct(
        &self,
        deployment: &DeploymentRef,
        attributes: Option<&serde_json::Value>,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()>;
}
