//! Provider attributes and their precedence.
//!
//! A provider attribute is a fact a provider acts on that can be authored at
//! more than one level. The AWS region is the exemplar; its levels, from
//! most to least specific:
//!
//! 1. **resource-attached** — on the resource's own input
//!    (`DsqlCluster { region, .. }`);
//! 2. **deployment-level** — on the definition's configuration, inside the
//!    provider's namespace block (`Compose { aws: Aws { region }, .. }`,
//!    the deployed context materialized as authored, create-fixed
//!    attributes);
//! 3. **ambient** — the deployed context itself (for AWS, the SDK's own
//!    default resolution chain).
//!
//! Ownership is the whole design: **the precedence rule belongs to the
//! provider.** The definition authors values at whichever levels it has; the
//! framework transports declared attributes without interpreting them; the
//! provider resolves — and documents its rule where it implements it.
//! Nothing in the framework knows that `aws_region` means anything.

// ----------------------------------------------------------------------
// Declaration: a selection claims its NAMESPACE, and its deployment-level
// attributes live inside it.
// ----------------------------------------------------------------------
//
// Collisions are not checked away — they are unrepresentable: every
// provider's names live under its own namespace, which the selection
// already carries (`KindSet.provider`). The declaration surface grows by
// nothing:
//
//     pub struct KindSet {
//         /// The provider's namespace: scopes its kinds AND its
//         /// deployment-level attributes.
//         pub provider: &'static str,
//         pub entries: Vec<KindEntry>,
//     }
//
// **Attributes** — the definition authors a per-provider block on the
// configuration, named by the namespace:
//
//     struct Compose {
//         #[create]
//         storage: Storage,
//         #[create]
//         aws: Aws,                 // the aws namespace's attributes
//         ...
//     }
//     struct Aws { region: String }
//
// The framework extracts the top-level field named by each selection's
// namespace and delivers the whole block as data; the provider decodes its
// own block with its own shape. `aws.region` and `gcp.region` coexist by
// construction; names inside one block are the provider's private
// business. An absent block is simply absent — the ambient floor applies.
//
// **Kinds** — the vocabulary keys entries by (namespace, name), and
// definitions may reference kinds qualified by namespace. An unqualified
// reference stays valid sugar while exactly one wired provider exports the
// name — today's definitions unchanged — and a genuinely ambiguous
// unqualified reference is refused at its use site, naming the qualified
// candidates. The composition-time duplicate-name refusal dissolves: two
// providers exporting `Cluster` is no longer an error, only an unqualified
// reference to it is.
//
// **Both frontends carry this**, each in its own idiom:
//
//   - `.tkd` — qualified kind paths (`aws::DsqlCluster { .. }`), with the
//     bare name as the unique-case sugar;
//   - `.tkdp` — namespace modules in the synthesized facade
//     (`from tokeira.aws import DsqlCluster`), with `from tokeira import
//     DsqlCluster` as the unique-case sugar.
//
// The vocabulary's lookup contract changes accordingly, once, for both:
// resolution by qualified name, or by bare name yielding unique / ambiguous
// (naming the candidate namespaces) / unknown; enumeration yields
// (namespace, name) pairs so the tkdp facade can synthesize its modules.
// The attribute half costs the frontends nothing: namespace blocks are
// ordinary configuration fields, already evaluated — extraction is the
// framework's, after evaluation.

// ----------------------------------------------------------------------
// Transport: the framework extracts declared names; two consumers.
// ----------------------------------------------------------------------
//
// After evaluation the engine holds the configuration as data. For each
// declared attribute name it looks up the top-level field of that name on
// the evaluated configuration value; a scalar value is captured, an absent
// field is simply absent (the ambient floor applies). The captured
// attributes travel to the provider at the two moments it acts:
//
// 1. **Realization** — `PlacementContext` carries each namespace's block:
//
//        pub struct PlacementContext {
//            ...,
//            /// Deployment-level provider attribute blocks, by namespace,
//            /// as evaluated data the owning provider decodes.
//            pub provider_attributes: BTreeMap<String, LocatedValue>,
//        }
//
//    A kind resolves its own precedence at input validation and
//    realization: the resource-attached value when authored, else its
//    namespace's block from the placement, else the kind's rule for the
//    ambient case. A kind adopts fallback per attribute, on purpose —
//    `DsqlCluster.region` staying required is a legitimate rule; making it
//    optional-with-fallback is that kind's own evolution, never a
//    framework-imposed change.
//
// 2. **Registration** — the engine hands the provider its own block when
//    preparing the context, so the extension the resources read
//    (`AwsClients`) is built from the deployment-level attribute — the
//    registration mechanism intact, its missing input re-materialized as
//    an authored fact:
//
//        ProviderExecution::install(&self, deployment, attributes, ctx)
//            -> Result<()>;
//
//    where `attributes` is the provider's namespace block (absent when the
//    definition authors none). (`probe` likewise receives it when a
//    provider's reachability depends on an attribute; AWS's does not.)

// ----------------------------------------------------------------------
// The rule, written down where it is implemented: the provider.
// ----------------------------------------------------------------------
//
// In `tokeira-aws`, beside the kinds that use it:
//
//     /// The AWS region precedence rule.
//     ///
//     /// Clients (the `AwsClients` extension) are built from the
//     /// deployment-level `aws_region`, falling back to the SDK's ambient
//     /// resolution when the deployment declares none. A resource-attached
//     /// region governs the resource's own semantics — identity,
//     /// validation, manifest — and takes precedence over the deployment
//     /// level wherever the resource speaks for itself. A resource-attached
//     /// region that DIVERGES from the deployment-level region is refused
//     /// at input validation until per-region clients exist: acting in one
//     /// region while recording another is a wrong answer, and refusing is
//     /// the honest one.
//
// The divergence stance is part of the rule and belongs to the provider;
// the conservative refusal above is AWS's opening rule, revisable by the
// provider alone when multi-region execution is real. Namespace blocks are
// structured by construction — the provider's own decode gives its
// attributes their shape, scalar or otherwise.
