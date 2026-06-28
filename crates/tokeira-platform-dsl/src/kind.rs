//! The kind library: typed schemas for the compiled resource/service/image
//! kinds a program may instantiate.
//!
//! A program references kinds **by name** (Requirement 2); the running `tkp`
//! resolves them within its single kind-library version (Requirement 9). The
//! compiler consults a [`KindLibrary`] to check that a referenced kind exists,
//! that its fields are in schema, and (later) that an output reference names a
//! real output (Requirement 15).
//!
//! This module owns only the *schema* surface the compiler needs (names,
//! fields, outputs). The executable lifecycle (`create`/`update`/…) and the
//! `lower` step live with the concrete kinds in the platform crates; the schema
//! is what the language is type-checked against. Behaviour is never expressed in
//! the DSL — only composition over these kinds.

use std::collections::HashMap;

/// Which composition plane a kind contributes to when lowered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KindCategory {
    /// An infra `Resource` (lowered into the `InfraComposition`).
    Resource,
    /// A service: lowered into **both** an infra `Resource` and a deploy-engine
    /// `Service` (mirroring today's `OwnedComposeResource` + `ComposeWorkload`).
    Service,
    /// An image (lowered into the deploy-engine image set).
    Image,
}

/// The static type of a kind field's value.
///
/// The compiler infers an expression's type (where it confidently can) and
/// checks it against the field's declared `FieldType` (Requirement 3.2 / task
/// 4.3). [`FieldType::Any`] is the soundness escape hatch: an expression whose
/// type cannot be inferred (a sum-payload field access, a deferred output
/// reference) is never rejected, so the checker produces no false positives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// A string.
    Str,
    /// A non-negative integer.
    Int,
    /// A TCP port; accepts an integer value (a `Port` is an `Int` 0..=65535).
    Port,
    /// A boolean.
    Bool,
    /// A filesystem path; accepts a `Path` or a `Str`.
    Path,
    /// A homogeneous list.
    List(Box<FieldType>),
    /// A string-keyed record with homogeneous values (e.g. an env map).
    Record(Box<FieldType>),
    /// A secret-bearing field — the only sink a `Secret<T>` value may flow into
    /// (Requirement 12.3 / task 4.4).
    Secret(Box<FieldType>),
    /// No static constraint (struct-shaped fields like `healthcheck`, or values
    /// only known after evaluation). Never the source of a type diagnostic.
    Any,
}

/// A cross-field or value-range constraint a kind imposes, enforced before
/// lowering when the relevant values are statically known (Requirement 5 / task
/// 4.5).
///
/// These are the DSL analogs of the imperative checks in `EcsConfig::validate`
/// (canonical ports, cpu/memory pairing, capacity ranges). The compose kind
/// library uses few; the rich numeric set arrives with the ECS kinds (task 13).
/// A constraint over a value that is not statically known (an input reference
/// whose default is non-literal, an output reference) is skipped, not failed —
/// the same soundness rule as field typing.
#[derive(Debug, Clone)]
pub enum Constraint {
    /// An inclusive integer range on a field (e.g. a port is `1..=65535`).
    Range {
        /// The field the range applies to.
        field: &'static str,
        /// Inclusive lower bound.
        min: i64,
        /// Inclusive upper bound.
        max: i64,
    },
    /// Two fields must both be present or both absent (e.g. ECS `cpu`/`memory`).
    PairedPresence {
        /// The first field.
        first: &'static str,
        /// The second field.
        second: &'static str,
    },
}

/// One declarable field of a kind.
#[derive(Debug, Clone)]
pub struct FieldSpec {
    /// Field name as written in the DSL (`image`, `ports`, `cluster_arn`, …).
    pub name: &'static str,
    /// Whether the field must be supplied; a missing required field is a
    /// diagnostic (Requirement 2.3).
    pub required: bool,
    /// The field's static type, checked against the supplied value's inferred
    /// type (task 4.3).
    pub ty: FieldType,
}

impl FieldSpec {
    /// A required field of type `ty`.
    pub const fn required(name: &'static str, ty: FieldType) -> Self {
        Self {
            name,
            required: true,
            ty,
        }
    }

    /// An optional field of type `ty`.
    pub const fn optional(name: &'static str, ty: FieldType) -> Self {
        Self {
            name,
            required: false,
            ty,
        }
    }
}

/// The typed schema of one kind: its category, declarable fields, the outputs
/// other resources may reference (Requirement 15), and any cross-field/range
/// constraints (task 4.5).
#[derive(Debug, Clone)]
pub struct KindSchema {
    /// Kind name as referenced in the DSL (`ComposeService`, `DsqlCluster`, …).
    pub name: &'static str,
    /// The plane this kind lowers into.
    pub category: KindCategory,
    /// Declarable fields.
    pub fields: Vec<FieldSpec>,
    /// Output names this kind exposes for `<resource>.<output>` references.
    pub outputs: Vec<&'static str>,
    /// Cross-field / value-range constraints enforced before lowering.
    pub constraints: Vec<Constraint>,
}

impl KindSchema {
    /// Whether `field` is a declarable field of this kind.
    pub fn has_field(&self, field: &str) -> bool {
        self.fields.iter().any(|spec| spec.name == field)
    }

    /// The declared type of `field`, if it exists.
    pub fn field_type(&self, field: &str) -> Option<&FieldType> {
        self.fields
            .iter()
            .find(|spec| spec.name == field)
            .map(|spec| &spec.ty)
    }

    /// Whether `output` is an output this kind exposes.
    pub fn has_output(&self, output: &str) -> bool {
        self.outputs.contains(&output)
    }

    /// The names of the kind's required fields.
    pub fn required_fields(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.fields
            .iter()
            .filter(|spec| spec.required)
            .map(|spec| spec.name)
    }
}

/// The set of kinds a given `tkp` provides, keyed by name.
///
/// Resolved at compile time; an unknown kind reference is a diagnostic
/// (Property 3). The catalogue here is fixed by the binary's
/// `(language, kind-library)` version (Requirement 9.3).
#[derive(Debug, Clone, Default)]
pub struct KindLibrary {
    kinds: HashMap<&'static str, KindSchema>,
}

impl KindLibrary {
    /// An empty library.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a kind schema, returning the library for chaining.
    pub fn with(mut self, schema: KindSchema) -> Self {
        self.kinds.insert(schema.name, schema);
        self
    }

    /// Look up a kind by name.
    pub fn get(&self, name: &str) -> Option<&KindSchema> {
        self.kinds.get(name)
    }

    /// Whether a kind of `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.kinds.contains_key(name)
    }

    /// The compose kind library used by tests and (initially) by the
    /// `compose-dsl` platform: schemas mirroring the existing compose resource
    /// kinds. The authoritative library lives with the platform crate; this is
    /// the in-crate reference used to exercise the compiler.
    pub fn compose() -> Self {
        use FieldType::{Bool, Int, Port, Str};
        use KindCategory::{Image, Resource, Service};
        Self::new()
            .with(KindSchema {
                name: "LocalStateDir",
                category: Resource,
                fields: vec![],
                outputs: vec!["path"],
                constraints: vec![],
            })
            .with(KindSchema {
                name: "DsqlCluster",
                category: Resource,
                fields: vec![
                    FieldSpec::required("mode", Str),
                    FieldSpec::required("region", Str),
                    FieldSpec::optional("endpoint", Str),
                    FieldSpec::optional("arn", Str),
                ],
                outputs: vec!["cluster_arn", "cluster_endpoint"],
                constraints: vec![],
            })
            .with(KindSchema {
                name: "DynamoDbTable",
                category: Resource,
                fields: vec![
                    FieldSpec::required("hash_key", Str),
                    FieldSpec::optional("ttl", Str),
                ],
                outputs: vec!["table_name"],
                constraints: vec![],
            })
            .with(KindSchema {
                name: "ObservabilityConfigFiles",
                category: Resource,
                fields: vec![
                    FieldSpec::required("metrics_target_host", Str),
                    FieldSpec::required("metrics_target_port", Port),
                    FieldSpec::optional("cluster", Str),
                    FieldSpec::optional("deployment", Str),
                ],
                outputs: vec![],
                // A scrape target port is a TCP port; reject a statically-known
                // out-of-range value before lowering (task 4.5).
                constraints: vec![Constraint::Range {
                    field: "metrics_target_port",
                    min: 1,
                    max: 65535,
                }],
            })
            .with(KindSchema {
                name: "ComposeService",
                category: Service,
                fields: vec![
                    FieldSpec::required("image", Str),
                    FieldSpec::optional("replicas", Int),
                    FieldSpec::optional("ports", FieldType::List(Box::new(Str))),
                    FieldSpec::optional("volumes", FieldType::List(Box::new(Str))),
                    FieldSpec::optional("env", FieldType::Record(Box::new(Str))),
                    FieldSpec::optional("command", FieldType::List(Box::new(Str))),
                    // `depends_on` carries bare names (not values); it is lifted
                    // out before lowering, so it is left untyped here.
                    FieldSpec::optional("depends_on", FieldType::Any),
                    FieldSpec::optional("aws_auth", Bool),
                    FieldSpec::optional("healthcheck", FieldType::Any),
                ],
                outputs: vec![],
                constraints: vec![],
            })
            .with(KindSchema {
                name: "Build",
                category: Image,
                fields: vec![FieldSpec::required("repository", Str)],
                outputs: vec![],
                constraints: vec![],
            })
            .with(KindSchema {
                name: "Mirror",
                category: Image,
                fields: vec![
                    FieldSpec::required("repository", Str),
                    FieldSpec::required("upstream", Str),
                ],
                outputs: vec![],
                constraints: vec![],
            })
    }
}
