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

/// One declarable field of a kind.
#[derive(Debug, Clone)]
pub struct FieldSpec {
    /// Field name as written in the DSL (`image`, `ports`, `cluster_arn`, …).
    pub name: &'static str,
    /// Whether the field must be supplied; a missing required field is a
    /// diagnostic (Requirement 2.3).
    pub required: bool,
}

impl FieldSpec {
    /// A required field.
    pub const fn required(name: &'static str) -> Self {
        Self {
            name,
            required: true,
        }
    }

    /// An optional field.
    pub const fn optional(name: &'static str) -> Self {
        Self {
            name,
            required: false,
        }
    }
}

/// The typed schema of one kind: its category, declarable fields, and the
/// outputs other resources may reference (Requirement 15).
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
}

impl KindSchema {
    /// Whether `field` is a declarable field of this kind.
    pub fn has_field(&self, field: &str) -> bool {
        self.fields.iter().any(|spec| spec.name == field)
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
        use KindCategory::{Image, Resource, Service};
        Self::new()
            .with(KindSchema {
                name: "LocalStateDir",
                category: Resource,
                fields: vec![],
                outputs: vec!["path"],
            })
            .with(KindSchema {
                name: "DsqlCluster",
                category: Resource,
                fields: vec![
                    FieldSpec::required("mode"),
                    FieldSpec::required("region"),
                    FieldSpec::optional("endpoint"),
                    FieldSpec::optional("arn"),
                ],
                outputs: vec!["cluster_arn", "cluster_endpoint"],
            })
            .with(KindSchema {
                name: "DynamoDbTable",
                category: Resource,
                fields: vec![FieldSpec::required("hash_key"), FieldSpec::optional("ttl")],
                outputs: vec!["table_name"],
            })
            .with(KindSchema {
                name: "ObservabilityConfigFiles",
                category: Resource,
                fields: vec![
                    FieldSpec::required("metrics_target_host"),
                    FieldSpec::required("metrics_target_port"),
                    FieldSpec::optional("cluster"),
                    FieldSpec::optional("deployment"),
                ],
                outputs: vec![],
            })
            .with(KindSchema {
                name: "ComposeService",
                category: Service,
                fields: vec![
                    FieldSpec::required("image"),
                    FieldSpec::optional("replicas"),
                    FieldSpec::optional("ports"),
                    FieldSpec::optional("volumes"),
                    FieldSpec::optional("env"),
                    FieldSpec::optional("command"),
                    FieldSpec::optional("depends_on"),
                    FieldSpec::optional("aws_auth"),
                    FieldSpec::optional("healthcheck"),
                ],
                outputs: vec![],
            })
            .with(KindSchema {
                name: "Build",
                category: Image,
                fields: vec![FieldSpec::required("repository")],
                outputs: vec![],
            })
            .with(KindSchema {
                name: "Mirror",
                category: Image,
                fields: vec![
                    FieldSpec::required("repository"),
                    FieldSpec::required("upstream"),
                ],
                outputs: vec![],
            })
    }
}
