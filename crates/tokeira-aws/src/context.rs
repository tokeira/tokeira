//! Common context for AWS resource construction.

use std::collections::HashMap;

/// Common parameters needed by all AWS resource constructors.
///
/// Constructed by the project crate from `ProjectConfig` and passed to
/// each resource. Keeps the `aws` crate free of config dependencies.
#[derive(Debug, Clone)]
pub struct ResourceContext {
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}
