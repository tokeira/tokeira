//! Definition-backed EKS platform package.
//!
//! The package declares the authoring namespaces and execution seams consumed
//! by `tkp`; AWS resources remain in `tokeira-aws`, Kubernetes mechanics remain
//! in `tokeira-k8s`, and this crate owns only EKS vocabulary and pod shape.

use std::sync::Arc;

use tokeira_platform::{declaration::PlatformDeclaration, definition::Namespace};

pub mod execution;
mod k8s_resource;
pub mod kinds;
mod manifests;
pub mod observability_content;
mod ops;
mod service;

fn namespaces() -> Vec<Namespace> {
    vec![
        kinds::namespace(),
        tokeira_deployment::server_config::namespace(),
        observability_content::namespace(),
        Namespace {
            name: tokeira_aws::kinds::NAMESPACE,
            kinds: tokeira_aws::kinds::KINDS,
            defaults: None,
            decode: tokeira_aws::kinds::decode,
        },
    ]
}

/// Pure EKS platform declaration.
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration {
        namespaces: namespaces(),
        ops: Some(Box::new(ops::EksOps::default())),
        execution: Box::new(execution::EksExecution),
        implementation: Arc::new(execution::EksIntegration::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declaration_is_pure_and_kind_names_are_unique() {
        let declaration = platform();
        assert_eq!(
            declaration
                .namespaces
                .iter()
                .map(|namespace| namespace.name)
                .collect::<Vec<_>>(),
            [
                "tokeira_eks_deployment",
                "tokeira_deployment",
                "tokeira_eks_content",
                "tokeira_aws"
            ]
        );
        assert!(declaration.ops.is_some());
        let mut names = std::collections::BTreeSet::new();
        for namespace in declaration.namespaces {
            for kind in namespace.kinds {
                assert!(names.insert(kind), "duplicate kind `{kind}`");
            }
        }
    }
}
