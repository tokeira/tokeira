//! EKS-owned authoring kinds.
//!
//! Generic AWS kinds remain in `tokeira_aws`; this namespace owns only the
//! two EKS resources not exported there and the Kubernetes lifecycle kinds.
//! That split keeps kind names collision-free while the platform declaration
//! includes the complete AWS namespace that triggers the framework's standard
//! deployment-scoped client bundle.

use serde::Deserialize;
use tokeira_aws::{
    ResourceContext,
    resources::{
        eks::{EksClusterResource, EksConfig},
        pod_identity_association::{
            PodIdentityAssociation as AwsPodIdentityAssociation, PodIdentityAssociationConfig,
        },
    },
};
use tokeira_iac::ResourceId;
use tokeira_k8s::{NamespaceConfig, NamespaceResource};
use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace as AuthoringNamespace,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

use crate::{
    k8s_resource::K8sManifestResource,
    manifests::{self, ServiceManifest},
    service::KubernetesService,
};

/// Normalized package namespace definitions import.
pub const NAMESPACE: &str = "tokeira_eks_deployment";
/// Author-visible EKS cluster type.
pub const EKS_CLUSTER_TYPE: &str = "EksCluster";
/// Author-visible EKS Pod Identity association type.
pub const POD_IDENTITY_TYPE: &str = "PodIdentityAssociation";
/// Author-visible Kubernetes namespace type.
pub const NAMESPACE_TYPE: &str = "Namespace";
/// Author-visible Auto Mode node-pool type.
pub const NODE_POOL_TYPE: &str = "NodePool";
/// Author-visible Kubernetes workload type.
pub const SERVICE_DEPLOYMENT_TYPE: &str = "ServiceDeployment";

/// Complete kind vocabulary owned by this package.
pub const KINDS: &[&str] = &[
    EKS_CLUSTER_TYPE,
    POD_IDENTITY_TYPE,
    NAMESPACE_TYPE,
    NODE_POOL_TYPE,
    SERVICE_DEPLOYMENT_TYPE,
];

fn resource_context(region: &str, placement: &PlacementContext) -> ResourceContext {
    ResourceContext {
        project: placement.deployment_id.clone(),
        region: region.to_string(),
        tags: placement.tags.clone().into_iter().collect(),
    }
}

fn required_dependency(
    placement: &PlacementContext,
    kind: &str,
    predicate: impl Fn(&str) -> bool,
) -> Result<ResourceId, KindError> {
    placement
        .dependencies
        .iter()
        .find(|dependency| predicate(&dependency.0))
        .cloned()
        .ok_or_else(|| {
            KindError::new(format!(
                "{kind} needs its provider prerequisite declared as a dependency"
            ))
        })
}

/// Authored EKS Auto Mode cluster configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EksCluster {
    /// AWS region.
    pub region: String,
    /// Kubernetes minor version.
    pub version: String,
    /// Primary application namespace recorded with the cluster.
    pub namespace: String,
    /// Optional KMS key used for secret encryption.
    #[serde(default)]
    pub kms_key_arn: Option<String>,
    /// Protect the cluster from provider-side deletion.
    pub deletion_protection: bool,
    /// Grant the creator bootstrap administrator permissions.
    pub bootstrap_admin_permissions: bool,
    /// Optional explicit cluster administrator principal.
    #[serde(default)]
    pub cluster_admin_principal_arn: Option<String>,
}

impl Kind<EksClusterResource> for EksCluster {
    fn realize(&self, placement: &PlacementContext) -> Result<EksClusterResource, KindError> {
        Ok(EksClusterResource::new(
            &resource_context(&self.region, placement),
            EksConfig {
                version: self.version.clone(),
                namespace: self.namespace.clone(),
                kms_key_arn: self.kms_key_arn.clone(),
                deletion_protection: self.deletion_protection,
                bootstrap_admin_permissions: self.bootstrap_admin_permissions,
                cluster_admin_principal_arn: self.cluster_admin_principal_arn.clone(),
            },
            placement.module.clone(),
        ))
    }
}

/// Authored binding from one Kubernetes service account to one IAM role.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PodIdentityAssociation {
    /// AWS region.
    pub region: String,
    /// Kubernetes namespace containing the service account.
    pub namespace: String,
    /// Kubernetes service-account name.
    pub service_account: String,
}

impl Kind<AwsPodIdentityAssociation> for PodIdentityAssociation {
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<AwsPodIdentityAssociation, KindError> {
        let cluster = required_dependency(placement, POD_IDENTITY_TYPE, |id| {
            id.ends_with("-eks-cluster")
        })?;
        let role = required_dependency(placement, POD_IDENTITY_TYPE, |id| {
            id.starts_with("iam-role-")
        })?;
        Ok(AwsPodIdentityAssociation::new(
            PodIdentityAssociationConfig {
                eks_cluster_dependency: cluster,
                namespace: self.namespace.clone(),
                service_account: self.service_account.clone(),
                iam_role_dependency: role,
                module: placement.module.clone(),
            },
            &resource_context(&self.region, placement),
        ))
    }
}

/// Authored Kubernetes namespace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Namespace {
    /// Namespace name.
    pub name: String,
}

impl Kind<NamespaceResource> for Namespace {
    fn realize(&self, placement: &PlacementContext) -> Result<NamespaceResource, KindError> {
        let cluster =
            required_dependency(placement, NAMESPACE_TYPE, |id| id.ends_with("-eks-cluster"))?;
        Ok(NamespaceResource::new(
            self.name.clone(),
            NamespaceConfig {
                eks_cluster_dependency: cluster,
                module: placement.module.clone(),
            },
            placement.deployment_id.clone(),
        ))
    }
}

/// Authored EKS Auto Mode node-family policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodePool {
    /// Allowed EC2 instance families.
    pub node_families: Vec<String>,
}

impl Kind<K8sManifestResource> for NodePool {
    fn realize(&self, placement: &PlacementContext) -> Result<K8sManifestResource, KindError> {
        let cluster =
            required_dependency(placement, NODE_POOL_TYPE, |id| id.ends_with("-eks-cluster"))?;
        Ok(K8sManifestResource::new(
            NODE_POOL_TYPE,
            format!("nodepool/{}", placement.logical_id),
            placement.module.clone(),
            vec![cluster],
            vec![manifests::node_pool(&self.node_families)],
        ))
    }
}

/// Authored Kubernetes workload and its platform-owned ConfigMap delivery.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceDeployment {
    /// Desired pod and Service shape.
    pub spec: ServiceManifest,
    /// Rendered configuration used by non-`tokeirad` workloads.
    pub config_content: String,
    /// Rendered Alloy sidecar configuration.
    pub alloy_config_content: String,
    /// Service names that must become ready first.
    #[serde(default)]
    pub depends_on: Vec<String>,
}

impl Kind<KubernetesService> for ServiceDeployment {
    fn realize(&self, placement: &PlacementContext) -> Result<KubernetesService, KindError> {
        let server_config = placement
            .dependencies
            .iter()
            .any(|dependency| dependency == &tokeira_deployment::server_config::resource_id());
        if self.spec.name == "tokeirad" && !server_config {
            return Err(KindError::new(
                "tokeirad ServiceDeployment needs ServerConfig declared as a dependency",
            ));
        }
        let dsql_endpoint_dependency = self
            .config_content
            .contains("__WRITEBACK_DSQL_ENDPOINT__")
            .then(|| {
                required_dependency(placement, SERVICE_DEPLOYMENT_TYPE, |id| {
                    id == "dsql:connection-endpoint"
                })
            })
            .transpose()?;
        Ok(KubernetesService::new(
            self.spec.clone(),
            self.config_content.clone(),
            self.alloy_config_content.clone(),
            self.depends_on.clone(),
            placement.module.clone(),
            server_config.then(|| placement.definition_dir.join("tokeirad.toml")),
            server_config.then(|| placement.deployment_dir.join("tokeirad.toml")),
            dsql_endpoint_dependency,
        ))
    }
}

/// Decode an authored kind; unknown names are not this namespace's.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    Some(match name {
        EKS_CLUSTER_TYPE => {
            kind::decode_resource::<EksCluster, EksClusterResource>(EKS_CLUSTER_TYPE, value)
        }
        POD_IDENTITY_TYPE => kind::decode_resource::<
            PodIdentityAssociation,
            AwsPodIdentityAssociation,
        >(POD_IDENTITY_TYPE, value),
        NAMESPACE_TYPE => {
            kind::decode_resource::<Namespace, NamespaceResource>(NAMESPACE_TYPE, value)
        }
        NODE_POOL_TYPE => {
            kind::decode_resource::<NodePool, K8sManifestResource>(NODE_POOL_TYPE, value)
        }
        SERVICE_DEPLOYMENT_TYPE => kind::decode_service::<ServiceDeployment, KubernetesService>(
            SERVICE_DEPLOYMENT_TYPE,
            value,
        ),
        _ => return None,
    })
}

/// Assemble the EKS package namespace.
pub fn namespace() -> AuthoringNamespace {
    AuthoringNamespace {
        name: NAMESPACE,
        kinds: KINDS,
        defaults: None,
        decode,
    }
}

#[cfg(test)]
mod tests {
    use tokeira_iac::Resource as _;
    use tokeira_platform::{author::ValueShape, kind::Kind as _};

    use super::*;

    fn placement(dependencies: Vec<ResourceId>) -> PlacementContext {
        PlacementContext {
            deployment_id: "demo".to_string(),
            deployment_dir: "/tmp/demo".into(),
            definition_dir: "/tmp/demo".into(),
            module: "cluster".to_string(),
            logical_id: "probe".to_string(),
            dependencies,
            dependency_content: Default::default(),
            tags: Default::default(),
        }
    }

    #[test]
    fn every_advertised_kind_decodes() {
        for name in KINDS {
            let value = LocatedValue::new(ValueShape::Struct {
                name: (*name).to_string(),
                fields: Vec::new(),
            });
            let decoded = decode(name, value).expect("advertised kind belongs to the namespace");
            if let Err(error) = decoded {
                assert!(
                    !error.message.contains("unknown"),
                    "{name}: {}",
                    error.message
                );
            }
        }
        let value = LocatedValue::new(ValueShape::Struct {
            name: "Unknown".to_string(),
            fields: Vec::new(),
        });
        assert!(decode("Unknown", value).is_none());
    }

    // Feature: platform-eks, Property 9
    #[test]
    fn type_names_match_realized_resources() {
        let cluster = EksCluster {
            region: "eu-west-2".to_string(),
            version: "1.36".to_string(),
            namespace: "tokeira-system".to_string(),
            kms_key_arn: None,
            deletion_protection: true,
            bootstrap_admin_permissions: false,
            cluster_admin_principal_arn: None,
        }
        .realize(&placement(Vec::new()))
        .expect("cluster");
        assert_eq!(cluster.resource_type().0, EKS_CLUSTER_TYPE);

        let node_pool = NodePool {
            node_families: vec!["m8g".into(), "c8g".into(), "r8g".into()],
        }
        .realize(&placement(vec![cluster.resource_id()]))
        .expect("node pool");
        assert_eq!(node_pool.resource_type().0, NODE_POOL_TYPE);
    }
}
