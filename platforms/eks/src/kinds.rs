//! The author's kinds — typed structs the operator names in `definition.tkd`,
//! each realizing directly to one concrete engine [`iac::Resource`].
//!
//! Two families live here:
//! - **AWS kinds** (this file) realize to the existing `tokeira-aws` resources
//!   unchanged (Requirement 5.1); no AWS resource is reimplemented here. Each
//!   kind is a thin, typed façade over a `tokeira_aws` resource's constructor.
//! - **Kubernetes kinds** (added alongside) wrap `k8s-openapi` manifests as
//!   `iac::Resource`s applied through the context's `KubePlatform`.
//!
//! ## Implicit dependency wiring (load-bearing)
//!
//! `tokeira-aws` resources derive both their own `ResourceId` **and their
//! dependencies' IDs** from the passed name/identity + the project — e.g.
//! `EksClusterResource` hard-codes deps on `{project}-vpc`,
//! `sg-{project}-eks-nodes-sg`, `iam-role-{project}-eks-cluster-role`, and
//! `iam-role-{project}-eks-auto-node-role` (`resources/eks.rs`). There is no
//! explicit dependency-injection seam. Therefore these kinds MUST construct each
//! resource with a name of the form `{project}-{suffix}` so the reconstructed IDs
//! line up. The `*_suffix` constants below are that contract; changing one
//! without changing the matching `tokeira-aws` reconstruction silently breaks the
//! dependency graph. IDs produced: VPC `{project}-vpc`, SG `sg-{name}`, IAM role
//! `iam-role-{name}`, EKS `{project}-eks-cluster`, DSQL `dsql-{identity}`.

use std::collections::HashMap;

use tokeira_aws::{
    ResourceContext,
    resources::{
        dsql_cluster::{DsqlCluster as AwsDsqlCluster, DsqlClusterConfig, DsqlClusterMode},
        dsql_connection_endpoint::{
            DsqlConnectionEndpoint as AwsDsqlConnectionEndpoint, DsqlConnectionEndpointConfig,
        },
        dynamodb_table::{
            AttributeType, BillingMode, DynamoDbTable as AwsDynamoDbTable, DynamoDbTableConfig,
            KeyAttribute, KeyType,
        },
        ecr_repository::EcrRepository as AwsEcrRepository,
        eks::{EksClusterResource, EksConfig as AwsEksConfig},
        iam_role::{IamRole as AwsIamRole, IamRoleConfig},
        pod_identity_association::{
            PodIdentityAssociation as AwsPodIdentityAssociation, PodIdentityAssociationConfig,
        },
        s3_bucket::{S3Bucket as AwsS3Bucket, S3BucketConfig},
        secrets_manager_secret::{
            SecretValue, SecretsManagerSecret as AwsSecretsManagerSecret,
            SecretsManagerSecretConfig,
        },
        security_group::{SecurityGroup as AwsSecurityGroup, SecurityGroupConfig, SecurityRule},
        vpc::{VpcConfig, VpcResource},
        vpc_endpoint::{EndpointType, VpcEndpoint as AwsVpcEndpoint, VpcEndpointConfig},
    },
};
use tokeira_iac::{self as iac, ResourceId};
use tokeira_k8s::{NamespaceConfig, NamespaceResource};

use crate::{
    builder::Kind,
    context::Cx,
    k8s_resource::K8sManifestResource,
    manifests::{self, ServiceManifest},
};

/// The `foundation` module owns the private AWS substrate (VPC, endpoints,
/// security groups, IAM, DSQL + its connection endpoint, DynamoDB, S3, secret,
/// ECR). Recorded as each foundation resource's owning module.
const MODULE_FOUNDATION: &str = "foundation";
/// The `cluster` module owns the EKS cluster, the Kubernetes namespaces, and the
/// Pod-Identity associations. Depends only on `foundation`.
const MODULE_CLUSTER: &str = "cluster";

/// Fallback region when the context carries none (in-memory/plan contexts).
/// Mirrors compose-syn's `unwrap_or("us-east-1")` so realize is total.
const DEFAULT_REGION: &str = "us-east-1";

/// The resolved AWS region for realize-time resource construction.
fn region(cx: &Cx) -> String {
    cx.region
        .clone()
        .unwrap_or_else(|| DEFAULT_REGION.to_string())
}

/// The shared AWS resource context (project + region + the `ManagedBy` tag).
/// `tkp` is the provisioner identity, matching the Kubernetes field manager.
fn aws_ctx(cx: &Cx) -> ResourceContext {
    ResourceContext {
        project: cx.project_name.clone(),
        region: region(cx),
        tags: HashMap::from([("ManagedBy".to_owned(), "tkp".to_owned())]),
    }
}

/// The EKS cluster's derived `ResourceId` (`{project}-eks-cluster`), the anchor
/// for Pod-Identity and namespace dependencies.
fn eks_cluster_id(cx: &Cx) -> ResourceId {
    ResourceId(format!("{}-eks-cluster", cx.project_name))
}

// ─────────────────────────── Networking ───────────────────────────

/// The private-only VPC (→ `tokeira_aws::VpcResource`): one /24 private subnet
/// per AZ, no public subnet, no internet gateway, no NAT — all AWS access is via
/// VPC endpoints (Requirement 11.1). Its `subnet_ids` output feeds the EKS
/// cluster and the interface endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Vpc {
    pub cidr: String,
    pub availability_zones: Vec<String>,
}

impl Kind for Vpc {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(VpcResource::new(
            &aws_ctx(cx),
            VpcConfig {
                cidr: self.cidr.clone(),
                availability_zones: self.availability_zones.clone(),
            },
            MODULE_FOUNDATION,
        ))
    }
}

/// An ingress rule in the operator vocabulary. `source` is a CIDR or the literal
/// `"self"` (the SG's own id) — never `0.0.0.0/0` in a private-only deployment
/// (Requirement 11.3; enforced structurally by Property 10, which rejects a
/// `0.0.0.0/0` source anywhere in the realized plan).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressRule {
    pub description: String,
    pub protocol: String,
    pub from_port: u16,
    pub to_port: u16,
    pub source: String,
}

impl IngressRule {
    fn to_security_rule(&self) -> SecurityRule {
        SecurityRule {
            description: self.description.clone(),
            protocol: self.protocol.clone(),
            from_port: self.from_port,
            to_port: self.to_port,
            source: self.source.clone(),
        }
    }
}

/// A security group (→ `tokeira_aws::SecurityGroup`). `suffix` is prefixed with
/// the project to form the group name, so its `ResourceId` is
/// `sg-{project}-{suffix}` — the exact id the EKS cluster and DSQL endpoint
/// reconstruct as their SG dependency (see the module doc). Depends on the VPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityGroup {
    /// Project-relative name suffix, e.g. `eks-nodes-sg` or `vpc-endpoints-sg`.
    pub suffix: String,
    pub description: String,
    pub ingress: Vec<IngressRule>,
}

impl Kind for SecurityGroup {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsSecurityGroup::new(
            format!("{}-{}", cx.project_name, self.suffix),
            SecurityGroupConfig {
                vpc_dependency: ResourceId(format!("{}-vpc", cx.project_name)),
                description: self.description.clone(),
                ingress_rules: self
                    .ingress
                    .iter()
                    .map(IngressRule::to_security_rule)
                    .collect(),
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

/// An interface or gateway VPC endpoint (→ `tokeira_aws::VpcEndpoint`). The full
/// AWS service name is derived from the region and the short `service` name
/// (e.g. `com.amazonaws.{region}.ecr.api`). Interface endpoints attach the
/// VPC-endpoints security group; gateway endpoints (s3, dynamodb) do not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VpcEndpoint {
    /// Short service token, e.g. `s3`, `dynamodb`, `ecr.api`, `dsql`.
    pub service: String,
    /// `true` for an Interface endpoint (ENI + SG), `false` for a Gateway
    /// endpoint (route-table entry; s3/dynamodb only).
    pub interface: bool,
}

impl Kind for VpcEndpoint {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        let endpoint_type = if self.interface {
            EndpointType::Interface
        } else {
            EndpointType::Gateway
        };
        // Interface endpoints need the shared VPC-endpoints SG for :443 ingress
        // from the VPC; gateway endpoints have no ENI and take no SG.
        let security_group_dependency = self
            .interface
            .then(|| ResourceId(format!("sg-{}-vpc-endpoints-sg", cx.project_name)));
        Box::new(AwsVpcEndpoint::new(
            self.service.replace('.', "-"),
            VpcEndpointConfig {
                service_name: format!("com.amazonaws.{}.{}", region(cx), self.service),
                endpoint_type,
                vpc_dependency: ResourceId(format!("{}-vpc", cx.project_name)),
                security_group_dependency,
                resource_id: None,
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

// ─────────────────────────── Identity (IAM) ───────────────────────────

/// The trust/permission role kind (→ `tokeira_aws::IamRole`). `suffix` is
/// prefixed with the project to form `iam-role-{project}-{suffix}`, matching the
/// ids the EKS cluster (cluster-role, auto-node-role) and Pod-Identity
/// associations (per-service task roles) reconstruct. Inline policies are the
/// only privileges granted (least-privilege, Requirement 5.3/11.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IamRole {
    /// Project-relative role suffix, e.g. `eks-cluster-role`, `tokeira-task`.
    pub suffix: String,
    /// The assume-role (trust) policy JSON.
    pub trust_policy: String,
    /// Named inline policy documents (name → JSON).
    pub inline_policies: Vec<(String, String)>,
    /// AWS-managed policy ARNs to attach (empty for the least-privilege roles).
    pub managed_policy_arns: Vec<String>,
}

impl Kind for IamRole {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsIamRole::new(
            format!("{}-{}", cx.project_name, self.suffix),
            IamRoleConfig {
                trust_policy: self.trust_policy.clone(),
                inline_policies: self.inline_policies.iter().cloned().collect(),
                managed_policy_arns: self.managed_policy_arns.clone(),
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

// ─────────────────────────── DSQL (persistence + projection) ───────────────────────────

/// The single Aurora DSQL cluster (→ `tokeira_aws::DsqlCluster`) backing both
/// persistence and the projection plane (Requirement 8 — no separate visibility
/// store). Its mode is **inferred**: an endpoint or arn present means the
/// operator is adopting a pre-existing cluster (`Preexisting`); otherwise the
/// cluster is provider-`Managed` (Requirement 5.4). Identity is the project, so
/// its id is `dsql-{project}` — the id the connection endpoint depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DsqlCluster {
    pub endpoint: Option<String>,
    pub arn: Option<String>,
}

impl Kind for DsqlCluster {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        // Mode inference: presence of an operator-supplied endpoint/arn is the
        // signal to adopt rather than provision (Req 5.4). This is the single
        // #[create]-guarded identity decision (retarget-immutable in Block 4).
        let mode = if self.endpoint.is_some() || self.arn.is_some() {
            DsqlClusterMode::Preexisting
        } else {
            DsqlClusterMode::Managed
        };
        Box::new(AwsDsqlCluster::new(
            cx.project_name.clone(),
            DsqlClusterConfig {
                mode,
                preexisting_endpoint: self.endpoint.clone(),
                preexisting_arn: self.arn.clone(),
                fallback_identifier: None,
                resource_id: None,
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

/// The DSQL PrivateLink connection endpoint (→ `tokeira_aws::DsqlConnectionEndpoint`),
/// whose `private_hostname` output is the preferred in-VPC DSQL endpoint the
/// writeback projects into the server config (Requirement 9.1). Depends on the
/// VPC, the VPC-endpoints SG, and the DSQL cluster (`dsql-{project}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DsqlConnectionEndpoint;

impl Kind for DsqlConnectionEndpoint {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsDsqlConnectionEndpoint::new(
            cx.project_name.clone(),
            DsqlConnectionEndpointConfig {
                vpc_dependency: ResourceId(format!("{}-vpc", cx.project_name)),
                security_group_dependency: ResourceId(format!(
                    "sg-{}-vpc-endpoints-sg",
                    cx.project_name
                )),
                dsql_cluster_dependency: ResourceId(format!("dsql-{}", cx.project_name)),
                resource_id: None,
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

// ─────────────────────────── Coordination + storage ───────────────────────────

/// A DynamoDB coordination table (→ `tokeira_aws::DynamoDbTable`): on-demand,
/// single hash key, optional TTL — the DSQL rate-limiter and connection-lease
/// tables. Its `table_name` output feeds the writeback coordination-table keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbTable {
    pub table: String,
    pub hash_key: String,
    pub ttl: Option<String>,
}

impl Kind for DynamoDbTable {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsDynamoDbTable::new(
            self.table.clone(),
            DynamoDbTableConfig {
                key_schema: vec![KeyAttribute {
                    name: self.hash_key.clone(),
                    key_type: KeyType::Hash,
                    attribute_type: AttributeType::String,
                }],
                billing_mode: BillingMode::OnDemand,
                ttl_attribute: self.ttl.clone(),
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

/// An S3 bucket (→ `tokeira_aws::S3Bucket`) — the Mimir/Loki object stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Bucket {
    pub bucket: String,
    pub versioning: bool,
}

impl Kind for S3Bucket {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsS3Bucket::new(
            self.bucket.clone(),
            S3BucketConfig {
                versioning: self.versioning,
                module: MODULE_FOUNDATION.to_string(),
                key_prefix: None,
            },
            &aws_ctx(cx),
        ))
    }
}

/// A Secrets Manager secret holding a generated `{username,password}` JSON
/// (→ `tokeira_aws::SecretsManagerSecret`) — the Grafana admin credential. The
/// password is generated once at create and never recomputed on update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretsManagerSecret {
    pub name: String,
    pub username: String,
    pub password_length: i32,
}

impl Kind for SecretsManagerSecret {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsSecretsManagerSecret::new(
            self.name.clone(),
            SecretsManagerSecretConfig {
                value: SecretValue::GeneratedPasswordJson {
                    username: self.username.clone(),
                    password_length: self.password_length,
                },
                recovery_window_days: None,
                module: MODULE_FOUNDATION.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

/// An ECR repository (→ `tokeira_aws::EcrRepository`) for a tokeira image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EcrRepository {
    pub name: String,
}

impl Kind for EcrRepository {
    fn realize(&self, _cx: &Cx) -> Box<dyn iac::Resource> {
        // `EcrRepository::new` validates the repository name and returns `Err`
        // only for a syntactically invalid name. The names here are operator- or
        // author-derived tokeira image repos (e.g. `tokeirad`), which are always
        // valid ECR names, so the error path is unreachable in practice; realize
        // is infallible by the `Kind` contract. If a future config surfaces a
        // free-form repo name, validate it via `#[require]` in the `.tkd`.
        Box::new(
            AwsEcrRepository::new(self.name.clone(), MODULE_FOUNDATION.to_string())
                .expect("ECR repository name is author-derived and valid"),
        )
    }
}

// ─────────────────────────── EKS cluster + Pod Identity ───────────────────────────

/// The EKS Auto Mode cluster (→ `tokeira_aws::EksClusterResource`): private API
/// (`endpoint_public_access(false)`), Karpenter-managed compute, arm64 node role.
/// It reconstructs its own dependency ids from the project (VPC, eks-nodes SG,
/// cluster + auto-node IAM roles), so those resources must exist under the
/// matching `{project}-…` names (module doc). Owned by the `cluster` module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EksCluster {
    pub version: String,
    pub namespace: String,
    pub kms_key_arn: Option<String>,
    pub deletion_protection: bool,
    pub bootstrap_admin_permissions: bool,
    pub cluster_admin_principal_arn: Option<String>,
}

impl Kind for EksCluster {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(EksClusterResource::new(
            &aws_ctx(cx),
            AwsEksConfig {
                version: self.version.clone(),
                namespace: self.namespace.clone(),
                kms_key_arn: self.kms_key_arn.clone(),
                deletion_protection: self.deletion_protection,
                bootstrap_admin_permissions: self.bootstrap_admin_permissions,
                cluster_admin_principal_arn: self.cluster_admin_principal_arn.clone(),
            },
            MODULE_CLUSTER,
        ))
    }
}

/// A Pod-Identity association (→ `tokeira_aws::PodIdentityAssociation`) binding a
/// Kubernetes `ServiceAccount` to an IAM role, so a service's pods get AWS
/// credentials with no OIDC provider and no static keys (Requirement 11.4). The
/// role is referenced by its project-prefixed id (`iam-role-{project}-{role_suffix}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodIdentityAssociation {
    pub namespace: String,
    pub service_account: String,
    /// Project-relative suffix of the target task role (e.g. `tokeira-task`).
    pub role_suffix: String,
}

impl Kind for PodIdentityAssociation {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsPodIdentityAssociation::new(
            PodIdentityAssociationConfig {
                eks_cluster_dependency: eks_cluster_id(cx),
                namespace: self.namespace.clone(),
                service_account: self.service_account.clone(),
                iam_role_dependency: ResourceId(format!(
                    "iam-role-{}-{}",
                    cx.project_name, self.role_suffix
                )),
                module: MODULE_CLUSTER.to_string(),
            },
            &aws_ctx(cx),
        ))
    }
}

// ─────────────────────────── Kubernetes objects ───────────────────────────

/// A Kubernetes namespace (→ `tokeira_k8s::NamespaceResource`), applied through
/// the context's `KubePlatform`. Owned by the `cluster` module and dependent on
/// the EKS cluster (there is no API server to create it in until the cluster
/// exists).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Namespace {
    pub name: String,
}

impl Kind for Namespace {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(NamespaceResource::new(
            self.name.clone(),
            NamespaceConfig {
                eks_cluster_dependency: eks_cluster_id(cx),
                module: MODULE_CLUSTER.to_string(),
            },
            cx.project_name.clone(),
        ))
    }
}

/// The Karpenter `NodePool` (cluster-scoped), applied through `KubePlatform`.
/// Depends on the EKS cluster (the NodePool CRD is served by the Auto Mode
/// control plane).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodePool {
    pub node_families: Vec<String>,
}

impl Kind for NodePool {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(K8sManifestResource::new(
            "k8s-nodepool",
            MODULE_CLUSTER,
            vec![eks_cluster_id(cx)],
            vec![manifests::node_pool(&self.node_families)],
        ))
    }
}

/// One tokeira service's Kubernetes objects — `ServiceAccount`, config
/// `ConfigMap`, Alloy-config `ConfigMap`, `Deployment`, and ClusterIP `Service`
/// — applied together through `KubePlatform`. Ordered so the ServiceAccount and
/// ConfigMaps exist before the Deployment that references them. Depends on the
/// EKS cluster and the target namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceDeployment {
    /// The per-service pod/service shape (name, image, ports, resources, SA).
    pub spec: ServiceManifest,
    /// The rendered server config file (`tokeirad.toml`/`controller.toml`/
    /// `autoscaler.toml`); its DSQL endpoint is filled by writeback (Req 9).
    pub config_content: String,
    /// The rendered Alloy sidecar config (`config.alloy`).
    pub alloy_config_content: String,
}

impl Kind for ServiceDeployment {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        let s = &self.spec;
        // Order is load-bearing: the ServiceAccount and both ConfigMaps must be
        // applied before the Deployment that mounts/uses them.
        let objects = vec![
            manifests::service_account(&s.service_account, &s.namespace, &s.project),
            manifests::config_map(
                &s.config_map,
                &s.namespace,
                &s.project,
                &s.config_file,
                &self.config_content,
            ),
            manifests::config_map(
                &format!("alloy-config-{}", s.name),
                &s.namespace,
                &s.project,
                "config.alloy",
                &self.alloy_config_content,
            ),
            manifests::deployment(s),
            manifests::service(s),
        ];
        Box::new(K8sManifestResource::new(
            format!("k8s-{}", s.name),
            MODULE_CLUSTER,
            vec![
                eks_cluster_id(cx),
                ResourceId(format!("namespace/{}", s.namespace)),
            ],
            objects,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn cx() -> Cx {
        Cx {
            project_name: "tokeira".into(),
            region: Some("eu-west-2".into()),
            account_id: Some("123456789012".into()),
            deployment_dir: PathBuf::from("/tmp/deploy"),
        }
    }

    #[test]
    fn vpc_realizes_with_project_derived_id() {
        let r = Vpc {
            cidr: "10.0.0.0/16".into(),
            availability_zones: vec!["eu-west-2a".into()],
        }
        .realize(&cx());
        assert_eq!(r.resource_id(), ResourceId("tokeira-vpc".into()));
        assert_eq!(r.module(), MODULE_FOUNDATION);
    }

    #[test]
    fn security_group_id_matches_eks_cluster_dependency() {
        // The EKS cluster depends on `sg-{project}-eks-nodes-sg`; the SG kind must
        // produce exactly that id or the implicit dep graph breaks.
        let r = SecurityGroup {
            suffix: "eks-nodes-sg".into(),
            description: "eks nodes".into(),
            ingress: vec![],
        }
        .realize(&cx());
        assert_eq!(
            r.resource_id(),
            ResourceId("sg-tokeira-eks-nodes-sg".into())
        );
    }

    #[test]
    fn iam_role_ids_match_eks_cluster_dependencies() {
        for suffix in ["eks-cluster-role", "eks-auto-node-role"] {
            let r = IamRole {
                suffix: suffix.into(),
                trust_policy: "{}".into(),
                inline_policies: vec![],
                managed_policy_arns: vec![],
            }
            .realize(&cx());
            assert_eq!(
                r.resource_id(),
                ResourceId(format!("iam-role-tokeira-{suffix}"))
            );
        }
    }

    #[test]
    fn eks_cluster_has_expected_id_and_module() {
        let r = EksCluster {
            version: "1.36".into(),
            namespace: "tokeira-system".into(),
            kms_key_arn: None,
            deletion_protection: true,
            bootstrap_admin_permissions: false,
            cluster_admin_principal_arn: None,
        }
        .realize(&cx());
        assert_eq!(r.resource_id(), eks_cluster_id(&cx()));
        assert_eq!(r.module(), MODULE_CLUSTER);
    }

    #[test]
    fn dsql_mode_is_inferred_from_endpoint_arn_presence() {
        // Managed when neither endpoint nor arn is set…
        let managed = DsqlCluster {
            endpoint: None,
            arn: None,
        }
        .realize(&cx());
        assert_eq!(managed.resource_id(), ResourceId("dsql-tokeira".into()));

        // …and the connection endpoint depends on exactly that cluster id.
        let ce = DsqlConnectionEndpoint.realize(&cx());
        assert!(
            ce.dependencies()
                .contains(&ResourceId("dsql-tokeira".into())),
            "connection endpoint must depend on the dsql cluster id"
        );
    }

    #[test]
    fn dynamodb_and_s3_and_secret_ids() {
        let ddb = DynamoDbTable {
            table: "tokeira-dsql-rate-limiter".into(),
            hash_key: "pk".into(),
            ttl: Some("ttl_epoch".into()),
        }
        .realize(&cx());
        assert_eq!(
            ddb.resource_id(),
            ResourceId("dynamodb-tokeira-dsql-rate-limiter".into())
        );

        let bucket = S3Bucket {
            bucket: "tokeira-mimir".into(),
            versioning: false,
        }
        .realize(&cx());
        assert_eq!(bucket.resource_id(), ResourceId("s3-tokeira-mimir".into()));

        let secret = SecretsManagerSecret {
            name: "tokeira-grafana-admin".into(),
            username: "admin".into(),
            password_length: 32,
        }
        .realize(&cx());
        assert_eq!(
            secret.resource_id(),
            ResourceId("secret-tokeira-grafana-admin".into())
        );
    }

    #[test]
    fn pod_identity_binds_service_account_to_task_role() {
        let r = PodIdentityAssociation {
            namespace: "tokeira-system".into(),
            service_account: "edge-api".into(),
            role_suffix: "tokeira-task".into(),
        }
        .realize(&cx());
        let deps = r.dependencies();
        assert!(deps.contains(&eks_cluster_id(&cx())));
        assert!(deps.contains(&ResourceId("iam-role-tokeira-tokeira-task".into())));
        assert_eq!(r.module(), MODULE_CLUSTER);
    }
}
