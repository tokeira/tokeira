//! Reusable, typed author inputs for AWS resources.
//!
//! This module is the AWS provider's kind export. Kinds and resources are
//! distinct: each kind here is the authored face of one resource in
//! [`crate::resources`] and realizes it directly. The namespace facts —
//! [`NAMESPACE`], [`KINDS`], [`decode`] — are what the assembled binary
//! lists for the frontend; nothing is registered anywhere else.
//!
//! Kinds whose paired resource keeps its fields private construct through
//! the resource's own validated constructor — wrapping, never redefining,
//! provider behaviour. Kind files carry their own `TYPE` consts (pinned to
//! the realized `resource_type()` by the tests below) so the authoring
//! surface stays within this module. Typed dependency fields are filled by
//! classifying the declared dependencies' realized ids against the
//! provider's stable id conventions; a missing class refuses by name.

pub mod alb;
pub mod cloud_map;
pub mod dsql_cluster;
pub mod dsql_connection_endpoint;
pub mod dynamodb_table;
pub mod ecr_repository;
pub mod ecs_cluster;
pub mod iam;
pub mod remote_state_bucket;
pub mod s3_bucket;
pub mod s3_object;
pub mod secrets_manager_secret;
pub mod security_group;
pub mod ssm_parameter;
pub mod vpc;
pub mod vpc_endpoint;

use tokeira_iac::ResourceId;
use tokeira_platform::{
    author::LocatedValue,
    error::KindError,
    kind::{self, DecodedKind, PlacementContext},
};

use crate::resources::{
    dsql_cluster::DsqlCluster as DsqlClusterResource,
    dsql_connection_endpoint::DsqlConnectionEndpoint as DsqlConnectionEndpointResource,
    dynamodb_table::DynamoDbTable as DynamoDbTableResource,
    ecr_repository::EcrRepository as EcrRepositoryResource,
    ecs_cluster::{
        AsgResource, CapacityProviderResource, EcsClusterResource, LaunchTemplateResource,
    },
    ecs_service::CloudMapNamespaceResource,
    elbv2::{AlbListenerResource, AlbResource, AlbTargetGroupResource},
    iam_instance_profile::IamInstanceProfile as IamInstanceProfileResource,
    iam_role::IamRole as IamRoleResource,
    remote_state_bucket::RemoteStateBucket as RemoteStateBucketResource,
    s3_bucket::S3Bucket as S3BucketResource,
    s3_object::S3Object as S3ObjectResource,
    secrets_manager_secret::SecretsManagerSecret as SecretsManagerSecretResource,
    security_group::SecurityGroup as SecurityGroupResource,
    ssm_parameter::SsmParameterResource,
    vpc::VpcResource,
    vpc_endpoint::VpcEndpoint as VpcEndpointResource,
};

pub use dsql_cluster::DsqlCluster;
pub use dynamodb_table::DynamoDbTable;

/// The namespace word: the normalized crate name definitions import from.
pub const NAMESPACE: &str = "tokeira_aws";

/// The provider's author-visible kind names, each the word its resource
/// owns.
pub const KINDS: &[&str] = &[
    alb::ALB_TYPE,
    alb::LISTENER_TYPE,
    alb::TARGET_GROUP_TYPE,
    ecs_cluster::ASG_TYPE,
    cloud_map::TYPE,
    DsqlClusterResource::TYPE,
    dsql_connection_endpoint::TYPE,
    DynamoDbTableResource::TYPE,
    ecr_repository::TYPE,
    ecs_cluster::CAPACITY_PROVIDER_TYPE,
    ecs_cluster::CLUSTER_TYPE,
    iam::INSTANCE_PROFILE_TYPE,
    iam::ROLE_TYPE,
    ecs_cluster::LAUNCH_TEMPLATE_TYPE,
    remote_state_bucket::TYPE,
    s3_bucket::TYPE,
    s3_object::TYPE,
    secrets_manager_secret::TYPE,
    security_group::TYPE,
    ssm_parameter::TYPE,
    vpc::TYPE,
    vpc_endpoint::TYPE,
];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    Some(match name {
        n if n == alb::ALB_TYPE => {
            kind::decode_resource::<alb::Alb, AlbResource>(alb::ALB_TYPE, value)
        }
        n if n == alb::LISTENER_TYPE => kind::decode_resource::<
            alb::AlbListener,
            AlbListenerResource,
        >(alb::LISTENER_TYPE, value),
        n if n == alb::TARGET_GROUP_TYPE => kind::decode_resource::<
            alb::AlbTargetGroup,
            AlbTargetGroupResource,
        >(alb::TARGET_GROUP_TYPE, value),
        n if n == ecs_cluster::ASG_TYPE => kind::decode_resource::<
            ecs_cluster::AutoScalingGroup,
            AsgResource,
        >(ecs_cluster::ASG_TYPE, value),
        n if n == cloud_map::TYPE => kind::decode_resource::<
            cloud_map::CloudMapNamespace,
            CloudMapNamespaceResource,
        >(cloud_map::TYPE, value),
        DsqlClusterResource::TYPE => kind::decode_resource::<DsqlCluster, DsqlClusterResource>(
            DsqlClusterResource::TYPE,
            value,
        ),
        n if n == dsql_connection_endpoint::TYPE => kind::decode_resource::<
            dsql_connection_endpoint::DsqlConnectionEndpoint,
            DsqlConnectionEndpointResource,
        >(dsql_connection_endpoint::TYPE, value),
        DynamoDbTableResource::TYPE => {
            kind::decode_resource::<DynamoDbTable, DynamoDbTableResource>(
                DynamoDbTableResource::TYPE,
                value,
            )
        }
        n if n == ecr_repository::TYPE => kind::decode_resource::<
            ecr_repository::EcrRepository,
            EcrRepositoryResource,
        >(ecr_repository::TYPE, value),
        n if n == ecs_cluster::CAPACITY_PROVIDER_TYPE => {
            kind::decode_resource::<ecs_cluster::CapacityProvider, CapacityProviderResource>(
                ecs_cluster::CAPACITY_PROVIDER_TYPE,
                value,
            )
        }
        n if n == ecs_cluster::CLUSTER_TYPE => kind::decode_resource::<
            ecs_cluster::EcsCluster,
            EcsClusterResource,
        >(ecs_cluster::CLUSTER_TYPE, value),
        n if n == iam::INSTANCE_PROFILE_TYPE => kind::decode_resource::<
            iam::IamInstanceProfile,
            IamInstanceProfileResource,
        >(iam::INSTANCE_PROFILE_TYPE, value),
        n if n == iam::ROLE_TYPE => {
            kind::decode_resource::<iam::IamRole, IamRoleResource>(iam::ROLE_TYPE, value)
        }
        n if n == ecs_cluster::LAUNCH_TEMPLATE_TYPE => {
            kind::decode_resource::<ecs_cluster::LaunchTemplate, LaunchTemplateResource>(
                ecs_cluster::LAUNCH_TEMPLATE_TYPE,
                value,
            )
        }
        n if n == remote_state_bucket::TYPE => kind::decode_resource::<
            remote_state_bucket::RemoteStateBucket,
            RemoteStateBucketResource,
        >(remote_state_bucket::TYPE, value),
        n if n == s3_bucket::TYPE => {
            kind::decode_resource::<s3_bucket::S3Bucket, S3BucketResource>(s3_bucket::TYPE, value)
        }
        n if n == s3_object::TYPE => {
            kind::decode_resource::<s3_object::S3Object, S3ObjectResource>(s3_object::TYPE, value)
        }
        n if n == secrets_manager_secret::TYPE => kind::decode_resource::<
            secrets_manager_secret::SecretsManagerSecret,
            SecretsManagerSecretResource,
        >(secrets_manager_secret::TYPE, value),
        n if n == security_group::TYPE => kind::decode_resource::<
            security_group::SecurityGroup,
            SecurityGroupResource,
        >(security_group::TYPE, value),
        n if n == ssm_parameter::TYPE => kind::decode_resource::<
            ssm_parameter::SsmParameter,
            SsmParameterResource,
        >(ssm_parameter::TYPE, value),
        n if n == vpc::TYPE => kind::decode_resource::<vpc::Vpc, VpcResource>(vpc::TYPE, value),
        n if n == vpc_endpoint::TYPE => kind::decode_resource::<
            vpc_endpoint::VpcEndpoint,
            VpcEndpointResource,
        >(vpc_endpoint::TYPE, value),
        _ => return None,
    })
}

/// Build the provider [`crate::ResourceContext`] for one realization: the
/// project scope is the deployment identity, the region is authored on the
/// kind, and tags flow from placement. (On the bound path placement tags are
/// deliberately empty — authored tags travel as kind fields.)
fn resource_context(region: &str, placement: &PlacementContext) -> crate::ResourceContext {
    crate::ResourceContext {
        project: placement.deployment_id.clone(),
        region: region.to_string(),
        tags: placement.tags.clone().into_iter().collect(),
    }
}

/// Select the declared dependency whose realized id matches the class
/// predicate, refusing with the class name when absent. Dependency ids are
/// stable resource-id conventions, so classification by shape is the seam
/// between logical `.tkd` dependency edges and typed dependency fields.
fn required_dependency(
    placement: &PlacementContext,
    class: &str,
    matches: impl Fn(&str) -> bool,
) -> Result<ResourceId, KindError> {
    optional_dependency(placement, matches).ok_or_else(|| {
        KindError::new(format!(
            "no declared dependency realizes a {class}; declare one in the resource's dependency list"
        ))
    })
}

/// Select a declared dependency by id shape when present.
fn optional_dependency(
    placement: &PlacementContext,
    matches: impl Fn(&str) -> bool,
) -> Option<ResourceId> {
    placement
        .dependencies
        .iter()
        .find(|id| matches(&id.0))
        .cloned()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokeira_iac::Resource as _;
    use tokeira_platform::author::LocatedValue;

    use super::*;

    // The namespace facts hold together: every listed name decodes here
    // (each entry admits its own input shape), and an unknown name is
    // refused as not-ours rather than an error.
    #[test]
    fn every_listed_kind_decodes_and_unknown_names_refuse() {
        assert_eq!(
            KINDS,
            [
                "Alb",
                "AlbListener",
                "AlbTargetGroup",
                "AutoScalingGroup",
                "CloudMapNamespace",
                "DsqlCluster",
                "DsqlConnectionEndpoint",
                "DynamoDbTable",
                "EcrRepository",
                "EcsCapacityProvider",
                "EcsCluster",
                "IamInstanceProfile",
                "IamRole",
                "LaunchTemplate",
                "RemoteStateBucket",
                "S3Bucket",
                "S3Object",
                "SecretsManagerSecret",
                "SecurityGroup",
                "SsmParameter",
                "Vpc",
                "VpcEndpoint",
            ]
        );
        for name in KINDS {
            let probe = decode(
                name,
                LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
                    name: (*name).to_string(),
                    fields: Vec::new(),
                }),
            )
            .unwrap_or_else(|| panic!("kind `{name}` must belong to {NAMESPACE}"));
            if let Err(error) = probe {
                assert!(
                    !error.message.contains("unknown"),
                    "entry `{name}` failed as unknown: {}",
                    error.message
                );
            }
        }
        assert!(
            decode(
                "NotAnAwsKind",
                LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
                    name: "NotAnAwsKind".to_string(),
                    fields: Vec::new(),
                }),
            )
            .is_none()
        );
    }

    fn placement(dependencies: Vec<ResourceId>) -> PlacementContext {
        PlacementContext {
            deployment_id: "demo".to_string(),
            deployment_dir: std::path::PathBuf::from("/tmp/demo"),
            definition_dir: std::path::PathBuf::from("/tmp/demo"),
            module: "networking".to_string(),
            logical_id: "probe".to_string(),
            dependencies,
            dependency_content: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    // Each thin twin's TYPE const agrees with the realized resource's own
    // resource_type() — the pin that lets the authoring surface carry the
    // name while the resource stays untouched.
    #[test]
    fn twin_type_consts_match_realized_resource_types() {
        use tokeira_platform::kind::Kind as _;

        let no_deps = placement(Vec::new());
        let with_vpc = placement(vec![ResourceId("demo-vpc".into())]);

        assert_eq!(
            vpc::Vpc {
                region: "eu-west-2".into(),
                cidr: "10.0.0.0/16".into(),
                availability_zones: vec!["eu-west-2a".into()],
            }
            .realize(&no_deps)
            .expect("vpc")
            .resource_type()
            .0,
            vpc::TYPE
        );

        assert_eq!(
            security_group::SecurityGroup {
                region: "eu-west-2".into(),
                name: "edge".into(),
                description: "edge ingress".into(),
                ingress: Vec::new(),
            }
            .realize(&with_vpc)
            .expect("security group")
            .resource_type()
            .0,
            security_group::TYPE
        );

        assert_eq!(
            vpc_endpoint::VpcEndpoint {
                region: "eu-west-2".into(),
                short_name: "ssm".into(),
                service_name: "com.amazonaws.eu-west-2.ssm".into(),
                endpoint_type: vpc_endpoint::EndpointKind::Interface,
                id: None,
            }
            .realize(&with_vpc)
            .expect("endpoint")
            .resource_type()
            .0,
            vpc_endpoint::TYPE
        );

        assert_eq!(
            remote_state_bucket::RemoteStateBucket {
                region: "eu-west-2".into(),
                bucket: "demo-state".into(),
                key_prefix: Some("demo/dev".into()),
            }
            .realize(&no_deps)
            .expect("remote-state bucket")
            .resource_type()
            .0,
            remote_state_bucket::TYPE
        );

        assert_eq!(
            s3_bucket::S3Bucket {
                region: "eu-west-2".into(),
                bucket: "demo-state".into(),
                versioning: true,
                key_prefix: None,
            }
            .realize(&no_deps)
            .expect("bucket")
            .resource_type()
            .0,
            s3_bucket::TYPE
        );

        assert_eq!(
            ecr_repository::EcrRepository {
                repository: "tokeira/tokeirad".into(),
            }
            .realize(&no_deps)
            .expect("repository")
            .resource_type()
            .0,
            ecr_repository::TYPE
        );

        let with_vpc_sg = placement(vec![
            ResourceId("demo-vpc".into()),
            ResourceId("sg-alb".into()),
        ]);
        assert_eq!(
            alb::Alb {
                name: "tokeira".into()
            }
            .realize(&with_vpc_sg)
            .expect("alb")
            .resource_type()
            .0,
            alb::ALB_TYPE
        );

        assert_eq!(
            alb::AlbTargetGroup {
                name: "edge-api".into(),
                port: 7233,
                health_check_path: "/health".into(),
                health_check_interval_secs: 30,
            }
            .realize(&with_vpc)
            .expect("target group")
            .resource_type()
            .0,
            alb::TARGET_GROUP_TYPE
        );

        let listener_placement = placement(vec![
            ResourceId("alb-tokeira".into()),
            ResourceId("alb-tg-edge-api".into()),
            ResourceId("alb-tg-edge-poll".into()),
        ]);
        assert_eq!(
            alb::AlbListener {
                name: "tokeira".into(),
                protocol: alb::ListenerProtocol::Http2,
                certificate_arn: None,
                private_dns_zone: "tokeira.internal".into(),
                edge_api_target_group: "edge-api".into(),
                edge_poll_target_group: "edge-poll".into(),
            }
            .realize(&listener_placement)
            .expect("listener")
            .resource_type()
            .0,
            alb::LISTENER_TYPE
        );

        assert_eq!(
            ecs_cluster::EcsCluster {
                name: "tokeira".into(),
                service_connect_namespace: "tokeira.internal".into(),
            }
            .realize(&no_deps)
            .expect("cluster")
            .resource_type()
            .0,
            ecs_cluster::CLUSTER_TYPE
        );

        let with_sg = placement(vec![ResourceId("sg-instances".into())]);
        assert_eq!(
            ecs_cluster::LaunchTemplate {
                name: "runtime".into(),
                cluster_name: "tokeira".into(),
                instance_type: "c7g.large".into(),
                workload: "runtime".into(),
                instance_profile_name: "runtime".into(),
            }
            .realize(&with_sg)
            .expect("launch template")
            .resource_type()
            .0,
            ecs_cluster::LAUNCH_TEMPLATE_TYPE
        );

        let asg_placement = placement(vec![
            ResourceId("lt-runtime".into()),
            ResourceId("demo-vpc".into()),
        ]);
        assert_eq!(
            ecs_cluster::AutoScalingGroup {
                name: "runtime".into(),
                min_size: 1,
                desired_capacity: 1,
                max_size: 3,
                new_instances_protected_from_scale_in: true,
            }
            .realize(&asg_placement)
            .expect("asg")
            .resource_type()
            .0,
            ecs_cluster::ASG_TYPE
        );

        let cp_placement = placement(vec![
            ResourceId("ecs:cluster".into()),
            ResourceId("asg-runtime".into()),
        ]);
        assert_eq!(
            ecs_cluster::CapacityProvider {
                name: "runtime".into()
            }
            .realize(&cp_placement)
            .expect("capacity provider")
            .resource_type()
            .0,
            ecs_cluster::CAPACITY_PROVIDER_TYPE
        );

        assert_eq!(
            iam::IamRole {
                region: "eu-west-2".into(),
                name: "task".into(),
                trust_policy: "{}".into(),
                inline_policies: Default::default(),
                dependent_inline_policies: Vec::new(),
                managed_policy_arns: Vec::new(),
            }
            .realize(&no_deps)
            .expect("role")
            .resource_type()
            .0,
            iam::ROLE_TYPE
        );

        let profile_placement = placement(vec![ResourceId("iam-role-instances".into())]);
        assert_eq!(
            iam::IamInstanceProfile {
                region: "eu-west-2".into(),
                profile_name: "instances".into(),
                role_name: "instances".into(),
            }
            .realize(&profile_placement)
            .expect("profile")
            .resource_type()
            .0,
            iam::INSTANCE_PROFILE_TYPE
        );

        assert_eq!(
            ssm_parameter::SsmParameter {
                name: "/tokeira/alloy/runtime".into(),
                value: "config".into(),
                secure: true,
            }
            .realize(&no_deps)
            .expect("parameter")
            .resource_type()
            .0,
            ssm_parameter::TYPE
        );

        let object_placement = placement(vec![ResourceId("s3-demo-artifacts".into())]);
        assert_eq!(
            s3_object::S3Object {
                key: "dashboards/example.json".into(),
                content: "{}".into(),
                content_type: "application/json".into(),
            }
            .realize(&object_placement)
            .expect("object")
            .resource_type()
            .0,
            s3_object::TYPE
        );

        assert_eq!(
            secrets_manager_secret::SecretsManagerSecret {
                region: "eu-west-2".into(),
                name: "tokeira/grafana/admin".into(),
                source: secrets_manager_secret::SecretSource::GeneratedPasswordJson(
                    secrets_manager_secret::GeneratedPassword {
                        username: "admin".into(),
                        password_length: 24,
                    },
                ),
                recovery_window_days: None,
            }
            .realize(&no_deps)
            .expect("secret")
            .resource_type()
            .0,
            secrets_manager_secret::TYPE
        );

        let dsql_placement = placement(vec![
            ResourceId("demo-vpc".into()),
            ResourceId("sg-dsql".into()),
            ResourceId("dsql:cluster".into()),
        ]);
        assert_eq!(
            dsql_connection_endpoint::DsqlConnectionEndpoint {
                region: "eu-west-2".into(),
                identity: "primary".into(),
                id: Some("dsql:connection-endpoint".into()),
            }
            .realize(&dsql_placement)
            .expect("dsql endpoint")
            .resource_type()
            .0,
            dsql_connection_endpoint::TYPE
        );

        assert_eq!(
            cloud_map::CloudMapNamespace {
                name: "tokeira.internal".into(),
            }
            .realize(&with_vpc)
            .expect("cloud map")
            .resource_type()
            .0,
            cloud_map::TYPE
        );
    }

    // The dependency-classification seam refuses by class name when the
    // declared edges cannot satisfy a typed dependency field.
    #[test]
    fn missing_vpc_dependency_is_refused_by_class() {
        use tokeira_platform::kind::Kind as _;

        let error = security_group::SecurityGroup {
            region: "eu-west-2".into(),
            name: "edge".into(),
            description: "edge ingress".into(),
            ingress: Vec::new(),
        }
        .realize(&placement(Vec::new()))
        .expect_err("no vpc declared");
        assert!(error.message.contains("Vpc"), "{}", error.message);
    }
}
