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
//! surface stays within this module.

pub mod dsql_cluster;
pub mod dynamodb_table;
pub mod ecr_repository;
pub mod s3_bucket;
pub mod security_group;
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
    dynamodb_table::DynamoDbTable as DynamoDbTableResource, ecr_repository::EcrRepository,
    s3_bucket::S3Bucket, security_group::SecurityGroup, vpc::VpcResource,
    vpc_endpoint::VpcEndpoint,
};

pub use dsql_cluster::DsqlCluster;
pub use dynamodb_table::DynamoDbTable;

/// The namespace word: the normalized crate name definitions import from.
pub const NAMESPACE: &str = "tokeira_aws";

/// The provider's author-visible kind names, each the word its resource
/// owns.
pub const KINDS: &[&str] = &[
    DsqlClusterResource::TYPE,
    DynamoDbTableResource::TYPE,
    ecr_repository::TYPE,
    s3_bucket::TYPE,
    security_group::TYPE,
    vpc::TYPE,
    vpc_endpoint::TYPE,
];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    Some(match name {
        DsqlClusterResource::TYPE => kind::decode_resource::<DsqlCluster, DsqlClusterResource>(
            DsqlClusterResource::TYPE,
            value,
        ),
        DynamoDbTableResource::TYPE => {
            kind::decode_resource::<DynamoDbTable, DynamoDbTableResource>(
                DynamoDbTableResource::TYPE,
                value,
            )
        }
        ecr_repository::TYPE => {
            kind::decode_resource::<ecr_repository::EcrRepository, EcrRepository>(
                ecr_repository::TYPE,
                value,
            )
        }
        s3_bucket::TYPE => {
            kind::decode_resource::<s3_bucket::S3Bucket, S3Bucket>(s3_bucket::TYPE, value)
        }
        security_group::TYPE => {
            kind::decode_resource::<security_group::SecurityGroup, SecurityGroup>(
                security_group::TYPE,
                value,
            )
        }
        vpc::TYPE => kind::decode_resource::<vpc::Vpc, VpcResource>(vpc::TYPE, value),
        vpc_endpoint::TYPE => kind::decode_resource::<vpc_endpoint::VpcEndpoint, VpcEndpoint>(
            vpc_endpoint::TYPE,
            value,
        ),
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
                "DsqlCluster",
                "DynamoDbTable",
                "EcrRepository",
                "S3Bucket",
                "SecurityGroup",
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
                },)
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

        let vpc = vpc::Vpc {
            region: "eu-west-2".into(),
            cidr: "10.0.0.0/16".into(),
            availability_zones: vec!["eu-west-2a".into()],
        }
        .realize(&placement(Vec::new()))
        .expect("vpc realizes");
        assert_eq!(vpc.resource_type().0, vpc::TYPE);

        let with_vpc = placement(vec![ResourceId("demo-vpc".into())]);
        let group = security_group::SecurityGroup {
            region: "eu-west-2".into(),
            name: "edge".into(),
            description: "edge ingress".into(),
            ingress: Vec::new(),
        }
        .realize(&with_vpc)
        .expect("security group realizes");
        assert_eq!(group.resource_type().0, security_group::TYPE);

        let endpoint = vpc_endpoint::VpcEndpoint {
            region: "eu-west-2".into(),
            short_name: "ssm".into(),
            service_name: "com.amazonaws.eu-west-2.ssm".into(),
            endpoint_type: vpc_endpoint::EndpointKind::Interface,
            id: None,
        }
        .realize(&with_vpc)
        .expect("endpoint realizes");
        assert_eq!(endpoint.resource_type().0, vpc_endpoint::TYPE);

        let bucket = s3_bucket::S3Bucket {
            region: "eu-west-2".into(),
            bucket: "demo-state".into(),
            versioning: true,
            key_prefix: None,
        }
        .realize(&placement(Vec::new()))
        .expect("bucket realizes");
        assert_eq!(bucket.resource_type().0, s3_bucket::TYPE);

        let repository = ecr_repository::EcrRepository {
            repository: "tokeira/tokeirad".into(),
        }
        .realize(&placement(Vec::new()))
        .expect("repository realizes");
        assert_eq!(repository.resource_type().0, ecr_repository::TYPE);
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
