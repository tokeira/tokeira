//! Reusable, typed author inputs for AWS resources.
//!
//! This module is the AWS provider's kind export. Kinds and resources are
//! distinct: each kind here is the authored face of one resource in
//! [`crate::resources`] and realizes it directly. The namespace facts —
//! [`NAMESPACE`], [`KINDS`], [`decode`] — are what the assembled binary
//! lists for the frontend; nothing is registered anywhere else.

pub mod dsql_cluster;
pub mod dynamodb_table;

use tokeira_platform::{
    author::LocatedValue,
    declaration::{DeploymentRef, InfraConstructor},
    error::KindError,
    kind::{self, Kind},
};

use crate::resources::{
    dsql_cluster::DsqlCluster as DsqlClusterResource,
    dynamodb_table::DynamoDbTable as DynamoDbTableResource,
};

pub use dsql_cluster::DsqlCluster;
pub use dynamodb_table::DynamoDbTable;

/// The AWS selection's infra-phase extension constructor: the
/// [`AwsClients`](crate::AwsClients) bundle every AWS resource reads from
/// the provision context.
///
/// The client region resolves by AWS's own precedence over its attribute
/// levels: the deployment-level `aws` block's `region` when authored,
/// otherwise the SDK's ambient default chain. Resource-attached regions
/// live on the resources themselves and are not this constructor's
/// question.
#[derive(Debug)]
pub struct AwsInfraConstructor;

#[async_trait::async_trait]
impl InfraConstructor for AwsInfraConstructor {
    async fn construct(
        &self,
        _deployment: &DeploymentRef,
        attributes: Option<&serde_json::Value>,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        let region = attributes
            .and_then(|block| block.get("region"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(aws_config::Region::new(region));
        }
        let sdk = loader.load().await;
        ctx.set_extension(crate::AwsClients::new(&sdk));
        Ok(())
    }
}

/// The namespace word: the normalized crate name definitions import from.
pub const NAMESPACE: &str = "tokeira_aws";

/// The provider's author-visible kind names, each the word its resource
/// owns.
pub const KINDS: &[&str] = &[DsqlClusterResource::TYPE, DynamoDbTableResource::TYPE];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<Box<dyn Kind>, KindError>> {
    Some(match name {
        DsqlClusterResource::TYPE => kind::decode::<DsqlCluster>(value),
        DynamoDbTableResource::TYPE => kind::decode::<DynamoDbTable>(value),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use tokeira_platform::author::LocatedValue;

    use super::*;

    // The namespace facts hold together: every listed name decodes here
    // (each entry admits its own input shape), and an unknown name is
    // refused as not-ours rather than an error.
    #[test]
    fn every_listed_kind_decodes_and_unknown_names_refuse() {
        assert_eq!(KINDS, ["DsqlCluster", "DynamoDbTable"]);
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
    }
}
