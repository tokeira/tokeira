//! Reusable, typed author inputs for AWS resources.
//!
//! This module is the AWS provider's kind export. Platforms select kinds by
//! type at their entry point — [`all`] for the complete export, or
//! [`select`] over `kind::<DsqlCluster>()`-style entries — and the selection
//! is the whole wiring act: selected kinds are authorable and executable,
//! unselected kinds are unknown at `definition check`, and a selection typo
//! is a compile error.

pub mod dsql_cluster;
pub mod dynamodb_table;

use tokeira_platform::declaration::{
    AuthorableKind, DeploymentRef, InfraConstructor, KindEntry, KindSet, kind,
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

impl AuthorableKind for DsqlCluster {
    const NAME: &'static str = "DsqlCluster";
}

impl AuthorableKind for DynamoDbTable {
    const NAME: &'static str = "DynamoDbTable";
}

/// The complete AWS kind selection, for a platform entry point that tracks
/// the provider: new AWS kinds become authorable without a platform edit.
pub fn all() -> KindSet {
    select(vec![kind::<DsqlCluster>(), kind::<DynamoDbTable>()])
}

/// A typed subset of the AWS kind export, for a platform entry point that
/// states its vocabulary exactly and grows it only on purpose:
/// `select(vec![kind::<DsqlCluster>()])`.
pub fn select(entries: Vec<KindEntry>) -> KindSet {
    KindSet::new("aws", entries).infra(AwsInfraConstructor)
}

#[cfg(test)]
mod tests {
    use tokeira_platform::author::LocatedValue;

    use super::*;

    // A selection carries exactly the requested kind types, named by the
    // types themselves; decode admits each entry's own input shape.
    #[test]
    fn selection_is_typed_and_decodes_each_entry() {
        let selection = all();
        let names: Vec<&str> = selection.entries.iter().map(|entry| entry.name).collect();
        assert_eq!(names, ["DsqlCluster", "DynamoDbTable"]);
        for entry in &selection.entries {
            let probe = (entry.decode)(LocatedValue::new(
                tokeira_platform::author::ValueShape::Struct {
                    name: entry.name.to_string(),
                    fields: Vec::new(),
                },
            ));
            if let Err(error) = probe {
                assert!(
                    !error.message.contains("unknown"),
                    "entry `{}` failed as unknown: {}",
                    entry.name,
                    error.message
                );
            }
        }
    }
}
