//! Reusable, typed author inputs for AWS resources.
//!
//! This module is the AWS provider's complete kind export: every authorable
//! AWS capability appears in [`KIND_NAMES`] and decodes through [`decode`].
//! The engine kind library aggregates provider exports verbatim — no platform
//! curates below this set, so a definition edited within one engine version
//! can adopt any kind the provider ships.

pub mod dsql_cluster;
pub mod dynamodb_table;

use tokeira_platform::{
    author::{LocatedValue, from_located_value},
    error::KindError,
    kind::{PlacementContext, ProviderKind},
};

/// Complete author-visible AWS kind names, in stable order.
pub const KIND_NAMES: &[&str] = &["DsqlCluster", "DynamoDbTable"];

/// The AWS provider's closed kind set.
#[derive(Debug, Clone, PartialEq)]
pub enum AwsKind {
    /// Aurora DSQL cluster.
    DsqlCluster(dsql_cluster::DsqlCluster),
    /// DynamoDB table.
    DynamoDbTable(dynamodb_table::DynamoDbTable),
}

macro_rules! delegate_kind {
    ($self:ident, $method:ident $(, $argument:expr)?) => {
        match $self {
            Self::DsqlCluster(kind) => kind.$method($($argument)?),
            Self::DynamoDbTable(kind) => kind.$method($($argument)?),
        }
    };
}

impl ProviderKind for AwsKind {
    fn kind_name(&self) -> &'static str {
        delegate_kind!(self, kind_name)
    }

    fn validate_input(&self) -> Result<(), KindError> {
        delegate_kind!(self, validate_input)
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        delegate_kind!(self, declared_outputs)
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        delegate_kind!(self, desired_manifest, placement)
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        delegate_kind!(self, realize, placement)
    }
}

/// Decode one named AWS kind from a host-free author value.
pub fn decode(name: &str, value: LocatedValue) -> Result<AwsKind, KindError> {
    let range = value.range;
    match name {
        "DsqlCluster" => from_located_value::<dsql_cluster::DsqlCluster>(value)
            .map(AwsKind::DsqlCluster)
            .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range))),
        "DynamoDbTable" => from_located_value::<dynamodb_table::DynamoDbTable>(value)
            .map(AwsKind::DynamoDbTable)
            .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range))),
        _ => Err(KindError::new(format!("unknown AWS kind `{name}`"))),
    }
}

/// Provider-owned `<Kind>::EMPTY` defaults; no AWS kind declares any.
pub fn defaults(_name: &str) -> Option<LocatedValue> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // The inventory is the provider's single kind authority: every listed
    // name reaches a decode arm (never the unknown-kind arm), and unlisted
    // names never decode.
    #[test]
    fn inventory_matches_decode_arms_exactly() {
        for name in KIND_NAMES {
            let probe = decode(
                name,
                LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
                    name: (*name).to_string(),
                    fields: Vec::new(),
                }),
            );
            if let Err(error) = probe {
                assert!(
                    !error.message.contains("unknown"),
                    "inventory name `{name}` hit the unknown-kind arm: {}",
                    error.message
                );
            }
        }
        assert!(
            decode(
                "NotAnAwsKind",
                LocatedValue::new(tokeira_platform::author::ValueShape::Unit)
            )
            .expect_err("unknown kind must not decode")
            .message
            .contains("unknown")
        );
    }
}
