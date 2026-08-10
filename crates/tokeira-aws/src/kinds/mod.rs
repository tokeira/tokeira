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
    error::KindError,
    kind::{self, DecodedKind},
};

use crate::resources::{
    dsql_cluster::DsqlCluster as DsqlClusterResource,
    dynamodb_table::DynamoDbTable as DynamoDbTableResource,
};

pub use dsql_cluster::DsqlCluster;
pub use dynamodb_table::DynamoDbTable;

/// The namespace word: the normalized crate name definitions import from.
pub const NAMESPACE: &str = "tokeira_aws";

/// The provider's author-visible kind names, each the word its resource
/// owns.
pub const KINDS: &[&str] = &[DsqlClusterResource::TYPE, DynamoDbTableResource::TYPE];

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
