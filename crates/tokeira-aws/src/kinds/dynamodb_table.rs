//! Typed author input for a DynamoDB table resource.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::dynamodb_table::{
    AttributeType, BillingMode, DynamoDbTable as Resource, KeyAttribute, KeyType,
};

/// Reusable author input for a single-hash-key on-demand DynamoDB table.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamoDbTable {
    /// Full table name.
    pub table: String,
    /// AWS region.
    pub region: String,
    /// String hash-key attribute.
    pub hash_key: String,
    /// Optional TTL attribute.
    pub ttl: Option<String>,
}

impl Kind<Resource> for DynamoDbTable {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        Ok(Resource {
            table_name: self.table.clone(),
            key_schema: vec![KeyAttribute {
                name: self.hash_key.clone(),
                key_type: KeyType::Hash,
                attribute_type: AttributeType::String,
            }],
            billing_mode: BillingMode::OnDemand,
            ttl_attribute: self.ttl.clone(),
            module: placement.module.clone(),
            project: placement.deployment_id.clone(),
            region: self.region.clone(),
            tags: placement.tags.clone().into_iter().collect(),
        })
    }
}
