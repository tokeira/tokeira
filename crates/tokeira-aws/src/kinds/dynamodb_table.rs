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

impl DynamoDbTable {
    fn validate(&self) -> Result<(), KindError> {
        for (field, value) in [
            ("table", self.table.as_str()),
            ("region", self.region.as_str()),
            ("hash_key", self.hash_key.as_str()),
        ] {
            if value.is_empty() {
                return Err(KindError::new(format!(
                    "DynamoDB table {field} cannot be empty"
                )));
            }
        }
        Ok(())
    }
}

impl Kind for DynamoDbTable {
    fn name(&self) -> &'static str {
        Resource::TYPE
    }

    fn validate_input(&self) -> Result<(), KindError> {
        self.validate()
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &["table_name", "table_arn"]
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        serde_json::json!({
            "table": self.table,
            "region": self.region,
            "hash_key": self.hash_key,
            "ttl": self.ttl,
            "module": placement.module,
        })
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        self.validate()?;
        Ok(Box::new(Resource {
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
        }))
    }
}
