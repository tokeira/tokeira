use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource, ResourceId,
    ResourceState, ResourceType, Reversibility, SemanticsContext, error::IacError,
};

/// DynamoDB key type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyType {
    Hash,
    Range,
}

/// DynamoDB scalar attribute type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeType {
    String,
    Number,
    Binary,
}

/// DynamoDB billing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BillingMode {
    OnDemand,
    Provisioned,
}

/// A single key attribute in the table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyAttribute {
    pub name: String,
    pub key_type: KeyType,
    pub attribute_type: AttributeType,
}

/// Provider resource that provisions exactly one DynamoDB table. Its
/// authored face is the separate [`crate::kinds::DynamoDbTable`] kind,
/// which realizes this resource directly — the table's facts are flat
/// fields here; no configuration middle struct exists.
#[derive(Debug)]
pub struct DynamoDbTable {
    pub table_name: String,
    pub key_schema: Vec<KeyAttribute>,
    pub billing_mode: BillingMode,
    pub ttl_attribute: Option<String>,
    pub module: String,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl DynamoDbTable {
    /// The resource's one word: engine resource type and author-visible
    /// name, stated once here.
    pub const TYPE: &'static str = "DynamoDbTable";

    fn sdk_key_type(kt: KeyType) -> aws_sdk_dynamodb::types::KeyType {
        match kt {
            KeyType::Hash => aws_sdk_dynamodb::types::KeyType::Hash,
            KeyType::Range => aws_sdk_dynamodb::types::KeyType::Range,
        }
    }

    fn sdk_attribute_type(at: AttributeType) -> aws_sdk_dynamodb::types::ScalarAttributeType {
        match at {
            AttributeType::String => aws_sdk_dynamodb::types::ScalarAttributeType::S,
            AttributeType::Number => aws_sdk_dynamodb::types::ScalarAttributeType::N,
            AttributeType::Binary => aws_sdk_dynamodb::types::ScalarAttributeType::B,
        }
    }

    fn sdk_billing_mode(bm: BillingMode) -> aws_sdk_dynamodb::types::BillingMode {
        match bm {
            BillingMode::OnDemand => aws_sdk_dynamodb::types::BillingMode::PayPerRequest,
            BillingMode::Provisioned => aws_sdk_dynamodb::types::BillingMode::Provisioned,
        }
    }

    fn desired_tags(&self) -> HashMap<String, String> {
        let mut tags = self.tags.clone();
        tags.insert("Name".into(), self.table_name.clone());
        tags.insert("Project".into(), self.project.clone());
        tags.insert("ManagedBy".into(), "tokeira-cli".into());
        tags
    }

    fn client(&self, ctx: &ProvisionContext) -> aws_sdk_dynamodb::Client {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .dynamodb_for(&self.region)
    }

    async fn wait_until_active(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        let table_name = self.table_name.clone();
        let dynamodb_client = self.client(ctx);
        super::poll_until(
            Duration::from_secs(5),
            Duration::from_secs(300),
            ctx,
            super::PollTarget {
                resource_desc: "DynamoDB table",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for table to become ACTIVE",
            },
            || async {
                let output = dynamodb_client
                    .describe_table()
                    .table_name(&table_name)
                    .send()
                    .await
                    .map_err(|e| {
                        let svc_err = e.into_service_error();
                        if svc_err.is_resource_not_found_exception() {
                            IacError::ResourceCreationFailed {
                                resource_type: "DynamoDB table".into(),
                                resource_id: table_name.clone(),
                                details: "table is not yet describable".into(),
                            }
                        } else {
                            IacError::AwsSdk(format!("dynamodb:DescribeTable: {svc_err}"))
                        }
                    })?;

                let status = output
                    .table()
                    .and_then(|table| table.table_status())
                    .map(|status| status.as_str())
                    .unwrap_or_default();
                Ok(status == "ACTIVE")
            },
        )
        .await
    }

    async fn wait_for_ttl_reflection(
        &self,
        ctx: &ProvisionContext,
        desired_attribute: Option<&str>,
        enable: bool,
    ) -> Result<(), IacError> {
        let table_name = self.table_name.clone();
        let desired_attribute = desired_attribute.map(str::to_string);
        let dynamodb_client = self.client(ctx);
        let phase = if enable {
            "waiting for TTL enablement to be reflected"
        } else {
            "waiting for TTL disablement to be reflected"
        };

        super::poll_until(
            Duration::from_secs(5),
            Duration::from_secs(300),
            ctx,
            super::PollTarget {
                resource_desc: "DynamoDB TTL configuration",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase,
            },
            || async {
                let output = dynamodb_client
                    .describe_time_to_live()
                    .table_name(&table_name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "dynamodb:DescribeTimeToLive: {}",
                            e.into_service_error()
                        ))
                    })?;

                let ttl = output.time_to_live_description();
                let current_attr = ttl.and_then(|ttl| ttl.attribute_name()).unwrap_or_default();
                let current_status = ttl
                    .and_then(|ttl| ttl.time_to_live_status())
                    .map(|status| status.as_str())
                    .unwrap_or("DISABLED");

                if enable {
                    Ok(desired_attribute.as_deref() == Some(current_attr)
                        && matches!(current_status, "ENABLING" | "ENABLED"))
                } else {
                    Ok(matches!(current_status, "DISABLING" | "DISABLED"))
                }
            },
        )
        .await
    }

    async fn wait_until_deleted(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        let table_name = self.table_name.clone();
        let dynamodb_client = self.client(ctx);
        super::poll_until(
            Duration::from_secs(5),
            Duration::from_secs(300),
            ctx,
            super::PollTarget {
                resource_desc: "DynamoDB table deletion",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for table deletion",
            },
            || async {
                match dynamodb_client
                    .describe_table()
                    .table_name(&table_name)
                    .send()
                    .await
                {
                    Ok(_) => Ok(false),
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        if svc_err.is_resource_not_found_exception() {
                            Ok(true)
                        } else {
                            Err(IacError::AwsSdk(format!(
                                "dynamodb:DescribeTable: {svc_err}"
                            )))
                        }
                    }
                }
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Resource for DynamoDbTable {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new(Self::TYPE)
    }

    fn validate_input(&self) -> Result<(), String> {
        let hash_key = self
            .key_schema
            .iter()
            .find(|attribute| attribute.key_type == KeyType::Hash)
            .map(|attribute| attribute.name.as_str())
            .unwrap_or_default();
        for (field, value) in [
            ("table", self.table_name.as_str()),
            ("region", self.region.as_str()),
            ("hash_key", hash_key),
        ] {
            if value.is_empty() {
                return Err(format!("DynamoDB table {field} cannot be empty"));
            }
        }
        Ok(())
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &["table_name", "table_arn"]
    }

    fn desired_manifest(&self) -> serde_json::Value {
        let hash_key = self
            .key_schema
            .iter()
            .find(|attribute| attribute.key_type == KeyType::Hash)
            .map(|attribute| attribute.name.as_str());
        serde_json::json!({
            "table": self.table_name,
            "region": self.region,
            "hash_key": hash_key,
            "ttl": self.ttl_attribute,
            "module": self.module,
        })
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("dynamodb-{}", self.table_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("DynamoDB table")
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_tags: HashMap<String, String> = current
            .properties
            .get("tags")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let current_ttl_attribute = current
            .properties
            .get("ttl_attribute")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if current_tags != self.desired_tags() {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("tags changed")],
            }
        } else if current_ttl_attribute != self.ttl_attribute {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation("ttl attribute changed")],
            }
        } else {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }

    /// What a DynamoDB-table change does. The engine-side facts come from
    /// this file's own paths (update issues control-plane calls only; there
    /// is no replace path — the diff never produces one). The provider-side
    /// facts carry AWS's own words: DeleteTable "deletes a table and all of
    /// its items", and recoverability is a provider guarantee grounded in the
    /// PITR model — the system backup exists only when PITR is enabled, this
    /// engine's create leaves PITR at its documented DISABLED default, and a
    /// restore in any case "always restores to a new table" (researched
    /// 2026-07-29). A TTL-bearing update enacts a data policy — the general
    /// `DataEffect::Policy` with the statement carrying the specific
    /// meaning. The diff covers tags and TTL today; DynamoDB's wider update
    /// surface (billing mode, provisioned throughput, streams, table class,
    /// SSE, deletion protection) joins these branches as the kind grows —
    /// settings changes declare in-place/`Preserved`, data-affecting
    /// policies declare `Policy`, each with its citation.
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        // Cited by module identity, never repo layout; every name is a real
        // identifier in this module. The provider claims cite AWS's own
        // pages, quoted.
        const CREATE_PATH: Citation = Citation::code(concat!(
            module_path!(),
            "::create — dynamodb:CreateTable, then wait_until_active"
        ));
        const UPDATE_PATH: Citation = Citation::code(concat!(
            module_path!(),
            "::update — dynamodb:TagResource and dynamodb:UpdateTimeToLive only; \
             no data-plane operation"
        ));
        const DELETE_TABLE_DOC: Citation = Citation::doc_quoted(
            "DeleteTable — Amazon DynamoDB API Reference",
            "https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html",
            "The DeleteTable operation deletes a table and all of its items.",
        );
        const TTL_DOC: Citation = Citation::doc_quoted(
            "Using time to live (TTL) in DynamoDB",
            "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html",
            "DynamoDB automatically deletes expired items within a few days of their \
             expiration time, without consuming write throughput.",
        );
        const PITR_DOC: Citation = Citation::doc_quoted(
            "Enable point-in-time recovery in DynamoDB",
            "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery_Howitworks.html",
            "When you delete a table that has point-in-time recovery enabled, \
             DynamoDB automatically creates a backup snapshot called a system \
             backup and retains it for 35 days",
        );
        match ctx.kind {
            ChangeKind::Create => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Created,
                    citation: CREATE_PATH,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: CREATE_PATH,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: CREATE_PATH,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: CREATE_PATH,
                },
                // Reversing a create is DeleteTable — anything written since
                // goes with the table, in AWS's own words.
                reversibility: Confidence::ProviderGuarantee {
                    value: Reversibility::ReversibleWithDataLoss,
                    citation: DELETE_TABLE_DOC,
                },
                statement: None,
                // dynamodb:CreateTable assigns the ARN; recorded as the
                // physical id post-create (see `create`).
                provider_assigned: vec!["table_arn".into()],
            },
            ChangeKind::Update => {
                // The only updates this kind's diff produces are tags and the
                // TTL attribute. A TTL change puts item expiry in play, and
                // what expires depends on per-item values no declaration can
                // know — those fields stay Unknown by design.
                let ttl_involved = ctx.field_diffs.iter().any(|d| d.field.contains("ttl"));
                ChangeSemantics {
                    operation: Confidence::EngineFact {
                        value: LifecycleOperation::UpdatedInPlace,
                        citation: UPDATE_PATH,
                    },
                    replacement: Confidence::EngineFact {
                        value: ReplacementPolicy::NotRequired,
                        citation: UPDATE_PATH,
                    },
                    // Inference, not fact: the engine fact is only that
                    // control-plane calls are issued — whether the provider
                    // disrupts on them is not established by the pages
                    // fetched. Ledgered: fetch the TagResource /
                    // UpdateTimeToLive docs and upgrade to ProviderGuarantee
                    // if they establish it.
                    disruption: Confidence::Inference {
                        value: Disruption::None,
                        citation: UPDATE_PATH,
                    },
                    // A TTL change enacts a data policy: the apply deletes
                    // nothing, and thereafter the provider deletes expired
                    // items on its own schedule (the general `Policy` value with the
                    // statement carrying the specific meaning).
                    data_effect: if ttl_involved {
                        Confidence::ProviderGuarantee {
                            value: DataEffect::Policy,
                            citation: TTL_DOC,
                        }
                    } else {
                        Confidence::EngineFact {
                            value: DataEffect::Preserved,
                            citation: UPDATE_PATH,
                        }
                    },
                    // Disabling TTL stops future expiry; items already
                    // expired and deleted do not return — derived from the
                    // documented model.
                    reversibility: if ttl_involved {
                        Confidence::Inference {
                            value: Reversibility::ReversibleWithDataLoss,
                            citation: TTL_DOC,
                        }
                    } else {
                        Confidence::EngineFact {
                            value: Reversibility::Reversible,
                            citation: UPDATE_PATH,
                        }
                    },
                    statement: if ttl_involved {
                        Some(std::borrow::Cow::Borrowed(
                            "items past their declared expiry would be deleted, on the \
                             provider's schedule",
                        ))
                    } else {
                        None
                    },
                    provider_assigned: Vec::new(),
                }
            }
            ChangeKind::Delete => ChangeSemantics {
                operation: Confidence::ProviderGuarantee {
                    value: LifecycleOperation::Deleted,
                    citation: DELETE_TABLE_DOC,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: DELETE_TABLE_DOC,
                },
                disruption: Confidence::ProviderGuarantee {
                    value: Disruption::UnavailableDuringChange,
                    citation: DELETE_TABLE_DOC,
                },
                data_effect: Confidence::ProviderGuarantee {
                    value: DataEffect::Destroyed,
                    citation: DELETE_TABLE_DOC,
                },
                // The system backup that would allow recovery exists only
                // when PITR is enabled; this engine's create leaves PITR at
                // its documented DISABLED default, and a restore always
                // creates a new table.
                reversibility: Confidence::ProviderGuarantee {
                    value: Reversibility::Irreversible,
                    citation: PITR_DOC,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            // The diff never produces a replacement; reported inapplicable
            // via the all-Unknown default, kept total.
            ChangeKind::Replace | ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.table_name;
        let tags = ctx.resource_tags(name);

        let key_schema: Vec<aws_sdk_dynamodb::types::KeySchemaElement> = self
            .key_schema
            .iter()
            .map(|ka| {
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name(&ka.name)
                    .key_type(Self::sdk_key_type(ka.key_type))
                    .build()
                    .map_err(|e| IacError::AwsSdk(format!("dynamodb:CreateTable key_schema: {e}")))
            })
            .collect::<Result<_, _>>()?;

        let attribute_defs: Vec<aws_sdk_dynamodb::types::AttributeDefinition> = self
            .key_schema
            .iter()
            .map(|ka| {
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name(&ka.name)
                    .attribute_type(Self::sdk_attribute_type(ka.attribute_type))
                    .build()
                    .map_err(|e| {
                        IacError::AwsSdk(format!("dynamodb:CreateTable attribute_definitions: {e}"))
                    })
            })
            .collect::<Result<_, _>>()?;

        // dynamodb:CreateTable
        let adopted_existing = match self
            .client(ctx)
            .create_table()
            .table_name(name)
            .set_key_schema(Some(key_schema))
            .set_attribute_definitions(Some(attribute_defs))
            .billing_mode(Self::sdk_billing_mode(self.billing_mode))
            .send()
            .await
        {
            Ok(_) => false,
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_in_use_exception() {
                    tracing::warn!(table = %name, "table already exists, adopting");
                    true
                } else {
                    return Err(IacError::AwsSdk(format!("dynamodb:CreateTable: {svc_err}")));
                }
            }
        };

        self.wait_until_active(ctx).await?;

        let table_arn = self
            .client(ctx)
            .describe_table()
            .table_name(name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "dynamodb:DescribeTable: {}",
                    e.into_service_error()
                ))
            })?
            .table()
            .and_then(|t| t.table_arn())
            .unwrap_or_default()
            .to_string();

        // dynamodb:UpdateTimeToLive if TTL configured.
        // For adopted tables, call DescribeTimeToLive first and only issue an
        // update when TTL is not already enabled on the desired attribute.
        if let Some(ttl_attr) = &self.ttl_attribute {
            let mut should_update_ttl = true;
            if adopted_existing {
                let ttl_desc = self
                    .client(ctx)
                    .describe_time_to_live()
                    .table_name(name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "dynamodb:DescribeTimeToLive: {}",
                            e.into_service_error()
                        ))
                    })?;

                if let Some(ttl) = ttl_desc.time_to_live_description() {
                    let current_attr = ttl.attribute_name().unwrap_or_default();
                    let current_status = ttl
                        .time_to_live_status()
                        .map(|s| s.as_str())
                        .unwrap_or_default();

                    let enabled_for_attr = current_attr == ttl_attr
                        && (current_status == "ENABLED" || current_status == "ENABLING");
                    should_update_ttl = !enabled_for_attr;
                }
            }

            if should_update_ttl {
                let ttl_spec = aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                    .attribute_name(ttl_attr)
                    .enabled(true)
                    .build()
                    .map_err(|e| {
                        IacError::AwsSdk(format!("dynamodb:UpdateTimeToLive build: {e}"))
                    })?;
                self.client(ctx)
                    .update_time_to_live()
                    .table_name(name)
                    .time_to_live_specification(ttl_spec)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "dynamodb:UpdateTimeToLive: {}",
                            e.into_service_error()
                        ))
                    })?;
                self.wait_for_ttl_reflection(ctx, Some(ttl_attr), true)
                    .await?;
            }
        }

        // dynamodb:TagResource
        let ddb_tags = super::dynamodb_tags(&tags);
        if !table_arn.is_empty() {
            self.client(ctx)
                .tag_resource()
                .resource_arn(&table_arn)
                .set_tags(Some(ddb_tags))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("dynamodb:TagResource: {}", e.into_service_error()))
                })?;
        }

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("DynamoDbTable"),
            physical_id: table_arn,
            properties: serde_json::json!({
                "table_name": self.table_name,
                "billing_mode": format!("{:?}", self.billing_mode),
                "ttl_attribute": self.ttl_attribute,
                "tags": tags,
            }),
            dependencies: vec![],
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        })
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&self.table_name);
        let ddb_tags = super::dynamodb_tags(&tags);

        // dynamodb:TagResource to update tags
        if !current.physical_id.is_empty() {
            self.client(ctx)
                .tag_resource()
                .resource_arn(&current.physical_id)
                .set_tags(Some(ddb_tags))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("dynamodb:TagResource: {}", e.into_service_error()))
                })?;
        }

        let current_ttl_attribute = current
            .properties
            .get("ttl_attribute")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if current_ttl_attribute != self.ttl_attribute {
            let ttl_spec = match self.ttl_attribute.as_deref() {
                Some(attr) => aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                    .attribute_name(attr)
                    .enabled(true)
                    .build()
                    .map_err(|e| {
                        IacError::AwsSdk(format!("dynamodb:UpdateTimeToLive build: {e}"))
                    })?,
                None => {
                    let disable_attr = current_ttl_attribute.as_deref().unwrap_or("ttl_epoch");
                    aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                        .attribute_name(disable_attr)
                        .enabled(false)
                        .build()
                        .map_err(|e| {
                            IacError::AwsSdk(format!("dynamodb:UpdateTimeToLive build: {e}"))
                        })?
                }
            };

            self.client(ctx)
                .update_time_to_live()
                .table_name(&self.table_name)
                .time_to_live_specification(ttl_spec)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "dynamodb:UpdateTimeToLive: {}",
                        e.into_service_error()
                    ))
                })?;

            self.wait_for_ttl_reflection(
                ctx,
                self.ttl_attribute.as_deref(),
                self.ttl_attribute.is_some(),
            )
            .await?;
        }

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "table_name": self.table_name,
                "billing_mode": format!("{:?}", self.billing_mode),
                "ttl_attribute": self.ttl_attribute,
                "tags": tags,
            }),
            dependencies: vec![],
            created_at: current.created_at.clone(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        })
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let name = &self.table_name;

        match self
            .client(ctx)
            .delete_table()
            .table_name(name)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    tracing::warn!(table = %name, "table not found, skipping deletion");
                } else {
                    return Err(IacError::AwsSdk(format!("dynamodb:DeleteTable: {svc_err}")));
                }
            }
        }

        self.wait_until_deleted(ctx).await
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let name = &self.table_name;

        match self
            .client(ctx)
            .describe_table()
            .table_name(name)
            .send()
            .await
        {
            Ok(output) => {
                let arn = output
                    .table()
                    .and_then(|t| t.table_arn())
                    .unwrap_or_default()
                    .to_string();
                let billing_mode = output
                    .table()
                    .and_then(|t| t.billing_mode_summary())
                    .and_then(|s| s.billing_mode())
                    .map(|m| match m {
                        aws_sdk_dynamodb::types::BillingMode::PayPerRequest => "OnDemand",
                        aws_sdk_dynamodb::types::BillingMode::Provisioned => "Provisioned",
                        _ => "",
                    })
                    .unwrap_or_default()
                    .to_string();
                let tags = if arn.is_empty() {
                    HashMap::new()
                } else {
                    self.client(ctx)
                        .list_tags_of_resource()
                        .resource_arn(&arn)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!(
                                "dynamodb:ListTagsOfResource: {}",
                                e.into_service_error()
                            ))
                        })?
                        .tags()
                        .iter()
                        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
                        .collect()
                };
                let ttl_attribute = self
                    .client(ctx)
                    .describe_time_to_live()
                    .table_name(name)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "dynamodb:DescribeTimeToLive: {}",
                            e.into_service_error()
                        ))
                    })?
                    .time_to_live_description()
                    .and_then(|ttl| match ttl.time_to_live_status().map(|s| s.as_str()) {
                        Some("ENABLED") | Some("ENABLING") => {
                            ttl.attribute_name().map(str::to_string)
                        }
                        _ => None,
                    });
                let now = chrono::Utc::now().to_rfc3339();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("DynamoDbTable"),
                    physical_id: arn,
                    properties: serde_json::json!({
                        "table_name": self.table_name,
                        "billing_mode": billing_mode,
                        "ttl_attribute": ttl_attribute,
                        "tags": tags,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "dynamodb:DescribeTable: {svc_err}"
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Golden declarations (operator-explanation Req 4.5): classification
    // and confidence only. A delete carries AWS's own words — the table and
    // all its items are deleted — and its
    // recoverability is a cited provider guarantee (the system backup exists
    // only under PITR, which this engine's create leaves at its documented
    // default). A TTL-bearing update's data effect stays Unknown: per-item
    // expiry is unknowable at declaration time. The diff never produces a
    // replacement, so that scenario is asserted inapplicable.
    #[test]
    fn dynamodb_declarations_cite_aws_and_stay_unknown_at_the_edges() {
        use tokeira_iac::{
            ChangeKind, ChangeSemantics, Confidence, DataEffect, Disruption, LifecycleOperation,
            Reversibility, SemanticsContext,
        };

        let table = DynamoDbTable {
            table_name: "t".into(),
            key_schema: Vec::new(),
            billing_mode: BillingMode::OnDemand,
            ttl_attribute: None,
            module: "dynamo".into(),
            project: "p".into(),
            region: "us-east-1".into(),
            tags: HashMap::new(),
        };
        let declared = |kind: ChangeKind, diffs: &[tokeira_iac::FieldDiff]| {
            table.change_semantics(&SemanticsContext {
                kind,
                current: None,
                field_diffs: diffs,
            })
        };

        let delete = declared(ChangeKind::Delete, &[]);
        assert!(matches!(
            delete.operation,
            Confidence::ProviderGuarantee {
                value: LifecycleOperation::Deleted,
                ..
            }
        ));
        assert!(matches!(
            delete.data_effect,
            Confidence::ProviderGuarantee {
                value: DataEffect::Destroyed,
                ..
            }
        ));
        assert!(matches!(
            delete.reversibility,
            Confidence::ProviderGuarantee {
                value: Reversibility::Irreversible,
                ..
            }
        ));

        let tags_update = declared(
            ChangeKind::Update,
            &[tokeira_iac::FieldDiff::observation("tags changed")],
        );
        assert!(matches!(
            tags_update.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::UpdatedInPlace,
                ..
            }
        ));
        assert!(matches!(
            tags_update.data_effect,
            Confidence::EngineFact {
                value: DataEffect::Preserved,
                ..
            }
        ));
        // Disruption on a control-plane update is an inference, not a fact:
        // the pages fetched do not establish the provider's behaviour.
        assert!(matches!(
            tags_update.disruption,
            Confidence::Inference {
                value: Disruption::None,
                ..
            }
        ));

        // A TTL change enacts a data policy (the resolved 6.7 decision):
        // guaranteed by the provider's documentation, with the statement
        // carrying the specific meaning, and reversal derived as lossy.
        let ttl_update = declared(
            ChangeKind::Update,
            &[tokeira_iac::FieldDiff::observation("ttl attribute changed")],
        );
        assert!(matches!(
            ttl_update.data_effect,
            Confidence::ProviderGuarantee {
                value: DataEffect::Policy,
                ..
            }
        ));
        assert!(matches!(
            ttl_update.reversibility,
            Confidence::Inference {
                value: Reversibility::ReversibleWithDataLoss,
                ..
            }
        ));
        assert!(
            ttl_update.statement.is_some(),
            "the statement speaks the policy"
        );

        let create = declared(ChangeKind::Create, &[]);
        assert!(matches!(
            create.reversibility,
            Confidence::ProviderGuarantee {
                value: Reversibility::ReversibleWithDataLoss,
                ..
            }
        ));

        // Inapplicable scenarios: the diff cannot produce a replacement, and
        // NoChange never declares — both report the all-Unknown default.
        assert_eq!(
            declared(ChangeKind::Replace, &[]),
            ChangeSemantics::default()
        );
        assert_eq!(
            declared(ChangeKind::NoChange, &[]),
            ChangeSemantics::default()
        );
    }
}
