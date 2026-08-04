use std::{collections::HashMap, time::Duration};

use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource, ResourceId,
    ResourceState, ResourceType, Reversibility, SemanticsContext, error::IacError,
};

/// Configuration for a single Secrets Manager secret provider resource.
#[derive(Debug)]
pub struct SecretsManagerSecretConfig {
    pub value: SecretValue,
    pub recovery_window_days: Option<i64>,
    pub module: String,
}

/// First-apply secret material source.
///
/// Generated values are intentionally not recomputed by `update()`: apply must
/// stay idempotent, and credential rotation is an operator action with its own
/// audit trail.
#[derive(Debug)]
pub enum SecretValue {
    Static(String),
    GeneratedPasswordJson {
        username: String,
        password_length: i32,
    },
}

/// Generic provider resource that provisions exactly one Secrets Manager secret.
#[derive(Debug)]
pub struct SecretsManagerSecret {
    pub secret_name: String,
    pub config: SecretsManagerSecretConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl SecretsManagerSecret {
    pub fn new(
        secret_name: String,
        config: SecretsManagerSecretConfig,
        rctx: &crate::ResourceContext,
    ) -> Self {
        Self {
            secret_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    async fn initial_secret_string(&self, ctx: &ProvisionContext) -> Result<String, IacError> {
        match &self.config.value {
            SecretValue::Static(value) => Ok(value.clone()),
            SecretValue::GeneratedPasswordJson {
                username,
                password_length,
            } => {
                let output = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .secretsmanager
                    .get_random_password()
                    .password_length((*password_length).into())
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "secretsmanager:GetRandomPassword: {}",
                            e.into_service_error()
                        ))
                    })?;
                Ok(serde_json::json!({
                    "username": username,
                    "password": output.random_password().unwrap_or_default(),
                })
                .to_string())
            }
        }
    }

    async fn wait_until_deleted(&self, ctx: &ProvisionContext) -> Result<(), IacError> {
        let name = self.secret_name.clone();
        let secretsmanager_client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager;
        super::poll_until(
            Duration::from_secs(2),
            Duration::from_secs(120),
            ctx,
            super::PollTarget {
                resource_desc: "Secrets Manager secret deletion",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for secret deletion",
            },
            || async {
                match secretsmanager_client
                    .describe_secret()
                    .secret_id(&name)
                    .send()
                    .await
                {
                    Ok(output) => Ok(output.deleted_date().is_some()),
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        if svc_err.is_resource_not_found_exception() {
                            Ok(true)
                        } else {
                            Err(IacError::AwsSdk(format!(
                                "secretsmanager:DescribeSecret: {svc_err}"
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
impl Resource for SecretsManagerSecret {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — secretsmanager:CreateSecret with the generated initial \
             value (ResourceExists adopted via DescribeSecret)"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op: the live secret value is never \
             overwritten after creation, and `diff` answers NoChange"
        ));
        const DELETE_WINDOWED: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — secretsmanager:DeleteSecret with the configured \
             recovery_window_in_days; secretsmanager:RestoreSecret can cancel \
             the deletion within that window"
        ));
        const DELETE_FORCED: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — secretsmanager:DeleteSecret with \
             force_delete_without_recovery(true): no recovery window, by our \
             own parameter choice"
        ));
        let claims = |operation,
                      data_effect: Confidence<DataEffect>,
                      reversibility: Confidence<Reversibility>,
                      citation: Citation| ChangeSemantics {
            operation: Confidence::EngineFact {
                value: operation,
                citation: citation.clone(),
            },
            replacement: Confidence::EngineFact {
                value: ReplacementPolicy::NotRequired,
                citation: citation.clone(),
            },
            disruption: Confidence::EngineFact {
                value: Disruption::None,
                citation,
            },
            data_effect,
            reversibility,
            statement: None,
            provider_assigned: Vec::new(),
        };
        match ctx.kind {
            ChangeKind::Create => {
                let mut semantics = claims(
                    LifecycleOperation::Created,
                    Confidence::EngineFact {
                        value: DataEffect::NoDataHeld,
                        citation: CREATE,
                    },
                    Confidence::EngineFact {
                        value: Reversibility::Reversible,
                        citation: CREATE,
                    },
                    CREATE,
                );
                // The committed secret version — the identity consumers pin.
                semantics.provider_assigned = vec!["version_id".into()];
                semantics
            }
            ChangeKind::Update | ChangeKind::Replace => claims(
                LifecycleOperation::UpdatedInPlace,
                Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: UPDATE,
                },
                Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: UPDATE,
                },
                UPDATE,
            ),
            // Both delete modes destroy the stored value by our call; how
            // recoverable that is depends on the configured mode. A
            // recreated secret gets a fresh generated value — any value
            // rotated after creation is not reproducible from the
            // definition, hence data loss on the forced path.
            ChangeKind::Delete => {
                if self.config.recovery_window_days.is_some() {
                    let mut semantics = claims(
                        LifecycleOperation::Deleted,
                        Confidence::EngineFact {
                            value: DataEffect::Destroyed,
                            citation: DELETE_WINDOWED,
                        },
                        Confidence::Inference {
                            value: Reversibility::Reversible,
                            citation: DELETE_WINDOWED,
                        },
                        DELETE_WINDOWED,
                    );
                    semantics.statement = Some(std::borrow::Cow::Borrowed(
                        "it would be scheduled for deletion with a recovery window; \
                         restoring within the window cancels the deletion",
                    ));
                    semantics
                } else {
                    claims(
                        LifecycleOperation::Deleted,
                        Confidence::EngineFact {
                            value: DataEffect::Destroyed,
                            citation: DELETE_FORCED,
                        },
                        Confidence::Inference {
                            value: Reversibility::ReversibleWithDataLoss,
                            citation: DELETE_FORCED,
                        },
                        DELETE_FORCED,
                    )
                }
            }
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("SecretsManagerSecret")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("secret-{}", self.secret_name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // Secrets are static — no mutable properties to diff.
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let name = &self.secret_name;
        let tags = ctx.resource_tags(name);
        let sm_tag_list = super::secretsmanager_tags(&tags);
        let secret_string = self.initial_secret_string(ctx).await?;

        let secret_arn;
        let version_id;

        // secretsmanager:CreateSecret with secret string and tags
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager
            .create_secret()
            .name(name)
            .secret_string(secret_string)
            .set_tags(Some(sm_tag_list))
            .send()
            .await
        {
            Ok(output) => {
                secret_arn = output.arn().unwrap_or_default().to_string();
                version_id = output.version_id().unwrap_or_default().to_string();
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_exists_exception() {
                    tracing::warn!(secret = %name, "secret already exists, adopting");
                    // secretsmanager:DescribeSecret to get ARN of existing secret
                    let desc = ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .secretsmanager
                        .describe_secret()
                        .secret_id(name)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!(
                                "secretsmanager:DescribeSecret: {}",
                                e.into_service_error()
                            ))
                        })?;
                    secret_arn = desc.arn().unwrap_or_default().to_string();
                    // The adopted secret's current version: the id staged
                    // AWSCURRENT, which is what a pinned consumer must name.
                    version_id = desc
                        .version_ids_to_stages()
                        .and_then(|stages| {
                            stages.iter().find_map(|(id, labels)| {
                                labels
                                    .iter()
                                    .any(|label| label == "AWSCURRENT")
                                    .then(|| id.clone())
                            })
                        })
                        .unwrap_or_default();
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "secretsmanager:CreateSecret: {svc_err}"
                    )));
                }
            }
        }

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("SecretsManagerSecret"),
            physical_id: secret_arn,
            // `version_id` is the committed secret version — the identity a
            // pinned consumer (an ECS task definition's `valueFrom`) names so
            // a content change is a visible, planned redeploy rather than a
            // silent drift between replicas.
            properties: serde_json::json!({
                "secret_name": self.secret_name,
                "tags": tags,
                "version_id": version_id,
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
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        // Secrets are static — update is a no-op that preserves current state.
        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: current.properties.clone(),
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
        let name = &self.secret_name;

        let mut request = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager
            .delete_secret()
            .secret_id(name);
        request = if let Some(days) = self.config.recovery_window_days {
            // Secrets that protect operator access should be recoverable after
            // accidental destroy. Other callers can still opt into force delete.
            request.recovery_window_in_days(days)
        } else {
            request.force_delete_without_recovery(true)
        };

        match request.send().await {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    tracing::warn!(secret = %name, "secret does not exist, skipping deletion");
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "secretsmanager:DeleteSecret: {svc_err}"
                    )));
                }
            }
        }

        self.wait_until_deleted(ctx).await
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let name = &self.secret_name;

        // secretsmanager:DescribeSecret for this single secret
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .secretsmanager
            .describe_secret()
            .secret_id(name)
            .send()
            .await
        {
            Ok(output) => {
                let now = chrono::Utc::now().to_rfc3339();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("SecretsManagerSecret"),
                    physical_id: output.arn().unwrap_or_default().to_string(),
                    properties: serde_json::json!({
                        "secret_name": self.secret_name,
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
                        "secretsmanager:DescribeSecret: {svc_err}"
                    )))
                }
            }
        }
    }
}
