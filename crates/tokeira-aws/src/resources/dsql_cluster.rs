use std::{collections::HashMap, time::Duration};

use aws_sdk_dsql::client::Waiters as DsqlWaiters;
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource, ResourceId,
    ResourceState, ResourceType, Reversibility, SemanticsContext, error::IacError,
};

/// Lifecycle mode for a single DSQL cluster resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DsqlClusterMode {
    /// Provision and manage the DSQL cluster lifecycle via AWS APIs.
    Managed,
    /// Adopt an externally managed cluster endpoint/ARN without create/delete.
    Preexisting,
}

/// Return whether the cluster should be treated as provider-managed for delete.
pub fn effective_managed(config_mode: DsqlClusterMode, state_mode: &str) -> bool {
    config_mode == DsqlClusterMode::Managed || state_mode == "managed"
}

/// Configuration for a single DSQL cluster provider resource.
#[derive(Debug)]
pub struct DsqlClusterConfig {
    pub mode: DsqlClusterMode,
    pub preexisting_endpoint: Option<String>,
    pub preexisting_arn: Option<String>,
    /// Fallback cluster identifier when not available from state.
    /// Extracted from config ARN/endpoint by the host application.
    pub fallback_identifier: Option<String>,
    pub resource_id: Option<ResourceId>,
    pub module: String,
}

/// Generic provider resource that provisions exactly one Aurora DSQL cluster.
#[derive(Debug)]
pub struct DsqlCluster {
    /// Logical identity used in resource ID generation, e.g. `proj-monitored`.
    pub cluster_identity: String,
    pub config: DsqlClusterConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl DsqlCluster {
    /// Construct a DsqlCluster resource from project config and per-instance settings.
    pub fn new(
        cluster_identity: String,
        config: DsqlClusterConfig,
        rctx: &crate::ResourceContext,
    ) -> Self {
        Self {
            cluster_identity,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    fn resolve_identifier(&self, state: Option<&ResourceState>) -> Option<String> {
        state
            .and_then(|s| s.properties.get("cluster_id"))
            .and_then(|v| v.as_str())
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .or_else(|| {
                state
                    .map(|s| s.physical_id.as_str())
                    .filter(|id| !id.is_empty())
                    .map(str::to_string)
            })
            .or_else(|| self.config.fallback_identifier.clone())
    }
}

#[async_trait::async_trait]
impl Resource for DsqlCluster {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("DsqlCluster")
    }

    fn resource_id(&self) -> ResourceId {
        self.config
            .resource_id
            .clone()
            .unwrap_or_else(|| ResourceId(format!("dsql-{}", self.cluster_identity)))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("Aurora DSQL cluster")
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        match self.config.mode {
            DsqlClusterMode::Managed => InternalChange::NoChange {
                resource_id: self.resource_id(),
            },
            DsqlClusterMode::Preexisting => {
                let current_endpoint = current
                    .properties
                    .get("cluster_endpoint")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let current_arn = current
                    .properties
                    .get("cluster_arn")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let desired_endpoint = self
                    .config
                    .preexisting_endpoint
                    .as_deref()
                    .unwrap_or_default();
                let desired_arn = self.config.preexisting_arn.as_deref().unwrap_or_default();

                if current_endpoint != desired_endpoint || current_arn != desired_arn {
                    InternalChange::Update {
                        resource_id: self.resource_id(),
                        resource_type: self.resource_type(),
                        details: vec![tokeira_iac::FieldDiff::observation(
                            "preexisting DSQL endpoint/ARN changed",
                        )],
                    }
                } else {
                    InternalChange::NoChange {
                        resource_id: self.resource_id(),
                    }
                }
            }
        }
    }

    /// What a DSQL-cluster change does, mode-aware: a **preexisting** cluster
    /// is referenced, never managed by the deployment — the provider is never
    /// called, so every declaration is an engine fact about this module's own
    /// restraint. A **managed** cluster is real: its delete disables deletion
    /// protection, then deletes the cluster. Data fate and recoverability are
    /// declared from AWS's documented recovery model — restores require a
    /// pre-existing recovery point and only ever create a *new* cluster — as
    /// a cited inference and a cited provider guarantee respectively.
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        // Cited by module identity, never repo layout — these resources are
        // usable outside this workspace, and their citations must stay true
        // there. Every name below is a real identifier in this module.
        const CREATE_MANAGED: Citation = Citation::code(concat!(
            module_path!(),
            "::create (Managed) — dsql:CreateCluster with \
             deletion_protection_enabled(true), then wait_until_cluster_active"
        ));
        const UPDATE_MANAGED: Citation = Citation::code(concat!(
            module_path!(),
            "::update (Managed) — dsql:TagResource only; no data-plane operation"
        ));
        const DELETE_MANAGED: Citation = Citation::code(concat!(
            module_path!(),
            "::delete (Managed) — dsql:UpdateCluster with \
             deletion_protection_enabled(false), then dsql:DeleteCluster, polling \
             until the cluster is absent from GetCluster and ListClusters"
        ));
        const PREEXISTING: Citation = Citation::code(concat!(
            module_path!(),
            "::{create,update,delete} (Preexisting) — only the recorded \
             cluster_endpoint/cluster_arn properties change; no provider call is \
             made. A preexisting cluster is referenced, never managed by the \
             deployment; the delete retires the record and leaves the cluster \
             running"
        ));
        const RESTORE_DOC: Citation = Citation::doc_quoted(
            "Amazon Aurora DSQL restore",
            "https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-auroradsql.html",
            "AWS Backup creates a new cluster from your snapshots; the restored \
             cluster won't overwrite the source cluster.",
        );
        const BACKUP_DOC: Citation = Citation::doc_quoted(
            "Backup and restore for Amazon Aurora DSQL",
            "https://docs.aws.amazon.com/aurora-dsql/latest/userguide/backup-aurora-dsql.html",
            "When you restore Aurora DSQL clusters, AWS Backup always creates new \
             clusters to preserve your source data.",
        );
        let preexisting = self.config.mode == DsqlClusterMode::Preexisting;
        match ctx.kind {
            ChangeKind::Create if preexisting => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Created,
                    citation: PREEXISTING,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: PREEXISTING,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: PREEXISTING,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: PREEXISTING,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: PREEXISTING,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            ChangeKind::Create => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Created,
                    citation: CREATE_MANAGED,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: CREATE_MANAGED,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: CREATE_MANAGED,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: CREATE_MANAGED,
                },
                // Reversing a managed create means deleting the cluster: the
                // deleted cluster never returns, and data written since the
                // last backup (if any) goes with it — derived from AWS's
                // documented restore model.
                reversibility: Confidence::Inference {
                    value: Reversibility::ReversibleWithDataLoss,
                    citation: RESTORE_DOC,
                },
                statement: None,
                // dsql:CreateCluster assigns all three; the recorded state
                // holds them post-create (see `create`, Managed arm).
                provider_assigned: vec![
                    "cluster_id".into(),
                    "cluster_endpoint".into(),
                    "cluster_arn".into(),
                ],
            },
            ChangeKind::Update | ChangeKind::Replace if preexisting => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::UpdatedInPlace,
                    citation: PREEXISTING,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: PREEXISTING,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: PREEXISTING,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: PREEXISTING,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: PREEXISTING,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            ChangeKind::Update | ChangeKind::Replace => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::UpdatedInPlace,
                    citation: UPDATE_MANAGED,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: UPDATE_MANAGED,
                },
                // Inference, not fact: the engine fact is only that
                // control-plane calls are issued — whether the provider
                // disrupts on them is not established by the pages fetched.
                // Ledgered: fetch the TagResource docs and upgrade to
                // ProviderGuarantee if they establish it.
                disruption: Confidence::Inference {
                    value: Disruption::None,
                    citation: UPDATE_MANAGED,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: UPDATE_MANAGED,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: UPDATE_MANAGED,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            ChangeKind::Delete if preexisting => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: PREEXISTING,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: PREEXISTING,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: PREEXISTING,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: PREEXISTING,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: PREEXISTING,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            ChangeKind::Delete => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: DELETE_MANAGED,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: DELETE_MANAGED,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: DELETE_MANAGED,
                },
                // Derived, and owned as derived: the documented recovery
                // model's only path is restore-from-a-prior-recovery-point
                // into a NEW cluster, so data not backed up before the
                // delete has no documented recovery.
                data_effect: Confidence::Inference {
                    value: DataEffect::Destroyed,
                    citation: BACKUP_DOC,
                },
                reversibility: Confidence::ProviderGuarantee {
                    value: Reversibility::Irreversible,
                    citation: RESTORE_DOC,
                },
                statement: Some(std::borrow::Cow::Borrowed(
                    "deletion protection would be disabled first, then the cluster deleted",
                )),
                provider_assigned: Vec::new(),
            },
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("dsql-{}", self.cluster_identity));

        match self.config.mode {
            DsqlClusterMode::Preexisting => {
                let endpoint = self.config.preexisting_endpoint.clone().unwrap_or_default();
                let arn = self.config.preexisting_arn.clone().unwrap_or_default();

                if endpoint.is_empty() && arn.is_empty() {
                    return Err(IacError::Other(anyhow::anyhow!(
                        "preexisting DSQL mode requires preexisting_endpoint or preexisting_arn"
                    )));
                }

                let now = chrono::Utc::now().to_rfc3339();
                Ok(ResourceState {
                    resource_type: ResourceType::new("DsqlCluster"),
                    physical_id: if !arn.is_empty() {
                        arn.clone()
                    } else {
                        format!("dsql-{}-preexisting", self.cluster_identity)
                    },
                    properties: serde_json::json!({
                        "mode": "preexisting",
                        "cluster_identity": self.cluster_identity,
                        "cluster_id": "",
                        "cluster_endpoint": endpoint,
                        "cluster_arn": arn,
                        "tags": tags,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                })
            }
            DsqlClusterMode::Managed => {
                let create = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .dsql
                    .create_cluster()
                    .set_tags(Some(tags.clone()))
                    .deletion_protection_enabled(true)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!("dsql:CreateCluster: {}", e.into_service_error()))
                    })?;

                let cluster_id = create.identifier().to_string();
                let cluster_arn = create.arn().to_string();

                ctx.extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .dsql
                    .wait_until_cluster_active()
                    .identifier(&cluster_id)
                    .wait(Duration::from_secs(900))
                    .await
                    .map_err(|e| IacError::AwsSdk(format!("dsql:WaitUntilClusterActive: {e}")))?;

                let cluster = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .dsql
                    .get_cluster()
                    .identifier(&cluster_id)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!("dsql:GetCluster: {}", e.into_service_error()))
                    })?;

                let cluster_endpoint = cluster.endpoint().unwrap_or_default().to_string();

                let now = chrono::Utc::now().to_rfc3339();
                Ok(ResourceState {
                    resource_type: ResourceType::new("DsqlCluster"),
                    physical_id: cluster_id.clone(),
                    properties: serde_json::json!({
                        "mode": "managed",
                        "cluster_identity": self.cluster_identity,
                        "cluster_id": cluster_id,
                        "cluster_endpoint": cluster_endpoint,
                        "cluster_arn": cluster_arn,
                        "tags": tags,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                })
            }
        }
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("dsql-{}", self.cluster_identity));

        if self.config.mode == DsqlClusterMode::Managed {
            let cluster_arn = current
                .properties
                .get("cluster_arn")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            if !cluster_arn.is_empty() {
                ctx.extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .dsql
                    .tag_resource()
                    .resource_arn(&cluster_arn)
                    .set_tags(Some(tags.clone()))
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!("dsql:TagResource: {}", e.into_service_error()))
                    })?;
            }
        }

        let mode_str = if self.config.mode == DsqlClusterMode::Managed {
            "managed"
        } else {
            "preexisting"
        };
        let cluster_id = prop_str(current, "cluster_id").unwrap_or_default();
        let cluster_endpoint = self
            .config
            .preexisting_endpoint
            .clone()
            .unwrap_or_else(|| prop_str(current, "cluster_endpoint").unwrap_or_default());
        let cluster_arn = self
            .config
            .preexisting_arn
            .clone()
            .unwrap_or_else(|| prop_str(current, "cluster_arn").unwrap_or_default());

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "mode": mode_str,
                "cluster_identity": self.cluster_identity,
                "cluster_id": cluster_id,
                "cluster_endpoint": cluster_endpoint,
                "cluster_arn": cluster_arn,
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
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let state_mode = current
            .properties
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if !effective_managed(self.config.mode, state_mode) {
            tracing::info!(
                resource = %self.resource_id().0,
                "preexisting DSQL cluster: skipping delete"
            );
            return Ok(());
        }

        let cluster_id = current
            .properties
            .get("cluster_id")
            .and_then(|v| v.as_str())
            .filter(|id| !id.is_empty())
            .map(|id| id.to_string())
            .ok_or_else(|| {
                IacError::StateNotFound(format!(
                    "DSQL cluster '{}' has no recoverable cluster identifier in state or config",
                    self.resource_id().0
                ))
            })?;

        ctx.emit_note_progress(
            &self.resource_id(),
            &self.resource_type(),
            &format!("deleting cluster_id '{cluster_id}'"),
        );

        let cluster = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .dsql
            .get_cluster()
            .identifier(&cluster_id)
            .send()
            .await
        {
            Ok(resp) => Some(resp),
            Err(e) => {
                let svc_err = e.into_service_error();
                let msg = format!("{svc_err}");
                if msg.contains("ResourceNotFoundException") {
                    tracing::warn!(cluster = %cluster_id, "DSQL cluster not found, skipping delete");
                    None
                } else {
                    return Err(IacError::AwsSdk(format!("dsql:GetCluster: {svc_err}")));
                }
            }
        };
        if cluster.is_none() {
            return Ok(());
        }

        if cluster
            .as_ref()
            .map(|c| c.deletion_protection_enabled())
            .unwrap_or(false)
        {
            ctx.emit_note_progress(
                &self.resource_id(),
                &self.resource_type(),
                "disabling deletion protection before delete",
            );
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .dsql
                .update_cluster()
                .identifier(&cluster_id)
                .deletion_protection_enabled(false)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("dsql:UpdateCluster: {}", e.into_service_error()))
                })?;

            let dsql_client = &ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .dsql;
            super::poll_until(
                Duration::from_secs(5),
                Duration::from_secs(900),
                ctx,
                super::PollTarget {
                    resource_desc: "DSQL cluster deletion protection",
                    resource_id: &self.resource_id(),
                    resource_type: self.resource_type(),
                    phase: "waiting for deletion protection to be disabled",
                },
                || async {
                    let refreshed = dsql_client
                        .get_cluster()
                        .identifier(&cluster_id)
                        .send()
                        .await
                        .map_err(|e| {
                            IacError::AwsSdk(format!("dsql:GetCluster: {}", e.into_service_error()))
                        })?;
                    Ok(!refreshed.deletion_protection_enabled())
                },
            )
            .await?;
            ctx.emit_note_progress(
                &self.resource_id(),
                &self.resource_type(),
                "confirmed deletion protection disabled",
            );
        } else {
            ctx.emit_note_progress(
                &self.resource_id(),
                &self.resource_type(),
                "deletion protection already disabled",
            );
        }

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .dsql
            .delete_cluster()
            .identifier(&cluster_id)
            .send()
            .await
        {
            Ok(delete_output) => {
                let delete_status = Some(delete_output.status().as_str().to_string());
                if let Some(status) = delete_status {
                    ctx.emit_note_progress(
                        &self.resource_id(),
                        &self.resource_type(),
                        &format!("delete accepted by AWS with status {status}; waiting for cluster disappearance"),
                    );
                } else {
                    ctx.emit_note_progress(
                        &self.resource_id(),
                        &self.resource_type(),
                        "delete accepted by AWS; waiting for cluster disappearance",
                    );
                }

                let dsql_client = &ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .dsql;
                super::poll_until(
                    Duration::from_secs(5),
                    Duration::from_secs(900),
                    ctx,
                    super::PollTarget {
                        resource_desc: "DSQL cluster deletion",
                        resource_id: &self.resource_id(),
                        resource_type: self.resource_type(),
                        phase: "waiting for cluster disappearance",
                    },
                    || async {
                        let get_cluster_gone = match dsql_client
                            .get_cluster()
                            .identifier(&cluster_id)
                            .send()
                            .await
                        {
                            Ok(_cluster) => false,
                            Err(e) => {
                                let svc_err = e.into_service_error();
                                let msg = format!("{svc_err}");
                                if msg.contains("ResourceNotFoundException") {
                                    true
                                } else {
                                    return Err(IacError::AwsSdk(format!(
                                        "dsql:GetCluster: {svc_err}"
                                    )));
                                }
                            }
                        };

                        let list_output =
                            dsql_client.list_clusters().send().await.map_err(|e| {
                                IacError::AwsSdk(format!(
                                    "dsql:ListClusters: {}",
                                    e.into_service_error()
                                ))
                            })?;
                        let listed = list_output
                            .clusters()
                            .iter()
                            .any(|cluster| cluster.identifier() == cluster_id);
                        if listed {
                            ctx.emit_note_progress(
                                &self.resource_id(),
                                &self.resource_type(),
                                "cluster still present in ListClusters",
                            );
                        }

                        Ok(get_cluster_gone && !listed)
                    },
                )
                .await?;
                ctx.emit_note_progress(
                    &self.resource_id(),
                    &self.resource_type(),
                    "confirmed cluster disappearance via GetCluster and ListClusters",
                );
                Ok(())
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                let msg = format!("{svc_err}");
                if msg.contains("ResourceNotFoundException") {
                    tracing::warn!(cluster = %cluster_id, "DSQL cluster not found, skipping delete");
                    Ok(())
                } else {
                    Err(IacError::AwsSdk(format!("dsql:DeleteCluster: {svc_err}")))
                }
            }
        }
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        match self.config.mode {
            DsqlClusterMode::Preexisting => {
                let endpoint = self.config.preexisting_endpoint.clone().unwrap_or_default();
                let arn = self.config.preexisting_arn.clone().unwrap_or_default();
                if endpoint.is_empty() && arn.is_empty() {
                    return Ok(DescribeResult::Unsupported);
                }
                let now = chrono::Utc::now().to_rfc3339();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("DsqlCluster"),
                    physical_id: if !arn.is_empty() {
                        arn.clone()
                    } else {
                        format!("dsql-{}-preexisting", self.cluster_identity)
                    },
                    properties: serde_json::json!({
                        "mode": "preexisting",
                        "cluster_identity": self.cluster_identity,
                        "cluster_id": "",
                        "cluster_endpoint": endpoint,
                        "cluster_arn": arn,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                }))
            }
            DsqlClusterMode::Managed => {
                let current = ctx.state.resources.get(&self.resource_id());
                let Some(cluster_id) = self.resolve_identifier(current) else {
                    return Ok(DescribeResult::Unsupported);
                };

                let cluster = match ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .dsql
                    .get_cluster()
                    .identifier(&cluster_id)
                    .send()
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        let msg = format!("{svc_err}");
                        if msg.contains("ResourceNotFoundException") {
                            return Ok(DescribeResult::Absent);
                        }
                        return Err(IacError::AwsSdk(format!("dsql:GetCluster: {svc_err}")));
                    }
                };

                let endpoint = cluster.endpoint().unwrap_or_default().to_string();
                let arn = cluster.arn().to_string();
                let now = chrono::Utc::now().to_rfc3339();
                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("DsqlCluster"),
                    physical_id: cluster_id.clone(),
                    properties: serde_json::json!({
                        "mode": "managed",
                        "cluster_identity": self.cluster_identity,
                        "cluster_id": cluster_id,
                        "cluster_endpoint": endpoint,
                        "cluster_arn": arn,
                    }),
                    dependencies: vec![],
                    created_at: now.clone(),
                    updated_at: now,
                    module: self.module().to_owned(),
                }))
            }
        }
    }
}

/// Extract a string property from a ResourceState.
fn prop_str(rs: &ResourceState, key: &str) -> Option<String> {
    rs.properties
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
}

#[cfg(test)]
mod tests {
    // Golden declarations (operator-explanation Req 4.5): classification
    // and confidence only. A managed delete's recoverability is now a cited
    // provider guarantee (restores only ever create a new cluster) and its
    // data fate a cited inference from the documented recovery model — while
    // a preexisting cluster's whole lifecycle remains an engine fact about
    // this module's own restraint.
    #[test]
    fn dsql_declarations_carry_the_researched_answers() {
        use tokeira_iac::{
            ChangeKind, ChangeSemantics, Confidence, DataEffect, Disruption, LifecycleOperation,
            SemanticsContext,
        };

        let cluster = |mode: DsqlClusterMode| DsqlCluster {
            cluster_identity: "t".into(),
            config: DsqlClusterConfig {
                mode,
                preexisting_endpoint: None,
                preexisting_arn: None,
                fallback_identifier: None,
                resource_id: None,
                module: "dsql".into(),
            },
            project: "p".into(),
            region: "us-east-1".into(),
            tags: HashMap::new(),
        };
        let declared = |mode: DsqlClusterMode, kind: ChangeKind| {
            cluster(mode).change_semantics(&SemanticsContext {
                kind,
                current: None,
                field_diffs: &[],
            })
        };

        let managed_delete = declared(DsqlClusterMode::Managed, ChangeKind::Delete);
        assert!(matches!(
            managed_delete.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::Deleted,
                ..
            }
        ));
        assert!(matches!(
            managed_delete.disruption,
            Confidence::EngineFact {
                value: Disruption::UnavailableDuringChange,
                ..
            }
        ));
        assert!(matches!(
            managed_delete.data_effect,
            Confidence::Inference {
                value: DataEffect::Destroyed,
                ..
            }
        ));
        assert!(matches!(
            managed_delete.reversibility,
            Confidence::ProviderGuarantee {
                value: tokeira_iac::Reversibility::Irreversible,
                ..
            }
        ));

        let managed_create = declared(DsqlClusterMode::Managed, ChangeKind::Create);
        assert!(matches!(
            managed_create.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::Created,
                ..
            }
        ));
        assert!(matches!(
            managed_create.reversibility,
            Confidence::Inference {
                value: tokeira_iac::Reversibility::ReversibleWithDataLoss,
                ..
            }
        ));

        // Managed updates are tag-only; preexisting deletes skip the
        // provider entirely — the cluster keeps running, data preserved.
        let managed_update = declared(DsqlClusterMode::Managed, ChangeKind::Update);
        assert!(matches!(
            managed_update.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::UpdatedInPlace,
                ..
            }
        ));
        // Disruption on a control-plane update is an inference, not a fact.
        assert!(matches!(
            managed_update.disruption,
            Confidence::Inference {
                value: Disruption::None,
                ..
            }
        ));
        let preexisting_delete = declared(DsqlClusterMode::Preexisting, ChangeKind::Delete);
        assert!(matches!(
            preexisting_delete.data_effect,
            Confidence::EngineFact {
                value: DataEffect::Preserved,
                ..
            }
        ));
        assert!(matches!(
            preexisting_delete.disruption,
            Confidence::EngineFact {
                value: Disruption::None,
                ..
            }
        ));

        assert_eq!(
            declared(DsqlClusterMode::Managed, ChangeKind::NoChange),
            ChangeSemantics::default()
        );
    }

    use super::*;

    #[test]
    fn effective_managed_matches_config_and_state_modes() {
        assert!(effective_managed(DsqlClusterMode::Managed, "managed"));
        assert!(effective_managed(DsqlClusterMode::Managed, "preexisting"));
        assert!(effective_managed(DsqlClusterMode::Preexisting, "managed"));
        assert!(!effective_managed(
            DsqlClusterMode::Preexisting,
            "preexisting"
        ));
    }
}
