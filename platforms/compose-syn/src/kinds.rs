//! The author's kinds — typed structs the operator names, each realizing
//! directly to a concrete engine resource. The constructor logic is the same as
//! the (deleted) bespoke-DSL realizer arms, but fed by typed fields rather than a
//! dynamic field map.

use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use tokeira_aws::{
    ResourceContext,
    resources::{
        dsql_cluster::{DsqlCluster as AwsDsqlCluster, DsqlClusterConfig, DsqlClusterMode},
        dynamodb_table::{
            AttributeType, BillingMode, DynamoDbTable as AwsDynamoDbTable, DynamoDbTableConfig,
            KeyAttribute, KeyType,
        },
    },
};
use tokeira_compose::ComposeService;
use tokeira_iac as iac;

use crate::observability_config::{ObservabilityConfigFilesResource, ObservabilityParams};

use crate::{
    builder::{Kind, Vol},
    context::Cx,
};

/// DSQL cluster lifecycle, as the *kind* sees it (flat, mirroring the engine's
/// `DsqlClusterConfig`). The operator's nested `Storage` config is flattened to
/// this by the definition — that mapping is the author/operator seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DsqlMode {
    Managed,
    Preexisting,
}

/// An Aurora DSQL cluster (→ `tokeira_aws` `DsqlCluster`). The cluster identity
/// follows the compose convention `<project>-compose`, derived from the context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DsqlCluster {
    pub region: String,
    pub mode: DsqlMode,
    pub endpoint: Option<String>,
    pub arn: Option<String>,
}

impl Kind for DsqlCluster {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        let mode = match self.mode {
            DsqlMode::Managed => DsqlClusterMode::Managed,
            DsqlMode::Preexisting => DsqlClusterMode::Preexisting,
        };
        Box::new(AwsDsqlCluster::new(
            format!("{}-compose", cx.project_name),
            DsqlClusterConfig {
                mode,
                preexisting_endpoint: self.endpoint.clone(),
                preexisting_arn: self.arn.clone(),
                fallback_identifier: None,
                resource_id: None,
                module: cx.project_name.clone(),
            },
            &aws_ctx(cx, &self.region),
        ))
    }
}

/// A DynamoDB coordination table (→ `tokeira_aws` `DynamoDbTable`): on-demand,
/// single hash key, optional TTL. The operator sets the full table name (the
/// compose convention is `<project>-dsql-<id>`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DynamoDbTable {
    pub table: String,
    pub hash_key: String,
    pub ttl: Option<String>,
}

impl Kind for DynamoDbTable {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(AwsDynamoDbTable::new(
            self.table.clone(),
            DynamoDbTableConfig {
                key_schema: vec![KeyAttribute {
                    name: self.hash_key.clone(),
                    key_type: KeyType::Hash,
                    attribute_type: AttributeType::String,
                }],
                billing_mode: BillingMode::OnDemand,
                ttl_attribute: self.ttl.clone(),
                module: cx.project_name.clone(),
            },
            &aws_ctx(cx, cx.region.as_deref().unwrap_or("us-east-1")),
        ))
    }
}

/// The rendered observability config tree (→ the compose-deployment
/// `ObservabilityConfigFilesResource`). It writes mimir/loki/alloy/grafana config
/// under `<deployment_dir>/config`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservabilityConfigFiles {
    pub scrape_host: String,
    pub scrape_port: u16,
    pub cluster: String,
    pub deployment: String,
    pub mimir_remote_write: String,
    pub loki_push: String,
    pub mimir_http_port: u16,
    pub loki_http_port: u16,
    pub retention_hours: u32,
}

impl Kind for ObservabilityConfigFiles {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(ObservabilityConfigFilesResource::new(
            cx.deployment_dir.clone(),
            ObservabilityParams {
                metrics_target_host: self.scrape_host.clone(),
                metrics_target_port: self.scrape_port,
                cluster: self.cluster.clone(),
                deployment: self.deployment.clone(),
                mimir_remote_write_url: self.mimir_remote_write.clone(),
                loki_push_url: self.loki_push.clone(),
                mimir_http_port: self.mimir_http_port,
                loki_http_port: self.loki_http_port,
                loki_retention_hours: self.retention_hours,
            },
        ))
    }
}

/// The local IaC state directory — the bootstrap module's resource. Realizes to a
/// resource that creates `<deployment_dir>/state`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalStateDir;

impl Kind for LocalStateDir {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource> {
        Box::new(LocalStateDirResource {
            state_dir: cx.deployment_dir.join("state"),
        })
    }
}

#[derive(Debug)]
struct LocalStateDirResource {
    state_dir: PathBuf,
}

impl LocalStateDirResource {
    fn state(&self) -> iac::ResourceState {
        iac::ResourceState {
            resource_type: iac::ResourceType::new("local_state_dir"),
            physical_id: self.state_dir.display().to_string(),
            properties: serde_json::json!({ "path": self.state_dir }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: "local_state".into(),
        }
    }
}

#[async_trait]
impl iac::Resource for LocalStateDirResource {
    fn resource_type(&self) -> iac::ResourceType {
        iac::ResourceType::new("local_state_dir")
    }

    fn resource_id(&self) -> iac::ResourceId {
        iac::ResourceId("state-dir".into())
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        "local_state"
    }

    async fn create(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        std::fs::create_dir_all(&self.state_dir).map_err(|e| iac::IacError::Other(e.into()))?;
        Ok(self.state())
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::DescribeResult, iac::IacError> {
        // `exists()` is a real check of the managed state dir → confirmed Absent.
        Ok(if self.state_dir.exists() {
            iac::DescribeResult::Present(self.state())
        } else {
            iac::DescribeResult::Absent
        })
    }

    fn diff(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        iac::InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

/// A service workload (→ `tokeira_compose::ComposeService`). A compose service is
/// realized two ways: as an infra `iac::Resource` (the service definition) and as
/// a deploy-engine workload (the builder handles that half). `needs` are
/// name-based deploy-ordering deps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    pub image: String,
    pub replicas: u32,
    pub publish: Vec<u16>,
    pub volumes: Vec<Vol>,
    pub env: Vec<(String, String)>,
    pub command: Vec<String>,
    pub needs: Vec<String>,
    /// Mount the operator's server config (`tokeirad.toml`) when present. The
    /// presence probe + mount is author mechanics, not operator-authored I/O.
    pub server_config: bool,
    /// When `Some(region)`, attach the DSQL AWS runtime edge (`~/.aws` mount +
    /// `AWS_*` forwarding). Author mechanics — credentials never touch the `.tkd`.
    pub aws: Option<String>,
}

impl Service {
    /// The empty service — every optional field at its zero. Operator definitions
    /// elide defaults with `..Service::EMPTY`, so a service is only its non-zero
    /// fields. (The interpreter mirrors this as the registry `defaults` map.)
    pub const EMPTY: Service = Service {
        image: String::new(),
        replicas: 0,
        publish: Vec::new(),
        volumes: Vec::new(),
        env: Vec::new(),
        command: Vec::new(),
        needs: Vec::new(),
        server_config: false,
        aws: None,
    };

    /// The `ComposeService` this service realizes to (used for both the infra
    /// resource and the deploy-engine workload). This is the SOLE owner of the
    /// relocated mechanics: host-path volume resolution, the conditional
    /// `tokeirad.toml` mount, and the DSQL AWS edge — all kept byte-identical to
    /// `tokeira_compose_deployment::compose_services`. Build order is load-bearing
    /// (`to_manifest` serializes `volumes`/`environment` positionally): base
    /// fields first, then `server_config`, then `aws`.
    pub(crate) fn to_compose_service(&self, name: &str, cx: &Cx) -> ComposeService {
        let mut volumes: Vec<String> = self.volumes.iter().map(|v| realize_vol(v, cx)).collect();
        let mut environment: HashMap<String, String> = self.env.iter().cloned().collect();

        // WART — conditional server-config mount (compose.rs L56-63), BEFORE aws.
        if self.server_config {
            let toml = cx.deployment_dir.join("tokeirad.toml");
            if toml.exists() {
                volumes.push(format!("{}:/etc/tokeira/tokeirad.toml:ro", toml.display()));
                environment.insert("TOKEIRA_CONFIG".into(), "/etc/tokeira/tokeirad.toml".into());
            }
        }

        // WART — DSQL AWS runtime edge (compose.rs L65-83), AFTER server_config.
        if let Some(region) = &self.aws {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
            volumes.push(format!("{home}/.aws:/home/nonroot/.aws:ro"));
            environment.insert("HOME".into(), "/home/nonroot".into());
            environment.insert("AWS_REGION".into(), region.clone());
            for key in [
                "AWS_PROFILE",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_ROLE_ARN",
            ] {
                if let Ok(value) = std::env::var(key) {
                    environment.insert(key.to_string(), value);
                }
            }
        }

        ComposeService {
            name: name.to_string(),
            image: self.image.clone(),
            ports: self.publish.iter().map(|p| format!("{p}:{p}")).collect(),
            volumes,
            environment,
            depends_on: self.needs.clone(),
            healthcheck: None,
            command: self.command.clone(),
            // Wired by the builder from the typed `Vol::Config` anchors —
            // the service realizer only shapes the container.
            resource_dependencies: Vec::new(),
        }
    }
}

/// Resolve a logical [`Vol`] anchor to its concrete `host:container` mount string,
/// byte-identical to the old inline `vol(state_dir.join(sub), at)` math.
fn realize_vol(v: &Vol, cx: &Cx) -> String {
    match v {
        Vol::State { sub, at } => format!("{}:{}", cx.state_dir().join(sub).display(), at),
        Vol::Config { sub, at } => format!("{}:{}", cx.config_dir().join(sub).display(), at),
        Vol::Raw(s) => s.clone(),
    }
}

/// The shared AWS resource context (project + region + the `ManagedBy` tag).
fn aws_ctx(cx: &Cx, region: &str) -> ResourceContext {
    ResourceContext {
        project: cx.project_name.clone(),
        region: region.to_owned(),
        tags: HashMap::from([("ManagedBy".to_owned(), "tkr".to_owned())]),
    }
}
