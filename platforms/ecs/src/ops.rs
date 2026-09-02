//! Definition-derived coordinates for live ECS operations.
//!
//! The framework gives every platform the same identity-only
//! [`DeploymentRef`]. ECS owns
//! the extra facts its AWS operations require, so it re-evaluates the exact
//! definition revision recorded in the admitted deployment directory using
//! the ECS namespaces and frontend selected by metadata. This intentionally
//! repeats platform evaluation at the operational boundary: frontend choice,
//! validation, and the meaning of ECS configuration remain platform policy
//! rather than additions to the generic operations interface.
//!
//! Only non-secret routing coordinates are retained. Provider credentials
//! stay ambient, provider state is queried live by each operation, and this
//! projection never contains resource identifiers discovered during apply.
//! The metadata state location remains authoritative for infrastructure,
//! runtime, and lock state; it does not relocate the authored definition,
//! which is always read from the admitted deployment directory.

use std::{fs, sync::Arc};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use tokeira_deployment::DeploymentBindingMetadata;
use tokeira_platform::{
    author::from_located_value,
    declaration::DeploymentRef,
    definition::{
        DefinitionSource, DefinitionSourceName, DirectoryPartSources, evaluate_definition,
    },
};

const METADATA_JSON: &str = "metadata.json";

#[derive(Debug, Serialize)]
struct EvaluationContext {
    project_name: String,
}

// This is deliberately a projection, not a second ECS configuration model.
// Serde ignores fields owned by deployment realization, while definition
// evaluation above still validates the complete graph through every kind.
#[derive(Debug, Deserialize)]
struct OpsConfiguration {
    environment: String,
    aws: OpsAwsConfiguration,
    cluster: OpsClusterConfiguration,
    networking: OpsNetworkingConfiguration,
}

#[derive(Debug, Deserialize)]
struct OpsAwsConfiguration {
    region: String,
}

#[derive(Debug, Deserialize)]
struct OpsClusterConfiguration {
    name: String,
    service_connect_namespace: String,
}

#[derive(Debug, Deserialize)]
struct OpsNetworkingConfiguration {
    private_dns_zone: String,
}

/// Authored, non-secret coordinates required by ECS day-2 operations.
///
/// These values are recovered afresh from the admitted definition instead
/// of copied into lifecycle state. That keeps remote-state deployments
/// portable and prevents desired routing facts from drifting between the
/// definition and a platform-private operations ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsOperationCoordinates {
    environment: String,
    region: String,
    cluster: String,
    service_connect_namespace: String,
    private_dns_zone: String,
}

impl EcsOperationCoordinates {
    /// Re-evaluate a deployment's recorded definition and recover its ECS
    /// operation coordinates.
    ///
    /// The deployment directory must belong to the supplied name, record the
    /// `ecs` platform, and contain a supported `tkd` or `tkdp` definition.
    /// Full definition validation runs before this narrow projection is
    /// admitted, so malformed resources are refused before any AWS client is
    /// selected.
    pub fn read(deployment: &DeploymentRef) -> Result<Self> {
        let metadata_path = deployment.dir.join(METADATA_JSON);
        let metadata: DeploymentBindingMetadata = serde_json::from_slice(
            &fs::read(&metadata_path)
                .with_context(|| format!("failed to read {}", metadata_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", metadata_path.display()))?;
        if metadata.name != deployment.name {
            bail!(
                "deployment directory records name `{}` but the admitted deployment is `{}`",
                metadata.name,
                deployment.name
            );
        }
        if metadata.platform.as_str() != "ecs" {
            bail!(
                "deployment `{}` records platform `{}`, not `ecs`",
                deployment.name,
                metadata.platform
            );
        }
        let definition = metadata.definition.ok_or_else(|| {
            anyhow::anyhow!(
                "deployment `{}` records no definition revision",
                deployment.name
            )
        })?;
        let definition_path = deployment.dir.join(definition.path.as_path());
        let source = DefinitionSource {
            format: definition.format.clone(),
            source_name: DefinitionSourceName::DeploymentRelative(definition.path),
            bytes: Arc::from(fs::read(&definition_path).with_context(|| {
                format!("failed to read definition {}", definition_path.display())
            })?),
        };
        let parts_dir = definition_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(deployment.dir.as_path());
        let parts = DirectoryPartSources::new(parts_dir, definition.format.as_str());
        let context = EvaluationContext {
            project_name: deployment.name.clone(),
        };
        let namespaces = crate::namespaces();
        let evaluated = match definition.format.as_str() {
            "tkd" => evaluate_definition(
                &tokeira_platform_definition::tkd::frontend(),
                source,
                &context,
                &namespaces,
                &parts,
            ),
            "tkdp" => evaluate_definition(
                &tokeira_platform_definition::tkdp::frontend(),
                source,
                &context,
                &namespaces,
                &parts,
            ),
            format => bail!(
                "deployment `{}` records unsupported ECS definition format `{format}`",
                deployment.name
            ),
        }
        .with_context(|| {
            format!(
                "failed to evaluate admitted ECS definition {}",
                definition_path.display()
            )
        })?;
        let config: OpsConfiguration = from_located_value(evaluated.config)
            .context("admitted ECS definition has no usable operations configuration")?;

        Ok(Self {
            environment: required(config.environment, "environment")?,
            region: required(config.aws.region, "aws.region")?,
            cluster: required(config.cluster.name, "cluster.name")?,
            service_connect_namespace: required(
                config.cluster.service_connect_namespace,
                "cluster.service_connect_namespace",
            )?,
            private_dns_zone: required(
                config.networking.private_dns_zone,
                "networking.private_dns_zone",
            )?,
        })
    }

    /// Authored environment label used by workload and telemetry selection.
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// AWS region in which live ECS resources must be queried.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Authored ECS cluster name.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Authored Service Connect namespace used for service discovery.
    pub fn service_connect_namespace(&self) -> &str {
        &self.service_connect_namespace
    }

    /// Authored private DNS zone, independent of Service Connect naming.
    pub fn private_dns_zone(&self) -> &str {
        &self.private_dns_zone
    }
}

fn required(value: String, path: &str) -> Result<String> {
    if value.trim().is_empty() {
        bail!("admitted ECS definition has an empty `{path}`");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn replace_required(source: &mut String, authored: &str, replacement: &str) {
        assert!(source.contains(authored), "fixture contains `{authored}`");
        *source = source.replacen(authored, replacement, 1);
    }

    fn stage_definition(temp: &Path, format: &str) -> DeploymentRef {
        let source_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root_name = if format == "tkd" {
            "deployment.tkd"
        } else {
            "definition.tkdp"
        };
        for entry in fs::read_dir(source_dir).expect("ECS source directory") {
            let entry = entry.expect("source entry");
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some(format) {
                continue;
            }
            let mut source = fs::read_to_string(&path).expect("definition source");
            if path.file_name().and_then(|name| name.to_str()) == Some(root_name) {
                if format == "tkd" {
                    replace_required(
                        &mut source,
                        "environment: \"dev\".into(),",
                        "environment: \"review\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "region: \"eu-west-2\".into(),",
                        "region: \"eu-north-1\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "name: \"tokeira\".into(),",
                        "name: \"ops-cluster\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "service_connect_namespace: \"tokeira.internal\".into(),",
                        "service_connect_namespace: \"mesh.example\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "private_dns_zone: \"tokeira.internal\".into(),",
                        "private_dns_zone: \"private.example\".into(),",
                    );
                } else {
                    replace_required(
                        &mut source,
                        "environment=\"dev\",",
                        "environment=\"review\",",
                    );
                    replace_required(&mut source, "region=\"eu-west-2\"", "region=\"eu-north-1\"");
                    replace_required(&mut source, "name=\"tokeira\",", "name=\"ops-cluster\",");
                    replace_required(
                        &mut source,
                        "service_connect_namespace=\"tokeira.internal\",",
                        "service_connect_namespace=\"mesh.example\",",
                    );
                    replace_required(
                        &mut source,
                        "private_dns_zone=\"tokeira.internal\",",
                        "private_dns_zone=\"private.example\",",
                    );
                }
            }
            fs::write(temp.join(entry.file_name()), source).expect("stage definition source");
        }
        fs::write(
            temp.join(METADATA_JSON),
            serde_json::to_vec(&serde_json::json!({
                "name": "ops-fixture",
                "id": "7698ae09-197e-4325-9f77-256dac98f23a",
                "platform": "ecs",
                "definition": { "format": format, "path": root_name }
            }))
            .expect("metadata serializes"),
        )
        .expect("stage metadata");
        DeploymentRef {
            name: "ops-fixture".to_string(),
            dir: temp.to_path_buf(),
        }
    }

    #[test]
    fn coordinates_follow_each_admitted_definition_format() {
        for format in ["tkd", "tkdp"] {
            let temp = tempfile::tempdir().expect("deployment directory");
            let deployment = stage_definition(temp.path(), format);
            let coordinates =
                EcsOperationCoordinates::read(&deployment).expect("definition-derived coordinates");

            assert_eq!(coordinates.environment(), "review");
            assert_eq!(coordinates.region(), "eu-north-1");
            assert_eq!(coordinates.cluster(), "ops-cluster");
            assert_eq!(coordinates.service_connect_namespace(), "mesh.example");
            assert_eq!(coordinates.private_dns_zone(), "private.example");
        }
    }

    #[test]
    fn coordinates_refuse_a_directory_bound_to_another_deployment() {
        let temp = tempfile::tempdir().expect("deployment directory");
        let mut deployment = stage_definition(temp.path(), "tkd");
        deployment.name = "different-deployment".to_string();

        let error = EcsOperationCoordinates::read(&deployment)
            .expect_err("metadata name must match admission");
        assert!(error.to_string().contains("records name `ops-fixture`"));
    }

    #[test]
    fn coordinates_refuse_another_platform() {
        let temp = tempfile::tempdir().expect("deployment directory");
        let deployment = stage_definition(temp.path(), "tkd");
        let metadata_path = temp.path().join(METADATA_JSON);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read staged metadata"))
                .expect("decode staged metadata");
        metadata["platform"] = "compose".into();
        fs::write(
            metadata_path,
            serde_json::to_vec(&metadata).expect("encode staged metadata"),
        )
        .expect("rewrite staged metadata");

        let error = EcsOperationCoordinates::read(&deployment)
            .expect_err("platform identity must match admission");
        assert!(error.to_string().contains("platform `compose`, not `ecs`"));
    }

    #[test]
    fn coordinates_refuse_an_unregistered_frontend() {
        let temp = tempfile::tempdir().expect("deployment directory");
        let deployment = stage_definition(temp.path(), "tkd");
        let definition_path = temp.path().join("definition.json");
        fs::write(&definition_path, "{}").expect("stage unsupported definition");
        let metadata_path = temp.path().join(METADATA_JSON);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read staged metadata"))
                .expect("decode staged metadata");
        metadata["definition"] = serde_json::json!({
            "format": "json",
            "path": "definition.json"
        });
        fs::write(
            metadata_path,
            serde_json::to_vec(&metadata).expect("encode staged metadata"),
        )
        .expect("rewrite staged metadata");

        let error = EcsOperationCoordinates::read(&deployment)
            .expect_err("only registered ECS frontends are admitted");
        assert!(
            error
                .to_string()
                .contains("unsupported ECS definition format `json`")
        );
    }

    #[test]
    fn required_coordinates_refuse_whitespace() {
        let error = required("  \n".to_string(), "cluster.name")
            .expect_err("whitespace is not a usable provider coordinate");
        assert_eq!(
            error.to_string(),
            "admitted ECS definition has an empty `cluster.name`"
        );
    }
}
