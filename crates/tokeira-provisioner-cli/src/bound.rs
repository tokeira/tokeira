//! Identity and placed-bundle admission for one concrete provisioner.

use std::path::Path;

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_provisioner::DeploymentBindingMetadata;

use crate::{
    AppliedOutcome, ChangeLogEntry, ConfigSource, DesiredSnapshot, LogStream, ProvisionerPlatform,
    Realization,
};

const METADATA_JSON: &str = "metadata.json";

/// Thin evidence gate around a statically assembled concrete provisioner.
#[derive(Debug, Clone)]
pub struct BoundPlatform<P> {
    expected_platform: PlatformId,
    expected_format: DefinitionFormatId,
    inner: P,
}

impl<P> BoundPlatform<P> {
    /// Admit the identities embedded by the generated composition root.
    pub fn new(
        expected_platform: &'static str,
        expected_format: &'static str,
        inner: P,
    ) -> Result<Self> {
        Ok(Self {
            expected_platform: PlatformId::new(expected_platform)?,
            expected_format: DefinitionFormatId::new(expected_format)?,
            inner,
        })
    }

    fn metadata(&self, deployment_dir: &Path) -> Result<DeploymentBindingMetadata> {
        let path = deployment_dir.join(METADATA_JSON);
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let metadata: DeploymentBindingMetadata = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode {}", path.display()))?;
        if metadata.platform != self.expected_platform {
            bail!(
                "deployment metadata selects platform `{}` but this provisioner is bound to `{}`",
                metadata.platform,
                self.expected_platform
            );
        }
        let definition = metadata.definition.as_ref().ok_or_else(|| {
            anyhow::anyhow!("bound deployment metadata records no definition format/path")
        })?;
        if definition.format != self.expected_format {
            bail!(
                "deployment metadata selects definition format `{}` but this provisioner is bound to `{}`",
                definition.format,
                self.expected_format
            );
        }
        self.validate_bundle(deployment_dir, &metadata, &definition.format)?;
        Ok(metadata)
    }

    fn validate_bundle(
        &self,
        deployment_dir: &Path,
        metadata: &DeploymentBindingMetadata,
        format: &DefinitionFormatId,
    ) -> Result<()> {
        let bundle_path = deployment_dir.join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME);
        if !bundle_path.is_file() {
            return Ok(());
        }
        let bundle: tokeira_provisioner::ProvisionerBundle = serde_json::from_slice(
            &std::fs::read(&bundle_path)
                .with_context(|| format!("failed to read {}", bundle_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", bundle_path.display()))?;
        bundle.validate_bound_evidence()?;
        let evidence = bundle.bound.as_ref().ok_or_else(|| {
            anyhow::anyhow!("placed provisioner bundle carries no bound platform/frontend evidence")
        })?;
        if evidence.platform != metadata.platform || &evidence.format != format {
            bail!(
                "placed provisioner bundle selects platform/format `{}/{}` but deployment metadata records `{}/{}`",
                evidence.platform,
                evidence.format,
                metadata.platform,
                format
            );
        }
        let manifest = bundle.integrity_manifest();
        manifest.validate().map_err(|error| {
            anyhow::anyhow!("placed provisioner bundle has an invalid integrity manifest: {error}")
        })?;
        let executable =
            std::env::current_exe().context("failed to locate the running bound provisioner")?;
        let executable_bytes = std::fs::read(&executable)
            .with_context(|| format!("failed to read {}", executable.display()))?;
        manifest
            .verify_artifact(
                &executable_bytes,
                &tokeira_provisioner::Target(env!("TKP_TARGET").to_string()),
            )
            .map_err(|error| {
                anyhow::anyhow!(
                    "the running bound provisioner disagrees with its placed bundle: {error}"
                )
            })
    }
}

impl<P: ProvisionerPlatform> ProvisionerPlatform for BoundPlatform<P> {
    fn admit_deployment(&self, deployment_dir: &Path) -> Result<()> {
        self.metadata(deployment_dir)?;
        if self.inner.label(deployment_dir) != self.expected_platform.as_str() {
            bail!(
                "generated provisioner expects platform `{}` but the concrete provisioner exports `{}`",
                self.expected_platform,
                self.inner.label(deployment_dir)
            );
        }
        if self.inner.definition_format() != Some(self.expected_format.as_str()) {
            bail!(
                "generated provisioner expects definition format `{}` but the concrete provisioner exports `{}`",
                self.expected_format,
                self.inner.definition_format().unwrap_or("<none>")
            );
        }
        self.inner.admit_deployment(deployment_dir)
    }

    fn label(&self, deployment_dir: &Path) -> &'static str {
        self.inner.label(deployment_dir)
    }

    fn config_source(&self, deployment_dir: &Path) -> Result<ConfigSource> {
        self.inner.config_source(deployment_dir)
    }

    fn definition_format(&self) -> Option<&str> {
        self.inner.definition_format()
    }

    fn deployment_id(&self, deployment_dir: &Path) -> Result<String> {
        self.inner.deployment_id(deployment_dir)
    }

    async fn infra_plan(
        &self,
        deployment_dir: &Path,
        module: Option<&str>,
    ) -> Result<tokeira_iac::PlanOutcome> {
        self.inner.infra_plan(deployment_dir, module).await
    }

    async fn infra_apply(
        &self,
        deployment_dir: &Path,
        module: Option<&str>,
    ) -> Result<AppliedOutcome> {
        self.inner.infra_apply(deployment_dir, module).await
    }

    async fn infra_apply_with_artifacts(
        &self,
        deployment_dir: &Path,
        module: Option<&str>,
    ) -> Result<AppliedOutcome> {
        self.inner
            .infra_apply_with_artifacts(deployment_dir, module)
            .await
    }

    async fn publish_inspection(&self, deployment_dir: &Path) -> Result<usize> {
        self.inner.publish_inspection(deployment_dir).await
    }

    async fn infra_destroy(&self, deployment_dir: &Path, module: Option<&str>) -> Result<usize> {
        self.inner.infra_destroy(deployment_dir, module).await
    }

    async fn infra_destroy_selected(
        &self,
        deployment_dir: &Path,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        self.inner.infra_destroy_selected(deployment_dir, ids).await
    }

    async fn definition_check(
        &self,
        deployment_dir: &Path,
        source: Option<&Path>,
    ) -> Result<Realization<()>> {
        self.inner.definition_check(deployment_dir, source).await
    }

    async fn log_stream(
        &self,
        deployment_dir: &Path,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> Result<Realization<LogStream>> {
        self.inner
            .log_stream(deployment_dir, service, follow, tail)
            .await
    }

    async fn port_mappings(
        &self,
        deployment_dir: &Path,
        service: &str,
    ) -> Result<Realization<Vec<tokeira_orchestrator::PortMapping>>> {
        self.inner.port_mappings(deployment_dir, service).await
    }

    async fn desired_snapshot(
        &self,
        deployment_dir: &Path,
        definition: &Path,
    ) -> Result<Realization<DesiredSnapshot>> {
        self.inner
            .desired_snapshot(deployment_dir, definition)
            .await
    }

    async fn recorded_state(&self, deployment_dir: &Path) -> Result<tokeira_iac::InfraState> {
        self.inner.recorded_state(deployment_dir).await
    }

    async fn deploy_plan(
        &self,
        deployment_dir: &Path,
    ) -> Result<Realization<tokeira_iac::PlanOutcome>> {
        self.inner.deploy_plan(deployment_dir).await
    }

    async fn deploy_apply(&self, deployment_dir: &Path) -> Result<Realization<AppliedOutcome>> {
        self.inner.deploy_apply(deployment_dir).await
    }

    async fn scale(&self, deployment_dir: &Path, specs: &[String]) -> Result<Realization<usize>> {
        self.inner.scale(deployment_dir, specs).await
    }
}
