//! Static adapter from one platform binding and one frontend to the lifecycle shell.

use std::{path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId, PlatformLaunchClass};
use tokeira_platform::{
    binding::{Platform, PlatformBinding},
    context::InvocationContext,
    definition::{
        DefinitionEngine, DefinitionFrontend, DefinitionRequest, DefinitionSource,
        DefinitionSourceName,
    },
};
use tokeira_provisioner::DeploymentBindingMetadata;

use crate::{
    AppliedOutcome, ChangeLogEntry, ConfigSource, DesiredSnapshot, ProvisionerPlatform, Realization,
};

const METADATA_JSON: &str = "metadata.json";

/// One statically selected platform binding and Definition Frontend.
#[derive(Debug, Clone)]
pub struct BoundPlatform<P, F>
where
    P: Platform,
    F: DefinitionFrontend<P>,
{
    expected_platform: &'static str,
    expected_format: &'static str,
    binding: PlatformBinding<P>,
    frontend: F,
}

impl<P, F> BoundPlatform<P, F>
where
    P: Platform,
    F: DefinitionFrontend<P>,
{
    /// Assemble and validate the identities embedded by the generated root.
    pub fn new(
        expected_platform: &'static str,
        expected_format: &'static str,
        binding: PlatformBinding<P>,
        frontend: F,
    ) -> Result<Self> {
        let expected_platform_id = PlatformId::new(expected_platform)?;
        let expected_format_id = DefinitionFormatId::new(expected_format)?;
        if binding.id != expected_platform_id {
            bail!(
                "generated provisioner expects platform `{expected_platform_id}` but binding exports `{}`",
                binding.id
            );
        }
        if frontend.format() != &expected_format_id {
            bail!(
                "generated provisioner expects definition format `{expected_format_id}` but frontend exports `{}`",
                frontend.format()
            );
        }
        Ok(Self {
            expected_platform,
            expected_format,
            binding,
            frontend,
        })
    }

    fn metadata(&self, deployment_dir: &Path) -> Result<DeploymentBindingMetadata> {
        let path = deployment_dir.join(METADATA_JSON);
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let metadata: DeploymentBindingMetadata = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode {}", path.display()))?;
        let expected_platform = PlatformId::new(self.expected_platform)?;
        if metadata.platform != expected_platform {
            bail!(
                "deployment metadata selects platform `{}` but this provisioner is bound to `{expected_platform}`",
                metadata.platform
            );
        }
        if metadata.launch_class != Some(PlatformLaunchClass::BoundProvisioner) {
            bail!("deployment metadata does not select the `bound-provisioner` launch class");
        }
        let definition = metadata.definition.as_ref().ok_or_else(|| {
            anyhow::anyhow!("bound deployment metadata records no definition format/path")
        })?;
        let expected_format = DefinitionFormatId::new(self.expected_format)?;
        if definition.format != expected_format {
            bail!(
                "deployment metadata selects definition format `{}` but this provisioner is bound to `{expected_format}`",
                definition.format
            );
        }
        let bundle_path = deployment_dir.join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME);
        if bundle_path.is_file() {
            let bundle: tokeira_provisioner::ProvisionerBundle = serde_json::from_slice(
                &std::fs::read(&bundle_path)
                    .with_context(|| format!("failed to read {}", bundle_path.display()))?,
            )
            .with_context(|| format!("failed to decode {}", bundle_path.display()))?;
            bundle.validate_bound_evidence()?;
            let evidence = bundle.bound.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "placed provisioner bundle carries no bound platform/frontend evidence"
                )
            })?;
            if evidence.platform != metadata.platform || evidence.format != definition.format {
                bail!(
                    "placed provisioner bundle selects platform/format `{}/{}` but deployment metadata records `{}/{}`",
                    evidence.platform,
                    evidence.format,
                    metadata.platform,
                    definition.format
                );
            }
            let manifest = bundle.integrity_manifest();
            manifest.validate().map_err(|error| {
                anyhow::anyhow!(
                    "placed provisioner bundle has an invalid integrity manifest: {error}"
                )
            })?;
            let executable = std::env::current_exe()
                .context("failed to locate the running bound provisioner")?;
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
                })?;
        }
        Ok(metadata)
    }

    fn invocation(
        &self,
        deployment_dir: &Path,
        metadata: &DeploymentBindingMetadata,
    ) -> Result<P::Context> {
        self.binding
            .context
            .construct(&InvocationContext {
                deployment_id: metadata.name.clone(),
                deployment_uuid: metadata.id,
                environment: None,
                region: None,
                account_id: None,
                deployment_dir: deployment_dir.to_path_buf(),
            })
            .map_err(Into::into)
    }

    fn evaluate(
        &self,
        deployment_dir: &Path,
        source_path: &Path,
        authoring: bool,
    ) -> Result<tokeira_platform::definition::EvaluatedDefinition<P>> {
        let (format, source_name, context) = if authoring {
            (
                DefinitionFormatId::new(self.expected_format)?,
                DefinitionSourceName::AuthoringPath(source_path.to_path_buf()),
                self.binding.context.authoring()?,
            )
        } else {
            let metadata = self.metadata(deployment_dir)?;
            let definition = metadata
                .definition
                .as_ref()
                .expect("metadata admission requires a definition");
            (
                definition.format.clone(),
                DefinitionSourceName::DeploymentRelative(definition.path.clone()),
                self.invocation(deployment_dir, &metadata)?,
            )
        };
        let bytes = std::fs::read(source_path)
            .with_context(|| format!("failed to read {}", source_path.display()))?;
        DefinitionEngine::new(self.binding.clone(), self.frontend.clone())
            .evaluate(DefinitionRequest {
                source: DefinitionSource {
                    format,
                    source_name,
                    bytes: Arc::from(bytes),
                },
                context,
            })
            .map_err(Into::into)
    }

    fn verified_definition(
        &self,
        deployment_dir: &Path,
        source_path: &Path,
        authoring: bool,
    ) -> Result<tokeira_platform::definition::EvaluatedDefinition<P>> {
        let definition = self.evaluate(deployment_dir, source_path, authoring)?;
        DefinitionEngine::new(self.binding.clone(), self.frontend.clone())
            .verify(&definition)
            .map_err(|report| anyhow::anyhow!(report.to_string()))?;
        Ok(definition)
    }

    fn execution_not_ready<T>(&self) -> Result<T> {
        bail!(
            "platform `{}` is bound for definition evaluation, but its provider execution registrations are not complete",
            self.expected_platform
        )
    }
}

impl<P, F> ProvisionerPlatform for BoundPlatform<P, F>
where
    P: Platform,
    F: DefinitionFrontend<P>,
{
    fn admit_deployment(&self, deployment_dir: &Path) -> Result<()> {
        self.metadata(deployment_dir).map(|_| ())
    }

    fn label(&self, _deployment_dir: &Path) -> &'static str {
        self.expected_platform
    }

    fn config_source(&self, deployment_dir: &Path) -> Result<ConfigSource> {
        let metadata = self.metadata(deployment_dir)?;
        let definition = metadata
            .definition
            .expect("metadata admission requires a definition");
        Ok(ConfigSource::recorded(definition.format, definition.path))
    }

    fn definition_format(&self) -> Option<&'static str> {
        Some(self.expected_format)
    }

    fn deployment_id(&self, deployment_dir: &Path) -> Result<String> {
        Ok(self.metadata(deployment_dir)?.name)
    }

    async fn infra_plan(&self, _deployment_dir: &Path) -> Result<tokeira_iac::PlanOutcome> {
        self.execution_not_ready()
    }

    async fn infra_apply(&self, _deployment_dir: &Path) -> Result<AppliedOutcome> {
        self.execution_not_ready()
    }

    async fn infra_destroy(&self, _deployment_dir: &Path) -> Result<usize> {
        self.execution_not_ready()
    }

    async fn infra_destroy_selected(
        &self,
        _deployment_dir: &Path,
        _ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        self.execution_not_ready()
    }

    async fn definition_check(
        &self,
        deployment_dir: &Path,
        source: Option<&Path>,
    ) -> Result<Realization<()>> {
        let source_path = match source {
            Some(path) => path.to_path_buf(),
            None => {
                let source = self.config_source(deployment_dir)?;
                deployment_dir.join(source.path.as_path())
            }
        };
        self.verified_definition(deployment_dir, &source_path, source.is_some())?;
        Ok(Realization::Realized(()))
    }

    async fn desired_snapshot(
        &self,
        deployment_dir: &Path,
        definition: &Path,
    ) -> Result<Realization<DesiredSnapshot>> {
        let evaluated = self.verified_definition(deployment_dir, definition, false)?;
        let realized = tokeira_platform::projection::realize_resources(
            &evaluated.graph,
            &self.deployment_id(deployment_dir)?,
            &Default::default(),
        )?;
        let snapshot = realized
            .iter()
            .zip(evaluated.graph.resources())
            .map(|(physical, logical)| (physical.resource_id(), logical.kind().desired_manifest()))
            .collect();
        Ok(Realization::Realized(snapshot))
    }

    async fn deploy_plan(
        &self,
        _deployment_dir: &Path,
    ) -> Result<Realization<tokeira_iac::PlanOutcome>> {
        self.execution_not_ready()
    }

    async fn deploy_apply(&self, _deployment_dir: &Path) -> Result<Realization<AppliedOutcome>> {
        self.execution_not_ready()
    }

    async fn scale(&self, _deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: "desired capacity is changed in the recorded definition followed by plan/apply",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};
    use tokeira_platform::{
        artifact::ArtifactCatalog,
        author::AuthorSession,
        binding::{StateBinding, StatePolicy},
        catalog::{ImageCatalog, KindSet, ProviderSet, ServiceCatalog},
        config::{ConfigContract, PlatformConfig},
        context::{ContextArgument, ContextContract, ContextProjection, PlatformContext},
        error::{ConfigError, ContextError, FrontendDiagnostic},
        ops::PlatformOps,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FakeConfig;

    impl PlatformConfig for FakeConfig {
        fn validate(&self) -> Result<(), ConfigError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct FakeContext;

    impl PlatformContext for FakeContext {
        type Value = ();

        fn fields() -> &'static [&'static str] {
            &[]
        }

        fn methods() -> &'static [&'static str] {
            &[]
        }

        fn field(&self, name: &str) -> Result<ContextProjection<Self::Value>, ContextError> {
            Err(ContextError::new(format!("unknown field `{name}`")))
        }

        fn call(
            &self,
            method: &str,
            _args: &[ContextArgument<Self::Value>],
        ) -> Result<ContextProjection<Self::Value>, ContextError> {
            Err(ContextError::new(format!("unknown method `{method}`")))
        }
    }

    fn fake_context(_invocation: &InvocationContext) -> Result<FakeContext, ContextError> {
        Ok(FakeContext)
    }

    fn authoring_context() -> Result<FakeContext, ContextError> {
        Ok(FakeContext)
    }

    #[derive(Debug, Clone)]
    struct FakePlatform;

    impl Platform for FakePlatform {
        type Config = FakeConfig;
        type Context = FakeContext;

        fn binding(&self) -> PlatformBinding<Self> {
            fake_binding("compose")
        }
    }

    fn fake_binding(id: &str) -> PlatformBinding<FakePlatform> {
        PlatformBinding::new(
            PlatformId::new(id).expect("platform id"),
            "bootstrap",
            ConfigContract::new(),
            ContextContract::new(fake_context, authoring_context),
            KindSet::default(),
            ServiceCatalog::default(),
            ArtifactCatalog::default(),
            ImageCatalog::default(),
            ProviderSet::default(),
            StateBinding::new(StatePolicy::LocalCas),
            PlatformOps::default(),
            Vec::new(),
        )
        .expect("binding")
    }

    #[derive(Debug, Clone)]
    struct FakeFrontend(DefinitionFormatId);

    impl DefinitionFrontend<FakePlatform> for FakeFrontend {
        fn format(&self) -> &DefinitionFormatId {
            &self.0
        }

        fn evaluate(
            &self,
            _source: tokeira_platform::definition::FrontendSource<'_>,
            _author: &mut AuthorSession<FakePlatform>,
        ) -> Result<tokeira_platform::definition::FrontendOutput, FrontendDiagnostic> {
            unreachable!("identity tests refuse before frontend evaluation")
        }
    }

    fn fake_frontend() -> FakeFrontend {
        FakeFrontend(DefinitionFormatId::new("tkd").expect("format"))
    }

    #[test]
    fn generated_root_identity_mismatch_refuses_during_assembly() {
        let error = BoundPlatform::new("ecs", "tkd", fake_binding("compose"), fake_frontend())
            .expect_err("binding mismatch");
        assert!(error.to_string().contains("expects platform `ecs`"));

        let error = BoundPlatform::new("compose", "tkdp", fake_binding("compose"), fake_frontend())
            .expect_err("frontend mismatch");
        assert!(
            error
                .to_string()
                .contains("expects definition format `tkdp`")
        );
    }

    fn write_metadata(root: &Path, platform: &str, format: &str) {
        std::fs::write(
            root.join(METADATA_JSON),
            serde_json::to_vec_pretty(&serde_json::json!({
                "name": "demo",
                "id": "7698ae09-197e-4325-9f77-256dac98f23a",
                "platform": platform,
                "launch_class": "bound-provisioner",
                "definition": {
                    "format": format,
                    "path": "definition.tkd"
                }
            }))
            .expect("metadata"),
        )
        .expect("write metadata");
    }

    #[test]
    fn recorded_platform_and_format_are_admitted_before_source_access() {
        let root = tempfile::tempdir().expect("deployment");
        let platform =
            BoundPlatform::new("compose", "tkd", fake_binding("compose"), fake_frontend())
                .expect("bound platform");

        write_metadata(root.path(), "ecs", "tkd");
        let error = platform
            .admit_deployment(root.path())
            .expect_err("platform mismatch");
        assert!(error.to_string().contains("selects platform `ecs`"));

        write_metadata(root.path(), "compose", "tkdp");
        let error = platform
            .admit_deployment(root.path())
            .expect_err("format mismatch");
        assert!(
            error
                .to_string()
                .contains("selects definition format `tkdp`")
        );
    }
}
