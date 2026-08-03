//! Static adapter from one platform binding and one frontend to the lifecycle shell.

use std::{collections::HashSet, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::{
    DefinitionFormatId, DeployEngine, Deployment as _, InfraEngine, PlatformId, PlatformLaunchClass,
};
use tokeira_platform::{
    artifact::OperationalArtifactStage,
    binding::{Platform, PlatformBinding},
    context::InvocationContext,
    definition::{
        DefinitionEngine, DefinitionFrontend, DefinitionRequest, DefinitionSource,
        DefinitionSourceName,
    },
    projection::FrameworkDeployment,
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

    fn invocation_input(
        &self,
        deployment_dir: &Path,
        metadata: &DeploymentBindingMetadata,
    ) -> InvocationContext {
        InvocationContext {
            deployment_id: metadata.name.clone(),
            deployment_uuid: metadata.id,
            environment: None,
            region: None,
            account_id: None,
            deployment_dir: deployment_dir.to_path_buf(),
        }
    }

    fn invocation(
        &self,
        deployment_dir: &Path,
        metadata: &DeploymentBindingMetadata,
    ) -> Result<P::Context> {
        self.binding
            .context
            .construct(&self.invocation_input(deployment_dir, metadata))
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

    fn framework(&self, deployment_dir: &Path) -> Result<FrameworkDeployment<P>> {
        let metadata = self.metadata(deployment_dir)?;
        let definition = metadata
            .definition
            .as_ref()
            .expect("metadata admission requires a definition");
        let source_path = deployment_dir.join(definition.path.as_path());
        let evaluated = self.verified_definition(deployment_dir, &source_path, false)?;
        FrameworkDeployment::new(
            evaluated,
            self.binding.clone(),
            self.invocation_input(deployment_dir, &metadata),
        )
        .map_err(Into::into)
    }

    async fn infra_engine(
        &self,
        deployment_dir: &Path,
    ) -> Result<(
        InfraEngine<FrameworkDeployment<P>>,
        tokeira_iac::InfraComposition,
    )> {
        let framework = self.framework(deployment_dir)?;
        let config = framework.engine_config();
        let engine = InfraEngine::new(framework, &config, deployment_dir)
            .await
            .context("failed to open the generic infrastructure engine")?;
        let composition = engine.compose(tokeira_iac::ModuleSelection::All)?;
        Ok((engine, composition))
    }

    async fn apply_infra(
        &self,
        deployment_dir: &Path,
        materialize_artifacts: bool,
    ) -> Result<AppliedOutcome> {
        let (mut engine, composition) = self.infra_engine(deployment_dir).await?;
        if materialize_artifacts {
            let (framework, context) = engine.deployment_and_context_mut();
            framework
                .materialize_operational_artifacts(
                    OperationalArtifactStage::Infrastructure,
                    context,
                )
                .await
                .context("operational artifact publication failed before infrastructure apply")?;
        }
        let changes = engine
            .apply(&composition, tokeira_iac::ModuleSelection::All)
            .await
            .context("infrastructure apply failed")?;
        Ok(AppliedOutcome {
            display_by_id: engine.display_map(&composition)?,
            changes,
        })
    }
}

fn service_changes(changes: Vec<tokeira_orchestrator::ServiceChange>) -> Vec<tokeira_iac::Change> {
    changes
        .into_iter()
        .map(|change| tokeira_iac::Change {
            kind: match change.kind {
                tokeira_orchestrator::ServiceChangeKind::Create => tokeira_iac::ChangeKind::Create,
                tokeira_orchestrator::ServiceChangeKind::Update => tokeira_iac::ChangeKind::Update,
                tokeira_orchestrator::ServiceChangeKind::Delete => tokeira_iac::ChangeKind::Delete,
                tokeira_orchestrator::ServiceChangeKind::NoChange => {
                    tokeira_iac::ChangeKind::NoChange
                }
            },
            resource_type: "service".to_string(),
            module: change.module,
            resource: change.service,
            details: Vec::new(),
        })
        .collect()
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

    async fn infra_plan(&self, deployment_dir: &Path) -> Result<tokeira_iac::PlanOutcome> {
        let (mut engine, composition) = self.infra_engine(deployment_dir).await?;
        engine
            .plan(&composition, tokeira_iac::ModuleSelection::All)
            .await
            .context("infrastructure plan failed")
    }

    async fn infra_apply(&self, deployment_dir: &Path) -> Result<AppliedOutcome> {
        self.apply_infra(deployment_dir, false).await
    }

    async fn infra_apply_with_artifacts(&self, deployment_dir: &Path) -> Result<AppliedOutcome> {
        self.apply_infra(deployment_dir, true).await
    }

    async fn publish_inspection(&self, deployment_dir: &Path) -> Result<usize> {
        self.framework(deployment_dir)?
            .publish_inspection()
            .map(|publications| publications.len())
            .context("inspection artifact publication failed")
    }

    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize> {
        let (mut engine, composition) = self.infra_engine(deployment_dir).await?;
        Ok(engine
            .destroy(&composition, tokeira_iac::ModuleSelection::All)
            .await
            .context("infrastructure destroy failed")?
            .len())
    }

    async fn infra_destroy_selected(
        &self,
        deployment_dir: &Path,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        let (mut engine, composition) = self.infra_engine(deployment_dir).await?;
        let ids = ids
            .iter()
            .cloned()
            .map(tokeira_iac::ResourceId)
            .collect::<HashSet<_>>();
        let changes = engine
            .destroy_selected(&composition, &ids)
            .await
            .context("selected infrastructure destroy failed")?;
        Ok(crate::change_log_entries(&changes))
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

    async fn recorded_state(&self, deployment_dir: &Path) -> Result<tokeira_iac::InfraState> {
        let framework = self.framework(deployment_dir)?;
        let config = framework.engine_config();
        let (state, _) = framework
            .create_infra_store(&config, deployment_dir)
            .load()
            .await
            .context("failed to load recorded infrastructure state")?;
        Ok(state)
    }

    async fn deploy_plan(
        &self,
        deployment_dir: &Path,
    ) -> Result<Realization<tokeira_iac::PlanOutcome>> {
        let framework = self.framework(deployment_dir)?;
        if !framework.has_runtime_workloads() {
            return if framework.has_workloads() {
                Ok(Realization::Realized(
                    self.infra_plan(deployment_dir).await?,
                ))
            } else {
                Ok(Realization::NotApplicable {
                    reason: "the definition declares no runtime workloads",
                })
            };
        }
        let config = framework.engine_config();
        let mut engine = DeployEngine::new(framework, &config, deployment_dir)
            .await
            .context("failed to open the generic deploy engine")?;
        Ok(Realization::Realized(tokeira_iac::PlanOutcome {
            changes: service_changes(engine.plan().await.context("workload plan failed")?),
            ..Default::default()
        }))
    }

    async fn deploy_apply(&self, deployment_dir: &Path) -> Result<Realization<AppliedOutcome>> {
        let framework = self.framework(deployment_dir)?;
        if !framework.has_runtime_workloads() {
            return if framework.has_workloads() {
                Ok(Realization::Realized(
                    self.infra_apply_with_artifacts(deployment_dir).await?,
                ))
            } else {
                Ok(Realization::NotApplicable {
                    reason: "the definition declares no runtime workloads",
                })
            };
        }
        let platform = framework.deploy_platform()?.ok_or_else(|| {
            anyhow::anyhow!(
                "runtime workloads are declared but no selected provider supplies a deploy executor"
            )
        })?;
        let mut operational_context = framework.prepare_operational_context().await?;
        framework
            .materialize_operational_artifacts(
                OperationalArtifactStage::Workload,
                &mut operational_context,
            )
            .await
            .context("operational artifact publication failed before workload apply")?;
        let config = framework.engine_config();
        let mut engine = DeployEngine::new(framework, &config, deployment_dir)
            .await
            .context("failed to open the generic deploy engine")?;
        let changes = service_changes(
            engine
                .apply(platform.as_ref())
                .await
                .context("workload apply failed")?,
        );
        let display_by_id = changes
            .iter()
            .map(|change| {
                (
                    tokeira_iac::ResourceId(format!("service/{}", change.resource)),
                    change.resource.clone(),
                )
            })
            .collect();
        Ok(Realization::Realized(AppliedOutcome {
            changes,
            display_by_id,
        }))
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

    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use tokeira_platform::{
        artifact::{
            ArtifactCatalog, ArtifactClass, ArtifactUse, CanonicalDocument, ContentIdentitySet,
            DeliveryKey, DesiredContent, DesiredDocument, OperationalArtifactReceipt,
            OperationalArtifactRequest, PlatformArtifact,
        },
        author::{
            AuthorArgument, AuthorHandle, AuthorNode, AuthorResult, AuthorSession, AuthorValue,
        },
        binding::{StateBinding, StatePolicy},
        catalog::{
            DeliveryProjection, HealthDeclaration, ImageCatalog, ImageSelection, KindSet,
            PlacementContext, PlacementDeclaration, PlatformService, ProviderDelivery,
            ProviderExecution, ProviderSet, ServiceCatalog,
        },
        config::{ConfigContract, PlatformConfig},
        context::{ContextArgument, ContextContract, ContextProjection, PlatformContext},
        error::{ConfigError, ContextError, DeliveryError, FrontendDiagnostic},
        graph::WorkloadDeclaration,
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
        fake_binding_with(id, ServiceCatalog::default(), ProviderSet::default())
    }

    fn fake_binding_with(
        id: &str,
        services: ServiceCatalog<FakePlatform>,
        providers: ProviderSet<FakePlatform>,
    ) -> PlatformBinding<FakePlatform> {
        fake_binding_with_artifacts(id, services, ArtifactCatalog::default(), providers)
    }

    fn fake_binding_with_artifacts(
        id: &str,
        services: ServiceCatalog<FakePlatform>,
        artifacts: ArtifactCatalog<FakePlatform>,
        providers: ProviderSet<FakePlatform>,
    ) -> PlatformBinding<FakePlatform> {
        let images = ImageCatalog::new(
            services
                .entries()
                .iter()
                .map(|service| service.image.logical_id.clone())
                .collect(),
        );
        PlatformBinding::new(
            PlatformId::new(id).expect("platform id"),
            "bootstrap",
            ConfigContract::new(),
            ContextContract::new(fake_context, authoring_context),
            KindSet::default(),
            services,
            artifacts,
            images,
            providers,
            StateBinding::new(StatePolicy::LocalCas),
            PlatformOps::default(),
            Vec::new(),
        )
        .expect("binding")
    }

    #[derive(Debug, Clone)]
    struct FakeFrontend {
        format: DefinitionFormatId,
        workload: bool,
    }

    impl DefinitionFrontend<FakePlatform> for FakeFrontend {
        fn format(&self) -> &DefinitionFormatId {
            &self.format
        }

        fn evaluate(
            &self,
            _source: tokeira_platform::definition::FrontendSource<'_>,
            author: &mut AuthorSession<FakePlatform>,
        ) -> Result<tokeira_platform::definition::FrontendOutput, FrontendDiagnostic> {
            let AuthorResult::Handle(AuthorHandle::Deployment(deployment)) = author
                .associated("Deployment.new", Vec::new())
                .expect("deployment constructor")
            else {
                panic!("Deployment.new returns a deployment handle");
            };
            let AuthorResult::Handle(AuthorHandle::Module(bootstrap)) = author
                .call(
                    AuthorHandle::Deployment(deployment.clone()),
                    "module",
                    vec![AuthorArgument::Value(AuthorNode::string("bootstrap"))],
                )
                .expect("bootstrap module")
            else {
                panic!("Deployment.module returns a module handle");
            };
            if self.workload {
                author
                    .call(
                        AuthorHandle::Module(bootstrap),
                        "workload",
                        vec![
                            AuthorArgument::Value(AuthorNode::string("server")),
                            AuthorArgument::Value(AuthorNode::new(AuthorValue::Integer(1))),
                        ],
                    )
                    .expect("runtime workload");
            }
            Ok(tokeira_platform::definition::FrontendOutput {
                config: AuthorNode::new(AuthorValue::Unit),
                deployment,
            })
        }
    }

    fn fake_frontend() -> FakeFrontend {
        FakeFrontend {
            format: DefinitionFormatId::new("tkd").expect("format"),
            workload: false,
        }
    }

    #[derive(Debug)]
    struct TestService {
        name: String,
        module: String,
        dependencies: Vec<String>,
        document: serde_json::Value,
    }

    impl tokeira_orchestrator::DeployService for TestService {
        fn name(&self) -> &str {
            &self.name
        }

        fn module(&self) -> &str {
            &self.module
        }

        fn dependencies(&self) -> Vec<&str> {
            self.dependencies.iter().map(String::as_str).collect()
        }

        fn manifests(
            &self,
            _context: &tokeira_orchestrator::ServiceContext,
        ) -> Result<Vec<serde_json::Value>, tokeira_orchestrator::DeployRuntimeError> {
            Ok(vec![self.document.clone()])
        }
    }

    #[derive(Debug)]
    struct TestDelivery {
        key: DeliveryKey,
        publications: Arc<AtomicUsize>,
        runtime: bool,
    }

    #[async_trait]
    impl ProviderDelivery for TestDelivery {
        fn key(&self) -> &DeliveryKey {
            &self.key
        }

        fn canonicalize(
            &self,
            document: &DesiredDocument,
        ) -> Result<CanonicalDocument, DeliveryError> {
            Ok(CanonicalDocument {
                bytes: serde_json::to_vec(&document.value)
                    .map_err(|error| DeliveryError::new(error.to_string()))?,
            })
        }

        fn realize(
            &self,
            declaration: &WorkloadDeclaration,
            placement: &PlacementContext,
            _content: &ContentIdentitySet,
        ) -> Result<DeliveryProjection, DeliveryError> {
            if self.runtime {
                Ok(DeliveryProjection::Workload(Box::new(TestService {
                    name: declaration.service.clone(),
                    module: placement.module.clone(),
                    dependencies: declaration.dependencies.clone(),
                    document: declaration.document.value.clone(),
                })))
            } else {
                Ok(DeliveryProjection::Infrastructure(Box::new(
                    TestInfraResource {
                        id: tokeira_iac::ResourceId(declaration.service.clone()),
                        module: placement.module.clone(),
                    },
                )))
            }
        }

        async fn materialize_operational(
            &self,
            request: OperationalArtifactRequest<'_>,
            _context: &tokeira_iac::ProvisionContext,
        ) -> Result<OperationalArtifactReceipt, DeliveryError> {
            self.publications.fetch_add(1, Ordering::SeqCst);
            Ok(OperationalArtifactReceipt {
                artifact: request.artifact.logical_id.clone(),
                provider_reference: format!("test/{}", request.artifact.logical_id),
                identity: request.identity.clone(),
                consumers: request.artifact.consumers.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct TestInfraResource {
        id: tokeira_iac::ResourceId,
        module: String,
    }

    #[async_trait]
    impl tokeira_iac::Resource for TestInfraResource {
        fn resource_type(&self) -> tokeira_iac::ResourceType {
            tokeira_iac::ResourceType::new("test-infra-service")
        }

        fn resource_id(&self) -> tokeira_iac::ResourceId {
            self.id.clone()
        }

        fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
            Vec::new()
        }

        fn module(&self) -> &str {
            &self.module
        }

        async fn create(
            &self,
            _context: &tokeira_iac::ProvisionContext,
        ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
            Ok(tokeira_iac::ResourceState {
                resource_type: self.resource_type(),
                physical_id: self.id.0.clone(),
                properties: serde_json::json!({"service": self.id.0}),
                dependencies: Vec::new(),
                created_at: "created".into(),
                updated_at: "created".into(),
                module: self.module.clone(),
            })
        }

        async fn update(
            &self,
            current: &tokeira_iac::ResourceState,
            _context: &tokeira_iac::ProvisionContext,
        ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
            Ok(current.clone())
        }

        async fn delete(
            &self,
            _current: &tokeira_iac::ResourceState,
            _context: &tokeira_iac::ProvisionContext,
        ) -> Result<(), tokeira_iac::IacError> {
            Ok(())
        }

        async fn describe(
            &self,
            _context: &tokeira_iac::ProvisionContext,
        ) -> Result<tokeira_iac::DescribeResult, tokeira_iac::IacError> {
            Ok(tokeira_iac::DescribeResult::Absent)
        }

        fn diff(
            &self,
            _current: &tokeira_iac::ResourceState,
            _context: &tokeira_iac::ProvisionContext,
        ) -> tokeira_iac::InternalChange {
            tokeira_iac::InternalChange::NoChange {
                resource_id: self.id.clone(),
            }
        }
    }

    #[derive(Debug)]
    struct TestRuntimePlatform {
        applies: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl tokeira_orchestrator::DeployPlatform for TestRuntimePlatform {
        async fn apply_manifests(
            &self,
            manifests: &[serde_json::Value],
        ) -> Result<usize, tokeira_orchestrator::DeployRuntimeError> {
            self.applies.fetch_add(1, Ordering::SeqCst);
            Ok(manifests.len())
        }
    }

    struct TestExecution {
        platform: Arc<dyn tokeira_orchestrator::DeployPlatform>,
    }

    impl std::fmt::Debug for TestExecution {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestExecution").finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl ProviderExecution<FakePlatform> for TestExecution {
        fn provider(&self) -> &str {
            "test"
        }

        fn deploy_platform(&self) -> Option<Arc<dyn tokeira_orchestrator::DeployPlatform>> {
            Some(Arc::clone(&self.platform))
        }
    }

    fn fake_service_binding(
        applies: Arc<AtomicUsize>,
        publications: Option<Arc<AtomicUsize>>,
        runtime: bool,
    ) -> PlatformBinding<FakePlatform> {
        let has_artifact = publications.is_some();
        let publications = publications.unwrap_or_else(|| Arc::new(AtomicUsize::new(0)));
        let delivery = Arc::new(TestDelivery {
            key: DeliveryKey::new("test").expect("delivery key"),
            publications,
            runtime,
        });
        let service = PlatformService {
            logical_id: "server".into(),
            image: ImageSelection {
                logical_id: "server".into(),
            },
            command: Vec::new(),
            ports: Vec::new(),
            health: HealthDeclaration::default(),
            placement: PlacementDeclaration::default(),
            configuration: has_artifact
                .then(|| ArtifactUse {
                    artifact: "runtime-config".into(),
                    role: "server-config".into(),
                })
                .into_iter()
                .collect(),
            delivery: delivery.key().clone(),
            document: DesiredDocument {
                schema: "test-service-v1".into(),
                value: serde_json::json!({"service": "server"}),
            },
        };
        let artifacts = has_artifact
            .then(|| PlatformArtifact {
                logical_id: "runtime-config".into(),
                class: ArtifactClass::Operational,
                content: DesiredContent::Text("runtime = true\n".into()),
                consumers: vec!["server".into()],
                delivery: delivery.key().clone(),
            })
            .into_iter()
            .collect();
        fake_binding_with_artifacts(
            "compose",
            ServiceCatalog::new(vec![service]),
            ArtifactCatalog::new(artifacts),
            ProviderSet::with_executions(
                vec![delivery],
                vec![Arc::new(TestExecution {
                    platform: Arc::new(TestRuntimePlatform { applies }),
                })],
            ),
        )
    }

    fn fake_runtime_binding(
        applies: Arc<AtomicUsize>,
        publications: Option<Arc<AtomicUsize>>,
    ) -> PlatformBinding<FakePlatform> {
        fake_service_binding(applies, publications, true)
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

    #[tokio::test]
    async fn bound_platform_executes_the_generic_engine_lifecycle() {
        let root = tempfile::tempdir().expect("deployment");
        write_metadata(root.path(), "compose", "tkd");
        std::fs::write(root.path().join("definition.tkd"), "definition").expect("definition");
        let platform =
            BoundPlatform::new("compose", "tkd", fake_binding("compose"), fake_frontend())
                .expect("bound platform");

        let plan = platform.infra_plan(root.path()).await.expect("plan");
        assert!(plan.changes.is_empty());
        let applied = platform.infra_apply(root.path()).await.expect("apply");
        assert!(applied.changes.is_empty());
        assert!(
            platform
                .recorded_state(root.path())
                .await
                .expect("recorded state")
                .resources
                .is_empty()
        );
        assert_eq!(
            platform.infra_destroy(root.path()).await.expect("destroy"),
            0
        );
        assert!(matches!(
            platform
                .deploy_plan(root.path())
                .await
                .expect("deploy plan"),
            Realization::NotApplicable { .. }
        ));
    }

    #[tokio::test]
    async fn apply_publishes_only_artifacts_consumed_in_its_engine_universe() {
        let root = tempfile::tempdir().expect("deployment");
        write_metadata(root.path(), "compose", "tkd");
        std::fs::write(root.path().join("definition.tkd"), "definition").expect("definition");
        let publications = Arc::new(AtomicUsize::new(0));
        let platform = BoundPlatform::new(
            "compose",
            "tkd",
            fake_runtime_binding(
                Arc::new(AtomicUsize::new(0)),
                Some(Arc::clone(&publications)),
            ),
            FakeFrontend {
                format: DefinitionFormatId::new("tkd").expect("format"),
                workload: true,
            },
        )
        .expect("bound platform");

        let _ = platform.infra_plan(root.path()).await.expect("plan");
        assert_eq!(publications.load(Ordering::SeqCst), 0);
        let _ = platform
            .infra_apply(root.path())
            .await
            .expect("rollback-safe core reconcile");
        assert_eq!(publications.load(Ordering::SeqCst), 0);
        let _ = platform
            .infra_apply_with_artifacts(root.path())
            .await
            .expect("ordinary apply");
        assert_eq!(publications.load(Ordering::SeqCst), 0);
        let _ = platform
            .deploy_apply(root.path())
            .await
            .expect("workload apply");
        assert_eq!(publications.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deploy_apply_publishes_infrastructure_service_artifacts() {
        let root = tempfile::tempdir().expect("deployment");
        write_metadata(root.path(), "compose", "tkd");
        std::fs::write(root.path().join("definition.tkd"), "definition").expect("definition");
        let publications = Arc::new(AtomicUsize::new(0));
        let platform = BoundPlatform::new(
            "compose",
            "tkd",
            fake_service_binding(
                Arc::new(AtomicUsize::new(0)),
                Some(Arc::clone(&publications)),
                false,
            ),
            FakeFrontend {
                format: DefinitionFormatId::new("tkd").expect("format"),
                workload: true,
            },
        )
        .expect("bound platform");

        let Realization::Realized(applied) = platform
            .deploy_apply(root.path())
            .await
            .expect("infra-only deploy apply")
        else {
            panic!("infrastructure service is realized");
        };
        assert_eq!(applied.changes.len(), 1);
        assert_eq!(publications.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bound_platform_delegates_runtime_workloads_to_the_selected_provider() {
        let root = tempfile::tempdir().expect("deployment");
        write_metadata(root.path(), "compose", "tkd");
        std::fs::write(root.path().join("definition.tkd"), "definition").expect("definition");
        let applies = Arc::new(AtomicUsize::new(0));
        let publications = Arc::new(AtomicUsize::new(0));
        let platform = BoundPlatform::new(
            "compose",
            "tkd",
            fake_runtime_binding(Arc::clone(&applies), Some(Arc::clone(&publications))),
            FakeFrontend {
                format: DefinitionFormatId::new("tkd").expect("format"),
                workload: true,
            },
        )
        .expect("bound platform");

        let Realization::Realized(plan) = platform
            .deploy_plan(root.path())
            .await
            .expect("deploy plan")
        else {
            panic!("runtime workload is realized");
        };
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].kind, tokeira_iac::ChangeKind::Create);
        assert_eq!(publications.load(Ordering::SeqCst), 0);

        let Realization::Realized(applied) = platform
            .deploy_apply(root.path())
            .await
            .expect("deploy apply")
        else {
            panic!("runtime workload is applied");
        };
        assert_eq!(applied.changes.len(), 1);
        assert_eq!(applies.load(Ordering::SeqCst), 1);
        assert_eq!(publications.load(Ordering::SeqCst), 1);

        let Realization::Realized(after) = platform
            .deploy_plan(root.path())
            .await
            .expect("second plan")
        else {
            panic!("runtime workload remains realized");
        };
        assert_eq!(after.changes[0].kind, tokeira_iac::ChangeKind::NoChange);
    }
}
