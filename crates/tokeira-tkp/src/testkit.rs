//! Test scaffolding for the bound path: a stub frontend, a declaration
//! fixture, and an admitted temp deployment — everything a verb test needs
//! to drive the real engine over a minimal evaluable world.

use std::{path::Path, sync::Arc};

use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    author::{LocatedValue, ValueShape},
    declaration::{DeploymentRef, PlatformDeclaration, PlatformExecution, PlatformIntegration},
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource},
    error::FrontendDiagnostic,
    graph::StructuralGraphBuilder,
    kind::DecodedKind,
};

use crate::{
    engine::Engine,
    platform::{Admitted, BoundPlatform},
};

/// A frontend whose evaluation is canned: one empty dependency-free module
/// (the bootstrap the execution state requires). Retarget refuses when
/// constructed refusing — the gate tests' fixture.
#[derive(Clone)]
pub(crate) struct StubFrontend {
    format: DefinitionFormatId,
    refuse_retarget: Vec<String>,
}

impl StubFrontend {
    pub(crate) fn new() -> Self {
        Self {
            format: DefinitionFormatId::new("tkd").expect("static format id"),
            refuse_retarget: Vec::new(),
        }
    }

    pub(crate) fn refusing_retarget(messages: Vec<String>) -> Self {
        Self {
            refuse_retarget: messages,
            ..Self::new()
        }
    }
}

impl DefinitionFrontend for StubFrontend {
    fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    fn evaluate<C: serde::Serialize>(
        &self,
        _source: FrontendSource<'_>,
        _context: &C,
        _namespaces: &[tokeira_platform::definition::Namespace],
        _parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> std::result::Result<FrontendOutput, FrontendDiagnostic> {
        let mut graph = StructuralGraphBuilder::<DecodedKind>::new();
        graph.add_module("state", Vec::new());
        Ok(FrontendOutput {
            config: LocatedValue::new(ValueShape::Unit),
            graph: graph.finish().expect("the one-module graph verifies"),
        })
    }

    fn retarget_check<C: serde::Serialize>(
        &self,
        _prior: FrontendSource<'_>,
        _current: FrontendSource<'_>,
        _context: &C,
        _namespaces: &[tokeira_platform::definition::Namespace],
        _prior_parts: &dyn tokeira_platform::definition::SourceResolver,
        _current_parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> std::result::Result<(), Vec<String>> {
        if self.refuse_retarget.is_empty() {
            Ok(())
        } else {
            Err(self.refuse_retarget.clone())
        }
    }
}

/// A probe answering a fixed result: `None` for the reachable world, an
/// issue for the blocked one.
#[derive(Debug)]
pub(crate) struct FixedProbe(pub(crate) Option<tokeira_iac::PlatformIssue>);

#[async_trait::async_trait]
impl PlatformExecution for FixedProbe {
    async fn probe(
        &self,
        _deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
        Ok(self.0.clone())
    }
}

/// Platform implementation for definition-driven framework tests: no shared
/// extensions, and an applier that accepts exactly the manifests supplied.
#[derive(Debug)]
pub(crate) struct TestIntegration;

#[async_trait::async_trait]
impl PlatformIntegration for TestIntegration {
    async fn register_infra_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_deploy_engine::ServiceContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn register_image_extensions(
        &self,
        _deployment: &DeploymentRef,
        _ctx: &mut tokeira_deploy_engine::ImageContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn service_platform(
        &self,
        _deployment: &DeploymentRef,
    ) -> anyhow::Result<Box<dyn tokeira_deploy_engine::Platform>> {
        Ok(Box::new(TestServicePlatform))
    }
}

#[derive(Debug)]
struct TestServicePlatform;

#[async_trait::async_trait]
impl tokeira_deploy_engine::Platform for TestServicePlatform {
    async fn apply_manifests(
        &self,
        manifests: &[serde_json::Value],
    ) -> std::result::Result<usize, tokeira_deploy_engine::RuntimeError> {
        Ok(manifests.len())
    }
}

/// A platform-issue value in the class the direction table establishes
/// nothing from, so `direction: None` is a world the real table produces.
pub(crate) fn unreachable_issue() -> tokeira_iac::PlatformIssue {
    tokeira_iac::PlatformIssue {
        component: "Test".to_string(),
        fact: "Unable to reach the test substrate".to_string(),
        evidence: "connection refused".to_string(),
        direction: None,
    }
}

fn declaration(probe: FixedProbe) -> PlatformDeclaration {
    PlatformDeclaration {
        namespaces: Vec::new(),
        ops: None,
        observability: None,
        execution: Box::new(probe),
        implementation: Arc::new(TestIntegration),
    }
}

/// Write the minimal admissible deployment into `dir`: binding metadata plus
/// the recorded (empty) definition document the stub frontend ignores.
pub(crate) fn write_deployment(dir: &Path) {
    std::fs::write(
        dir.join("metadata.json"),
        serde_json::json!({
            "name": "test",
            "id": "00000000-0000-0000-0000-000000000000",
            "platform": "test",
            "definition": {"format": "tkd", "path": "definition.tkd"}
        })
        .to_string(),
    )
    .expect("test metadata writes");
    std::fs::write(dir.join("definition.tkd"), "").expect("test definition writes");
}

/// A bound engine over the stub world, with `dir` admitted: the triple every
/// verb test starts from.
pub(crate) fn engine_over(
    dir: &Path,
    probe: FixedProbe,
    frontend: StubFrontend,
) -> (Engine<StubFrontend>, Admitted) {
    write_deployment(dir);
    let platform =
        BoundPlatform::bind("test", "tkd", declaration(probe)).expect("test declaration binds");
    let admitted = platform
        .admit_deployment(dir)
        .expect("the test deployment admits");
    let engine = Engine::new(platform, frontend).expect("format agreement holds");
    (engine, admitted)
}

/// The reachable-world engine most tests want.
pub(crate) fn engine(dir: &Path) -> (Engine<StubFrontend>, Admitted) {
    engine_over(dir, FixedProbe(None), StubFrontend::new())
}

/// Realize the creation record test verbs require without routing through a
/// CLI lifecycle command. Production creation is owned by `tkr`; this helper
/// supplies the same envelope/source precondition to shell unit tests.
pub(crate) async fn realize_creation(admitted: &Admitted) {
    let deployment_dir = &admitted.deployment_ref.dir;
    let store = crate::envelope_store(deployment_dir);
    let (existing, version) = store.load().await.expect("load test envelope");
    assert!(existing.binding.is_none(), "test deployment starts unbound");
    crate::config_history::snapshot(deployment_dir, &admitted.config_source(), 0)
        .expect("retain test revision zero");
    let envelope = tokeira_deployment::DeploymentStateEnvelope {
        deployment_id: admitted.deployment_ref.name.clone(),
        binding: Some(tokeira_deployment::ProvenanceStamp::current(
            chrono::Utc::now(),
        )),
        integrity: Some(crate::running_integrity_manifest().expect("running test manifest")),
        config_revision: 0,
        ..Default::default()
    };
    store
        .save(&envelope, &version)
        .await
        .expect("save test creation record");
}
