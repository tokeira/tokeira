//! Test scaffolding for the bound path: a stub frontend, a declaration
//! fixture, and an admitted temp deployment — everything a verb test needs
//! to drive the real engine over a minimal evaluable world.

use std::path::Path;

use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    author::{LocatedValue, ValueShape},
    declaration::{DeploymentRef, KindSet, PlatformDeclaration, ProviderExecution, ProviderExport},
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource},
    error::FrontendDiagnostic,
    graph::StructuralGraphBuilder,
    kind::DecodedKind,
};

use crate::{
    engine::Engine,
    platform::{Admitted, BoundPlatform},
};

/// A frontend whose evaluation is canned: unit config, one empty
/// dependency-free module (the bootstrap the execution state requires).
/// Retarget refuses when constructed refusing — the gate tests' fixture.
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
        _vocabulary: &tokeira_platform::declaration::Vocabulary,
        _parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> std::result::Result<FrontendOutput, FrontendDiagnostic> {
        let mut graph = StructuralGraphBuilder::<DecodedKind>::new();
        graph.add_module("state", Vec::new());
        Ok(FrontendOutput {
            config: LocatedValue {
                value: ValueShape::Unit,
                range: None,
            },
            graph: graph.finish().expect("the one-module graph verifies"),
        })
    }

    fn retarget_check<C: serde::Serialize>(
        &self,
        _prior: FrontendSource<'_>,
        _current: FrontendSource<'_>,
        _context: &C,
        _vocabulary: &tokeira_platform::declaration::Vocabulary,
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
impl ProviderExecution for FixedProbe {
    async fn probe(
        &self,
        _deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>> {
        Ok(self.0.clone())
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
    PlatformDeclaration::on(ProviderExport {
        kinds: KindSet::new("test", Vec::new()),
        ops: None,
        execution: Box::new(probe),
        infra: None,
        workload: None,
    })
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

/// The bound platform alone, for tests asserting admission outcomes
/// directly (a refused admission never yields an engine pair).
pub(crate) fn bound_platform() -> BoundPlatform {
    BoundPlatform::bind("test", "tkd", declaration(FixedProbe(None)))
        .expect("test declaration binds")
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
