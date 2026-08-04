//! Diagnostic mapping battery (spike-carried): Monty failures land on `.tkdp`
//! positions — case bodies, guards, subjects, code outside any match — and
//! facade frames are labelled internal rather than leaking transient
//! coordinates.
//
// Feature: tkdp-frontend, Property 6: failure positions are operator
// positions.

use serde::Serialize;
use tokeira_orchestrator::RelativeDefinitionPath;
use tokeira_platform::{
    author::LocatedValue,
    definition::{DefinitionFrontend, DefinitionSourceName, FrontendSource},
    error::KindError,
    kind::{KindFunctions, PlacementContext, ProviderKind},
};
use tokeira_tkdp::frontend;

/// These sources fail before any kind decodes, so the platform is kind-less.
#[derive(Debug, Clone, PartialEq)]
enum NoKind {}

impl ProviderKind for NoKind {
    fn kind_name(&self) -> &'static str {
        match *self {}
    }

    fn validate_input(&self) -> Result<(), KindError> {
        match *self {}
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        match *self {}
    }

    fn desired_manifest(&self, _placement: &PlacementContext) -> serde_json::Value {
        match *self {}
    }

    fn realize(
        &self,
        _placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        match *self {}
    }
}

#[derive(Debug, Serialize)]
struct Ctx {
    project_name: String,
}

fn failure(source: &str) -> String {
    let kinds: KindFunctions<NoKind> = KindFunctions {
        names: &[],
        contains: |_| false,
        defaults: |_| None::<LocatedValue>,
        decode: |name, _| Err(KindError::new(format!("unknown kind `{name}`"))),
    };
    let path = RelativeDefinitionPath::new("definition.tkdp").expect("path");
    let source_name = DefinitionSourceName::DeploymentRelative(path);
    let ctx = Ctx {
        project_name: "demo".to_string(),
    };
    frontend()
        .evaluate(
            FrontendSource {
                source_name: &source_name,
                bytes: source.as_bytes(),
            },
            &ctx,
            kinds,
        )
        .expect_err("source must fail")
        .message
}

#[test]
fn runtime_error_in_case_body_maps_to_original_line() {
    let source = "\
def config():
    return None


def deployment(cfg, cx):
    x = 1
    match x:
        case 1:
            boom = 1 / 0
";
    let rendered = failure(source);
    assert!(rendered.contains("ZeroDivisionError"), "{rendered}");
    assert!(rendered.contains("definition.tkdp:9:"), "{rendered}");
    assert!(rendered.contains("boom = 1 / 0"), "{rendered}");
}

#[test]
fn error_in_guard_maps_to_guard_expression() {
    let source = "\
def config():
    return None


def deployment(cfg, cx):
    match 1:
        case x if x / 0 > 1:
            pass
        case _:
            pass
";
    let rendered = failure(source);
    assert!(rendered.contains("ZeroDivisionError"), "{rendered}");
    // The guard is spliced verbatim, so the position is the guard's own line.
    assert!(rendered.contains("definition.tkdp:7:"), "{rendered}");
}

#[test]
fn error_in_subject_maps_to_subject_expression() {
    let source = "\
def config():
    return None


def deployment(cfg, cx):
    match missing_name:
        case _:
            pass
";
    let rendered = failure(source);
    assert!(rendered.contains("NameError"), "{rendered}");
    assert!(rendered.contains("definition.tkdp:6:"), "{rendered}");
}

#[test]
fn facade_frames_are_labelled_internal() {
    // A missing pattern field raises inside the facade's match helper; the
    // rendered failure names the facade region rather than leaking transient
    // coordinates, and still carries the mapped pattern position.
    let source = "\
from dataclasses import dataclass


@dataclass
class P:
    region: str


def config():
    return None


def deployment(cfg, cx):
    match P(region=\"eu\"):
        case P(endpoint=e):
            pass
";
    let rendered = failure(source);
    assert!(rendered.contains("does not exist on P"), "{rendered}");
    assert!(
        rendered.contains("in the tokeira facade (internal)"),
        "{rendered}"
    );
    assert!(rendered.contains("definition.tkdp:15:"), "{rendered}");
    assert!(!rendered.contains("transient"), "{rendered}");
}

#[test]
fn error_outside_any_match_maps_identically() {
    let source = "\
def config():
    return undefined_setting


def deployment(cfg, cx):
    pass
";
    let rendered = failure(source);
    assert!(rendered.contains("NameError"), "{rendered}");
    assert!(rendered.contains("definition.tkdp:2:"), "{rendered}");
    assert!(rendered.contains("return undefined_setting"), "{rendered}");
}
