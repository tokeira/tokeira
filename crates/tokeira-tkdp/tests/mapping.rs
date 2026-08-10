//! Diagnostic mapping battery (spike-carried): Monty failures land on `.tkdp`
//! positions — case bodies, guards, subjects, code outside any match — and
//! facade frames are labelled internal rather than leaking transient
//! coordinates.
//
// Feature: tkdp-frontend, Property 6: failure positions are operator
// positions.

use serde::Serialize;
use tokeira_orchestrator::RelativeDefinitionPath;
use tokeira_platform::definition::{DefinitionFrontend, DefinitionSourceName, FrontendSource};
use tokeira_tkdp::frontend;

#[derive(Debug, Serialize)]
struct Ctx {
    project_name: String,
}

fn failure(source: &str) -> String {
    // These sources fail before any kind decodes, so the platform exposes no
    // resource namespaces.
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
            &[],
            &tokeira_platform::definition::NoPartSources,
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
