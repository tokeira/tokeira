//! End-to-end frontend tests: `evaluate` from `.tkdp` bytes to the completed
//! structural output, over a synthetic two-kind platform, with the spike's
//! semantics corpus carried across as writeback-observable fixtures.

use ruff_text_size::TextSize;
use serde::{Deserialize, Serialize};
use tokeira_orchestrator::RelativeDefinitionPath;
use tokeira_platform::{
    author::{LocatedValue, from_located_value},
    definition::{
        DefinitionFrontend, DefinitionSourceName, FrontendOutput, FrontendSource, Namespace,
        NoPartSources,
    },
    error::KindError,
    graph::WritebackValue,
    kind::{DecodedKind, Kind, PlacementContext},
};
use tokeira_platform_definition::tkdp::frontend;

mod support;

use support::{FixtureResource, desired_manifest};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Store {
    #[serde(default)]
    path: String,
    #[serde(default = "default_replicas")]
    replicas: u32,
}

fn default_replicas() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    mode: Option<Mode>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Mode {
    Fast,
    Careful(CarefulMode),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CarefulMode {
    retries: u32,
}

#[derive(Debug, Clone, PartialEq)]
enum TestKind {
    Store(Store),
    Probe(Probe),
}

impl Kind<FixtureResource> for TestKind {
    fn realize(&self, _placement: &PlacementContext) -> Result<FixtureResource, KindError> {
        Ok(match self {
            Self::Store(store) => FixtureResource::new(
                "Store",
                &["endpoint"],
                serde_json::to_value(store).expect("store serializes"),
            ),
            Self::Probe(probe) => FixtureResource::new(
                "Probe",
                &[],
                serde_json::to_value(probe).expect("probe serializes"),
            ),
        })
    }
}

fn decode_kind(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    match name {
        "Probe" => Some({
            let range = value.range;
            from_located_value::<Probe>(value)
                .map(|probe| {
                    DecodedKind::resource::<TestKind, FixtureResource>(
                        "Probe",
                        TestKind::Probe(probe),
                    )
                })
                .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
        }),
        "Store" => Some({
            let range = value.range;
            from_located_value::<Store>(value)
                .map(|store| {
                    DecodedKind::resource::<TestKind, FixtureResource>(
                        "Store",
                        TestKind::Store(store),
                    )
                })
                .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
        }),
        _ => None,
    }
}

fn namespaces() -> [Namespace; 1] {
    [Namespace {
        name: "test",
        kinds: &["Probe", "Store"],
        defaults: None,
        decode: decode_kind,
    }]
}

#[derive(Debug, Serialize)]
struct Ctx {
    project_name: String,
    replica_default: u32,
}

fn ctx() -> Ctx {
    Ctx {
        project_name: "demo".to_string(),
        replica_default: 3,
    }
}

fn evaluate(source: &str) -> Result<FrontendOutput, String> {
    let path = RelativeDefinitionPath::new("definition.tkdp").expect("path");
    let source_name = DefinitionSourceName::DeploymentRelative(path);
    frontend()
        .evaluate(
            FrontendSource {
                source_name: &source_name,
                bytes: source.as_bytes(),
            },
            &ctx(),
            &namespaces(),
            &tokeira_platform::definition::NoPartSources,
        )
        .map_err(|diagnostic| diagnostic.message)
}

/// Wraps a statement body (computing `out`) in a complete definition and
/// returns the recorded writeback literal — the spike corpus's observation
/// channel, now through the full frontend.
fn run_snippet(body: &str) -> Result<String, String> {
    let indented: String = body
        .lines()
        .map(|line| {
            if line.is_empty() {
                "\n".to_string()
            } else {
                format!("    {line}\n")
            }
        })
        .collect();
    let source = format!(
        "from dataclasses import dataclass\n\n\
         from tokeira import Context, Deployment\n\n\n\
         @dataclass\n\
         class Empty:\n    pass\n\n\n\
         @dataclass\n\
         class InMemory:\n    pass\n\n\n\
         @dataclass\n\
         class Dsql:\n    region: str\n    endpoint: str = \"\"\n\n\n\
         def config() -> Empty:\n    return Empty()\n\n\n\
         def deployment(cfg, cx):\n    d = Deployment([\"default\"])\n\
         {indented}    d.writeback(\"out\", out)\n    return d\n"
    );
    let output = evaluate(&source)?;
    let entry = output
        .graph
        .writeback()
        .iter()
        .find(|entry| entry.key() == "out")
        .expect("out writeback");
    match entry.value() {
        WritebackValue::Literal(value) => Ok(value.clone()),
        WritebackValue::Output(_) => panic!("corpus records literals"),
    }
}

// ---------------------------------------------------------------------------
// The exemplar end to end.
// ---------------------------------------------------------------------------

const EXEMPLAR: &str = r#"from dataclasses import dataclass

from tokeira import Context, Deployment, Probe, Store


@dataclass
class InMemory:
    pass


@dataclass
class Dsql:
    region: str
    endpoint: str = ""


@dataclass
class Config:
    storage: InMemory | Dsql
    label: str


def config() -> Config:
    return Config(storage=Dsql(region="eu-west-2"), label="demo")


def deployment(cfg: Config, cx: Context) -> Deployment:
    d = Deployment(["default"])
    base = d.module("base")
    store = base.resource("state", Store(path="/var/lib/" + cx.project_name))

    match cfg.storage:
        case Dsql(region=region, endpoint=_) if region != "":
            data = d.module("data", [base])
            data.resource(
                "probe",
                Probe(label=cfg.label + ":" + region),
                [store],
            )
            d.writeback("infrastructure.region", region)
            d.writeback("infrastructure.endpoint", store.output("endpoint"))
        case InMemory():
            pass

    return d
"#;

#[test]
// Feature: tkdp-frontend, Property 12: structural declaration order is
// preserved through the envelope into the verified graph.
fn exemplar_evaluates_to_the_expected_structure() {
    let output = evaluate(EXEMPLAR).expect("exemplar evaluates");

    let config: TestConfig = from_located_value(output.config).expect("config admits");
    assert_eq!(
        config,
        TestConfig {
            storage: Storage::Dsql(DsqlPayload {
                region: "eu-west-2".to_string(),
                endpoint: String::new(),
            }),
            label: "demo".to_string(),
        }
    );

    let graph = output.graph;
    assert_eq!(graph.namespaces(), ["default"]);
    let modules: Vec<_> = graph
        .modules()
        .iter()
        .map(|m| m.name().to_string())
        .collect();
    assert_eq!(modules, ["base", "data"]);
    assert_eq!(graph.modules()[1].dependencies(), ["base"]);

    let resources = graph.resources();
    assert_eq!(resources.len(), 2);
    assert_eq!(resources[0].module(), "base");
    assert_eq!(resources[0].logical_id(), "state");
    assert_eq!(resources[0].kind().name(), "Store");
    // Facade-derived context value plus the provider default for the
    // omitted `replicas` field, observed through the fixture manifest.
    assert_eq!(
        desired_manifest(resources[0].kind()),
        serde_json::json!({ "path": "/var/lib/demo", "replicas": 1 })
    );
    assert_eq!(resources[1].module(), "data");
    assert_eq!(resources[1].logical_id(), "probe");
    assert_eq!(resources[1].kind().name(), "Probe");
    assert_eq!(
        desired_manifest(resources[1].kind()),
        serde_json::json!({ "label": "demo:eu-west-2", "mode": null })
    );
    assert_eq!(resources[1].dependencies().len(), 1);
    assert_eq!(resources[1].dependencies()[0].logical_id(), "state");

    let writebacks = graph.writeback();
    assert_eq!(writebacks.len(), 2);
    assert!(matches!(
        writebacks[0].value(),
        WritebackValue::Literal(value) if value == "eu-west-2"
    ));
    assert!(matches!(
        writebacks[1].value(),
        WritebackValue::Output(output)
            if output.resource().logical_id() == "state" && output.output() == "endpoint"
    ));
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestConfig {
    storage: Storage,
    label: String,
}

#[derive(Debug, PartialEq, Deserialize)]
enum Storage {
    InMemory,
    Dsql(DsqlPayload),
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct DsqlPayload {
    region: String,
    endpoint: String,
}

#[test]
// Feature: tkdp-frontend, Property 11: the dataclass variant spelling admits
// into enum-typed kind fields identically to the explicit spelling.
fn variant_spelling_reaches_enum_typed_kind_fields() {
    let source = r#"from dataclasses import dataclass

from tokeira import Context, Deployment, Probe


@dataclass
class Empty:
    pass


@dataclass
class Careful:
    retries: int


def config() -> Empty:
    return Empty()


def deployment(cfg, cx):
    d = Deployment(["default"])
    m = d.module("m")
    m.resource("fast", Probe(mode=Fast()))
    m.resource("careful", Probe(mode=Careful(retries=4)))
    return d


@dataclass
class Fast:
    pass
"#;
    let output = evaluate(source).expect("variant kinds evaluate");
    let kinds: Vec<_> = output
        .graph
        .resources()
        .iter()
        .map(|r| desired_manifest(r.kind()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            serde_json::json!({ "label": null, "mode": "Fast" }),
            serde_json::json!({ "label": null, "mode": { "Careful": { "retries": 4 } } }),
        ]
    );
}

// ---------------------------------------------------------------------------
// Semantics corpus (spike-carried), observed through writeback literals.
// Feature: tkdp-frontend, Property 4: dispatch semantics.
// ---------------------------------------------------------------------------

#[test]
fn first_matching_case_wins() {
    let out = run_snippet(
        "match 1:\n    case 1:\n        out = \"first\"\n    case x:\n        out = \"capture\"",
    )
    .expect("snippet");
    assert_eq!(out, "first");
}

#[test]
fn class_pattern_uses_exact_identity_and_captures_fields() {
    let out = run_snippet(
        "match Dsql(region=\"eu\"):\n    case InMemory():\n        out = \"memory\"\n    case Dsql(region=r):\n        out = \"dsql:\" + r",
    )
    .expect("snippet");
    assert_eq!(out, "dsql:eu");
}

#[test]
fn guard_falls_through_and_bindings_persist() {
    let out = run_snippet(
        "match Dsql(region=\"us\"):\n    case Dsql(region=r) if r == \"eu\":\n        out = \"eu\"\n    case _:\n        out = \"fell:\" + r",
    )
    .expect("snippet");
    assert_eq!(out, "fell:us");
}

#[test]
fn literals_use_equality_and_singletons_use_identity() {
    let out = run_snippet(
        "value = 0.0\nmatch value:\n    case None:\n        out = \"none\"\n    case 0:\n        out = \"zero\"\n    case _:\n        out = \"other\"",
    )
    .expect("snippet");
    assert_eq!(out, "zero");
}

#[test]
fn nested_match_and_loop_break_behave() {
    let out = run_snippet(
        "acc = \"\"\nfor item in [Dsql(region=\"eu\"), InMemory(), Dsql(region=\"us\")]:\n    match item:\n        case InMemory():\n            break\n        case Dsql(region=r):\n            match r:\n                case \"eu\":\n                    acc = acc + \"E\"\n                case _:\n                    acc = acc + \"O\"\nout = acc",
    )
    .expect("snippet");
    assert_eq!(out, "E");
}

#[test]
fn bare_capture_case_binds_the_subject() {
    let out =
        run_snippet("match 5:\n    case n:\n        out = \"got:\" + str(n)").expect("snippet");
    assert_eq!(out, "got:5");
}

#[test]
fn guards_run_only_for_cases_whose_pattern_matched() {
    // `.append` returns None, so `or True` keeps the guard truthy while the
    // call records that the guard was evaluated at all.
    let out = run_snippet(
        "calls = []\nmatch 3:\n    case 1 if calls.append(\"one\") or True:\n        out = \"one\"\n    case 3 if calls.append(\"three\") or True:\n        out = \"three\"\n    case _:\n        out = \"other\"\nout = out + \":\" + calls[0] + \":\" + str(len(calls))",
    )
    .expect("snippet");
    assert_eq!(out, "three:three:1");
}

// ---------------------------------------------------------------------------
// Failure paths.
// ---------------------------------------------------------------------------

#[test]
// Feature: tkdp-frontend, Property 5: strict exhaustion names the original
// position.
fn exhaustion_raises_with_definition_position() {
    let error = run_snippet("match \"nope\":\n    case 1:\n        out = \"one\"")
        .expect_err("must fall through");
    assert!(error.contains("match fell through"), "{error}");
    assert!(error.contains("definition.tkdp:"), "{error}");
}

#[test]
// Feature: tkdp-frontend, Property 2: preflight findings are complete and
// coded.
fn preflight_findings_are_folded_and_coded() {
    let source = "from tokeira import Nope\n\nmatch s:\n    case [x]:\n        pass\n";
    let error = evaluate(source).expect_err("rejected");
    for needle in ["TKDP012", "TKDP002", "TKDP008", "definition rejected"] {
        assert!(error.contains(needle), "missing {needle} in: {error}");
    }
}

#[test]
// Feature: tkdp-frontend, Property 9: kind decode failures carry the
// declaring call's position.
fn unknown_kind_field_is_located_at_the_resource_call() {
    let source = r#"from dataclasses import dataclass

from tokeira import Context, Deployment, Store


@dataclass
class Empty:
    pass


def config() -> Empty:
    return Empty()


def deployment(cfg, cx):
    d = Deployment(["default"])
    m = d.module("m")
    m.resource("state", Store(path="/x", replicaz=2))
    return d
"#;
    let error = evaluate(source).expect_err("unknown field");
    assert!(error.contains("replicaz"), "{error}");
}

#[test]
// Feature: tkdp-frontend, Property 6 (spot): runtime failures map to the
// operator's line with its text.
fn runtime_error_maps_to_original_line() {
    let out = run_snippet("boom = 1 / 0\nout = \"unreachable\"");
    let error = out.expect_err("division fails");
    assert!(error.contains("ZeroDivisionError"), "{error}");
    assert!(error.contains("definition.tkdp:"), "{error}");
    assert!(error.contains("1 / 0"), "{error}");
}

#[test]
// Print output is trace-logged on success and attached to failures, so a
// definition author's debug prints are never silently lost.
fn captured_print_output_is_attached_to_failures() {
    let error = run_snippet("print(\"hello from tkdp\")\nboom = 1 / 0")
        .expect_err("division fails after print");
    assert!(error.contains("captured output:"), "{error}");
    assert!(error.contains("hello from tkdp"), "{error}");
}

#[test]
// Feature: tkdp-frontend, Property 7: evaluation is stateless — repeated
// evaluation of identical inputs yields identical structures.
fn repeated_evaluation_is_identical() {
    let first = evaluate(EXEMPLAR).expect("first");
    let second = evaluate(EXEMPLAR).expect("second");
    let render = |output: &FrontendOutput| {
        let graph = &output.graph;
        format!(
            "{:?}|{:?}|{:?}",
            graph.namespaces(),
            graph
                .resources()
                .iter()
                .map(|r| (
                    r.module().to_string(),
                    r.logical_id().to_string(),
                    r.kind().name(),
                    desired_manifest(r.kind()),
                ))
                .collect::<Vec<_>>(),
            graph.writeback().len()
        )
    };
    assert_eq!(render(&first), render(&second));
}

#[test]
// The inspection seam (`transient_program`, carried from the spike CLI's
// `lower --show-generated`) assembles exactly the program `evaluate`
// executes, deterministically, with the source map covering every byte.
fn transient_program_is_assembled_without_executing() {
    let path = RelativeDefinitionPath::new("definition.tkdp").expect("path");
    let source_name = DefinitionSourceName::DeploymentRelative(path);
    let program = |bytes: &str| {
        frontend()
            .transient_program(
                FrontendSource {
                    source_name: &source_name,
                    bytes: bytes.as_bytes(),
                },
                &ctx(),
                &namespaces(),
                &NoPartSources,
            )
            .expect("assembles")
    };
    let first = program(EXEMPLAR);
    let second = program(EXEMPLAR);
    assert_eq!(first.text, second.text);
    // Facade, lowered match, and driver are all present in the one text.
    for needle in [
        "__tokeira_internal_match",
        "__tokeira_internal_subject_0",
        "__tokeira_internal_export",
    ] {
        assert!(first.text.contains(needle), "missing {needle}");
    }
    // The map covers the assembled text to its last byte.
    let last = TextSize::new(first.text.len() as u32 - 1);
    assert!(first.map.resolve(last).is_some());
}

#[test]
// Feature: tkdp-frontend, Property 10: every inventory name is importable and
// constructible; unimported names are not bound.
fn facade_totality_and_unimported_names_stay_unbound() {
    let all = r#"from dataclasses import dataclass

from tokeira import Context, Deployment, Probe, Store


@dataclass
class Empty:
    pass


def config() -> Empty:
    return Empty()


def deployment(cfg, cx):
    d = Deployment(["default"])
    m = d.module("m")
    m.resource("a", Probe())
    m.resource("b", Store(path="/x"))
    return d
"#;
    assert!(evaluate(all).is_ok());

    // `Store` not imported ⇒ the name is unbound in the sandbox.
    let unbound = r#"from dataclasses import dataclass

from tokeira import Context, Deployment


@dataclass
class Empty:
    pass


def config() -> Empty:
    return Empty()


def deployment(cfg, cx):
    d = Deployment(["default"])
    d.module("m").resource("a", Store(path="/x"))
    return d
"#;
    let error = evaluate(unbound).expect_err("unbound kind name");
    assert!(error.contains("Store"), "{error}");
}
