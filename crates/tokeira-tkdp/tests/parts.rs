//! The parts mechanism, end to end through the public `.tkdp` frontend: a
//! root declares parts by importing them, parts execute as real Monty
//! modules against the same deployment, and every boundary rule refuses by
//! name. Mirrors the `.tkd` parts suite where the languages allow.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokeira_orchestrator::RelativeDefinitionPath;
use tokeira_platform::{
    author::from_located_value,
    declaration::{KindEntry, KindSet, Vocabulary},
    definition::{
        DefinitionFrontend, DefinitionSourceName, FrontendOutput, FrontendSource, NoPartSources,
        PartResolveError, SourceResolver,
    },
    error::KindError,
    kind::{PlacementContext, ProviderKind},
};
use tokeira_tkdp::frontend;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Store {
    path: String,
    replicas: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct StoreKind(Store);

impl ProviderKind for StoreKind {
    fn kind_name(&self) -> &'static str {
        "Store"
    }

    fn validate_input(&self) -> Result<(), KindError> {
        Ok(())
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &["endpoint"]
    }

    fn desired_manifest(&self, _placement: &PlacementContext) -> serde_json::Value {
        serde_json::to_value(&self.0).expect("store serializes")
    }

    fn realize(
        &self,
        _placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        Err(KindError::new("not exercised by parts tests"))
    }
}

fn vocabulary() -> Vocabulary {
    let store = KindEntry {
        name: "Store",
        defaults: None,
        decode: |value| {
            let range = value.range;
            from_located_value::<Store>(value)
                .map(|store| Box::new(StoreKind(store)) as Box<dyn ProviderKind>)
                .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
        },
    };
    Vocabulary::of(vec![KindSet::new("test", vec![store])]).expect("test vocabulary composes")
}

#[derive(Debug, Serialize)]
struct Ctx {
    project_name: String,
}

struct MapParts(std::collections::BTreeMap<&'static str, &'static str>);

impl SourceResolver for MapParts {
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
        self.0
            .get(name)
            .map(|text| Arc::from(text.as_bytes().to_vec()))
            .ok_or_else(|| PartResolveError {
                name: name.to_string(),
                reason: "absent from the fixture".to_string(),
            })
    }
}

fn evaluate(root: &str, parts: &dyn SourceResolver) -> Result<FrontendOutput, String> {
    let path = RelativeDefinitionPath::new("definition.tkdp").expect("path");
    let source_name = DefinitionSourceName::DeploymentRelative(path);
    frontend()
        .evaluate(
            FrontendSource {
                source_name: &source_name,
                bytes: root.as_bytes(),
            },
            &Ctx {
                project_name: "demo".to_string(),
            },
            &vocabulary(),
            parts,
        )
        .map_err(|diagnostic| diagnostic.message)
}

const ROOT: &str = r#"import networking

from dataclasses import dataclass

from tokeira import Context, Deployment


@dataclass
class Config:
    name: str


def config() -> Config:
    return Config(name="demo")


def deployment(cfg: Config, cx: Context) -> Deployment:
    d = Deployment(["default"])
    state = d.module("state")
    networking.declare(d, cfg, cx, state)
    return d
"#;

const NETWORKING: &str = r#"from tokeira import Deployment, Store


def declare(d, cfg, cx, state):
    net = d.module("net", [state])
    net.resource("primary", Store(path="/var/" + cx.project_name, replicas=1))
    return net
"#;

// The mechanism, whole: the root wires, the part declares its module and a
// vocabulary-kind resource against the same deployment, using the same
// facade class identities.
#[test]
fn a_root_and_its_part_build_one_graph() {
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        NETWORKING,
    )]));
    let output = evaluate(ROOT, &parts).expect("multi-document definition evaluates");
    let modules: Vec<&str> = output
        .graph
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert_eq!(modules, ["state", "net"]);
}

#[test]
fn a_part_imports_another_part() {
    let root = r#"import networking

from dataclasses import dataclass

from tokeira import Context, Deployment


@dataclass
class Config:
    name: str


def config() -> Config:
    return Config(name="demo")


def deployment(cfg: Config, cx: Context) -> Deployment:
    d = Deployment(["default"])
    networking.declare(d)
    return d
"#;
    let shared = "WIDTH = 2\n";
    let networking = r#"from shared import WIDTH

from tokeira import Deployment, Store


def declare(d):
    net = d.module("net")
    net.resource("primary", Store(path="/x", replicas=WIDTH))
    return net
"#;
    let parts = MapParts(std::collections::BTreeMap::from([
        ("networking", networking),
        ("shared", shared),
    ]));
    evaluate(root, &parts).expect("part-to-part imports evaluate");
}

#[test]
fn an_import_cycle_is_refused_by_name() {
    let parts = MapParts(std::collections::BTreeMap::from([
        ("a", "import b\n"),
        ("b", "import a\n"),
    ]));
    let root = r#"import a

from dataclasses import dataclass

from tokeira import Context, Deployment


@dataclass
class Config:
    name: str


def config() -> Config:
    return Config(name="demo")


def deployment(cfg: Config, cx: Context) -> Deployment:
    return Deployment(["default"])
"#;
    let message = evaluate(root, &parts).unwrap_err();
    assert!(
        message.contains("import cycle among registered modules"),
        "{message}"
    );
    assert!(message.contains("a -> b -> a"), "{message}");
}

#[test]
fn a_plain_import_shadowed_by_an_own_binding_is_refused() {
    let root = r#"import deployment

from dataclasses import dataclass

from tokeira import Context, Deployment


@dataclass
class Config:
    name: str


def config() -> Config:
    return Config(name="demo")


def deployment(cfg: Config, cx: Context) -> Deployment:
    return Deployment(["default"])
"#;
    let message = evaluate(root, &NoPartSources).unwrap_err();
    assert!(
        message.contains("shadowed by this file's own `deployment`"),
        "{message}"
    );
    assert!(message.contains("from deployment import"), "{message}");
}

#[test]
fn dotted_imports_are_refused() {
    let root = "import provider.aws\n";
    let message = evaluate(root, &NoPartSources).unwrap_err();
    assert!(message.contains("single-level"), "{message}");
    assert!(message.contains("provider.aws"), "{message}");
}

#[test]
fn a_part_preflight_failure_names_the_part_file() {
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        "__tokeira_internal_leak = 1\n",
    )]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(message.contains("networking.tkdp"), "{message}");
    assert!(message.contains("reserved"), "{message}");
}

// An import the resolver does not serve is not a part: it reaches Monty,
// which answers with `ModuleNotFoundError` at the import site.
#[test]
fn an_unserved_import_falls_through_to_monty() {
    let root = r#"import absent

from dataclasses import dataclass

from tokeira import Context, Deployment


@dataclass
class Config:
    name: str


def config() -> Config:
    return Config(name="demo")


def deployment(cfg: Config, cx: Context) -> Deployment:
    return Deployment(["default"])
"#;
    let message = evaluate(root, &NoPartSources).unwrap_err();
    assert!(message.contains("ModuleNotFoundError"), "{message}");
    assert!(message.contains("definition.tkdp:1"), "{message}");
}

// A traceback from inside a part names the part's own file at the original
// position, with the original source line as the preview.
#[test]
fn a_traceback_in_a_part_names_the_part_file() {
    let networking = r#"from tokeira import Deployment


def declare(d, cfg, cx, state):
    return 1 / 0
"#;
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        networking,
    )]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(message.contains("ZeroDivisionError"), "{message}");
    assert!(message.contains("networking.tkdp:5"), "{message}");
    assert!(message.contains("return 1 / 0"), "{message}");
    // The root call site is also on the stack, in root coordinates.
    assert!(message.contains("definition.tkdp:20"), "{message}");
}

// A part containing a `match` statement exercises the part's own source map:
// the lowering shifts lines inside the part, and the frame still renders at
// the original position.
#[test]
fn a_traceback_in_a_matched_part_maps_through_the_lowering() {
    let networking = r#"from tokeira import Deployment


def declare(d, cfg, cx, state):
    match cx.project_name:
        case "other":
            pass
        case _:
            pass
    return 1 / 0
"#;
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        networking,
    )]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(message.contains("networking.tkdp:10"), "{message}");
    assert!(message.contains("return 1 / 0"), "{message}");
}

// The loop closed with the real resolver: parts served from a directory
// beside the root, exactly as the engine serves them.
#[test]
fn parts_resolve_from_a_directory_beside_the_root() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("networking.tkdp"), NETWORKING).unwrap();
    let resolver = tokeira_platform::definition::DirectoryPartSources::new(dir.path(), "tkdp");
    let output = evaluate(ROOT, &resolver).expect("the directory-resolved part evaluates");
    assert_eq!(output.graph.modules().len(), 2);
}
