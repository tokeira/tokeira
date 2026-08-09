//! The `mod` mechanism, end to end through the public frontend: a root
//! declares parts, parts declare their modules against the same graph, and
//! every boundary rule refuses by name.

use std::sync::Arc;

use serde::Serialize;
use tokeira_platform::{
    declaration::Vocabulary,
    definition::{
        DefinitionFrontend, DefinitionSourceName, FrontendSource, NoPartSources, PartResolveError,
        SourceResolver,
    },
};

#[derive(Serialize)]
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

fn evaluate(
    root: &str,
    parts: &MapParts,
) -> Result<tokeira_platform::definition::FrontendOutput, String> {
    let source_name = DefinitionSourceName::AuthoringPath("definition.tkd".into());
    tokeira_tkd::frontend()
        .evaluate(
            FrontendSource {
                source_name: &source_name,
                bytes: root.as_bytes(),
            },
            &Ctx {
                project_name: "demo".to_string(),
            },
            &Vocabulary::of(Vec::new()).expect("empty vocabulary composes"),
            parts,
        )
        .map_err(|diagnostic| diagnostic.message)
}

const ROOT: &str = r#"
    mod networking;

    struct Cfg {
        name: String,
    }

    fn config() -> Cfg {
        Cfg { name: "demo".to_string() }
    }

    fn deployment(cfg: Cfg, cx: Context) -> Deployment {
        let d = Deployment::new(&["default"]);
        let state = d.module("state", vec![]);
        let net = networking::declare(d, cfg, vec![state]);
        d
    }
"#;

const NETWORKING: &str = r#"
    pub struct Handles {
        pub module: Module,
    }

    pub fn declare(d: Deployment, cfg: Cfg, deps: Vec<Module>) -> Handles {
        let m = d.module("net", deps);
        Handles { module: m }
    }
"#;

// The mechanism, whole: the root wires, the part declares its module
// against the same graph, the returned handle struct flows back as data.
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

// A part references the root's types (the shared configuration language):
// `cfg: Cfg` above is annotation-only, but constructing a root type inside
// a part must resolve too.
#[test]
fn a_part_constructs_root_types() {
    let root = r#"
        mod helper;

        struct Cfg { name: String }

        fn config() -> Cfg {
            Cfg { name: "demo".to_string() }
        }

        fn deployment(cfg: Cfg, cx: Context) -> Deployment {
            let d = Deployment::new(&["default"]);
            let state = d.module("state", vec![]);
            let replacement = helper::renamed(cfg);
            d
        }
    "#;
    let helper = r#"
        pub fn renamed(cfg: Cfg) -> Cfg {
            Cfg { name: cfg.name.clone() }
        }
    "#;
    let parts = MapParts(std::collections::BTreeMap::from([("helper", helper)]));
    evaluate(root, &parts).expect("a part builds root types");
}

#[test]
fn an_unserved_part_refuses_with_the_resolver_reason() {
    let message = evaluate(ROOT, &MapParts(std::collections::BTreeMap::new())).unwrap_err();
    assert!(message.contains("`networking`"), "{message}");
    assert!(message.contains("absent from the fixture"), "{message}");
}

#[test]
fn an_inline_module_body_is_refused() {
    let root = "mod inline { }\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}";
    let message = evaluate(root, &MapParts(std::collections::BTreeMap::new())).unwrap_err();
    assert!(message.contains("inline module bodies"), "{message}");
}

#[test]
fn a_part_declaring_a_part_is_refused() {
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        "mod deeper;\npub fn declare(d: Deployment, cfg: Cfg, deps: Vec<Module>) -> Deployment { d }",
    )]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(message.contains("networking.tkd"), "{message}");
    assert!(message.contains("one level deep"), "{message}");
}

#[test]
fn a_private_part_function_is_refused_by_name() {
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        "fn declare(d: Deployment, cfg: Cfg, deps: Vec<Module>) -> Deployment { d }",
    )]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(message.contains("not `pub`"), "{message}");
}

#[test]
fn a_part_shadowing_a_root_type_is_refused() {
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        "pub struct Cfg {}\npub fn declare(d: Deployment, cfg: Cfg, deps: Vec<Module>) -> Deployment { d }",
    )]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(message.contains("shadows a root type"), "{message}");
}

#[test]
fn a_part_calling_another_part_is_refused() {
    let parts = MapParts(std::collections::BTreeMap::from([
        (
            "networking",
            "pub fn declare(d: Deployment, cfg: Cfg, deps: Vec<Module>) -> Deployment { let x = other::thing(); d }",
        ),
        ("other", "pub fn thing() -> Cfg { Cfg {} }"),
    ]));
    let message = evaluate(ROOT, &parts).unwrap_err();
    assert!(
        message.contains("call `other::thing` is not allowed"),
        "{message}"
    );
}

#[test]
fn a_marked_root_item_is_refused() {
    let root = "pub fn config() -> Cfg { Cfg {} }\nstruct Cfg {}";
    let message = evaluate(root, &MapParts(std::collections::BTreeMap::new())).unwrap_err();
    assert!(message.contains("the root exports nothing"), "{message}");
}

// The loop closed with the real resolver: parts served from a directory
// beside the root, exactly as the engine serves them.
#[test]
fn parts_resolve_from_a_directory_beside_the_root() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("networking.tkd"), NETWORKING).unwrap();
    let resolver = tokeira_platform::definition::DirectoryPartSources::new(dir.path(), "tkd");
    let source_name = DefinitionSourceName::AuthoringPath("definition.tkd".into());
    tokeira_tkd::frontend()
        .evaluate(
            FrontendSource {
                source_name: &source_name,
                bytes: ROOT.as_bytes(),
            },
            &Ctx {
                project_name: "demo".to_string(),
            },
            &Vocabulary::of(Vec::new()).expect("empty vocabulary composes"),
            &resolver,
        )
        .expect("the directory-resolved part evaluates");
}

// The retarget gate compares a multi-document definition as the set it
// was: each side evaluates with its own part resolver. An unchanged set
// reconciles; a `#[create]` change refuses even when the definition
// declares parts.
#[test]
fn the_retarget_gate_compares_part_bearing_definitions() {
    let root = |mode: &str| {
        format!(
            r#"
            mod networking;

            struct Cfg {{ #[create] mode: String }}

            fn config() -> Cfg {{
                Cfg {{ mode: "{mode}".to_string() }}
            }}

            fn deployment(cfg: Cfg, cx: Context) -> Deployment {{
                let d = Deployment::new(&["default"]);
                let state = d.module("state", vec![]);
                let net = networking::declare(d, cfg, vec![state]);
                d
            }}
            "#
        )
    };
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        NETWORKING,
    )]));
    let source_name = DefinitionSourceName::AuthoringPath("definition.tkd".into());
    let check = |prior: &str, current: &str| {
        tokeira_tkd::frontend().retarget_check(
            FrontendSource {
                source_name: &source_name,
                bytes: prior.as_bytes(),
            },
            FrontendSource {
                source_name: &source_name,
                bytes: current.as_bytes(),
            },
            &Ctx {
                project_name: "demo".to_string(),
            },
            &Vocabulary::of(Vec::new()).expect("empty vocabulary composes"),
            &parts,
            &parts,
        )
    };

    let prior = root("dsql");
    check(&prior, &prior).expect("an unchanged part-bearing set reconciles");
    let messages = check(&prior, &root("in-memory")).expect_err("a create change refuses");
    assert!(
        messages.iter().any(|message| message.contains("mode")),
        "{messages:?}"
    );
    let _ = NoPartSources;
}
