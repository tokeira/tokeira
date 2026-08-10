//! The `mod` mechanism, end to end through the public frontend: a root
//! declares parts, parts declare their modules against the same graph, and
//! every boundary rule refuses by name.

use std::sync::Arc;

use serde::Serialize;
use tokeira_platform::definition::{
    DefinitionFrontend, DefinitionSourceName, FrontendSource, NoPartSources, PartResolveError,
    SourceResolver,
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
            &[],
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
            &[],
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
            &[],
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

// ---------------------------------------------------------------------
// `use` — types crossing documents (full `mod` support)
// ---------------------------------------------------------------------

// The model-part shape: the root takes pub types (with pub fields) from a
// part and builds its config from them — the enabling case for
// platform.tkd.
#[test]
fn the_root_takes_part_types_through_use() {
    let root = r#"
        mod platform;

        use platform::{Cfg, Storage};

        fn config() -> Cfg {
            Cfg { name: "demo".to_string(), storage: Storage::InMemory }
        }

        fn deployment(cfg: Cfg, cx: Context) -> Deployment {
            let d = Deployment::new(&["default"]);
            let state = d.module("state", vec![]);
            if let Storage::Dsql(region) = &cfg.storage {
                let dsql = d.module("dsql", vec![state]);
            }
            d
        }
    "#;
    let platform = r#"
        pub struct Cfg {
            pub name: String,
            pub storage: Storage,
        }

        pub enum Storage {
            InMemory,
            Dsql(String),
        }
    "#;
    let parts = MapParts(std::collections::BTreeMap::from([("platform", platform)]));
    let output = evaluate(root, &parts).expect("the root builds config from part types");
    assert_eq!(output.graph.modules().len(), 1);
}

// A tuple variant taken through `use` constructs and matches — the subset's
// effective-type table admits what the scope resolves.
#[test]
fn tuple_variants_cross_through_use() {
    let root = r#"
        mod platform;

        use platform::{Cfg, Storage};

        fn config() -> Cfg {
            Cfg { storage: Storage::Dsql("eu-west-2".to_string()) }
        }

        fn deployment(cfg: Cfg, cx: Context) -> Deployment {
            let d = Deployment::new(&["default"]);
            if let Storage::Dsql(region) = &cfg.storage {
                let named = d.module("dsql", vec![]);
            }
            d
        }
    "#;
    let platform = r#"
        pub struct Cfg {
            pub storage: Storage,
        }

        pub enum Storage {
            InMemory,
            Dsql(String),
        }
    "#;
    let parts = MapParts(std::collections::BTreeMap::from([("platform", platform)]));
    let output = evaluate(root, &parts).expect("tuple variants construct and match");
    let modules: Vec<&str> = output
        .graph
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert_eq!(modules, ["dsql"]);
}

// The observability-part shape: a part takes another part's pub type for
// its signature and body — part-to-part `use` under the DAG.
#[test]
fn a_part_takes_another_parts_types_through_use() {
    let root = r#"
        mod platform;
        mod observability;

        use platform::Cfg;

        fn config() -> Cfg {
            Cfg { replicas: 2 }
        }

        fn deployment(cfg: Cfg, cx: Context) -> Deployment {
            let d = Deployment::new(&["default"]);
            let state = d.module("state", vec![]);
            observability::define(d, cfg, vec![state]);
            d
        }
    "#;
    let platform = r#"
        pub struct Cfg {
            pub replicas: u32,
        }
    "#;
    let observability = r#"
        use platform::Cfg;

        pub fn define(d: Deployment, cfg: Cfg, deps: Vec<Module>) -> Deployment {
            let observability = d.module("observability", deps);
            d
        }
    "#;
    let parts = MapParts(std::collections::BTreeMap::from([
        ("platform", platform),
        ("observability", observability),
    ]));
    let output = evaluate(root, &parts).expect("part-to-part takes evaluate");
    let modules: Vec<&str> = output
        .graph
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert_eq!(modules, ["state", "observability"]);
}

// A part constructing a root tuple variant passes the subset: the part's
// effective table stands on the root's types.
#[test]
fn a_part_constructs_a_root_tuple_variant() {
    let root = r#"
        mod helper;

        enum Mode {
            Fast(String),
        }

        struct Cfg { name: String }

        fn config() -> Cfg {
            Cfg { name: "demo".to_string() }
        }

        fn deployment(cfg: Cfg, cx: Context) -> Deployment {
            let d = Deployment::new(&["default"]);
            let mode = helper::pick(cfg);
            d
        }
    "#;
    let helper = r#"
        pub fn pick(cfg: Cfg) -> Mode {
            Mode::Fast(cfg.name.clone())
        }
    "#;
    let parts = MapParts(std::collections::BTreeMap::from([("helper", helper)]));
    evaluate(root, &parts).expect("a part builds root tuple variants");
}

#[test]
fn use_of_a_function_is_refused() {
    let root = "mod networking;\nuse networking::declare;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([(
        "networking",
        NETWORKING,
    )]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("takes a function"), "{message}");
    assert!(message.contains("called qualified"), "{message}");
}

#[test]
fn use_of_a_private_type_is_refused() {
    let root = "mod p;\nuse p::Hidden;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([(
        "p",
        "struct Hidden {}\npub fn declare(d: Deployment) -> Deployment { d }",
    )]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("not `pub`"), "{message}");
}

#[test]
fn use_renames_are_refused() {
    let root = "mod p;\nuse p::Remote as Config;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([(
        "p",
        "pub struct Remote {}",
    )]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("rename would split"), "{message}");
}

#[test]
fn use_globs_are_refused() {
    let root = "mod p;\nuse p::*;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([("p", "pub struct X {}")]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("take names explicitly"), "{message}");
}

#[test]
fn use_of_an_undeclared_part_is_refused() {
    let root = "use ghost::Cfg;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let message = evaluate(root, &MapParts(std::collections::BTreeMap::new())).unwrap_err();
    assert!(message.contains("names no declared part"), "{message}");
    assert!(message.contains("mod ghost;"), "{message}");
}

#[test]
fn a_part_does_not_use_itself() {
    let root = "mod p;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([(
        "p",
        "use p::Own;\npub struct Own {}",
    )]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("does not `use` itself"), "{message}");
}

// The root-side collision is unreachable — a part can never export a type
// named like a root type (the shadow rule refuses it first) — so the
// reachable collision is part-side: a part's own type against its take.
#[test]
fn a_use_colliding_with_an_own_type_is_refused() {
    let root = "mod a;\nmod b;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([
        ("a", "use b::Shape;\npub struct Shape {}"),
        ("b", "pub struct Shape {}"),
    ]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("a.tkd"), "{message}");
    assert!(
        message.contains("collides with this document's own type"),
        "{message}"
    );
}

#[test]
fn use_cycles_among_parts_are_refused() {
    let root = "mod a;\nmod b;\nfn config() -> Cfg { Cfg {} }\nstruct Cfg {}\nfn deployment(cfg: Cfg, cx: Context) -> Deployment { Deployment::new(&[\"default\"]) }";
    let parts = MapParts(std::collections::BTreeMap::from([
        ("a", "use b::B;\npub struct A {}"),
        ("b", "use a::A;\npub struct B {}"),
    ]));
    let message = evaluate(root, &parts).unwrap_err();
    assert!(message.contains("form a cycle among parts"), "{message}");
    assert!(message.contains("a -> b -> a"), "{message}");
}

// `#[create]` may sit in any document of the set: a model part carries the
// configuration types, and the retarget gate reads the whole set's
// annotations.
#[test]
fn create_annotations_in_a_part_gate_retarget() {
    let root = |mode: &str| {
        format!(
            r#"
            mod platform;

            use platform::Cfg;

            fn config() -> Cfg {{
                Cfg {{ mode: "{mode}".to_string(), replicas: 1 }}
            }}

            fn deployment(cfg: Cfg, cx: Context) -> Deployment {{
                let d = Deployment::new(&["default"]);
                d
            }}
            "#
        )
    };
    let platform = r#"
        pub struct Cfg {
            #[create]
            pub mode: String,
            pub replicas: u32,
        }
    "#;
    let parts = MapParts(std::collections::BTreeMap::from([("platform", platform)]));
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
            &[],
            &parts,
            &parts,
        )
    };

    let prior = root("dsql");
    check(&prior, &prior).expect("an unchanged set reconciles");
    let messages = check(&prior, &root("in-memory")).expect_err("a create change refuses");
    assert!(
        messages.iter().any(|message| message.contains("mode")),
        "{messages:?}"
    );
}
