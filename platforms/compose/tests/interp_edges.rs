//! Regression tests for the adversarial-review findings — each is a subset-valid
//! `.tkd` edit that previously diverged from compiled-Rust semantics (or panicked).
//! They lock the fidelity guarantee under operator *edits*, not just the shipped
//! definition.

use std::path::PathBuf;

use tokeira_compose_deployment::{
    builder::WbValue,
    context::Cx,
    interp::{interpret, retarget_check, validate},
};

fn cx() -> Cx {
    Cx {
        project_name: "p".into(),
        region: Some("us-east-1".into()),
        deployment_dir: PathBuf::from("/tmp/x"),
    }
}

// 1. `.to_string()` on an int must stringify (not be an identity no-op).
const TO_STRING: &str = r#"
struct Compose { n: u32 }
fn config() -> Compose { Compose { n: 8080 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);
    let m = d.module("m", &[]);
    d.service(&m, "s", Service { image: "i".into(), replicas: 1, env: vec![("PORT".into(), cfg.n.to_string())], ..Service::EMPTY });
    d
}
"#;

#[test]
fn to_string_on_int_stringifies() {
    // Before the fix the env-pair coercion failed with "expected string, got Int".
    assert!(interpret(TO_STRING, &cx()).is_ok());
}

// 2. A pattern naming the WRONG enum (shared leaf name) must error, not match.
const WRONG_ENUM: &str = r#"
enum Storage { InMemory, Disk }
enum Other { InMemory }
struct Compose { s: Storage }
fn config() -> Compose { Compose { s: Storage::InMemory } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);
    let _m = matches!(&cfg.s, Other::InMemory);
    d
}
"#;

#[test]
fn pattern_on_wrong_enum_errors() {
    assert!(interpret(WRONG_ENUM, &cx()).is_err());
}

// 3. `..` rest in a tuple-variant pattern must match (not be a positional element).
const TUPLE_REST: &str = r#"
enum E { A(u32, u32, u32) }
struct Compose { e: E }
fn config() -> Compose { Compose { e: E::A(1, 2, 3) } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);
    let v = match &cfg.e { E::A(x, ..) => "matched", };
    d.writeback("k", v);
    d
}
"#;

#[test]
fn tuple_rest_pattern_matches() {
    let dep = match interpret(TUPLE_REST, &cx()) {
        Ok((d, _)) => d,
        Err(e) => panic!("{}", e.msg),
    };
    assert!(matches!(&dep.writeback_entries()[0].1, WbValue::Const(s) if s == "matched"));
}

// 4. Config struct literals are validated against declared fields.
const MISSING_FIELD: &str = r#"
struct Compose { a: u32, b: u32 }
fn config() -> Compose { Compose { a: 1 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;
const EXTRA_FIELD: &str = r#"
struct Compose { a: u32 }
fn config() -> Compose { Compose { a: 1, ghost: 9 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn config_missing_field_errors() {
    assert!(interpret(MISSING_FIELD, &cx()).is_err());
}

#[test]
fn config_unknown_field_errors() {
    assert!(interpret(EXTRA_FIELD, &cx()).is_err());
}

// 5. A block-bodied match arm (subset-valid) must evaluate.
const BLOCK_ARM: &str = r#"
enum Mode { A, B }
struct Compose { mode: Mode }
fn config() -> Compose { Compose { mode: Mode::A } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);
    let m = d.module("m", &[]);
    match &cfg.mode { Mode::A => { d.resource(&m, "one", LocalStateDir); } Mode::B => {} }
    d
}
"#;

#[test]
fn block_bodied_match_arm_evaluates() {
    let dep = match interpret(BLOCK_ARM, &cx()) {
        Ok((d, _)) => d,
        Err(e) => panic!("{}", e.msg),
    };
    assert_eq!(dep.resource_ids("m"), ["one"]);
}

// 6. Comparing host handles with `==` must error, never panic.
const HOST_EQ: &str = r#"
struct Compose { x: u32 }
fn config() -> Compose { Compose { x: 1 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);
    let m = d.module("a", &[]);
    let _b = m.clone() == m.clone();
    d
}
"#;

#[test]
fn comparing_host_handles_errors_not_panics() {
    assert!(interpret(HOST_EQ, &cx()).is_err());
}

// 7. A typo'd enum variant must error (fail closed), not build a phantom value.
const VARIANT_TYPO: &str = r#"
enum Storage { InMemory, Dsql { region: String } }
struct Compose { storage: Storage }
fn config() -> Compose { Compose { storage: Storage::Dssql { region: "x".into() } } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn typoed_enum_variant_errors() {
    assert!(interpret(VARIANT_TYPO, &cx()).is_err());
}

// 8. A free call inside #[require] is caught by the subset, not smuggled to eval.
const REQUIRE_FREE_CALL: &str = r#"
#[require(do_thing(replicas))]
struct Compose { replicas: u32 }
fn config() -> Compose { Compose { replicas: 1 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn require_attr_is_subset_checked() {
    let msgs = validate(REQUIRE_FREE_CALL).expect_err("free call in #[require] must be rejected");
    assert!(msgs.iter().any(|m| m.contains("do_thing")), "{msgs:?}");
}

// 9. A #[create] field nested inside an enum variant is a retarget when changed.
const NESTED_CREATE: &str = r#"
struct Inner { #[create] id: u32 }
enum Wrap { V(Inner) }
struct Compose { w: Wrap }
fn config() -> Compose { Compose { w: Wrap::V(Inner { id: 1 }) } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn nested_create_field_change_is_a_retarget() {
    let cx = cx();
    let new_src = NESTED_CREATE.replace("id: 1", "id: 2");
    let (_, old) = interpret(NESTED_CREATE, &cx).unwrap();
    let (_, new) = interpret(&new_src, &cx).unwrap();
    assert!(
        retarget_check(NESTED_CREATE, &old, &new).is_err(),
        "a #[create] field nested in an enum variant must be refused when changed"
    );
}

// 10. #[require] holds for EVERY instance of the scope type, not just the first.
const REQUIRE_SECOND: &str = r#"
#[require(replicas > 0)]
struct Backend { replicas: u32 }
struct Compose { a: Backend, b: Backend }
fn config() -> Compose { Compose { a: Backend { replicas: 1 }, b: Backend { replicas: 0 } } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn require_checks_every_instance() {
    // b.replicas == 0 violates; must be caught even though a (the first) passes.
    assert!(interpret(REQUIRE_SECOND, &cx()).is_err());
}

// 11. An author kind leaking into the config surface is rejected (no panic).
const KIND_IN_CONFIG: &str = r#"
struct Compose { k: LocalStateDir, n: u32 }
fn config() -> Compose { Compose { k: LocalStateDir, n: 1 } }
fn deployment(cfg: &Compose, cx: &Cx) -> Deployment { Deployment::new(&["default"]) }
"#;

#[test]
fn kind_in_config_is_rejected() {
    assert!(interpret(KIND_IN_CONFIG, &cx()).is_err());
}
