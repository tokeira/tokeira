//! The interpreted-subset reject pass (Proposal 004 Phase 3): the clean
//! `definition.tkd` passes; every out-of-subset construct is rejected with a
//! diagnostic; the original non-hermetic definition is rejected; and malformed
//! input never panics (the no-panic security invariant).

use std::path::PathBuf;

use tokeira_compose_deployment::{
    context::Cx,
    interp::{interpret, validate},
};

const SRC: &str = include_str!("../definition.tkd");

fn cx() -> Cx {
    Cx {
        project_name: "tokeira".into(),
        region: None,
        deployment_dir: PathBuf::from("/tmp/x"),
    }
}

fn rejects(src: &str, needle: &str) {
    let msgs = validate(src).expect_err("expected the definition to be rejected");
    assert!(
        msgs.iter().any(|m| m.contains(needle)),
        "expected a diagnostic containing `{needle}`, got: {msgs:?}"
    );
}

#[test]
fn clean_definition_passes_the_subset() {
    assert_eq!(validate(SRC), Ok(()));
}

#[test]
fn for_loop_is_rejected() {
    rejects("fn deployment() { for _ in 0..3 {} }", "for-loop");
}

#[test]
fn closure_is_rejected() {
    rejects("fn deployment() { let f = |x| x; }", "closure");
}

#[test]
fn std_env_call_is_rejected() {
    rejects(
        r#"fn deployment() { let h = std::env::var("HOME"); }"#,
        "std::env::var",
    );
}

#[test]
fn filesystem_methods_are_rejected() {
    rejects("fn deployment() { let b = p.exists(); }", "exists");
    rejects(r#"fn deployment() { let b = p.join("x"); }"#, "join");
}

#[test]
fn unknown_macro_is_rejected() {
    rejects(r#"fn deployment() { println!("hi"); }"#, "println!");
}

#[test]
fn unknown_method_is_rejected() {
    rejects("fn deployment() { let b = d.frobnicate(); }", "frobnicate");
}

#[test]
fn free_function_call_is_rejected() {
    rejects("fn deployment() { do_thing(); }", "do_thing");
}

#[test]
fn unplaced_kind_binding_is_rejected() {
    rejects("fn deployment() { let k = LocalStateDir; }", "inline");
}

#[test]
fn use_item_is_rejected() {
    rejects("use std::fs;\nfn deployment() {}", "use");
}

#[test]
fn the_original_non_hermetic_definition_is_rejected() {
    // The pre-Phase-1 `definition.rs` (the faithful warts): the allow-list must
    // bite on every relocated mechanic.
    let snap = include_str!("fixtures/non_hermetic_definition.rs.snapshot");
    let msgs = validate(snap).expect_err("the non-hermetic definition must be rejected");
    let joined = msgs.join("\n");
    for needle in ["for-loop", "std::env::var", "closure", "exists"] {
        assert!(joined.contains(needle), "expected `{needle}` in:\n{joined}");
    }
}

#[test]
fn malformed_definitions_never_panic() {
    let bad = [
        "@#$%^&*",
        "fn deployment() { for _ in 0..3 {} }",
        "fn config() -> X { loop {} }",
        "struct",
        "fn deployment(cfg, cx) { d.module() }",
        "}{}{}{",
        "",
    ];
    for src in bad {
        let _ = validate(src); // must not panic
        let _ = interpret(src, &cx()); // must not panic
    }

    // prefix-truncation fuzz over the real definition (ASCII, so char-boundary safe)
    let mut n = 0;
    while n <= SRC.len() {
        if SRC.is_char_boundary(n) {
            let _ = validate(&SRC[..n]);
            let _ = interpret(&SRC[..n], &cx());
        }
        n += 29;
    }
}
