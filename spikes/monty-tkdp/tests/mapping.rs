//! Diagnostic mapping tests: Monty failures land on `.tkdp` positions.

use spike_monty_tkdp::{lower::LowerOptions, run};

fn failure(source: &str) -> String {
    match run(source, "test.tkdp", &LowerOptions::default()) {
        Ok(Err(failure)) => failure.to_string(),
        Ok(Ok(outcome)) => panic!("expected failure, got {}", outcome.value),
        Err(diags) => panic!("preflight rejected: {diags:?}"),
    }
}

#[test]
fn runtime_error_in_case_body_maps_to_original_line() {
    // Line 4 of the original file divides by zero inside the matched body.
    let source = "\
x = 1
match x:
    case 1:
        boom = 1 / 0
";
    let rendered = failure(source);
    assert!(rendered.contains("ZeroDivisionError"), "{rendered}");
    assert!(rendered.contains("test.tkdp:4:"), "{rendered}");
    assert!(rendered.contains("boom = 1 / 0"), "{rendered}");
}

#[test]
fn error_in_guard_maps_to_guard_expression() {
    let source = "\
match 1:
    case x if x / 0 > 1:
        pass
    case _:
        pass
";
    let rendered = failure(source);
    assert!(rendered.contains("ZeroDivisionError"), "{rendered}");
    // The guard is spliced verbatim, so the position is the guard's own
    // line in the original file.
    assert!(rendered.contains("test.tkdp:2:"), "{rendered}");
}

#[test]
fn error_in_subject_maps_to_subject_expression() {
    let source = "\
match missing_name:
    case _:
        pass
";
    let rendered = failure(source);
    assert!(rendered.contains("NameError"), "{rendered}");
    assert!(rendered.contains("test.tkdp:1:"), "{rendered}");
}

#[test]
fn prelude_frames_are_labelled_internal() {
    // A missing pattern field raises inside the prelude helper; the rendered
    // traceback names the prelude region rather than leaking generated
    // coordinates, and still carries the mapped pattern position.
    let source = "\
match ManagedDsql(region=\"eu\"):
    case ManagedDsql(endpoint=e):
        pass
";
    let rendered = failure(source);
    assert!(
        rendered.contains("in the tokeira prelude (internal)"),
        "{rendered}"
    );
    assert!(rendered.contains("test.tkdp:2:"), "{rendered}");
    assert!(!rendered.contains("<generated>"), "{rendered}");
}

#[test]
fn error_in_user_code_outside_match_maps_identically() {
    let source = "\
def config():
    return undefined_setting


match 1:
    case 1:
        pass
";
    let rendered = failure(source);
    assert!(rendered.contains("NameError"), "{rendered}");
    assert!(rendered.contains("test.tkdp:2:"), "{rendered}");
    assert!(rendered.contains("return undefined_setting"), "{rendered}");
}
