//! Match semantics executed through unmodified Monty — the behavioural
//! contract of the lowering, asserted on real interpreter output.

use spike_monty_tkdp::{lower::LowerOptions, run};

fn run_ok(source: &str) -> String {
    match run(source, "test.tkdp", &LowerOptions::default()) {
        Ok(Ok(outcome)) => outcome.value,
        Ok(Err(failure)) => panic!("execution failed:\n{failure}"),
        Err(diags) => panic!("preflight rejected: {diags:?}"),
    }
}

fn run_err(source: &str, options: &LowerOptions) -> String {
    match run(source, "test.tkdp", options) {
        Ok(Err(failure)) => failure.to_string(),
        Ok(Ok(outcome)) => panic!("expected failure, got {}", outcome.value),
        Err(diags) => panic!("preflight rejected: {diags:?}"),
    }
}

#[test]
fn class_pattern_dispatches_on_exact_variant() {
    let source = r#"
def name(s):
    match s:
        case InMemory():
            return "memory"
        case ManagedDsql(region=region):
            return "managed:" + region
        case PreexistingDsql(region=region, endpoint=endpoint, arn=_):
            return "existing:" + region + ":" + endpoint
        case _:
            return "unknown"

[
    name(InMemory()),
    name(ManagedDsql(region="eu-west-1")),
    name(PreexistingDsql(region="us-east-1", endpoint="db.example", arn="arn:x")),
    name("something else"),
]
"#;
    assert_eq!(
        run_ok(source),
        "['memory', 'managed:eu-west-1', 'existing:us-east-1:db.example', 'unknown']"
    );
}

#[test]
fn first_matching_case_wins() {
    let source = r#"
match 1:
    case 1:
        out = "first"
    case x:
        out = "capture"
out
"#;
    assert_eq!(run_ok(source), "'first'");
}

#[test]
fn literal_uses_equality_and_singleton_uses_identity() {
    let source = r#"
def probe(s):
    match s:
        case None:
            return "none"
        case 0:
            return "zero"
        case "dsql":
            return "dsql"
        case -2:
            return "neg"
        case _:
            return "other"

[probe(None), probe(0), probe(0.0), probe("dsql"), probe(-2), probe([])]
"#;
    // 0.0 == 0 in Python, so the literal-equality case takes it — faithful
    // to CPython match semantics.
    assert_eq!(
        run_ok(source),
        "['none', 'zero', 'zero', 'dsql', 'neg', 'other']"
    );
}

#[test]
fn guard_falls_through_and_bindings_persist() {
    let source = r#"
leaked = "unset"

def pick(s):
    match s:
        case ManagedDsql(region=r) if r == "eu-west-1":
            return "eu:" + r
        case ManagedDsql(region=r2):
            return "other:" + r2

out = pick(ManagedDsql(region="us-east-1"))
out
"#;
    assert_eq!(run_ok(source), "'other:us-east-1'");
}

#[test]
fn failed_guard_leaves_captures_bound_like_cpython() {
    let source = r#"
match ManagedDsql(region="us-east-1"):
    case ManagedDsql(region=r) if r == "eu-west-1":
        out = "eu"
    case _:
        out = "fell through, r = " + r
out
"#;
    assert_eq!(run_ok(source), "'fell through, r = us-east-1'");
}

#[test]
fn guard_runs_only_when_pattern_matched() {
    let source = r#"
calls = []

def noisy(r):
    calls.append(r)
    return False

match ManagedDsql(region="us"):
    case InMemory() if noisy("wrong type"):
        out = "a"
    case ManagedDsql(region=r) if noisy(r):
        out = "b"
    case _:
        out = "c"
[out, calls]
"#;
    assert_eq!(run_ok(source), "['c', ['us']]");
}

#[test]
fn capture_case_binds_subject() {
    let source =
        "match ManagedDsql(region=\"eu\"):\n    case whole:\n        out = whole.region\nout\n";
    assert_eq!(run_ok(source), "'eu'");
}

#[test]
fn strict_exhaustion_raises_with_original_position() {
    let source = "match \"nope\":\n    case 1:\n        pass\n";
    let failure = run_err(source, &LowerOptions::default());
    assert!(
        failure.contains("test.tkdp:1: match fell through: no case matched 'nope'"),
        "{failure}"
    );
}

#[test]
fn faithful_exhaustion_falls_through_silently() {
    let source = "out = \"before\"\nmatch \"nope\":\n    case 1:\n        out = \"matched\"\nout\n";
    let options = LowerOptions {
        strict_exhaustion: false,
    };
    match run(source, "test.tkdp", &options) {
        Ok(Ok(outcome)) => assert_eq!(outcome.value, "'before'"),
        other => panic!("expected silent fall-through, got {other:?}"),
    }
}

#[test]
fn nested_match_and_loop_control_flow() {
    let source = r#"
results = []
for item in [ManagedDsql(region="eu"), InMemory(), ManagedDsql(region="us")]:
    match item:
        case ManagedDsql(region=r):
            match r:
                case "eu":
                    results.append("eu-managed")
                case _:
                    results.append("other-managed")
        case InMemory():
            results.append("memory")
results
"#;
    assert_eq!(run_ok(source), "['eu-managed', 'memory', 'other-managed']");
}

#[test]
fn break_inside_case_body_exits_enclosing_loop() {
    let source = r#"
seen = []
for item in [1, "stop", 3]:
    match item:
        case "stop":
            break
        case x:
            seen.append(x)
seen
"#;
    assert_eq!(run_ok(source), "[1]");
}

#[test]
fn missing_pattern_field_reports_variant_and_field() {
    let source =
        "match ManagedDsql(region=\"eu\"):\n    case ManagedDsql(endpoint=e):\n        out = e\n";
    let failure = run_err(source, &LowerOptions::default());
    assert!(
        failure.contains("pattern field 'endpoint' does not exist on ManagedDsql"),
        "{failure}"
    );
    // The failing frame maps to the pattern in the original file, not into
    // the generated program or the prelude alone.
    assert!(failure.contains("test.tkdp:2"), "{failure}");
}

#[test]
fn match_inside_user_dataclass_flow() {
    // User-defined dataclasses in the .tkdp itself participate in matching
    // exactly like prelude variants (monty#626 in-sandbox dataclasses).
    let source = r#"
from dataclasses import dataclass


@dataclass
class Blue:
    depth: int


@dataclass
class Red:
    heat: int


def describe(c):
    match c:
        case Blue(depth=d):
            return "blue:" + repr(d)
        case Red(heat=h):
            return "red:" + repr(h)

[describe(Blue(depth=3)), describe(Red(heat=9))]
"#;
    assert_eq!(run_ok(source), "['blue:3', 'red:9']");
}

#[test]
fn print_output_is_captured() {
    let source = "match 1:\n    case 1:\n        print(\"hello from tkdp\")\n";
    match run(source, "test.tkdp", &LowerOptions::default()) {
        Ok(Ok(outcome)) => assert_eq!(outcome.output, "hello from tkdp\n"),
        other => panic!("run failed: {other:?}"),
    }
}
