//! Lowering shape tests: canonical output, determinism, and the source map.

use ruff_text_size::TextSize;
use spike_monty_tkdp::{
    lower::{LowerOptions, lower},
    preflight::preflight,
    source_map::{Mapped, OriginKind, SourceMapBuilder},
};

fn lowered_text(source: &str, options: &LowerOptions) -> String {
    let pf = preflight(source).expect("preflight");
    lower(source, &pf.module, "test.tkdp", options).text
}

const REPRESENTATIVE: &str = r#"def pick(s):
    match s:
        case ManagedDsql(region=region):
            return "managed:" + region
        case "dsql":
            return "literal"
        case None:
            return "none"
        case _:
            return "other"
"#;

/// The exact canonical lowering of [`REPRESENTATIVE`]. Deliberately a full
/// golden string: the lowered form is a contract (deterministic digests
/// depend on it), so shape drift must be a conscious edit here.
const REPRESENTATIVE_LOWERED: &str = r#"def pick(s):
    __tokeira_internal_subject_0 = (s)
    __tokeira_internal_done_0 = False
    if not __tokeira_internal_done_0:
        __tokeira_internal_case_0_0 = __tokeira_internal_match(__tokeira_internal_subject_0, ManagedDsql, ["region"])
        if __tokeira_internal_case_0_0 is not None:
            region = __tokeira_internal_case_0_0[0]
            __tokeira_internal_done_0 = True
            return "managed:" + region
    if not __tokeira_internal_done_0:
        if __tokeira_internal_subject_0 == ("dsql"):
            __tokeira_internal_done_0 = True
            return "literal"
    if not __tokeira_internal_done_0:
        if __tokeira_internal_subject_0 is None:
            __tokeira_internal_done_0 = True
            return "none"
    if not __tokeira_internal_done_0:
        if True:
            __tokeira_internal_done_0 = True
            return "other"
"#;

#[test]
fn representative_lowering_is_canonical() {
    let text = lowered_text(REPRESENTATIVE, &LowerOptions::default());
    assert_eq!(text, REPRESENTATIVE_LOWERED);
}

#[test]
fn lowering_is_deterministic() {
    let opts = LowerOptions::default();
    assert_eq!(
        lowered_text(REPRESENTATIVE, &opts),
        lowered_text(REPRESENTATIVE, &opts)
    );
}

#[test]
fn strict_exhaustion_appends_raise_only_without_irrefutable_case() {
    let refutable_only = "match s:\n    case 1:\n        pass\n";
    let strict = lowered_text(refutable_only, &LowerOptions::default());
    assert!(
        strict.contains("raise RuntimeError(\"test.tkdp:1: match fell through"),
        "{strict}"
    );

    let faithful = lowered_text(
        refutable_only,
        &LowerOptions {
            strict_exhaustion: false,
        },
    );
    assert!(!faithful.contains("raise RuntimeError"), "{faithful}");

    // A trailing wildcard makes the raise dead code, so it is not emitted.
    let with_wildcard = lowered_text(REPRESENTATIVE, &LowerOptions::default());
    assert!(
        !with_wildcard.contains("raise RuntimeError"),
        "{with_wildcard}"
    );
}

#[test]
fn guard_wraps_verbatim_and_shifts_body() {
    let source = r#"match s:
    case ManagedDsql(region=r) if r != "":
        use(r)
"#;
    let text = lowered_text(source, &LowerOptions::default());
    // Top-level match: probe at +4, guard at +8, body shifted to +12.
    assert!(text.contains("        if (r != \"\"):\n"), "{text}");
    assert!(text.contains("            use(r)\n"), "{text}");
    // Captures bind before the guard line.
    let bind = text
        .find("r = __tokeira_internal_case_0_0[0]")
        .expect("binding");
    let guard = text.find("if (r != \"\")").expect("guard");
    assert!(bind < guard);
}

#[test]
fn nested_and_sequential_matches_get_distinct_indices() {
    let source = r#"match a:
    case 1:
        match b:
            case 2:
                pass
match c:
    case 3:
        pass
"#;
    let text = lowered_text(source, &LowerOptions::default());
    for needle in [
        "__tokeira_internal_subject_0 = (a)",
        "__tokeira_internal_subject_1 = (b)",
        "__tokeira_internal_subject_2 = (c)",
    ] {
        assert!(text.contains(needle), "missing {needle} in:\n{text}");
    }
}

#[test]
fn two_space_indentation_is_reindented() {
    let source = "def f(s):\n  match s:\n    case _:\n      out = 1\n  return out\n";
    let text = lowered_text(source, &LowerOptions::default());
    // Body normalizes to the match column plus two generated steps.
    assert!(text.contains("\n          out = 1\n"), "{text}");
    // Code after the match keeps its original indentation.
    assert!(text.contains("\n  return out\n"), "{text}");
}

#[test]
fn inline_case_body_splices_onto_its_own_line() {
    let source = "match s:\n    case _: out = 1\n";
    let text = lowered_text(source, &LowerOptions::default());
    assert!(text.contains("\n        out = 1\n"), "{text}");
}

#[test]
fn comments_between_statements_survive() {
    let source =
        "# leading comment\nx = 1\n# between\nmatch x:\n    case _:\n        pass\n# trailing\n";
    let text = lowered_text(source, &LowerOptions::default());
    for needle in ["# leading comment", "# between", "# trailing"] {
        assert!(text.contains(needle), "missing {needle} in:\n{text}");
    }
}

#[test]
fn map_covers_verbatim_body_and_generated_scaffolding() {
    let pf = preflight(REPRESENTATIVE).expect("preflight");
    let lowered = lower(
        REPRESENTATIVE,
        &pf.module,
        "test.tkdp",
        &LowerOptions::default(),
    );
    let mut builder = SourceMapBuilder::new();
    for seg in &lowered.segments {
        builder.push(seg.generated.len(), seg.origin.clone());
    }
    let map = builder.finish();

    // A byte inside the copied case body maps linearly back to the original.
    let gen_body = lowered.text.find("\"managed:\"").expect("body copy");
    let orig_body = REPRESENTATIVE.find("\"managed:\"").expect("body original");
    match map.translate(TextSize::new(gen_body as u32)) {
        Some(Mapped::Original {
            offset,
            context: None,
        }) => {
            assert_eq!(usize::from(offset), orig_body);
        }
        other => panic!("expected verbatim mapping, got {other:?}"),
    }

    // A byte inside the probe maps to the pattern with Pattern context.
    let gen_probe = lowered
        .text
        .find("__tokeira_internal_match(")
        .expect("probe");
    let orig_pattern = REPRESENTATIVE
        .find("ManagedDsql(region=region)")
        .expect("pattern");
    match map.translate(TextSize::new(gen_probe as u32)) {
        Some(Mapped::Original {
            offset,
            context: Some(OriginKind::Pattern),
        }) => {
            assert_eq!(usize::from(offset), orig_pattern);
        }
        other => panic!("expected pattern mapping, got {other:?}"),
    }
}
