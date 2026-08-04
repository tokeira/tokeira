//! Preflight boundary battery (spike-carried): the restricted subset admits
//! exactly what it documents and rejects everything else with the right code.
//
// Feature: tkdp-frontend, Properties 1–2: admission soundness and rejection
// completeness.

use tokeira_tkdp::preflight::preflight;

const FACADE: &[&str] = &["Context", "Deployment", "Probe", "Store"];

/// Entrypoint scaffolding so snippet-level sources satisfy the entrypoint
/// rules; the snippet lands inside `deployment`.
fn definition(snippet: &str) -> String {
    let indented: String = snippet
        .lines()
        .map(|line| {
            if line.is_empty() {
                "\n".to_string()
            } else {
                format!("    {line}\n")
            }
        })
        .collect();
    format!(
        "def config():\n    return None\n\n\n\
         def deployment(cfg, cx):\n{indented}    return None\n"
    )
}

fn codes_of(source: &str) -> Vec<&'static str> {
    match preflight(source, FACADE) {
        Ok(_) => Vec::new(),
        Err(findings) => findings.iter().map(|finding| finding.code).collect(),
    }
}

fn codes(snippet: &str) -> Vec<&'static str> {
    codes_of(&definition(snippet))
}

#[test]
fn full_supported_subset_is_admitted() {
    let snippet = r#"
match s:
    case Probe(label=label) if label != "":
        out = label
    case Store(path=path, replicas=_):
        out = path
    case "literal":
        out = "literal"
    case b"raw":
        out = "bytes"
    case 3:
        out = "three"
    case -1.5:
        out = "neg"
    case None:
        out = "none"
    case True:
        out = "true"
    case x if x == 2:
        out = "guarded capture"
    case _:
        out = "other"
"#;
    assert_eq!(codes(snippet), Vec::<&str>::new());
}

#[test]
fn syntax_error_is_tkdp001() {
    assert_eq!(codes_of("def broken(:\n    pass\n"), vec!["TKDP001"]);
}

#[test]
fn sequence_mapping_or_star_as_value_patterns_are_tkdp002() {
    let snippet = r#"
match s:
    case [first, second]:
        pass
    case {"region": r}:
        pass
    case Probe() | Store(path=_):
        pass
    case Store(path=_) as whole:
        pass
    case Color.RED:
        pass
"#;
    let codes = codes(snippet);
    assert!(codes.iter().all(|code| *code == "TKDP002"), "{codes:?}");
    assert!(codes.len() >= 5, "{codes:?}");
}

#[test]
fn positional_class_args_are_tkdp003() {
    assert_eq!(
        codes("match s:\n    case Store(path):\n        pass"),
        vec!["TKDP003"]
    );
}

#[test]
fn nested_keyword_subpattern_is_tkdp004() {
    assert_eq!(
        codes("match s:\n    case Store(path=\"/x\"):\n        pass"),
        vec!["TKDP004"]
    );
}

#[test]
fn dotted_class_name_is_tkdp005() {
    assert_eq!(
        codes("match s:\n    case aws.Store(path=p):\n        pass"),
        vec!["TKDP005"]
    );
}

#[test]
fn irrefutable_case_must_be_last() {
    assert_eq!(
        codes("match s:\n    case _:\n        pass\n    case 3:\n        pass"),
        vec!["TKDP006"]
    );
    assert_eq!(
        codes("match s:\n    case x:\n        pass\n    case 3:\n        pass"),
        vec!["TKDP006"]
    );
    // A guard makes a capture refutable, so mid-list is fine.
    assert_eq!(
        codes("match s:\n    case x if x > 1:\n        pass\n    case 3:\n        pass"),
        Vec::<&str>::new()
    );
}

#[test]
fn reserved_prefix_rejected_everywhere() {
    for snippet in [
        "__tokeira_internal_x = 1",
        "def __tokeira_internal_f():\n    pass",
        "def f(__tokeira_internal_p):\n    pass",
        "class __tokeira_internal_C:\n    pass",
        "y = obj.__tokeira_internal_attr",
        "f(__tokeira_internal_kw=1)",
        "import os as __tokeira_internal_os",
        "match s:\n    case __tokeira_internal_cap:\n        pass",
        "match s:\n    case C(__tokeira_internal_f=v):\n        pass",
    ] {
        assert_eq!(codes(snippet), vec!["TKDP007"], "snippet: {snippet}");
    }
}

#[test]
fn entrypoint_rules_are_tkdp008() {
    // Missing both entrypoints: one finding each.
    assert_eq!(codes_of("x = 1\n"), vec!["TKDP008", "TKDP008"]);
    // Wrong arities.
    assert_eq!(
        codes_of("def config(extra):\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n"),
        vec!["TKDP008"]
    );
    assert_eq!(
        codes_of("def config():\n    pass\n\n\ndef deployment(cfg):\n    pass\n"),
        vec!["TKDP008"]
    );
    // Duplicates.
    assert_eq!(
        codes_of(
            "def config():\n    pass\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n"
        ),
        vec!["TKDP008"]
    );
}

#[test]
fn duplicate_field_and_capture_are_rejected() {
    assert_eq!(
        codes("match s:\n    case C(a=x, a=y):\n        pass"),
        vec!["TKDP009"]
    );
    assert_eq!(
        codes("match s:\n    case C(a=x, b=x):\n        pass"),
        vec!["TKDP010"]
    );
}

#[test]
fn complex_literal_is_tkdp002() {
    assert_eq!(
        codes("match s:\n    case 1j:\n        pass"),
        vec!["TKDP002"]
    );
}

#[test]
fn tab_indentation_is_tkdp011() {
    assert_eq!(
        codes_of("def config():\n\treturn None\n\n\ndef deployment(cfg, cx):\n    pass\n"),
        vec!["TKDP011"]
    );
}

#[test]
fn nested_match_is_validated_too() {
    assert_eq!(
        codes("if s:\n    match s:\n        case [x]:\n            pass"),
        vec!["TKDP002"]
    );
}

#[test]
fn import_contract_is_tkdp012() {
    // Unpublished name.
    assert_eq!(
        codes_of(
            "from tokeira import Nope\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n"
        ),
        vec!["TKDP012"]
    );
    // `import tokeira` form.
    assert_eq!(
        codes_of(
            "import tokeira\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n"
        ),
        vec!["TKDP012"]
    );
    // Star import.
    assert_eq!(
        codes_of(
            "from tokeira import *\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n"
        ),
        vec!["TKDP012"]
    );
    // Aliases are fine, and recorded.
    let ok = preflight(
        "from tokeira import Store as S\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n",
        FACADE,
    )
    .expect("alias import admits");
    assert_eq!(ok.imports.len(), 1);
    assert_eq!(ok.imports[0].name, "Store");
    assert_eq!(ok.imports[0].bound_as, "S");
}
