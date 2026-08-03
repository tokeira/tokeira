//! Preflight boundary tests: the restricted subset admits exactly what it
//! documents and rejects everything else with the right code.

use spike_monty_tkdp::check;

fn codes(source: &str) -> Vec<&'static str> {
    match check(source) {
        Ok(_) => Vec::new(),
        Err(diags) => diags.iter().map(|d| d.code).collect(),
    }
}

#[test]
fn full_supported_subset_is_admitted() {
    let source = r#"
def pick(s, limit):
    match s:
        case ManagedDsql(region=region) if region != "":
            return region
        case PreexistingDsql(region=region, endpoint=endpoint, arn=_):
            return region + endpoint
        case InMemory():
            return "mem"
        case "dsql":
            return "literal"
        case b"raw":
            return "bytes"
        case 3:
            return "three"
        case -1.5:
            return "neg"
        case None:
            return "none"
        case True:
            return "true"
        case x if x == limit:
            return "guarded capture"
        case _:
            return "other"
"#;
    assert_eq!(codes(source), Vec::<&str>::new());
}

#[test]
fn syntax_error_is_tkdp001() {
    assert_eq!(codes("def broken(:\n    pass\n"), vec!["TKDP001"]);
}

#[test]
fn sequence_mapping_or_star_as_value_patterns_rejected() {
    let source = r#"
match s:
    case [first, second]:
        pass
    case {"region": r}:
        pass
    case InMemory() | ManagedDsql(region=_):
        pass
    case ManagedDsql(region=_) as whole:
        pass
    case Color.RED:
        pass
"#;
    let codes = codes(source);
    // `as` carries its inner pattern too, so the OR arm inside it may also
    // report; assert the essential shape: every arm rejected via TKDP002.
    assert!(codes.iter().all(|c| *c == "TKDP002"), "{codes:?}");
    assert!(codes.len() >= 5, "{codes:?}");
}

#[test]
fn positional_class_args_are_tkdp003() {
    assert_eq!(
        codes("match s:\n    case Dsql(region):\n        pass\n"),
        vec!["TKDP003"]
    );
}

#[test]
fn nested_keyword_subpattern_is_tkdp004() {
    assert_eq!(
        codes("match s:\n    case ManagedDsql(region=\"us\"):\n        pass\n"),
        vec!["TKDP004"]
    );
}

#[test]
fn dotted_class_name_is_tkdp005() {
    assert_eq!(
        codes("match s:\n    case aws.ManagedDsql(region=r):\n        pass\n"),
        vec!["TKDP005"]
    );
}

#[test]
fn irrefutable_case_must_be_last() {
    let wildcard_mid = "match s:\n    case _:\n        pass\n    case 3:\n        pass\n";
    assert_eq!(codes(wildcard_mid), vec!["TKDP006"]);
    let capture_mid = "match s:\n    case x:\n        pass\n    case 3:\n        pass\n";
    assert_eq!(codes(capture_mid), vec!["TKDP006"]);
    // A guard makes a capture refutable, so mid-list is fine.
    let guarded_mid = "match s:\n    case x if x > 1:\n        pass\n    case 3:\n        pass\n";
    assert_eq!(codes(guarded_mid), Vec::<&str>::new());
}

#[test]
fn reserved_prefix_rejected_everywhere() {
    for source in [
        "__tokeira_internal_x = 1\n",
        "def __tokeira_internal_f():\n    pass\n",
        "def f(__tokeira_internal_p):\n    pass\n",
        "class __tokeira_internal_C:\n    pass\n",
        "y = obj.__tokeira_internal_attr\n",
        "f(__tokeira_internal_kw=1)\n",
        "import os as __tokeira_internal_os\n",
        "match s:\n    case __tokeira_internal_cap:\n        pass\n",
        "match s:\n    case C(__tokeira_internal_f=v):\n        pass\n",
    ] {
        assert_eq!(codes(source), vec!["TKDP007"], "source: {source}");
    }
}

#[test]
fn entrypoint_arity_is_enforced() {
    assert_eq!(codes("def config(extra):\n    pass\n"), vec!["TKDP008"]);
    assert_eq!(codes("def deployment(cfg):\n    pass\n"), vec!["TKDP008"]);
    assert_eq!(
        codes("def config():\n    pass\ndef deployment(cfg, cx):\n    pass\n"),
        Vec::<&str>::new()
    );
}

#[test]
fn duplicate_field_and_capture_are_rejected() {
    assert_eq!(
        codes("match s:\n    case C(a=x, a=y):\n        pass\n"),
        vec!["TKDP009"]
    );
    assert_eq!(
        codes("match s:\n    case C(a=x, b=x):\n        pass\n"),
        vec!["TKDP010"]
    );
}

#[test]
fn complex_literal_rejected() {
    assert_eq!(
        codes("match s:\n    case 1j:\n        pass\n"),
        vec!["TKDP002"]
    );
}

#[test]
fn nested_match_is_validated_too() {
    let source = "
def f(s):
    if s:
        match s:
            case [x]:
                pass
";
    assert_eq!(codes(source), vec!["TKDP002"]);
}

#[test]
fn tab_indentation_is_rejected() {
    assert_eq!(codes("def f():\n\treturn 1\n"), vec!["TKDP011"]);
    assert_eq!(codes("def f():\n    return 1\n"), Vec::<&str>::new());
}
