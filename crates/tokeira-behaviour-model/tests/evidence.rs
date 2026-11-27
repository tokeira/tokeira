//! Evidence integrity, dependency independence, and exact comparison precision.

use std::{collections::BTreeMap, process::Command};

use proptest::prelude::*;
use serde_json::Value;
use tokeira_behaviour_model::catalog::{
    Comparison, Evidence, RuleCatalog, SourceIndex, Support, compare_exact, validate_dependencies,
};

#[test]
fn checked_in_catalog_resolves() {
    RuleCatalog::checked_in()
        .validate(&SourceIndex::checked_in())
        .unwrap();
}

#[test]
fn only_exact_boundary_rules_have_source_only_support() {
    let catalog = RuleCatalog::checked_in();
    assert_eq!(
        catalog.supports("activity.retry-window"),
        Support::InvariantsOnly
    );
    let window = catalog
        .rules
        .iter()
        .find(|rule| rule.key == "activity.retry-window")
        .unwrap();
    assert_eq!(window.evidence, Evidence::AwaitingCalibration);
    let source_only: Vec<_> = catalog
        .rules
        .iter()
        .filter_map(|rule| {
            matches!(rule.evidence, Evidence::SourceOnly { .. }).then_some(rule.key.as_str())
        })
        .collect();
    assert_eq!(
        source_only,
        ["activity.retry-expiration-equality", "activity.timeout-due"]
    );
    for key in source_only {
        assert_eq!(catalog.supports(key), Support::SourceOnly);
    }
}

fn reject_mutation(which: u8, index: usize) {
    let mut catalog = RuleCatalog::checked_in();
    let source = SourceIndex::checked_in();
    let index = index % catalog.rules.len();
    let citation = &mut catalog.rules[index].rows[0].citations[0];
    match which {
        0 => citation.start = 0,
        1 => citation.end = usize::MAX,
        2 => citation.tag = "v1.30.0".into(),
        3 => citation.range_digest = "0".repeat(64),
        4 => citation.source_digest = "0".repeat(64),
        5 => catalog.rules.push(catalog.rules[index].clone()),
        _ => catalog.rules[index].rows[0].citations.clear(),
    }
    assert!(catalog.validate(&source).is_err());
}

#[test]
fn invalid_range_tag_digest_duplicate_and_missing_citation_fail() {
    for which in 0..7 {
        reject_mutation(which, 0);
    }
    let mut source = SourceIndex::checked_in();
    source.commit = "0".repeat(40);
    assert!(RuleCatalog::checked_in().validate(&source).is_err());
}

#[test]
fn dependency_graph_is_independent_and_production_does_not_use_model() {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--all-features",
            "--offline",
            "--locked",
            "--format-version",
            "1",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).unwrap();
    let names: BTreeMap<_, _> = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p["id"].as_str().unwrap(), p["name"].as_str().unwrap()))
        .collect();
    let mut graph: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for node in metadata["resolve"]["nodes"].as_array().unwrap() {
        let name = names[node["id"].as_str().unwrap()];
        for dependency in node["deps"].as_array().unwrap() {
            let dependency_name = names[dependency["pkg"].as_str().unwrap()];
            graph
                .entry(name.into())
                .or_default()
                .push(dependency_name.into());
            if matches!(
                dependency_name,
                "tokeira-behaviour-model" | "tokeira-behaviour-harness"
            ) {
                assert!(
                    dependency["dep_kinds"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|kind| {
                            kind["kind"] == "dev"
                                // The unpublished harness consumes the pure model;
                                // forbidding production edges into the harness keeps
                                // this exception outside every production closure.
                                || (dependency_name == "tokeira-behaviour-model"
                                    && name == "tokeira-behaviour-harness"
                                    && kind["kind"].is_null())
                        }),
                    "production dependency from {name} to {dependency_name}"
                );
            }
        }
    }
    validate_dependencies("tokeira-behaviour-model", &graph).unwrap();
}

#[test]
fn every_forbidden_dependency_fails_transitively() {
    for forbidden in [
        "tokeira-kernel",
        "tokeira-runtime",
        "tokeira-edge",
        "tokeira-storage",
        "tokeira-compatibility",
        "tokeira-compatibility-service",
    ] {
        let graph = BTreeMap::from([
            ("model".into(), vec!["value".into()]),
            ("value".into(), vec![forbidden.into()]),
        ]);
        assert!(validate_dependencies("model", &graph).is_err());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: conformance-harness, Property 4. Corrupt evidence never resolves.
    #[test]
    fn generated_evidence_mutations_fail(which in 0u8..7, index in any::<usize>()) {
        reject_mutation(which, index);
    }

    // Feature: conformance-harness, Property 4. An indirect forbidden edge invalidates independence.
    #[test]
    fn generated_forbidden_dependency_fails(depth in 1usize..20, forbidden in 0usize..5) {
        let names = ["tokeira-kernel", "tokeira-runtime", "tokeira-edge", "tokeira-storage", "tokeira-compatibility"];
        let mut graph = BTreeMap::new();
        for n in 0..depth { graph.insert(n.to_string(), vec![(n+1).to_string()]); }
        graph.insert(depth.to_string(), vec![names[forbidden].into()]);
        prop_assert!(validate_dependencies("0", &graph).is_err());
    }

    // Feature: conformance-harness, Property 10. Exact comparisons retain the first differing ID or time.
    #[test]
    fn exact_comparison_detects_first_divergence(values in prop::collection::vec(any::<i128>(), 1..100), offset in any::<usize>()) {
        let index = offset % values.len();
        let mut altered = values.clone();
        altered[index] = altered[index].wrapping_add(1);
        for support in [Support::Calibrated, Support::SourceOnly, Support::InvariantsOnly] {
            prop_assert_eq!(compare_exact(&values, &altered, support), Comparison::Diverged(index));
            prop_assert_eq!(compare_exact(&values, &values[..index], support), Comparison::Diverged(index));
        }
        prop_assert_eq!(compare_exact(&values, &values, Support::InvariantsOnly), Comparison::InvariantsOnly);
        prop_assert_eq!(RuleCatalog::checked_in().supports("activity.retry-precedence"), Support::InvariantsOnly);
        prop_assert_eq!(RuleCatalog::checked_in().supports("unknown"), Support::InvariantsOnly);
    }
}
