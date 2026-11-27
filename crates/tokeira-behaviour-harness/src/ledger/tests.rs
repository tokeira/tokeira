//! Ledger mechanics use synthetic observations, never evidence about the engine.

use proptest::prelude::*;
use tokeira_behaviour_model::catalog::{Evidence, RuleCatalog};

use super::{
    check::{ignore_matches, test_declarations},
    render::cell,
    *,
};

const PAGE: &str = "Notes before.\n<!-- BEGIN GENERATED decisions -->\nold\n<!-- END GENERATED decisions -->\nNotes between.\n<!-- BEGIN GENERATED rpcs -->\nold\n<!-- END GENERATED rpcs -->\nArea note.\n<!-- BEGIN GENERATED areas -->\nold\n<!-- END GENERATED areas -->\nNotes after.\n";

fn fixture() -> (Ledger, RuleCatalog) {
    let catalog = RuleCatalog::checked_in();
    let mut ledger = Ledger::from_json(include_str!("../../decision-evidence.json")).unwrap();
    for decision in &mut ledger.decisions {
        let rule = catalog
            .rules
            .iter()
            .find(|r| r.key == decision.rule)
            .unwrap();
        decision.verdict = Verdict::Agrees;
        decision.fix_commit = None;
        decision.run = Some(RunEvidence {
            rule_digest: rule_digest(rule),
            junit_digest: "a".repeat(64),
            tests: decision
                .tests
                .iter()
                .map(|t| (t.identity(), Outcome::Passed))
                .collect(),
        });
    }
    (ledger, catalog)
}

fn set_outcome(decision: &mut DecisionEvidence, outcome: Outcome) {
    for result in decision.run.as_mut().unwrap().tests.values_mut() {
        *result = outcome;
    }
}

#[test]
fn each_verdict_requires_its_executed_outcome() {
    let (mut ledger, catalog) = fixture();
    let decision = &mut ledger.decisions[0];
    let rule = catalog
        .rules
        .iter()
        .find(|r| r.key == decision.rule)
        .unwrap();
    for verdict in [
        Verdict::Agrees,
        Verdict::DifferedAndFixed,
        Verdict::IgnoredDifference,
        Verdict::NotYetTested,
    ] {
        decision.verdict = verdict;
        decision.fix_commit = (verdict == Verdict::DifferedAndFixed).then(|| "b68c7a86".into());
        let expected = if verdict == Verdict::IgnoredDifference {
            Outcome::Failed
        } else {
            Outcome::Passed
        };
        set_outcome(decision, expected);
        assert_eq!(
            decision.effective_verdict(rule),
            (verdict, decision.reason.clone())
        );
        for outcome in [Outcome::Missing, Outcome::Skipped, Outcome::Error] {
            set_outcome(decision, outcome);
            assert_eq!(decision.effective_verdict(rule).0, Verdict::NotYetTested);
        }
        if verdict != Verdict::NotYetTested {
            set_outcome(
                decision,
                if expected == Outcome::Passed {
                    Outcome::Failed
                } else {
                    Outcome::Passed
                },
            );
            assert_eq!(decision.effective_verdict(rule).0, Verdict::NotYetTested);
        }
    }
}

#[test]
fn source_only_label_and_reason_come_from_catalog() {
    let (ledger, catalog) = fixture();
    let page = render_page(PAGE, &ledger, &catalog).unwrap();
    for rule in &catalog.rules {
        if let Evidence::SourceOnly { reason } = &rule.evidence {
            let row = page
                .lines()
                .find(|line| line.starts_with(&format!("| {} |", rule.key)))
                .unwrap();
            assert!(row.contains("agrees (source-only)"));
            assert!(row.contains(&cell(reason)));
        }
    }
}

#[test]
fn notes_outside_generated_blocks_are_byte_preserved() {
    let (ledger, catalog) = fixture();
    let page = render_page(PAGE, &ledger, &catalog).unwrap();
    for note in [
        "Notes before.\n",
        "\nNotes between.\n",
        "\nArea note.\n",
        "\nNotes after.\n",
    ] {
        assert!(page.contains(note));
    }
    assert_eq!(render_page(&page, &ledger, &catalog).unwrap(), page);
    assert!(
        render_page(
            &PAGE.replace("END GENERATED areas", "END GENERATED rpcs"),
            &ledger,
            &catalog
        )
        .is_err()
    );
}

#[test]
fn stale_digest_cannot_claim_agreement_and_check_rejects_it() {
    let (ledger, mut catalog) = fixture();
    catalog.rules[0].rows[0].transition.push_str(" changed");
    assert!(
        ledger
            .check_current(&catalog)
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
    let page = render_page(PAGE, &ledger, &catalog).unwrap();
    let row = page
        .lines()
        .find(|line| line.starts_with(&format!("| {} |", catalog.rules[0].key)))
        .unwrap();
    assert!(row.contains("not yet tested"));
    assert!(row.contains("stale"));
    assert!(!row.contains("| agrees"));
}

#[test]
fn junit_preserves_missing_skipped_failed_and_error_outcomes() {
    let (mut ledger, catalog) = fixture();
    let first = ledger.decisions[0].tests[0].clone();
    for (contents, expected) in [
        ("", Outcome::Passed),
        ("<failure message=\"difference\"/>", Outcome::Failed),
        ("<error/>", Outcome::Error),
        ("<skipped/>", Outcome::Skipped),
        ("<flakyFailure/>", Outcome::Error),
    ] {
        let xml = format!(
            "<testsuites><testsuite name=\"{}\"><testcase name=\"{}\">{contents}</testcase></testsuite></testsuites>",
            first.crate_name, first.name
        );
        record_run(&mut ledger, &catalog, &xml).unwrap();
        assert_eq!(
            ledger.decisions[0].run.as_ref().unwrap().tests[&first.identity()],
            expected
        );
        assert!(
            ledger
                .decisions
                .iter()
                .flat_map(|d| d.run.as_ref().unwrap().tests.values())
                .any(|o| *o == Outcome::Missing)
        );
        assert_eq!(
            ledger.decisions[0].run.as_ref().unwrap().junit_digest,
            digest(xml.as_bytes())
        );
    }
    let duplicate = format!(
        "<testsuites><testsuite name=\"{}\"><testcase name=\"{}\"/><testcase name=\"{}\"/></testsuite></testsuites>",
        first.crate_name, first.name, first.name
    );
    assert!(record_run(&mut ledger, &catalog, &duplicate).is_err());
    assert!(record_run(&mut ledger, &catalog, "<testsuites/>").is_err());
    assert!(record_run(&mut ledger, &catalog, "<testsuites><testsuite name=\"wrong\"><testcase name=\"test\"/></testsuite></testsuites>").is_err());
}

#[test]
fn rust_declarations_retain_ignore_reasons_and_exclude_helpers_and_comments() {
    let source = r##"
        // #[test] fn fake() {}
        const S: &str = "#[test] fn fake_string() {}";
        fn helper() {}
        #[tokio::test(start_paused = true)] async fn ordinary() {}
        proptest! {
            #![proptest_config(Config::default())]
            #[test]
            #[ignore = "precise reason"]
            fn generated(value in any::<bool>()) {}
        }
    "##;
    let declarations = test_declarations(source).unwrap();
    assert_eq!(declarations.len(), 2);
    assert_eq!(declarations["ordinary"], None);
    assert_eq!(declarations["generated"].as_deref(), Some("precise reason"));
    assert!(test_declarations("#[test] #[cfg_attr(unix, ignore)] fn conditional() {}").is_err());
}

#[test]
fn source_check_rejects_missing_test_or_wrong_ignore_disposition() {
    assert!(ignore_matches(
        Verdict::IgnoredDifference,
        "specific difference",
        Some("specific difference")
    ));
    assert!(!ignore_matches(
        Verdict::IgnoredDifference,
        "specific difference",
        Some("different reason")
    ));
    assert!(!ignore_matches(
        Verdict::IgnoredDifference,
        "specific difference",
        Some("")
    ));
    assert!(!ignore_matches(
        Verdict::Agrees,
        "specific difference",
        Some("specific difference")
    ));
    let (mut ledger, _) = fixture();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    check_sources(&ledger, root).unwrap();
    ledger.decisions[0].verdict = Verdict::IgnoredDifference;
    assert!(
        check_sources(&ledger, root)
            .unwrap_err()
            .to_string()
            .contains("ignore")
    );
    ledger.decisions[0].verdict = Verdict::Agrees;
    ledger.decisions[0].tests[0].name = "absent".into();
    assert!(
        check_sources(&ledger, root)
            .unwrap_err()
            .to_string()
            .contains("missing test")
    );
}

#[test]
fn descriptor_partition_and_all_three_kinds_are_retained() {
    let rpcs = surface::rpcs().unwrap();
    assert_eq!(rpcs.len(), 129);
    assert_eq!(rpcs.iter().filter(|rpc| rpc.newer).count(), 8);
    let (ledger, catalog) = fixture();
    let page = render_page(PAGE, &ledger, &catalog).unwrap();
    assert_eq!(
        page.lines()
            .filter(|line| line.starts_with("| activity."))
            .count(),
        16
    );
    assert_eq!(
        page.lines()
            .filter(|line| line.starts_with("| /temporal."))
            .count(),
        129
    );
    assert!(page.contains("16 decision rows | 14 | 2 |"));
}

fn profile_config(filter: &str) -> String {
    format!("[profile.behaviour-ledger]\ndefault-filter = '''\n{filter}\n'''\n")
}

#[test]
fn profile_filter_rejects_missing_comparison() {
    let (ledger, _) = fixture();
    let expected = comparison_filter(&ledger);
    let compact = expected.split_whitespace().collect::<String>();
    check_profile_filter(&ledger, &profile_config(&compact)).unwrap();
    let missing = expected.lines().skip(1).collect::<Vec<_>>().join("\n");
    let error = check_profile_filter(&ledger, &profile_config(&missing)).unwrap_err();
    assert!(error.to_string().contains(&expected));
}

#[test]
fn profile_filter_rejects_extra_comparison() {
    let (ledger, _) = fixture();
    let expected = comparison_filter(&ledger);
    let extra = format!("{expected} |\ntest(=extra_comparison)");
    let error = check_profile_filter(&ledger, &profile_config(&extra)).unwrap_err();
    assert!(error.to_string().contains(&expected));
}

#[test]
fn refresh_preserves_recorded_run_facts() {
    let (mut ledger, mut catalog) = fixture();
    set_outcome(&mut ledger.decisions[0], Outcome::Missing);
    set_outcome(&mut ledger.decisions[1], Outcome::Failed);
    let before = ledger.to_json();
    let page = refresh_page(PAGE, &ledger, &catalog).unwrap();
    assert_ne!(page, PAGE);
    assert_eq!(ledger.to_json(), before);
    assert_eq!(refresh_page(&page, &ledger, &catalog).unwrap(), page);
    catalog.rules[0].rows[0].transition.push_str(" changed");
    assert!(
        refresh_page(&page, &ledger, &catalog)
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
    assert_eq!(ledger.to_json(), before);
}

#[test]
fn import_replaces_outcomes_after_comparison_rename() {
    let (mut ledger, catalog) = fixture();
    let old_identity = ledger.decisions[0].tests[0].identity();
    ledger.decisions[0].tests[0].name.push_str("_renamed");
    assert!(ledger.check_current(&catalog).is_err());
    let before = ledger.to_json();
    assert!(record_run(&mut ledger, &catalog, "<testsuites/>").is_err());
    assert_eq!(ledger.to_json(), before);
    let test = ledger.decisions[0].tests[0].clone();
    let report = format!(
        "<testsuites><testsuite name=\"{}\"><testcase name=\"{}\"/></testsuite></testsuites>",
        test.crate_name, test.name
    );
    record_run(&mut ledger, &catalog, &report).unwrap();
    ledger.check_current(&catalog).unwrap();
    let run = ledger.decisions[0].run.as_ref().unwrap();
    assert!(!run.tests.contains_key(&old_identity));
    assert_eq!(run.tests[&test.identity()], Outcome::Passed);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, failure_persistence: None, ..ProptestConfig::default() })]

    // Feature: conformance-harness, Property 15. Input order cannot change provenance, reasons, gaps or row kinds.
    #[test]
    fn generated_evidence_is_deterministic_and_conservative(
        states in prop::collection::vec(0u8..8, 16),
        reason in "[a-zA-Z0-9|<>&][a-zA-Z0-9 |<>&]{0,63}",
        rotation in any::<usize>(),
        ranks in prop::collection::vec(any::<u64>(), 16),
    ) {
        let (mut ledger, mut catalog) = fixture();
        for (decision, state) in ledger.decisions.iter_mut().zip(states) {
            decision.reason = reason.clone();
            match state {
                0 => {},
                1 => { decision.verdict = Verdict::DifferedAndFixed; decision.fix_commit = Some("b68c7a86".into()); },
                2 => { decision.verdict = Verdict::IgnoredDifference; set_outcome(decision, Outcome::Failed); },
                3 => { decision.run.as_mut().unwrap().rule_digest = "b".repeat(64); },
                4 => set_outcome(decision, Outcome::Missing),
                5 => set_outcome(decision, Outcome::Skipped),
                6 => set_outcome(decision, Outcome::Error),
                _ => decision.verdict = Verdict::NotYetTested,
            }
        }
        let first = render_page(PAGE, &ledger, &catalog).unwrap();
        for decision in &ledger.decisions {
            let row = first.lines().find(|line| line.starts_with(&format!("| {} |", decision.rule))).unwrap();
            prop_assert!(row.contains(&cell(&reason)));
            prop_assert!(row.contains(&decision.run.as_ref().unwrap().junit_digest));
            if decision.verdict == Verdict::IgnoredDifference || decision.run.as_ref().unwrap().rule_digest == "b".repeat(64) {
                prop_assert!(!row.contains("| agrees"));
                prop_assert!(!row.contains("| differed and fixed"));
            }
            let rule = catalog.rules.iter().find(|r| r.key == decision.rule).unwrap();
            for branch in &rule.rows {
                for citation in &branch.citations { prop_assert!(row.contains(&citation.path)); }
            }
        }
        prop_assert_eq!(first.lines().filter(|line| line.starts_with("| /temporal.")).count(), 129);
        prop_assert!(first.lines().filter(|line| line.starts_with("| /temporal.")).all(|line| line.ends_with("| no behavioural evidence yet |")));
        prop_assert!(first.contains("rule catalog"));
        prop_assert!(first.contains("16 decision rows | 14 | 2 |"));
        ledger.decisions.rotate_left(rotation % 16);
        ledger.decisions.reverse();
        ledger.decisions.sort_by_key(|decision| ranks[catalog.rules.iter().position(|rule| rule.key == decision.rule).unwrap()]);
        catalog.rules.reverse();
        for decision in &mut ledger.decisions { decision.tests.reverse(); decision.counterparts.reverse(); decision.rpcs.reverse(); }
        prop_assert_eq!(&first, &render_page(PAGE, &ledger, &catalog).unwrap());
        let mut missing = ledger.clone(); missing.decisions.pop();
        prop_assert!(render_page(PAGE, &missing, &catalog).is_err());
        let mut unknown = ledger.clone(); unknown.decisions[0].rule = "unknown".into();
        prop_assert!(render_page(PAGE, &unknown, &catalog).is_err());
    }
}
