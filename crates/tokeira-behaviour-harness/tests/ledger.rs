//! Check mode is part of ordinary workspace tests; write mode refreshes the page
//! or imports a local run when an explicit JUnit report is supplied.

use std::{env, fs, path::Path};

use tokeira_behaviour_harness::ledger::{
    Ledger, check_profile_filter, check_sources, record_run, refresh_page,
};
use tokeira_behaviour_model::catalog::{RuleCatalog, SourceIndex};

#[test]
fn behavioural_ledger_matches_evidence() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let evidence_path = root.join("crates/tokeira-behaviour-harness/decision-evidence.json");
    let page_path = root.join("docs/conformance/behavioural-ledger.md");
    let catalog = RuleCatalog::checked_in();
    catalog.validate(&SourceIndex::checked_in()).unwrap();
    let original = fs::read_to_string(&evidence_path).unwrap();
    let mut ledger = Ledger::from_json(&original).unwrap();
    let config = fs::read_to_string(root.join(".config/nextest.toml")).unwrap();
    check_profile_filter(&ledger, &config).unwrap();
    check_sources(&ledger, root).unwrap();
    let write = match env::var("TOKEIRA_LEDGER_WRITE") {
        Err(env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        _ => panic!("TOKEIRA_LEDGER_WRITE must be unset or exactly 1"),
    };
    let junit = if write {
        env::var_os("TOKEIRA_LEDGER_JUNIT")
    } else {
        None
    };
    if let Some(path) = &junit {
        // Nextest starts tests in their package directory; relative report paths
        // belong to the workspace so the documented root commands remain valid.
        let xml = fs::read_to_string(root.join(path)).expect("read nextest JUnit report");
        record_run(&mut ledger, &catalog, &xml).unwrap();
    }
    let page = fs::read_to_string(&page_path).unwrap();
    let rendered = refresh_page(&page, &ledger, &catalog).unwrap();
    if write {
        if junit.is_some() {
            fs::write(evidence_path, ledger.to_json()).unwrap();
        }
        fs::write(page_path, rendered).unwrap();
    } else {
        assert_eq!(
            page, rendered,
            "generated blocks differ; refresh with TOKEIRA_LEDGER_WRITE=1 and no TOKEIRA_LEDGER_JUNIT, or, if a rule or comparison changed, rerun the behaviour-ledger comparison profile and import with TOKEIRA_LEDGER_WRITE=1 TOKEIRA_LEDGER_JUNIT=<report>"
        );
    }
}
