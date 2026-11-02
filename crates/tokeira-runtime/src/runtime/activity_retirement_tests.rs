//! Executable delivery-call inventory: old storage adapters remain decodable,
//! while no production runtime/edge path may produce or consume legacy work.

use std::path::Path;

use super::lease_fence_tests::{before_tests, production_sources};

const RETIRED_OPERATIONS: [&str; 4] = [
    ".persist_to_backlog(",
    ".drain_backlog(",
    ".list_due_dispatchable_activity_tasks(",
    ".list_due_dispatchable_activity_tasks_for_shard(",
];

fn unexpected_calls(source: &str, adapter: bool) -> Vec<&'static str> {
    RETIRED_OPERATIONS
        .into_iter()
        .filter(|operation| {
            // HistoryWaitRepository forwards the storage trait transparently. It is
            // an adapter, not a delivery caller; a second call is a new producer.
            source.matches(operation).count() != usize::from(adapter)
        })
        .collect()
}

#[test]
fn delivery_operation_inventory_has_no_legacy_producers_or_consumers() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let mut violations = Vec::new();
    for crate_name in ["tokeira-runtime", "tokeira-edge"] {
        let root = workspace.join("crates").join(crate_name).join("src");
        let mut files = Vec::new();
        production_sources(&root, &mut files);
        for file in files {
            let text = std::fs::read_to_string(&file).unwrap();
            let adapter = crate_name == "tokeira-edge" && file.ends_with("history_wait.rs");
            for operation in unexpected_calls(before_tests(&text), adapter) {
                violations.push(format!(
                    "{}: {operation}",
                    file.strip_prefix(workspace).unwrap().display()
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "retired delivery calls: {violations:#?}"
    );
    for operation in RETIRED_OPERATIONS {
        assert_eq!(unexpected_calls(operation, false), vec![operation]);
    }
    assert!(
        !workspace
            .join("crates/tokeira-runtime/src/backlog.rs")
            .exists(),
        "obsolete backlog delivery module restored"
    );
}

#[test]
fn activity_discovery_cannot_gain_a_foreign_mutation_path() {
    let source = include_str!("../activity_discovery.rs");
    for operation in [
        ".commit_",
        ".delete_",
        ".reconcile_",
        ".persist_",
        ".drain_",
        "list_workflow_rules",
        "matching_pause_rule",
    ] {
        assert!(
            !before_tests(source).contains(operation),
            "queue-home discovery gained owner operation {operation}"
        );
    }
}
