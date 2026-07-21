use std::{collections::BTreeSet, fs, path::PathBuf};

#[derive(Debug)]
struct SurfaceAuditRow {
    qualified_name: String,
    classification: String,
    implementation_notes: String,
    target_spec: String,
}

#[derive(Debug)]
struct MatrixRow {
    kernel_impact: String,
    runtime_impact: String,
    projection_impact: String,
    implementation_notes: String,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should be two levels above tokeira-edge")
        .to_path_buf()
}

fn design_doc() -> String {
    let path = workspace_root().join(".kiro/specs/temporal-api-v1.62-sync/design.md");
    fs::read_to_string(path).expect("temporal API sync design should be readable")
}

fn table_cells(line: &str) -> Option<Vec<String>> {
    if !line.starts_with('|') || line.contains("|---") {
        return None;
    }
    let cells = line
        .trim()
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect::<Vec<_>>();
    (!cells.is_empty()).then_some(cells)
}

fn surface_audit_rows(design: &str) -> Vec<SurfaceAuditRow> {
    design
        .split("## Surface_Audit")
        .nth(1)
        .expect("Surface_Audit section should exist")
        .split("## Implementation & Escalation Matrix")
        .next()
        .expect("Surface_Audit section should end before matrix")
        .lines()
        .filter_map(table_cells)
        .filter(|cells| cells.len() == 6 && cells[0] != "Kind")
        .map(|cells| SurfaceAuditRow {
            qualified_name: cells[1].clone(),
            classification: cells[3].clone(),
            implementation_notes: cells[4].clone(),
            target_spec: cells[5].clone(),
        })
        .collect()
}

fn matrix_rows(design: &str) -> Vec<MatrixRow> {
    design
        .split("## Implementation & Escalation Matrix")
        .nth(1)
        .expect("Implementation & Escalation Matrix section should exist")
        .split("## Classification Rationale")
        .next()
        .expect("matrix section should end before classification rationale")
        .lines()
        .filter_map(table_cells)
        .filter(|cells| cells.len() == 6 && cells[0] != "Qualified Name")
        .map(|cells| MatrixRow {
            kernel_impact: cells[2].clone(),
            runtime_impact: cells[3].clone(),
            projection_impact: cells[4].clone(),
            implementation_notes: cells[5].clone(),
        })
        .collect()
}

fn target_spec_name(cell: &str) -> Option<String> {
    let cell = cell.trim();
    if cell.is_empty() || cell == "—" {
        return None;
    }
    if let Some((_, rest)) = cell.split_once('`')
        && let Some((name, _)) = rest.split_once('`')
    {
        return Some(name.to_string());
    }
    cell.split_whitespace()
        .next()
        .map(|name| name.trim_matches('`').to_string())
}

#[test]
fn every_target_spec_name_exists_as_workspace_directory() {
    let design = design_doc();
    let specs_dir = workspace_root().join(".kiro/specs");
    let missing = surface_audit_rows(&design)
        .iter()
        .filter_map(|row| target_spec_name(&row.target_spec))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|spec| !specs_dir.join(spec).is_dir())
        .collect::<Vec<_>>();

    assert!(missing.is_empty(), "missing target specs: {missing:?}");
}

#[test]
fn implementation_matrix_escalation_invariant_holds() {
    let design = design_doc();
    for row in matrix_rows(&design) {
        let classified_deferred = row
            .implementation_notes
            .starts_with("**Classified Deferred**");
        if !row.kernel_impact.starts_with("none") {
            assert!(
                classified_deferred || row.kernel_impact == "existing transition field",
                "kernel-impact row is not escalated: {row:?}"
            );
        }
        if !row.runtime_impact.starts_with("none") {
            assert!(
                classified_deferred
                    || row.runtime_impact.contains("single-file edit")
                    || row.runtime_impact.contains("single new file")
                    || row.runtime_impact.contains("existing broker state")
                    || row
                        .runtime_impact
                        .contains("existing reachability queries unchanged")
                    || row.runtime_impact.contains("HeartbeatStore"),
                "runtime-impact row exceeds in-scope budget: {row:?}"
            );
        }
        if !row.projection_impact.starts_with("none") {
            assert!(
                classified_deferred || !row.projection_impact.contains("migration"),
                "projection-impact row requires migration without escalation: {row:?}"
            );
        }
    }

    let kernel_cargo =
        fs::read_to_string(workspace_root().join("crates/tokeira-kernel/Cargo.toml"))
            .expect("kernel Cargo.toml should be readable");
    assert!(!kernel_cargo.contains("tokio"));
    assert!(!kernel_cargo.contains("async-trait"));
    assert!(!kernel_cargo.contains("tonic"));

    let kernel_src = workspace_root().join("crates/tokeira-kernel/src");
    let forbidden_imports = ["use tokio", "use async_trait", "use tonic", "use prost"];
    for entry in fs::read_dir(kernel_src).expect("kernel src should be readable") {
        let entry = entry.expect("kernel src entry should be readable");
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(entry.path()).expect("kernel source should be readable");
        for import in forbidden_imports {
            assert!(
                !source.contains(import),
                "kernel source contains forbidden import {import}"
            );
        }
    }
}

#[test]
fn worker_inventory_surface_audit_is_observation_backed() {
    let design = design_doc();
    let rows = surface_audit_rows(&design);
    for rpc in ["RecordWorkerHeartbeat", "DescribeWorker", "ListWorkers"] {
        let qualified_name = format!("`WorkflowService.{rpc}`");
        let row = rows
            .iter()
            .find(|row| row.qualified_name == qualified_name)
            .unwrap_or_else(|| panic!("{rpc} surface audit row should exist"));

        assert_eq!(row.classification, "Wire through");
        assert!(
            row.implementation_notes.contains("HeartbeatStore"),
            "{rpc} row should mention HeartbeatStore: {row:?}"
        );
        assert_eq!(
            target_spec_name(&row.target_spec).as_deref(),
            Some("worker-heartbeat-observability")
        );
    }
}
