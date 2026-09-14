//! Audit contracts keep deferred ownership explicit and wire changes out of the kernel.

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
    qualified_name: String,
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

fn rust_sources(root: &std::path::Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("source directory should be readable") {
        let path = entry.expect("source entry should be readable").path();
        if path.is_dir() {
            rust_sources(&path, output);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            output.push(path);
        }
    }
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
            qualified_name: cells[0].clone(),
            kernel_impact: cells[2].clone(),
            runtime_impact: cells[3].clone(),
            projection_impact: cells[4].clone(),
            implementation_notes: cells[5].clone(),
        })
        .collect()
}

fn campaign_design_doc() -> String {
    fs::read_to_string(workspace_root().join(".kiro/specs/temporal-v1.32-compatibility/design.md"))
        .expect("Temporal v1.32 campaign design should be readable")
}

fn qualified_name(cell: &str) -> &str {
    cell.split('`')
        .nth(1)
        .expect("qualified name is code-formatted")
}

// Feature: temporal-v1.32-compatibility, Property 3: every deferred surface has an owner directory
#[test]
fn campaign_deferred_surfaces_have_owner_directories() {
    let rows = surface_audit_rows(&campaign_design_doc());
    let deferred = rows.iter().filter(|row| row.classification == "Deferred");
    assert!(rows.iter().any(|row| row.classification == "Deferred"));
    for row in deferred {
        let spec = target_spec_name(&row.target_spec)
            .unwrap_or_else(|| panic!("deferred row has no owner: {row:?}"));
        assert!(
            workspace_root().join(".kiro/specs").join(spec).is_dir(),
            "{row:?}"
        );
    }
}

// Feature: temporal-v1.32-compatibility, Property 4: wire-through rows are kernel-free
#[test]
fn campaign_wire_through_rows_are_kernel_free() {
    let design = campaign_design_doc();
    let rows = surface_audit_rows(&design);
    let matrix = matrix_rows(&design);
    assert!(rows.iter().any(|row| row.classification == "Wire through"));
    for row in rows
        .iter()
        .filter(|row| row.classification == "Wire through")
    {
        let matches = matrix
            .iter()
            .filter(|entry| {
                qualified_name(&entry.qualified_name) == qualified_name(&row.qualified_name)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            matches.len(),
            1,
            "wire-through row must have one matrix entry: {row:?}"
        );
        assert_eq!(matches[0].kernel_impact, "none", "{row:?}");
    }
    for row in matrix {
        if row.kernel_impact != "none" {
            assert!(
                row.implementation_notes
                    .starts_with("**Classified Deferred**"),
                "{row:?}"
            );
        }
    }
}

// Feature: temporal-v1.32-compatibility, Property 7: capability literals match the policy table
#[test]
fn capability_construction_sites_never_use_default_spread() {
    let mut sources = Vec::new();
    rust_sources(
        &workspace_root().join("crates/tokeira-edge/src"),
        &mut sources,
    );
    let mut sites = 0;
    for path in sources {
        let source = fs::read_to_string(&path).expect("edge source should be readable");
        let markers = [
            "NamespaceCapabilities {",
            "SystemCapabilities {",
            "get_system_info_response::Capabilities {",
            "namespace_info::Capabilities {",
        ];
        for (start, marker) in markers
            .iter()
            .flat_map(|marker| source.match_indices(*marker))
        {
            sites += 1;
            // Restrict the scan to this body; an outer response may use defaults.
            let body = &source[start + marker.len()..];
            let mut depth = 1;
            let end = body
                .char_indices()
                .find_map(|(index, ch)| {
                    match ch {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        _ => {}
                    }
                    (depth == 0).then_some(index)
                })
                .expect("capability body should close");
            let compact = body[..end].split_whitespace().collect::<String>();
            assert!(
                !compact.contains("..Default::default()"),
                "implicit capability in {}",
                path.display()
            );
        }
    }
    assert!(
        sites > 0,
        "capability source scan must cover construction sites"
    );
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

#[test]
fn scoped_worker_authorization_is_absent_from_kernel_authority() {
    let root = workspace_root();
    let manifest = fs::read_to_string(root.join("crates/tokeira-kernel/Cargo.toml"))
        .expect("kernel manifest should be readable");
    for forbidden in ["tokeira-auth", "tokeira-storage"] {
        assert!(
            !manifest.contains(forbidden),
            "scoped Worker authorization must not add kernel dependency {forbidden}"
        );
    }

    let mut sources = Vec::new();
    rust_sources(&root.join("crates/tokeira-kernel/src"), &mut sources);
    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("kernel source should be readable");
        for forbidden in [
            "WorkerScope",
            "WorkerTaskProvenance",
            "ScopedWorkerSession",
            "tokeira_auth",
            "worker_task_provenance",
        ] {
            assert!(
                !source.contains(forbidden),
                "scoped Worker authority leaked into {} through {forbidden}",
                source_path.display()
            );
        }
    }
}
