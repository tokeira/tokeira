//! Local source-index regeneration requires the read-only Temporal checkout, so
//! this test is ignored: CI validates the checked-in evidence without reading
//! Temporal's source. Default execution checks both JSON files byte for byte.
//! `TOKEIRA_MODEL_SOURCE_WRITE=1` writes their regenerated contents instead;
//! `TOKEIRA_MODEL_TEMPORAL_CHECKOUT` overrides the checkout discovered from
//! linked-worktree metadata. Neither mode executes git in the Tokeira worktree.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};
use tokeira_behaviour_model::catalog::{COMMIT, RuleCatalog, SourceFile, SourceIndex, TAG};

const WRITE_ENV: &str = "TOKEIRA_MODEL_SOURCE_WRITE";
const CHECKOUT_ENV: &str = "TOKEIRA_MODEL_TEMPORAL_CHECKOUT";

fn reference_checkout(root: &Path) -> PathBuf {
    let mut metadata = root.join(".git");
    if metadata.is_file() {
        let pointer = fs::read_to_string(&metadata).expect("read worktree git metadata");
        metadata = root
            .join(
                pointer
                    .trim()
                    .strip_prefix("gitdir: ")
                    .expect("gitdir pointer"),
            )
            .canonicalize()
            .expect("resolve worktree git metadata");
    }
    let common = metadata.join("commondir");
    if common.exists() {
        metadata = metadata
            .join(
                fs::read_to_string(common)
                    .expect("read common git directory")
                    .trim(),
            )
            .canonicalize()
            .expect("resolve common git directory");
    }
    metadata
        .parent()
        .and_then(Path::parent)
        .expect("main checkout has a parent")
        .join("temporal")
}

fn git(checkout: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(checkout)
        .args(arguments)
        .output()
        .expect("execute read-only reference git command");
    assert!(
        output.status.success(),
        "git {arguments:?} in {} failed: {}",
        checkout.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
#[ignore = "requires the local read-only Temporal checkout; CI validates checked-in evidence"]
fn source_index_matches_tag() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checkout = env::var_os(CHECKOUT_ENV).map_or_else(
        || {
            reference_checkout(
                crate_dir
                    .parent()
                    .and_then(Path::parent)
                    .expect("workspace root"),
            )
        },
        PathBuf::from,
    );
    let write = match env::var(WRITE_ENV) {
        Err(env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        _ => panic!("{WRITE_ENV} must be unset for check mode or exactly 1 for write mode"),
    };
    let commit = git(&checkout, &["rev-parse", &format!("{TAG}^{{commit}}")]);
    assert_eq!(
        String::from_utf8(commit)
            .expect("reference commit is UTF-8")
            .trim(),
        COMMIT,
        "reference tag does not resolve to the pinned commit"
    );
    let rules_path = crate_dir.join("rules.json");
    let original_rules = fs::read_to_string(&rules_path).expect("read rules.json");
    let original_index =
        fs::read_to_string(crate_dir.join("source-index.json")).expect("read source-index.json");
    let mut catalog: RuleCatalog = serde_json::from_str(&original_rules).expect("parse rules.json");
    let mut index = SourceIndex {
        version: 1,
        tag: TAG.into(),
        commit: COMMIT.into(),
        files: BTreeMap::new(),
    };
    let mut sources = BTreeMap::new();
    for rule in &mut catalog.rules {
        for row in &mut rule.rows {
            for citation in &mut row.citations {
                assert_eq!(citation.tag, TAG, "wrong tag in {}", rule.key);
                let data = sources.entry(citation.path.clone()).or_insert_with(|| {
                    git(&checkout, &["show", &format!("{TAG}:{}", citation.path)])
                });
                // Inclusive splitting keeps each original newline byte in the
                // digest and does not invent an empty line after a final LF.
                let lines: Vec<_> = data.split_inclusive(|byte| *byte == b'\n').collect();
                assert!(
                    citation.start > 0
                        && citation.start <= citation.end
                        && citation.end <= lines.len(),
                    "invalid range {}:{}-{} in {}",
                    citation.path,
                    citation.start,
                    citation.end,
                    rule.key
                );
                let range = lines[citation.start - 1..citation.end].concat();
                citation.source_digest = sha256(data);
                citation.range_digest = sha256(&range);
                let file = index
                    .files
                    .entry(citation.path.clone())
                    .or_insert_with(|| SourceFile {
                        line_count: lines.len(),
                        digest: citation.source_digest.clone(),
                        ranges: BTreeMap::new(),
                    });
                file.ranges.insert(
                    format!("{}-{}", citation.start, citation.end),
                    citation.range_digest.clone(),
                );
            }
        }
    }
    catalog
        .validate(&index)
        .expect("regenerated evidence is valid");
    let outputs = [
        (
            "rules.json",
            original_rules,
            serde_json::to_string_pretty(&catalog).expect("serialize rules"),
        ),
        (
            "source-index.json",
            original_index,
            serde_json::to_string_pretty(&index).expect("serialize index"),
        ),
    ];
    let mut differences = Vec::new();
    for (name, original, mut regenerated) in outputs {
        regenerated.push('\n');
        if write {
            fs::write(crate_dir.join(name), regenerated)
                .unwrap_or_else(|error| panic!("write {name}: {error}"));
        } else if original != regenerated {
            let original_value: serde_json::Value = serde_json::from_str(&original)
                .unwrap_or_else(|error| panic!("parse {name}: {error}"));
            let regenerated_value: serde_json::Value =
                serde_json::from_str(&regenerated).expect("regenerated JSON is valid");
            differences.push(if original_value == regenerated_value {
                format!("{name}: only JSON layout differs; every source digest and value agrees")
            } else {
                format!("{name}: contents differ from the pinned source")
            });
        }
    }
    assert!(
        differences.is_empty(),
        "{}; regenerate with {WRITE_ENV}=1",
        differences.join("; ")
    );
}
