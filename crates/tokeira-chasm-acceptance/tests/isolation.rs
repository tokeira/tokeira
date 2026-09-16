//! Guard the independent acceptance library boundary without importing activity tests.
#![cfg(test)]

use std::{fs, path::Path};

fn check_sources(directory: &Path, crate_root: &Path) {
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            check_sources(&path, crate_root);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let text = fs::read_to_string(&path).unwrap();
            for line in text.lines() {
                if let Some(include) = line.trim().strip_prefix("#[path = \"") {
                    let include = include.split('"').next().unwrap();
                    let target = path.parent().unwrap().join(include).canonicalize().unwrap();
                    assert!(
                        target.starts_with(crate_root),
                        "{} imports {}",
                        path.display(),
                        target.display()
                    );
                }
            }
        }
    }
}

#[test]
fn activity_library_is_dev_only_and_source_includes_stay_inside_the_crate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap();
    let manifest = fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let mut section = "";
    let mut activity_entries = 0;
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            section = line;
        }
        if line.starts_with("tokeira-chasm-activity") {
            activity_entries += 1;
            assert_eq!(section, "[dev-dependencies]");
            assert!(!line.contains("features"));
        }
    }
    assert_eq!(activity_entries, 1);
    for directory in ["src", "tests"] {
        check_sources(&root.join(directory), &root);
    }
}
