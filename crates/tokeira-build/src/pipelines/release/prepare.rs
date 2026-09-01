//! Unified Version rewriting and the exact command shapes the executor runs.
//!
//! The rewrite is textual on purpose: a TOML round-trip would reorder tables and
//! normalize formatting, and the Release Commit must touch only the version scalars
//! it owns. Every rewrite is re-parsed afterwards and its dependency membership is
//! compared with the input, so a slipped edit can never reach the tag.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use super::{ExtraVersionField, ReleaseError, ReleasePlan};

/// Rewrite every release-owned manifest and extra version field of a workspace.
///
/// `manifests` lists each publishable package with its workspace-relative manifest
/// path; the root manifest is always rewritten as well. One function serves both the
/// read-only planning build and the apply-time preparation, so the bytes the operator
/// confirmed are the bytes that get tagged.
pub fn rewrite_workspace_manifests(
    workspace_root: &Path,
    manifests: &[(String, PathBuf)],
    internal_packages: &BTreeSet<String>,
    current_version: &str,
    target_version: &str,
    extra_version_fields: &[ExtraVersionField],
) -> Result<BTreeMap<PathBuf, String>, ReleaseError> {
    let read = |relative: &Path| -> Result<String, ReleaseError> {
        let path = workspace_root.join(relative);
        std::fs::read_to_string(&path).map_err(|source| ReleaseError::Workspace {
            reason: format!("could not read {}: {source}", path.display()),
        })
    };
    let root = PathBuf::from("Cargo.toml");
    let mut rewritten = BTreeMap::new();
    rewritten.insert(
        root.clone(),
        rewrite_manifest(
            &read(&root)?,
            current_version,
            target_version,
            internal_packages,
            true,
        )?,
    );
    for (name, manifest_path) in manifests {
        if rewritten.contains_key(manifest_path) {
            continue;
        }
        let text = read(manifest_path).map_err(|source| ReleaseError::Workspace {
            reason: format!("manifest for {name}: {source}"),
        })?;
        rewritten.insert(
            manifest_path.clone(),
            rewrite_manifest(
                &text,
                current_version,
                target_version,
                internal_packages,
                true,
            )?,
        );
    }
    for field in extra_version_fields {
        let input = match rewritten.get(&field.path) {
            Some(text) => text.clone(),
            None => read(&field.path)?,
        };
        rewritten.insert(
            field.path.clone(),
            rewrite_extra_version_field(&input, &field.key, current_version, target_version)?,
        );
    }
    Ok(rewritten)
}

/// Rewrite release-owned TOML scalar values without reordering or changing membership.
pub fn rewrite_manifest(
    input: &str,
    current_version: &str,
    target_version: &str,
    internal_packages: &BTreeSet<String>,
    rewrite_package_version: bool,
) -> Result<String, ReleaseError> {
    let parsed: toml::Value = toml::from_str(input).map_err(|source| ReleaseError::Plan {
        reason: format!("manifest is not valid TOML before rewrite: {source}"),
    })?;
    let before_membership = dependency_membership(input)?;
    // A dependency written as its own table (`[dependencies.tokeira-kernel]`) carries
    // `version` on a line of its own, and an alias (`package = "..."`) may follow it.
    // The internal-package decision is therefore taken from the parsed tree before
    // the line walk; the walk only needs to know which section it is standing in.
    let internal_subtables = internal_dependency_subtables(&parsed, internal_packages);
    let mut section = String::new();
    let mut output = String::with_capacity(input.len());
    let mut replacements = 0_usize;
    for raw_line in input.split_inclusive('\n') {
        let line = raw_line.strip_suffix('\n').unwrap_or(raw_line);
        let newline = if raw_line.ends_with('\n') { "\n" } else { "" };
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            section = normalize_section(trimmed.trim_matches(&['[', ']'][..]));
            output.push_str(line);
            output.push_str(newline);
            continue;
        }

        let key = assignment_key(trimmed);
        let rewritten = if rewrite_package_version
            && matches!(section.as_str(), "package" | "workspace.package")
            && key == Some("version")
        {
            replace_quoted_assignment(line, current_version, target_version).inspect(|_| {
                replacements += 1;
            })?
        } else if internal_subtables.contains(&section) && key == Some("version") {
            replace_quoted_assignment(line, current_version, target_version).inspect(|_| {
                replacements += 1;
            })?
        } else if dependency_section(&section)
            && key.is_some_and(|key| dependency_targets_internal(line, key, internal_packages))
        {
            let line = rewrite_dependency(line, current_version, target_version)?;
            replacements += usize::from(line != raw_line.trim_end_matches('\n'));
            line
        } else {
            line.to_owned()
        };
        output.push_str(&rewritten);
        output.push_str(newline);
    }
    if replacements == 0 && rewrite_package_version {
        // Workspace-inherited member manifests legitimately delegate both package and
        // dependency versions to the root manifest.
        let delegates_package_version = input
            .lines()
            .any(|line| line.trim() == "version.workspace = true");
        if !delegates_package_version {
            return Err(ReleaseError::Plan {
                reason: "manifest contains no release-owned version scalar".to_owned(),
            });
        }
    }
    let _: toml::Value = toml::from_str(&output).map_err(|source| ReleaseError::Plan {
        reason: format!("manifest is not valid TOML after rewrite: {source}"),
    })?;
    if dependency_membership(&output)? != before_membership {
        return Err(ReleaseError::Plan {
            reason: "version rewrite changed dependency membership".to_owned(),
        });
    }
    Ok(output)
}

fn dependency_targets_internal(
    line: &str,
    key: &str,
    internal_packages: &BTreeSet<String>,
) -> bool {
    if internal_packages.contains(key) {
        return true;
    }
    let Ok(value) = toml::from_str::<toml::Value>(&format!("[dependencies]\n{line}\n")) else {
        return false;
    };
    value
        .get("dependencies")
        .and_then(|dependencies| dependencies.get(key))
        .and_then(|dependency| dependency.get("package"))
        .and_then(toml::Value::as_str)
        .is_some_and(|package| internal_packages.contains(package))
}

/// Section paths of dependency sub-tables that resolve to an internal package.
///
/// Every publish-relevant dependency table (`dependency_section`) is walked; a child
/// that is itself a table is a sub-table dependency whose package is the `package`
/// alias when present and the key otherwise. Dev-dependency tables are excluded by
/// `dependency_section`, so their sub-tables keep their pins.
fn internal_dependency_subtables(
    value: &toml::Value,
    internal_packages: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut sections = BTreeSet::new();
    collect_internal_subtables(String::new(), value, internal_packages, &mut sections);
    sections
}

fn collect_internal_subtables(
    path: String,
    value: &toml::Value,
    internal_packages: &BTreeSet<String>,
    sections: &mut BTreeSet<String>,
) {
    let Some(table) = value.as_table() else {
        return;
    };
    for (key, child) in table {
        let child_path = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        if dependency_section(&path) && child.is_table() {
            let resolved = child
                .get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(key);
            if internal_packages.contains(resolved) {
                sections.insert(normalize_section(&child_path));
            }
        }
        collect_internal_subtables(child_path, child, internal_packages, sections);
    }
}

/// Reduce a raw table header to the dotted key path the parsed tree reports.
///
/// Headers may quote components (`[target.'cfg(unix)'.dependencies]`), while the
/// parsed tree stores the unquoted key; dropping the quote characters on both sides
/// makes the two comparable without a second TOML parse per line.
fn normalize_section(raw: &str) -> String {
    raw.chars()
        .filter(|character| !matches!(character, '"' | '\''))
        .collect::<String>()
        .split('.')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(".")
}

/// Rewrite one repository-owned TOML string scalar selected by an exact key path.
pub fn rewrite_extra_version_field(
    input: &str,
    key_path: &[String],
    current_version: &str,
    target_version: &str,
) -> Result<String, ReleaseError> {
    if key_path.is_empty() || key_path.iter().any(String::is_empty) {
        return Err(ReleaseError::Plan {
            reason: "extra version field key path must not be empty".to_owned(),
        });
    }
    let before: toml::Value = toml::from_str(input).map_err(|source| ReleaseError::Plan {
        reason: format!("extra version file is not valid TOML before rewrite: {source}"),
    })?;
    let observed = toml_string_at(&before, key_path).ok_or_else(|| ReleaseError::Plan {
        reason: format!("extra version field {:?} is not a string scalar", key_path),
    })?;
    if observed != current_version {
        return Err(ReleaseError::Plan {
            reason: format!(
                "extra version field {:?} expected {current_version}, observed {observed}",
                key_path
            ),
        });
    }

    let (leaf, parents) = key_path
        .split_last()
        .expect("non-empty key path checked above");
    let parent = parents.join(".");
    let dotted = key_path.join(".");
    let mut section = String::new();
    let mut replacements = 0_usize;
    let mut output = String::with_capacity(input.len());
    for raw_line in input.split_inclusive('\n') {
        let line = raw_line.strip_suffix('\n').unwrap_or(raw_line);
        let newline = if raw_line.ends_with('\n') { "\n" } else { "" };
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            section = trimmed.trim_matches(&['[', ']'][..]).to_owned();
            output.push_str(line);
            output.push_str(newline);
            continue;
        }
        let key = assignment_key(trimmed);
        let selected = (section == parent && key == Some(leaf.as_str()))
            || (section.is_empty() && key == Some(dotted.as_str()));
        if selected {
            output.push_str(&replace_quoted_assignment(
                line,
                current_version,
                target_version,
            )?);
            replacements += 1;
        } else {
            output.push_str(line);
        }
        output.push_str(newline);
    }
    if replacements != 1 {
        return Err(ReleaseError::Plan {
            reason: format!(
                "extra version field {:?} must match exactly once, observed {replacements}",
                key_path
            ),
        });
    }
    let after: toml::Value = toml::from_str(&output).map_err(|source| ReleaseError::Plan {
        reason: format!("extra version file is not valid TOML after rewrite: {source}"),
    })?;
    if toml_string_at(&after, key_path) != Some(target_version) {
        return Err(ReleaseError::Plan {
            reason: format!("extra version field {:?} did not reach target", key_path),
        });
    }
    Ok(output)
}

fn toml_string_at<'a>(value: &'a toml::Value, key_path: &[String]) -> Option<&'a str> {
    let value = key_path
        .iter()
        .try_fold(value, |value, key| value.get(key))?;
    value.as_str()
}

fn dependency_section(section: &str) -> bool {
    (section == "dependencies"
        || section == "build-dependencies"
        || section == "workspace.dependencies"
        || section.ends_with(".dependencies")
        || section.ends_with(".build-dependencies"))
        && !section.ends_with("dev-dependencies")
}

fn assignment_key(line: &str) -> Option<&str> {
    let (key, _) = line.split_once('=')?;
    Some(key.trim().trim_matches('"'))
}

fn replace_quoted_assignment(
    line: &str,
    current_version: &str,
    target_version: &str,
) -> Result<String, ReleaseError> {
    let first_quote = line.find(['"', '\'']).ok_or_else(|| ReleaseError::Plan {
        reason: format!("version scalar is not a quoted string: {line}"),
    })?;
    let quote = line.as_bytes()[first_quote] as char;
    let rest = &line[first_quote + 1..];
    let second_quote = rest.find(quote).ok_or_else(|| ReleaseError::Plan {
        reason: format!("version scalar is not terminated: {line}"),
    })? + first_quote
        + 1;
    let observed = &line[first_quote + 1..second_quote];
    if observed != current_version {
        return Err(ReleaseError::Plan {
            reason: format!("version scalar expected {current_version}, observed {observed}"),
        });
    }
    Ok(format!(
        "{}{}{}",
        &line[..first_quote + 1],
        target_version,
        &line[second_quote..]
    ))
}

fn rewrite_dependency(
    line: &str,
    current_version: &str,
    target_version: &str,
) -> Result<String, ReleaseError> {
    let (_, value) = line.split_once('=').ok_or_else(|| ReleaseError::Plan {
        reason: format!("dependency is not an assignment: {line}"),
    })?;
    let value = value.trim();
    if value
        .chars()
        .next()
        .is_some_and(|character| matches!(character, '"' | '\''))
    {
        return replace_quoted_assignment(line, current_version, target_version);
    }
    if value.contains("workspace = true") {
        return Ok(line.to_owned());
    }
    let version =
        inline_key_offset(value, "version").ok_or_else(|| ReleaseError::PackageDryRun {
            reason: format!("internal publish dependency has no registry version: {line}"),
        })?;
    let prefix = &line[..line.len() - value.len() + version];
    let suffix = &line[prefix.len()..];
    let rewritten = replace_quoted_assignment(suffix, current_version, target_version)?;
    Ok(format!("{prefix}{rewritten}"))
}

fn inline_key_offset(value: &str, selected: &str) -> Option<usize> {
    let mut in_string = false;
    let mut escaped = false;
    for (offset, character) in value.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            continue;
        }
        if character == '"' {
            in_string = true;
            continue;
        }
        let rest = &value[offset..];
        if !rest.starts_with(selected) {
            continue;
        }
        let before = value[..offset].trim_end().chars().last();
        let after = rest[selected.len()..].trim_start();
        if matches!(before, Some('{') | Some(',')) && after.starts_with('=') {
            return Some(offset);
        }
    }
    None
}

fn dependency_membership(input: &str) -> Result<BTreeMap<String, Vec<String>>, ReleaseError> {
    let value: toml::Value = toml::from_str(input).map_err(|source| ReleaseError::Plan {
        reason: format!("manifest is not valid TOML: {source}"),
    })?;
    let mut membership = BTreeMap::new();
    collect_dependency_tables(String::new(), &value, &mut membership);
    Ok(membership)
}

fn collect_dependency_tables(
    path: String,
    value: &toml::Value,
    membership: &mut BTreeMap<String, Vec<String>>,
) {
    let Some(table) = value.as_table() else {
        return;
    };
    if dependency_section(&path) {
        membership.insert(path.clone(), table.keys().cloned().collect());
    }
    for (key, child) in table {
        let child_path = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        collect_dependency_tables(child_path, child, membership);
    }
}

/// Exact one-invocation Cargo package arguments for a Plan's publishable closure.
pub fn cargo_package_arguments(plan: &ReleasePlan) -> Vec<String> {
    cargo_package_arguments_for_names(plan.packages.iter().map(|package| package.name.as_str()))
}

/// Exact one-invocation Cargo package arguments for an ordered set of package names.
///
/// One invocation matters: per-crate packaging resolves sibling versions against the
/// registry and fails until they are published, while a single multi-package call
/// resolves them against the workspace.
pub fn cargo_package_arguments_for_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let mut arguments = vec![
        "package".to_owned(),
        "--locked".to_owned(),
        "--allow-dirty".to_owned(),
    ];
    for name in names {
        arguments.push("--package".to_owned());
        arguments.push(name.to_owned());
    }
    arguments
}

/// Exact all-or-nothing branch and annotated-tag publication arguments.
pub fn atomic_git_push_arguments(remote: &str, branch: &str, tag: &str) -> Vec<String> {
    vec![
        "push".to_owned(),
        "--atomic".to_owned(),
        remote.to_owned(),
        format!("HEAD:refs/heads/{branch}"),
        format!("refs/tags/{tag}:refs/tags/{tag}"),
    ]
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 4: Unified Version rewrite preserves dependency membership
        #[test]
        fn rewrite_changes_only_owned_versions(
            target_minor in 2_u16..500,
            feature in "[a-z]{1,12}",
            unrelated in "x[a-z]{1,11}",
            subtable in any::<bool>(),
        ) {
            // The internal dependency is declared either inline or as its own table;
            // both forms are release-owned and must move with the train.
            let dependencies = if subtable {
                format!(
                    "[dependencies]\n{unrelated} = \"1\"\n\n[dependencies.internal]\npath = \"../internal\"\nversion = \"0.1.1\"\nfeatures = [\"{feature}\"]\n"
                )
            } else {
                format!(
                    "[dependencies]\n{unrelated} = \"1\"\ninternal = {{ path = \"../internal\", version = \"0.1.1\", features = [\"{feature}\"] }}\n"
                )
            };
            let input = format!(
                "[package]\nname = \"example\"\nversion = \"0.1.1\"\n\n{dependencies}\n[dev-dependencies]\ninternal = {{ path = \"../internal\", version = \"0.1.1\" }}\n"
            );
            let target = format!("0.{target_minor}.0");
            let rewritten = rewrite_manifest(
                &input,
                "0.1.1",
                &target,
                &BTreeSet::from(["internal".to_owned()]),
                true,
            )
            .expect("valid rewrite");

            let stale_internal_pins = rewritten
                .split("[dev-dependencies]")
                .next()
                .expect("manifest keeps its dev-dependency table")
                .matches("version = \"0.1.1\"")
                .count();
            let feature_present = rewritten.contains(&format!("features = [\"{}\"]", feature));
            let unrelated_present = rewritten.contains(&format!("{} = \"1\"", unrelated));
            let dev_unchanged = rewritten.contains(
                "[dev-dependencies]\ninternal = { path = \"../internal\", version = \"0.1.1\" }"
            );
            let target_present = rewritten.contains(&format!("version = \"{target}\""));
            prop_assert_eq!(stale_internal_pins, 0);
            prop_assert!(target_present);
            prop_assert!(feature_present);
            prop_assert!(unrelated_present);
            prop_assert!(dev_unchanged);
            prop_assert_eq!(dependency_membership(&input)?, dependency_membership(&rewritten)?);
        }

        // Feature: release-engineering, Property 4: Unified Version rewrite preserves dependency membership
        #[test]
        fn extra_field_rewrite_changes_only_the_exact_scalar(
            target_minor in 2_u16..500,
            unrelated in "[a-z]{1,12}",
        ) {
            let input = format!(
                "[release]\nversion = \"0.1.1\"\nchannel = \"{unrelated}\"\n\n[other]\nversion = \"0.1.1\"\n"
            );
            let target = format!("0.{target_minor}.0");
            let rewritten = rewrite_extra_version_field(
                &input,
                &["release".to_owned(), "version".to_owned()],
                "0.1.1",
                &target,
            )?;
            let expected_version = format!("[release]\nversion = \"{target}\"");
            let expected_channel = format!("channel = \"{unrelated}\"");
            prop_assert!(rewritten.contains(&expected_version));
            prop_assert!(rewritten.contains(&expected_channel));
            prop_assert!(rewritten.contains("[other]\nversion = \"0.1.1\""));
        }

        // Feature: release-engineering, Property 7: packaging gate covers the publishable closure
        #[test]
        fn one_package_invocation_selects_the_complete_ordered_closure(
            names in proptest::collection::btree_set("[a-z][a-z0-9-]{0,12}", 1..30),
            path_only in any::<bool>(),
        ) {
            let ordered = names.iter().map(String::as_str).collect::<Vec<_>>();
            let arguments = cargo_package_arguments_for_names(ordered.iter().copied());
            let selected = arguments
                .windows(2)
                .filter(|pair| pair[0] == "--package")
                .map(|pair| pair[1].as_str())
                .collect::<Vec<_>>();
            prop_assert_eq!(selected, ordered);
            prop_assert_eq!(arguments.iter().filter(|arg| *arg == "package").count(), 1);

            let dependency = if path_only {
                "internal = { path = \"../internal\" }"
            } else {
                "internal = { path = \"../internal\", version = \"0.1.1\" }"
            };
            let manifest = format!(
                "[package]\nname = \"fixture\"\nversion = \"0.1.1\"\n\n[dependencies]\n{dependency}\n"
            );
            let admitted = rewrite_manifest(
                &manifest,
                "0.1.1",
                "0.2.0",
                &BTreeSet::from(["internal".to_owned()]),
                true,
            )
            .is_ok();
            prop_assert_eq!(admitted, !path_only);
        }
    }

    #[test]
    fn release_commands_are_exact_and_single_invocation() {
        let arguments = atomic_git_push_arguments("origin", "main", "v1.2.3");
        assert_eq!(
            arguments,
            [
                "push",
                "--atomic",
                "origin",
                "HEAD:refs/heads/main",
                "refs/tags/v1.2.3:refs/tags/v1.2.3",
            ]
        );
    }

    #[test]
    fn subtable_internal_dependencies_move_with_the_train() {
        // The shape `crates/tokeira-edge/Cargo.toml` uses for every internal edge, plus
        // an aliased sub-table, a target-specific sub-table, and a dev sub-table that
        // must keep its pin.
        let input = "[package]\nname = \"consumer\"\nversion = \"0.1.1\"\n\n[dependencies]\nserde = \"1\"\n\n[dependencies.internal]\npath = \"../internal\"\nversion = \"0.1.1\"\n\n[dependencies.alias]\nversion = \"0.1.1\"\npath = \"../internal\"\npackage = \"internal\"\n\n[target.'cfg(unix)'.dependencies.internal]\npath = \"../internal\"\nversion = \"0.1.1\"\n\n[dev-dependencies.internal]\npath = \"../internal\"\nversion = \"0.1.1\"\n";
        let rewritten = rewrite_manifest(
            input,
            "0.1.1",
            "0.2.0",
            &BTreeSet::from(["internal".to_owned()]),
            true,
        )
        .expect("sub-table internal dependencies are release-owned");
        let (release_owned, dev) = rewritten
            .split_once("[dev-dependencies.internal]")
            .expect("dev sub-table survives");
        assert_eq!(release_owned.matches("version = \"0.2.0\"").count(), 4);
        assert_eq!(release_owned.matches("version = \"0.1.1\"").count(), 0);
        assert!(dev.contains("version = \"0.1.1\""));
        assert!(rewritten.contains("serde = \"1\""));
        assert_eq!(
            dependency_membership(input).expect("input membership"),
            dependency_membership(&rewritten).expect("rewritten membership")
        );
    }

    #[test]
    fn aliased_internal_dependency_moves_with_the_train() {
        let input = "[package]\nname = \"consumer\"\nversion = \"0.1.1\"\n\n[dependencies]\nalias = { package = \"internal\", path = \"../internal\", version = \"0.1.1\" }\n";
        let rewritten = rewrite_manifest(
            input,
            "0.1.1",
            "0.2.0",
            &BTreeSet::from(["internal".to_owned()]),
            true,
        )
        .expect("aliased internal dependency is release-owned");
        assert!(rewritten.contains("version = \"0.2.0\" }"));
        assert_eq!(
            dependency_membership(input).expect("input membership"),
            dependency_membership(&rewritten).expect("rewritten membership")
        );
    }

    #[test]
    fn workspace_rewrite_covers_root_members_and_extra_fields() {
        let workspace = tempfile::tempdir().expect("fixture workspace");
        let root = workspace.path();
        std::fs::create_dir_all(root.join("crates/member")).expect("member dir");
        std::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/member\"]\n\n[workspace.package]\nversion = \"0.1.1\"\n\n[workspace.dependencies]\nmember = { path = \"crates/member\", version = \"0.1.1\" }\n",
        )
        .expect("root manifest");
        std::fs::write(
            root.join("crates/member/Cargo.toml"),
            "[package]\nname = \"member\"\nversion.workspace = true\n\n[dependencies]\nserde = \"1\"\n",
        )
        .expect("member manifest");
        std::fs::write(
            root.join("release.toml"),
            "[release]\nversion = \"0.1.1\"\n",
        )
        .expect("extra field file");
        let rewritten = rewrite_workspace_manifests(
            root,
            &[(
                "member".to_owned(),
                PathBuf::from("crates/member/Cargo.toml"),
            )],
            &BTreeSet::from(["member".to_owned()]),
            "0.1.1",
            "0.2.0",
            &[ExtraVersionField {
                path: PathBuf::from("release.toml"),
                key: vec!["release".to_owned(), "version".to_owned()],
            }],
        )
        .expect("complete rewrite");
        assert_eq!(rewritten.len(), 3);
        assert!(
            rewritten[Path::new("Cargo.toml")]
                .contains("version = \"0.2.0\"\n\n[workspace.dependencies]")
        );
        assert!(rewritten[Path::new("Cargo.toml")].contains("version = \"0.2.0\" }"));
        assert!(
            rewritten[Path::new("crates/member/Cargo.toml")].contains("version.workspace = true")
        );
        assert!(rewritten[Path::new("release.toml")].contains("version = \"0.2.0\""));
    }
}
