//! Transactional Unified Version rewriting and release command construction.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use super::{ReleaseConfig, ReleaseError, ReleasePlan};

/// Fully validated source mutation set, ready for one atomic export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRelease {
    /// Complete replacement contents keyed by workspace-relative path.
    pub files: BTreeMap<PathBuf, String>,
    /// Exact pinned changie batch arguments.
    pub changie_batch_arguments: Vec<String>,
    /// Exact pinned changie merge arguments.
    pub changie_merge_arguments: Vec<String>,
    /// Release commit message including the Train Identity trailer.
    pub commit_message: String,
    /// Annotated tag message carrying the same Train Identity.
    pub tag_message: String,
}

/// Isolated source seam; a production implementation is backed by one Dagger directory.
pub trait ReleaseSource {
    /// Read exact UTF-8 bytes from an isolated source snapshot.
    fn read_text(&self, path: &Path) -> Result<String, ReleaseError>;
    /// Run pinned changie batch and merge in isolation, returning all resulting replacements.
    fn batch_changelog(
        &self,
        target_version: &str,
    ) -> Result<BTreeMap<PathBuf, String>, ReleaseError>;
    /// Atomically export a completely validated replacement set.
    fn export(&self, files: &BTreeMap<PathBuf, String>) -> Result<(), ReleaseError>;
}

/// Prepare all release-owned source bytes, exporting only after every check succeeds.
pub fn prepare_release_source(
    plan: &ReleasePlan,
    source: &dyn ReleaseSource,
) -> Result<PreparedRelease, ReleaseError> {
    plan.validate_digest()?;
    let current_version = plan
        .packages
        .first()
        .map(|package| package.from_version.as_str())
        .ok_or_else(|| ReleaseError::Plan {
            reason: "release Plan has no publishable packages".to_owned(),
        })?;
    let internal = plan
        .packages
        .iter()
        .map(|package| package.name.clone())
        .collect::<BTreeSet<_>>();
    let mut files = BTreeMap::new();
    let root_manifest = PathBuf::from("Cargo.toml");
    let root_text = source.read_text(&root_manifest)?;
    let rewritten_root = rewrite_manifest(
        &root_text,
        current_version,
        &plan.target_version,
        &internal,
        true,
    )?;
    files.insert(root_manifest, rewritten_root);
    for package in &plan.packages {
        if files.contains_key(&package.manifest_path) {
            continue;
        }
        let text = source.read_text(&package.manifest_path)?;
        let rewritten = rewrite_manifest(
            &text,
            &package.from_version,
            &plan.target_version,
            &internal,
            true,
        )?;
        files.insert(package.manifest_path.clone(), rewritten);
    }
    let config = ReleaseConfig::load(&plan.workspace_root, &plan.repository)?;
    for field in &config.extra_version_fields {
        let input = match files.get(&field.path) {
            Some(rewritten) => rewritten.clone(),
            None => source.read_text(&field.path)?,
        };
        let rewritten =
            rewrite_extra_version_field(&input, &field.key, current_version, &plan.target_version)?;
        files.insert(field.path.clone(), rewritten);
    }

    let changelog_files = source.batch_changelog(&plan.target_version)?;
    for fragment in &plan.fragments {
        if changelog_files.contains_key(&fragment.path) {
            return Err(ReleaseError::Changelog {
                path: fragment.path.clone(),
                reason: "batched fragments must be consumed, not replaced".to_owned(),
            });
        }
    }
    let version_file = PathBuf::from(format!(".changes/{}.md", plan.target_version));
    for required in [Path::new("CHANGELOG.md"), version_file.as_path()] {
        if !changelog_files.contains_key(required) {
            return Err(ReleaseError::Changelog {
                path: required.to_path_buf(),
                reason: "pinned changie preparation omitted a required output".to_owned(),
            });
        }
    }
    if let Some(path) = changelog_files
        .keys()
        .find(|path| path.as_path() != Path::new("CHANGELOG.md") && **path != version_file)
    {
        return Err(ReleaseError::Changelog {
            path: path.clone(),
            reason: "pinned changie preparation changed an unowned path".to_owned(),
        });
    }
    files.extend(changelog_files);
    let prepared = PreparedRelease {
        files,
        changie_batch_arguments: vec![
            "batch".to_owned(),
            plan.target_version.clone(),
            "--allow-no-changes=false".to_owned(),
        ],
        changie_merge_arguments: vec!["merge".to_owned()],
        commit_message: format!(
            "release: prepare {}\n\nRelease-Plan-Digest: sha256:{}\n",
            plan.tag, plan.digest
        ),
        tag_message: format!(
            "{}\n\nRelease-Plan-Digest: sha256:{}\n",
            plan.tag, plan.digest
        ),
    };
    source.export(&prepared.files)?;
    Ok(prepared)
}

/// Rewrite release-owned TOML scalar values without reordering or changing membership.
pub fn rewrite_manifest(
    input: &str,
    current_version: &str,
    target_version: &str,
    internal_packages: &BTreeSet<String>,
    rewrite_package_version: bool,
) -> Result<String, ReleaseError> {
    let _: toml::Value = toml::from_str(input).map_err(|source| ReleaseError::Plan {
        reason: format!("manifest is not valid TOML before rewrite: {source}"),
    })?;
    let before_membership = dependency_membership(input)?;
    let mut section = String::new();
    let mut output = String::with_capacity(input.len());
    let mut replacements = 0_usize;
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
        let rewritten = if rewrite_package_version
            && matches!(section.as_str(), "package" | "workspace.package")
            && key == Some("version")
        {
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

/// Exact one-invocation Cargo package arguments for a publishable closure.
pub fn cargo_package_arguments(plan: &ReleasePlan) -> Vec<String> {
    package_arguments_for_names(plan.packages.iter().map(|package| package.name.as_str()))
}

fn package_arguments_for_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
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
    use std::sync::Mutex;

    use proptest::prelude::*;

    use super::*;
    use crate::pipelines::release::{
        ChangieIdentity, PackagePlan, PlannedRegistryState, RELEASE_SCHEMA_VERSION, ReleaseEffect,
        ReleaseEffectKind, RepositoryIdentity, ToolchainIdentity,
    };

    struct RecordingSource {
        files: BTreeMap<PathBuf, String>,
        changelog: BTreeMap<PathBuf, String>,
        exports: Mutex<Vec<BTreeMap<PathBuf, String>>>,
    }

    impl ReleaseSource for RecordingSource {
        fn read_text(&self, path: &Path) -> Result<String, ReleaseError> {
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| ReleaseError::Plan {
                    reason: format!("fixture omitted {}", path.display()),
                })
        }

        fn batch_changelog(
            &self,
            _target_version: &str,
        ) -> Result<BTreeMap<PathBuf, String>, ReleaseError> {
            Ok(self.changelog.clone())
        }

        fn export(&self, files: &BTreeMap<PathBuf, String>) -> Result<(), ReleaseError> {
            self.exports
                .lock()
                .expect("export recorder lock")
                .push(files.clone());
            Ok(())
        }
    }

    fn preparation_plan(root: &Path) -> ReleasePlan {
        let mut plan = ReleasePlan {
            schema_version: RELEASE_SCHEMA_VERSION,
            repository: RepositoryIdentity {
                slug: "tokeira/tokeira".to_owned(),
                remote: "https://github.com/tokeira/tokeira".to_owned(),
            },
            workspace_root: root.to_path_buf(),
            base_commit: "a".repeat(40),
            target_version: "0.2.0".to_owned(),
            tag: "v0.2.0".to_owned(),
            packages: vec![PackagePlan {
                name: "fixture".to_owned(),
                manifest_path: PathBuf::from("Cargo.toml"),
                from_version: "0.1.1".to_owned(),
                target_version: "0.2.0".to_owned(),
                publishable_dependencies: Vec::new(),
                registry: PlannedRegistryState::Absent,
            }],
            fragments: Vec::new(),
            changelog_config_sha256: "b".repeat(64),
            changie_release: ChangieIdentity {
                version: "1.25.2".to_owned(),
                source_revision: "c".repeat(40),
                platform: "linux-x86_64".to_owned(),
                asset: "changie.tar.gz".to_owned(),
                asset_sha256: "d".repeat(64),
            },
            toolchain: ToolchainIdentity {
                rust: "1.97.1".to_owned(),
                dagger: "v1".to_owned(),
            },
            release_notes_sha256: "e".repeat(64),
            effects: vec![ReleaseEffect {
                kind: ReleaseEffectKind::Source,
                summary: "prepare source".to_owned(),
            }],
            digest: String::new(),
        };
        plan.seal().expect("fixture Plan seals");
        plan
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 4: Unified Version rewrite preserves dependency membership
        #[test]
        fn rewrite_changes_only_owned_versions(
            target_minor in 2_u16..500,
            feature in "[a-z]{1,12}",
            unrelated in "[a-z]{1,12}",
        ) {
            let input = format!(
                "[package]\nname = \"example\"\nversion = \"0.1.1\"\n\n[dependencies]\ninternal = {{ path = \"../internal\", version = \"0.1.1\", features = [\"{feature}\"] }}\n{unrelated} = \"1\"\n\n[dev-dependencies]\ninternal = {{ path = \"../internal\", version = \"0.1.1\" }}\n"
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

            let target_present = rewritten.contains(&format!("version = \"{}\"", target));
            let feature_present = rewritten.contains(&format!("features = [\"{}\"]", feature));
            let unrelated_present = rewritten.contains(&format!("{} = \"1\"", unrelated));
            let dev_unchanged = rewritten.contains(
                "[dev-dependencies]\ninternal = { path = \"../internal\", version = \"0.1.1\" }"
            );
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
            let arguments = package_arguments_for_names(ordered.iter().copied());
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
    fn preparation_exports_only_after_the_complete_changie_diff_is_admitted() {
        let workspace = tempfile::tempdir().expect("fixture workspace");
        std::fs::write(
            workspace.path().join(".tokeira-release.toml"),
            "schema_version = 1\nrelease_branch = \"main\"\n",
        )
        .expect("release config");
        let manifest = "[package]\nname = \"fixture\"\nversion = \"0.1.1\"\n";
        let required = BTreeMap::from([
            (PathBuf::from("CHANGELOG.md"), "# Changelog\n".to_owned()),
            (PathBuf::from(".changes/0.2.0.md"), "## 0.2.0\n".to_owned()),
        ]);
        let source = RecordingSource {
            files: BTreeMap::from([(PathBuf::from("Cargo.toml"), manifest.to_owned())]),
            changelog: required.clone(),
            exports: Mutex::new(Vec::new()),
        };
        let plan = preparation_plan(workspace.path());
        let prepared = prepare_release_source(&plan, &source).expect("complete preparation");
        assert_eq!(source.exports.lock().expect("exports").len(), 1);
        assert_eq!(prepared.files[Path::new("CHANGELOG.md")], "# Changelog\n");

        let mut unexpected = required;
        unexpected.insert(PathBuf::from("README.md"), "changed\n".to_owned());
        let refused = RecordingSource {
            files: BTreeMap::from([(PathBuf::from("Cargo.toml"), manifest.to_owned())]),
            changelog: unexpected,
            exports: Mutex::new(Vec::new()),
        };
        assert!(prepare_release_source(&plan, &refused).is_err());
        assert!(refused.exports.lock().expect("exports").is_empty());
    }
}
