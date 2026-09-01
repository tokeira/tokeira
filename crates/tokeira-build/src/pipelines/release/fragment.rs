//! Shared changie configuration identity and fragment admission.

use std::path::{Path, PathBuf};

use super::{FragmentIdentity, ReleaseError, sha256_hex};

const CONFIG_PATH: &str = ".changie.yaml";
const HEADER_PATH: &str = ".changes/header.tpl.md";
#[cfg(test)]
const CANONICAL_CONFIG: &[u8] = include_bytes!("../../../../../.changie.yaml");
#[cfg(test)]
const CANONICAL_HEADER: &[u8] = include_bytes!("../../../../../.changes/header.tpl.md");
/// Canonical digest of `.changie.yaml` and the shared header template.
pub const CANONICAL_CHANGELOG_CONFIG_SHA256: &str =
    "f96827bf41065762e5a87a3185473e02e20b77cb4560dee7d980ca3d67218259";
const KIND_ORDER: [(&str, &str); 7] = [
    ("added", "Added"),
    ("changed", "Changed"),
    ("deprecated", "Deprecated"),
    ("removed", "Removed"),
    ("fixed", "Fixed"),
    ("security", "Security"),
    ("internal", "Internal"),
];

/// Parsed fields required from one pinned changie fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedFragment {
    /// Workspace-relative path.
    pub path: PathBuf,
    /// Configured lowercase kind key.
    pub kind: String,
    /// Body stored by changie; internal fragments intentionally have none.
    pub body: Option<String>,
    /// Lowercase UUID version 4 collision-resistant slice identity.
    pub slice: String,
    /// Digest of exact fragment bytes.
    pub sha256: String,
}

/// Digest the two canonical config files in sorted relative-path order.
pub fn canonical_changelog_config_sha256() -> String {
    CANONICAL_CHANGELOG_CONFIG_SHA256.to_owned()
}

/// Admit a selected workspace only when its config bytes match the embedded contract.
pub fn admit_changelog_config(workspace_root: &Path) -> Result<String, ReleaseError> {
    let config = read(workspace_root, CONFIG_PATH)?;
    let header = read(workspace_root, HEADER_PATH)?;
    let observed = config_set_digest([
        (CONFIG_PATH, config.as_slice()),
        (HEADER_PATH, header.as_slice()),
    ]);
    let expected = canonical_changelog_config_sha256();
    if observed != expected {
        return Err(ReleaseError::ChangelogConfigDrift { expected, observed });
    }
    Ok(observed)
}

fn read(workspace_root: &Path, relative: &str) -> Result<Vec<u8>, ReleaseError> {
    std::fs::read(workspace_root.join(relative)).map_err(|source| ReleaseError::Changelog {
        path: PathBuf::from(relative),
        reason: source.to_string(),
    })
}

fn config_set_digest<const N: usize>(mut files: [(&str, &[u8]); N]) -> String {
    files.sort_by_key(|(path, _)| *path);
    let mut bytes = Vec::new();
    for (path, contents) in files {
        // Length-prefixing makes the identity unambiguous without normalizing any file byte.
        bytes.extend_from_slice(&(path.len() as u64).to_be_bytes());
        bytes.extend_from_slice(path.as_bytes());
        bytes.extend_from_slice(&(contents.len() as u64).to_be_bytes());
        bytes.extend_from_slice(contents);
    }
    sha256_hex(&bytes)
}

/// Read and admit every unreleased fragment in lexical path order.
pub fn admit_fragments(workspace_root: &Path) -> Result<Vec<AdmittedFragment>, ReleaseError> {
    let directory = workspace_root.join(".changes/unreleased");
    let entries = std::fs::read_dir(&directory).map_err(|source| ReleaseError::Changelog {
        path: PathBuf::from(".changes/unreleased"),
        reason: source.to_string(),
    })?;
    let mut paths = entries
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| ReleaseError::Changelog {
                    path: PathBuf::from(".changes/unreleased"),
                    reason: source.to_string(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    paths
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .map(|path| parse_fragment(workspace_root, &path))
        .collect()
}

fn parse_fragment(workspace_root: &Path, path: &Path) -> Result<AdmittedFragment, ReleaseError> {
    let bytes = std::fs::read(path).map_err(|source| ReleaseError::Changelog {
        path: path.to_path_buf(),
        reason: source.to_string(),
    })?;
    let text = std::str::from_utf8(&bytes).map_err(|source| ReleaseError::Changelog {
        path: path.to_path_buf(),
        reason: format!("fragment is not UTF-8: {source}"),
    })?;
    let mut kind = None;
    let mut body = None;
    let mut slice = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(value) = trimmed.strip_prefix("kind:") {
            if kind.is_some() {
                return Err(invalid_fragment(path, "duplicate `kind`"));
            }
            kind = Some(unquote(value.trim()).to_owned());
        } else if let Some(value) = trimmed.strip_prefix("body:") {
            if body.is_some() {
                return Err(invalid_fragment(path, "duplicate `body`"));
            }
            body = Some(unquote(value.trim()).to_owned());
        } else if let Some(value) = trimmed.strip_prefix("Slice:") {
            if slice.is_some() {
                return Err(invalid_fragment(path, "duplicate custom `Slice`"));
            }
            slice = Some(unquote(value.trim()).to_owned());
        }
    }
    let kind = kind.ok_or_else(|| invalid_fragment(path, "missing `kind`"))?;
    if !KIND_ORDER.iter().any(|(key, _)| *key == kind) {
        return Err(invalid_fragment(path, "unknown fragment kind"));
    }
    let body = body.filter(|body| !body.is_empty());
    if kind == "internal" && body.is_some() {
        return Err(invalid_fragment(
            path,
            "internal fragments must omit the skipped body field",
        ));
    }
    if kind != "internal" && !body.as_ref().is_some_and(|body| bounded_sentence(body)) {
        return Err(invalid_fragment(
            path,
            "non-internal body must be one 8 through 180 character user-facing sentence",
        ));
    }
    let slice = slice.ok_or_else(|| invalid_fragment(path, "missing custom `Slice`"))?;
    if !is_lowercase_uuid_v4(&slice) {
        return Err(invalid_fragment(
            path,
            "Slice must be a lowercase UUID version 4",
        ));
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_fragment(path, "fragment filename is not portable UTF-8"))?;
    let expected = fragment_filename(&kind, &slice)?;
    if file_name != expected {
        return Err(invalid_fragment(
            path,
            &format!("filename must be `{expected}`"),
        ));
    }
    let relative = path
        .strip_prefix(workspace_root)
        .map_err(|_| invalid_fragment(path, "fragment is outside the workspace"))?
        .to_path_buf();
    Ok(AdmittedFragment {
        path: relative,
        kind,
        body,
        slice,
        sha256: sha256_hex(&bytes),
    })
}

/// One user-facing sentence: 8 to 180 characters, a single line, ending in a stop.
///
/// Interior stops are allowed on purpose: release prose names versions (`1.31.0`),
/// hosts (`crates.io`), and abbreviations, and a sentence is judged by how it ends,
/// not by how many periods it contains.
fn bounded_sentence(body: &str) -> bool {
    let length = body.chars().count();
    (8..=180).contains(&length)
        && !body
            .chars()
            .any(|character| matches!(character, '\n' | '\r'))
        && body
            .chars()
            .last()
            .is_some_and(|character| matches!(character, '.' | '!' | '?'))
}

fn invalid_fragment(path: &Path, reason: &str) -> ReleaseError {
    ReleaseError::Changelog {
        path: path.to_path_buf(),
        reason: reason.to_owned(),
    }
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
}

/// Return the exact pinned changie filename for a kind and Slice identity.
pub fn fragment_filename(kind: &str, slice: &str) -> Result<String, ReleaseError> {
    if !KIND_ORDER.iter().any(|(key, _)| *key == kind) || !is_lowercase_uuid_v4(slice) {
        return Err(ReleaseError::Changelog {
            path: PathBuf::from(".changes/unreleased"),
            reason: "invalid fragment kind or Slice identity".to_owned(),
        });
    }
    Ok(format!("{kind}-{slice}.yaml"))
}

/// Render the public changelog body in pinned kind order.
pub fn render_version_body(fragments: &[AdmittedFragment]) -> String {
    let mut output = String::new();
    for (key, label) in KIND_ORDER {
        if key == "internal" {
            continue;
        }
        let bodies = fragments
            .iter()
            .filter(|fragment| fragment.kind == key)
            .filter_map(|fragment| fragment.body.as_deref())
            .collect::<Vec<_>>();
        if bodies.is_empty() {
            continue;
        }
        output.push_str("### ");
        output.push_str(label);
        output.push_str("\n\n");
        for body in bodies {
            output.push_str("* ");
            output.push_str(body);
            output.push('\n');
        }
        output.push('\n');
    }
    output
}

impl From<&AdmittedFragment> for FragmentIdentity {
    fn from(fragment: &AdmittedFragment) -> Self {
        Self {
            path: fragment.path.clone(),
            sha256: fragment.sha256.clone(),
        }
    }
}

fn is_lowercase_uuid_v4(value: &str) -> bool {
    if value.len() != 36 || value.as_bytes().get(14) != Some(&b'4') {
        return false;
    }
    value.char_indices().all(|(index, character)| match index {
        8 | 13 | 18 | 23 => character == '-',
        19 => matches!(character, '8' | '9' | 'a' | 'b'),
        _ => character.is_ascii_digit() || matches!(character, 'a'..='f'),
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 5: fragment gate is complete and explicit
        #[test]
        fn distinct_uuid_slices_have_distinct_paths(
            left in any::<u128>(),
            right in any::<u128>().prop_filter("different UUID bits", |right| *right != 0),
        ) {
            let left = format!("{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
                left as u32,
                (left >> 32) as u16,
                ((left >> 48) as u16) & 0x0fff,
                ((left >> 64) as u16) & 0x0fff,
                (left >> 80) & 0x0000_ffff_ffff_ffff,
            );
            let right_bits = left.as_bytes()[0] as u128 ^ right;
            let right = format!("{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
                right_bits as u32,
                (right_bits >> 32) as u16,
                ((right_bits >> 48) as u16) & 0x0fff,
                ((right_bits >> 64) as u16) & 0x0fff,
                (right_bits >> 80) & 0x0000_ffff_ffff_ffff,
            );
            prop_assume!(left != right);
            prop_assert_ne!(
                fragment_filename("added", &left).expect("valid UUID"),
                fragment_filename("added", &right).expect("valid UUID"),
            );
        }

        // Feature: release-engineering, Property 5: fragment gate is complete and explicit
        #[test]
        fn rendered_body_omits_internal_changes_and_enforces_sentence_bounds(
            public_body in "[A-Za-z][A-Za-z ]{6,80}",
            internal_marker in "INTERNAL[0-9]{4}",
        ) {
            let public_body = format!("{public_body}.");
            let fragments = vec![
                AdmittedFragment {
                    path: PathBuf::from(".changes/unreleased/added-a.yaml"),
                    kind: "added".to_owned(),
                    body: Some(public_body.clone()),
                    slice: "00000000-0000-4000-8000-000000000000".to_owned(),
                    sha256: "a".repeat(64),
                },
                AdmittedFragment {
                    path: PathBuf::from(".changes/unreleased/internal-b.yaml"),
                    kind: "internal".to_owned(),
                    body: Some(internal_marker.clone()),
                    slice: "00000000-0000-4000-8000-000000000001".to_owned(),
                    sha256: "b".repeat(64),
                },
            ];
            let rendered = render_version_body(&fragments);
            prop_assert!(bounded_sentence(&public_body));
            prop_assert!(rendered.contains(&public_body));
            prop_assert!(!rendered.contains(&internal_marker));
            prop_assert!(!rendered.contains("Internal"));
        }
    }

    #[test]
    fn sentences_may_contain_versions_hosts_and_abbreviations() {
        assert!(bounded_sentence("Bump the minimum Rust version to 1.97."));
        assert!(bounded_sentence(
            "Publish every crate to crates.io in one train."
        ));
        assert!(bounded_sentence(
            "Support GitLab.com as an issuer, e.g. for mirrors!"
        ));
        assert!(!bounded_sentence("Short."));
        assert!(!bounded_sentence(
            "No terminal stop at the end of this sentence"
        ));
        assert!(!bounded_sentence("Two lines.\nAre not one sentence."));
    }

    #[test]
    fn repository_config_matches_embedded_contract() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        assert_eq!(
            config_set_digest([
                (CONFIG_PATH, CANONICAL_CONFIG),
                (HEADER_PATH, CANONICAL_HEADER),
            ]),
            CANONICAL_CHANGELOG_CONFIG_SHA256
        );
        assert_eq!(
            admit_changelog_config(&root).expect("repository config is canonical"),
            canonical_changelog_config_sha256()
        );
    }
}
