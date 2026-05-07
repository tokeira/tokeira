use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum WritebackError {
    #[error("failed to read TOML config at {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML config at {path}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml_edit::TomlError,
    },
    #[error("invalid writeback key '{key}': {reason}")]
    InvalidKey { key: String, reason: String },
    #[error("failed to write TOML config at {path}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn write_config_values(path: &Path, values: &[(&str, &str)]) -> Result<(), WritebackError> {
    if values.is_empty() {
        return Ok(());
    }

    let mut document = std::fs::read_to_string(path)
        .map_err(|source| WritebackError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .parse::<toml_edit::DocumentMut>()
        .map_err(|source| WritebackError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

    for (key, value) in values {
        set_dotted_string(&mut document, key, value)?;
    }

    std::fs::write(path, document.to_string()).map_err(|source| WritebackError::Write {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn set_dotted_string(
    document: &mut toml_edit::DocumentMut,
    dotted_key: &str,
    value: &str,
) -> Result<(), WritebackError> {
    let parts = dotted_key.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        return Err(WritebackError::InvalidKey {
            key: dotted_key.to_owned(),
            reason: "key must contain non-empty dotted segments".to_owned(),
        });
    }

    let mut item = document.as_item_mut();
    for part in &parts[..parts.len() - 1] {
        if item.get(part).is_none() {
            item[part] = toml_edit::Item::Table(toml_edit::Table::new());
        }
        item = item
            .get_mut(part)
            .ok_or_else(|| WritebackError::InvalidKey {
                key: dotted_key.to_owned(),
                reason: format!("failed to create TOML table {part}"),
            })?;
        if !item.is_table() {
            return Err(WritebackError::InvalidKey {
                key: dotted_key.to_owned(),
                reason: format!("{part} exists but is not a table"),
            });
        }
    }

    item[parts[parts.len() - 1]] = toml_edit::value(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    use super::*;

    proptest! {
        #[test]
        fn toml_writeback_round_trips(
            pairs in prop::collection::vec((dotted_key_strategy(), "[a-zA-Z0-9_-]{1,24}"), 1..30)
        ) {
            prop_assume!(!has_prefix_conflict(&pairs));
            let temp = tempfile::tempdir().expect("tempdir");
            let path = temp.path().join("config.toml");
            std::fs::write(&path, "").expect("write config");

            let values = pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>();
            write_config_values(&path, &values)
                .map_err(|err| TestCaseError::fail(err.to_string()))?;

            let document = std::fs::read_to_string(&path)
                .expect("read config")
                .parse::<toml_edit::DocumentMut>()
                .expect("parse config");
            let expected = pairs.into_iter().collect::<BTreeMap<_, _>>();
            for (key, value) in expected {
                let actual = read_dotted_string(&document, &key);
                prop_assert_eq!(actual.as_deref(), Some(value.as_str()));
            }
        }

        #[test]
        fn toml_writeback_preserves_comments(
            first_comment in "[a-zA-Z0-9 _-]{1,40}",
            second_comment in "[a-zA-Z0-9 _-]{1,40}",
            value in "[a-zA-Z0-9_-]{1,24}"
        ) {
            let temp = tempfile::tempdir().expect("tempdir");
            let path = temp.path().join("config.toml");
            let source = format!(
                "# {first_comment}\n[infrastructure]\n# {second_comment}\ndsql_endpoint = \"old\"\n"
            );
            std::fs::write(&path, source).expect("write config");

            write_config_values(&path, &[("infrastructure.dsql_endpoint", &value)])
                .map_err(|err| TestCaseError::fail(err.to_string()))?;

            let rendered = std::fs::read_to_string(&path).expect("read config");
            let expected_first = format!("# {first_comment}");
            let expected_second = format!("# {second_comment}");
            prop_assert!(rendered.contains(&expected_first));
            prop_assert!(rendered.contains(&expected_second));
            let document = rendered
                .parse::<toml_edit::DocumentMut>()
                .expect("parse config");
            let actual = read_dotted_string(&document, "infrastructure.dsql_endpoint");
            prop_assert_eq!(actual.as_deref(), Some(value.as_str()));
        }
    }

    #[test]
    fn writeback_targets_explicit_file_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let server = temp.path().join("tokeirad.toml");
        let platform = temp.path().join("deployment.toml");
        std::fs::write(&server, "").expect("write server");
        std::fs::write(&platform, "").expect("write platform");

        write_config_values(&platform, &[("services.runtime.image", "example/repo:tag")])
            .expect("write platform");

        assert_eq!(std::fs::read_to_string(&server).expect("read server"), "");
        assert!(
            std::fs::read_to_string(&platform)
                .expect("read platform")
                .contains("example/repo:tag")
        );
    }

    fn dotted_key_strategy() -> impl Strategy<Value = String> {
        prop::collection::vec("[a-zA-Z_][a-zA-Z0-9_]{0,10}", 1..5).prop_map(|parts| parts.join("."))
    }

    fn has_prefix_conflict(pairs: &[(String, String)]) -> bool {
        pairs.iter().any(|(left, _)| {
            pairs.iter().any(|(right, _)| {
                left != right
                    && right
                        .strip_prefix(left)
                        .is_some_and(|suffix| suffix.starts_with('.'))
            })
        })
    }

    fn read_dotted_string(document: &toml_edit::DocumentMut, dotted_key: &str) -> Option<String> {
        let mut item = document.as_item();
        for part in dotted_key.split('.') {
            item = item.get(part)?;
        }
        item.as_str().map(ToOwned::to_owned)
    }
}
