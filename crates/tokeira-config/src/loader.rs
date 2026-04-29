use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

/// Errors from the generic config loader.
#[derive(Debug, Error)]
pub enum ConfigLoaderError {
    #[error("failed to read config file '{path}': {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("TOML parse error in '{path}': {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("TOML serialization error: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("validation errors: {errors:?}")]
    Validation { errors: Vec<String> },
}

/// Load a TOML config file, optionally deep-merging a profile overlay,
/// and deserialize into `T`.
///
/// - `base_path`: path to the base `config.toml` file (not directory).
/// - `profile_path`: optional path to a profile TOML overlay file.
pub fn load_config<T: DeserializeOwned>(
    base_path: &Path,
    profile_path: Option<&Path>,
) -> Result<T, ConfigLoaderError> {
    let raw = std::fs::read_to_string(base_path).map_err(|source| {
        ConfigLoaderError::ReadFile {
            path: base_path.to_path_buf(),
            source,
        }
    })?;
    let mut base: toml::Value =
        toml::from_str(&raw).map_err(|source| ConfigLoaderError::Parse {
            path: base_path.to_path_buf(),
            source,
        })?;

    if let Some(profile) = profile_path {
        let profile_raw = std::fs::read_to_string(profile).map_err(|source| {
            ConfigLoaderError::ReadFile {
                path: profile.to_path_buf(),
                source,
            }
        })?;
        let overlay: toml::Value =
            toml::from_str(&profile_raw).map_err(|source| ConfigLoaderError::Parse {
                path: profile.to_path_buf(),
                source,
            })?;
        deep_merge(&mut base, overlay);
    }

    // Substitute {project} if present
    let project_name = base
        .get("project")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_owned();
    substitute_project_vars(&mut base, &project_name);

    base.try_into().map_err(|source| ConfigLoaderError::Parse {
        path: base_path.to_path_buf(),
        source,
    })
}

/// Serialize a config value to a TOML string.
pub fn write_config_toml<T: Serialize>(config: &T) -> Result<String, ConfigLoaderError> {
    toml::to_string_pretty(config).map_err(ConfigLoaderError::Serialize)
}

/// Recursively merge `override_val` into `base`. Tables merge key-by-key;
/// all other value types are replaced outright.
pub fn deep_merge(base: &mut toml::Value, override_val: toml::Value) {
    match (base, override_val) {
        (toml::Value::Table(base_table), toml::Value::Table(override_table)) => {
            for (key, value) in override_table {
                match base_table.get_mut(&key) {
                    Some(existing) => deep_merge(existing, value),
                    None => {
                        base_table.insert(key, value);
                    }
                }
            }
        }
        (base, override_val) => {
            *base = override_val;
        }
    }
}

/// Replace `{project}` placeholders in all string values of a TOML tree.
pub fn substitute_project_vars(value: &mut toml::Value, project_name: &str) {
    match value {
        toml::Value::String(s) => {
            if s.contains("{project}") {
                *s = s.replace("{project}", project_name);
            }
        }
        toml::Value::Table(table) => {
            for (_k, v) in table.iter_mut() {
                substitute_project_vars(v, project_name);
            }
        }
        toml::Value::Array(arr) => {
            for v in arr.iter_mut() {
                substitute_project_vars(v, project_name);
            }
        }
        _ => {}
    }
}
