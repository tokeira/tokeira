//! Where a configuration document comes from.
//!
//! Every Tokeira binary loads its configuration through this one locator
//! grammar, whatever schema it parses the bytes into:
//!
//! | Locator | Meaning |
//! |---|---|
//! | `/etc/tokeira/tokeirad.toml` | a file path |
//! | `file:/etc/tokeira/tokeirad.toml` | the same, spelled out |
//! | `env:TOKEIRA_CONFIG_CONTENT` | the document is the content of that environment variable |
//!
//! Naming a source selects it; a named source that cannot be read is a hard
//! error that repeats the locator — never a silent fall-through to defaults.
//! This module deliberately never fetches over a network: carrying bytes to a
//! process is platform wiring (the ECS agent injects environment content, the
//! kubelet mounts files). A locator becomes bytes; nothing more.

use std::{fmt, path::PathBuf};

use crate::ConfigError;

/// The environment variable every Tokeira binary reads for its configuration
/// locator. One name for tokeirad, the controller, and the autoscaler: they
/// are separate processes, so sharing the name costs nothing and means an
/// operator learns exactly one variable.
pub const CONFIG_ENV: &str = "TOKEIRA_CONFIG";

/// A parsed configuration locator: where one document's bytes come from.
///
/// The resolver is schema-blind — it produces the document's text and each
/// binary parses its own type — so the same two forms serve `tokeirad`, the
/// controller, and the autoscaler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigSource {
    /// Read the document from a file.
    File(PathBuf),
    /// Read the document from the content of an environment variable.
    Env(String),
}

impl ConfigSource {
    /// Parse a locator string.
    ///
    /// A bare string is a file path; `file:` and `env:` are the spelled-out
    /// forms. Any other `scheme:` prefix is refused by name rather than read
    /// as a relative path, so a typo (`evn:X`) or an unsupported scheme
    /// (`s3:bucket/key`) fails loudly instead of surfacing later as a
    /// confusing file-not-found. A single character before the colon is a
    /// Windows drive letter, not a scheme.
    pub fn parse(locator: &str) -> Result<Self, ConfigError> {
        let locator = locator.trim();
        if locator.is_empty() {
            return Err(ConfigError::Source {
                locator: locator.to_string(),
                reason: "the locator is empty".to_string(),
            });
        }
        if let Some(path) = locator.strip_prefix("file:") {
            if path.is_empty() {
                return Err(ConfigError::Source {
                    locator: locator.to_string(),
                    reason: "`file:` needs a path after the colon".to_string(),
                });
            }
            return Ok(Self::File(PathBuf::from(path)));
        }
        if let Some(var) = locator.strip_prefix("env:") {
            if var.is_empty() {
                return Err(ConfigError::Source {
                    locator: locator.to_string(),
                    reason: "`env:` needs an environment variable name after the colon".to_string(),
                });
            }
            return Ok(Self::Env(var.to_string()));
        }
        if let Some(scheme) = scheme_prefix(locator) {
            return Err(ConfigError::UnknownScheme {
                scheme: scheme.to_string(),
                locator: locator.to_string(),
            });
        }
        Ok(Self::File(PathBuf::from(locator)))
    }

    /// Read the document's text.
    ///
    /// Absence is fatal by design: the error repeats the locator so the
    /// operator sees exactly which source was selected and why it failed. An
    /// environment variable that is set but empty is an empty document (all
    /// defaults), the same as an empty file — present is present.
    pub fn read(&self) -> Result<String, ConfigError> {
        match self {
            Self::File(path) => std::fs::read_to_string(path).map_err(|err| ConfigError::Source {
                locator: self.to_string(),
                reason: err.to_string(),
            }),
            Self::Env(var) => match std::env::var(var) {
                Ok(content) => Ok(content),
                Err(std::env::VarError::NotPresent) => Err(ConfigError::Source {
                    locator: self.to_string(),
                    reason: format!("environment variable {var} is not set"),
                }),
                Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::Source {
                    locator: self.to_string(),
                    reason: format!("environment variable {var} is not valid UTF-8"),
                }),
            },
        }
    }

    /// Resolve the standard precedence: the `--config` flag, then
    /// [`CONFIG_ENV`], then nothing.
    ///
    /// Returns the chosen source and a label for logs and `--dump-config`, or
    /// `None` when neither is set. What "nothing" means is the binary's call:
    /// `tokeirad` falls back to built-in defaults, the controller and
    /// autoscaler to their conventional file names.
    pub fn from_cli_env(flag: Option<&str>) -> Result<Option<(Self, String)>, ConfigError> {
        if let Some(locator) = flag {
            let source = Self::parse(locator)?;
            let label = format!("--config {source}");
            return Ok(Some((source, label)));
        }
        match std::env::var(CONFIG_ENV) {
            Ok(locator) => {
                let source = Self::parse(&locator)?;
                let label = format!("{CONFIG_ENV} {source}");
                Ok(Some((source, label)))
            }
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::Source {
                locator: CONFIG_ENV.to_string(),
                reason: format!("environment variable {CONFIG_ENV} is not valid UTF-8"),
            }),
        }
    }
}

impl fmt::Display for ConfigSource {
    /// The canonical locator: a bare path for files, `env:VAR` for the
    /// environment. A path that would itself re-parse as something else (one
    /// literally named `env:x`, say) is spelled `file:` so that
    /// display-then-parse always returns the same source.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => {
                let bare = path.display().to_string();
                match Self::parse(&bare) {
                    Ok(Self::File(round_trip)) if round_trip == *path => write!(f, "{bare}"),
                    _ => write!(f, "file:{bare}"),
                }
            }
            Self::Env(var) => write!(f, "env:{var}"),
        }
    }
}

/// The would-be scheme before the first colon, when the prefix looks like a
/// scheme token: two or more characters, letter first, then letters, digits,
/// `_`, `+`, or `-`. Everything else — no colon, a single-letter drive
/// prefix, a path whose first segment holds a dot or slash — is a path.
fn scheme_prefix(locator: &str) -> Option<&str> {
    let (prefix, _) = locator.split_once(':')?;
    let mut chars = prefix.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if prefix.len() < 2 {
        return None;
    }
    if chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '+' | '-')) {
        Some(prefix)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn bare_and_spelled_paths_parse_as_files() {
        for locator in [
            "/etc/tokeira/tokeirad.toml",
            "./relative.toml",
            "tokeirad.toml",
            "file:/etc/tokeira/tokeirad.toml",
            "file:with spaces.toml",
        ] {
            let source = ConfigSource::parse(locator).unwrap();
            assert!(matches!(source, ConfigSource::File(_)), "{locator}");
        }
    }

    #[test]
    fn env_locator_parses_and_displays() {
        let source = ConfigSource::parse("env:TOKEIRA_CONFIG_CONTENT").unwrap();
        assert_eq!(
            source,
            ConfigSource::Env("TOKEIRA_CONFIG_CONTENT".to_string())
        );
        assert_eq!(source.to_string(), "env:TOKEIRA_CONFIG_CONTENT");
    }

    #[test]
    fn drive_letter_is_a_path_not_a_scheme() {
        let source = ConfigSource::parse(r"C:\tokeira\tokeirad.toml").unwrap();
        assert!(matches!(source, ConfigSource::File(_)));
    }

    #[test]
    fn unknown_scheme_is_refused_by_name() {
        let err = ConfigSource::parse("s3:bucket/tokeirad.toml").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("s3"), "{message}");
        assert!(message.contains("file:<path>"), "{message}");
        assert!(message.contains("env:<VAR>"), "{message}");
    }

    #[test]
    fn empty_forms_are_refused() {
        for locator in ["", "   ", "file:", "env:"] {
            let err = ConfigSource::parse(locator).unwrap_err();
            assert!(
                matches!(err, ConfigError::Source { .. }),
                "{locator}: {err}"
            );
        }
    }

    #[test]
    fn pathological_file_names_display_unambiguously() {
        let source = ConfigSource::File(PathBuf::from("env:not-a-var"));
        assert_eq!(source.to_string(), "file:env:not-a-var");
        assert_eq!(ConfigSource::parse(&source.to_string()).unwrap(), source);
    }

    #[test]
    fn missing_file_error_repeats_the_locator() {
        let source = ConfigSource::parse("/definitely/not/here.toml").unwrap();
        let err = source.read().unwrap_err();
        assert!(
            err.to_string().contains("/definitely/not/here.toml"),
            "{err}"
        );
    }

    proptest! {
        // Property: displaying any parsed source and parsing it again returns
        // the same source — the canonical form is unambiguous.
        #[test]
        fn display_then_parse_round_trips(locator in "\\PC{1,60}") {
            if let Ok(source) = ConfigSource::parse(&locator) {
                let round_trip = ConfigSource::parse(&source.to_string()).unwrap();
                prop_assert_eq!(round_trip, source);
            }
        }
    }
}
