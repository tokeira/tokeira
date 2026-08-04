//! Secret references and the seam that resolves them.
//!
//! Configuration carries references to secrets, never values. A reference is
//! a locator string in a schema field — `env:VAR`, `aws-sm:<arn-or-name>`,
//! or `aws-ssm:<parameter>` — and the value it names exists only inside the
//! running process, wrapped in a type that refuses to print, log, or
//! serialize. That split is what keeps secret material out of definitions,
//! rendered documents, revisions, diffs, and plans by construction.
//!
//! Resolution happens once, at process start: `env:` references read the
//! environment directly, store-backed references go through a
//! [`SecretsProvider`]. The AWS provider lives in the `tokeira-secrets`
//! crate so this one never carries an SDK; a binary with no provider wires
//! [`NoSecretsProvider`] and still resolves `env:` references. Rotating a
//! secret is a restart, never a live re-read.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// A reference to one secret value. References are ordinary configuration —
/// comparable, serializable, safe to log; only resolved values are guarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretRef {
    /// The value is the content of an environment variable, injected by the
    /// platform at start. Needs no provider.
    Env(String),
    /// The value lives in AWS Secrets Manager; resolution reads the current
    /// version under the process's ambient identity.
    AwsSecretsManager(String),
    /// The value lives in AWS SSM Parameter Store as a SecureString.
    AwsSsmParameter(String),
}

impl SecretRef {
    /// Parse a secret reference.
    ///
    /// Unlike a config-document locator there is no bare form: a secret
    /// field must name where its value lives, so a plain string is refused
    /// rather than guessed at.
    pub fn parse(locator: &str) -> Result<Self, SecretError> {
        let locator = locator.trim();
        if let Some(var) = locator.strip_prefix("env:") {
            if var.is_empty() {
                return Err(SecretError::invalid(
                    locator,
                    "`env:` needs an environment variable name after the colon",
                ));
            }
            return Ok(Self::Env(var.to_string()));
        }
        if let Some(name) = locator.strip_prefix("aws-sm:") {
            if name.is_empty() {
                return Err(SecretError::invalid(
                    locator,
                    "`aws-sm:` needs a secret name or ARN after the colon",
                ));
            }
            return Ok(Self::AwsSecretsManager(name.to_string()));
        }
        if let Some(name) = locator.strip_prefix("aws-ssm:") {
            if name.is_empty() {
                return Err(SecretError::invalid(
                    locator,
                    "`aws-ssm:` needs a parameter name after the colon",
                ));
            }
            return Ok(Self::AwsSsmParameter(name.to_string()));
        }
        Err(SecretError::invalid(
            locator,
            "a secret reference must name its scheme: `env:<VAR>`, \
             `aws-sm:<arn-or-name>`, or `aws-ssm:<parameter>`",
        ))
    }

    /// Resolve the reference at process start: `env:` locally, store-backed
    /// references through the provider. Absence is fatal and repeats the
    /// locator — a missing secret is a deployment defect, not a default.
    pub async fn resolve(
        &self,
        provider: &dyn SecretsProvider,
    ) -> Result<Secret<String>, SecretError> {
        match self {
            Self::Env(var) => match std::env::var(var) {
                Ok(value) => Ok(Secret::new(value)),
                Err(std::env::VarError::NotPresent) => Err(SecretError::Unresolvable {
                    locator: self.to_string(),
                    reason: format!("environment variable {var} is not set"),
                }),
                Err(std::env::VarError::NotUnicode(_)) => Err(SecretError::Unresolvable {
                    locator: self.to_string(),
                    reason: format!("environment variable {var} is not valid UTF-8"),
                }),
            },
            _ => provider.fetch(self).await,
        }
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Env(var) => write!(f, "env:{var}"),
            Self::AwsSecretsManager(name) => write!(f, "aws-sm:{name}"),
            Self::AwsSsmParameter(name) => write!(f, "aws-ssm:{name}"),
        }
    }
}

impl Serialize for SecretRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let locator = String::deserialize(deserializer)?;
        Self::parse(&locator).map_err(serde::de::Error::custom)
    }
}

/// A resolved secret value. The wrapper is the guard: no `Display`, no
/// `Serialize`, no `Deserialize`, and a `Debug` that redacts — reaching the
/// value takes a visible, greppable [`expose`](Self::expose).
pub struct Secret<T>(T);

impl<T> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// The value. Every use site is explicit by design.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consume the wrapper where an API needs ownership (a client builder
    /// taking a password `String`, say).
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Clone> Clone for Secret<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(REDACTED)")
    }
}

/// Errors from parsing or resolving secret references.
#[derive(Debug, Error)]
pub enum SecretError {
    #[error("secret reference `{locator}` is invalid: {reason}")]
    Invalid { locator: String, reason: String },
    #[error("secret `{locator}` cannot be resolved: {reason}")]
    Unresolvable { locator: String, reason: String },
    #[error(
        "secret `{locator}` needs a secrets provider, and this process has none; \
         only `env:` references resolve without one"
    )]
    NoProvider { locator: String },
}

impl SecretError {
    fn invalid(locator: &str, reason: &str) -> Self {
        Self::Invalid {
            locator: locator.to_string(),
            reason: reason.to_string(),
        }
    }
}

/// The seam a store-backed secret resolves through. Implementations fetch
/// `aws-sm:` and `aws-ssm:` references under the process's ambient identity;
/// `env:` references never reach a provider — [`SecretRef::resolve`] handles
/// them locally first.
#[async_trait]
pub trait SecretsProvider: Send + Sync {
    async fn fetch(&self, reference: &SecretRef) -> Result<Secret<String>, SecretError>;
}

/// The provider for a process that has none. `env:` references still
/// resolve; anything store-backed fails with a message that says exactly
/// what is missing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSecretsProvider;

#[async_trait]
impl SecretsProvider for NoSecretsProvider {
    async fn fetch(&self, reference: &SecretRef) -> Result<Secret<String>, SecretError> {
        Err(SecretError::NoProvider {
            locator: reference.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn references_parse_and_display_canonically() {
        for (locator, expected) in [
            ("env:GRAFANA_ADMIN", SecretRef::Env("GRAFANA_ADMIN".into())),
            (
                "aws-sm:arn:aws:secretsmanager:eu-west-2:1:secret:x",
                SecretRef::AwsSecretsManager("arn:aws:secretsmanager:eu-west-2:1:secret:x".into()),
            ),
            (
                "aws-ssm:/acme/grafana/admin",
                SecretRef::AwsSsmParameter("/acme/grafana/admin".into()),
            ),
        ] {
            let parsed = SecretRef::parse(locator).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), locator);
        }
    }

    #[test]
    fn bare_strings_are_refused_with_the_forms_named() {
        let err = SecretRef::parse("hunter2").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("env:<VAR>"), "{message}");
        assert!(message.contains("aws-sm:"), "{message}");
        assert!(message.contains("aws-ssm:"), "{message}");
    }

    #[test]
    fn empty_forms_are_refused() {
        for locator in ["env:", "aws-sm:", "aws-ssm:"] {
            assert!(SecretRef::parse(locator).is_err(), "{locator}");
        }
    }

    #[test]
    fn debug_redacts_the_value() {
        let secret = Secret::new("hunter2".to_string());
        let debug = format!("{secret:?}");
        assert_eq!(debug, "Secret(REDACTED)");
        assert!(!debug.contains("hunter2"));
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn references_round_trip_through_serde_as_strings() {
        #[derive(Serialize, Deserialize)]
        struct Doc {
            admin: SecretRef,
        }
        let doc: Doc = toml::from_str("admin = \"aws-sm:acme/grafana\"").unwrap();
        assert_eq!(
            doc.admin,
            SecretRef::AwsSecretsManager("acme/grafana".into())
        );
        let rendered = toml::to_string(&doc).unwrap();
        assert!(rendered.contains("aws-sm:acme/grafana"), "{rendered}");
    }

    proptest! {
        // Property: displaying any parsed reference and parsing it again
        // returns the same reference.
        #[test]
        fn display_then_parse_round_trips(locator in "(env|aws-sm|aws-ssm):[\\PC]{1,40}") {
            if let Ok(reference) = SecretRef::parse(&locator) {
                let round_trip = SecretRef::parse(&reference.to_string()).unwrap();
                prop_assert_eq!(round_trip, reference);
            }
        }
    }
}
