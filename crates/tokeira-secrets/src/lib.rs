//! The AWS secrets provider: Secrets Manager and SSM Parameter Store.
//!
//! Resolves `aws-sm:` and `aws-ssm:` references for any Tokeira binary,
//! always under the process's ambient identity — a task role on ECS, pod
//! identity on EKS, the operator's credentials on compose. A secret is never
//! used to fetch a secret, and this crate is the only place the AWS SDKs
//! meet the secret seam, so `tokeira-config` stays SDK-free.
//!
//! The IAM surface a consumer needs is exactly what it references:
//! `secretsmanager:GetSecretValue` for `aws-sm:` and `ssm:GetParameter` for
//! `aws-ssm:`, scoped to the named ARNs. Resolution reads the current
//! version at process start; rotating a secret is a rolling restart.

use async_trait::async_trait;
use tokeira_config::{Secret, SecretError, SecretRef, SecretsProvider};

/// The AWS-backed [`SecretsProvider`].
#[derive(Clone, Debug)]
pub struct AwsSecretsProvider {
    secrets_manager: aws_sdk_secretsmanager::Client,
    ssm: aws_sdk_ssm::Client,
}

impl AwsSecretsProvider {
    /// Build from an already-loaded SDK config, the same shape every other
    /// AWS client in the workspace takes.
    pub(crate) fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        Self {
            secrets_manager: aws_sdk_secretsmanager::Client::new(sdk_config),
            ssm: aws_sdk_ssm::Client::new(sdk_config),
        }
    }

    /// Build from the process's ambient identity: environment, task role,
    /// pod identity, or the operator's credential chain.
    pub async fn from_ambient() -> Self {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        Self::new(&sdk_config)
    }
}

#[async_trait]
impl SecretsProvider for AwsSecretsProvider {
    async fn fetch(&self, reference: &SecretRef) -> Result<Secret<String>, SecretError> {
        match reference {
            SecretRef::AwsSecretsManager(name) => {
                let value = self
                    .secrets_manager
                    .get_secret_value()
                    .secret_id(name)
                    .send()
                    .await
                    .map_err(|err| unresolvable(reference, err))?;
                string_secret(reference, &value)
            }
            SecretRef::AwsSsmParameter(name) => {
                let value = self
                    .ssm
                    .get_parameter()
                    .name(name)
                    .with_decryption(true)
                    .send()
                    .await
                    .map_err(|err| unresolvable(reference, err))?;
                parameter_value(reference, &value)
            }
            // `env:` never reaches a provider — SecretRef::resolve handles it
            // locally. Answering anyway would invite a second resolution path.
            SecretRef::Env(_) => Err(SecretError::Unresolvable {
                locator: reference.to_string(),
                reason: "env references resolve without a provider".to_string(),
            }),
        }
    }
}

// The response-interpretation decisions are separated from the wire calls so the
// offline suite can exercise them against builder-constructed SDK outputs — a
// live client cannot be driven without credentials, and the default suite runs
// with none by contract.

/// Interpret a Secrets Manager response: a string secret resolves; a binary
/// secret is refused — binary has no place in a config value, and refusing
/// beats handing a schema field undecodable bytes.
fn string_secret(
    reference: &SecretRef,
    value: &aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput,
) -> Result<Secret<String>, SecretError> {
    match value.secret_string() {
        Some(secret) => Ok(Secret::new(secret.to_string())),
        None => Err(SecretError::Unresolvable {
            locator: reference.to_string(),
            reason: "the secret holds binary data, not a string value".to_string(),
        }),
    }
}

/// Interpret an SSM response: a parameter with a value resolves; a parameter
/// record without one is refused with the locator named.
fn parameter_value(
    reference: &SecretRef,
    value: &aws_sdk_ssm::operation::get_parameter::GetParameterOutput,
) -> Result<Secret<String>, SecretError> {
    match value.parameter().and_then(|parameter| parameter.value()) {
        Some(secret) => Ok(Secret::new(secret.to_string())),
        None => Err(SecretError::Unresolvable {
            locator: reference.to_string(),
            reason: "the parameter exists but has no value".to_string(),
        }),
    }
}

fn unresolvable(reference: &SecretRef, err: impl std::fmt::Display) -> SecretError {
    SecretError::Unresolvable {
        locator: reference.to_string(),
        reason: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use tokeira_config::NoSecretsProvider;

    use super::*;

    // Offline by contract: the default suite needs no AWS credentials, so
    // these tests exercise dispatch and error shape, never a live call.

    #[tokio::test]
    async fn env_references_resolve_without_any_provider() {
        // PATH is present in every test environment; no env mutation needed.
        let reference = SecretRef::parse("env:PATH").unwrap();
        let secret = reference.resolve(&NoSecretsProvider).await.unwrap();
        assert!(!secret.expose().is_empty());
    }

    #[tokio::test]
    async fn an_unset_env_reference_is_fatal_and_names_the_locator() {
        let reference = SecretRef::parse("env:TOKEIRA_SECRET_TEST_DEFINITELY_UNSET").unwrap();
        let err = reference.resolve(&NoSecretsProvider).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("env:TOKEIRA_SECRET_TEST_DEFINITELY_UNSET"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn store_references_without_a_provider_name_whats_missing() {
        let reference = SecretRef::parse("aws-sm:acme/grafana").unwrap();
        let err = reference.resolve(&NoSecretsProvider).await.unwrap_err();
        let message = err.to_string();
        assert!(message.contains("aws-sm:acme/grafana"), "{message}");
        assert!(message.contains("secrets provider"), "{message}");
    }

    #[test]
    fn a_string_secret_resolves() {
        let reference = SecretRef::parse("aws-sm:acme/grafana").unwrap();
        let output =
            aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput::builder()
                .secret_string("hunter2")
                .build();
        let secret = string_secret(&reference, &output).unwrap();
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn a_binary_secret_is_refused_with_the_locator_named() {
        let reference = SecretRef::parse("aws-sm:acme/blob").unwrap();
        let output =
            aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput::builder()
                .secret_binary(aws_sdk_secretsmanager::primitives::Blob::new(vec![0u8, 1]))
                .build();
        let err = string_secret(&reference, &output).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("aws-sm:acme/blob"), "{message}");
        assert!(message.contains("binary"), "{message}");
    }

    #[test]
    fn a_parameter_with_a_value_resolves() {
        let reference = SecretRef::parse("aws-ssm:/acme/db-password").unwrap();
        let output = aws_sdk_ssm::operation::get_parameter::GetParameterOutput::builder()
            .parameter(
                aws_sdk_ssm::types::Parameter::builder()
                    .name("/acme/db-password")
                    .value("swordfish")
                    .build(),
            )
            .build();
        let secret = parameter_value(&reference, &output).unwrap();
        assert_eq!(secret.expose(), "swordfish");
    }

    #[test]
    fn a_parameter_without_a_value_is_refused_with_the_locator_named() {
        let reference = SecretRef::parse("aws-ssm:/acme/empty").unwrap();
        let output = aws_sdk_ssm::operation::get_parameter::GetParameterOutput::builder()
            .parameter(
                aws_sdk_ssm::types::Parameter::builder()
                    .name("/acme/empty")
                    .build(),
            )
            .build();
        let err = parameter_value(&reference, &output).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("aws-ssm:/acme/empty"), "{message}");
        assert!(message.contains("no value"), "{message}");
    }

    #[test]
    fn sdk_errors_map_to_unresolvable_with_locator_and_reason() {
        let reference = SecretRef::parse("aws-sm:acme/grafana").unwrap();
        let err = unresolvable(&reference, "connection refused");
        let message = err.to_string();
        assert!(message.contains("aws-sm:acme/grafana"), "{message}");
        assert!(message.contains("connection refused"), "{message}");
    }

    #[tokio::test]
    async fn the_aws_provider_refuses_env_references() {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let provider = AwsSecretsProvider::new(&sdk_config);
        let reference = SecretRef::parse("env:X").unwrap();
        let err = provider.fetch(&reference).await.unwrap_err();
        assert!(err.to_string().contains("without a provider"), "{err}");
    }
}
