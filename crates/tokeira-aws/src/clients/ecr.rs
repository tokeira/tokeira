//! ECR client abstraction, repository preparation, and short-lived registry
//! authorization.
//!
//! Callers provide repository identities; this module owns all AWS-specific
//! mechanics. Authorization passwords are returned only to the immediate
//! publishing boundary and are redacted by that boundary's secret type.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use base64::Engine as _;
use tokeira_iac::{ProvisionContext, error::IacError};

use super::AwsClients;

type TaggedRepositories = Vec<(String, HashMap<String, String>)>;

/// Short-lived credentials and the provider-returned ECR registry host.
///
/// The password is intentionally private and redacted from [`Debug`]. Callers
/// may expose it only while transferring it into a secret-aware registry
/// client; it must never be persisted or logged.
#[derive(Clone, PartialEq, Eq)]
pub struct EcrAuthorization {
    /// Account- and partition-qualified registry hostname returned by ECR.
    pub registry_host: String,
    /// Registry username returned by ECR (normally `AWS`).
    pub username: String,
    password: String,
}

impl EcrAuthorization {
    /// Construct the tagged destination for a prepared repository.
    ///
    /// The registry host is taken from ECR authorization rather than rebuilt
    /// from account and region inputs, preserving partition-specific endpoint
    /// semantics.
    #[must_use]
    pub fn image_ref(&self, repository: &str, tag: &str) -> String {
        format!("{}/{repository}:{tag}", self.registry_host)
    }

    /// Borrow the password for immediate transfer to a secret-aware client.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for EcrAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EcrAuthorization")
            .field("registry_host", &self.registry_host)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryDescription {
    pub(crate) name: String,
    pub(crate) arn: String,
    pub(crate) uri: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageTagMutability {
    Mutable,
    Immutable,
}

#[derive(Debug, thiserror::Error)]
pub enum EcrError {
    #[error("ECR repository not found: {name}")]
    NotFound { name: String },
    #[error("invalid ECR authorization token: {reason}")]
    InvalidToken { reason: String },
    #[error("ECR SDK error: {0}")]
    AwsSdk(String),
    #[error("ECR validation error: {0}")]
    Validation(String),
}

#[async_trait::async_trait]
pub trait EcrClient: Send + Sync {
    async fn get_authorization_token(&self) -> Result<EcrAuthorization, EcrError>;
    async fn describe_repository(&self, name: &str) -> Result<RepositoryDescription, EcrError>;
    async fn create_repository(
        &self,
        name: &str,
        mutability: ImageTagMutability,
        tags: &HashMap<String, String>,
    ) -> Result<RepositoryDescription, EcrError>;
    async fn delete_repository(&self, name: &str, force: bool) -> Result<(), EcrError>;
    async fn put_lifecycle_policy(&self, name: &str, policy: &str) -> Result<(), EcrError>;
    async fn tag_resource(&self, arn: &str, tags: &HashMap<String, String>)
    -> Result<(), EcrError>;
    async fn list_tags_for_resource(&self, arn: &str) -> Result<HashMap<String, String>, EcrError>;
    async fn get_lifecycle_policy(&self, name: &str) -> Result<Option<String>, EcrError>;
}

#[derive(Debug, Clone)]
pub struct DefaultEcrClient {
    client: aws_sdk_ecr::Client,
}

impl DefaultEcrClient {
    pub(crate) fn new(client: aws_sdk_ecr::Client) -> Self {
        Self { client }
    }

    pub fn from_aws_config(config: &aws_config::SdkConfig) -> Self {
        Self::new(aws_sdk_ecr::Client::new(config))
    }
}

#[async_trait::async_trait]
impl EcrClient for DefaultEcrClient {
    async fn get_authorization_token(&self) -> Result<EcrAuthorization, EcrError> {
        let output = self
            .client
            .get_authorization_token()
            .send()
            .await
            .map_err(|err| EcrError::AwsSdk(format!("GetAuthorizationToken: {err}")))?;
        let data = output
            .authorization_data()
            .first()
            .ok_or_else(|| EcrError::InvalidToken {
                reason: "authorization_data was empty".to_owned(),
            })?;
        let token = data
            .authorization_token()
            .ok_or_else(|| EcrError::InvalidToken {
                reason: "authorization_token was missing".to_owned(),
            })?;
        let endpoint = data
            .proxy_endpoint()
            .ok_or_else(|| EcrError::InvalidToken {
                reason: "proxy_endpoint was missing".to_owned(),
            })?;
        decode_authorization_data(token, endpoint)
    }

    async fn describe_repository(&self, name: &str) -> Result<RepositoryDescription, EcrError> {
        match self
            .client
            .describe_repositories()
            .repository_names(name)
            .send()
            .await
        {
            Ok(output) => output
                .repositories()
                .first()
                .map(repository_description)
                .transpose()?
                .ok_or_else(|| EcrError::NotFound {
                    name: name.to_owned(),
                }),
            Err(err) => {
                let service = err.into_service_error();
                if service.is_repository_not_found_exception() {
                    Err(EcrError::NotFound {
                        name: name.to_owned(),
                    })
                } else {
                    Err(EcrError::AwsSdk(format!(
                        "DescribeRepositories({name}): {service}"
                    )))
                }
            }
        }
    }

    async fn create_repository(
        &self,
        name: &str,
        mutability: ImageTagMutability,
        tags: &HashMap<String, String>,
    ) -> Result<RepositoryDescription, EcrError> {
        let output = self
            .client
            .create_repository()
            .repository_name(name)
            .image_tag_mutability(match mutability {
                ImageTagMutability::Mutable => aws_sdk_ecr::types::ImageTagMutability::Mutable,
                ImageTagMutability::Immutable => aws_sdk_ecr::types::ImageTagMutability::Immutable,
            })
            .set_tags(Some(crate::resources::ecr_tags(tags)))
            .send()
            .await
            .map_err(|err| EcrError::AwsSdk(format!("CreateRepository({name}): {err}")))?;
        output
            .repository()
            .map(repository_description)
            .transpose()?
            .ok_or_else(|| EcrError::AwsSdk("CreateRepository returned no repository".to_owned()))
    }

    async fn delete_repository(&self, name: &str, force: bool) -> Result<(), EcrError> {
        match self
            .client
            .delete_repository()
            .repository_name(name)
            .force(force)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                let service = err.into_service_error();
                if service.is_repository_not_found_exception() {
                    Ok(())
                } else {
                    Err(EcrError::AwsSdk(format!(
                        "DeleteRepository({name}): {service}"
                    )))
                }
            }
        }
    }

    async fn put_lifecycle_policy(&self, name: &str, policy: &str) -> Result<(), EcrError> {
        self.client
            .put_lifecycle_policy()
            .repository_name(name)
            .lifecycle_policy_text(policy)
            .send()
            .await
            .map_err(|err| EcrError::AwsSdk(format!("PutLifecyclePolicy({name}): {err}")))?;
        Ok(())
    }

    async fn tag_resource(
        &self,
        arn: &str,
        tags: &HashMap<String, String>,
    ) -> Result<(), EcrError> {
        self.client
            .tag_resource()
            .resource_arn(arn)
            .set_tags(Some(crate::resources::ecr_tags(tags)))
            .send()
            .await
            .map_err(|err| EcrError::AwsSdk(format!("TagResource({arn}): {err}")))?;
        Ok(())
    }

    async fn list_tags_for_resource(&self, arn: &str) -> Result<HashMap<String, String>, EcrError> {
        let output = self
            .client
            .list_tags_for_resource()
            .resource_arn(arn)
            .send()
            .await
            .map_err(|err| EcrError::AwsSdk(format!("ListTagsForResource({arn}): {err}")))?;
        Ok(output
            .tags()
            .iter()
            .map(|tag| (tag.key().to_owned(), tag.value().to_owned()))
            .collect())
    }

    async fn get_lifecycle_policy(&self, name: &str) -> Result<Option<String>, EcrError> {
        match self
            .client
            .get_lifecycle_policy()
            .repository_name(name)
            .send()
            .await
        {
            Ok(output) => Ok(output.lifecycle_policy_text().map(ToOwned::to_owned)),
            Err(err) => {
                let service = err.into_service_error();
                if service.is_lifecycle_policy_not_found_exception() {
                    Ok(None)
                } else {
                    Err(EcrError::AwsSdk(format!(
                        "GetLifecyclePolicy({name}): {service}"
                    )))
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct EcrClientHandle(pub Arc<dyn EcrClient>);

impl fmt::Debug for EcrClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EcrClientHandle")
            .field(&"<dyn EcrClient>")
            .finish()
    }
}

pub(crate) fn ecr_client(ctx: &ProvisionContext) -> Result<Arc<dyn EcrClient>, IacError> {
    ctx.extension::<EcrClientHandle>()
        .map(|handle| Arc::clone(&handle.0))
        .ok_or_else(|| {
            IacError::Other(anyhow::anyhow!(
                "ProvisionContext missing extension: EcrClientHandle"
            ))
        })
}

pub fn decode_authorization_data(
    token_b64: &str,
    proxy_endpoint: &str,
) -> Result<EcrAuthorization, EcrError> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token_b64)
        .map_err(|source| EcrError::InvalidToken {
            reason: format!("base64 decode failed: {source}"),
        })?;
    let decoded = String::from_utf8(decoded).map_err(|source| EcrError::InvalidToken {
        reason: format!("token is not UTF-8: {source}"),
    })?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or_else(|| EcrError::InvalidToken {
            reason: "token does not contain ':' separator".to_owned(),
        })?;

    Ok(EcrAuthorization {
        registry_host: normalize_proxy_endpoint(proxy_endpoint),
        username: username.to_owned(),
        password: password.to_owned(),
    })
}

pub async fn ensure_ecr_repository(
    ecr: &dyn EcrClient,
    name: &str,
    tags: &HashMap<String, String>,
) -> Result<(), EcrError> {
    let arn = match ecr.describe_repository(name).await {
        Ok(desc) => desc.arn,
        Err(EcrError::NotFound { .. }) => {
            ecr.create_repository(name, ImageTagMutability::Mutable, tags)
                .await?
                .arn
        }
        Err(err) => return Err(err),
    };
    ecr.put_lifecycle_policy(name, crate::resources::ecr_repository::ECR_LIFECYCLE_POLICY)
        .await?;
    ecr.tag_resource(&arn, tags).await?;
    Ok(())
}

pub async fn ensure_ecr_repositories(
    ecr: &dyn EcrClient,
    repos: &[(String, HashMap<String, String>)],
) -> Result<(), EcrError> {
    for (name, tags) in repos {
        ensure_ecr_repository(ecr, name, tags).await?;
    }
    Ok(())
}

/// Authenticate and prepare a definition-owned set of ECR repositories.
///
/// This is the provider-facing entry point for image operations. It loads one
/// AWS SDK bundle in the definition-authored region and applies the standard
/// deployment resource tags. Repository names remain caller-owned definition
/// data; this layer neither invents an image catalogue nor consults deployment
/// state.
pub async fn prepare_ecr_registry(
    region: &str,
    project: &str,
    repositories: &[String],
) -> Result<EcrAuthorization, EcrError> {
    let clients = AwsClients::load(Some(region)).await;
    let context = ProvisionContext::new(project, HashMap::new());
    prepare_ecr_registry_with_client(&clients.ecr_client(), &context, repositories).await
}

/// Authenticate and prepare repositories with an injected ECR client.
///
/// The supplied [`ProvisionContext`] is used only for the standard resource
/// tags applied by infrastructure provisioning. Repository creation is
/// idempotent, but malformed or duplicate names are refused before either
/// authentication or mutation. The returned password is short-lived AWS
/// credential material and must be passed directly into a secret-aware
/// registry client; callers must never persist or log it.
pub async fn prepare_ecr_registry_with_client(
    ecr: &dyn EcrClient,
    ctx: &ProvisionContext,
    repositories: &[String],
) -> Result<EcrAuthorization, EcrError> {
    let repositories = repository_inputs(ctx, repositories)?;
    // Authenticate before creating anything: invalid ambient credentials
    // must not leave a partially prepared registry behind.
    let authorization = ecr.get_authorization_token().await?;
    ensure_ecr_repositories(ecr, &repositories).await?;
    Ok(authorization)
}

fn repository_inputs(
    ctx: &ProvisionContext,
    repositories: &[String],
) -> Result<TaggedRepositories, EcrError> {
    let mut seen = HashSet::with_capacity(repositories.len());
    repositories
        .iter()
        .map(|repository| {
            if repository.trim().is_empty() {
                return Err(EcrError::Validation(
                    "repository name must not be empty".to_owned(),
                ));
            }
            if !seen.insert(repository.as_str()) {
                return Err(EcrError::Validation(format!(
                    "duplicate repository `{repository}`"
                )));
            }
            Ok((repository.clone(), ctx.resource_tags(repository)))
        })
        .collect()
}

fn repository_description(
    repository: &aws_sdk_ecr::types::Repository,
) -> Result<RepositoryDescription, EcrError> {
    let name = repository.repository_name().unwrap_or_default().to_owned();
    let arn = repository.repository_arn().unwrap_or_default().to_owned();
    let uri = repository.repository_uri().unwrap_or_default().to_owned();
    if name.is_empty() || arn.is_empty() || uri.is_empty() {
        return Err(EcrError::AwsSdk(
            "repository description missing name, arn, or uri".to_owned(),
        ));
    }
    Ok(RepositoryDescription { name, arn, uri })
}

fn normalize_proxy_endpoint(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_authorization_data_succeeds() {
        let token = base64::engine::general_purpose::STANDARD.encode("AWS:secret");

        let auth =
            decode_authorization_data(&token, "https://123.dkr.ecr.us-east-1.amazonaws.com/")
                .expect("authorization");

        assert_eq!(
            auth,
            EcrAuthorization {
                registry_host: "123.dkr.ecr.us-east-1.amazonaws.com".to_owned(),
                username: "AWS".to_owned(),
                password: "secret".to_owned(),
            }
        );
        assert_eq!(auth.password(), "secret");
        assert_eq!(
            auth.image_ref("demo/runtime", "stable"),
            "123.dkr.ecr.us-east-1.amazonaws.com/demo/runtime:stable"
        );
        assert!(!format!("{auth:?}").contains("secret"));
    }

    #[test]
    fn decode_authorization_data_rejects_invalid_base64() {
        assert!(matches!(
            decode_authorization_data("not base64", "https://example.invalid"),
            Err(EcrError::InvalidToken { .. })
        ));
    }

    #[test]
    fn decode_authorization_data_rejects_invalid_utf8() {
        let token = base64::engine::general_purpose::STANDARD.encode([0xff, 0xff]);

        assert!(matches!(
            decode_authorization_data(&token, "https://example.invalid"),
            Err(EcrError::InvalidToken { .. })
        ));
    }

    #[test]
    fn decode_authorization_data_rejects_missing_separator() {
        let token = base64::engine::general_purpose::STANDARD.encode("AWS");

        assert!(matches!(
            decode_authorization_data(&token, "https://example.invalid"),
            Err(EcrError::InvalidToken { .. })
        ));
    }

    #[test]
    fn repository_preparation_refuses_duplicates_before_provider_work() {
        let context = ProvisionContext::new("demo", HashMap::new());
        let error = repository_inputs(
            &context,
            &["demo/runtime".to_owned(), "demo/runtime".to_owned()],
        )
        .expect_err("duplicate repository");

        assert!(matches!(error, EcrError::Validation(reason) if reason.contains("duplicate")));
    }
}
