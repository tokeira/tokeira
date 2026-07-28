//! AWS IAM authentication through presigned STS `GetCallerIdentity` requests.
//!
//! The client URL is treated as an untrusted credential, never as an outbound
//! destination. Validation derives a static AWS STS host and preserves only the
//! signed query when reconstructing the request. This is both the identity proof
//! and the SSRF boundary for Tokeira's AWS-native authentication extension.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use base64::Engine as _;
use quick_xml::de::from_str;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::{
    Duration, OffsetDateTime, PrimitiveDateTime, format_description::FormatItem,
    macros::format_description,
};
use url::Url;

use crate::{AuthError, Claims, GrantRules, WorkerScopeRules};

const AWS_BEARER_PREFIX: &str = "tokeira-aws-v1.";
const MAX_STS_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CACHE_ENTRIES: usize = 1024;
const STS_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
const SIGV4_TIMESTAMP: &[FormatItem<'static>] =
    format_description!("[year][month][day]T[hour][minute][second]Z");

/// A validated request whose outbound host is derived from the AWS allow-list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedStsRequest {
    host: String,
    query: String,
    expires_at: OffsetDateTime,
}

impl ValidatedStsRequest {
    /// Validated AWS STS host used for the reconstructed request and Host header.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Remaining signed validity deadline used to bound cache retention.
    pub fn expires_at(&self) -> OffsetDateTime {
        self.expires_at
    }
}

/// Purely validate a client-supplied presigned URL at `now`.
pub fn validate_presigned_sts_url(
    encoded_url: &str,
    now: OffsetDateTime,
) -> Result<ValidatedStsRequest, AuthError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded_url)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(encoded_url))
        .map_err(|error| AuthError::new(format!("decode AWS bearer: {error}")))?;
    let raw_url = std::str::from_utf8(&bytes)
        .map_err(|error| AuthError::new(format!("decode AWS bearer URL: {error}")))?;
    let url = Url::parse(raw_url)
        .map_err(|error| AuthError::new(format!("parse AWS bearer URL: {error}")))?;
    if url.scheme() != "https" {
        return Err(AuthError::new("AWS bearer URL must use https"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AuthError::new("AWS bearer URL must not contain userinfo"));
    }
    if url.fragment().is_some() {
        return Err(AuthError::new("AWS bearer URL must not contain a fragment"));
    }
    if url.port().is_some_and(|port| port != 443) {
        return Err(AuthError::new("AWS bearer URL must not use a non-443 port"));
    }
    if url.path() != "/" {
        return Err(AuthError::new("AWS bearer URL path must be /"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| AuthError::new("AWS bearer URL host is missing"))?;
    let host_region = sts_host_region(host)?;
    let query = url
        .query()
        .ok_or_else(|| AuthError::new("AWS bearer URL query is missing"))?;
    let mut parameters: HashMap<String, Vec<String>> = HashMap::new();
    for (name, value) in url.query_pairs() {
        parameters
            .entry(name.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    for (name, values) in &parameters {
        if (name == "Action" || name == "Version" || name.starts_with("X-Amz-"))
            && values.len() != 1
        {
            return Err(AuthError::new(format!(
                "AWS bearer query parameter {name} must occur once"
            )));
        }
    }
    require_parameter(&parameters, "Action", "GetCallerIdentity")?;
    require_parameter(&parameters, "X-Amz-Algorithm", "AWS4-HMAC-SHA256")?;
    let credential = single_parameter(&parameters, "X-Amz-Credential")?;
    let scope = credential.split('/').collect::<Vec<_>>();
    if scope.len() != 5 || scope[3] != "sts" || scope[4] != "aws4_request" {
        return Err(AuthError::new(
            "AWS credential scope must end /sts/aws4_request",
        ));
    }
    if scope[2] != host_region {
        return Err(AuthError::new(
            "AWS credential region is inconsistent with STS host",
        ));
    }
    let signed_at = PrimitiveDateTime::parse(
        single_parameter(&parameters, "X-Amz-Date")?,
        SIGV4_TIMESTAMP,
    )
    .map_err(|error| AuthError::new(format!("invalid X-Amz-Date: {error}")))?
    .assume_utc();
    let expires = single_parameter(&parameters, "X-Amz-Expires")?
        .parse::<i64>()
        .map_err(|error| AuthError::new(format!("invalid X-Amz-Expires: {error}")))?;
    if !(0..=900).contains(&expires) {
        return Err(AuthError::new("X-Amz-Expires must be at most 900"));
    }
    let expires_at = signed_at + Duration::seconds(expires);
    if now < signed_at || now > expires_at {
        return Err(AuthError::new("AWS bearer is outside its validity window"));
    }
    Ok(ValidatedStsRequest {
        host: host.to_owned(),
        query: query.to_owned(),
        expires_at,
    })
}

fn sts_host_region(host: &str) -> Result<&str, AuthError> {
    if host == "sts.amazonaws.com" {
        return Ok("us-east-1");
    }
    let Some(region) = host
        .strip_prefix("sts.")
        .and_then(|host| host.strip_suffix(".amazonaws.com"))
    else {
        return Err(AuthError::new("AWS bearer host is not an allowed STS host"));
    };
    if region.is_empty()
        || region.contains('.')
        || !region
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(AuthError::new("AWS bearer STS region is invalid"));
    }
    Ok(region)
}

fn single_parameter<'a>(
    parameters: &'a HashMap<String, Vec<String>>,
    name: &str,
) -> Result<&'a str, AuthError> {
    let values = parameters
        .get(name)
        .ok_or_else(|| AuthError::new(format!("AWS bearer query parameter {name} is missing")))?;
    if values.len() != 1 {
        return Err(AuthError::new(format!(
            "AWS bearer query parameter {name} must occur once"
        )));
    }
    Ok(&values[0])
}

fn require_parameter(
    parameters: &HashMap<String, Vec<String>>,
    name: &str,
    expected: &str,
) -> Result<(), AuthError> {
    if single_parameter(parameters, name)? != expected {
        return Err(AuthError::new(format!(
            "AWS bearer query parameter {name} is invalid"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct CachedVerdict {
    claims: Claims,
    expires_at: OffsetDateTime,
}

/// AWS IAM claim mapper backed by an STS verification request.
pub struct StsAuthenticator {
    grants: GrantRules,
    worker_scopes: WorkerScopeRules,
    client: reqwest::Client,
    verification_base_url: Option<Url>,
    cache: Arc<Mutex<HashMap<[u8; 32], CachedVerdict>>>,
}

impl std::fmt::Debug for StsAuthenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StsAuthenticator")
            .field("verification_base_url", &self.verification_base_url)
            .finish_non_exhaustive()
    }
}

impl StsAuthenticator {
    /// Construct the production verifier with redirects disabled.
    pub fn new(grants: GrantRules) -> Self {
        Self {
            grants,
            worker_scopes: WorkerScopeRules::default(),
            client: reqwest::Client::builder()
                // Proxy environment variables would move the actual outbound
                // peer outside the validated STS-host boundary. Direct-only
                // transport keeps the SSRF proof aligned with the destination.
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                // Authentication must fail closed without allowing an STS
                // outage to hold the edge admission path indefinitely.
                .timeout(STS_REQUEST_TIMEOUT)
                .build()
                .expect("static reqwest client configuration is valid"),
            verification_base_url: None,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach validated ARN-to-Worker-scope mappings.
    pub fn with_worker_scopes(mut self, worker_scopes: WorkerScopeRules) -> Self {
        self.worker_scopes = worker_scopes;
        self
    }

    /// Override only the outbound fixture origin while retaining the validated
    /// AWS Host header and query. This seam is programmatic and never configurable.
    #[doc(hidden)]
    pub fn with_test_base_url(mut self, base_url: Url) -> Self {
        self.verification_base_url = Some(base_url);
        self
    }

    /// Verify one encoded URL after the `tokeira-aws-v1.` discriminator.
    pub async fn authenticate_encoded(&self, encoded_url: &str) -> Result<Claims, AuthError> {
        let now = OffsetDateTime::now_utc();
        let digest: [u8; 32] = Sha256::digest(encoded_url.as_bytes()).into();
        if let Some(cached) = self.cache().get(&digest).cloned()
            && now <= cached.expires_at
        {
            return Ok(cached.claims);
        }
        let validated = validate_presigned_sts_url(encoded_url, now)?;
        let arn = self.verify_with_sts(&validated).await?;
        let mut claims = Claims {
            subject: arn.clone(),
            auth_type: "aws-iam".to_owned(),
            ..Default::default()
        };
        self.grants.apply(&arn, &mut claims);
        claims.worker_scope = self
            .worker_scopes
            .resolve(&arn)
            .map_err(|_| AuthError::new("conflicting configured Worker scopes"))?;
        let mut cache = self.cache();
        cache.retain(|_, verdict| now <= verdict.expires_at);
        if cache.len() >= MAX_CACHE_ENTRIES
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, verdict)| verdict.expires_at)
                .map(|(digest, _)| *digest)
        {
            cache.remove(&oldest);
        }
        cache.insert(
            digest,
            CachedVerdict {
                claims: claims.clone(),
                expires_at: validated.expires_at,
            },
        );
        Ok(claims)
    }

    async fn verify_with_sts(&self, validated: &ValidatedStsRequest) -> Result<String, AuthError> {
        let mut destination = match &self.verification_base_url {
            Some(base) => base.clone(),
            None => Url::parse(&format!("https://{}/", validated.host))
                .map_err(|error| AuthError::new(format!("construct STS URL: {error}")))?,
        };
        destination.set_path("/");
        destination.set_query(Some(&validated.query));
        destination.set_fragment(None);
        let mut response = self
            .client
            .get(destination)
            .header(reqwest::header::HOST, &validated.host)
            .send()
            .await
            .map_err(|error| AuthError::new(format!("STS verification failed: {error}")))?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(AuthError::new(format!(
                "STS verification returned {}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_STS_RESPONSE_BYTES as u64)
        {
            return Err(AuthError::new("STS response exceeded size limit"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| AuthError::new(format!("read STS response: {error}")))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_STS_RESPONSE_BYTES {
                return Err(AuthError::new("STS response exceeded size limit"));
            }
            body.extend_from_slice(&chunk);
        }
        let body = std::str::from_utf8(&body)
            .map_err(|error| AuthError::new(format!("decode STS response: {error}")))?;
        let response: GetCallerIdentityResponse = from_str(body)
            .map_err(|error| AuthError::new(format!("decode STS response: {error}")))?;
        if response.result.arn.trim().is_empty() {
            return Err(AuthError::new("STS response ARN is empty"));
        }
        Ok(response.result.arn)
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 32], CachedVerdict>> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Whether a bearer body is reserved for the AWS IAM authenticator.
pub fn is_aws_bearer(encoded: &str) -> bool {
    encoded.starts_with(AWS_BEARER_PREFIX)
}

/// Strip the versioned AWS bearer discriminator.
pub fn strip_aws_bearer_prefix(encoded: &str) -> Option<&str> {
    encoded.strip_prefix(AWS_BEARER_PREFIX)
}

#[derive(Debug, Deserialize)]
struct GetCallerIdentityResponse {
    #[serde(rename = "GetCallerIdentityResult")]
    result: GetCallerIdentityResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetCallerIdentityResult {
    arn: String,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use proptest::prelude::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;
    use crate::{
        Access, Authorizer, AuthzDecision, CallClassification, CallTarget, DefaultAuthorizer, Scope,
    };

    const NOW: OffsetDateTime = time::macros::datetime!(2026-07-17 12:00:00 UTC);

    fn encoded_url(host: &str, region: &str, extra: &str) -> String {
        let raw = format!(
            "https://{host}/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20260717%2F{region}%2Fsts%2Faws4_request&X-Amz-Date=20260717T115900Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=dummy{extra}"
        );
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
    }

    fn live_encoded_url() -> String {
        let signed_at = OffsetDateTime::now_utc() - Duration::seconds(1);
        let timestamp = signed_at.format(SIGV4_TIMESTAMP).expect("timestamp");
        let date = signed_at
            .format(format_description!("[year][month][day]"))
            .expect("date");
        let raw = format!(
            "https://sts.eu-west-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F{date}%2Feu-west-1%2Fsts%2Faws4_request&X-Amz-Date={timestamp}&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=dummy"
        );
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
    }

    #[test]
    fn validator_accepts_regional_and_global_sts_hosts() {
        let regional = validate_presigned_sts_url(
            &encoded_url("sts.eu-west-1.amazonaws.com", "eu-west-1", ""),
            NOW,
        )
        .expect("regional");
        assert_eq!(regional.host(), "sts.eu-west-1.amazonaws.com");
        validate_presigned_sts_url(&encoded_url("sts.amazonaws.com", "us-east-1", ""), NOW)
            .expect("global");
    }

    #[test]
    fn duplicated_security_parameter_is_rejected() {
        let result = validate_presigned_sts_url(
            &encoded_url(
                "sts.eu-west-1.amazonaws.com",
                "eu-west-1",
                "&X-Amz-Date=20260717T115900Z",
            ),
            NOW,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sts_verification_maps_grants_and_caches_only_the_allow_verdict() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = vec![0; 4096];
            let _ = stream.read(&mut request).await.expect("request");
            server_calls.fetch_add(1, Ordering::SeqCst);
            let body = "<GetCallerIdentityResponse><GetCallerIdentityResult><Arn>arn:aws:sts::123456789012:assumed-role/tokeira-worker-a/session</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("response");
        });
        let worker_scope = crate::WorkerScope::try_new(
            "prod".to_owned(),
            vec!["payments-worker".to_owned()],
            "payments".to_owned(),
            "build-a".to_owned(),
        )
        .expect("scope");
        let authenticator = StsAuthenticator::new(GrantRules::new(vec![
            crate::GrantRule::new(
                "arn:aws:sts::123456789012:assumed-role/tokeira-worker-*",
                ["prod:worker", "prod:write"],
            )
            .expect("grant"),
        ]))
        .with_worker_scopes(crate::WorkerScopeRules::new(vec![
            crate::WorkerScopeRule::new(
                "arn:aws:sts::123456789012:assumed-role/tokeira-worker-*",
                worker_scope.clone(),
            )
            .expect("scope rule"),
        ]))
        .with_test_base_url(Url::parse(&format!("http://{address}/")).expect("base URL"));

        let rejected = encoded_url("attacker.example", "us-east-1", "");
        assert!(authenticator.authenticate_encoded(&rejected).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let bearer = live_encoded_url();
        let digest: [u8; 32] = Sha256::digest(bearer.as_bytes()).into();
        authenticator.cache().insert(
            digest,
            CachedVerdict {
                claims: Claims {
                    subject: "stale-cached-subject".to_owned(),
                    ..Default::default()
                },
                expires_at: OffsetDateTime::now_utc() - Duration::seconds(1),
            },
        );
        let first = authenticator
            .authenticate_encoded(&bearer)
            .await
            .expect("verified ARN");
        server.await.expect("server");
        let second = authenticator
            .authenticate_encoded(&bearer)
            .await
            .expect("cached ARN");
        assert_eq!(first, second);
        assert_eq!(first.auth_type, "aws-iam");
        assert_eq!(
            first.namespaces.get("prod"),
            Some(&(crate::Role::WORKER | crate::Role::WRITER))
        );
        assert_eq!(first.worker_scope, Some(worker_scope));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let decision = DefaultAuthorizer
            .authorize(
                Some(&first),
                &CallTarget {
                    api_name: "/test.Service/Write",
                    namespace: Some("prod"),
                    classification: CallClassification {
                        scope: Scope::Namespace,
                        access: Access::Write,
                    },
                    worker: None,
                },
            )
            .expect("authorization decision");
        assert!(matches!(decision, AuthzDecision::Deny { .. }));
        let worker_decision = DefaultAuthorizer
            .authorize(
                Some(&first),
                &CallTarget {
                    api_name: "/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueue",
                    namespace: Some("prod"),
                    classification: CallClassification {
                        scope: Scope::Namespace,
                        access: Access::ReadOnly,
                    },
                    worker: Some(crate::WorkerCallTarget {
                        operation: crate::WorkerOperation::DescribeTaskQueue,
                        target: crate::WorkerTarget::TaskQueue {
                            normal_task_queue: "payments-worker",
                        },
                    }),
                },
            )
            .expect("Worker authorization decision");
        assert!(matches!(
            worker_decision,
            AuthzDecision::Allow {
                principal: Some(crate::AuthPrincipal {
                    principal_type,
                    ..
                })
            } if principal_type == "aws-iam"
        ));
        let cache = authenticator.cache();
        assert_eq!(cache.len(), 1);
        assert!(cache.values().all(|verdict| {
            verdict.expires_at
                <= validate_presigned_sts_url(&bearer, OffsetDateTime::now_utc())
                    .expect("still valid")
                    .expires_at
        }));
    }

    #[tokio::test]
    async fn sts_outage_fails_closed_without_caching_a_denial() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        drop(listener);
        let authenticator = StsAuthenticator::new(GrantRules::default())
            .with_test_base_url(Url::parse(&format!("http://{address}/")).expect("base URL"));
        assert!(
            authenticator
                .authenticate_encoded(&live_encoded_url())
                .await
                .is_err()
        );
        assert!(authenticator.cache().is_empty());
    }

    // Feature: authorization-foundation, Property 9: arbitrary unapproved hosts
    // cannot cross the pure validation boundary and therefore trigger no I/O.
    proptest! {
        #[test]
        fn property_unapproved_hosts_are_rejected(host in "[a-z0-9-]{1,20}\\.(example|internal|amazonaws\\.com\\.cn)") {
            let encoded = encoded_url(&host, "us-east-1", "");
            prop_assert!(validate_presigned_sts_url(&encoded, NOW).is_err());
        }
    }
}
