//! Offline DynamoDB client for test and local-only constructors.
//!
//! Config defaults must default pure values only — never live SDK clients,
//! network stacks, TLS providers, credential providers, or OS trust-store
//! reads. A real `aws_sdk_dynamodb::Client` eagerly constructs the default
//! HTTPS stack (hyper + rustls + aws-lc-rs), which parses the platform native
//! trust store. On a host whose native store yields no parseable roots that
//! parse trips a `debug_assert!` inside `aws-smithy-http-client`
//! (`client/tls/rustls_provider.rs`), turning a stub client into a panic.
//!
//! Test and local-only code that needs a `Client`-shaped value but never makes
//! a network call uses [`offline_ddb_client`]: a client whose HTTP connector
//! resolves every request to an error without touching DNS, TLS, credentials,
//! the OS trust store, or a socket. Production paths never use this — they
//! inject a real client built through the AWS SDK startup configuration.

use aws_sdk_dynamodb::config::{BehaviorVersion, Region};
use aws_smithy_runtime_api::client::{
    http::{HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn},
    orchestrator::HttpRequest,
    result::ConnectorError,
};

/// An HTTP connector that never establishes a connection.
///
/// Every request resolves immediately to a `never_connected` IO error. This is
/// the inert connector the AWS Smithy docs sanction for fake/mock clients: it
/// owns no DNS resolver, no TLS provider, and no connection pool, so building a
/// client around it performs no trust-store read and cannot panic.
#[derive(Debug)]
struct NeverConnect;

impl HttpConnector for NeverConnect {
    fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
        // `never_connected` records that no socket was opened, which is the
        // honest classification for a connector that deliberately refuses to
        // dial. The error text is diagnostic only; no test asserts on it.
        HttpConnectorFuture::ready(Err(ConnectorError::io(
            "offline DynamoDB client: HTTP connector is disabled".into(),
        )
        .never_connected()))
    }
}

/// Build a DynamoDB client that is structurally valid but never dials.
///
/// The injected HTTP client short-circuits every request, so no TLS connector
/// is constructed and no native roots are parsed. Region and behavior version
/// are set so client construction is total; they are never used because no
/// request reaches the network. Intended only for test and local-only
/// constructors — production injects a real client.
///
/// Marked `#[doc(hidden)]` and exposed via [`crate::dsql::offline_ddb_client`]
/// only so downstream crates can wire their own DSQL tests without constructing
/// a real SDK/TLS stack. It is not part of the supported public API.
#[doc(hidden)]
pub fn offline_ddb_client() -> aws_sdk_dynamodb::Client {
    let http_client =
        http_client_fn(|_settings, _components| SharedHttpConnector::new(NeverConnect));
    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .http_client(http_client)
        .build();
    aws_sdk_dynamodb::Client::from_conf(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Constructing the offline client performs no trust-store read and does
    /// not panic — this is the regression guard for the original
    /// native-roots `debug_assert!`.
    #[test]
    fn offline_client_constructs_without_panic() {
        let _client = offline_ddb_client();
    }

    /// The connector refuses to dial: a request resolves to a connector error
    /// rather than attempting a real connection.
    #[tokio::test]
    async fn offline_client_never_connects() {
        let client = offline_ddb_client();
        let result = client.describe_table().table_name("never").send().await;
        assert!(
            result.is_err(),
            "offline client must not reach a real DynamoDB endpoint"
        );
    }
}
