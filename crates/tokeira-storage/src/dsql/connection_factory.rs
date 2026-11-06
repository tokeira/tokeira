//! Raw DSQL connection creation.
//!
//! The rest of the storage layer deals only in `PgConnection`s. This module is
//! the single place that knows about `aurora_dsql_sqlx_connector`, which keeps
//! IAM-token generation and connector error taxonomy out of repository code.
//!
//! Every connection authenticates with an IAM token minted for that connection
//! alone; tokens are never reused. The AWS credentials that sign the tokens are
//! loaded once and kept until shortly before they expire. The connector would
//! otherwise build a fresh default credentials chain for each connection, and
//! the SDK's token signer asks its provider for credentials directly, bypassing
//! the SDK's credential cache — so each connection would fetch credentials anew,
//! from the ECS credentials endpoint or STS, at up to the cluster's connection
//! rate.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use async_trait::async_trait;
use aurora_dsql_sqlx_connector::{
    DsqlConnectOptions, DsqlConnectOptionsBuilder, DsqlError, Region, SharedCredentialsProvider,
};
use aws_sdk_dynamodb::config::{Credentials, ProvideCredentials};
use sqlx::{
    PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell};

/// The front of every connection's `application_name`, which names Tokeira and
/// its release to the cluster. Database Insights attributes load and plans by
/// `application_name`, so an operator can tell Tokeira's statements from every
/// other client's, and one release's from the next.
///
/// The connector sets `application_name` itself, after any value in the
/// options, to `{ormPrefix}:aurora-dsql-rust-sqlx/{its version}`; its
/// `ormPrefix` parameter is the one place a client name goes
/// (aurora-dsql-sqlx-connector 0.2.2, `src/util.rs`, `build_application_name`).
pub const APPLICATION_NAME_PREFIX: &str = concat!("tokeira/", env!("CARGO_PKG_VERSION"));

/// The longest one connection attempt may take, from loading credentials to the
/// end of the database handshake.
///
/// Neither sqlx nor the connector bounds a connect, so a peer that accepts TCP
/// and never answers would hold the attempt forever. Thirty seconds covers a
/// credentials refresh plus a TLS handshake to a loaded cluster; a hung attempt
/// then holds one of the refillers for at most that long while the others keep
/// filling.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Credentials are replaced this long before they expire. A token signed with
/// credentials that lapse before the handshake reaches the cluster is refused;
/// five minutes leaves room for a slow connect and for clock skew.
const CREDENTIALS_REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

const DSQL_PORT: u16 = 5432;
const DSQL_USER: &str = "admin";
const DSQL_DATABASE: &str = "postgres";

/// Creates IAM-authenticated connections from endpoint and region configuration.
#[derive(Clone, Debug)]
pub struct ConnectionFactory {
    /// Endpoint, user and database; the connector adds the token, TLS mode and
    /// `application_name` on every connect.
    pg_options: PgConnectOptions,
    region: String,
    credentials: Arc<CredentialCache>,
}

impl ConnectionFactory {
    /// Build connector options for the DSQL admin database.
    ///
    /// Aurora DSQL uses IAM authentication through the connector, not a stored
    /// password. Nothing is loaded here: the AWS credentials chain is built on
    /// the first connect, so constructing a factory needs no AWS environment.
    pub fn new(endpoint: &str, region: &str) -> Result<Self> {
        let factory = Self {
            pg_options: PgConnectOptions::new()
                .host(endpoint)
                .port(DSQL_PORT)
                .username(DSQL_USER)
                .database(DSQL_DATABASE)
                .ssl_mode(PgSslMode::VerifyFull),
            region: region.to_owned(),
            credentials: Arc::new(CredentialCache::default()),
        };
        // The connector validates the endpoint when its options are built;
        // build them once now so a bad endpoint fails at startup, not at the
        // first refill.
        factory.connect_options(None)?;
        Ok(factory)
    }

    /// Mint an IAM token and open a TLS connection, preserving the failure category.
    pub async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        let credentials = self.credentials.current(&self.region).await?;
        let options = self
            .connect_options(Some(credentials))
            .map_err(|error| ConnectionFactoryError::Config(error.to_string()))?;
        // Use the connector entry point directly. `sqlx::PgConnection` cannot
        // mint DSQL IAM tokens on its own.
        aurora_dsql_sqlx_connector::connection::connect_with(&options)
            .await
            .map_err(ConnectionFactoryError::from_dsql_error)
    }

    /// Connector options that sign with `credentials`. A static provider over
    /// already-fetched credentials makes the connector's signer use them as
    /// they are, instead of resolving a credentials chain of its own.
    fn connect_options(
        &self,
        credentials: Option<Credentials>,
    ) -> Result<DsqlConnectOptions, DsqlConnectOptionsBuilderFailure> {
        let mut builder = DsqlConnectOptionsBuilder::default();
        builder
            .pg_connect_options(self.pg_options.clone())
            .region(Region::new(self.region.clone()))
            .orm_prefix(APPLICATION_NAME_PREFIX.to_owned());
        if let Some(credentials) = credentials {
            builder.credentials_provider(SharedCredentialsProvider::new(credentials));
        }
        builder
            .build()
            .map_err(|error| DsqlConnectOptionsBuilderFailure(error.to_string()))
    }
}

/// The connector refused the options a factory built.
#[derive(Debug, Error)]
#[error("invalid DSQL connection options: {0}")]
pub struct DsqlConnectOptionsBuilderFailure(String);

/// The credentials every connect signs its token with, shared by all of a
/// factory's clones.
#[derive(Debug, Default)]
struct CredentialCache {
    /// The default AWS credentials chain, built once: building it reads the
    /// environment and profile files.
    chain: OnceCell<Option<SharedCredentialsProvider>>,
    /// The credentials last fetched from the chain. The mutex also makes one
    /// connect fetch while concurrent connects wait for its result, rather than
    /// every refiller fetching at once when the credentials near expiry.
    current: Mutex<Option<Credentials>>,
}

impl CredentialCache {
    async fn current(&self, region: &str) -> Result<Credentials, ConnectionFactoryError> {
        let mut current = self.current.lock().await;
        if let Some(credentials) = current.as_ref()
            && usable_until_refresh(credentials, SystemTime::now())
        {
            return Ok(credentials.clone());
        }
        let chain = self
            .chain
            .get_or_init(|| async {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(Region::new(region.to_owned()))
                    .load()
                    .await
                    .credentials_provider()
            })
            .await
            .as_ref()
            .ok_or_else(|| {
                ConnectionFactoryError::Token(
                    "no AWS credentials provider is configured for DSQL IAM authentication"
                        .to_owned(),
                )
            })?;
        let credentials = chain
            .provide_credentials()
            .await
            .map_err(|error| ConnectionFactoryError::Token(error.to_string()))?;
        *current = Some(credentials.clone());
        Ok(credentials)
    }
}

/// Whether `credentials` can still sign tokens at `now`: they have no expiry
/// (long-term keys), or it is further away than the refresh margin.
fn usable_until_refresh(credentials: &Credentials, now: SystemTime) -> bool {
    credentials.expiry().is_none_or(|expiry| {
        expiry
            .duration_since(now)
            .is_ok_and(|left| left > CREDENTIALS_REFRESH_MARGIN)
    })
}

/// Open one connection through `factory`, giving up after [`CONNECT_TIMEOUT`].
///
/// The bound lives here rather than in each factory so every reservoir's
/// refillers apply it to whichever factory they were given.
pub(crate) async fn create_within_timeout(
    factory: &dyn PhysicalConnectionFactory,
) -> Result<PgConnection, ConnectionFactoryError> {
    tokio::time::timeout(CONNECT_TIMEOUT, factory.create_connection())
        .await
        .unwrap_or(Err(ConnectionFactoryError::Timeout(CONNECT_TIMEOUT)))
}

/// Stable categories for failures while opening a physical DSQL connection.
#[derive(Debug, Error)]
pub enum ConnectionFactoryError {
    /// Invalid connection configuration.
    #[error("DSQL connection configuration failed: {0}")]
    Config(String),
    /// IAM token generation failed before connecting.
    #[error("DSQL IAM token generation failed: {0}")]
    Token(String),
    /// The TCP or TLS connection could not be established.
    #[error("DSQL TCP/TLS connection failed: {0}")]
    Connection(String),
    /// The database rejected the connection handshake.
    #[error("DSQL database handshake failed: {0}")]
    Database(String),
    /// The attempt did not finish within the connect timeout.
    #[error("DSQL connection attempt did not finish within {0:?}")]
    Timeout(Duration),
}

impl ConnectionFactoryError {
    /// Preserve the connector's failure classes for metrics and operator
    /// messages. Flattening everything into `sqlx::Error` would hide whether
    /// the failure came from local configuration, IAM token generation, TCP/TLS,
    /// or the database handshake.
    pub fn from_dsql_error(error: DsqlError) -> Self {
        match error {
            DsqlError::ConfigError(error) => Self::Config(error.to_string()),
            DsqlError::TokenError(error) => Self::Token(error.to_string()),
            DsqlError::ConnectionError(error) => Self::Connection(error.to_string()),
            DsqlError::DatabaseError(error) => Self::Database(error.to_string()),
            // Compiler-required: DsqlError is #[non_exhaustive] (connector
            // 0.2.2, src/error.rs). At our exact =0.2.2 pin and feature set no
            // value can reach this arm. A connector pin move is a reviewed
            // dependency change whose review must revisit this mapping.
            error => Self::Connection(error.to_string()),
        }
    }

    /// Return the bounded label used by connection-failure metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Token(_) => "token",
            Self::Connection(_) => "connection",
            Self::Database(_) => "database",
            Self::Timeout(_) => "timeout",
        }
    }
}

#[async_trait]
pub trait PhysicalConnectionFactory: std::fmt::Debug + Send + Sync {
    async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError>;
}

#[async_trait]
impl PhysicalConnectionFactory for ConnectionFactory {
    async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        ConnectionFactory::create_connection(self).await
    }
}

#[cfg(any(test, feature = "dsql-integration"))]
#[derive(Clone, Debug)]
pub struct DatabaseUrlConnectionFactory {
    url: String,
}

#[cfg(any(test, feature = "dsql-integration"))]
impl DatabaseUrlConnectionFactory {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[cfg(any(test, feature = "dsql-integration"))]
#[async_trait]
impl PhysicalConnectionFactory for DatabaseUrlConnectionFactory {
    async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        <PgConnection as sqlx::Connection>::connect(&self.url)
            .await
            .map_err(|error| ConnectionFactoryError::Connection(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        time::{Duration, SystemTime},
    };

    use aurora_dsql_sqlx_connector::DsqlError;
    use aws_sdk_dynamodb::config::Credentials;
    use proptest::prelude::*;

    use super::{
        APPLICATION_NAME_PREFIX, CREDENTIALS_REFRESH_MARGIN, ConnectionFactory,
        ConnectionFactoryError, usable_until_refresh,
    };

    // The prefix travels as a query value, where `+` decodes to a space and `&`
    // ends it, and PostgreSQL truncates `application_name` past 63 bytes.
    #[test]
    fn application_name_survives_the_connection_string_and_the_server_limit() {
        assert!(
            APPLICATION_NAME_PREFIX
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"./-_".contains(&byte)),
            "{APPLICATION_NAME_PREFIX}"
        );
        let connector_name = ":aurora-dsql-rust-sqlx/0.2.2";
        assert!(APPLICATION_NAME_PREFIX.len() + connector_name.len() <= 63);
        assert!(ConnectionFactory::new("example.dsql.us-east-1.on.aws", "us-east-1").is_ok());
    }

    #[test]
    fn a_bad_endpoint_fails_when_the_factory_is_built() {
        assert!(ConnectionFactory::new("", "us-east-1").is_err());
    }

    #[test]
    fn credentials_are_refreshed_before_they_expire() {
        let now = SystemTime::now();
        let expiring = |left: Duration| {
            Credentials::new(
                "key",
                "secret",
                Some("session".into()),
                Some(now + left),
                "test",
            )
        };
        assert!(usable_until_refresh(
            &Credentials::new("key", "secret", None, None, "test"),
            now
        ));
        assert!(usable_until_refresh(
            &expiring(CREDENTIALS_REFRESH_MARGIN + Duration::from_secs(1)),
            now
        ));
        assert!(!usable_until_refresh(
            &expiring(CREDENTIALS_REFRESH_MARGIN),
            now
        ));
        assert!(!usable_until_refresh(
            &Credentials::new(
                "key",
                "secret",
                None,
                Some(now - Duration::from_secs(1)),
                "test"
            ),
            now
        ));
    }

    #[test]
    fn dsql_error_classification_returns_stable_categories() {
        let cases = [
            (
                DsqlError::ConfigError(Box::new(io::Error::other("bad config"))),
                "config",
            ),
            (
                DsqlError::TokenError(Box::new(io::Error::other("bad token"))),
                "token",
            ),
            (
                DsqlError::ConnectionError(sqlx::Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timeout",
                ))),
                "connection",
            ),
            (
                DsqlError::DatabaseError(sqlx::Error::Protocol("db".to_owned())),
                "database",
            ),
        ];

        for (error, category) in cases {
            assert_eq!(
                ConnectionFactoryError::from_dsql_error(error).kind(),
                category
            );
        }
        assert_eq!(
            ConnectionFactoryError::Timeout(Duration::from_secs(1)).kind(),
            "timeout"
        );
    }

    proptest! {
        // Feature: dsql-connector-sqlx-09, Property 3: payloads cannot change failure categories.
        #[test]
        fn dsql_error_categories_are_message_independent(message in any::<String>()) {
            let errors = [
                DsqlError::ConfigError(Box::new(io::Error::other(message.clone()))),
                DsqlError::TokenError(Box::new(io::Error::other(message.clone()))),
                DsqlError::ConnectionError(sqlx::Error::Io(io::Error::other(message.clone()))),
                DsqlError::DatabaseError(sqlx::Error::Protocol(message)),
            ];
            let categories = errors.map(|error| ConnectionFactoryError::from_dsql_error(error).kind());
            prop_assert_eq!(categories, ["config", "token", "connection", "database"]);
        }
    }
}
