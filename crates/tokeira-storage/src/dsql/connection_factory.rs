//! Raw DSQL connection creation.
//!
//! The rest of the storage layer deals only in `PgConnection`s. This module is
//! the single place that knows about `aurora_dsql_sqlx_connector`, which keeps
//! IAM-token generation and connector error taxonomy out of repository code.

use anyhow::Result;
use async_trait::async_trait;
use aurora_dsql_sqlx_connector::{DsqlConnectOptions, DsqlError};
use sqlx::PgConnection;
use thiserror::Error;

/// Creates IAM-authenticated connections from endpoint and region configuration.
#[derive(Clone, Debug)]
pub struct ConnectionFactory {
    options: DsqlConnectOptions,
}

impl ConnectionFactory {
    /// Build connector options for the DSQL admin database.
    ///
    /// Aurora DSQL uses IAM authentication through the connector, not a stored
    /// password in the URL. The URL therefore carries endpoint and region only.
    pub fn new(endpoint: &str, region: &str) -> Result<Self> {
        let connection_string = format!(
            "postgres://admin@{}:5432/postgres?region={}",
            endpoint, region
        );
        let options = DsqlConnectOptions::from_connection_string(&connection_string)?;
        Ok(Self { options })
    }

    /// Mint an IAM token and open a TLS connection, preserving the failure category.
    pub async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        // Use the connector entry point directly. `sqlx::PgConnection` cannot
        // mint DSQL IAM tokens on its own.
        aurora_dsql_sqlx_connector::connection::connect_with(&self.options)
            .await
            .map_err(ConnectionFactoryError::from_dsql_error)
    }
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
    use std::io;

    use aurora_dsql_sqlx_connector::DsqlError;
    use proptest::prelude::*;

    use super::ConnectionFactoryError;

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
