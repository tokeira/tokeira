//! Aurora DSQL storage foundation.
//!
//! This module contains the schema/migration and connection-management
//! primitives used by the production DSQL backend. The public entry point is
//! [`DsqlStore`], which wires a connection director, migration runner, and
//! DSQL `RunRepository` over the same physical pool/reservoir foundation.

use std::sync::Arc;

pub mod codec;
pub mod config;
pub mod connection;
pub(crate) mod convert;
pub mod migration;
pub mod projection_log;
pub mod rate_limiter;
pub mod reservoir;
pub mod run_repository;
pub mod validation;

pub use config::*;
pub use connection::*;
pub use migration::*;
pub use projection_log::*;
pub use rate_limiter::*;
pub use reservoir::*;
pub use run_repository::*;
pub use validation::*;

/// Production DSQL storage foundation.
#[derive(Debug)]
pub struct DsqlStore {
    /// Shared connection admission and reservoir controller.
    director: Arc<connection::DsqlConnectionDirector>,
    /// Forward-only schema migration runner.
    migration_runner: migration::MigrationRunner,
    /// Projection log reader used by projection workers.
    projection_log: projection_log::DsqlProjectionLog,
    /// Semantic run repository backed by DSQL tables.
    run_repository: run_repository::DsqlRunRepository,
}

impl DsqlStore {
    /// Construct the foundational DSQL components from IAM auth settings.
    ///
    /// Uses `DsqlRole::Runtime` for the connection pool. Migration connections
    /// should use a separate `DsqlConnector` with `DsqlRole::Admin`.
    pub async fn connect(
        auth: config::DsqlAuthConfig,
        config: config::DsqlPoolConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        auth.validate()?;
        let connector =
            connection::DsqlConnector::connect(&auth, &config, config::DsqlRole::Runtime).await?;
        Self::from_connector(connector, config).await
    }

    /// Construct the foundational DSQL components from an existing SQLx pool.
    pub async fn from_pool(
        pool: sqlx::PgPool,
        config: config::DsqlPoolConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let connector = connection::DsqlConnector::new(pool);
        Self::from_connector(connector, config).await
    }

    async fn from_connector(
        connector: connection::DsqlConnector,
        config: config::DsqlPoolConfig,
    ) -> anyhow::Result<Self> {
        // One `Arc<DsqlConnectionDirector>` is shared by all DSQL surfaces so
        // class budgets and reservoir state remain globally coordinated.
        let director = connection::DsqlConnectionDirector::start(config.clone(), connector).await?;
        let director = Arc::new(director);
        let migration_runner = migration::MigrationRunner::new(config.migration);
        let run_repository = run_repository::DsqlRunRepository::new(
            Arc::clone(&director),
            config.shard_count,
            config.conflict_policy,
            config.lease_duration,
        )?;
        let projection_log = projection_log::DsqlProjectionLog::new(Arc::clone(&director));
        Ok(Self {
            director,
            migration_runner,
            projection_log,
            run_repository,
        })
    }

    /// Access the migration runner for schema management.
    pub fn migration_runner(&self) -> &migration::MigrationRunner {
        &self.migration_runner
    }

    /// Access the connection director used by the DSQL backend.
    pub fn connection_director(&self) -> &connection::DsqlConnectionDirector {
        self.director.as_ref()
    }

    /// Clone the shared connection director handle.
    ///
    /// Tokeirad startup needs owned handles for runtime, projection, visibility,
    /// and schema surfaces that outlive the facade value. Cloning the `Arc`
    /// preserves one coordinated reservoir and rate-limiter state.
    pub fn connection_director_arc(&self) -> Arc<connection::DsqlConnectionDirector> {
        Arc::clone(&self.director)
    }

    /// Access the DSQL-backed run repository.
    pub fn run_repository(&self) -> &run_repository::DsqlRunRepository {
        &self.run_repository
    }

    /// Access the DSQL-backed projection log reader.
    pub fn projection_log(&self) -> &projection_log::DsqlProjectionLog {
        &self.projection_log
    }

    /// Decompose the facade into owned backend components.
    ///
    /// This is the production bootstrap escape hatch: each subsystem takes
    /// ownership of the handle it needs while all DSQL paths continue sharing
    /// the same connection director.
    pub fn into_parts(
        self,
    ) -> (
        Arc<connection::DsqlConnectionDirector>,
        run_repository::DsqlRunRepository,
        projection_log::DsqlProjectionLog,
        migration::MigrationRunner,
    ) {
        (
            self.director,
            self.run_repository,
            self.projection_log,
            self.migration_runner,
        )
    }
}

#[cfg(test)]
mod tests {
    use time::Duration;

    use super::*;

    #[tokio::test]
    async fn store_connect_rejects_invalid_config_before_network() {
        let auth = config::DsqlAuthConfig {
            endpoint: "cluster.dsql.us-east-1.on.aws".to_owned(),
            ..config::DsqlAuthConfig::default()
        };
        let config = config::DsqlPoolConfig {
            reservoir: config::ReservoirConfig {
                target_ready: 0,
                ..config::ReservoirConfig::default()
            },
            ..config::DsqlPoolConfig::default()
        };

        assert!(DsqlStore::connect(auth, config).await.is_err());
    }

    #[tokio::test]
    async fn store_connect_rejects_invalid_auth_before_network() {
        let auth = config::DsqlAuthConfig::default();
        let config = config::DsqlPoolConfig {
            reservoir: config::ReservoirConfig {
                base_lifetime: Duration::minutes(50),
                ..config::ReservoirConfig::default()
            },
            ..config::DsqlPoolConfig::default()
        };

        assert!(DsqlStore::connect(auth, config).await.is_err());
    }
}
