//! Aurora DSQL storage foundation.
//!
//! This module contains the schema/migration and connection-management
//! primitives used by the production DSQL backend. The public entry point is
//! [`DsqlStore`], which wires a connection director, migration runner, and
//! DSQL `RunRepository` over the same raw-connection reservoir foundation.
//!
//! Two external systems are involved before any SQL work can run:
//! DynamoDB coordinates cluster-wide connection creation and slot ownership,
//! while Aurora DSQL owns the workflow data itself. Startup intentionally
//! validates both DynamoDB coordination tables before warming connections so a
//! misprovisioned deployment fails before serving traffic.

use std::sync::Arc;

pub mod aws_http;
pub mod codec;
pub mod config;
pub mod connection;
pub mod connection_factory;
pub(crate) mod convert;
pub mod distributed_bucket;
pub mod migration;
pub mod projection_log;
pub mod reservoir;
pub mod run_repository;
pub mod slot_block_manager;
pub mod validation;
pub mod worker_deployment_repository;

pub use aws_http::offline_ddb_client;
pub use config::*;
pub use connection::*;
pub use connection_factory::*;
pub use distributed_bucket::*;
pub use migration::*;
pub use projection_log::*;
pub use reservoir::*;
pub use run_repository::*;
pub use slot_block_manager::*;
pub use validation::*;
pub use worker_deployment_repository::*;

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
    /// Worker Deployment registry repository backed by DSQL.
    worker_deployment_repository: worker_deployment_repository::DsqlWorkerDeploymentRepository,
}

impl DsqlStore {
    /// Construct the foundational DSQL components from IAM auth settings.
    ///
    /// The full startup sequence is internal: DynamoDB coordination validation,
    /// slot-block ownership, distributed rate limiting, and reservoir warmup.
    pub async fn connect(
        auth: config::DsqlAuthConfig,
        config: config::DsqlPoolConfig,
        ddb_client: aws_sdk_dynamodb::Client,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        auth.validate()?;
        let region = auth.resolved_region().ok_or_else(|| {
            anyhow::anyhow!("dsql region must be configured or derivable from endpoint")
        })?;
        // The Aurora connector owns IAM token generation. Tokeira keeps the
        // boundary at "raw PgConnection factory" so token caching/refresh
        // details never leak into repository code.
        let factory = Arc::new(connection_factory::ConnectionFactory::new(
            &auth.endpoint,
            &region,
        )?);
        // DynamoDB coordination is mandatory in production DSQL mode. There is
        // no local fallback here because uncoordinated refillers can overshoot
        // DSQL connection limits when more than one node starts. The client is
        // injected by the caller (a runtime resource, not config) so config
        // defaults never construct an SDK/TLS stack.
        let bucket = Arc::new(distributed_bucket::DistributedTokenBucket::new(
            ddb_client.clone(),
            config.coordination.rate_limiter_table.clone(),
            auth.endpoint.clone(),
            config.connection_rate_per_second,
            config.burst_capacity,
        ));
        bucket.validate_table().await?;
        let slot_manager = slot_block_manager::SlotBlockManager::start(
            ddb_client,
            config.coordination.conn_lease_table.clone(),
            auth.endpoint.clone(),
        )
        .await?;
        let reservoir = reservoir::Reservoir::start(
            config.reservoir.clone(),
            factory,
            bucket,
            Arc::clone(&slot_manager),
        )
        .await?;
        Self::from_reservoir(reservoir, config).await
    }

    /// Construct the foundational DSQL components from a database URL for tests.
    ///
    /// This path deliberately bypasses IAM and DynamoDB so unit/integration
    /// tests can exercise repository behavior against a local PostgreSQL-like
    /// endpoint without requiring AWS credentials.
    #[cfg(any(test, feature = "dsql-integration"))]
    pub async fn from_database_url_for_tests(
        url: impl Into<String>,
        config: config::DsqlPoolConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let factory = Arc::new(connection_factory::DatabaseUrlConnectionFactory::new(url));
        let bucket = Arc::new(distributed_bucket::DistributedTokenBucket::local_for_tests(
            config.connection_rate_per_second,
            config.burst_capacity,
        ));
        let slot_manager =
            slot_block_manager::SlotBlockManager::local_for_tests(config.reservoir.target_ready);
        let reservoir = reservoir::Reservoir::start(
            config.reservoir.clone(),
            factory,
            bucket,
            Arc::clone(&slot_manager),
        )
        .await?;
        Self::from_reservoir(reservoir, config).await
    }

    async fn from_reservoir(
        reservoir: reservoir::Reservoir,
        config: config::DsqlPoolConfig,
    ) -> anyhow::Result<Self> {
        // One `Arc<DsqlConnectionDirector>` is shared by all DSQL surfaces so
        // class budgets and reservoir state remain globally coordinated.
        let director = connection::DsqlConnectionDirector::start(config.clone(), reservoir)?;
        let director = Arc::new(director);
        let migration_runner = migration::MigrationRunner::new(config.migration);
        let run_repository = run_repository::DsqlRunRepository::new(
            Arc::clone(&director),
            config.shard_count,
            config.projection_partition_count,
            config.conflict_policy,
            config.lease_duration,
        )?;
        let projection_log = projection_log::DsqlProjectionLog::new(Arc::clone(&director));
        let worker_deployment_repository =
            worker_deployment_repository::DsqlWorkerDeploymentRepository::new(Arc::clone(
                &director,
            ));
        Ok(Self {
            director,
            migration_runner,
            projection_log,
            run_repository,
            worker_deployment_repository,
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

    /// Access the DSQL-backed Worker Deployment registry repository.
    pub fn worker_deployment_repository(
        &self,
    ) -> &worker_deployment_repository::DsqlWorkerDeploymentRepository {
        &self.worker_deployment_repository
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
        worker_deployment_repository::DsqlWorkerDeploymentRepository,
        migration::MigrationRunner,
    ) {
        (
            self.director,
            self.run_repository,
            self.projection_log,
            self.worker_deployment_repository,
            self.migration_runner,
        )
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.director.shutdown().await
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

        assert!(
            DsqlStore::connect(auth, config, aws_http::offline_ddb_client())
                .await
                .is_err()
        );
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

        assert!(
            DsqlStore::connect(auth, config, aws_http::offline_ddb_client())
                .await
                .is_err()
        );
    }
}
