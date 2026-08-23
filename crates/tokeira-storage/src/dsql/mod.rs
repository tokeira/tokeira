//! Aurora DSQL storage foundation.
//!
//! This module contains the schema/migration and connection-management
//! primitives used by the production DSQL backend. The public entry point is
//! [`DsqlStore`], which wires a connection director, migration runner, and
//! DSQL `RunRepository` over mode-specific connection foundations.
//!
//! Distributed startup retains its established DynamoDB-backed reservoir,
//! token bucket, and slot-block manager. Exclusive embedded startup uses a
//! separate bounded process-local reservoir and never constructs DynamoDB
//! resources; the repository implementations remain shared.

use std::{sync::Arc, time::Instant};

pub mod aws_http;
pub mod chasm_node;
pub mod codec;
pub mod config;
pub mod connection;
pub(crate) mod connection_coordinator;
pub mod connection_factory;
pub mod control_lease;
pub(crate) mod convert;
pub mod distributed_bucket;
pub(crate) mod embedded_reservoir;
pub mod migration;
pub mod projection_log;
pub mod reservoir;
pub mod run_repository;
pub mod schema_compatibility;
pub mod slot_block_manager;
pub mod task_queue_config;
pub mod validation;
pub mod worker_compute_repository;
pub mod worker_deployment_repository;
pub mod worker_task_provenance;

pub use aws_http::offline_ddb_client;
pub use chasm_node::*;
pub use config::*;
pub use connection::*;
pub use connection_factory::*;
pub use control_lease::*;
pub use distributed_bucket::*;
pub use embedded_reservoir::WarmupDeadline;
pub use migration::*;
pub use projection_log::*;
pub use reservoir::*;
pub use run_repository::*;
pub use schema_compatibility::*;
pub use slot_block_manager::*;
pub use task_queue_config::*;
pub use validation::*;
pub use worker_compute_repository::*;
pub use worker_deployment_repository::*;
pub use worker_task_provenance::*;

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

#[derive(Debug)]
struct StoreFoundationConfig {
    reservoir: config::ReservoirConfig,
    migration: config::MigrationConfig,
    shard_count: u32,
    projection_partition_count: u32,
    conflict_policy: crate::CurrentExecutionConflictPolicy,
    lease_duration: time::Duration,
}

impl From<config::DsqlPoolConfig> for StoreFoundationConfig {
    fn from(config: config::DsqlPoolConfig) -> Self {
        Self {
            reservoir: config.reservoir,
            migration: config.migration,
            shard_count: config.shard_count,
            projection_partition_count: config.projection_partition_count,
            conflict_policy: config.conflict_policy,
            lease_duration: config.lease_duration,
        }
    }
}

impl From<config::EmbeddedDsqlPoolConfig> for StoreFoundationConfig {
    fn from(config: config::EmbeddedDsqlPoolConfig) -> Self {
        Self {
            reservoir: config.reservoir_config(),
            migration: config.migration,
            shard_count: config.shard_count,
            projection_partition_count: config.projection_partition_count,
            conflict_policy: config.conflict_policy,
            lease_duration: config.lease_duration,
        }
    }
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
        Self::connect_distributed(auth, config, ddb_client).await
    }

    /// Construct the distributed DSQL foundation with DynamoDB coordination.
    pub async fn connect_distributed(
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

    /// Construct a DynamoDB-free DSQL foundation for one embedded engine process.
    ///
    /// The caller owns the end-to-end startup deadline. This path constructs
    /// only IAM authentication, the embedded raw-connection reservoir, five
    /// operation-class budgets, and process-local connection admission.
    pub async fn connect_embedded(
        auth: config::DsqlAuthConfig,
        config: config::EmbeddedDsqlPoolConfig,
        warmup_deadline: embedded_reservoir::WarmupDeadline,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        auth.validate()?;
        let region = auth.resolved_region().ok_or_else(|| {
            anyhow::anyhow!("dsql region must be configured or derivable from endpoint")
        })?;
        let factory = Arc::new(connection_factory::ConnectionFactory::new(
            &auth.endpoint,
            &region,
        )?);
        let reservoir_config = config.reservoir_config();
        let coordinator: Arc<dyn connection_coordinator::EmbeddedConnectionCoordinator> = Arc::new(
            connection_coordinator::ProcessLocalConnectionCoordinator::new(
                config.max_connections,
                config.connection_rate_per_second,
                config.connection_burst,
            )?,
        );
        let reservoir = embedded_reservoir::EmbeddedReservoir::start_with_deadline(
            reservoir_config,
            factory,
            coordinator,
            warmup_deadline.instant(),
        )
        .await?;
        Self::from_embedded_reservoir(reservoir, StoreFoundationConfig::from(config)).await
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
        Self::from_director(director, StoreFoundationConfig::from(config)).await
    }

    async fn from_embedded_reservoir(
        reservoir: embedded_reservoir::EmbeddedReservoir,
        config: StoreFoundationConfig,
    ) -> anyhow::Result<Self> {
        let director =
            connection::DsqlConnectionDirector::start_embedded(&config.reservoir, reservoir)?;
        Self::from_director(director, config).await
    }

    async fn from_director(
        director: connection::DsqlConnectionDirector,
        config: StoreFoundationConfig,
    ) -> anyhow::Result<Self> {
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

    /// Construct a DSQL-backed CHASM node store over the shared connection
    /// director. The repository is stateless (it only holds the director handle),
    /// so it is built on demand rather than stored as a facade field.
    pub fn chasm_node_repository(&self) -> chasm_node::DsqlChasmNodeRepository {
        chasm_node::DsqlChasmNodeRepository::new(Arc::clone(&self.director))
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

    /// Construct a task-queue policy repository over the shared director.
    ///
    /// Like the CHASM node repository, this facade is stateless beyond the
    /// director handle and therefore does not need another owned `DsqlStore`
    /// field or a wider `into_parts` tuple.
    pub fn task_queue_config_repository(&self) -> task_queue_config::DsqlTaskQueueConfigRepository {
        task_queue_config::DsqlTaskQueueConfigRepository::new(Arc::clone(&self.director))
    }

    /// Construct a scoped Worker task-provenance repository.
    pub fn worker_task_provenance_store(
        &self,
    ) -> worker_task_provenance::DsqlWorkerTaskProvenanceStore {
        worker_task_provenance::DsqlWorkerTaskProvenanceStore::new(Arc::clone(&self.director))
    }

    /// Construct a Worker Compute Controller repository over the shared director.
    pub fn worker_compute_repository(
        &self,
    ) -> worker_compute_repository::DsqlWorkerComputeRepository {
        worker_compute_repository::DsqlWorkerComputeRepository::new(Arc::clone(&self.director))
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

    /// Shut down embedded storage within the caller's remaining monotonic budget.
    pub async fn shutdown_with_deadline(&self, deadline: Instant) -> anyhow::Result<()> {
        self.director.shutdown_with_deadline(deadline).await
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
