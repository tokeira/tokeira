#![cfg(feature = "dsql-integration")]

//! Actionable startup failures against a new, empty disposable DSQL cluster.
//!
//! Uses the takeover suite's environment and existing-cluster configuration.
//! The test acquires and conditionally releases real control claims, initializes
//! the schema, and leaves the cluster intact with no engine running.

use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context as _, Result, bail, ensure};
use http::HeaderMap;
use prost::Message as _;
use tokeira_config::{
    DsqlMigrationPolicy, EmbeddedDsqlLimits, EmbeddedEngineConfig, EmbeddedStorageConfig,
    ExistingEmbeddedDsqlConfig,
};
use tokeira_engine::{EmbeddedEngineStartError, Engine, InProcessGrpcRequest, TokeiraConfig};
use tokeira_proto::workflowservice::{GetSystemInfoRequest, GetSystemInfoResponse};
use tokeira_storage::dsql::{
    ConnectionControlLeaseRepository, ControlLeaseAcquireRequest, ControlLeaseClusterIdentity,
    MigrationRunner, OWNER_ADMISSION_MARGIN, OwnershipAdmissionGate,
    SCHEMA_MIGRATION_ADMISSION_MARGIN, SCHEMA_MIGRATION_LEASE_DURATION,
    schema_state_for_tests::observe,
};

/// The test holds the owner claim across a whole engine start, so it holds it
/// for longer than an engine's own renewed lease.
const TEST_OWNER_HOLD: time::Duration = time::Duration::minutes(6);

/// A cold first migration on a new cluster can take longer than ten minutes.
const STARTUP_TIMEOUT_MS: u64 = 1_200_000;

struct LiveCluster {
    url: String,
    cluster_id: String,
    cluster_arn: String,
    endpoint: String,
    region: String,
}

impl LiveCluster {
    fn from_env() -> Option<Self> {
        let var = |name: &str| std::env::var(name).ok();
        Some(Self {
            url: var("TOKEIRA_DSQL_TEST_DATABASE_URL")?,
            cluster_id: var("TOKEIRA_DSQL_TEST_CLUSTER_ID")?,
            cluster_arn: var("TOKEIRA_DSQL_TEST_CLUSTER_ARN")?,
            endpoint: var("TOKEIRA_DSQL_TEST_ENDPOINT")?,
            region: var("TOKEIRA_DSQL_TEST_REGION")?,
        })
    }

    fn config(&self, migration_policy: DsqlMigrationPolicy) -> EmbeddedEngineConfig {
        EmbeddedEngineConfig {
            server: TokeiraConfig::default(),
            storage: EmbeddedStorageConfig::ExistingDsql(ExistingEmbeddedDsqlConfig {
                region: self.region.clone(),
                cluster_id: self.cluster_id.clone(),
                cluster_arn: self.cluster_arn.clone(),
                endpoint: self.endpoint.clone(),
                migration_policy,
                limits: EmbeddedDsqlLimits::default(),
            }),
            startup_timeout_ms: STARTUP_TIMEOUT_MS,
        }
    }

    fn identity(&self) -> ControlLeaseClusterIdentity {
        ControlLeaseClusterIdentity {
            cluster_id: self.cluster_id.clone(),
            cluster_arn: self.cluster_arn.clone(),
        }
    }
}

async fn refused_start(config: EmbeddedEngineConfig) -> Result<EmbeddedEngineStartError> {
    match Engine::start_with_embedded_config(config).await {
        Err(error) => Ok(error),
        Ok(engine) => {
            engine.shutdown().await?;
            bail!("startup unexpectedly succeeded");
        }
    }
}

#[tokio::test]
#[ignore = "requires a new, empty disposable DSQL cluster with no other engine running"]
async fn embedded_startup_errors_identify_schema_policy_and_held_claims() -> Result<()> {
    let Some(cluster) = LiveCluster::from_env() else {
        return Ok(());
    };
    let pool = sqlx::PgPool::connect(&cluster.url).await?;
    let mut connection = pool.acquire().await?;
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure!(tables == 0, "the fixture must be a new, empty cluster");
    let before = observe(&mut connection).await?;
    ensure!(before.applied_migrations.is_empty() && before.compatibility.is_none());

    let required = refused_start(cluster.config(DsqlMigrationPolicy::ValidateOnly)).await?;
    let contract = MigrationRunner::compatibility_contract();
    ensure!(
        matches!(required,
        EmbeddedEngineStartError::SchemaMigrationRequired { current_version: 0, target_version }
        if target_version == contract.target_version),
        "validate-only must report this release's migration target"
    );
    ensure!(
        observe(&mut connection).await? == before,
        "validate-only must leave the schema uninitialized"
    );
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure!(tables == 0, "validate-only must not create schema tables");

    let leases = ConnectionControlLeaseRepository::new();
    leases.bootstrap(&mut connection).await?;
    let migration_owner = "startup-errors-test-migration-owner";
    let migration_guard = leases
        .acquire(
            &mut connection,
            &ControlLeaseAcquireRequest {
                claim_name: "schema-migration".to_owned(),
                cluster: cluster.identity(),
                owner_id: migration_owner.to_owned(),
                lease_duration: SCHEMA_MIGRATION_LEASE_DURATION,
                admission_margin: SCHEMA_MIGRATION_ADMISSION_MARGIN,
                acquire_deadline: Instant::now() + Duration::from_secs(30),
            },
        )
        .await?;
    let migration_gate = OwnershipAdmissionGate::for_guard(&migration_guard);
    let busy = refused_start(cluster.config(DsqlMigrationPolicy::Automatic)).await;
    let released = leases
        .release(&mut connection, &migration_guard, &migration_gate)
        .await;
    released.context("release the test's schema-migration claim")?;
    ensure!(
        matches!(busy?,
        EmbeddedEngineStartError::SchemaMigrationLeaseHeld { owner_id, expires_at }
        if owner_id == migration_owner && expires_at > SystemTime::now()),
        "automatic startup must identify the live migration holder"
    );

    let engine =
        Engine::start_with_embedded_config(cluster.config(DsqlMigrationPolicy::Automatic)).await?;
    let serving = async {
        ensure!(
            engine
                .startup_report()
                .schema
                .as_ref()
                .context("schema report")?
                .observed_version
                == contract.target_version
        );
        let response = engine
            .endpoint()
            .call(InProcessGrpcRequest {
                service: "temporal.api.workflowservice.v1.WorkflowService".to_owned(),
                rpc: "GetSystemInfo".to_owned(),
                headers: HeaderMap::new(),
                proto: GetSystemInfoRequest::default().encode_to_vec().into(),
            })
            .await?;
        ensure!(
            GetSystemInfoResponse::decode(response.proto.as_slice())?
                .capabilities
                .is_some(),
            "the migrated engine serves requests"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let shutdown = engine.shutdown().await;
    serving?;
    shutdown?;

    leases.bootstrap(&mut connection).await?;
    let owner = "startup-errors-test-engine-owner";
    let owner_guard = leases
        .acquire(
            &mut connection,
            &ControlLeaseAcquireRequest {
                claim_name: "embedded-owner".to_owned(),
                cluster: cluster.identity(),
                owner_id: owner.to_owned(),
                lease_duration: TEST_OWNER_HOLD,
                admission_margin: OWNER_ADMISSION_MARGIN,
                acquire_deadline: Instant::now() + Duration::from_secs(30),
            },
        )
        .await?;
    let owner_gate = OwnershipAdmissionGate::for_guard(&owner_guard);
    let busy = refused_start(cluster.config(DsqlMigrationPolicy::ValidateOnly)).await;
    let released = leases
        .release(&mut connection, &owner_guard, &owner_gate)
        .await;
    released.context("release the test's embedded-owner claim")?;
    ensure!(
        matches!(busy?,
        EmbeddedEngineStartError::OwnerLeaseHeld { owner_id, expires_at }
        if owner_id == owner && expires_at > SystemTime::now()),
        "validate-only startup must identify the live embedded owner"
    );
    drop(connection);
    pool.close().await;
    Ok(())
}
