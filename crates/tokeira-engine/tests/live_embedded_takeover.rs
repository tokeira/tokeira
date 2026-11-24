#![cfg(feature = "dsql-integration")]

//! An embedded owner survives an expired takeover and keeps renewing while serving.
//! The live environment is the same as the embedded attribute-copy suite. The test
//! removes the owner claim, so the supplied cluster must have no other engine running.

use std::{path::Path, time::Duration};

use anyhow::{Context as _, Result, ensure};
use http::HeaderMap;
use prost::Message as _;
use tokeira_config::{
    DsqlMigrationPolicy, EmbeddedDsqlLimits, EmbeddedEngineConfig, EmbeddedStorageConfig,
    ExistingEmbeddedDsqlConfig,
};
use tokeira_engine::{Engine, InProcessGrpcRequest, TokeiraConfig};
use tokeira_proto::workflowservice::{GetSystemInfoRequest, GetSystemInfoResponse};
use tokeira_storage::dsql::{
    ControlLeaseAcquireOutcome, DsqlPoolConfig, DsqlStore, EXPIRED_TAKEOVER_QUIESCENCE,
    MigrationConfig,
    control_lease_state_for_tests::{claim, database_now, remove},
};
use tokio::time::{Instant, MissedTickBehavior};

const CLAIM_NAME: &str = "embedded-owner";

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
}

impl LiveCluster {
    fn config(&self) -> EmbeddedEngineConfig {
        EmbeddedEngineConfig {
            server: TokeiraConfig::default(),
            storage: EmbeddedStorageConfig::ExistingDsql(ExistingEmbeddedDsqlConfig {
                region: self.region.clone(),
                cluster_id: self.cluster_id.clone(),
                cluster_arn: self.cluster_arn.clone(),
                endpoint: self.endpoint.clone(),
                migration_policy: DsqlMigrationPolicy::ValidateOnly,
                limits: EmbeddedDsqlLimits::default(),
            }),
            startup_timeout_ms: 600_000,
        }
    }
}

#[tokio::test]
#[ignore = "requires a disposable live DSQL cluster; waits through lease expiry and five-minute quiescence"]
async fn embedded_expired_takeover_renews_through_quiescence_and_serving() -> Result<()> {
    let Some(cluster) = LiveCluster::from_env() else {
        return Ok(());
    };
    let pool = sqlx::PgPool::connect(&cluster.url).await?;
    let store = DsqlStore::from_database_url_for_tests(
        cluster.url.clone(),
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            ..DsqlPoolConfig::default()
        },
    )
    .await?;
    store.migration_runner().apply(&pool).await?;
    store.shutdown().await?;
    let mut connection = pool.acquire().await?;
    remove(&mut connection, CLAIM_NAME).await?;

    let engine_a = Engine::start_with_embedded_config(cluster.config()).await?;
    ensure!(
        engine_a
            .startup_report()
            .ownership
            .as_ref()
            .context("A has an owner report")?
            .outcome
            == ControlLeaseAcquireOutcome::Clean,
        "A must acquire a clean claim"
    );
    let owner_a = claim(&mut connection, CLAIM_NAME)
        .await?
        .context("A holds the claim")?;
    ensure!(owner_a.owner_id.is_some(), "A must own the claim");
    drop(engine_a);

    let mut expiry_checks = tokio::time::interval(Duration::from_secs(1));
    expiry_checks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            expiry_checks.tick().await;
            let current = claim(&mut connection, CLAIM_NAME)
                .await?
                .context("A's claim remains")?;
            ensure!(
                current.owner_id == owner_a.owner_id,
                "drop must not release A's claim"
            );
            ensure!(
                current.fence_token == owner_a.fence_token,
                "A's fence must remain"
            );
            if database_now(&mut connection).await? >= current.expires_at {
                break Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await
    .context("A's hold expires after drop without further renewals")??;

    let takeover_started = Instant::now();
    let engine_b = Engine::start_with_embedded_config(cluster.config()).await?;
    let serving_started = Instant::now();
    let report_b = engine_b
        .startup_report()
        .ownership
        .as_ref()
        .context("B has an owner report")?;
    ensure!(
        report_b.outcome == ControlLeaseAcquireOutcome::ExpiredTakeover,
        "B must take an expired claim"
    );
    ensure!(
        serving_started.duration_since(takeover_started)
            >= Duration::try_from(EXPIRED_TAKEOVER_QUIESCENCE)?,
        "B must wait out quiescence before returning"
    );
    let owner_b = claim(&mut connection, CLAIM_NAME)
        .await?
        .context("B holds the claim")?;
    ensure!(
        owner_b.owner_id.is_some() && owner_b.owner_id != owner_a.owner_id,
        "B must be a new owner"
    );
    ensure!(
        owner_b.fence_token == report_b.fence_token && owner_b.fence_token > owner_a.fence_token,
        "B's report must identify the takeover claim"
    );

    let mut calls = tokio::time::interval(Duration::from_secs(3));
    calls.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        calls.tick().await;
        let response = engine_b
            .endpoint()
            .call(InProcessGrpcRequest {
                service: "temporal.api.workflowservice.v1.WorkflowService".to_owned(),
                rpc: "GetSystemInfo".to_owned(),
                headers: HeaderMap::new(),
                proto: GetSystemInfoRequest::default().encode_to_vec().into(),
            })
            .await
            .context("B keeps admitting calls after takeover")?;
        ensure!(
            GetSystemInfoResponse::decode(response.proto.as_slice())?
                .capabilities
                .is_some(),
            "B serves the endpoint"
        );
        if serving_started.elapsed() >= Duration::from_secs(65) {
            break;
        }
    }
    let renewed = claim(&mut connection, CLAIM_NAME)
        .await?
        .context("B still holds the claim")?;
    ensure!(
        renewed.owner_id == owner_b.owner_id && renewed.fence_token == owner_b.fence_token,
        "B remains the owner after serving"
    );
    ensure!(
        renewed.expires_at > database_now(&mut connection).await?,
        "B's hold is unexpired"
    );
    engine_b.shutdown().await?;
    let released = claim(&mut connection, CLAIM_NAME)
        .await?
        .context("the released row remains")?;
    ensure!(released.owner_id.is_none(), "shutdown releases B's claim");
    drop(connection);
    pool.close().await;
    Ok(())
}
