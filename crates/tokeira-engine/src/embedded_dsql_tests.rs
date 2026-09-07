//! Offline regressions for the production embedded DSQL startup wiring.
//!
//! Only AWS observations, lifecycle time, and the database connection boundary are
//! substituted. Canonical validation, readiness, pool configuration, wake refresh,
//! and report construction use the same functions as a real engine generation.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use proptest::prelude::*;
use tokeira_config::{
    EmbeddedDsqlLimits, ExistingEmbeddedDsqlConfig, ManagedClusterIntent, ManagedEmbeddedDsqlConfig,
};
use tokeira_managed_dsql::{
    ClusterDescriptorState, ClusterDescriptorStore, ClusterDescriptorV1, ClusterObservation,
    ClusterStatus, CreateClusterRequest, DeleteClusterRequest, DescriptorError, DsqlClientToken,
    DsqlControlError, SetDeletionProtectionRequest,
};

use super::*;

const ID: &str = "abcdefghijklmnopqrstuv1234";
const ARN: &str = "arn:aws:dsql:eu-west-2:123456789012:cluster/abcdefghijklmnopqrstuv1234";
const PRIVATE: &str = "abcdefghijklmnopqrstuv1234.dsql-test.eu-west-2.on.aws";
const PUBLIC: &str = "abcdefghijklmnopqrstuv1234.dsql.eu-west-2.on.aws";

#[derive(Clone, Debug)]
struct FakeControl {
    observations: Arc<Mutex<VecDeque<ClusterObservation>>>,
    allow_create: bool,
}

impl FakeControl {
    fn new(observations: impl IntoIterator<Item = ClusterObservation>) -> Self {
        Self {
            observations: Arc::new(Mutex::new(observations.into_iter().collect())),
            allow_create: false,
        }
    }

    fn next(&self) -> ClusterObservation {
        self.observations.lock().unwrap().pop_front().unwrap()
    }

    fn assert_consumed(&self) {
        assert!(self.observations.lock().unwrap().is_empty());
    }
}

#[async_trait]
impl DsqlControlPlane for FakeControl {
    async fn create_cluster(
        &self,
        request: CreateClusterRequest,
    ) -> Result<ClusterObservation, DsqlControlError> {
        assert!(
            self.allow_create,
            "existing startup cannot create a cluster"
        );
        assert_eq!(request.region, "eu-west-2");
        assert!(request.deletion_protection_enabled);
        Ok(self.next())
    }

    async fn get_cluster(
        &self,
        region: &str,
        cluster_id: &str,
    ) -> Result<ClusterObservation, DsqlControlError> {
        assert_eq!(region, "eu-west-2");
        assert_eq!(cluster_id, ID);
        Ok(self.next())
    }

    async fn set_deletion_protection(
        &self,
        _: SetDeletionProtectionRequest,
    ) -> Result<ClusterObservation, DsqlControlError> {
        panic!("startup cannot change deletion protection")
    }

    async fn delete_cluster(
        &self,
        _: DeleteClusterRequest,
    ) -> Result<ClusterStatus, DsqlControlError> {
        panic!("startup cannot delete a cluster")
    }
}

#[derive(Clone, Debug)]
struct FakeTime(Arc<Mutex<Instant>>);

impl FakeTime {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Instant::now())))
    }
}

#[async_trait]
impl LifecycleEnvironment for FakeTime {
    fn now(&self) -> Instant {
        *self.0.lock().unwrap()
    }

    fn new_client_token(&self) -> Result<DsqlClientToken, DescriptorError> {
        DsqlClientToken::new("offline-endpoint-regression")
    }

    async fn sleep(&self, duration: StdDuration) {
        *self.0.lock().unwrap() += duration;
    }
}

fn observation(status: ClusterStatus, endpoint: &str) -> ClusterObservation {
    ClusterObservation {
        region: "eu-west-2".to_owned(),
        identifier: ID.to_owned(),
        arn: ARN.to_owned(),
        endpoint: endpoint.to_owned(),
        status,
        deletion_protection_enabled: true,
        multi_region: false,
    }
}

fn existing_config(endpoint: &str) -> EmbeddedEngineConfig {
    let mut config = EmbeddedEngineConfig {
        storage: EmbeddedStorageConfig::ExistingDsql(ExistingEmbeddedDsqlConfig {
            region: "eu-west-2".to_owned(),
            cluster_id: ID.to_owned(),
            cluster_arn: ARN.to_owned(),
            endpoint: endpoint.to_owned(),
            migration_policy: DsqlMigrationPolicy::ValidateOnly,
            limits: EmbeddedDsqlLimits::default(),
        }),
        ..EmbeddedEngineConfig::default()
    };
    config.server.infrastructure.dsql.endpoint = Some("ignored-server-locator".to_owned());
    config
}

async fn capture_connection(
    auth: DsqlAuthConfig,
    pool: EmbeddedDsqlPoolConfig,
    _: WarmupDeadline,
) -> Result<DsqlAuthConfig> {
    assert_eq!(auth.region.as_deref(), Some("eu-west-2"));
    assert_eq!(pool.max_connections, 8);
    assert_eq!(pool.concurrent_connection_creations, 2);
    Ok(auth)
}

async fn verify_existing_locator(endpoint: &str, ready_status: ClusterStatus) {
    let config = existing_config(endpoint);
    config.validate().unwrap();
    let wake_required = matches!(ready_status, ClusterStatus::Idle | ClusterStatus::Inactive);
    let mut observations = vec![
        observation(ClusterStatus::Updating, "resolution.dsql.eu-west-2.on.aws"),
        observation(ready_status, PUBLIC),
    ];
    if wake_required {
        observations.push(observation(
            ClusterStatus::Updating,
            "waking.dsql.eu-west-2.on.aws",
        ));
        observations.push(observation(
            ClusterStatus::Active,
            "awake.dsql.eu-west-2.on.aws",
        ));
    }
    let control = FakeControl::new(observations);
    let time = FakeTime::new();
    let deadline = time.now() + StdDuration::from_secs(60);
    let connected = connect_embedded_dsql(
        config.clone(),
        control.clone(),
        time.clone(),
        deadline,
        capture_connection,
    )
    .await
    .unwrap();
    assert_eq!(
        connected.store.endpoint, endpoint,
        "database/authentication boundary"
    );
    assert_eq!(connected.connection_endpoint, endpoint);
    assert_eq!(
        connected.server.infrastructure.dsql.endpoint.as_deref(),
        Some(endpoint)
    );
    assert_eq!(
        connected.cluster.endpoint, PUBLIC,
        "AWS observation remains separate"
    );
    assert_eq!(connected.wake_required, wake_required);
    let cluster = if wake_required {
        refresh_cluster_after_wake(
            &config.storage,
            control.clone(),
            time,
            connected.cluster,
            StartupDeadline::at(deadline),
            deadline,
        )
        .await
        .unwrap()
    } else {
        connected.cluster
    };
    let report = cluster_startup_report(&cluster, &connected.connection_endpoint);
    assert_eq!(report.endpoint, connected.store.endpoint);
    assert_eq!(report.endpoint, endpoint);
    assert_eq!(report.cluster_id, ID);
    assert_eq!(report.cluster_arn, ARN);
    assert_eq!(report.action, ClusterAction::Existing);
    control.assert_consumed();
}

#[tokio::test]
async fn existing_private_and_public_locators_survive_readiness_and_wake() {
    for endpoint in [PRIVATE, PUBLIC] {
        for status in [
            ClusterStatus::Active,
            ClusterStatus::Idle,
            ClusterStatus::Inactive,
        ] {
            verify_existing_locator(endpoint, status).await;
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Feature: managed-embedded-dsql, Property 5: AWS refresh cannot retarget an explicit locator.
    #[test]
    fn existing_locator_is_independent_of_aws_refresh(suffix in "[a-z0-9]{1,12}", status in 0u8..3) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let endpoint = format!("{ID}.dsql-{suffix}.eu-west-2.on.aws");
        let status = match status {
            0 => ClusterStatus::Active,
            1 => ClusterStatus::Idle,
            _ => ClusterStatus::Inactive,
        };
        runtime.block_on(verify_existing_locator(&endpoint, status));
    }
}

#[tokio::test]
async fn identity_disagreement_is_rejected_at_resolution_readiness_and_wake() {
    for stage in 0..3 {
        for field in 0..3 {
            let mut invalid = observation(ClusterStatus::Active, PUBLIC);
            match field {
                0 => invalid.identifier = "1234abcdefghijklmnopqrstuv".to_owned(),
                1 => invalid.arn = ARN.replace("123456789012", "000000000000"),
                _ => invalid.region = "eu-west-1".to_owned(),
            }
            let mut observations = Vec::new();
            if stage > 0 {
                observations.push(observation(
                    if stage == 1 {
                        ClusterStatus::Updating
                    } else {
                        ClusterStatus::Idle
                    },
                    PUBLIC,
                ));
            }
            observations.push(invalid);
            let control = FakeControl::new(observations);
            let time = FakeTime::new();
            let deadline = time.now() + StdDuration::from_secs(60);
            let config = existing_config(PRIVATE);
            let result = connect_embedded_dsql(
                config.clone(),
                control.clone(),
                time.clone(),
                deadline,
                |auth, pool, warmup| async move {
                    assert_eq!(
                        stage, 2,
                        "identity failure must precede any database connection"
                    );
                    capture_connection(auth, pool, warmup).await
                },
            )
            .await;
            let error = if stage == 2 {
                let connected = result.unwrap();
                refresh_cluster_after_wake(
                    &config.storage,
                    control.clone(),
                    time,
                    connected.cluster,
                    StartupDeadline::at(deadline),
                    deadline,
                )
                .await
                .unwrap_err()
            } else {
                result.unwrap_err()
            };
            assert!(matches!(error, EmbeddedEngineStartError::Phase { phase }
                if phase == if stage == 2 { EmbeddedStartupPhase::ConnectionWarmup }
                    else { EmbeddedStartupPhase::ClusterResolution }));
            control.assert_consumed();
        }
    }
}

#[tokio::test]
async fn invalid_existing_configuration_never_falls_back_to_memory() {
    for field in 0..5 {
        let mut config = existing_config(PRIVATE);
        let EmbeddedStorageConfig::ExistingDsql(existing) = &mut config.storage else {
            unreachable!()
        };
        match field {
            0 => existing.endpoint.clear(),
            1 => existing.endpoint = " \t".to_owned(),
            2 => existing.cluster_id.clear(),
            3 => existing.region.clear(),
            _ => existing.limits.max_connections = 0,
        }
        assert!(Engine::start_with_embedded_config(config).await.is_err());
    }
}

#[tokio::test]
async fn managed_create_and_recover_keep_aws_endpoint_refresh() {
    for recover in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "tokeira-endpoint-{}-{recover}.json",
            std::process::id()
        ));
        let descriptors = LocalClusterDescriptorStore::new(&path);
        if recover {
            let mut descriptor = ClusterDescriptorV1::pending(
                "eu-west-2",
                DsqlClientToken::new("offline-recovery").unwrap(),
            );
            descriptor.state = ClusterDescriptorState::Ready {
                cluster_id: ID.to_owned(),
                cluster_arn: ARN.to_owned(),
                endpoint: "old.dsql.eu-west-2.on.aws".to_owned(),
            };
            descriptors
                .compare_and_swap(None, &descriptor)
                .await
                .unwrap();
        }
        let config = EmbeddedEngineConfig {
            storage: EmbeddedStorageConfig::ManagedDsql(ManagedEmbeddedDsqlConfig {
                intent: ManagedClusterIntent::CreateOrRecover,
                descriptor_path: path.clone(),
                region: "eu-west-2".to_owned(),
                migration_policy: None,
                limits: EmbeddedDsqlLimits::default(),
                tags: BTreeMap::new(),
            }),
            ..EmbeddedEngineConfig::default()
        };
        let mut control = FakeControl::new([
            observation(ClusterStatus::Updating, "resolved.dsql.eu-west-2.on.aws"),
            observation(ClusterStatus::Idle, PUBLIC),
            observation(ClusterStatus::Active, "awake.dsql.eu-west-2.on.aws"),
        ]);
        control.allow_create = !recover;
        let time = FakeTime::new();
        let deadline = time.now() + StdDuration::from_secs(60);
        let connected = connect_embedded_dsql(
            config.clone(),
            control.clone(),
            time.clone(),
            deadline,
            capture_connection,
        )
        .await
        .unwrap();
        assert_eq!(connected.store.endpoint, PUBLIC);
        assert_eq!(connected.cluster.endpoint, PUBLIC);
        assert!(connected.wake_required);
        assert!(
            matches!(descriptors.load().await.unwrap().unwrap().into_v1().state,
            ClusterDescriptorState::Ready { endpoint, .. } if endpoint == "resolved.dsql.eu-west-2.on.aws")
        );
        let cluster = refresh_cluster_after_wake(
            &config.storage,
            control.clone(),
            time,
            connected.cluster,
            StartupDeadline::at(deadline),
            deadline,
        )
        .await
        .unwrap();
        assert_eq!(cluster.endpoint, "awake.dsql.eu-west-2.on.aws");
        let report = cluster_startup_report(&cluster, &connected.connection_endpoint);
        assert_eq!(report.endpoint, connected.store.endpoint);
        assert_eq!(
            report.action,
            if recover {
                ClusterAction::Recovered
            } else {
                ClusterAction::Created
            }
        );
        control.assert_consumed();
        std::fs::remove_file(path).unwrap();
    }
}
