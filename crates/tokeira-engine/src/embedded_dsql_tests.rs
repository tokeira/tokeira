//! Offline regressions for the production embedded DSQL startup wiring.
//!
//! Only AWS observations, lifecycle time, and the database connection boundary are
//! substituted. Canonical validation, readiness, pool configuration, wake refresh,
//! and report construction use the same functions as a real engine generation.

use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Write},
    sync::{Arc, Mutex},
};
use tracing::instrument::WithSubscriber as _;

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

#[derive(Clone, Default)]
struct StartupLog(Arc<Mutex<Vec<u8>>>);

impl Write for StartupLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn phase_error<E: Into<anyhow::Error>>(
    phase: EmbeddedStartupPhase,
    error: E,
) -> EmbeddedEngineStartError {
    startup_phase(Instant::now() + StdDuration::from_secs(30), phase, async {
        Err::<(), _>(error)
    })
    .await
    .unwrap_err()
}

fn incompatible_schema() -> SchemaIncompatibility {
    SchemaIncompatibility {
        observed_version: Some(12),
        minimum_supported_version: 2,
        target_version: 9,
        maximum_readable_version: 10,
        category: SchemaIncompatibilityCategory::FutureSchema,
    }
}

#[tokio::test]
async fn acquire_busy_errors_preserve_owner_and_expiry_through_phase() {
    let expires_at = time::OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let busy = || ControlLeaseError::Busy {
        owner_id: "fake-owner-incarnation".to_owned(),
        expires_at,
    };
    let schema = phase_error(
        EmbeddedStartupPhase::Schema,
        schema_migration_acquire_error(busy()),
    )
    .await;
    assert!(matches!(schema,
        EmbeddedEngineStartError::SchemaMigrationLeaseHeld { owner_id, expires_at: actual }
        if owner_id == "fake-owner-incarnation" && actual == SystemTime::from(expires_at)));
    let owner = phase_error(EmbeddedStartupPhase::Ownership, owner_acquire_error(busy())).await;
    assert!(matches!(owner,
        EmbeddedEngineStartError::OwnerLeaseHeld { owner_id, expires_at: actual }
        if owner_id == "fake-owner-incarnation" && actual == SystemTime::from(expires_at)));
}

#[tokio::test]
async fn schema_decisions_preserve_versions_and_incompatibility_through_phase() {
    let required = embedded_schema_outcome(&SchemaDecision::MigrationRequired {
        current: 3,
        target: 9,
    })
    .unwrap_err();
    assert!(matches!(
        phase_error(EmbeddedStartupPhase::Schema, required).await,
        EmbeddedEngineStartError::SchemaMigrationRequired {
            current_version: 3,
            target_version: 9
        }
    ));
    let incompatible = incompatible_schema();
    let rejected =
        embedded_schema_outcome(&SchemaDecision::Reject(incompatible.clone())).unwrap_err();
    assert!(
        matches!(phase_error(EmbeddedStartupPhase::Schema, rejected).await,
        EmbeddedEngineStartError::SchemaIncompatible(actual) if actual == incompatible)
    );
}

#[tokio::test]
async fn owner_quiescence_distinguishes_remaining_wait_from_elapsed_deadline() {
    let now = Instant::now();
    let wait = StdDuration::from_secs(60);
    let error = check_owner_quiescence(now + wait, now + StdDuration::from_secs(30)).unwrap_err();
    let error = phase_error(EmbeddedStartupPhase::Ownership, error).await;
    let EmbeddedEngineStartError::OwnerQuiescenceExceedsDeadline { remaining } = error else {
        panic!("expected the remaining quiescence wait");
    };
    assert!(remaining <= wait);
    assert!(wait - remaining < StdDuration::from_millis(100));

    let elapsed = check_owner_quiescence(now + wait, now - StdDuration::from_secs(1)).unwrap_err();
    assert!(matches!(
        phase_error(EmbeddedStartupPhase::Ownership, elapsed).await,
        EmbeddedEngineStartError::DeadlineExceeded {
            phase: EmbeddedStartupPhase::Ownership
        }
    ));
    assert!(check_owner_quiescence(now + wait, now + wait).is_ok());
}

#[tokio::test]
async fn deadline_causes_retain_the_active_phase() {
    for phase in [
        EmbeddedStartupPhase::Schema,
        EmbeddedStartupPhase::Ownership,
    ] {
        let error = if phase == EmbeddedStartupPhase::Schema {
            schema_migration_acquire_error(ControlLeaseError::DeadlineElapsed)
        } else {
            owner_acquire_error(ControlLeaseError::DeadlineElapsed)
        };
        assert!(matches!(phase_error(phase, error).await,
            EmbeddedEngineStartError::DeadlineExceeded { phase: actual } if actual == phase));
    }
    for phase in [
        EmbeddedStartupPhase::ClusterResolution,
        EmbeddedStartupPhase::ConnectionWarmup,
    ] {
        assert!(
            matches!(phase_error(phase, ManagedDsqlError::DeadlineExceeded).await,
            EmbeddedEngineStartError::DeadlineExceeded { phase: actual } if actual == phase)
        );
    }
}

#[tokio::test]
async fn phase_failures_log_once_and_redact_outer_messages() {
    let canaries = [
        "password=fake-password-canary",
        "token=fake-token-canary",
        "postgres://user:pass@host/db",
    ];
    for (error, expected) in [
        (anyhow!("index build failed"), "index build failed"),
        (anyhow!(canaries[0]), "[redacted]"),
        (anyhow!(canaries[1]), "[redacted]"),
        (anyhow!(canaries[2]), "[redacted]"),
        (
            anyhow!(canaries.join(" ")).context("outer operation failed"),
            "outer operation failed",
        ),
    ] {
        let log = StartupLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        let error = phase_error(EmbeddedStartupPhase::Schema, error)
            .with_subscriber(subscriber)
            .await;
        assert!(matches!(
            error,
            EmbeddedEngineStartError::Phase {
                phase: EmbeddedStartupPhase::Schema
            }
        ));
        let output = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        let events = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["level"], "ERROR");
        assert_eq!(events[0]["fields"]["phase"], "schema compatibility");
        assert_eq!(events[0]["fields"]["cause"], expected);
        for canary in canaries {
            assert!(!output.contains(canary));
            assert!(!error.to_string().contains(canary));
            assert!(!format!("{error:?}").contains(canary));
        }
        assert!(std::error::Error::source(&error).is_none());
    }
}

#[tokio::test]
async fn stack_and_fixed_reason_failures_log_once_across_phase_wrapping() {
    for fixed_reason in [false, true] {
        let log = StartupLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        async {
            let error = if fixed_reason {
                startup_phase_failure(EmbeddedStartupPhase::RuntimeRestore, "ownership lost")
            } else {
                embedded_stack_error(anyhow!("stack construction failed"))
            };
            let error = phase_error(EmbeddedStartupPhase::RuntimeRestore, error).await;
            assert!(matches!(
                error,
                EmbeddedEngineStartError::Phase {
                    phase: EmbeddedStartupPhase::RuntimeRestore
                }
            ));
        }
        .with_subscriber(subscriber)
        .await;
        let output = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        let events = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["level"], "ERROR");
        assert_eq!(events[0]["fields"]["phase"], "runtime restoration");
        assert_eq!(
            events[0]["fields"]["cause"],
            if fixed_reason {
                "ownership lost"
            } else {
                "stack construction failed"
            }
        );
    }
}

#[test]
fn startup_error_display_names_conditions_and_remedies() {
    let expires_at = SystemTime::UNIX_EPOCH + StdDuration::from_secs(1_800_000_000);
    for (error, condition) in [
        (
            EmbeddedEngineStartError::OwnerLeaseHeld {
                owner_id: "fake-owner".to_owned(),
                expires_at,
            },
            "embedded owner lease",
        ),
        (
            EmbeddedEngineStartError::SchemaMigrationLeaseHeld {
                owner_id: "fake-owner".to_owned(),
                expires_at,
            },
            "schema migration lease",
        ),
    ] {
        let message = error.to_string();
        for expected in [
            condition,
            "held by fake-owner",
            "2027-01-15T08:00:00Z",
            "retry after",
        ] {
            assert!(message.contains(expected), "missing {expected}");
        }
    }
    let required = EmbeddedEngineStartError::SchemaMigrationRequired {
        current_version: 3,
        target_version: 9,
    }
    .to_string();
    for expected in [
        "migration required",
        "V3",
        "V9",
        "validate-only",
        "unchanged",
        "automatic",
    ] {
        assert!(required.contains(expected), "missing {expected}");
    }
    let incompatible =
        EmbeddedEngineStartError::SchemaIncompatible(incompatible_schema()).to_string();
    for expected in [
        "schema incompatible",
        "observed V12",
        "allowed V2..=V10",
        "target V9",
        "FutureSchema",
        "compatible release",
    ] {
        assert!(incompatible.contains(expected), "missing {expected}");
    }
    let quiescence = EmbeddedEngineStartError::OwnerQuiescenceExceedsDeadline {
        remaining: StdDuration::from_millis(1250),
    }
    .to_string();
    for expected in [
        "quiescence exceeds",
        "1.250 seconds remaining",
        "left to expire",
        "full 300-second quiescence again",
        "startup budget",
    ] {
        assert!(quiescence.contains(expected), "missing {expected}");
    }
}

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 10
    // Both durable bootstrap paths retain exclusive mode; the common embedded
    // router stays offer-free with a listener. Database/ownership integration
    // remains covered by the existing durable suite, not this offline adapter.
    #[test]
    fn generated_durable_peer_exemption(managed in any::<bool>(), listener in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let temporary = tempfile::tempdir().unwrap();
            let mut config = existing_config(PUBLIC);
            if managed {
                config.storage = EmbeddedStorageConfig::ManagedDsql(ManagedEmbeddedDsqlConfig {
                    intent: ManagedClusterIntent::CreateOrRecover,
                    descriptor_path: temporary.path().join("cluster.json"),
                    region: config.server.infrastructure.dsql.region.clone().unwrap_or_else(|| "eu-west-2".into()),
                    migration_policy: None, limits: EmbeddedDsqlLimits::default(), tags: BTreeMap::new(),
                });
            }
            let mut control = FakeControl::new([observation(ClusterStatus::Active, PUBLIC), observation(ClusterStatus::Active, PUBLIC)]);
            control.allow_create = managed;
            let time = FakeTime::new();
            let connected = connect_embedded_dsql(config, control, time.clone(), time.now() + StdDuration::from_secs(60), capture_connection).await.unwrap();
            assert!(connected.server.infrastructure.placement.controller_endpoint.is_none());
            assert!(connected.server.infrastructure.peer.is_none());
            let engine = Engine::start_with_config(connected.server).await.unwrap();
            let error = engine.endpoint().call(InProcessGrpcRequest {
                service: "tokeira.internal.runtime.v1.WorkflowTaskDispatch".into(), rpc: "OfferWorkflowTask".into(), headers: Default::default(), proto: bytes::Bytes::new(),
            }).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unimplemented);
            if listener {
                let public = engine.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
                let mut client = tokeira_proto::internal::runtime::workflow_task_dispatch_client::WorkflowTaskDispatchClient::connect(format!("http://{}", public.bound_addr())).await.unwrap();
                assert_eq!(client.offer_workflow_task(tokeira_proto::internal::runtime::OfferWorkflowTaskRequest::default()).await.unwrap_err().code(), tonic::Code::Unimplemented);
                public.shutdown().await.unwrap();
            }
            engine.shutdown().await.unwrap();
        });
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
