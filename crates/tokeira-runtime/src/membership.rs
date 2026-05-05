//! Runtime membership client primitives for placement-controller streams.

use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration as StdDuration,
};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_proto::controller::{
    self as proto, ConnectionBudgetDirective, LanePressure, RuntimeHeartbeat,
    RuntimeMembershipRequest, RuntimeRegistration, controller_directive::Directive,
    placement_controller_client::PlacementControllerClient,
    runtime_membership_request::Request as MembershipRequest,
};
use tokeira_storage::{LeaseOutcome, LeaseRepository};
use tokeira_types::{IncarnationId, NodeEndpoint, ShardEpoch, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{LaneHandle, RuntimeDrain, RuntimeDrainState, ShardOwner};

/// Runtime-side placement-controller stream configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipConfig {
    pub controller_endpoint: String,
    pub heartbeat_interval: StdDuration,
    pub reconnect_base_delay: StdDuration,
    pub reconnect_max_delay: StdDuration,
    pub node_id: IncarnationId,
    pub node_endpoint: NodeEndpoint,
    pub zone: Option<String>,
    pub version: String,
    pub build_id: String,
}

impl MembershipConfig {
    pub fn owner_identity(&self) -> String {
        self.node_id.to_string()
    }
}

/// Runtime-owned dependencies used by directive handling.
pub struct MembershipClient<R>
where
    R: LeaseRepository + 'static,
{
    config: MembershipConfig,
    leases: Arc<R>,
    shard_owner: Arc<RwLock<ShardOwner>>,
    drain: Arc<RuntimeDrain>,
    budget_applier: Arc<dyn ConnectionBudgetApplier>,
    last_budget_valid_until: Arc<Mutex<Option<prost_types::Timestamp>>>,
}

impl<R> Clone for MembershipClient<R>
where
    R: LeaseRepository + 'static,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            leases: Arc::clone(&self.leases),
            shard_owner: Arc::clone(&self.shard_owner),
            drain: Arc::clone(&self.drain),
            budget_applier: Arc::clone(&self.budget_applier),
            last_budget_valid_until: Arc::clone(&self.last_budget_valid_until),
        }
    }
}

impl<R> std::fmt::Debug for MembershipClient<R>
where
    R: LeaseRepository + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<R> MembershipClient<R>
where
    R: LeaseRepository + 'static,
{
    pub fn new(
        config: MembershipConfig,
        leases: Arc<R>,
        shard_owner: Arc<RwLock<ShardOwner>>,
        drain: Arc<RuntimeDrain>,
        budget_applier: Arc<dyn ConnectionBudgetApplier>,
    ) -> Self {
        Self {
            config,
            leases,
            shard_owner,
            drain,
            budget_applier,
            last_budget_valid_until: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let mut backoff = self.config.reconnect_base_delay;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            match self.run_once(shutdown.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%error, "membership stream disconnected; reconnecting");
                    if self.last_budget_expired() {
                        self.budget_applier.apply_budget(1.0, 1, 1)?;
                    }
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(self.config.reconnect_max_delay);
        }
    }

    async fn run_once(&self, shutdown: CancellationToken) -> Result<()> {
        let mut client =
            PlacementControllerClient::connect(self.config.controller_endpoint.clone()).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(RuntimeMembershipRequest {
            request: Some(MembershipRequest::Registration(self.registration_message())),
        })
        .await
        .map_err(|_| anyhow!("membership request stream closed"))?;
        let response = client
            .runtime_membership(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?;
        let mut directives = response.into_inner();
        let heartbeat_tx = tx.clone();
        let heartbeat_client = self.clone();
        let heartbeat_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_client.config.heartbeat_interval);
            loop {
                tokio::select! {
                    _ = heartbeat_shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        if heartbeat_tx.send(RuntimeMembershipRequest {
                            request: Some(MembershipRequest::Heartbeat(heartbeat_client.heartbeat_message())),
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                directive = directives.message() => {
                    let Some(directive) = directive? else {
                        return Ok(());
                    };
                    self.handle_directive(directive).await?;
                }
            }
        }
    }

    pub fn registration_message(&self) -> RuntimeRegistration {
        RuntimeRegistration {
            node_id: self.config.node_id.to_string(),
            host: self.config.node_endpoint.host.clone(),
            port: u32::from(self.config.node_endpoint.port),
            zone: self.config.zone.clone().unwrap_or_default(),
            version: self.config.version.clone(),
            build_id: self.config.build_id.clone(),
        }
    }

    pub fn heartbeat_message(&self) -> RuntimeHeartbeat {
        self.heartbeat_message_with_inputs(HeartbeatInputs::from_shard_owner(
            &self.shard_owner.read().unwrap(),
            self.drain.state(),
        ))
    }

    pub fn heartbeat_message_with_inputs(&self, inputs: HeartbeatInputs) -> RuntimeHeartbeat {
        let drain_state = match inputs.drain_state {
            RuntimeDrainState::Active => proto::NodeDrainState::Active as i32,
            RuntimeDrainState::Draining => proto::NodeDrainState::Draining as i32,
            RuntimeDrainState::SafeToTerminate => proto::NodeDrainState::SafeToTerminate as i32,
        };
        RuntimeHeartbeat {
            owned_bundle_count: inputs.owned_bundles.len() as u32,
            owned_bundles: inputs.owned_bundles.into_iter().map(|id| id.0).collect(),
            runnable_transitions: inputs.runnable_transitions,
            active_actor_count: inputs.active_actor_count,
            backlog_depth: inputs.backlog_depth,
            available_connections: inputs.available_connections,
            connection_rate_headroom: inputs.connection_rate_headroom,
            drain_state,
            lane_pressures: inputs.lane_pressures,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatInputs {
    pub owned_bundles: Vec<ShardId>,
    pub runnable_transitions: u64,
    pub active_actor_count: u64,
    pub backlog_depth: u64,
    pub available_connections: u32,
    pub connection_rate_headroom: f32,
    pub drain_state: RuntimeDrainState,
    pub lane_pressures: Vec<LanePressure>,
}

impl HeartbeatInputs {
    pub fn from_shard_owner(owner: &ShardOwner, drain_state: RuntimeDrainState) -> Self {
        let owned_bundles = owner.owned_shards().collect::<Vec<_>>();
        Self {
            owned_bundles,
            runnable_transitions: 0,
            active_actor_count: 0,
            backlog_depth: 0,
            available_connections: 0,
            connection_rate_headroom: 0.0,
            drain_state,
            lane_pressures: Vec::new(),
        }
    }

    pub fn from_runtime_components(
        owner: &ShardOwner,
        drain_state: RuntimeDrainState,
        lanes: &[LaneHandle],
        available_connections: u32,
        connection_rate_headroom: f32,
    ) -> Self {
        let lane_pressures = lanes
            .iter()
            .enumerate()
            .map(|(lane_id, lane)| {
                let runnable_depth = lane.queued_depth() as u64;
                let utilization = if lane.queued_depth() == 0 { 0.0 } else { 1.0 };
                LanePressure {
                    lane_id: lane_id as u32,
                    runnable_depth,
                    active_actors: if runnable_depth > 0 { 1 } else { 0 },
                    utilization,
                }
            })
            .collect::<Vec<_>>();
        let runnable_transitions = lane_pressures
            .iter()
            .map(|pressure| pressure.runnable_depth)
            .sum();
        let active_actor_count = lane_pressures
            .iter()
            .map(|pressure| pressure.active_actors)
            .sum();
        Self {
            owned_bundles: owner.owned_shards().collect(),
            runnable_transitions,
            active_actor_count,
            backlog_depth: 0,
            available_connections,
            connection_rate_headroom,
            drain_state,
            lane_pressures,
        }
    }
}

impl<R> MembershipClient<R>
where
    R: LeaseRepository + 'static,
{
    pub async fn handle_directive(&self, directive: proto::ControllerDirective) -> Result<()> {
        match directive.directive {
            Some(Directive::DesiredPlacement(desired)) => {
                for bundle in desired.acquire_bundles {
                    let bundle = ShardId(bundle);
                    let outcome = self
                        .leases
                        .try_acquire_bundle(
                            bundle,
                            self.config.owner_identity(),
                            self.config.node_endpoint.as_authority(),
                        )
                        .await?;
                    if let LeaseOutcome::Acquired { epoch } = outcome {
                        self.shard_owner
                            .write()
                            .unwrap()
                            .record_acquired(bundle, epoch);
                    }
                }
                for bundle in desired.relinquish_bundles {
                    self.relinquish_owned_bundle(ShardId(bundle)).await?;
                }
            }
            Some(Directive::ConnectionBudget(budget)) => {
                self.apply_connection_budget(budget)?;
            }
            Some(Directive::Drain(_)) => {
                self.drain.begin();
                let bundles = self
                    .shard_owner
                    .read()
                    .unwrap()
                    .owned_shards()
                    .collect::<Vec<_>>();
                for bundle in bundles {
                    self.relinquish_owned_bundle(bundle).await?;
                }
                let owned_bundle_count = self.shard_owner.read().unwrap().owned_shards().count();
                self.drain.record_progress(owned_bundle_count, 0, 0);
            }
            Some(Directive::RoutingUpdate(_)) | None => {}
        }
        Ok(())
    }

    fn apply_connection_budget(&self, budget: ConnectionBudgetDirective) -> Result<()> {
        *self.last_budget_valid_until.lock().unwrap() = budget.valid_until;
        self.budget_applier.apply_budget(
            budget.rate_per_second,
            budget.capacity,
            budget.max_reservoir_size,
        )
    }

    pub fn last_budget_expired(&self) -> bool {
        budget_valid_until_expired(self.last_budget_valid_until.lock().unwrap().clone())
    }

    async fn relinquish_owned_bundle(&self, bundle: ShardId) -> Result<()> {
        let epoch = {
            let owner = self.shard_owner.read().unwrap();
            owner.epoch_of(bundle).unwrap_or(ShardEpoch::ZERO)
        };
        if epoch == ShardEpoch::ZERO {
            return Ok(());
        }
        let outcome = self
            .leases
            .relinquish_bundle(bundle, self.config.owner_identity(), epoch)
            .await?;
        if matches!(outcome, LeaseOutcome::Acquired { .. }) {
            let mut owner = self.shard_owner.write().unwrap();
            owner.mark_draining(bundle);
            owner.remove(bundle);
        }
        Ok(())
    }
}

/// Runtime boundary for controller-provided DSQL connection budgets.
pub trait ConnectionBudgetApplier: Send + Sync + std::fmt::Debug {
    fn apply_budget(
        &self,
        rate_per_second: f64,
        capacity: u64,
        max_reservoir_size: u32,
    ) -> Result<()>;
}

pub fn budget_valid_until_expired(valid_until: Option<prost_types::Timestamp>) -> bool {
    let Some(valid_until) = valid_until else {
        return false;
    };
    let Ok(deadline) = OffsetDateTime::from_unix_timestamp(valid_until.seconds) else {
        return true;
    };
    deadline < OffsetDateTime::now_utc()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokeira_storage::{InMemoryStore, LeaseRepository};

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingBudgetApplier {
        calls: Mutex<Vec<(f64, u64, u32)>>,
    }

    impl ConnectionBudgetApplier for RecordingBudgetApplier {
        fn apply_budget(
            &self,
            rate_per_second: f64,
            capacity: u64,
            max_reservoir_size: u32,
        ) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((rate_per_second, capacity, max_reservoir_size));
            Ok(())
        }
    }

    fn config() -> MembershipConfig {
        MembershipConfig {
            controller_endpoint: "http://127.0.0.1:7240".to_owned(),
            heartbeat_interval: StdDuration::from_millis(50),
            reconnect_base_delay: StdDuration::from_secs(1),
            reconnect_max_delay: StdDuration::from_secs(30),
            node_id: IncarnationId::new(),
            node_endpoint: NodeEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 7233,
            },
            zone: Some("z1".to_owned()),
            version: "test-version".to_owned(),
            build_id: "test-build".to_owned(),
        }
    }

    fn client(
        store: Arc<InMemoryStore>,
        budget_applier: Arc<RecordingBudgetApplier>,
    ) -> MembershipClient<InMemoryStore> {
        MembershipClient::new(
            config(),
            store,
            Arc::new(RwLock::new(ShardOwner::new(4))),
            Arc::new(RuntimeDrain::default()),
            budget_applier,
        )
    }

    #[test]
    fn registration_message_includes_runtime_identity() {
        let store = Arc::new(InMemoryStore::default());
        let budget = Arc::new(RecordingBudgetApplier::default());
        let client = client(store, budget);
        let registration = client.registration_message();

        assert_eq!(registration.node_id, client.config.node_id.to_string());
        assert_eq!(registration.host, "127.0.0.1");
        assert_eq!(registration.port, 7233);
        assert_eq!(registration.zone, "z1");
        assert_eq!(registration.version, "test-version");
        assert_eq!(registration.build_id, "test-build");
    }

    #[tokio::test]
    async fn desired_placement_acquires_and_relinquishes_with_identity_and_endpoint() {
        let store = Arc::new(InMemoryStore::default());
        let budget = Arc::new(RecordingBudgetApplier::default());
        let client = client(Arc::clone(&store), budget);
        client
            .handle_directive(proto::ControllerDirective {
                directive: Some(Directive::DesiredPlacement(
                    proto::DesiredPlacementDirective {
                        acquire_bundles: vec![1],
                        relinquish_bundles: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let leases = store.list_bundle_leases().await.unwrap();
        assert!(leases.iter().any(|lease| {
            lease.bundle_id == ShardId(1)
                && lease.owner_node_id.as_deref()
                    == Some(client.config.node_id.to_string().as_str())
                && lease.node_endpoint.as_deref() == Some("127.0.0.1:7233")
        }));

        client
            .handle_directive(proto::ControllerDirective {
                directive: Some(Directive::DesiredPlacement(
                    proto::DesiredPlacementDirective {
                        acquire_bundles: Vec::new(),
                        relinquish_bundles: vec![1],
                    },
                )),
            })
            .await
            .unwrap();
        let leases = store.list_bundle_leases().await.unwrap();
        assert!(
            leases
                .iter()
                .any(|lease| { lease.bundle_id == ShardId(1) && lease.owner_node_id.is_none() })
        );
    }

    #[tokio::test]
    async fn connection_budget_and_drain_directives_update_runtime_state() {
        let store = Arc::new(InMemoryStore::default());
        let budget = Arc::new(RecordingBudgetApplier::default());
        let client = client(store, Arc::clone(&budget));

        client
            .handle_directive(proto::ControllerDirective {
                directive: Some(Directive::ConnectionBudget(ConnectionBudgetDirective {
                    rate_per_second: 10.0,
                    capacity: 20,
                    max_reservoir_size: 3,
                    valid_until: Some(prost_types::Timestamp {
                        seconds: OffsetDateTime::now_utc().unix_timestamp() + 60,
                        nanos: 0,
                    }),
                })),
            })
            .await
            .unwrap();
        assert_eq!(budget.calls.lock().unwrap().as_slice(), &[(10.0, 20, 3)]);
        assert!(!client.last_budget_expired());

        client
            .handle_directive(proto::ControllerDirective {
                directive: Some(Directive::Drain(proto::DrainDirective {})),
            })
            .await
            .unwrap();
        assert_eq!(client.drain.state(), RuntimeDrainState::SafeToTerminate);
    }

    #[tokio::test]
    async fn drain_directive_relinquishes_owned_bundles_and_reports_safe() {
        let store = Arc::new(InMemoryStore::default());
        let budget = Arc::new(RecordingBudgetApplier::default());
        let client = client(Arc::clone(&store), budget);
        client
            .handle_directive(proto::ControllerDirective {
                directive: Some(Directive::DesiredPlacement(
                    proto::DesiredPlacementDirective {
                        acquire_bundles: vec![1, 2],
                        relinquish_bundles: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();

        client
            .handle_directive(proto::ControllerDirective {
                directive: Some(Directive::Drain(proto::DrainDirective {})),
            })
            .await
            .unwrap();

        let leases = store.list_bundle_leases().await.unwrap();
        assert!(
            leases
                .iter()
                .filter(|lease| lease.bundle_id == ShardId(1) || lease.bundle_id == ShardId(2))
                .all(|lease| lease.owner_node_id.is_none())
        );
        assert_eq!(client.shard_owner.read().unwrap().owned_shards().count(), 0);
        assert_eq!(client.drain.state(), RuntimeDrainState::SafeToTerminate);
    }

    #[test]
    fn heartbeat_reports_owned_bundles_and_drain_state() {
        let store = Arc::new(InMemoryStore::default());
        let budget = Arc::new(RecordingBudgetApplier::default());
        let client = client(store, budget);
        client
            .shard_owner
            .write()
            .unwrap()
            .record_acquired(ShardId(2), ShardEpoch(7));
        client.drain.mark_safe_to_terminate();

        let heartbeat = client.heartbeat_message();

        assert_eq!(heartbeat.owned_bundle_count, 1);
        assert_eq!(heartbeat.owned_bundles, vec![2]);
        assert_eq!(
            heartbeat.drain_state,
            proto::NodeDrainState::SafeToTerminate as i32
        );
    }

    #[test]
    fn heartbeat_inputs_preserve_connection_headroom_and_lane_pressure_fields() {
        let mut owner = ShardOwner::new(4);
        owner.record_acquired(ShardId(2), ShardEpoch(7));
        let inputs = HeartbeatInputs::from_runtime_components(
            &owner,
            RuntimeDrainState::Draining,
            &[],
            12,
            0.75,
        );

        assert_eq!(inputs.owned_bundles, vec![ShardId(2)]);
        assert_eq!(inputs.available_connections, 12);
        assert_eq!(inputs.connection_rate_headroom, 0.75);
        assert_eq!(inputs.drain_state, RuntimeDrainState::Draining);
        assert!(inputs.lane_pressures.is_empty());
    }
}
