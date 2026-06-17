//! Runtime side of the placement-controller membership stream.
//!
//! Owns the runtime node's relationship with the placement controller: it
//! registers the node, sends periodic heartbeats describing owned bundles and
//! lane pressure, and applies the directives the controller streams back
//! (desired placement, connection budget, drain, routing). This is the runtime's
//! only voice in cluster membership — the controller decides placement, this
//! client enacts it locally.
//!
//! Uses connect-rust for the bidirectional streaming RPC. The stream is a single
//! handle (no split/clone), so registration, heartbeat ticks, and inbound
//! directives are all multiplexed through one `tokio::select!` loop in
//! `MembershipClient::run_once`.
//!
//! Invariants this client upholds:
//! - **The controller owns placement; bundle ownership is lease-gated.** Acquiring
//!   or relinquishing a bundle is mediated by the [`LeaseRepository`]: local
//!   [`ShardOwner`] state is only updated after the lease store confirms the
//!   acquire/relinquish, so two nodes cannot both believe they own a bundle.
//! - **Disconnects must not silently keep a stale connection budget.** A budget
//!   directive carries an expiry; on reconnect, if the last budget has expired the
//!   client resets to a safe minimal budget rather than continuing to honour a
//!   directive the controller may no longer endorse.
//! - **Reconnect backs off exponentially** between `reconnect_base_delay` and
//!   `reconnect_max_delay` so a flapping controller does not amplify into a
//!   reconnect storm.

use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration as StdDuration,
};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_proto::connect::tokeira::internal::controller::v1::{
    self as pb, LanePressure, PlacementControllerClient, RuntimeHeartbeat,
    RuntimeMembershipRequest, RuntimeRegistration, controller_directive,
    runtime_membership_request,
};
use tokeira_storage::{LeaseOutcome, LeaseRepository};
use tokeira_types::{IncarnationId, NodeEndpoint, ShardEpoch, ShardId};
use tokio_util::sync::CancellationToken;

use crate::{LaneHandle, RuntimeDrain, RuntimeDrainState, ShardOwner};

/// Runtime-side placement-controller stream configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipConfig {
    /// Controller endpoint the membership stream dials.
    pub controller_endpoint: String,
    /// Interval between outbound heartbeats once the stream is up.
    pub heartbeat_interval: StdDuration,
    /// First reconnect delay after a stream drop; doubles each attempt.
    pub reconnect_base_delay: StdDuration,
    /// Ceiling on the exponential reconnect backoff.
    pub reconnect_max_delay: StdDuration,
    /// This node's incarnation identity, used as the lease owner and reported in
    /// registration/heartbeats so the controller can distinguish restarts.
    pub node_id: IncarnationId,
    /// Network endpoint other nodes use to reach this runtime.
    pub node_endpoint: NodeEndpoint,
    /// Optional availability zone, reported for zone-aware placement.
    pub zone: Option<String>,
    /// Build/version strings reported to the controller for placement and
    /// versioning decisions.
    pub version: String,
    /// Build identifier reported to the controller.
    pub build_id: String,
}

impl MembershipConfig {
    /// Lease owner string for this node — its incarnation id. Used when
    /// acquiring/relinquishing bundle leases so ownership is attributable to a
    /// specific node incarnation.
    pub fn owner_identity(&self) -> String {
        self.node_id.to_string()
    }
}

/// Client that drives the runtime's placement-controller membership stream.
///
/// Holds the runtime-owned dependencies directive handling needs: the lease
/// repository (placement is lease-gated), the local [`ShardOwner`] view, the
/// drain coordinator, and the connection-budget applier. Generic over the
/// [`LeaseRepository`] so it can run against any storage backend.
pub struct MembershipClient<R>
where
    R: LeaseRepository + 'static,
{
    config: MembershipConfig,
    leases: Arc<R>,
    shard_owner: Arc<RwLock<ShardOwner>>,
    drain: Arc<RuntimeDrain>,
    budget_applier: Arc<dyn ConnectionBudgetApplier>,
    last_budget_valid_until: Arc<Mutex<Option<BudgetExpiry>>>,
}

/// Tracks when the last connection budget directive expires.
#[derive(Clone, Debug)]
struct BudgetExpiry {
    seconds: i64,
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
    /// Construct a client. Does not open the stream — call [`run`](Self::run).
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

    /// Run the membership stream until `shutdown` fires, reconnecting with
    /// exponential backoff on transient stream failures.
    ///
    /// On reconnect, if the previously applied connection budget has expired the
    /// client resets to a minimal safe budget — it must not keep honouring a
    /// budget the controller can no longer confirm while the stream is down.
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
        let http = connectrpc::client::HttpClient::plaintext();
        let config = connectrpc::client::ClientConfig::new(
            self.config
                .controller_endpoint
                .parse()
                .map_err(|e| anyhow!("invalid controller endpoint: {e}"))?,
        );
        let client = PlacementControllerClient::new(http, config);

        let mut bidi = client
            .runtime_membership()
            .await
            .map_err(|e| anyhow!("runtime_membership stream failed: {e}"))?;

        // Send registration as the first message.
        bidi.send(RuntimeMembershipRequest {
            request: Some(runtime_membership_request::Request::Registration(
                self.registration_message().into(),
            )),
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow!("failed to send registration: {e}"))?;

        // Heartbeat ticker — we send heartbeats inline in the select loop
        // because BidiStream is a single handle (no split/clone).
        let mut heartbeat_interval = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = heartbeat_interval.tick() => {
                    let msg = RuntimeMembershipRequest {
                        request: Some(runtime_membership_request::Request::Heartbeat(
                            self.heartbeat_message().into(),
                        )),
                        ..Default::default()
                    };
                    bidi.send(msg).await.map_err(|e| anyhow!("heartbeat send failed: {e}"))?;
                }
                directive = bidi.message() => {
                    let Some(directive) = directive.map_err(|e| anyhow!("{e}"))? else {
                        return Ok(());
                    };
                    // Work directly with the zero-copy view — only allocate
                    // when storing data (lease operations need owned strings).
                    self.handle_directive_view(&directive).await?;
                }
            }
        }
    }

    /// Build the registration message sent as the first frame on a new stream,
    /// announcing this node's identity, endpoint, zone, and build to the
    /// controller.
    pub fn registration_message(&self) -> RuntimeRegistration {
        RuntimeRegistration {
            node_id: self.config.node_id.to_string(),
            host: self.config.node_endpoint.host.clone(),
            port: u32::from(self.config.node_endpoint.port),
            zone: self.config.zone.clone().unwrap_or_default(),
            version: self.config.version.clone(),
            build_id: self.config.build_id.clone(),
            ..Default::default()
        }
    }

    /// Build a heartbeat from current owned-bundle and drain state. Convenience
    /// wrapper over [`heartbeat_message_with_inputs`](Self::heartbeat_message_with_inputs)
    /// that snapshots inputs from the live [`ShardOwner`] and [`RuntimeDrain`].
    pub fn heartbeat_message(&self) -> RuntimeHeartbeat {
        self.heartbeat_message_with_inputs(HeartbeatInputs::from_shard_owner(
            &self.shard_owner.read().unwrap(),
            self.drain.state(),
        ))
    }

    /// Build a heartbeat from explicitly supplied [`HeartbeatInputs`]. Separated
    /// from input collection so callers (and tests) can report richer pressure
    /// metrics without re-reading shared state.
    pub fn heartbeat_message_with_inputs(&self, inputs: HeartbeatInputs) -> RuntimeHeartbeat {
        use buffa::EnumValue;
        let drain_state = match inputs.drain_state {
            RuntimeDrainState::Active => {
                EnumValue::Known(pb::NodeDrainState::NODE_DRAIN_STATE_ACTIVE)
            }
            RuntimeDrainState::Draining => {
                EnumValue::Known(pb::NodeDrainState::NODE_DRAIN_STATE_DRAINING)
            }
            RuntimeDrainState::SafeToTerminate => {
                EnumValue::Known(pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE)
            }
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
            lane_pressures: inputs
                .lane_pressures
                .into_iter()
                .map(|lp| LanePressure {
                    lane_id: lp.lane_id,
                    runnable_depth: lp.runnable_depth,
                    active_actors: lp.active_actors,
                    utilization: lp.utilization,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}

/// Snapshot of runtime load reported in a single heartbeat.
///
/// Decouples heartbeat *content* from how it is gathered, so the basic path
/// (owned bundles + drain state) and the rich path (lane pressure, connection
/// headroom) can both produce a heartbeat without the client re-reading shared
/// state inline.
#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatInputs {
    /// Bundles this node currently owns.
    pub owned_bundles: Vec<ShardId>,
    /// Total runnable transitions across lanes.
    pub runnable_transitions: u64,
    /// Count of actors with work in flight.
    pub active_actor_count: u64,
    /// Pending backlog depth.
    pub backlog_depth: u64,
    /// Connections currently available to this node.
    pub available_connections: u32,
    /// Fraction of the connection rate budget still unused (0.0–1.0).
    pub connection_rate_headroom: f32,
    /// This node's drain state, so the controller knows whether it is safe to
    /// terminate.
    pub drain_state: RuntimeDrainState,
    /// Per-lane pressure detail.
    pub lane_pressures: Vec<HeartbeatLanePressure>,
}

/// Lane pressure data collected for heartbeat reporting.
#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatLanePressure {
    /// Index of the lane within the runtime's lane set.
    pub lane_id: u32,
    /// Number of runnable items queued on the lane.
    pub runnable_depth: u64,
    /// Number of actors actively executing on the lane.
    pub active_actors: u64,
    /// Lane utilization in [0.0, 1.0].
    pub utilization: f32,
}

impl HeartbeatInputs {
    /// Minimal inputs: owned bundles and drain state only, with load counters
    /// zeroed. Used when richer per-lane metrics are not being collected.
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

    /// Full inputs: derives per-lane pressure and aggregate runnable/active
    /// counts from live lane handles, and folds in connection availability and
    /// rate headroom. Used on the rich heartbeat path.
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
                HeartbeatLanePressure {
                    lane_id: lane_id as u32,
                    runnable_depth,
                    active_actors: if runnable_depth > 0 { 1 } else { 0 },
                    utilization,
                }
            })
            .collect::<Vec<_>>();
        let runnable_transitions = lane_pressures.iter().map(|p| p.runnable_depth).sum();
        let active_actor_count = lane_pressures.iter().map(|p| p.active_actors).sum();
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
    /// Handle a controller directive from the zero-copy view.
    /// Only allocates when storing data (lease owner strings for DSQL operations).
    pub async fn handle_directive_view(
        &self,
        directive: &pb::ControllerDirectiveView<'_>,
    ) -> Result<()> {
        use tokeira_proto::connect::tokeira::internal::controller::v1::__buffa::view::oneof::controller_directive::Directive;

        match &directive.directive {
            Some(Directive::DesiredPlacement(desired)) => {
                for &bundle in desired.acquire_bundles.iter() {
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
                for &bundle in desired.relinquish_bundles.iter() {
                    self.relinquish_owned_bundle(ShardId(bundle)).await?;
                }
            }
            Some(Directive::ConnectionBudget(budget)) => {
                let valid_until = if budget.valid_until.is_set() {
                    Some(BudgetExpiry {
                        seconds: budget.valid_until.seconds,
                    })
                } else {
                    None
                };
                *self.last_budget_valid_until.lock().unwrap() = valid_until;
                self.budget_applier.apply_budget(
                    budget.rate_per_second,
                    budget.capacity,
                    budget.max_reservoir_size,
                )?;
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

    /// Handle a controller directive (buffa-generated owned type).
    /// Used by tests that construct directives directly.
    pub async fn handle_directive(&self, directive: pb::ControllerDirective) -> Result<()> {
        match directive.directive {
            Some(controller_directive::Directive::DesiredPlacement(desired)) => {
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
            Some(controller_directive::Directive::ConnectionBudget(budget)) => {
                self.apply_connection_budget(&budget)?;
            }
            Some(controller_directive::Directive::Drain(_)) => {
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
            Some(controller_directive::Directive::RoutingUpdate(_)) | None => {}
        }
        Ok(())
    }

    fn apply_connection_budget(&self, budget: &pb::ConnectionBudgetDirective) -> Result<()> {
        let valid_until = if budget.valid_until.is_set() {
            Some(BudgetExpiry {
                seconds: budget.valid_until.seconds,
            })
        } else {
            None
        };
        *self.last_budget_valid_until.lock().unwrap() = valid_until;
        self.budget_applier.apply_budget(
            budget.rate_per_second,
            budget.capacity,
            budget.max_reservoir_size,
        )
    }

    /// Whether the most recently applied connection budget has passed its
    /// `valid_until` deadline.
    ///
    /// Returns `false` when no budget carried an expiry (the budget is open-ended).
    /// An unparseable deadline is treated as expired (`true`) — failing safe by
    /// re-requesting a budget rather than honouring an unreadable one.
    pub fn last_budget_expired(&self) -> bool {
        let guard = self.last_budget_valid_until.lock().unwrap();
        match &*guard {
            None => false,
            Some(expiry) => {
                let Ok(deadline) = OffsetDateTime::from_unix_timestamp(expiry.seconds) else {
                    return true;
                };
                deadline < OffsetDateTime::now_utc()
            }
        }
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
///
/// Implemented by whatever component owns the connection reservoir/rate limiter.
/// Keeping it a trait lets `membership` apply budgets without depending on the
/// connection-management internals, and lets tests record applied budgets.
pub trait ConnectionBudgetApplier: Send + Sync + std::fmt::Debug {
    /// Apply a controller-issued budget: sustained `rate_per_second`, burst
    /// `capacity`, and the maximum reservoir size to maintain.
    fn apply_budget(
        &self,
        rate_per_second: f64,
        capacity: u64,
        max_reservoir_size: u32,
    ) -> Result<()>;
}

/// Whether a `valid_until` Unix-second deadline lies in the past.
///
/// `None` means no expiry was set, so the budget is not expired. A deadline that
/// cannot be represented as a timestamp is treated as expired — fail safe rather
/// than trust an unreadable value. Free-function form for callers that hold only
/// the raw seconds (e.g. directive parsing) rather than a [`MembershipClient`].
pub fn budget_valid_until_expired(valid_until_seconds: Option<i64>) -> bool {
    let Some(seconds) = valid_until_seconds else {
        return false;
    };
    let Ok(deadline) = OffsetDateTime::from_unix_timestamp(seconds) else {
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
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::DesiredPlacement(
                    pb::DesiredPlacementDirective {
                        acquire_bundles: vec![1],
                        relinquish_bundles: Vec::new(),
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
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
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::DesiredPlacement(
                    pb::DesiredPlacementDirective {
                        acquire_bundles: Vec::new(),
                        relinquish_bundles: vec![1],
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
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
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::ConnectionBudget(
                    pb::ConnectionBudgetDirective {
                        rate_per_second: 10.0,
                        capacity: 20,
                        max_reservoir_size: 3,
                        valid_until: buffa_types::google::protobuf::Timestamp {
                            seconds: OffsetDateTime::now_utc().unix_timestamp() + 60,
                            nanos: 0,
                            ..Default::default()
                        }
                        .into(),
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(budget.calls.lock().unwrap().as_slice(), &[(10.0, 20, 3)]);
        assert!(!client.last_budget_expired());

        client
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::Drain(
                    pb::DrainDirective::default().into(),
                )),
                ..Default::default()
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
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::DesiredPlacement(
                    pb::DesiredPlacementDirective {
                        acquire_bundles: vec![1, 2],
                        relinquish_bundles: Vec::new(),
                        ..Default::default()
                    }
                    .into(),
                )),
                ..Default::default()
            })
            .await
            .unwrap();

        client
            .handle_directive(pb::ControllerDirective {
                directive: Some(controller_directive::Directive::Drain(
                    pb::DrainDirective::default().into(),
                )),
                ..Default::default()
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
            buffa::EnumValue::Known(pb::NodeDrainState::NODE_DRAIN_STATE_SAFE_TO_TERMINATE)
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
