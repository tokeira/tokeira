//! Authenticated activity publication and starts across independent runtime nodes.
//!
//! Discovery is blocked at the repository boundary until a recovery scenario
//! explicitly releases it. Transport faults surround, rather than replace, the
//! real decoder and independent peer/worker authorization gates.

mod controller;
#[cfg(feature = "dsql-integration")]
pub(super) mod live;

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use proptest::prelude::*;
use tokeira_clock::Clock;
use tokeira_edge::grpc::workflow_offer::MAX_OFFER_HANDLERS;
use tokeira_kernel::{
    ActivityControlTarget, Command, LoadedRun, PauseActivityRequest, SignalRequest, StartRequest,
    VersioningBehavior, WorkflowCommand, WorkflowState, WorkflowTaskCompletedRequest,
};
use tokeira_proto::{
    internal::runtime::{self as wire, activity_task_dispatch_client::ActivityTaskDispatchClient},
    public::temporal::api::taskqueue::v1::TaskQueue,
    workflowservice::{
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_runtime::{ActivityOfferError, ActivityOfferOutcome, UnpauseActivitiesRequest};
use tokeira_storage::{InMemoryStore, LeaseRepository, RunRepository};
use tokeira_types::{
    ActivityTaskToken, Payload, Payloads, QueueKey, RequestContext, RetryPolicy, ShardEpoch,
    TaskKind, TaskQueueName, WorkerIdentity,
};
use tonic::{Code, Request};

use super::delivery_harness::*;
use controller::Controller;

const BOUND: Duration = Duration::from_secs(3);

// Each command carries its own request id, so none is deduplicated as a
// retry of another.
fn fresh_request(now: time::OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: tokeira_types::RequestId(uuid::Uuid::new_v4().to_string()),
        ..RequestContext::unattributed(now)
    }
}

struct Cluster<R: RunRepository + 'static = InMemoryStore> {
    nodes: Vec<Node<R>>,
    store: Arc<R>,
    clock: Arc<TestClock>,
    controller: Controller,
    config: tokeira_config::peer::PeerConfig,
    _credentials: tempfile::TempDir,
    request: StartRequest,
    queue: QueueKey,
    epochs: [ShardEpoch; 2],
}
impl Cluster {
    async fn new(index: u32, count: usize) -> Self {
        let clock = TestClock::new();
        let store = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
        Self::with_store(store, clock, index, count).await
    }
}
impl<R: RunRepository + LeaseRepository + 'static> Cluster<R> {
    async fn with_store(store: Arc<R>, clock: Arc<TestClock>, index: u32, count: usize) -> Self {
        let (credentials, config) = credential();
        let controller = Controller::new().await;
        let mut nodes = Vec::new();
        for index in 0..count {
            nodes.push(
                activity_node(
                    store.clone(),
                    &format!("activity-node-{index}"),
                    &config,
                    clock.clone(),
                    &controller.endpoint,
                )
                .await,
            );
        }
        let mut request = request(index, &clock);
        let queue = QueueKey {
            task_kind: TaskKind::Activity,
            ..normal_queue(&request)
        };
        let home = execution(&request);
        request.task_queue = (0..1000)
            .map(|n| TaskQueueName(format!("workflow-local-{n}")))
            .find(|name| {
                queue_home(&QueueKey {
                    task_queue: name.clone(),
                    ..normal_queue(&request)
                }) == home
            })
            .expect("colocated workflow queue");
        let epochs = [
            nodes[0]
                .runtime
                .acquire_shard(home)
                .await
                .expect("execution acquisition"),
            nodes[1]
                .runtime
                .acquire_shard(queue_home(&queue))
                .await
                .expect("queue acquisition"),
        ];
        let cluster = Self {
            nodes,
            store,
            clock,
            controller,
            config,
            _credentials: credentials,
            request,
            queue,
            epochs,
        };
        cluster.routes(0, 1);
        cluster
    }

    fn routes(&self, owner: usize, queue: usize) {
        route(
            &self.nodes.iter().collect::<Vec<_>>(),
            &[
                (execution(&self.request), owner, self.epochs[0]),
                (queue_home(&self.queue), queue, self.epochs[1]),
            ],
        );
    }

    async fn schedule(&self, retry: bool) {
        let owner = &self.nodes[0];
        owner
            .runtime
            .start_workflow(self.request.clone())
            .await
            .expect("start fixture workflow");
        let first = owner
            .runtime
            .poll_workflow_task(
                normal_queue(&self.request),
                WorkerIdentity("workflow-worker".into()),
                BOUND,
            )
            .await
            .expect("poll fixture workflow")
            .expect("fixture workflow task");
        let now = self.clock.now();
        let completion = WorkflowTaskCompletedRequest {
            token: first.token.clone(),
            client_discards_speculative_with_events: false,
            identity: first.worker_identity,
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleActivity {
                activity_id: "activity".into(),
                activity_type: "delivery".into(),
                task_queue: self.queue.task_queue.clone(),
                input: Payloads(vec![Payload::new(vec![0, 255, 42])]),
                header: None,
                request_eager_execution: false,
                use_workflow_build_id: false,
                retry_policy: retry.then_some(RetryPolicy {
                    initial_interval: time::Duration::ZERO,
                    backoff_coefficient: 1.0,
                    maximum_interval: None,
                    maximum_attempts: 3,
                    non_retryable_error_types: Vec::new(),
                }),
                deployment: None,
                build_id: None,
                schedule_to_close_timeout: Some(time::Duration::hours(1)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: Some(time::Duration::minutes(10)),
                heartbeat_timeout: None,
                priority: None,
            }],
            command_sizes: Vec::new(),
            heartbeat: Default::default(),
            force_new_workflow_task: false,
            eager_activities: Default::default(),
            inline_workflow_task: None,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: fresh_request(now),
            now,
        };
        let result = owner
            .runtime
            .complete_workflow_task_with_result(completion)
            .await
            .expect("publish committed schedule");
        assert!(
            result.activity_tasks.is_empty(),
            "ordinary publication returned eager tasks: {:?}",
            result.activity_tasks
        );
    }

    async fn admitted(&mut self, owner: usize) -> ActivityReceipt {
        loop {
            let receipt = self.nodes[owner].activity_receipt().await;
            assert_eq!(
                receipt.destination,
                queue_home(&self.queue),
                "push receipt: {receipt:?}"
            );
            assert!(
                matches!(
                    receipt.outcome,
                    Ok(ActivityOfferOutcome::Admitted | ActivityOfferOutcome::Known)
                ),
                "push receipt: {receipt:?}"
            );
            if receipt.outcome == Ok(ActivityOfferOutcome::Admitted) {
                return receipt;
            }
        }
    }

    async fn state(&self) -> WorkflowState {
        match self
            .store
            .load_run(self.request.run_key)
            .await
            .expect("inspect durable state")
        {
            LoadedRun::Existing(state) => state,
            other => panic!("durable run: {other:?}"),
        }
    }

    fn committed_starts(&self, expected: usize) {
        let counts: Vec<_> = self
            .nodes
            .iter()
            .map(|n| n.repo.applied_starts.load(Ordering::SeqCst))
            .collect();
        assert_eq!(
            counts.iter().sum::<usize>(),
            expected,
            "applied starts by node: {counts:?}; expected={expected}"
        );
    }

    fn discovery_paused(&self) {
        let reads: Vec<_> = self
            .nodes
            .iter()
            .map(|node| node.repo.activity.reads.load(Ordering::SeqCst))
            .collect();
        assert!(
            reads.iter().all(|count| *count == 0),
            "activity discovery reads by node: {reads:?}"
        );
    }

    async fn assert_started(&self, owner: usize, response: &PollActivityTaskQueueResponse) {
        assert!(
            !response.task_token.is_empty(),
            "poll response: {response:?}"
        );
        let state = self.state().await;
        assert_eq!(
            state.activity_start_receipts.len(),
            1,
            "start receipts: {:?}",
            state.activity_start_receipts
        );
        assert!(
            state.activities["activity"].started_at.is_some(),
            "activity state: {:?}",
            state.activities["activity"]
        );
        assert_eq!(
            response.input.as_ref().expect("activity input").payloads[0].data,
            [0, 255, 42],
            "poll payload: {:?}",
            response.input
        );
        let starts = self.nodes[owner].starts.lock().unwrap();
        assert!(
            !starts.is_empty(),
            "owner saw no authenticated start RPCs: {starts:?}"
        );
        assert_eq!(
            state.activity_start_receipts["activity"]
                .request_id
                .as_bytes(),
            starts.last().unwrap().request_id.as_slice(),
            "committed receipt and wire starts: {:?}, {starts:?}",
            state.activity_start_receipts
        );
        assert!(
            self.nodes[owner].policy.0.load(Ordering::SeqCst) > 0,
            "owner worker authentication count: {}",
            self.nodes[owner].policy.0.load(Ordering::SeqCst)
        );
    }

    async fn stop(self) {
        for node in self.nodes {
            node.stop().await;
        }
        self.controller.stop().await;
    }
}

async fn poll(address: std::net::SocketAddr, queue: &QueueKey) -> PollActivityTaskQueueResponse {
    let mut client = WorkflowServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect public worker poll");
    let mut request = Request::new(PollActivityTaskQueueRequest {
        namespace: "default".into(),
        identity: "integration-worker".into(),
        task_queue: Some(TaskQueue {
            name: queue.task_queue.0.clone(),
            kind: 1,
            ..Default::default()
        }),
        ..Default::default()
    });
    request.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    tokio::time::timeout(BOUND, client.poll_activity_task_queue(request))
        .await
        .expect("activity poll exceeded transport bound")
        .expect("authenticated activity poll")
        .into_inner()
}

async fn parked<R: RunRepository + 'static>(
    node: &Node<R>,
    queue: &QueueKey,
) -> tokio::task::JoinHandle<PollActivityTaskQueueResponse> {
    let address = node.address;
    let selected = queue.clone();
    let polling = tokio::spawn(async move { poll(address, &selected).await });
    tokio::time::timeout(BOUND, async {
        while !node
            .runtime
            .activity_broker()
            .queues_with_waiters()
            .await
            .contains(queue)
        {
            assert!(
                !polling.is_finished(),
                "poll finished before parking; discovery={}, starts={:?}",
                node.repo.activity.reads.load(Ordering::SeqCst),
                node.starts.lock().unwrap()
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker poll did not park");
    polling
}

fn offer_wire(receipt: &ActivityReceipt) -> wire::OfferActivityTaskRequest {
    let offer = &receipt.offer;
    let task = &offer.task;
    wire::OfferActivityTaskRequest {
        run_key: task.run_key.0.as_bytes().to_vec(),
        activity_id: task.activity_id.clone(),
        schedule_event_id: task.schedule_event_id,
        attempt: task.attempt,
        stamp: task.stamp,
        execution_home: offer.execution_home.0,
        destination_home: receipt.destination.0,
        namespace_id: task.queue.namespace_id.0.as_bytes().to_vec(),
        selected_queue: Some(wire::OfferQueue {
            name: task.queue.task_queue.0.clone(),
            deployment: task.queue.deployment.as_ref().map(|v| v.0.clone()),
            build_id: task.queue.build_id.as_ref().map(|v| v.0.clone()),
        }),
        dispatch_revision: task.dispatch_revision,
        dispatch_at: Some(prost_types::Timestamp {
            seconds: offer.dispatch_at.unix_timestamp(),
            nanos: offer.dispatch_at.nanosecond() as i32,
        }),
        expires_at: Some(prost_types::Timestamp {
            seconds: offer.expires_at.unix_timestamp(),
            nanos: offer.expires_at.nanosecond() as i32,
        }),
        priority: None,
    }
}

async fn client<R: RunRepository + 'static>(
    node: &Node<R>,
) -> ActivityTaskDispatchClient<tonic::transport::Channel> {
    ActivityTaskDispatchClient::connect(format!("http://{}", node.address))
        .await
        .expect("connect internal activity service")
}

fn authorized<R: RunRepository + 'static>(
    node: &Node<R>,
    offer: wire::OfferActivityTaskRequest,
) -> Request<wire::OfferActivityTaskRequest> {
    let token = node
        .peer
        .signer()
        .token_at(node.peer.unix_seconds().unwrap())
        .expect("sign generated peer credential");
    let mut request = Request::new(offer);
    request
        .metadata_mut()
        .insert("authorization", token.authorization().parse().unwrap());
    request
}

// Feature: activity-queue-home-delivery, Property 1: due publications reach parked polls while discovery is paused.
#[tokio::test]
async fn grpc_activity_first_retry_control_and_renewal() {
    let mut c = Cluster::new(11, 2).await;
    let before =
        tokeira_config::peer::load_peer_file(std::path::Path::new(&c.config.path)).unwrap();
    let old_token = c.nodes[0]
        .peer
        .signer()
        .token_at(c.nodes[0].peer.unix_seconds().unwrap())
        .unwrap();
    let first = parked(&c.nodes[1], &c.queue).await;
    c.schedule(true).await;
    c.admitted(0).await;
    let response = first.await.expect("first poll joined");
    c.assert_started(0, &response).await;
    c.discovery_paused();
    let token: ActivityTaskToken =
        serde_json::from_slice(&response.task_token).expect("activity task token");
    c.clock.advance(331);
    let failed = c.nodes[0]
        .runtime
        .fail_activity_task(
            token,
            Payload::new(Vec::new()),
            None,
            false,
            None,
            None,
            fresh_request(c.clock.now()),
        )
        .await;
    assert!(failed.is_ok(), "failure transition: {failed:?}");
    let retry = c.admitted(0).await;
    assert_eq!(retry.offer.task.attempt, 2, "retry push: {retry:?}");
    let response = poll(c.nodes[1].address, &c.queue).await;
    assert_eq!(response.attempt, 2, "retry poll: {response:?}");
    c.assert_started(0, &response).await;
    let token: ActivityTaskToken = serde_json::from_slice(&response.task_token).unwrap();
    let now = c.clock.now();
    c.nodes[0]
        .runtime
        .pause_activities(
            c.request.run_key,
            PauseActivityRequest {
                target: ActivityControlTarget::All,
                identity: "control".into(),
                reason: "test pause".into(),
                request: fresh_request(now),
                now,
                rule_id: None,
            },
        )
        .await
        .expect("pause running activity");
    c.nodes[0]
        .runtime
        .fail_activity_task(
            token,
            Payload::new(Vec::new()),
            None,
            false,
            None,
            None,
            fresh_request(now),
        )
        .await
        .expect("failed paused attempt");
    let pending = parked(&c.nodes[1], &c.queue).await;
    // Scheduled unpause regenerates due work: service/history/workflow/activity.go @ v1.31.0.
    c.nodes[0]
        .runtime
        .unpause_activities(
            c.request.run_key,
            UnpauseActivitiesRequest {
                target: ActivityControlTarget::All,
                reset_attempts: true,
                reset_heartbeat: false,
                jitter: None,
                request: fresh_request(now),
                now,
            },
        )
        .await
        .expect("unpause activity");
    c.admitted(0).await;
    c.assert_started(0, &pending.await.expect("control poll joined"))
        .await;
    let mut expired = Request::new(wire::OfferActivityTaskRequest::default());
    expired
        .metadata_mut()
        .insert("authorization", old_token.authorization().parse().unwrap());
    let status = client(&c.nodes[1])
        .await
        .offer_activity_task(expired)
        .await
        .expect_err("expired peer token accepted")
        .code();
    assert_eq!(
        status,
        Code::Unauthenticated,
        "expired token status: {status:?}"
    );
    let unchanged = tokeira_config::peer::load_peer_file(std::path::Path::new(&c.config.path))
        .unwrap()
        == before;
    assert!(unchanged, "renewal changed protected credential bytes");
    c.committed_starts(3);
    c.discovery_paused();
    c.stop().await;
}

// Feature: activity-queue-home-delivery, Property 4: an ACK does not wait for the authoritative start.
#[tokio::test]
async fn grpc_activity_ack_while_start_is_blocked() {
    let mut c = Cluster::new(12, 2).await;
    c.nodes[0].start_faults.mode.store(3, Ordering::SeqCst);
    let pending = parked(&c.nodes[1], &c.queue).await;
    c.schedule(false).await;
    tokio::time::timeout(BOUND, c.nodes[0].start_faults.reached.notified())
        .await
        .expect("start never reached transport barrier");
    let receipt = c.admitted(0).await;
    let before = c.state().await;
    assert!(
        before.activities["activity"].started_at.is_none(),
        "start committed before release: {:?}; ACK={receipt:?}",
        before.activities["activity"]
    );
    assert!(
        !pending.is_finished(),
        "poll returned while start was held; ACK={receipt:?}"
    );
    c.nodes[0].start_faults.mode.store(0, Ordering::SeqCst);
    c.nodes[0].start_faults.release.notify_one();
    c.assert_started(0, &pending.await.expect("held poll joined"))
        .await;
    let starts = c.nodes[0].starts.lock().unwrap().clone();
    assert_eq!(starts.len(), 1, "wire starts: {starts:?}");
    c.committed_starts(1);
    c.discovery_paused();
    c.stop().await;
}

async fn restart_owner<R: RunRepository + LeaseRepository + 'static>(c: &mut Cluster<R>) {
    let home = execution(&c.request);
    c.nodes[0].runtime.relinquish_shard(home).await;
    c.store
        .relinquish_bundle(home, "activity-node-0".into(), c.epochs[0])
        .await
        .expect("release old durable lease");
    let replacement = activity_node(
        c.store.clone(),
        "activity-node-0",
        &c.config,
        c.clock.clone(),
        &c.controller.endpoint,
    )
    .await;
    let old = std::mem::replace(&mut c.nodes[0], replacement);
    old.stop().await;
    c.routes(0, 1);
    c.epochs[0] = c.nodes[0]
        .runtime
        .acquire_shard(home)
        .await
        .expect("recover owner acquisition");
    c.routes(0, 1);
}

// Feature: activity-queue-home-delivery, Property 1: activation restores due obligations without discovery.
#[tokio::test]
async fn grpc_activity_due_recovery_after_restart() {
    let mut c = Cluster::new(13, 2).await;
    c.nodes[1].activity_faults.mode.store(5, Ordering::SeqCst);
    c.schedule(false).await;
    let missed = c.nodes[0].activity_receipt().await;
    assert_eq!(
        missed.outcome,
        Err(ActivityOfferError::Unimplemented),
        "pre-restart delivery: {missed:?}"
    );
    c.nodes[1].activity_faults.mode.store(0, Ordering::SeqCst);
    let pending = parked(&c.nodes[1], &c.queue).await;
    restart_owner(&mut c).await;
    c.admitted(0).await;
    c.assert_started(0, &pending.await.expect("recovery poll joined"))
        .await;
    c.committed_starts(1);
    c.discovery_paused();
    c.stop().await;
}

async fn recover_discovery<R: RunRepository + LeaseRepository + 'static>(
    c: &Cluster<R>,
    owner: usize,
    queue: usize,
) {
    c.nodes[queue]
        .activity_faults
        .mode
        .store(0, Ordering::SeqCst);
    c.nodes[queue].repo.activity.release();
    let response = poll(c.nodes[queue].address, &c.queue).await;
    c.assert_started(owner, &response).await;
    c.committed_starts(1);
    let reads = c.nodes[queue].repo.activity.reads.load(Ordering::SeqCst);
    assert!(
        reads > 0,
        "recovery used no discovery pages: reads={reads}, response={response:?}"
    );
}

async fn loss(mode: u8) {
    let mut c = Cluster::new(21 + u32::from(mode), 3).await;
    c.nodes[1]
        .activity_faults
        .mode
        .store(mode, Ordering::SeqCst);
    c.schedule(false).await;
    for attempt in 1..=3 {
        let receipt = c.nodes[0].activity_receipt().await;
        assert_eq!(
            receipt.outcome,
            Err(ActivityOfferError::Transport),
            "loss mode={mode}, attempt={attempt}, receipt={receipt:?}"
        );
    }
    let before = c.state().await;
    assert!(
        before.activity_start_receipts.is_empty(),
        "transport loss committed a start: {:?}",
        before.activity_start_receipts
    );
    c.discovery_paused();
    // An ACK loss can retain a hint. Discard only that volatile copy so the
    // recovery assertion has to traverse canonical discovery, not the push.
    c.nodes[1]
        .runtime
        .activity_broker()
        .remove_run(c.request.run_key)
        .await;
    let retained = c.nodes[1]
        .runtime
        .activity_broker()
        .has_runnable_backlog(&c.queue)
        .await;
    assert!(
        !retained,
        "hint retained before discovery recovery: {retained}"
    );
    recover_discovery(&c, 0, 1).await;
    c.stop().await;
}

// Feature: activity-queue-home-delivery, Property 1: losing all bounded push attempts leaves recoverable durable work.
#[tokio::test]
async fn grpc_activity_lost_push_recovers_by_discovery() {
    loss(1).await;
}

// Feature: activity-queue-home-delivery, Property 1: losing acknowledgements does not consume durable work.
#[tokio::test]
async fn grpc_activity_lost_ack_recovers_by_discovery() {
    loss(2).await;
}

// Feature: activity-queue-home-delivery, Property 5: ambiguous replies preserve the request UUID and replay the sole committed start.
#[tokio::test]
async fn grpc_activity_lost_start_reply_replays_same_request() {
    let mut c = Cluster::new(25, 3).await;
    c.nodes[0].start_faults.mode.store(6, Ordering::SeqCst);
    c.schedule(false).await;
    c.admitted(0).await;
    let before = c.state().await.transition_seq;
    let response = poll(c.nodes[1].address, &c.queue).await;
    c.assert_started(0, &response).await;
    let starts = c.nodes[0].starts.lock().unwrap().clone();
    assert_eq!(starts.len(), 2, "start RPCs after reply loss: {starts:?}");
    assert_eq!(
        starts[0], starts[1],
        "start retry changed identity or incarnation: {starts:?}"
    );
    let after = c.state().await;
    assert_eq!(
        after.transition_seq.0,
        before.0 + 1,
        "start/replay sequences: before={before:?}, after={:?}",
        after.transition_seq
    );
    let observations = c.nodes[0].repo.observations.load(Ordering::SeqCst);
    assert_eq!(
        observations, 1,
        "fenced receipt observations: {observations}; requests={starts:?}"
    );
    // Same-request success follows service/history/api/recordactivitytaskstarted/api.go @ v1.31.0.
    c.committed_starts(1);
    c.discovery_paused();
    c.stop().await;
}

async fn refusal(kind: u8) {
    let mut c = Cluster::new(30 + u32::from(kind), 3).await;
    c.nodes[1].activity_faults.mode.store(5, Ordering::SeqCst);
    c.schedule(false).await;
    let receipt = c.nodes[0].activity_receipt().await;
    assert_eq!(
        receipt.outcome,
        Err(ActivityOfferError::Unimplemented),
        "old peer sender outcome: {receipt:?}"
    );
    let before = c.state().await;
    let request = offer_wire(&receipt);
    let expected = match kind {
        0 => Code::Unimplemented,
        1 | 2 => Code::PermissionDenied,
        _ => Code::Unauthenticated,
    };
    let mut rpc = authorized(&c.nodes[0], request);
    if kind != 0 {
        c.nodes[1].activity_faults.mode.store(0, Ordering::SeqCst);
        let now = c.nodes[0].peer.unix_seconds().unwrap();
        match kind {
            1 => {
                rpc.metadata_mut().insert(
                    "authorization",
                    c.nodes[0]
                        .peer
                        .signer()
                        .activity_start_token_at(now)
                        .unwrap()
                        .authorization()
                        .parse()
                        .unwrap(),
                );
            }
            2 => {
                rpc.metadata_mut().insert(
                    "authorization",
                    c.nodes[0]
                        .peer
                        .signer()
                        .outbound_token_at(now)
                        .unwrap()
                        .authorization()
                        .parse()
                        .unwrap(),
                );
            }
            _ => {
                rpc.metadata_mut().insert(
                    "authorization",
                    "Bearer integration-worker".parse().unwrap(),
                );
            }
        }
    }
    let status = client(&c.nodes[1])
        .await
        .offer_activity_task(rpc)
        .await
        .expect_err("refused offer was accepted")
        .code();
    assert_eq!(
        status, expected,
        "credential/old-peer case={kind}, status={status:?}"
    );
    let retained = c.nodes[1]
        .runtime
        .activity_broker()
        .has_runnable_backlog(&c.queue)
        .await;
    assert!(
        !retained,
        "refusal retained an offer: case={kind}, retained={retained}"
    );
    assert_eq!(
        c.state().await,
        before,
        "offer refusal mutated durable state; case={kind}"
    );
    c.discovery_paused();
    recover_discovery(&c, 0, 1).await;
    c.stop().await;
}

#[tokio::test]
async fn grpc_activity_old_peer_unimplemented_recovers() {
    refusal(0).await;
}
#[tokio::test]
async fn grpc_activity_start_purpose_is_refused() {
    refusal(1).await;
}
#[tokio::test]
async fn grpc_activity_outbound_purpose_is_refused() {
    refusal(2).await;
}
#[tokio::test]
async fn grpc_activity_public_credentials_are_refused() {
    refusal(3).await;
}

// Feature: activity-queue-home-delivery, Property 4: workflow and activity pushes consume the same authenticated handler permits.
#[tokio::test]
async fn grpc_activity_handler_capacity_is_shared_with_workflows() {
    let mut c = Cluster::new(35, 3).await;
    c.nodes[1].faults.mode.store(4, Ordering::SeqCst);
    let offer = super::delivery_harness::offer_wire(&c.request, queue_home(&c.queue));
    let mut held = Vec::new();
    for _ in 0..MAX_OFFER_HANDLERS {
        let address = c.nodes[1].address;
        let request = authorized_offer(&c.nodes[0].peer, offer.clone());
        held.push(tokio::spawn(async move {
            let mut client =
                wire::workflow_task_dispatch_client::WorkflowTaskDispatchClient::connect(format!(
                    "http://{address}"
                ))
                .await
                .unwrap();
            client.offer_workflow_task(request).await
        }));
    }
    tokio::time::timeout(BOUND, async {
        while c.nodes[1].faults.held.load(Ordering::SeqCst) < MAX_OFFER_HANDLERS {
            c.nodes[1].faults.reached.notified().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "handler barrier: {error}; held={}",
            c.nodes[1].faults.held.load(Ordering::SeqCst)
        )
    });
    c.schedule(false).await;
    let receipt = c.nodes[0].activity_receipt().await;
    assert_eq!(
        receipt.outcome,
        Err(ActivityOfferError::Capacity),
        "saturated sender outcome: {receipt:?}"
    );
    let status = client(&c.nodes[1])
        .await
        .offer_activity_task(authorized(&c.nodes[0], offer_wire(&receipt)))
        .await
        .expect_err("saturated activity push admitted")
        .code();
    assert_eq!(
        status,
        Code::ResourceExhausted,
        "shared handler status={status:?}, held={}",
        c.nodes[1].faults.held.load(Ordering::SeqCst)
    );
    let retained = c.nodes[1]
        .runtime
        .activity_broker()
        .has_runnable_backlog(&c.queue)
        .await;
    assert!(!retained, "saturated push retained activity: {retained}");
    c.nodes[1].faults.mode.store(0, Ordering::SeqCst);
    c.nodes[1].faults.release.notify_waiters();
    for result in held {
        let result = result.await.expect("held workflow push joined");
        assert!(result.is_ok(), "released workflow push: {result:?}");
    }
    c.discovery_paused();
    recover_discovery(&c, 0, 1).await;
    c.stop().await;
}

async fn wait_retired<R: RunRepository + 'static>(node: &Node<R>, queue: &QueueKey) {
    tokio::time::timeout(BOUND, async {
        while node
            .runtime
            .activity_broker()
            .has_runnable_backlog(queue)
            .await
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "retirement without discovery: {error}; discovery reads={}",
            node.repo.activity.reads.load(Ordering::SeqCst)
        )
    });
}

async fn queue_movement(index: u32, duplicates: usize) {
    let mut c = Cluster::new(index, 3).await;
    queue_movement_oracle(&mut c, duplicates).await;
    c.stop().await;
}

async fn queue_movement_oracle<R: RunRepository + LeaseRepository + 'static>(
    c: &mut Cluster<R>,
    duplicates: usize,
) {
    c.schedule(false).await;
    let initial = c.admitted(0).await;
    c.nodes[1].activity_faults.mode.store(3, Ordering::SeqCst);
    let now = c.clock.now();
    c.nodes[0]
        .runtime
        .pause_activities(
            c.request.run_key,
            PauseActivityRequest {
                target: ActivityControlTarget::All,
                identity: "control".into(),
                reason: "move".into(),
                request: fresh_request(now),
                now,
                rule_id: None,
            },
        )
        .await
        .unwrap();
    c.nodes[0]
        .runtime
        .unpause_activities(
            c.request.run_key,
            UnpauseActivitiesRequest {
                target: ActivityControlTarget::All,
                reset_attempts: false,
                reset_heartbeat: false,
                jitter: None,
                request: fresh_request(now),
                now,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(BOUND, c.nodes[1].activity_faults.reached.notified())
        .await
        .expect("push did not reach old queue transport");
    let home = queue_home(&c.queue);
    c.store
        .relinquish_bundle(home, "activity-node-1".into(), c.epochs[1])
        .await
        .expect("release old queue lease");
    let next = c.nodes[2]
        .runtime
        .acquire_shard(home)
        .await
        .expect("acquire replacement queue");
    assert!(
        next.0 > c.epochs[1].0,
        "queue epochs: old={:?}, new={next:?}",
        c.epochs[1]
    );
    assert!(
        c.nodes[1].runtime.active_shards().contains(&home),
        "old acquisition did not overlap: {:?}",
        c.nodes[1].runtime.active_shards()
    );
    c.nodes[1].runtime.relinquish_shard(home).await;
    wait_retired(&c.nodes[1], &c.queue).await;
    c.controller.point_to(home, 2, next, c.nodes[2].address);
    c.nodes[1].activity_faults.mode.store(0, Ordering::SeqCst);
    c.nodes[1].activity_faults.release.notify_one();
    let refused = c.nodes[0].activity_receipt().await;
    assert_eq!(
        refused.outcome,
        Ok(ActivityOfferOutcome::NotHome),
        "old acquisition reply: {refused:?}"
    );
    assert!(
        !refused.refresh,
        "first attempt refreshed unexpectedly: {refused:?}"
    );
    let refreshed = c.admitted(0).await;
    assert!(
        refreshed.refresh,
        "new home was not reached by trusted refresh: {refreshed:?}"
    );
    assert_eq!(
        c.controller.calls.load(Ordering::SeqCst),
        1,
        "trusted refresh count: {}",
        c.controller.calls.load(Ordering::SeqCst)
    );
    c.epochs[1] = next;
    c.routes(0, 2);
    for _ in 0..duplicates {
        let response = client(&c.nodes[2])
            .await
            .offer_activity_task(authorized(&c.nodes[0], offer_wire(&refreshed)))
            .await
            .expect("duplicate push")
            .into_inner();
        assert_eq!(
            response.outcome,
            wire::ActivityOfferOutcome::Known as i32,
            "replacement duplicate outcome: {response:?}"
        );
    }
    let before = c.state().await.transition_seq;
    c.assert_started(0, &poll(c.nodes[2].address, &c.queue).await)
        .await;
    assert_eq!(
        c.state().await.transition_seq.0,
        before.0 + 1,
        "multiple starts committed after queue movement: before={before:?}, state={:?}",
        c.state().await
    );
    let old_retained = c.nodes[1]
        .runtime
        .activity_broker()
        .contains_activity_task(&initial.offer.task)
        .await;
    assert!(
        !old_retained,
        "old incarnation survived queue movement: {old_retained}"
    );
    c.committed_starts(1);
    c.discovery_paused();
}

async fn execution_movement(index: u32, during: bool, duplicates: usize) {
    let mut c = Cluster::new(index, 3).await;
    execution_movement_oracle(&mut c, during, duplicates).await;
    c.stop().await;
}

async fn execution_movement_oracle<R: RunRepository + LeaseRepository + 'static>(
    c: &mut Cluster<R>,
    during: bool,
    duplicates: usize,
) {
    c.schedule(false).await;
    let receipt = c.admitted(0).await;
    let home = execution(&c.request);
    let local_queue = QueueKey {
        task_kind: TaskKind::Activity,
        ..normal_queue(&c.request)
    };
    let mut local_hint = offer_wire(&receipt);
    local_hint.destination_home = home.0;
    local_hint.selected_queue.as_mut().unwrap().name = local_queue.task_queue.0.clone();
    let admitted = client(&c.nodes[0])
        .await
        .offer_activity_task(authorized(&c.nodes[1], local_hint))
        .await
        .expect("old execution home transport offer")
        .into_inner();
    assert_eq!(
        admitted.outcome,
        wire::ActivityOfferOutcome::Admitted as i32,
        "old execution home admission: {admitted:?}"
    );
    let mut held = None;
    if during {
        let (entered, resume) = c.nodes[0].repo.pause_commit(c.request.run_key);
        let address = c.nodes[1].address;
        let queue = c.queue.clone();
        let pending = tokio::spawn(async move { poll(address, &queue).await });
        tokio::time::timeout(BOUND, entered)
            .await
            .expect("old owner commit barrier")
            .expect("old commit reached");
        held = Some((pending, resume));
    }
    c.store
        .relinquish_bundle(home, "activity-node-0".into(), c.epochs[0])
        .await
        .expect("release old execution lease");
    let next = c.nodes[2]
        .runtime
        .acquire_shard(home)
        .await
        .expect("activate new execution owner");
    assert!(
        next.0 > c.epochs[0].0,
        "execution epochs: old={:?}, next={next:?}",
        c.epochs[0]
    );
    assert!(
        c.nodes[0].runtime.active_shards().contains(&home),
        "old execution acquisition did not overlap: {:?}",
        c.nodes[0].runtime.active_shards()
    );
    c.epochs[0] = next;
    c.routes(2, 1);
    c.nodes[0].runtime.relinquish_shard(home).await;
    wait_retired(&c.nodes[0], &local_queue).await;
    let retained = c.nodes[1]
        .runtime
        .activity_broker()
        .contains_activity_task(&receipt.offer.task)
        .await;
    assert!(
        retained,
        "execution takeover retired the independent queue home's offer: {retained}"
    );
    for _ in 0..duplicates {
        let response = client(&c.nodes[1])
            .await
            .offer_activity_task(authorized(&c.nodes[0], offer_wire(&receipt)))
            .await
            .expect("former publisher duplicate")
            .into_inner();
        assert_eq!(
            response.outcome,
            wire::ActivityOfferOutcome::Known as i32,
            "former publisher current hint: {response:?}"
        );
    }
    let before = c.state().await.transition_seq;
    let response = if let Some((pending, resume)) = held {
        resume.send(()).expect("release old owner commit");
        pending.await.expect("takeover poll joined")
    } else {
        poll(c.nodes[1].address, &c.queue).await
    };
    c.assert_started(2, &response).await;
    assert_eq!(
        c.state().await.transition_seq.0,
        before.0 + 1,
        "takeover committed more than one start: before={before:?}, state={:?}",
        c.state().await
    );
    let token: ActivityTaskToken = serde_json::from_slice(&response.task_token).unwrap();
    assert_eq!(
        token.shard_epoch, next,
        "start token epoch: {token:?}, expected={next:?}"
    );
    if during {
        let old = c.nodes[0].starts.lock().unwrap().clone();
        let new = c.nodes[2].starts.lock().unwrap().clone();
        assert_eq!(
            old[0].request_id, new[0].request_id,
            "takeover retry identity: old={old:?}, new={new:?}"
        );
    }
    c.committed_starts(1);
    c.discovery_paused();
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(concat!(env!("CARGO_MANIFEST_DIR"), "/src/peer/activity_delivery_tests.proptest-regressions")))), ..ProptestConfig::default() })]
    // Feature: activity-queue-home-delivery, Property 3: overlapping queue acquisitions retire old offers and refresh to one new home.
    #[test]
    fn grpc_activity_queue_movement(index in 100u32..10000, duplicates in 1usize..5) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(queue_movement(index, duplicates));
    }
    // Feature: activity-queue-home-delivery, Property 5: only the new execution fence can commit a start during takeover.
    #[test]
    fn grpc_activity_execution_movement(index in 100u32..10000, during in any::<bool>(), duplicates in 1usize..5) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(execution_movement(index, during, duplicates));
    }
}

#[derive(Clone, Debug)]
struct Reads {
    loads: Vec<usize>,
    starts: Vec<usize>,
    history: Vec<usize>,
    discovery: Vec<usize>,
    observations: Vec<usize>,
}
impl Reads {
    fn at<R: RunRepository + LeaseRepository + 'static>(c: &Cluster<R>) -> Self {
        Self {
            loads: c
                .nodes
                .iter()
                .map(|n| n.repo.loads.load(Ordering::SeqCst))
                .collect(),
            starts: c
                .nodes
                .iter()
                .map(|n| n.repo.start_loads.load(Ordering::SeqCst))
                .collect(),
            history: c
                .nodes
                .iter()
                .map(|n| n.repo.history_reads.load(Ordering::SeqCst))
                .collect(),
            discovery: c
                .nodes
                .iter()
                .map(|n| n.repo.activity.reads.load(Ordering::SeqCst))
                .collect(),
            observations: c
                .nodes
                .iter()
                .map(|n| n.repo.observations.load(Ordering::SeqCst))
                .collect(),
        }
    }
    fn since(self, before: &Self) -> Self {
        fn delta(after: Vec<usize>, before: &[usize]) -> Vec<usize> {
            after.into_iter().zip(before).map(|(a, b)| a - b).collect()
        }
        Self {
            loads: delta(self.loads, &before.loads),
            starts: delta(self.starts, &before.starts),
            history: delta(self.history, &before.history),
            discovery: delta(self.discovery, &before.discovery),
            observations: delta(self.observations, &before.observations),
        }
    }
}

async fn read_matrix(warm: bool, conflict: bool) {
    let mut c = Cluster::new(41, 3).await;
    let before_publication = Reads::at(&c);
    c.schedule(false).await;
    c.admitted(0).await;
    let publication = Reads::at(&c).since(&before_publication);
    let before_background = Reads::at(&c);
    let owner = if warm { 0 } else { 2 };
    if warm {
        let now = c.clock.now();
        c.nodes[0]
            .runtime
            .submit(
                c.request.run_key,
                Command::Signal(SignalRequest {
                    signal_name: "warm-cache".into(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request: fresh_request(now),
                    now,
                }),
            )
            .await
            .expect("prime execution cache");
    } else {
        c.nodes[1]
            .runtime
            .activity_broker()
            .remove_run(c.request.run_key)
            .await;
        let home = execution(&c.request);
        c.nodes[0].runtime.relinquish_shard(home).await;
        c.store
            .relinquish_bundle(home, "activity-node-0".into(), c.epochs[0])
            .await
            .expect("release warmed owner");
        c.routes(owner, 1);
        c.epochs[0] = c.nodes[owner]
            .runtime
            .acquire_shard(home)
            .await
            .expect("activate fresh owner");
        c.routes(owner, 1);
        c.admitted(owner).await;
    }
    let background = Reads::at(&c).since(&before_background);
    let before_start = Reads::at(&c);
    let before_state = c.state().await;
    c.nodes[owner]
        .repo
        .conflicts
        .store(usize::from(conflict), Ordering::SeqCst);
    let response = poll(c.nodes[1].address, &c.queue).await;
    let start = Reads::at(&c).since(&before_start);
    let expected = 1 + usize::from(conflict);
    let concurrent_background: Vec<_> = start
        .loads
        .iter()
        .zip(&start.starts)
        .map(|(all, starts)| all - starts)
        .collect();
    let report = format!(
        "warm={warm}, conflict={conflict}, publication={publication:?}, background={background:?}, concurrent_background={concurrent_background:?}, start={start:?}, physical SQL=[] (in-memory repository)"
    );
    let mut expected_starts = vec![0, 0, 0];
    expected_starts[owner] = expected;
    assert_eq!(start.starts, expected_starts, "{report}");
    assert_eq!(start.observations, vec![0, 0, 0], "{report}");
    assert_eq!(
        c.nodes[owner].repo.rejected.load(Ordering::SeqCst),
        usize::from(conflict),
        "{report}"
    );
    assert_eq!(
        c.state().await.transition_seq.0,
        before_state.transition_seq.0 + 1,
        "{report}"
    );
    assert!(
        start
            .loads
            .iter()
            .zip(&start.starts)
            .all(|(all, starts)| all >= starts),
        "unaccounted reads: {report}"
    );
    c.assert_started(owner, &response).await;
    c.committed_starts(1);
    c.discovery_paused();
    c.stop().await;
}

#[tokio::test]
async fn grpc_activity_reads_warm() {
    read_matrix(true, false).await;
}
#[tokio::test]
async fn grpc_activity_reads_cold() {
    read_matrix(false, false).await;
}
#[tokio::test]
async fn grpc_activity_reads_warm_conflict() {
    read_matrix(true, true).await;
}
#[tokio::test]
async fn grpc_activity_reads_cold_conflict() {
    read_matrix(false, true).await;
}

// Feature: activity-queue-home-delivery, Property 3: cancellation and a late deadline preserve replacement and colocated homes without discovery.
#[tokio::test]
async fn grpc_activity_retirement_isolated_from_replacement_and_other_home() {
    let mut c = Cluster::new(51, 3).await;
    retirement_oracle(&mut c).await;
    c.stop().await;
}

async fn retirement_oracle<R: RunRepository + LeaseRepository + 'static>(c: &mut Cluster<R>) {
    let home = queue_home(&c.queue);
    let other = (0..1000)
        .map(|index| QueueKey {
            task_queue: TaskQueueName(format!("other-activity-{index}")),
            ..c.queue.clone()
        })
        .find(|q| queue_home(q) != home && queue_home(q) != execution(&c.request))
        .expect("independent queue home");
    c.nodes[1]
        .runtime
        .acquire_shard(queue_home(&other))
        .await
        .expect("colocated independent home");
    c.nodes[1].runtime.relinquish_shard(home).await;
    c.store
        .relinquish_bundle(home, "activity-node-1".into(), c.epochs[1])
        .await
        .unwrap();
    c.nodes[1].repo.lease_seconds.store(3, Ordering::SeqCst);
    c.epochs[1] = c.nodes[1]
        .runtime
        .acquire_shard(home)
        .await
        .expect("bounded acquisition");
    c.nodes[1].repo.lease_seconds.store(0, Ordering::SeqCst);
    c.routes(0, 1);
    c.schedule(false).await;
    let receipt = c.admitted(0).await;
    let mut unrelated = offer_wire(&receipt);
    unrelated.destination_home = queue_home(&other).0;
    unrelated.selected_queue.as_mut().unwrap().name = other.task_queue.0.clone();
    unrelated.run_key = uuid::Uuid::new_v4().as_bytes().to_vec();
    let response = client(&c.nodes[1])
        .await
        .offer_activity_task(authorized(&c.nodes[0], unrelated.clone()))
        .await
        .expect("colocated transport offer")
        .into_inner();
    assert_eq!(
        response.outcome,
        wire::ActivityOfferOutcome::Admitted as i32,
        "other home admission: {response:?}"
    );
    c.nodes[1].runtime.relinquish_shard(home).await;
    wait_retired(&c.nodes[1], &c.queue).await;
    let other_retained = c.nodes[1]
        .runtime
        .activity_broker()
        .has_runnable_backlog(&other)
        .await;
    assert!(
        other_retained,
        "cancellation removed the other home's offer: {other_retained}"
    );
    c.store
        .relinquish_bundle(home, "activity-node-1".into(), c.epochs[1])
        .await
        .unwrap();
    c.epochs[1] = c.nodes[1]
        .runtime
        .acquire_shard(home)
        .await
        .expect("replacement acquisition");
    c.routes(0, 1);
    let replacement = client(&c.nodes[1])
        .await
        .offer_activity_task(authorized(&c.nodes[0], offer_wire(&receipt)))
        .await
        .expect("replacement transport offer")
        .into_inner();
    assert_eq!(
        replacement.outcome,
        wire::ActivityOfferOutcome::Admitted as i32,
        "replacement admission: {replacement:?}"
    );
    // Advance only after the socket work completes; no elapsed real-time sleep
    // or discovery tick is needed to cross the cancelled acquisition's deadline.
    tokio::time::pause();
    c.clock.advance(4);
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::time::resume();
    let replacement_retained = c.nodes[1]
        .runtime
        .activity_broker()
        .contains_activity_task(&receipt.offer.task)
        .await;
    let other_retained = c.nodes[1]
        .runtime
        .activity_broker()
        .has_runnable_backlog(&other)
        .await;
    assert!(
        replacement_retained && other_retained,
        "late deadline changed independent records: replacement={replacement_retained}, other={other_retained}"
    );
    let response = client(&c.nodes[1])
        .await
        .offer_activity_task(authorized(&c.nodes[0], unrelated))
        .await
        .expect("other home duplicate")
        .into_inner();
    assert_eq!(
        response.outcome,
        wire::ActivityOfferOutcome::Known as i32,
        "other home after deadline: {response:?}"
    );
    c.assert_started(0, &poll(c.nodes[1].address, &c.queue).await)
        .await;
    c.committed_starts(1);
    c.discovery_paused();
}
