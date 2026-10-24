//! Live check of the single-node claim a listener-backed distributed node
//! without a controller takes, on an ephemeral Aurora DSQL cluster and the two
//! DynamoDB tables that coordinate its connections
//! (`docs/testing/dsql-live-suites.md`).
//!
//! The test is ignored by default and fails when its environment is unset. Its
//! crash step waits out a full quiescence, so it takes about seven minutes.

use std::time::{Duration as StdDuration, Instant};

use tokeira_proto::workflowservice::{
    GetSystemInfoRequest, workflow_service_client::WorkflowServiceClient,
};
use tokeira_storage::dsql::{
    ConnectionFactory, EXPIRED_TAKEOVER_QUIESCENCE, control_lease_state_for_tests,
    schema_state_for_tests,
};

use super::{
    distributed_schema_live_tests::{Live, bindable, free_port},
    node_claim::{ACQUIRE_WINDOW, CLAIM_NAME, Loss, NodeClaim, OWNER_PREFIX},
    *,
};

/// Whether a node's listener accepts and answers `GetSystemInfo`.
async fn serves(addr: SocketAddr) -> bool {
    match WorkflowServiceClient::connect(format!("http://{addr}")).await {
        Ok(mut client) => client
            .get_system_info(GetSystemInfoRequest::default())
            .await
            .is_ok(),
        Err(_) => false,
    }
}

/// A started node and its claim.
struct Node {
    stack: ServerStack,
    claim: NodeClaim,
    grpc: u16,
}

impl Node {
    async fn start(live: &Live) -> Result<Self> {
        let grpc = free_port();
        let (stack, _snapshot, claim) = live.start(grpc, free_port()).await?;
        let claim = claim.ok_or_else(|| anyhow!("a node without a controller holds no claim"))?;
        Ok(Self { stack, claim, grpc })
    }

    fn addr(&self) -> SocketAddr {
        self.stack.1
    }

    /// Stop as `tokeirad` does on SIGTERM: drain the server, cancel background
    /// work, and release the claim.
    async fn stop(self) -> Result<()> {
        let (server_task, _addr, shutdown_tx, background_cancel, _logs, _recorder) = self.stack;
        let _ = shutdown_tx.send(());
        server_task.await.context("the server task joins")??;
        background_cancel.cancel();
        self.claim.shut_down().await
    }

    /// Die without releasing anything: the claim's renewals stop and its gate
    /// admits nothing, then the server and background work stop.
    async fn crash(self) {
        let (server_task, _addr, shutdown_tx, background_cancel, _logs, _recorder) = self.stack;
        self.claim.crash();
        background_cancel.cancel();
        let _ = shutdown_tx.send(());
        let _ = server_task.await;
    }
}

/// One node without a controller serves a DSQL cluster at a time
/// (`single-node-claim` 2.1–2.8):
/// 1. a second node refuses to start while the first serves, and leaves
///    nothing started;
/// 2. after a clean stop, the next node starts at once;
/// 3. after a crash, the next node serves only once the crashed node's hold
///    has expired and the quiescence has passed;
/// 4. a node whose claim another owner takes stops serving and returns an
///    error.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live Aurora DSQL cluster and two DynamoDB tables; see docs/testing/dsql-live-suites.md"]
async fn one_node_without_a_controller_serves_a_cluster() {
    let live = Live::from_env();
    let mut connection = ConnectionFactory::new(&live.endpoint, &live.region)
        .expect("a connection factory")
        .create_connection()
        .await
        .expect("an admin connection");
    schema_state_for_tests::bring_to_target(&mut connection)
        .await
        .expect("the schema reaches the target");
    control_lease_state_for_tests::remove(&mut connection, CLAIM_NAME)
        .await
        .expect("the run starts from no claim");
    let dynamodb = live.dynamodb().await;

    // 1. A second node refuses, and nothing of it starts.
    let a = Node::start(&live).await.expect("node A starts");
    let state_before = schema_state_for_tests::state(&mut connection)
        .await
        .expect("the schema state reads");
    let claim_before = control_lease_state_for_tests::claim(&mut connection, CLAIM_NAME)
        .await
        .expect("the claim reads")
        .expect("node A's claim exists");
    let a_owner = claim_before
        .owner_id
        .clone()
        .expect("node A holds the claim");
    assert!(a_owner.starts_with(OWNER_PREFIX), "{a_owner}");
    let slot_owners_before = live.slot_block_owners(&dynamodb).await;
    let (grpc, nexus) = (free_port(), free_port());
    let attempted = Instant::now();
    let refusal = match live.start(grpc, nexus).await {
        Err(refusal) => refusal,
        Ok(_) => panic!("node B started while node A served"),
    };
    let tried_for = attempted.elapsed();
    let message = format!("{refusal:#}");
    assert!(
        message.contains("refuses to start")
            && message.contains(&a_owner)
            && message.contains("stop that node first"),
        "the refusal names the holder and what to do: {message}"
    );
    assert!(
        tried_for >= ACQUIRE_WINDOW - StdDuration::from_secs(5)
            && tried_for < ACQUIRE_WINDOW + StdDuration::from_secs(30),
        "node B tries for 80 s before it refuses, not {tried_for:?}"
    );
    assert!(
        bindable(grpc) && bindable(nexus),
        "a refused start binds no listener"
    );
    assert_eq!(
        schema_state_for_tests::state(&mut connection)
            .await
            .expect("the schema state reads"),
        state_before,
        "a refused start changes no ledger row, compatibility record or shard lease"
    );
    let claim_after = control_lease_state_for_tests::claim(&mut connection, CLAIM_NAME)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert_eq!(
        (claim_after.owner_id, claim_after.fence_token),
        (claim_before.owner_id, claim_before.fence_token),
        "a refused start leaves node A's claim with node A"
    );
    let slot_owners_after = live.slot_block_owners(&dynamodb).await;
    assert!(
        slot_owners_after.is_subset(&slot_owners_before),
        "a refused start releases its connection-slot blocks: {slot_owners_before:?} then \
         {slot_owners_after:?}"
    );
    assert!(serves(a.addr()).await, "node A still serves");

    // 2. A clean handover: the next node starts at once.
    a.stop()
        .await
        .expect("node A stops cleanly and releases its claim");
    let released = control_lease_state_for_tests::claim(&mut connection, CLAIM_NAME)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert_eq!(released.owner_id, None, "node A released its claim");
    let attempted = Instant::now();
    let b = Node::start(&live)
        .await
        .expect("node B starts after the release");
    assert!(
        attempted.elapsed() < StdDuration::from_secs(60),
        "node B starts at once after a release, not after {:?}",
        attempted.elapsed()
    );
    assert!(serves(b.addr()).await, "node B serves");
    b.stop()
        .await
        .expect("node B stops cleanly and releases its claim");

    // 3. A crash: the next node waits out the hold and the quiescence.
    let a = Node::start(&live).await.expect("node A starts again");
    a.crash().await;
    let crashed_hold = control_lease_state_for_tests::claim(&mut connection, CLAIM_NAME)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert!(
        crashed_hold.owner_id.is_some(),
        "a crashed node leaves its claim held"
    );
    let b = Node::start(&live)
        .await
        .expect("node B starts once the crashed node's hold has expired");
    let serving_at = control_lease_state_for_tests::database_now(&mut connection)
        .await
        .expect("the database's clock reads");
    assert!(
        serving_at >= crashed_hold.expires_at + EXPIRED_TAKEOVER_QUIESCENCE,
        "node B serves only after the crashed hold expired at {} and 5 minutes passed, not at \
         {serving_at}",
        crashed_hold.expires_at
    );
    assert!(serves(b.addr()).await, "node B serves");

    // 4. A lost claim: another owner takes it, and the node stops.
    control_lease_state_for_tests::give_to(
        &mut connection,
        CLAIM_NAME,
        "tokeirad-elsewhere",
        60_000,
    )
    .await
    .expect("the claim goes to another owner");
    let Node {
        stack: (server_task, _addr, _shutdown_tx, background_cancel, _logs, _recorder),
        mut claim,
        grpc,
    } = b;
    let loss = tokio::time::timeout(StdDuration::from_secs(45), claim.lost())
        .await
        .expect("node B's next renewal finds another owner");
    assert_eq!(loss, Loss::Fenced);
    tokio::time::timeout(StdDuration::from_secs(60), server_task)
        .await
        .expect("the lost claim drains node B's server")
        .expect("the server task joins")
        .expect("node B's server drains cleanly");
    background_cancel.cancel();
    let error = claim
        .shut_down()
        .await
        .expect_err("node B returns an error for its lost claim");
    assert!(
        format!("{error:#}").contains("lost its single-node claim"),
        "{error:#}"
    );
    assert!(bindable(grpc), "node B stopped serving");

    control_lease_state_for_tests::remove(&mut connection, CLAIM_NAME)
        .await
        .expect("the claim is removed for the next run");
}
