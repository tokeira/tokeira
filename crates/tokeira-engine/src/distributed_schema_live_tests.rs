//! Live checks of a listener-backed distributed node's schema check, on an
//! operator-supplied Aurora DSQL cluster and the two DynamoDB tables that
//! coordinate its connections (`docs/testing/dsql-live-suites.md`).
//!
//! Each test brings the schema to this release's target first, so the tests
//! run in any order and again on the same cluster. They are ignored by default
//! and fail when their environment is unset.

use std::{collections::BTreeSet, net::TcpListener as StdTcpListener};

use aws_sdk_dynamodb::types::AttributeValue;
use tokeira_proto::workflowservice::{
    GetSystemInfoRequest, workflow_service_client::WorkflowServiceClient,
};
use tokeira_storage::dsql::{ConnectionFactory, schema_state_for_tests};

use super::*;

/// The cluster and tables the tests run against, from the environment.
struct Live {
    endpoint: String,
    region: String,
    rate_limiter_table: String,
    conn_lease_table: String,
}

impl Live {
    fn from_env() -> Self {
        let var = |name: &str| {
            std::env::var(name)
                .unwrap_or_else(|_| panic!("set {name} to run the live schema check"))
        };
        Self {
            endpoint: var("TOKEIRA_DSQL_DISTRIBUTED_ENDPOINT"),
            region: var("TOKEIRA_DSQL_DISTRIBUTED_REGION"),
            rate_limiter_table: var("TOKEIRA_DSQL_DISTRIBUTED_RATE_LIMITER_TABLE"),
            conn_lease_table: var("TOKEIRA_DSQL_DISTRIBUTED_CONN_LEASE_TABLE"),
        }
    }

    /// A distributed node's configuration, its Nexus completion listener on
    /// `nexus`.
    fn config(&self, nexus: u16) -> TokeiraConfig {
        let mut config = TokeiraConfig::default();
        config.infrastructure.storage = ConfigStorageKind::Dsql;
        config.infrastructure.dsql.endpoint = Some(self.endpoint.clone());
        config.infrastructure.dsql.region = Some(self.region.clone());
        config.infrastructure.dsql.rate_limiter_table = Some(self.rate_limiter_table.clone());
        config.infrastructure.dsql.conn_lease_table = Some(self.conn_lease_table.clone());
        config.policy.nexus_completion.http_addr = format!("127.0.0.1:{nexus}");
        config.policy.nexus_completion.system_callback_url = format!("http://127.0.0.1:{nexus}");
        config
            .validate()
            .expect("a valid distributed configuration");
        config
    }

    /// Start a distributed node, its gRPC listener on `grpc`.
    async fn start(
        &self,
        grpc: u16,
        nexus: u16,
    ) -> Result<(ServerStack, Option<EngineSnapshotPolicy>)> {
        build_and_serve(
            SocketAddr::from(([127, 0, 0, 1], grpc)),
            Arc::new(self.config(nexus)),
        )
        .await
    }

    async fn dynamodb(&self) -> aws_sdk_dynamodb::Client {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()))
            .load()
            .await;
        aws_sdk_dynamodb::Client::new(&sdk_config)
    }

    /// The owners of the connection-slot blocks in the lease table. A node
    /// that leaves its blocks owned adds its own owner here.
    async fn slot_block_owners(&self, dynamodb: &aws_sdk_dynamodb::Client) -> BTreeSet<String> {
        let mut owners = BTreeSet::new();
        let mut start = None;
        loop {
            let page = dynamodb
                .scan()
                .table_name(&self.conn_lease_table)
                .set_exclusive_start_key(start)
                .send()
                .await
                .expect("the connection-lease table scans");
            for item in page.items() {
                if let Some(AttributeValue::S(owner)) = item.get("owner_id") {
                    owners.insert(owner.clone());
                }
            }
            match page.last_evaluated_key() {
                Some(key) if !key.is_empty() => start = Some(key.clone()),
                _ => return owners,
            }
        }
    }
}

/// A loopback port nothing is bound to.
fn free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free loopback port")
        .port()
}

fn bindable(port: u16) -> bool {
    StdTcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Start a node against the schema as the test left it, expect a refusal
/// whose error passes `$check`, and show that the refusal left nothing
/// started.
macro_rules! assert_refused_and_nothing_started {
    ($live:expr, $connection:expr, $check:expr) => {{
        let dynamodb = $live.dynamodb().await;
        let before = schema_state_for_tests::state($connection)
            .await
            .expect("the schema state reads");
        let owners_before = $live.slot_block_owners(&dynamodb).await;
        let (grpc, nexus) = (free_port(), free_port());
        let error = $live
            .start(grpc, nexus)
            .await
            .expect_err("the node refuses to start");
        let message = format!("{error:#}");
        assert!(($check)(&message), "unexpected refusal: {message}");
        assert_eq!(
            schema_state_for_tests::state($connection)
                .await
                .expect("the schema state reads"),
            before,
            "a refused start changes no ledger row, compatibility record or shard lease"
        );
        let owners_after = $live.slot_block_owners(&dynamodb).await;
        assert!(
            owners_after.is_subset(&owners_before),
            "a refused start releases its connection-slot blocks: {owners_before:?} then {owners_after:?}"
        );
        assert!(
            bindable(grpc) && bindable(nexus),
            "a refused start binds no listener"
        );
    }};
}

/// A schema one migration behind the release's target is refused, naming both
/// versions and the way to migrate (`distributed-schema-startup-check` 2.3,
/// 2.5).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live Aurora DSQL cluster and two DynamoDB tables; see docs/testing/dsql-live-suites.md"]
async fn a_schema_behind_the_target_is_refused_and_nothing_starts() {
    let live = Live::from_env();
    let mut connection = ConnectionFactory::new(&live.endpoint, &live.region)
        .expect("a connection factory")
        .create_connection()
        .await
        .expect("an admin connection");
    schema_state_for_tests::bring_to_target(&mut connection)
        .await
        .expect("the schema reaches the target");
    let target = MigrationRunner::compatibility_contract().target_version;
    schema_state_for_tests::forget_versions_from(&mut connection, target)
        .await
        .expect("the target is forgotten");

    assert_refused_and_nothing_started!(live, &mut connection, |message: &str| {
        message.contains(&format!("V{}", target - 1))
            && message.contains(&format!("V{target}"))
            && message.contains("tkr schema setup")
    });

    schema_state_for_tests::bring_to_target(&mut connection)
        .await
        .expect("the schema returns to the target");
}

/// The schema at the release's target starts a node that serves (3.4).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live Aurora DSQL cluster and two DynamoDB tables; see docs/testing/dsql-live-suites.md"]
async fn a_schema_at_the_target_starts_and_serves() {
    let live = Live::from_env();
    let mut connection = ConnectionFactory::new(&live.endpoint, &live.region)
        .expect("a connection factory")
        .create_connection()
        .await
        .expect("an admin connection");
    schema_state_for_tests::bring_to_target(&mut connection)
        .await
        .expect("the schema reaches the target");

    let ((server_task, bound_addr, shutdown_tx, background_cancel, _logs, _recorder), _snapshot) =
        live.start(free_port(), free_port())
            .await
            .expect("the node starts on the target schema");
    let mut client = WorkflowServiceClient::connect(format!("http://{bound_addr}"))
        .await
        .expect("the node's listener accepts");
    client
        .get_system_info(GetSystemInfoRequest::default())
        .await
        .expect("the node serves");
    let _ = shutdown_tx.send(());
    background_cancel.cancel();
    server_task
        .await
        .expect("the server task joins")
        .expect("the node shuts down cleanly");
}

/// A schema one version past what the release reads is refused with the
/// assessment's report of a future schema (2.4, 2.5).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live Aurora DSQL cluster and two DynamoDB tables; see docs/testing/dsql-live-suites.md"]
async fn a_schema_past_what_the_release_reads_is_refused_and_nothing_starts() {
    let live = Live::from_env();
    let mut connection = ConnectionFactory::new(&live.endpoint, &live.region)
        .expect("a connection factory")
        .create_connection()
        .await
        .expect("an admin connection");
    schema_state_for_tests::bring_to_target(&mut connection)
        .await
        .expect("the schema reaches the target");
    let later = MigrationRunner::compatibility_contract().maximum_readable_version + 1;
    schema_state_for_tests::record_unknown_version(&mut connection, later)
        .await
        .expect("a later version is recorded");

    assert_refused_and_nothing_started!(live, &mut connection, |message: &str| {
        message.contains("FutureSchema") && message.contains("a later release")
    });

    schema_state_for_tests::bring_to_target(&mut connection)
        .await
        .expect("the schema returns to the target");
}
