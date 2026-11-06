use std::sync::atomic::AtomicUsize;

use proptest::prelude::*;

use super::*;

const RATE: f64 = 100.0;
const INTERVAL_US: i64 = 10_000;
const EPOCH_US: i64 = 1_800_000_000_000_000;

/// A node's clock: a shared true time plus the node's fixed skew.
#[derive(Debug)]
struct NodeClock {
    now: Arc<AtomicI64>,
    skew_us: i64,
}

impl WallClock for NodeClock {
    fn now_us(&self) -> i64 {
        self.now.load(Ordering::Acquire) + self.skew_us
    }
}

fn node(
    store: &Arc<InProcessArrivalTime>,
    now: &Arc<AtomicI64>,
    skew_us: i64,
    burst: u64,
) -> DistributedTokenBucket {
    DistributedTokenBucket::with_store(
        Arc::clone(store) as Arc<dyn ArrivalTimeStore>,
        Arc::new(NodeClock {
            now: Arc::clone(now),
            skew_us,
        }),
        "cluster.dsql.example.on.aws",
        RATE,
        burst,
    )
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

#[tokio::test]
async fn an_idle_and_a_busy_admission_each_take_one_update_and_a_wrong_guess_two() {
    let store = Arc::new(InProcessArrivalTime::default());
    let now = Arc::new(AtomicI64::new(EPOCH_US));
    let first = node(&store, &now, 0, 1_000);
    let second = node(&store, &now, 0, 1_000);

    // An idle bucket admits now, in one update.
    let idle = first.reserve().await.unwrap().unwrap();
    assert_eq!(
        idle,
        Reservation {
            scheduled_us: EPOCH_US,
            requested_us: EPOCH_US,
            updates: 1
        }
    );
    // The first node saw the bucket busy, and guesses right.
    let busy = first.reserve().await.unwrap().unwrap();
    assert_eq!((busy.scheduled_us, busy.updates), (EPOCH_US, 1));
    // The second node has seen nothing and guesses idle: one failed
    // condition, then the busy update.
    let guessed = second.reserve().await.unwrap().unwrap();
    assert_eq!((guessed.scheduled_us, guessed.updates), (EPOCH_US, 2));
    assert_eq!(store.updates(), 4);

    // Once the arrival time passes, the first node's busy guess is wrong.
    now.fetch_add(INTERVAL_US * 4, Ordering::AcqRel);
    let after = first.reserve().await.unwrap().unwrap();
    assert_eq!(after.updates, 1, "a past arrival time guesses idle");
}

#[tokio::test]
async fn a_bucket_past_its_horizon_refuses_and_is_left_unchanged() {
    let store = Arc::new(InProcessArrivalTime::default());
    let now = Arc::new(AtomicI64::new(EPOCH_US));
    let bucket = node(&store, &now, 0, 10);
    // Ten admissions spend the burst; three thousand more fill the thirty
    // seconds an admission may wait.
    for _ in 0..(10 + 3_000) {
        bucket.reserve().await.unwrap().unwrap();
    }
    let updates = store.updates();
    let stored = store.items.lock().unwrap().clone();
    assert_eq!(
        bucket.reserve().await,
        Err(AdmissionError::WaitTooLong(RATE_MAX_WAIT))
    );
    assert_eq!(*store.items.lock().unwrap(), stored);
    assert_eq!(store.updates(), updates + 1);
    assert!(
        !bucket.fallback_active(),
        "a failed condition is no failure"
    );
}

#[tokio::test(start_paused = true)]
async fn admit_sleeps_until_the_scheduled_time() {
    let bucket = DistributedTokenBucket::with_store(
        Arc::new(InProcessArrivalTime::default()),
        Arc::new(VirtualWallClock::at(EPOCH_US)),
        "cluster.dsql.example.on.aws",
        RATE,
        2,
    );
    let started = tokio::time::Instant::now();
    bucket.admit().await.unwrap();
    bucket.admit().await.unwrap();
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "the burst admits at once"
    );
    bucket.admit().await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_micros(INTERVAL_US as u64));
}

#[tokio::test(start_paused = true)]
async fn a_dynamodb_error_admits_through_the_fallback_until_its_period_ends() {
    let store = Arc::new(InProcessArrivalTime::default());
    let bucket = DistributedTokenBucket::with_store(
        Arc::clone(&store) as Arc<dyn ArrivalTimeStore>,
        Arc::new(VirtualWallClock::at(EPOCH_US)),
        "cluster.dsql.example.on.aws",
        RATE,
        1_000,
    );
    store.set_failing(true);
    assert_eq!(bucket.reserve().await, Ok(None));
    assert!(bucket.fallback_active());
    assert_eq!(store.updates(), 1);

    // Within the period nothing calls DynamoDB; the fallback admits at
    // FALLBACK_RATE with a burst of one second's admissions.
    let started = tokio::time::Instant::now();
    for _ in 0..20 {
        bucket.admit().await.unwrap();
    }
    assert_eq!(store.updates(), 1);
    let expected = Duration::from_secs_f64(10.0 / FALLBACK_RATE);
    assert!(
        started.elapsed().abs_diff(expected) < Duration::from_millis(1),
        "{:?}",
        started.elapsed()
    );

    store.set_failing(false);
    tokio::time::advance(FALLBACK_PERIOD).await;
    assert!(!bucket.fallback_active());
    assert!(bucket.reserve().await.unwrap().is_some());
}

#[tokio::test(start_paused = true)]
async fn the_fallback_admits_at_the_directive_rate_share() {
    let store = Arc::new(InProcessArrivalTime::default());
    let bucket = DistributedTokenBucket::with_store(
        Arc::clone(&store) as Arc<dyn ArrivalTimeStore>,
        Arc::new(VirtualWallClock::at(EPOCH_US)),
        "cluster.dsql.example.on.aws",
        RATE,
        1_000,
    );
    bucket.set_fallback_rate(2.0);
    // Rates that are not positive numbers keep the last good one.
    bucket.set_fallback_rate(0.0);
    bucket.set_fallback_rate(f64::NAN);
    store.set_failing(true);
    let started = tokio::time::Instant::now();
    for _ in 0..6 {
        bucket.admit().await.unwrap();
    }
    // A burst of two, then one every half second.
    assert_eq!(started.elapsed(), Duration::from_secs(2));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: dsql-reservoir-redesign, Property 10: Distributed Rate Bound
    #[test]
    fn admissions_from_skewed_nodes_stay_within_the_burst_and_the_rate(
        skews in prop::collection::vec(-50_000i64..50_000, 2..6),
        requests in prop::collection::vec((any::<prop::sample::Index>(), 0i64..4_000_000), 1..800),
        burst in 1u64..60,
    ) {
        let store = Arc::new(InProcessArrivalTime::default());
        let now = Arc::new(AtomicI64::new(EPOCH_US));
        let nodes: Vec<_> = skews.iter().map(|skew| node(&store, &now, *skew, burst)).collect();
        let runtime = runtime();
        // Requests reach the item in any order: an update made late may carry
        // an earlier clock reading than one made before it.
        let mut admitted = Vec::new();
        for (index, at) in requests {
            let which = index.index(nodes.len());
            now.store(EPOCH_US + at, Ordering::Release);
            let before = store.updates();
            match runtime.block_on(nodes[which].reserve()) {
                Ok(Some(reservation)) => {
                    prop_assert!(store.updates() - before <= 2);
                    prop_assert_eq!(reservation.updates as usize, store.updates() - before);
                    let wait = reservation.scheduled_us - reservation.requested_us;
                    prop_assert!((0..=RATE_MAX_WAIT.as_micros() as i64).contains(&wait));
                    // Back to the true clock.
                    admitted.push(reservation.scheduled_us - skews[which]);
                }
                Ok(None) => prop_assert!(false, "a failed condition began a fallback"),
                Err(AdmissionError::WaitTooLong(_)) => prop_assert!(store.updates() - before <= 2),
                Err(AdmissionError::Contended(_)) => prop_assert!(false, "a lone writer was contended"),
            }
        }
        // At most `burst + rate × (w + δ)` admissions in any window of length
        // `w`: for every pair i < j of sorted admission times,
        // (j − i + 1 − burst) × interval ≤ s[j] − s[i] + δ, which is
        // a[j] − a[i] ≤ δ + (burst − 1) × interval for a[k] = k × interval − s[k].
        admitted.sort_unstable();
        let skew = skews.iter().max().unwrap() - skews.iter().min().unwrap();
        let allowance = skew + (i64::try_from(burst).unwrap() - 1) * INTERVAL_US;
        let mut lowest = i64::MAX;
        for (k, scheduled) in admitted.iter().enumerate() {
            let a = i64::try_from(k).unwrap() * INTERVAL_US - scheduled;
            lowest = lowest.min(a);
            prop_assert!(a - lowest <= allowance, "admission {} ran ahead of the bound", k);
        }
    }

    // Feature: dsql-reservoir-redesign, Property 13: Fallback Admissions
    #[test]
    fn the_fallback_admits_no_more_than_one_seconds_burst_plus_its_rate(
        rate in 0.5f64..60.0,
        gaps in prop::collection::vec(0u64..400_000, 1..400),
    ) {
        let mut gcra = LocalGcra::default();
        let origin = tokio::time::Instant::now();
        let mut at = origin;
        let mut admitted = Vec::new();
        for gap in gaps {
            at += Duration::from_micros(gap);
            if let Ok(wait) = gcra.reserve(at, rate) {
                prop_assert!(wait <= RATE_MAX_WAIT);
                admitted.push((at + wait - origin).as_nanos() as i128);
            }
        }
        admitted.sort_unstable();
        let interval = Duration::from_secs_f64(1.0 / rate).as_nanos() as i128;
        let burst = rate.ceil().max(1.0) as i128;
        // a[j] − a[i] ≤ (burst − 1) × interval, as for the shared bucket; one
        // nanosecond per admission absorbs rounding the interval to whole ns.
        let mut lowest = i128::MAX;
        for (k, scheduled) in admitted.iter().enumerate() {
            let a = k as i128 * interval - scheduled;
            lowest = lowest.min(a);
            prop_assert!(a - lowest <= (burst - 1) * interval + k as i128);
        }
    }

    // Feature: dsql-reservoir-redesign, Property 13: Fallback Admissions
    #[test]
    fn a_failed_condition_never_begins_a_fallback(
        stored in prop::collection::vec(prop::option::of(-60_000_000i64..60_000_000), 1..20),
    ) {
        let store = Arc::new(InProcessArrivalTime::default());
        let now = Arc::new(AtomicI64::new(EPOCH_US));
        let bucket = node(&store, &now, 0, 1_000);
        let runtime = runtime();
        for offset in stored {
            {
                let mut items = store.items.lock().unwrap();
                match offset {
                    Some(offset) => items.insert(bucket.key.clone(), EPOCH_US + offset),
                    None => items.remove(&bucket.key),
                };
            }
            let outcome = runtime.block_on(bucket.reserve());
            prop_assert!(!matches!(outcome, Ok(None)));
            prop_assert!(!bucket.fallback_active());
        }
    }
}

/// Opt-in tests of the DynamoDB updates themselves.
///
/// They run against DynamoDB Local when `TOKEIRA_DYNAMODB_LOCAL_ENDPOINT` is
/// set (creating their own table), or against an existing rate limiter table
/// named by `TOKEIRA_DSQL_TEST_RATE_LIMITER_TABLE` in
/// `TOKEIRA_DSQL_TEST_REGION`; with neither, they return without connecting.
/// Each test uses its own item, so they may share a table.
mod dynamodb {
    use aws_sdk_dynamodb::{
        config::{BehaviorVersion, Credentials, Region},
        types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
    };

    use super::*;

    static ITEMS: AtomicUsize = AtomicUsize::new(0);

    async fn table() -> Option<(Client, String)> {
        if let Ok(endpoint) = std::env::var("TOKEIRA_DYNAMODB_LOCAL_ENDPOINT") {
            let config = aws_sdk_dynamodb::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .endpoint_url(endpoint)
                .credentials_provider(Credentials::new("local", "local", None, None, "local"))
                .build();
            let client = Client::from_conf(config);
            let table = "tokeira-dsql-rate-limiter-test".to_owned();
            let created = client
                .create_table()
                .table_name(&table)
                .billing_mode(BillingMode::PayPerRequest)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("pk")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .send()
                .await;
            if let Err(error) = created
                && !error
                    .as_service_error()
                    .is_some_and(|error| error.is_resource_in_use_exception())
            {
                panic!("could not create the test table: {error:?}");
            }
            return Some((client, table));
        }
        let table = std::env::var("TOKEIRA_DSQL_TEST_RATE_LIMITER_TABLE").ok()?;
        let region = std::env::var("TOKEIRA_DSQL_TEST_REGION").ok()?;
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region))
            .load()
            .await;
        Some((Client::new(&config), table))
    }

    /// A fresh item key for this test.
    fn endpoint() -> String {
        format!(
            "test-{}-{}.dsql.example.on.aws",
            std::process::id(),
            ITEMS.fetch_add(1, Ordering::AcqRel)
        )
    }

    fn bucket(
        client: &Client,
        table: &str,
        endpoint: &str,
        clock: Arc<dyn WallClock>,
        burst: u64,
    ) -> DistributedTokenBucket {
        DistributedTokenBucket::with_store(
            Arc::new(DynamoDbArrivalTime {
                client: client.clone(),
                table_name: table.to_owned(),
            }),
            clock,
            endpoint,
            RATE,
            burst,
        )
    }

    #[tokio::test]
    async fn idle_busy_and_horizon_answer_the_first_update_as_designed() {
        let Some((client, table)) = table().await else {
            return;
        };
        let now = Arc::new(AtomicI64::new(EPOCH_US));
        let endpoint = endpoint();
        let clock = |skew_us| -> Arc<dyn WallClock> {
            Arc::new(NodeClock {
                now: Arc::clone(&now),
                skew_us,
            })
        };
        let first = bucket(&client, &table, &endpoint, clock(0), 3);
        let second = bucket(&client, &table, &endpoint, clock(0), 3);
        first.validate_table().await.unwrap();

        // No item: the idle update creates it and admits now.
        let idle = first.reserve().await.unwrap().unwrap();
        assert_eq!((idle.scheduled_us, idle.updates), (EPOCH_US, 1));
        // Busy, guessed right.
        let busy = first.reserve().await.unwrap().unwrap();
        assert_eq!((busy.scheduled_us, busy.updates), (EPOCH_US, 1));
        // Busy, guessed idle: the failed condition returns the item.
        let guessed = second.reserve().await.unwrap().unwrap();
        assert_eq!((guessed.scheduled_us, guessed.updates), (EPOCH_US, 2));
        // The fourth admission waits one interval past the burst of three.
        let waited = first.reserve().await.unwrap().unwrap();
        assert_eq!(waited.scheduled_us, EPOCH_US + INTERVAL_US);

        // Past the horizon: refused, and the item unchanged.
        let behind = bucket(&client, &table, &endpoint, clock(-40_000_000), 3);
        let at_horizon = behind.reserve().await;
        assert_eq!(at_horizon, Err(AdmissionError::WaitTooLong(RATE_MAX_WAIT)));
        let next = first.reserve().await.unwrap().unwrap();
        assert_eq!(next.scheduled_us, EPOCH_US + 2 * INTERVAL_US);
    }

    #[tokio::test]
    async fn two_nodes_share_one_rate_through_the_conditional_updates() {
        let Some((client, table)) = table().await else {
            return;
        };
        let endpoint = endpoint();
        let now = Arc::new(AtomicI64::new(EPOCH_US));
        let nodes: Vec<_> = [0, 7_000]
            .into_iter()
            .map(|skew_us| {
                Arc::new(bucket(
                    &client,
                    &table,
                    &endpoint,
                    Arc::new(NodeClock {
                        now: Arc::clone(&now),
                        skew_us,
                    }),
                    10,
                ))
            })
            .collect();
        let mut tasks = tokio::task::JoinSet::new();
        for node in &nodes {
            for _ in 0..20 {
                let node = Arc::clone(node);
                tasks.spawn(async move { node.reserve().await });
            }
        }
        let mut scheduled = Vec::new();
        while let Some(reservation) = tasks.join_next().await {
            scheduled.push(reservation.unwrap().unwrap().unwrap().scheduled_us);
        }
        // Forty admissions at a burst of ten: thirty intervals past the first,
        // whichever node made them.
        scheduled.sort_unstable();
        let span = scheduled[39] - scheduled[0];
        assert!(span >= 30 * INTERVAL_US - 7_000, "{span}");
    }
}
