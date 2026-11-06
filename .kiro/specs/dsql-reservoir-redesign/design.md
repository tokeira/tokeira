# Design Document: DSQL Reservoir Redesign

## Overview

This design eliminates the sqlx `PgPool` from the DSQL connection path entirely. The current implementation wraps a sqlx PgPool inside the reservoir: the refiller calls `pool.acquire()` to create connections, but the pool's `max_connections` equals `target_ready`, so when the reservoir holds all connections in its ready channel, the pool has nothing left for the refiller — causing `pool timed out` errors under load.

The redesign makes the reservoir the **sole connection owner**. Physical connections are created directly via IAM-authenticated TCP/TLS using `aurora_dsql_sqlx_connector::DsqlConnectOptions` and `aurora_dsql_sqlx_connector::connection::connect_with()`, bypassing the sqlx pool entirely. The reservoir IS the pool.

Three background tokio tasks manage the connection lifecycle:
1. **Refiller** — continuously creates connections to maintain the ready channel at `target_ready`.
2. **Expiry Scanner** — periodically retires connections approaching their IAM token expiry.
3. **Return Processor** — validates returned connections (lifetime and bad-flag checks only, no ping) before reuse.

For multi-node coordination, DynamoDB-backed components distribute the rate budget and connection count:
- **Distributed Token Bucket** — enforces the cluster-wide 100/sec DSQL connection creation rate.
- **Slot Block Manager** — partitions the 10,000 connection limit across nodes using block-based allocation.

All connection management parameters are internal constants derived from DSQL's known constraints. The operator provides an endpoint and credentials. Everything else is automatic.

**Key design change from current implementation:** The return processor no longer pings connections on return. Guard window lifetime checks are sufficient — a connection that was healthy during use and has remaining lifetime is safe to reuse without a network round-trip. This eliminates per-return latency and connection churn.

## Architecture

```mermaid
graph TD
    subgraph "Startup"
        BOOT[tokeirad bootstrap]
        WARMUP[Warmup Phase<br/>fill to target_ready<br/>timeout 30s]
        GRPC[Accept gRPC traffic]
    end

    subgraph "Reservoir Core"
        READY[Ready Channel<br/>async_channel bounded PhysicalConn<br/>capacity = 2 x target_ready]
        RETURN_CH[Return Channel<br/>mpsc unbounded ReturnedConn]
    end

    subgraph "Background Tasks"
        REFILLER[Refiller Loop]
        SCANNER[Expiry Scanner<br/>interval 1s]
        RETURN_PROC[Return Processor<br/>no ping - lifetime and bad-flag checks only]
    end

    subgraph "Rate Coordination - DynamoDB"
        DIST_BUCKET[Distributed Token Bucket<br/>100/sec sustained - 1000 burst]
        SLOT_MGR[Slot Block Manager<br/>100 slots per block]
    end

    subgraph "Connection Factory"
        IAM[IAM Token Provider<br/>fresh token per connection<br/>shared AWS credentials]
        FACTORY[ConnectionFactory<br/>DsqlConnectOptions + connector connect_with]
    end

    subgraph "Admission Control"
        CLASS[Class Budgets<br/>commit 50% - read 20%<br/>projection 10% - control 10%<br/>maintenance 10%]
    end

    BOOT --> WARMUP
    WARMUP --> GRPC
    CLASS --> READY
    READY --> RETURN_CH
    RETURN_CH --> RETURN_PROC
    RETURN_PROC --> READY
    REFILLER --> DIST_BUCKET
    REFILLER --> SLOT_MGR
    REFILLER --> IAM
    IAM --> FACTORY
    FACTORY --> READY
    SCANNER --> READY
```

### Key Design Invariants

1. **The reservoir is the sole connection owner.** No sqlx PgPool exists. The reservoir creates, holds, and retires all DSQL connections.
2. **Checkout is O(1) with no network I/O.** `try_recv()` on the ready channel returns immediately or signals empty.
3. **Connection creation failures never block checkout callers.** The refiller runs independently; its errors are logged and retried with backoff.
4. **No connection is ever handed out with remaining_lifetime < guard_window.** Both the expiry scanner and the return processor enforce this.
5. **Class budgets guarantee isolation.** Projection cannot consume commit-class permits — separate semaphore instances.
6. **DynamoDB coordination is mandatory for DSQL.** No local-only fallback. Unreachable DynamoDB = startup failure.
7. **No ping on return.** Lifetime check and bad-flag check only.
8. **Every reserved slot is released exactly once.** Any path that drops a physical connection created after `acquire_slot()` must call `release_slot()` before the connection leaves the reservoir lifecycle.
9. **A class permit travels with its connection.** A caller's class permit goes back to its class only after the return processor has put the connection back in the ready channel or retired it. Otherwise the next caller of that class could be admitted while the connection it needs is still on its way back, and find the reservoir empty although no more callers hold connections than the budgets allow.
10. **A returned connection always fits in the ready channel.** The channel holds `2 × target_ready`: the refiller fills to `target_ready` ready connections, and the class budgets admit at most `target_ready` callers, each returning one connection. A return therefore never waits for room, and no healthy connection is retired for lack of it.
11. **A connection an operation broke is retired, not reused.** Each storage operation runs inside `in_operation`, which records every connection the operation checks out. When the operation fails with an IO, TLS or protocol error, or the server ends the session with a FATAL error, every recorded connection is flagged broken, and the return processor, checkout and the expiry scanner each retire a flagged connection when they meet it. The flag is set after the operation's permit dropped, so a caller that checks the connection out in that window fails on it the same way and flags it again.


## Components and Interfaces

### 1. PhysicalConn and ReturnedConn

**Location:** `crates/tokeira-storage/src/dsql/reservoir.rs`

```rust
pub struct PhysicalConn {
    pub(crate) connection: PgConnection,  // raw, NOT PoolConnection<Postgres>
    pub(crate) created_at: Instant,
    pub(crate) lifetime: Duration,        // BASE_LIFETIME ± LIFETIME_JITTER
    pub(crate) broken: BrokenFlag,        // set by a failed operation; shared by every holder
}

impl PhysicalConn {
    pub fn remaining_lifetime(&self) -> Duration {
        self.lifetime.saturating_sub(self.created_at.elapsed())
    }
    pub fn within_guard_window(&self, guard_window: Duration) -> bool {
        self.remaining_lifetime() <= guard_window
    }
}

pub struct ReturnedConn {
    pub(crate) connection: PgConnection,
    pub(crate) created_at: Instant,
    pub(crate) lifetime: Duration,
    pub(crate) broken: BrokenFlag,
    pub(crate) marked_bad: bool,
    // The caller's class permit, released by the return processor (invariant 9).
    pub(crate) class_permit: Option<OwnedSemaphorePermit>,
}
```

### 2. Connection Factory

**Location:** `crates/tokeira-storage/src/dsql/connection_factory.rs` (new file)

Creates raw `PgConnection` instances via IAM-authenticated TCP/TLS. Each call signs a fresh IAM token for that connection alone; tokens are never cached.

The AWS credentials that sign the tokens are shared by every connect. The factory builds the default credentials chain once and keeps the credentials it returns until five minutes before they expire, then fetches new ones; one connect fetches while concurrent connects wait for its result. The connector would otherwise build a new credentials chain for each connection, and its token signer asks the provider directly rather than through the SDK's credential cache, so every connect would fetch credentials from the ECS credentials endpoint or STS at up to the cluster's connection rate.

```rust
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const CREDENTIALS_REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

pub struct ConnectionFactory {
    pg_options: PgConnectOptions,         // endpoint, admin, postgres, port 5432, verify-full
    region: String,
    credentials: Arc<CredentialCache>,    // shared by every clone
}

impl ConnectionFactory {
    pub async fn create_connection(&self) -> Result<PgConnection, ConnectionFactoryError> {
        let credentials = self.credentials.current(&self.region).await?;
        // A static provider over the fetched credentials: the connector signs
        // with them as they are.
        let options = self.connect_options(Some(credentials))?;
        aurora_dsql_sqlx_connector::connection::connect_with(&options)
            .await
            .map_err(ConnectionFactoryError::from_dsql_error)
    }
}

/// Every reservoir connects through this bound, whichever factory it holds.
pub(crate) async fn create_within_timeout(
    factory: &dyn PhysicalConnectionFactory,
) -> Result<PgConnection, ConnectionFactoryError> {
    tokio::time::timeout(CONNECT_TIMEOUT, factory.create_connection())
        .await
        .unwrap_or(Err(ConnectionFactoryError::Timeout(CONNECT_TIMEOUT)))
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionFactoryError {
    #[error("DSQL connection configuration failed: {0}")] Config(String),
    #[error("DSQL IAM token generation failed: {0}")] Token(String),
    #[error("DSQL TCP/TLS connection failed: {0}")] Connection(String),
    #[error("DSQL database handshake failed: {0}")] Database(String),
    #[error("DSQL connection attempt did not finish within {0:?}")] Timeout(Duration),
}
```

Neither sqlx nor the connector bounds a connect, so a peer that accepts TCP and never answers would hold an attempt, its in-flight permit and its slot forever. `CONNECT_TIMEOUT` ends the attempt; the attempt then fails like any other and gives its slot back.

### 2.5 DSQL Coordination Config

**Location:** `crates/tokeira-storage/src/dsql/config.rs`

The DynamoDB coordination table names and client are runtime wiring, not
operator-tunable TOML. `apps/tokeirad` constructs this config from the effective
server config before calling `DsqlStore::connect(auth, pool_config)`.

```rust
#[derive(Clone, Debug)]
pub struct DsqlCoordinationConfig {
    pub rate_limiter_table: String,
    pub conn_lease_table: String,
    pub ddb_client: aws_sdk_dynamodb::Client,
}

#[derive(Clone, Debug)]
pub struct DsqlPoolConfig {
    pub reservoir: ReservoirConfig,
    pub migration: MigrationConfig,
    pub coordination: DsqlCoordinationConfig,
    // existing DSQL mechanical fields...
}
```

`DsqlPoolConfig` is constructed programmatically by the DSQL startup path. If
the existing serde derives conflict with the non-serializable DynamoDB client,
remove those derives from `DsqlPoolConfig` or move serde coverage to the
serializable sub-configs that still need it.

`apps/tokeirad/src/lib.rs` derives table names from the effective project
identifier:

```rust
let project = &effective_config.infrastructure.cluster_name;
let rate_limiter_table = format!("{project}-dsql-rate-limiter");
let conn_lease_table = format!("{project}-dsql-conn-lease");
let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .region(aws_config::Region::new(auth.resolved_region().context("missing DSQL region")?))
    .load()
    .await;
let ddb_client = aws_sdk_dynamodb::Client::new(&sdk_config);

let pool_config = DsqlPoolConfig {
    coordination: DsqlCoordinationConfig {
        rate_limiter_table,
        conn_lease_table,
        ddb_client,
    },
    ..dsql_pool_config(&effective_config)
};
let dsql_store = DsqlStore::connect(auth, pool_config).await?;
```

`DsqlStore::connect` remains the single external entry point and uses
`config.coordination` internally to validate DynamoDB reachability, construct
the Distributed_Token_Bucket and Slot_Block_Manager, start the reservoir, warm
it, and build the `DsqlConnectionDirector`.

### 3. Reservoir

**Location:** `crates/tokeira-storage/src/dsql/reservoir.rs`

**Internal constants (NOT configurable):**

| Constant | Value | Derivation |
|----------|-------|------------|
| `TARGET_READY` | 50 | Expected peak concurrency for single-node |
| `BASE_LIFETIME` | 10 min | 70% of DSQL 15-min IAM token TTL |
| `LIFETIME_JITTER` | ±2 min | Prevents synchronized expiry storms |
| `GUARD_WINDOW` | 45 sec | Safety margin before token expiry |
| `INFLIGHT_LIMIT` | 8 | Max concurrent TCP/TLS handshakes |
| `SCAN_INTERVAL` | 1 sec | Expiry scanner frequency |
| `SCAN_BUDGET` | `TARGET_READY / 2` | Max ready entries examined per scan |
| `WARMUP_TIMEOUT` | 30 sec | Max time to wait for initial fill |
| `REFILLER_IDLE_INTERVAL` | 100ms | Check interval when full |
| `REFILLER_ERROR_BACKOFF` | 250ms | Backoff after the first failure; doubles per consecutive failure |
| `REFILLER_MAX_BACKOFF` | 10 sec | Backoff ceiling; a recovering cluster is retried at least this often |
| `CONNECT_TIMEOUT` | 30 sec | Bound on one connect: a credentials refresh plus a TLS handshake to a loaded cluster |
| `READY_CAPACITY` | `2 × TARGET_READY` | Ready channel capacity (invariant 10) |
| `DISTRIBUTED_RATE` | 100/sec | DSQL cluster-wide sustained rate |
| `DISTRIBUTED_BURST` | 1000 | DSQL cluster-wide burst capacity |
| `SLOT_BLOCK_SIZE` | 100 | Connections per block |
| `SLOT_BLOCK_TTL` | 3 min | Crash recovery timeout |
| `SLOT_BLOCK_RENEW` | 1 min | Renewal interval |

```rust
pub struct Reservoir {
    ready_rx: async_channel::Receiver<PhysicalConn>,
    ready_tx: async_channel::Sender<PhysicalConn>,
    return_tx: mpsc::UnboundedSender<ReturnedConn>,
    refiller_handle: JoinHandle<()>,
    scanner_handle: JoinHandle<()>,
    return_processor_handle: JoinHandle<()>,
}

impl Reservoir {
    pub async fn start(factory: ConnectionFactory, bucket: Arc<DistributedTokenBucket>,
                       slots: Arc<SlotBlockManager>) -> Result<Self> { /* ... */ }

    /// Non-blocking checkout. Returns immediately or ErrReservoirEmpty.
    pub fn checkout(&self) -> Result<PhysicalConn, ReservoirError> {
        loop {
            match self.ready_rx.try_recv() {
                // Flagged after it was returned (invariant 11): retire, look again.
                Ok(conn) if conn.broken.is_set() => record_retirement("broken", conn.created_at.elapsed()),
                Ok(conn) => { metrics::record_dsql_pool_connections_total(self.ready_rx.len()); return Ok(conn) }
                Err(_) => { metrics::record_dsql_pool_empty_reservoir(); return Err(ReservoirError::Empty) }
            }
        }
    }

    pub async fn warmup(&self) -> WarmupResult { /* poll until full or timeout */ }
    pub fn return_sender(&self) -> mpsc::UnboundedSender<ReturnedConn> { self.return_tx.clone() }
    pub fn ready_count(&self) -> usize { self.ready_rx.len() }
}
```

### 4. Refiller Loop

One task admits connection creations and runs each in its own child task, so up to `INFLIGHT_LIMIT` connects proceed at once. A refill after a mass retirement takes about `target_ready / INFLIGHT_LIMIT` connect latencies rather than `target_ready`. The refiller counts creations still running against the target, so it never starts more than the target needs.

Consecutive failures share one streak: the refiller waits `refill_backoff(streak)` before admitting the next creation, and the first success resets the streak and cuts a running wait short, so a recovered cluster is refilled at once rather than after the longest backoff.

```rust
// Pseudocode — exact implementation mechanics
let mut creating = JoinSet::new();   // child creations; aborted with the refiller
loop {
    if failures > 0 {
        select! { sleep(refill_backoff(failures)), recovered.notified() }
    }
    let inflight_permit = inflight_sem.acquire_owned().await;  // max 8 concurrent
    reap_finished(&mut creating);
    if ready_tx.len() + creating.len() >= target_ready() { drop(inflight_permit); sleep(100ms); continue; }
    if slot_manager.acquire_slot().await.is_err() { failures += 1; continue; }
    let slot = SlotCharge::new(slot_manager);         // releases the slot on drop
    if let Err(e) = distributed_bucket.wait().await {  // DynamoDB round-trip
        drop(slot); log_warn(e); failures += 1; continue;
    }
    creating.spawn(async move {
        let _inflight_permit = inflight_permit;        // held for the whole attempt
        match create_within_timeout(factory).await {   // CONNECT_TIMEOUT
            Ok(conn) => {
                failures = 0; recovered.notify_waiters();
                let entry = PhysicalConn { connection: conn, created_at: now(),
                                           lifetime: assign_jittered_lifetime(), broken: BrokenFlag::default() };
                if ready_tx.send(entry).await.is_ok() { slot.transfer_to_connection(); }
                // else: shutdown closed the channel; dropping `slot` releases it.
            }
            Err(e) => { failures += 1; log_warn(e); }  // dropping `slot` releases it
        }
    });
}

/// REFILLER_ERROR_BACKOFF × 2^(failures - 1), capped at REFILLER_MAX_BACKOFF, with
/// equal jitter (half fixed, half random) so nodes refused together retry apart.
fn refill_backoff(failures: u32) -> Duration;

fn assign_jittered_lifetime() -> Duration {
    let offset = rand::gen_range(0..=(LIFETIME_JITTER.as_secs() * 2));
    BASE_LIFETIME - LIFETIME_JITTER + Duration::from_secs(offset)
}
```

### 5. Expiry Scanner

The scanner receives an `Arc<SlotBlockManager>` so every discarded physical
connection releases the slot reserved when it was created.

```rust
loop {
    sleep(SCAN_INTERVAL); // 1 second
    for _ in 0..SCAN_BUDGET {  // bounded: max 25 entries per pass
        match ready_rx.try_recv() {
            Ok(conn) if conn.broken.is_set() => {
                metrics::record_retirement("broken", conn.created_at.elapsed());
                slot_manager.release_slot();
                drop(conn);
            }
            Ok(conn) if conn.within_guard_window(GUARD_WINDOW) => {
                metrics::record_retirement("guard_window", conn.created_at.elapsed());
                slot_manager.release_slot();
                drop(conn);
            }
            Ok(conn) => {
                if let Err(err) = ready_tx.try_send(conn) {
                    slot_manager.release_slot();
                    drop(err.into_inner());
                }
            }
            Err(_) => break,
        }
    }
}
```

### 6. Return Processor

The return processor receives the same `Arc<SlotBlockManager>` and releases a
slot whenever it discards a returned connection instead of putting it back into
the ready channel. It releases the caller's class permit last (invariant 9).

```rust
while let Some(returned) = return_rx.recv().await {
    let reason = if returned.marked_bad { Some("bad_flag") }
                 else if returned.broken.is_set() { Some("broken") }
                 else if returned.within_guard_window(GUARD_WINDOW) { Some("guard_window") }
                 else { None };
    if let Some(reason) = reason {
        metrics::record_retirement(reason, returned.created_at.elapsed());
        slot_manager.release_slot();
        continue; // discard; dropping `returned` releases its class permit
    }
    // Put back — NO PING, no network I/O, and never wait: the channel has room
    // for every return (invariant 10).
    match ready_tx.try_send(returned.conn()) {
        Ok(()) => {}
        Err(Full(conn)) => { metrics::record_retirement("ready_channel_full", conn.created_at.elapsed());
                             slot_manager.release_slot(); }
        Err(Closed(_)) => { slot_manager.release_slot(); return; }
    }
    drop(returned.class_permit);
}
```

### 7. Distributed Token Bucket

**Location:** `crates/tokeira-storage/src/dsql/distributed_bucket.rs` (new file)

**DynamoDB Table:** `{project}-dsql-rate-limiter`

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | PK | `dsql_connect_bucket#{endpoint}` |
| `tokens_milli` | Number | — | Current tokens × 1000 (avoids floats) |
| `last_refill_ms` | Number | — | Unix millis of last refill |
| `rate_milli` | Number | — | 100,000 (= 100 tokens/sec × 1000) |
| `capacity_milli` | Number | — | 1,000,000 (= 1000 tokens × 1000) |
| `ttl_epoch` | Number | TTL | Unix seconds + 180 (3 min auto-cleanup) |

**Wait protocol (Rust pseudocode):**

```rust
/// Blocks until one token is acquired from the distributed bucket.
/// Uses optimistic read-modify-write with DynamoDB conditional updates.
///
/// IMPORTANT: The AWS SDK (`aws-sdk-dynamodb`) has built-in retry with
/// exponential backoff for DynamoDB throttling (ThrottlingException).
/// Do NOT add redundant retry logic for throttling — only handle
/// ConditionalCheckFailedException (race condition) explicitly.
pub async fn wait(&self, ctx: &Context) -> Result<(), TokenBucketError> {
    let deadline = Instant::now() + MAX_WAIT; // 30 seconds
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        let (acquired, retry_after_ms) = self.try_acquire().await?;

        if acquired { return Ok(()); }

        if Instant::now() >= deadline {
            return Err(TokenBucketError::Timeout { attempts });
        }

        // Backoff: use retry_after_ms hint (10ms per token at 100/sec) + jitter
        let backoff = if retry_after_ms > 0 {
            Duration::from_millis(retry_after_ms as u64)
        } else {
            BACKOFF_BASE // 50ms — used when ConditionalCheckFailed (immediate retry)
        };
        let jitter = rand::gen_range(0..=backoff.as_millis() / 2) as u64;
        let sleep_for = Duration::from_millis(backoff.as_millis() as u64 + jitter)
            .min(deadline.duration_since(Instant::now()));

        tokio::time::sleep(sleep_for).await;
    }
}

/// Single attempt to acquire one token.
/// Returns (true, 0) on success.
/// Returns (false, retry_after_ms) if bucket empty.
/// Returns Err on DynamoDB error (NOT ConditionalCheckFailed — that returns (false, 0)).
async fn try_acquire(&self) -> Result<(bool, i64), TokenBucketError> {
    let pk = format!("dsql_connect_bucket#{}", self.endpoint);
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).as_millis() as i64;

    // Milli-token constants (integer math, no floats)
    let rate_milli: i64 = self.rate * 1000;         // 100_000
    let capacity_milli: i64 = self.capacity * 1000; // 1_000_000
    let one_token_milli: i64 = 1000;

    // Step 1: Read current bucket state (ConsistentRead)
    let get_result = self.ddb.get_item()
        .table_name(&self.table_name)
        .key("pk", AttributeValue::S(pk.clone()))
        .consistent_read(true)
        .send().await?;

    // Step 2: Compute new state
    let (current_tokens_milli, last_refill_ms, is_new_bucket) = match get_result.item {
        None => (capacity_milli, now_ms, true),  // New bucket: start at full capacity
        Some(item) => {
            let tokens = parse_number(&item, "tokens_milli");
            let refill = parse_number(&item, "last_refill_ms");
            (tokens, refill, false)
        }
    };

    // Compute refill: tokens += (elapsed_ms × rate_milli) / 1000
    let elapsed_ms = (now_ms - last_refill_ms).max(0);
    let refill_milli = (elapsed_ms * rate_milli) / 1000;
    let tokens_after_refill = (current_tokens_milli + refill_milli).min(capacity_milli);

    // Check if we have at least 1 token
    if tokens_after_refill < one_token_milli {
        // Bucket empty — hint: one token refills every 10ms at 100/sec
        let ms_per_token = 1000 / self.rate;
        return Ok((false, ms_per_token));
    }

    // Deduct 1 token
    let new_tokens_milli = tokens_after_refill - one_token_milli;
    let ttl_epoch = (now_ms / 1000) + 3600; // 1 hour TTL

    // Step 3: Conditional write
    let result = if is_new_bucket {
        // New bucket: condition on attribute_not_exists(pk)
        self.ddb.update_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(pk))
            .update_expression("SET tokens_milli = :tokens, last_refill_ms = :now, rate_milli = :rate, capacity_milli = :cap, ttl_epoch = :ttl")
            .condition_expression("attribute_not_exists(pk)")
            .expression_attribute_values(":tokens", AttributeValue::N(new_tokens_milli.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
            .expression_attribute_values(":rate", AttributeValue::N(rate_milli.to_string()))
            .expression_attribute_values(":cap", AttributeValue::N(capacity_milli.to_string()))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl_epoch.to_string()))
            .send().await
    } else {
        // Existing bucket: condition on last_refill_ms matching what we read
        self.ddb.update_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(pk))
            .update_expression("SET tokens_milli = :tokens, last_refill_ms = :now, ttl_epoch = :ttl")
            .condition_expression("last_refill_ms = :expected_refill")
            .expression_attribute_values(":tokens", AttributeValue::N(new_tokens_milli.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl_epoch.to_string()))
            .expression_attribute_values(":expected_refill", AttributeValue::N(last_refill_ms.to_string()))
            .send().await
    };

    match result {
        Ok(_) => Ok((true, 0)),
        Err(e) if is_conditional_check_failed(&e) => {
            // Another node raced us — retry immediately (return 0 for retry_after)
            Ok((false, 0))
        }
        Err(e) => Err(TokenBucketError::DynamoDb(e.into())),
    }
}
```

**Key implementation notes:**
- `ConsistentRead: true` on GetItem ensures we see the latest state.
- Milli-token math (×1000) avoids floating-point precision issues in DynamoDB Number attributes.
- `ConditionalCheckFailedException` is a normal race condition (another node consumed the token first) — retry immediately with 0 backoff.
- The AWS SDK's built-in retry handles `ThrottlingException` transparently — do NOT add redundant retry for throttling.
- `MAX_WAIT = 30 seconds` caps total acquisition time. If the bucket stays empty for 30s, the refiller's connection creation attempt fails (and the refiller backs off 250ms before trying again).
- `BACKOFF_BASE = 50ms` is used when `ConditionalCheckFailed` returns `retry_after_ms = 0`.

**Atomicity:** The condition expression `last_refill_ms = :expected_refill` ensures only one node succeeds per read-modify-write cycle. Losers see `ConditionalCheckFailedException` and retry with fresh state.

### 8. Slot Block Manager

**Location:** `crates/tokeira-storage/src/dsql/slot_block_manager.rs` (new file)

**DynamoDB Table:** `{project}-dsql-conn-lease`

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | PK | `connslots#{endpoint}#block-{block_id}` |
| `owner_id` | String | — | Node ID (hex-encoded 16 random bytes), empty string if unowned |
| `ttl_epoch` | Number | TTL | Unix seconds + 180 (crash recovery) |
| `slots` | Number | — | 100 (slots per block) |
| `service_name` | String | — | For debugging (e.g., "tokeirad") |
| `acquired_at_ms` | Number | — | Unix millis when acquired |

**Rust struct:**

```rust
pub struct SlotBlockManager {
    ddb: aws_sdk_dynamodb::Client,
    table: String,
    endpoint: String,
    owner_id: String,  // hex-encoded 16 random bytes, generated at startup
    
    // Internal constants
    slots_per_block: u32,   // 100
    block_count: u32,  // 100 (total capacity: 10,000 connections)
    ttl: Duration,     // 3 minutes
    renew_period: Duration, // 1 minute
    
    // State
    owned_blocks: RwLock<HashSet<u32>>,  // block indices we own
    total_slots: AtomicU32,              // sum of owned blocks × slots_per_block
    used_slots: AtomicI64,               // currently in use (atomic for fast-path check)
    renewer_handle: JoinHandle<()>,
}
```

**Acquire slots on startup (Rust pseudocode):**

```rust
/// Acquire enough blocks to have at least target_slots available.
/// Called once during startup before the refiller begins.
///
/// CRITICAL: Randomize starting block index to avoid thundering herd.
/// Without this, all nodes starting simultaneously would race for blocks 0, 1, 2...
pub async fn acquire_slots(&self, target_slots: u32) -> Result<u32> {
    let blocks_needed = (target_slots + self.slots_per_block - 1) / self.slots_per_block; // ceil division
    let mut blocks_acquired = 0u32;
    
    // Randomize start to reduce contention on simultaneous startup
    let start_idx = rand::gen_range(0..self.block_count);
    
    for i in 0..self.block_count {
        if blocks_acquired >= blocks_needed { break; }
        let block_idx = (start_idx + i) % self.block_count;
        
        if self.owned_blocks.read().await.contains(&block_idx) { continue; }
        
        match self.try_acquire_block(block_idx).await {
            Ok(true) => {
                self.owned_blocks.write().await.insert(block_idx);
                self.total_slots.fetch_add(self.slots_per_block, Ordering::Release);
                blocks_acquired += 1;
            }
            Ok(false) => continue,  // Owned by another node
            Err(e) => { tracing::debug!(block_idx, ?e, "failed to acquire block"); continue; }
        }
    }
    
    if blocks_acquired > 0 { self.start_renewer(); }
    Ok(self.total_slots.load(Ordering::Acquire))
}

/// Attempt to acquire a single block via conditional PutItem.
/// Returns Ok(true) if acquired, Ok(false) if owned by another, Err on DynamoDB error.
async fn try_acquire_block(&self, block_idx: u32) -> Result<bool> {
    let pk = format!("connslots#{}#block-{}", self.endpoint, block_idx);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).as_millis() as i64;
    let ttl_epoch = (now / 1000) + self.ttl.as_secs() as i64;
    
    let result = self.ddb.put_item()
        .table_name(&self.table)
        .item("pk", AttributeValue::S(pk))
        .item("owner_id", AttributeValue::S(self.owner_id.clone()))
        .item("ttl_epoch", AttributeValue::N(ttl_epoch.to_string()))
        .item("slots", AttributeValue::N(self.slots_per_block.to_string()))
        .item("service_name", AttributeValue::S("tokeirad".into()))
        .item("acquired_at_ms", AttributeValue::N(now.to_string()))
        // Acquire if: not exists OR owner_id is empty OR TTL expired (crash recovery)
        .condition_expression("attribute_not_exists(pk) OR owner_id = :empty OR ttl_epoch < :now")
        .expression_attribute_values(":empty", AttributeValue::S(String::new()))
        .expression_attribute_values(":now", AttributeValue::N((now / 1000).to_string()))
        .send().await;
    
    match result {
        Ok(_) => Ok(true),
        Err(e) if is_conditional_check_failed(&e) => Ok(false), // Owned by another
        Err(e) => Err(e.into()),
    }
}
```

**Fast-path budget check (called by refiller before each connection creation):**

```rust
/// Check if we have available slots. O(1), no DynamoDB call.
/// The refiller calls this before each connection creation attempt.
pub fn has_budget(&self) -> bool {
    let total = self.total_slots.load(Ordering::Acquire) as i64;
    let used = self.used_slots.load(Ordering::Acquire);
    used < total
}

/// Acquire one slot (called by refiller before creating a connection).
/// Returns Err if all slots are in use.
pub fn acquire_slot(&self) -> Result<(), SlotBudgetExhausted> {
    let total = self.total_slots.load(Ordering::Acquire) as i64;
    let new_used = self.used_slots.fetch_add(1, Ordering::AcqRel) + 1;
    if new_used > total {
        self.used_slots.fetch_sub(1, Ordering::AcqRel); // Roll back
        return Err(SlotBudgetExhausted);
    }
    Ok(())
}

/// Release one slot (called when a connection is retired/discarded).
pub fn release_slot(&self) {
    self.used_slots.fetch_sub(1, Ordering::AcqRel);
}
```

**Renewal loop (background task):**

```rust
async fn renew_loop(&self) {
    let mut interval = tokio::time::interval(self.renew_period); // 1 minute
    loop {
        interval.tick().await;
        let blocks: Vec<u32> = self.owned_blocks.read().await.iter().copied().collect();
        for block_idx in blocks {
            match self.renew_block(block_idx).await {
                Ok(()) => {}
                Err(e) if is_conditional_check_failed(&e) => {
                    self.handle_lost_block(block_idx).await;
                }
                Err(e) => tracing::warn!(block_idx, ?e, "failed to renew slot block"),
            }
        }
    }
}

async fn renew_block(&self, block_idx: u32) -> Result<()> {
    let pk = format!("connslots#{}#block-{}", self.endpoint, block_idx);
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).as_millis() as i64;
    let ttl_epoch = (now_ms / 1000) + self.ttl.as_secs() as i64;
    
    self.ddb.update_item()
        .table_name(&self.table)
        .key("pk", AttributeValue::S(pk))
        .update_expression("SET ttl_epoch = :ttl, renewed_at_ms = :now")
        .condition_expression("owner_id = :owner")  // Only renew if we still own it
        .expression_attribute_values(":ttl", AttributeValue::N(ttl_epoch.to_string()))
        .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
        .expression_attribute_values(":owner", AttributeValue::S(self.owner_id.clone()))
        .send().await?;
    Ok(())
}

async fn handle_lost_block(&self, block_idx: u32) {
    if self.owned_blocks.write().await.remove(&block_idx) {
        self.total_slots.fetch_sub(self.slots_per_block, Ordering::AcqRel);
        metrics::record_dsql_slot_block_lost();
        tracing::warn!(block_idx, "lost DSQL slot block ownership");
    }
}
```

**Graceful shutdown (release = clear owner_id, NOT delete):**

```rust
pub async fn release_all(&self) {
    let blocks: Vec<u32> = self.owned_blocks.write().await.drain().collect();
    for block_idx in blocks {
        let pk = format!("connslots#{}#block-{}", self.endpoint, block_idx);
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).as_millis() as i64;
        // Clear owner_id to release (don't delete — keep for visibility/debugging)
        let _ = self.ddb.update_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(pk))
            .update_expression("SET owner_id = :empty, released_at_ms = :now")
            .condition_expression("owner_id = :owner")
            .expression_attribute_values(":empty", AttributeValue::S(String::new()))
            .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
            .expression_attribute_values(":owner", AttributeValue::S(self.owner_id.clone()))
            .send().await;
    }
    self.total_slots.store(0, Ordering::Release);
}
```

**Key implementation notes:**
- `owner_id` is 16 random bytes hex-encoded at startup — unique per process incarnation.
- Release clears `owner_id` to empty string (NOT delete) — keeps the item for debugging visibility.
- Renewal condition `owner_id = :owner` ensures only the owner can renew — if another node stole the block (after TTL expiry), renewal fails silently.
- `has_budget()` and `acquire_slot()` are O(1) atomic operations — no DynamoDB call on the hot path.
- The refiller calls `slot_manager.acquire_slot()` BEFORE creating a connection and calls `slot_manager.release_slot()` if creation fails or when a connection is retired (in the expiry scanner or return processor discard path).
- If a renewal receives `ConditionalCheckFailedException`, the node removes that block from `owned_blocks`, subtracts `SLOT_BLOCK_SIZE` from `total_slots`, emits `tokeira_dsql_slot_block_lost_total`, and continues running with reduced capacity. If `used_slots > total_slots`, `has_budget()` returns false until enough existing connections are returned/retired or a later refiller cycle acquires replacement capacity.
- Crash recovery: if a node crashes, its blocks' `ttl_epoch` expires after 3 minutes. The `attribute_not_exists(pk) OR owner_id = :empty OR ttl_epoch < :now` condition allows other nodes to claim expired blocks.

### 9. Class-Based Admission Control

**Location:** `crates/tokeira-storage/src/dsql/connection.rs` (existing, modified)

```rust
impl ConnectionDirector for DsqlConnectionDirector {
    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
        let started = Instant::now();
        let class_guard = self.class_budgets.acquire(class).await?;  // may block
        match self.reservoir.checkout() {                              // non-blocking
            Ok(conn) => { /* build permit with Arc<SlotBlockManager> clone */ }
            Err(ReservoirError::Empty) => {
                drop(class_guard);  // release permit on backpressure
                Err(anyhow::Error::new(ReservoirError::Empty).context(PersistenceCapacityExhausted))
            }
        }
    }
}
```

**Allocation (TARGET_READY = 50):**

| Class | % | Permits |
|-------|---|---------|
| Commit | 50% | 25 |
| Read | 20% | 10 |
| Projection | 10% | 5 |
| Control | 10% | 5 |
| Maintenance | 10% | 5 |

### 10. DsqlPermit

Each permit stores an `Arc<SlotBlockManager>` so the synchronous drop path can
release the reserved slot if it closes an expired connection or cannot hand the
connection to the return processor during shutdown.

```rust
pub struct DsqlPermit {
    pub class: DbClass,
    connection: Option<PgConnection>,
    created_at: Instant,
    lifetime: Duration,
    marked_bad: bool,
    broken: BrokenFlag,                      // registered with the enclosing operation at checkout
    class_guard: Option<OwnedSemaphorePermit>, // handed to the return processor on drop
    reservoir_return: mpsc::UnboundedSender<ReturnedConn>,
    slot_manager: Arc<SlotBlockManager>,
    director_in_flight: Arc<AtomicUsize>,
}

impl DsqlPermit {
    pub fn connection(&mut self) -> Result<&mut PgConnection> { /* ... */ }
    pub fn mark_bad(&mut self) { self.marked_bad = true; }
}

impl Drop for DsqlPermit {
    fn drop(&mut self) {
        self.director_in_flight.fetch_sub(1, Ordering::AcqRel);
        if let Some(conn) = self.connection.take() {
            if self.created_at.elapsed() >= self.lifetime {
                metrics::record_retirement("expired", self.created_at.elapsed());
                self.slot_manager.release_slot();
                drop(conn);
                return;
            }
            if let Err(err) = self.reservoir_return.send(ReturnedConn {
                connection: conn, created_at: self.created_at,
                lifetime: self.lifetime, broken: self.broken.clone(), marked_bad: self.marked_bad,
                class_permit: self.class_guard.take(),
            }) {
                self.slot_manager.release_slot();
                drop(err.0);
            }
        }
    }
}
```

### 11. Pool Warmup and Startup Sequence

```
1. Read `DsqlPoolConfig.coordination` for DynamoDB table names and client.
2. Validate DynamoDB tables reachable (fail hard if not).
3. Start SlotBlockManager (acquire initial blocks).
4. Start DistributedTokenBucket.
5. Start Reservoir (spawns refiller, scanner, return processor).
6. Warmup: poll until ready_count >= TARGET_READY (timeout: 30s).
7. Build DsqlConnectionDirector with class budgets.
8. Accept gRPC traffic.
```

If DynamoDB is unreachable during validation:
```
Error: DynamoDB table '{project}-dsql-rate-limiter' is unreachable.
DSQL deployments require DynamoDB coordination tables.
Run 'tkr infra apply' to provision them.
```

### 12. Compose IaC DynamoDB Provisioning

**Location:** `platforms/compose/src/modules/dsql.rs` (extend existing module)

Two DynamoDB tables provisioned alongside the DSQL cluster:

| Table | Name | Purpose |
|-------|------|---------|
| Rate Limiter | `{project}-dsql-rate-limiter` | Distributed token bucket |
| Slot Blocks | `{project}-dsql-conn-lease` | Connection slot allocation |

Both tables: on-demand billing, TTL enabled on `ttl_epoch`, same region as DSQL, created by `tkr infra apply`, destroyed by `tkr infra destroy`.


## Data Models

### PhysicalConn

| Field | Type | Description |
|-------|------|-------------|
| `connection` | `PgConnection` | Raw PostgreSQL connection (no pool wrapper) |
| `created_at` | `Instant` | Monotonic creation timestamp |
| `lifetime` | `Duration` | Assigned lifetime: 8–12 minutes |

### ReturnedConn

| Field | Type | Description |
|-------|------|-------------|
| `connection` | `PgConnection` | Connection being returned |
| `created_at` | `Instant` | Original creation timestamp |
| `lifetime` | `Duration` | Original assigned lifetime |
| `marked_bad` | `bool` | Caller signals connection-level error |

### DsqlCoordinationConfig

| Field | Type | Description |
|-------|------|-------------|
| `rate_limiter_table` | `String` | DynamoDB table used by Distributed_Token_Bucket |
| `conn_lease_table` | `String` | DynamoDB table used by Slot_Block_Manager |
| `ddb_client` | `aws_sdk_dynamodb::Client` | Runtime DynamoDB client constructed from the effective DSQL region and AWS credentials |

### DynamoDB: Rate Limiter (`{project}-dsql-rate-limiter`)

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | PK | `dsql_connect_bucket#{endpoint}` |
| `tokens_milli` | Number | — | Current tokens × 1000 |
| `last_refill_ms` | Number | — | Unix millis of last refill |
| `rate_milli` | Number | — | 100,000 (100 tokens/sec × 1000) |
| `capacity_milli` | Number | — | 1,000,000 (1000 tokens × 1000) |
| `ttl_epoch` | Number | TTL | Unix seconds + 180 |

### DynamoDB: Slot Blocks (`{project}-dsql-conn-lease`)

| Attribute | Type | Key | Description |
|-----------|------|-----|-------------|
| `pk` | String | PK | `connslots#{endpoint}#block-{block_id}` |
| `owner_id` | String | — | Node ID |
| `acquired_at_ms` | Number | — | Unix millis |
| `ttl_epoch` | Number | TTL | Unix seconds + 180 |
| `slots` | Number | — | 100 |

### Metrics

| Metric Name | Type | Labels |
|-------------|------|--------|
| `tokeira_dsql_pool_connections_total` | Gauge | — |
| `tokeira_dsql_pool_checkout_duration_seconds` | Histogram | `class` |
| `tokeira_dsql_pool_empty_reservoir_total` | Counter | — |
| `tokeira_dsql_reservoir_connection_create_duration_seconds` | Histogram | — |
| `tokeira_dsql_reservoir_connection_age_seconds` | Histogram | `retirement_reason` |
| `tokeira_dsql_reservoir_in_flight` | Gauge | — |
| `tokeira_dsql_pool_rate_limiter_tokens` | Gauge | — |
| `tokeira_dsql_pool_rate_limiter_rate` | Gauge | — |
| `tokeira_dsql_slot_blocks_owned` | Gauge | — |
| `tokeira_dsql_pool_class_budget_total` | Gauge | `class` |
| `tokeira_dsql_pool_class_in_use` | Gauge | `class` |
| `tokeira_dsql_pool_class_waiters` | Gauge | `class` |
| `tokeira_dsql_class_permit_wait_duration_seconds` | Histogram | `class` |
| `tokeira_dsql_slot_block_lost_total` | Counter | — |
| `tokeira_dsql_pool_connections_created_total` | Counter | — |
| `tokeira_dsql_pool_connections_retired_total` | Counter | `reason` |
| `tokeira_dsql_pool_connections_returned_total` | Counter | — |
| `tokeira_dsql_connection_error_total` | Counter | `error_kind` |

All metrics emitted unconditionally via the `metrics` crate. No configuration options.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Lifetime Jitter Bounds

*For any* connection created by the refiller, its assigned lifetime SHALL be within `[BASE_LIFETIME - LIFETIME_JITTER, BASE_LIFETIME + LIFETIME_JITTER]` (8 to 12 minutes inclusive).

**Validates: Requirements 4.6, 11.3**

### Property 2: Lifetime Safety Against Token TTL

*For any* valid reservoir configuration, `BASE_LIFETIME + LIFETIME_JITTER + GUARD_WINDOW` SHALL be strictly less than 15 minutes (the DSQL IAM token TTL). With specified constants: 12 min + 45 sec = 12:45 < 15:00.

**Validates: Requirements 11.4**

### Property 3: Guard Window Enforcement

*For any* connection where `remaining_lifetime() <= GUARD_WINDOW`, both the expiry scanner and the return processor SHALL discard it. No such connection SHALL be placed back in the ready channel or handed to a caller.

**Validates: Requirements 5.2, 5.5, 6.2, 6.6, 16.1, 16.2, 16.6**

### Property 4: Bad Connection Discard

*For any* connection returned with `marked_bad = true`, the return processor SHALL discard it regardless of remaining lifetime.

**Validates: Requirements 16.3, 16.6**

### Property 5: No-Network Return Path

*For any* connection returned outside the guard window and not marked bad, the return processor SHALL place it back in the ready channel without executing any network I/O.

**Validates: Requirements 16.4, 16.5**

### Property 6: Class Budget Sum Invariant

*For any* `target_ready` ≥ 5, the sum of all class allocations SHALL equal `target_ready`, and each class SHALL have at least 1 permit.

**Validates: Requirements 9.1**

### Property 7: Class Isolation

*For any* valid system state, exhausting all permits in one class SHALL NOT reduce available permits in any other class.

**Validates: Requirements 9.4**

### Property 8: Bounded Scan Per Pass

*For any* expiry scanner pass, the scanner SHALL examine at most `SCAN_BUDGET` (`TARGET_READY / 2`) entries from the ready channel.

**Validates: Requirements 5.4**

### Property 9: Slot Budget Enforcement

*For any* node with N owned slot blocks, the refiller SHALL NOT create more than `N × SLOT_BLOCK_SIZE` connections.

**Validates: Requirements 8.3**

### Property 10: Distributed Rate Burst Limit

*For any* sequence of token acquisitions, the bucket SHALL NOT dispense more than `DISTRIBUTED_BURST` (1000) tokens without refill time. Sustained rate SHALL NOT exceed `DISTRIBUTED_RATE` (100/sec).

**Validates: Requirements 7.3, 8.1**

### Property 11: Non-Blocking Checkout

*For any* state of the ready channel, `checkout()` SHALL return in O(1) time without network I/O, DynamoDB calls, or connection creation.

**Validates: Requirements 3.1, 3.2**

### Property 12: Inflight Concurrency Limit

*For any* state of the refiller, concurrent connection creation attempts SHALL NOT exceed `INFLIGHT_LIMIT` (8).

**Validates: Requirements 4.4**

## Error Handling

### Connection Creation Failures

| Error Category | Cause | Refiller Behavior | Caller Impact |
|---------------|-------|-------------------|---------------|
| `config` | The connector rejected the connection options | Back off, retry | None |
| `token` | No credentials, or IAM token generation failed | Back off, retry | None |
| `connection` | TCP or TLS failure, DNS failure, or the cluster refusing the connection (for example, its connection rate exceeded) | Back off, retry | None |
| `database` | The database rejected the handshake | Back off, retry | None |
| `timeout` | The attempt did not finish within `CONNECT_TIMEOUT` | Back off, retry | None |

Every failure lengthens the shared failure streak: the refiller waits `refill_backoff(streak)` (250ms doubling to 10 sec, with equal jitter) before its next attempt, and the first success resets the streak. The refiller never propagates errors to checkout callers. It logs, backs off, and retries indefinitely.

### DynamoDB Coordination Failures

| Scenario | Behavior |
|----------|----------|
| Table unreachable at startup | **Fail hard** — process exits with clear error |
| Cannot acquire any slot blocks | **Fail hard** — process exits |
| Condition check failed (race) | Normal — retry with jitter |
| Transient error during operation | Log error, retry with backoff |
| Slot block renewal transient failure | Log warning, retry next interval |
| Slot block renewal conditional check failure | Remove block locally, reduce total slots, emit `tokeira_dsql_slot_block_lost_total`, continue with reduced capacity |
| Slot block TTL expires (crash) | Blocks become available to other nodes |

### Checkout Failures

| Scenario | Behavior |
|----------|----------|
| Ready channel empty | Return `Err(ReservoirError::Empty)` immediately; the director adds `PersistenceCapacityExhausted`, which the frontend answers with `RESOURCE_EXHAUSTED` (cause `RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT`, scope `RESOURCE_EXHAUSTED_SCOPE_SYSTEM`), as Temporal does when its persistence rate limit is reached (`common/persistence/persistence_rate_limited_clients.go` @ v1.31.0) |
| Class budget exhausted | Block at semaphore until permit available |
| Reservoir dropped | Return `Err` (channel closed) |

### Return Path

| Scenario | Behavior |
|----------|----------|
| Within guard window | Discard, emit metric |
| Marked bad | Discard, emit metric |
| Flagged broken by its operation | Discard, emit metric |
| Ready channel full (unreachable while class budgets sum to `target_ready`) | Discard, emit metric |
| Healthy, outside guard window | Put back (no ping) |

### Graceful Shutdown

1. Abort refiller, scanner, return processor tasks.
2. Close ready and return channels.
3. Release all slot blocks via `SlotBlockManager::release_all()`.
4. Drop all `PhysicalConn` instances (closes TCP connections).

## Testing Strategy

### Property-Based Tests (proptest)

Each correctness property maps to a property-based test with minimum 100 iterations:

| Property | Generator Strategy |
|----------|-------------------|
| 1: Lifetime Jitter Bounds | Random seeds, verify output in [8min, 12min] |
| 2: Lifetime Safety | Verify max_lifetime + guard < 15min for any valid config |
| 3: Guard Window Enforcement | Random (age, lifetime) pairs, verify retirement decision |
| 4: Bad Connection Discard | Random lifetimes with bad=true, verify always discarded |
| 5: No-Network Return | Mock-based, verify no I/O calls on healthy return |
| 6: Class Budget Sum | `target_ready in 5..500`, verify sum invariant |
| 7: Class Isolation | Exhaust one class, verify others unchanged |
| 8: Bounded Scan | `channel_size in 1..200`, verify ≤ SCAN_BUDGET examined |
| 9: Slot Budget | `blocks in 0..10, connections in 0..1000`, verify budget check |
| 10: Rate Burst | `acquisitions in 1..2000`, verify burst limit |
| 11: Non-Blocking Checkout | Various channel states, verify O(1) return |
| 12: Inflight Limit | `concurrent in 1..20`, verify semaphore enforcement |

**Library:** `proptest` (already in workspace). **Tag format:** `Feature: dsql-reservoir-redesign, Property {N}: {title}`

### Unit Tests (example-based)

- Connection factory error classification (each variant → correct category)
- Warmup completes at target / times out gracefully
- Non-blocking checkout returns immediately when empty
- DsqlPermit::drop sends ReturnedConn to return channel
- DsqlPermit::mark_bad propagates to ReturnedConn
- Class budget rejects zero-allocation configs
- Startup fails without DynamoDB coordination tables
- Scanner retires expired connections, preserves healthy ones
- Return processor discards bad/expired, keeps healthy
- Connect timeout, credentials refresh margin, and the backoff's doubling and ceiling
- A failed operation flags every connection it checked out, and only when the failure broke a connection

### Scenario Tests (paused time)

Real connections to a local server that answers only the startup handshake, opened before time is paused, are handed out by a scripted factory that connects, refuses or hangs with a virtual connect latency:

- After a mass retirement the reservoir refills in about `target_ready / INFLIGHT_LIMIT` connect latencies, never past its target, with no more than `INFLIGHT_LIMIT` connects at once
- A class permit comes back only with its connection: the next caller of the class is never refused for an empty reservoir while the connection is on its way back
- A burst within the class budgets never finds the reservoir empty and makes no more than `2 × target_ready` connections
- A hung connect times out and gives its slot back
- Refused connects back off within the expected attempt rate and the reservoir refills promptly on the first success
- A connection an operation broke is retired and replaced, including one flagged after its return

### Integration Tests (require DynamoDB Local)

- Distributed token bucket: two nodes share rate budget
- Slot block acquisition: conditional PutItem succeeds for first claimer
- Slot block TTL expiry: block reclaimable after crash
- Full lifecycle: warmup → checkout → use → return → expiry → refill
- Graceful shutdown releases slot blocks

### Live Tests (opt-in, throwaway cluster)

- A burst through a small reservoir never finds it empty
- A connection storm from several reservoirs past the cluster's connection burst: every refill completes, refused connects back off, and no slot is lost
