//! Nexus operation types, endpoint registry, and timeout scanning.
//!
//! Contains the HTTP client trait for Nexus operations, endpoint configuration
//! and registry, timeout tracking state, and the background scanner that
//! detects timed-out Nexus operations.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use prost::Message;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    Command, Link, LoadedRun, NexusCancellationRetryRequest, NexusOperationResolvedRequest,
    NexusOperationRetryRequest, NexusResolution, NexusTimeoutType, PendingNexusOperation,
    WorkerDeploymentVersionRef,
};
use tokeira_storage::RunRepository;
use tokeira_types::{
    BuildId, DeploymentId, NamespaceId, Payload, Payloads, RunKey, ShardId, TaskQueueName,
    WorkerComputeTaskType, WorkerIdentity, WorkerTaskClass, WorkerTaskOrigin,
};
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    DispatchEligibility, DispatchRateLimits, EffectivePriority, InMemoryTaskQueueConfigStore,
    TaskQueueConfigKey, TaskQueueConfigKind, TaskQueueConfigStore, lane::LaneHandle,
    metrics as runtime_metrics, scanner::pick_lane_for_run_key, shard::ShardOwner,
};

/// Currently-supported completion-callback token envelope version. Mirrors
/// `TokenVersion = 1` (`common/nexus/callback_token.go:15 @ v1.31.0`); `decode`
/// rejects any other value (the version check is the token's only validation —
/// it is opaque + version-checked, not signed).
pub const COMPLETION_TOKEN_VERSION: u8 = 1;

/// HTTP header carrying the completion-callback token on the outbound
/// `StartOperation` request and on the inbound completion `POST`. Verbatim from
/// `CallbackTokenHeader = "Temporal-Callback-Token"`
/// (`common/nexus/callback_token.go:17 @ v1.31.0`).
pub const TEMPORAL_CALLBACK_TOKEN_HEADER: &str = "Temporal-Callback-Token";

/// HTTP header carrying the terminal operation state (`succeeded`/`failed`/`canceled`)
/// on a completion `POST`. Verbatim from `headerOperationState = "nexus-operation-state"`
/// (`common/nexus/nexusrpc/api.go:23 @ v1.31.0`). The firing client sets it; the inbound
/// `/nexus/callback` server reads it to select the resolution shape.
pub const NEXUS_OPERATION_STATE_HEADER: &str = "nexus-operation-state";

/// In-cluster callback URL sentinel attached to a Worker-target `StartOperation`.
/// Verbatim from `SystemCallbackURL = "temporal://system"`
/// (`common/nexus/constants.go @ v1.31.0`). The runtime resolves it to the
/// configured local HTTP listener (`NexusCompletionConfig.system_callback_url`)
/// when firing — the loopback v1.31.0 performs via `routeSystemCallbackRequest`.
pub const SYSTEM_CALLBACK_URL: &str = "temporal://system";

/// Path of the inbound completion endpoint, appended to the resolved loopback base URL
/// (`NexusCompletionConfig.system_callback_url`) when firing a `temporal://system`
/// callback, and served by the `tokeirad` listener. A tokeira-local convention: v1.31.0
/// routes the system callback to its frontend's per-namespace Nexus completion route
/// (`routeSystemCallbackRequest @ v1.31.0`); the single-cluster loopback uses one fixed
/// path. External callback URLs already encode their own path and are POSTed verbatim.
pub const NEXUS_CALLBACK_PATH: &str = "/nexus/callback";

/// Default Nexus-operation invocation retry policy (config-as-constant), mirroring
/// v1.31.0's component default `RetryPolicy`
/// (`components/nexusoperations/config.go:134-210 @ v1.31.0`):
/// `NewExponentialRetryPolicy(initialInterval=1s).WithMaximumInterval(1h)
/// .WithExpirationInterval(NoInterval)`. The standard exponential coefficient is 2.0.
/// Retries are bounded only by schedule-to-close (the timeout scanner owns that cap),
/// not by an expiration interval.
pub const NEXUS_RETRY_INITIAL_INTERVAL: Duration = Duration::seconds(1);
/// Maximum backoff between Nexus StartOperation/CancelOperation attempts.
pub const NEXUS_RETRY_MAXIMUM_INTERVAL: Duration = Duration::hours(1);
/// Exponential backoff coefficient for Nexus operation retries.
pub const NEXUS_RETRY_BACKOFF_COEFFICIENT: f64 = 2.0;

/// Next-attempt time for a retryable Nexus `StartOperation` failure, or `None` when the
/// retry would fall at/after schedule-to-close — in which case the operation must resolve
/// terminally rather than back off (Invariant 3: the schedule-to-close timeout scanner is
/// the terminal authority). The kernel performs no backoff math; this is the runtime's
/// computation, fed into `NexusResolution::AttemptFailed`.
///
/// `failed_attempts` is the count of attempts that have already failed *before* this one
/// (0 for the first failure), matching v1.31.0's `recordAttempt`-then-`ComputeNextDelay`
/// order (`statemachine.go:91-94,280-285`): the next delay for the first retry is the
/// initial interval (`coefficient^0`).
pub fn nexus_operation_next_attempt_at(
    failed_attempts: u32,
    scheduled_at: OffsetDateTime,
    schedule_to_close_timeout: Option<Duration>,
    now: OffsetDateTime,
) -> Option<OffsetDateTime> {
    let factor = NEXUS_RETRY_BACKOFF_COEFFICIENT.powi(failed_attempts as i32);
    let delay_secs = (NEXUS_RETRY_INITIAL_INTERVAL.as_seconds_f64() * factor)
        .min(NEXUS_RETRY_MAXIMUM_INTERVAL.as_seconds_f64());
    let next = now + Duration::seconds_f64(delay_secs);
    // Only cap when schedule-to-close is set and positive (zero/absent = unbounded,
    // matching the request-timeout treatment); a retry at/after the deadline is terminal.
    if let Some(stc) = schedule_to_close_timeout
        && stc.is_positive()
        && next >= scheduled_at + stc
    {
        return None;
    }
    Some(next)
}

/// The HTTP URL an async Nexus completion is `POST`ed to: the configured listener base
/// (`NexusCompletionConfig.system_callback_url`) joined with [`NEXUS_CALLBACK_PATH`]. Used
/// both when minting the callback URL handed to a Worker handler (which POSTs its eventual
/// async outcome here) and when the firing client resolves a `temporal://system` callback
/// to a concrete address. A trailing slash on the base is trimmed so the join never
/// doubles `/`.
pub fn system_callback_post_url(base: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), NEXUS_CALLBACK_PATH)
}

#[derive(Clone, Debug, PartialEq)]
pub enum NexusStartResult {
    SyncCompleted {
        result: Payloads,
        /// Links the handler returned on the sync-success response.
        links: Vec<Link>,
    },
    SyncFailed {
        message: String,
    },
    AsyncAccepted {
        /// Handler-issued token identifying the running operation.
        operation_token: String,
        /// Links the handler returned on the async-start response.
        links: Vec<Link>,
    },
    /// A Nexus *handler* error response (a non-2xx HTTP status the handler returned, e.g.
    /// `400 BAD_REQUEST`), as opposed to [`SyncFailed`](NexusStartResult::SyncFailed) (an
    /// unsuccessful *operation*, HTTP 424). Both resolve the caller's operation as failed;
    /// the distinction drives the `nexus_outbound_requests` outcome tag
    /// (`handler-error:<TYPE>` vs `operation-unsuccessful:<state>`,
    /// `startCallOutcomeTag @ v1.31.0`).
    HandlerError {
        /// The Nexus `HandlerErrorType` (e.g. `BAD_REQUEST`, `INTERNAL`), mapped from the
        /// HTTP status.
        error_type: String,
        /// The decoded `nexus.Failure` body, when the handler returned one. Used to
        /// rehydrate the caller's `NexusOperationError -> HandlerError -> ApplicationError`
        /// chain; `None` when the body was absent or not a JSON Nexus failure.
        failure: Option<NexusHttpFailureBody>,
        /// Status/header-derived retry classification. Existing workflow callers
        /// remain single-attempt; worker-compute outbox delivery consumes this bit.
        retryable: bool,
    },
}

/// Classified result of one External-endpoint `CancelOperation` call.
///
/// Transport failures remain the trait's outer `Err` and are retryable. HTTP
/// responses are returned here so the runtime can durably record success,
/// retryable backoff, or terminal failure without making the kernel understand
/// HTTP statuses or headers.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusCancelResult {
    /// The handler acknowledged the cancellation request with `202 Accepted`.
    Succeeded,
    /// A mapped Nexus handler error.
    HandlerError {
        /// Nexus handler-error type derived from the status.
        error_type: String,
        /// Decoded Nexus failure body when present.
        failure: Option<NexusHttpFailureBody>,
        /// Status/header-derived retry decision.
        retryable: bool,
    },
    /// An HTTP status outside the Nexus handler-error mapping. v1.31.0 treats
    /// this as a retryable unexpected-response call error.
    UnexpectedResponse {
        /// Diagnostic error text retained in attempt state.
        message: String,
    },
}

/// A Nexus failure decoded from an External handler's HTTP error response body (the JSON
/// `nexus.Failure` shape: message + metadata + raw JSON details). Carries enough to rebuild
/// the caller's `NexusOperationError -> HandlerError -> ApplicationError` chain faithfully,
/// mirroring `NexusFailureToTemporalFailure` (`common/nexus/failure.go @ v1.31.0`): the
/// non-typed body becomes an `ApplicationFailureInfo{type:"NexusFailure"}` whose details
/// payload is the JSON `{metadata, details}` (message cleared), so the SDK's
/// `appErr.Details(&nexus.Failure)` recovers the original metadata and details.
#[derive(Clone, Debug, PartialEq, Default, Deserialize)]
pub struct NexusHttpFailureBody {
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub metadata: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub details: serde_json::Value,
}

/// Resolves a namespace id to its human-readable name for outbound-metric tagging.
///
/// The caller-side StartOperation against an *External* endpoint is dispatched from the
/// runtime publisher, which holds only the originator's [`RunKey`]/[`NamespaceId`] — never
/// the namespace *name* (tokeira namespace ids are a non-invertible function of the name).
/// The edge owns the name registry, so the bootstrap injects an implementation backed by
/// the namespace cache; when absent (e.g. unit harnesses) the outbound metric simply omits
/// the request rather than tagging it with an unresolved namespace.
#[async_trait]
pub trait NexusNamespaceResolver: Send + Sync {
    /// The namespace name for `namespace_id`, or `None` when it is unknown.
    async fn name_for_id(&self, namespace_id: NamespaceId) -> Option<String>;
}

#[async_trait]
pub trait NexusHttpClient: Send + Sync {
    /// Start a Nexus operation on an External endpoint over HTTP.
    ///
    /// `operation_id` is tokeira's per-operation identifier, sent as the
    /// `Nexus-Request-Id` for handler-side idempotency. The returned
    /// [`NexusStartResult`] is mapped to a `NexusResolution` by the publisher.
    async fn start_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
        operation: &str,
        input: &Payloads,
        schedule_to_close_timeout: Option<Duration>,
        trace_headers: &[KeyValue],
    ) -> Result<NexusStartResult>;

    /// Cancel a started Nexus operation on an External endpoint.
    ///
    /// The cancel URL is `{address}/{service}/{operation}/cancel`, so the
    /// operation **name** (not tokeira's operation id) is required — the caller
    /// resolves it from the pending operation. `operation_token` is sent as the
    /// `Nexus-Operation-Token` header (`handle.go @ v1.31.0`).
    async fn cancel_operation(
        &self,
        address: &str,
        service: &str,
        operation: &str,
        operation_token: &str,
        trace_headers: &[KeyValue],
    ) -> Result<NexusCancelResult>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum EndpointTarget {
    External {
        address: String,
    },
    Worker {
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointConfig {
    pub target: EndpointTarget,
}

/// The mutable, client-supplied fields of a Nexus endpoint (mirrors v1.31.0's
/// `persistencespb.NexusEndpointSpec`, `apiSpecToPersistenceSpec @ v1.31.0`). The
/// `description` is the opaque encoded `temporal.api.common.v1.Payload` bytes —
/// stored verbatim and echoed back on read, never interpreted here.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointSpec {
    pub name: String,
    pub description: Vec<u8>,
    pub target: NexusEndpointSpecTarget,
}

/// The stored endpoint target. Unlike the dispatch-only [`EndpointTarget`], the
/// Worker variant carries **both** the namespace `name` and the resolved `id`: the
/// name is echoed verbatim on read (`endpointPersistedEntryToExternalAPI` returns the
/// namespace *name*, `@ v1.31.0`) while the id is what runtime dispatch routes on.
/// Tokeira namespace ids are a non-invertible function of the name, so the name must
/// be persisted rather than recovered from the id.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusEndpointSpecTarget {
    External {
        url: String,
    },
    Worker {
        namespace_name: String,
        namespace_id: String,
        task_queue: String,
    },
}

/// A persisted Nexus endpoint entry: the server-authored id/version/timestamps plus
/// the client `spec` (mirrors `persistencespb.NexusEndpointEntry` collapsed with its
/// `NexusEndpoint`, `service/matching/nexus_endpoint_client.go @ v1.31.0`).
///
/// `version` is the per-endpoint optimistic-concurrency token: `1` after create, and
/// `previous + 1` after each update (`CreateOrUpdateNexusEndpoint` returns
/// `Entry.Version + 1`, `common/persistence/nexus_endpoint_manager.go:130 @ v1.31.0`).
/// `last_modified_time_unix_nanos` is `0` until the endpoint has actually been
/// modified — the external API only sets `last_modified_time` when `version > 1`
/// (`endpointPersistedEntryToExternalAPI @ v1.31.0`).
#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointRecord {
    pub id: String,
    pub version: i64,
    pub spec: NexusEndpointSpec,
    pub created_time_unix_nanos: i64,
    pub last_modified_time_unix_nanos: i64,
}

/// Storage-layer errors from the [`NexusEndpointStore`]. These are the
/// table-owner outcomes v1.31.0's matching side produces; the edge maps each to the
/// gRPC status + verbatim message for the specific operation (the create/update/
/// delete "not found" messages differ, so the message is formatted at the edge, not
/// here — keeping the store neutral).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NexusEndpointStoreError {
    /// A create collided with an existing endpoint name (→ `ALREADY_EXISTS`,
    /// `service/matching/nexus_endpoint_client.go:100 @ v1.31.0`).
    #[error("nexus endpoint name already registered: {0}")]
    DuplicateName(String),
    /// An update/delete/get targeted an id with no entry (→ `NOT_FOUND`,
    /// `:152`/`:218`/Get path @ v1.31.0).
    #[error("nexus endpoint not found: {0}")]
    NotFound(String),
    /// An update supplied a version `!=` the stored version (→ `FAILED_PRECONDITION`,
    /// **never `ABORTED`**, `:155-156 @ v1.31.0`).
    #[error("nexus endpoint version mismatch. received: {received} expected {expected}")]
    VersionMismatch { received: i64, expected: i64 },
    /// A list `next_page_token` referenced an id no longer present (→
    /// `FAILED_PRECONDITION`, `ListNexusEndpoints @ v1.31.0`).
    #[error("could not find endpoint indicated by nexus list endpoints next page token")]
    PageTokenNotFound,
}

/// One page of a `ListNexusEndpoints` scan: the entries (id-ordered) and the next
/// page token (the id to resume after), `None` when the page is the last.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusEndpointPage {
    pub entries: Vec<NexusEndpointRecord>,
    pub next_page_token: Option<String>,
}

/// The Nexus endpoint table owner — the single source of truth for endpoint admin
/// CRUD and the runtime dispatch lookup. Collapses v1.31.0's matching-owned
/// `nexus_endpoints` table into one neutral trait reached by both the OperatorService
/// edge (CRUD) and runtime Nexus dispatch (`resolve_by_name`), with no edge↔runtime
/// cycle (it lives below the edge). Methods are synchronous because the in-memory
/// implementation is lock-backed and the runtime dispatch path resolves
/// synchronously; the edge calls them from async handlers without blocking on I/O.
///
/// Server-authored fields (id, version, timestamps) are assigned by the store, never
/// the caller — matching the table owner's authorship (`service/matching/
/// nexus_endpoint_client.go @ v1.31.0`).
pub trait NexusEndpointStore: Send + Sync {
    /// Create a new endpoint with a server-authored UUID id and version `1`. Returns
    /// [`NexusEndpointStoreError::DuplicateName`] if the name is already registered.
    fn create(
        &self,
        spec: NexusEndpointSpec,
        now_unix_nanos: i64,
    ) -> Result<NexusEndpointRecord, NexusEndpointStoreError>;

    /// Read an endpoint by id, or [`NexusEndpointStoreError::NotFound`].
    fn get(&self, id: &str) -> Result<NexusEndpointRecord, NexusEndpointStoreError>;

    /// Apply a mutation iff `expected_version` equals the stored version (optimistic
    /// CAS), bumping the version and `last_modified_time`. Returns
    /// [`NexusEndpointStoreError::NotFound`] for a missing id or
    /// [`NexusEndpointStoreError::VersionMismatch`] on a stale version.
    fn update(
        &self,
        id: &str,
        expected_version: i64,
        spec: NexusEndpointSpec,
        now_unix_nanos: i64,
    ) -> Result<NexusEndpointRecord, NexusEndpointStoreError>;

    /// Remove an endpoint by id, or [`NexusEndpointStoreError::NotFound`]. The
    /// version is **not** CAS-checked here: v1.31.0's frontend validates `version >
    /// 0` and forwards only the id to matching, which deletes by id without
    /// comparing the version (`service/matching/nexus_endpoint_client.go:200-220 @
    /// v1.31.0`). The `> 0` guard is enforced at the edge.
    fn delete(&self, id: &str) -> Result<(), NexusEndpointStoreError>;

    /// One id-ordered page starting after `start_after_id` (the page token).
    /// `start_after_id` referencing a missing id is
    /// [`NexusEndpointStoreError::PageTokenNotFound`]. Ordering is by id byte order,
    /// matching the table owner's `slices.BinarySearchFunc` on id and the page token
    /// being the next entry's id (`service/matching/nexus_endpoint_client.go:242-296
    /// @ v1.31.0`).
    fn list(
        &self,
        start_after_id: Option<&str>,
        page_size: usize,
    ) -> Result<NexusEndpointPage, NexusEndpointStoreError>;

    /// Find an endpoint by exact name (the list-and-filter-by-name read path,
    /// `listAndFilterByName @ v1.31.0`), or `None`.
    fn find_by_name(&self, name: &str) -> Option<NexusEndpointRecord>;

    /// Resolve an endpoint name to its dispatch target for runtime Nexus dispatch.
    /// Owned (not a borrow) because a lock-backed store cannot hand out a reference.
    fn resolve_by_name(&self, name: &str) -> Option<NexusEndpointConfig>;
}

#[derive(Default)]
struct NexusEndpointStoreState {
    by_id: HashMap<String, NexusEndpointRecord>,
    /// name → id, the unique-name index (`endpointsByName @ v1.31.0`).
    by_name: HashMap<String, String>,
}

/// In-memory [`NexusEndpointStore`] — the default-suite implementation (no live
/// AWS/DSQL). Lock-protected like the matching table owner's `sync.RWMutex`, which
/// serializes mutations to keep version authoring conflict-free.
#[derive(Default)]
pub struct InMemoryNexusEndpointStore {
    state: Mutex<NexusEndpointStoreState>,
}

// Manual impl: summarizes without taking the interior lock — a `Debug` that
// must lock the state invites deadlock from inside failure paths.
impl std::fmt::Debug for InMemoryNexusEndpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryNexusEndpointStore")
            .finish_non_exhaustive()
    }
}

impl InMemoryNexusEndpointStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NexusEndpointStore for InMemoryNexusEndpointStore {
    fn create(
        &self,
        spec: NexusEndpointSpec,
        now_unix_nanos: i64,
    ) -> Result<NexusEndpointRecord, NexusEndpointStoreError> {
        let mut state = self.state.lock().expect("state lock poisoned");
        if state.by_name.contains_key(&spec.name) {
            return Err(NexusEndpointStoreError::DuplicateName(spec.name));
        }
        let record = NexusEndpointRecord {
            id: uuid::Uuid::new_v4().to_string(),
            // version 1 = the table owner's create result (`entry.Version + 1` with a
            // 0 seed, `nexus_endpoint_manager.go:130 @ v1.31.0`).
            version: 1,
            spec,
            created_time_unix_nanos: now_unix_nanos,
            last_modified_time_unix_nanos: 0,
        };
        state
            .by_name
            .insert(record.spec.name.clone(), record.id.clone());
        state.by_id.insert(record.id.clone(), record.clone());
        Ok(record)
    }

    fn get(&self, id: &str) -> Result<NexusEndpointRecord, NexusEndpointStoreError> {
        self.state
            .lock()
            .expect("state lock poisoned")
            .by_id
            .get(id)
            .cloned()
            .ok_or_else(|| NexusEndpointStoreError::NotFound(id.to_owned()))
    }

    fn update(
        &self,
        id: &str,
        expected_version: i64,
        spec: NexusEndpointSpec,
        now_unix_nanos: i64,
    ) -> Result<NexusEndpointRecord, NexusEndpointStoreError> {
        let mut state = self.state.lock().expect("state lock poisoned");
        let previous = state
            .by_id
            .get(id)
            .cloned()
            .ok_or_else(|| NexusEndpointStoreError::NotFound(id.to_owned()))?;
        // Optimistic CAS: the supplied version must equal the stored one
        // (`request.version != previous.Version` → FailedPrecondition, `:155 @ v1.31.0`).
        if expected_version != previous.version {
            return Err(NexusEndpointStoreError::VersionMismatch {
                received: expected_version,
                expected: previous.version,
            });
        }
        // A rename moves the unique-name index; reject a collision with a *different*
        // endpoint's name (the name index is keyed by name, so a clash means another
        // id already holds it).
        if spec.name != previous.spec.name && state.by_name.contains_key(&spec.name) {
            return Err(NexusEndpointStoreError::DuplicateName(spec.name));
        }
        let updated = NexusEndpointRecord {
            id: previous.id.clone(),
            version: previous.version + 1,
            spec,
            created_time_unix_nanos: previous.created_time_unix_nanos,
            last_modified_time_unix_nanos: now_unix_nanos,
        };
        if updated.spec.name != previous.spec.name {
            state.by_name.remove(&previous.spec.name);
            state
                .by_name
                .insert(updated.spec.name.clone(), updated.id.clone());
        }
        state.by_id.insert(updated.id.clone(), updated.clone());
        Ok(updated)
    }

    fn delete(&self, id: &str) -> Result<(), NexusEndpointStoreError> {
        let mut state = self.state.lock().expect("state lock poisoned");
        let record = state
            .by_id
            .remove(id)
            .ok_or_else(|| NexusEndpointStoreError::NotFound(id.to_owned()))?;
        state.by_name.remove(&record.spec.name);
        Ok(())
    }

    fn list(
        &self,
        start_after_id: Option<&str>,
        page_size: usize,
    ) -> Result<NexusEndpointPage, NexusEndpointStoreError> {
        let state = self.state.lock().expect("state lock poisoned");
        // Id byte-order, mirroring the table owner's `endpointEntries` sorted by id.
        let mut ids: Vec<&String> = state.by_id.keys().collect();
        ids.sort();
        let start_idx = match start_after_id {
            None => 0,
            Some(token) => {
                let pos = ids
                    .iter()
                    .position(|id| id.as_str() == token)
                    .ok_or(NexusEndpointStoreError::PageTokenNotFound)?;
                // The token is the last id returned, so the next page starts after it.
                pos + 1
            }
        };
        let end_idx = (start_idx + page_size).min(ids.len());
        let entries: Vec<NexusEndpointRecord> = ids[start_idx..end_idx]
            .iter()
            .map(|id| state.by_id[id.as_str()].clone())
            .collect();
        // A next token is the last returned id iff more entries remain after this page.
        let next_page_token = (end_idx < ids.len()).then(|| ids[end_idx - 1].clone());
        Ok(NexusEndpointPage {
            entries,
            next_page_token,
        })
    }

    fn find_by_name(&self, name: &str) -> Option<NexusEndpointRecord> {
        let state = self.state.lock().expect("state lock poisoned");
        state
            .by_name
            .get(name)
            .and_then(|id| state.by_id.get(id))
            .cloned()
    }

    fn resolve_by_name(&self, name: &str) -> Option<NexusEndpointConfig> {
        let record = self.find_by_name(name)?;
        let target = match record.spec.target {
            NexusEndpointSpecTarget::External { url } => EndpointTarget::External { address: url },
            NexusEndpointSpecTarget::Worker {
                namespace_id,
                task_queue,
                ..
            } => EndpointTarget::Worker {
                // The stored id is a namespace UUID; a malformed one means the
                // endpoint cannot be dispatched, so it resolves to nothing.
                namespace_id: NamespaceId(uuid::Uuid::parse_str(&namespace_id).ok()?),
                task_queue: TaskQueueName(task_queue),
            },
        };
        Some(NexusEndpointConfig { target })
    }
}

/// The runtime Nexus dispatch lookup, backed by the live [`NexusEndpointStore`]
/// (Req 3). Replaces the former static `Arc<HashMap>` so dispatch reflects current
/// committed endpoint admin state. `resolve` returns an **owned** config because the
/// store is lock-backed and cannot lend a borrow.
#[derive(Clone)]
pub struct NexusEndpointRegistry {
    store: Arc<dyn NexusEndpointStore>,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for NexusEndpointRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusEndpointRegistry")
            .finish_non_exhaustive()
    }
}

impl Default for NexusEndpointRegistry {
    fn default() -> Self {
        Self {
            store: Arc::new(InMemoryNexusEndpointStore::new()),
        }
    }
}

impl NexusEndpointRegistry {
    /// Build a registry backed by `store` (the same store the OperatorService admin
    /// mutates, so created endpoints resolve and deleted ones stop resolving).
    pub fn new(store: Arc<dyn NexusEndpointStore>) -> Self {
        Self { store }
    }

    /// Resolve an endpoint name to its dispatch config, or `None` if unregistered.
    pub fn resolve(&self, endpoint_name: &str) -> Option<NexusEndpointConfig> {
        self.store.resolve_by_name(endpoint_name)
    }
}

/// Worker-visible Nexus task token.
///
/// The field numbers are the exact `temporal.server.api.token.v1.NexusTask`
/// wire contract from `proto/internal/temporal/server/api/token/v1/message.proto
/// @ v1.31.0`. That server-internal proto is not part of Tokeira's vendored public
/// API tree, so the compatible prost message lives beside the broker that authors it.
/// Private workflow routing is deliberately absent and retained in
/// [`NexusTaskCorrelation`] instead.
#[derive(Clone, PartialEq, Message)]
pub struct NexusTaskToken {
    /// Stable namespace UUID of the worker target.
    #[prost(string, tag = "1")]
    pub namespace_id: String,
    /// Normal Nexus task-queue name used for poll delivery.
    #[prost(string, tag = "2")]
    pub task_queue: String,
    /// Server-authored UUID identifying one outstanding dispatch.
    #[prost(string, tag = "3")]
    pub task_id: String,
}

impl NexusTaskToken {
    /// Encode the token with protobuf, matching Temporal's task-token serializer.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.encode_to_vec())
    }

    /// Decode the Temporal v1.31.0 protobuf token shape.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        <Self as Message>::decode(bytes)
            .map_err(|error| anyhow!("Error deserializing task token.: {error}"))
    }
}

/// Private delivery route retained by Tokeira for one worker-visible Nexus
/// `task_id`.
///
/// The token exposes only the stable worker-delivery identity. Tokeira keeps
/// its private routing address beside the disposable delivery queue, so neither
/// workflow authority nor an HTTP waiter leaks onto the wire. Workflow routing
/// is reconstructible from authoritative pending state; HTTP routing is scoped
/// to the lifetime of the current caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NexusTaskCorrelation {
    /// Route a worker outcome to the authoritative pending workflow operation.
    Workflow {
        /// Run owning the pending operation.
        run_key: RunKey,
        /// Pending-operation key within the run.
        operation_id: String,
        /// Scheduled-event fence for stale-result rejection.
        scheduled_event_id: i64,
        /// Whether this delivery attempts to start or cancel the operation. Failed
        /// worker responses do not echo a response variant, so the private route
        /// retains this distinction for correct lifecycle handling.
        task_kind: NexusWorkflowTaskKind,
        /// Exact queue/version origin returned to the Worker.
        origin: WorkerTaskOrigin,
    },
    /// Deliver one worker response to the caller-facing HTTP request that
    /// synchronously dispatched this task.
    Http {
        /// Opaque ID understood only by the edge-owned waiter registry.
        waiter_id: String,
        /// Unversioned origin retained for uniform response validation.
        origin: WorkerTaskOrigin,
    },
    /// Deliver one worker response to the current worker-compute outbox attempt.
    WorkerCompute {
        /// Durable action identity.
        action_id: uuid::Uuid,
        /// Current delivery-claim fence.
        claim_epoch: u64,
        /// Unversioned origin retained for uniform response validation.
        origin: WorkerTaskOrigin,
    },
}

/// Kind of workflow-originated Nexus delivery retained in private correlation state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NexusWorkflowTaskKind {
    /// `StartOperation` invocation.
    StartOperation,
    /// `CancelOperation` invocation.
    CancelOperation,
}

/// Routes an async Nexus completion back to the originator's pending operation.
///
/// Mirrors the role of v1.31.0's `tokenspb.NexusOperationCompletion`
/// (`proto/internal/temporal/server/api/token/v1/message.proto @ v1.31.0`), wrapped
/// in the versioned `{v,d}` envelope of `CallbackToken`
/// (`common/nexus/callback_token.go @ v1.31.0`): versioned + opaque, verified only by
/// version on decode. It is **not** signed — v1.31.0's `CallbackTokenGenerator` is a
/// zero-field struct and the source notes signing/encryption "will come later"; token
/// integrity rests on op-fencing at resolution (`StaleNexusResolution`/
/// `UnknownNexusOperation`), tokeira's analogue of v1.31.0's `StateMachineRef`
/// staleness check (design.md "Requirement refinements", Req 1.5).
///
/// `originator_run_key` (a tokeira global handle) subsumes v1.31.0's deprecated
/// `namespace_id`/`workflow_id`/`run_id` identity tuple and enables cross-namespace
/// routing. `request_id` keeps wire-parity with `NexusOperationCompletion.request_id`.
/// v1.31.0's `ref` (StateMachineRef) is intentionally not modelled: tokeira addresses
/// the pending op directly via `(operation_id, scheduled_event_id)` and fences
/// staleness in `apply_nexus_operation_resolved`.
///
/// The token version lives on the outer envelope only (matching `CallbackToken.Version`
/// @ v1.31.0; the inner `NexusOperationCompletion` proto has no version field), so this
/// struct carries no `version` of its own — one source of truth that cannot diverge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusCompletionToken {
    /// Global handle of the originator (caller) run that owns the pending operation.
    pub originator_run_key: RunKey,
    /// tokeira's per-operation identifier (the pending op key).
    pub operation_id: String,
    /// Event ID of the `NexusOperationScheduled` event — the fencing key.
    pub scheduled_event_id: i64,
    /// Originating request ID (wire-parity with `NexusOperationCompletion.request_id`).
    pub request_id: String,
    /// Caller-side operation target, carried so the inbound `/nexus/callback` handler can
    /// wrap a failed completion in a `NexusOperationFailureInfo` (so the caller decodes a
    /// `NexusOperationError`) without loading the run — it works off `runtime` only. Opaque
    /// and integrity-bound: read like the rest of the token (decode + version check), never
    /// an operator-forgeable input. `#[serde(default)]` keeps decode tolerant of tokens
    /// minted before this field existed.
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub service: String,
    #[serde(default)]
    pub operation: String,
}

/// Outer `{v,d}` envelope. Mirrors `CallbackToken { Version int json:"v"; Data string
/// json:"d" }` (`common/nexus/callback_token.go @ v1.31.0`): `d` is base64url of the
/// serialized inner token.
#[derive(Serialize, Deserialize)]
struct CompletionTokenEnvelope {
    #[serde(rename = "v")]
    version: u8,
    #[serde(rename = "d")]
    data: String,
}

impl NexusCompletionToken {
    /// Encode to the versioned `{v,d}` envelope JSON string carried in the
    /// `Temporal-Callback-Token` header.
    ///
    /// `d = base64url(serde_json(inner))`; outer = `serde_json(envelope)`. Mirrors
    /// `Tokenize` (`common/nexus/callback_token.go @ v1.31.0`) which is
    /// `base64.URLEncoding(proto.Marshal(..))` then `json.Marshal({v,d})`. The inner
    /// codec is `serde_json` rather than `proto.Marshal` (the inner type carries a
    /// tokeira `RunKey`, not the proto identity tuple; matches the existing
    /// `NexusTaskToken` convention). The outer envelope shape and the URL-safe base64
    /// alphabet are matched so wire-parity with a Temporal peer is later a swap of the
    /// inner codec only.
    pub fn encode(&self) -> Result<String> {
        use base64::Engine as _;
        let inner = serde_json::to_vec(self)
            .map_err(|error| anyhow!("encoding nexus completion token: {error}"))?;
        let envelope = CompletionTokenEnvelope {
            version: COMPLETION_TOKEN_VERSION,
            data: base64::engine::general_purpose::URL_SAFE.encode(inner),
        };
        serde_json::to_string(&envelope)
            .map_err(|error| anyhow!("encoding nexus completion token envelope: {error}"))
    }

    /// Decode and version-check the `{v,d}` envelope.
    ///
    /// Validates the envelope version before decoding the inner payload, matching
    /// `DecodeCallbackToken`'s `status.Errorf(codes.InvalidArgument, "unsupported token
    /// version: %d")` (`common/nexus/callback_token.go @ v1.31.0`). The
    /// `InvalidArgument`→`BadRequest` mapping for the wire response lives at the inbound
    /// `/nexus/callback` handler (Wave 5); here a wrong version is simply a decode
    /// failure whose message contains `"unsupported nexus completion token version"`.
    pub fn decode(s: &str) -> Result<Self> {
        use base64::Engine as _;
        let envelope: CompletionTokenEnvelope = serde_json::from_str(s)
            .map_err(|error| anyhow!("invalid nexus completion token: {error}"))?;
        if envelope.version != COMPLETION_TOKEN_VERSION {
            bail!(
                "unsupported nexus completion token version: {}",
                envelope.version
            );
        }
        let inner = base64::engine::general_purpose::URL_SAFE
            .decode(envelope.data.as_bytes())
            .map_err(|error| anyhow!("invalid nexus completion token data: {error}"))?;
        serde_json::from_slice(&inner)
            .map_err(|error| anyhow!("invalid nexus completion token payload: {error}"))
    }
}

/// A terminal Nexus operation completion to deliver. The `Nexus-Operation-State` header
/// value and the body are a **single discriminator** — mirroring `applyToHTTPRequest`'s
/// `Error != nil` branch (`common/nexus/nexusrpc/completion.go @ v1.31.0`) — so an
/// inconsistent state/body pair (e.g. `succeeded` carrying a failure body) is
/// unrepresentable. There is no terminal `running` completion, so it is excluded.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusCompletion {
    /// `Nexus-Operation-State: succeeded` — the result payloads (the payload serializer
    /// picks the content-type; may be empty).
    Succeeded(Payloads),
    /// `Nexus-Operation-State: failed` — the JSON-marshaled Nexus failure body bytes
    /// (built by the Wave 4 firing handler), sent with `Content-Type: application/json`.
    Failed(Vec<u8>),
    /// `Nexus-Operation-State: canceled` — the JSON-marshaled Nexus failure body, sent as
    /// state `canceled` (NOT `failed`), per `GetNexusCompletion`'s canceled arm @ v1.31.0.
    Canceled(Vec<u8>),
}

impl NexusCompletion {
    /// The exact `Nexus-Operation-State` header value (lowercase, American spelling),
    /// matching the nexus-rpc SDK `OperationState` consts used by `completion.go @ v1.31.0`.
    pub fn operation_state(&self) -> &'static str {
        match self {
            NexusCompletion::Succeeded(_) => "succeeded",
            NexusCompletion::Failed(_) => "failed",
            NexusCompletion::Canceled(_) => "canceled",
        }
    }
}

/// The JSON body of a `failed`/`canceled` completion `POST`. Carries the human-readable
/// message and the originating failure as a kernel [`Payload`] (a serialized
/// `temporal.api.failure.v1.Failure`), so the inbound `/nexus/callback` handler (Wave 5)
/// reconstructs the exact failure to record on the originator's terminal
/// `NexusOperationFailed`/`Canceled` event.
///
/// This is tokeira's **single-cluster loopback** failure shape — it is encoded here
/// (firing) and decoded by tokeira's own inbound endpoint. It deliberately differs from
/// v1.31.0's external `nexus.Failure` JSON (`{message, metadata, details}` with `details`
/// carrying an encoded payload); full external wire-parity is a `nexus-multi-cluster`
/// concern (design "Out of Scope"). The `Content-Type` is still `application/json`, so a
/// v1.31.0 peer's content-type check (Wave 5) is satisfied.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusCompletionFailureBody {
    pub message: String,
    pub failure: Payload,
}

impl NexusCompletionFailureBody {
    /// Serialize to the JSON body bytes carried in a `NexusCompletion::Failed`/`Canceled`.
    pub fn encode(&self) -> Vec<u8> {
        // A plain struct of (String, Payload) cannot fail to serialize; fall back to an
        // empty body rather than panicking on the off chance it does.
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Decode the JSON body bytes (used by the inbound `/nexus/callback` handler, Wave 5).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|error| anyhow!("invalid nexus completion failure body: {error}"))
    }
}

/// Outcome of a single completion-callback `POST`. The Wave 4 firing handler maps this
/// to a kernel `CallbackAttemptOutcome` (`Delivered`→`Succeeded`,
/// `RetryableError`→`RetryableFailure`, `NonRetryableError`→`NonRetryableFailure`).
#[derive(Clone, Debug, PartialEq)]
pub enum CompletionDeliveryOutcome {
    /// HTTP 2xx — the completion was accepted by the endpoint.
    Delivered,
    /// Transport error, or a retryable status — the callback should back off and retry.
    RetryableError { detail: String },
    /// A non-retryable handler-error status — the callback is terminally failed.
    NonRetryableError { detail: String },
}

/// Delivers an async Nexus operation completion to a callback URL over the Nexus
/// completion HTTP protocol. Mirrors `nexusrpc.CompletionHTTPClient.CompleteOperation`
/// (`common/nexus/nexusrpc/completion.go @ v1.31.0`).
#[async_trait]
pub trait NexusCompletionClient: Send + Sync {
    /// `POST {url}` a Nexus operation `completion`.
    ///
    /// Sets `Nexus-Operation-State` (from `completion`), `Temporal-Callback-Token` (the
    /// already-encoded `token` string, sent verbatim), and `User-Agent: temporalio/server`
    /// (the inbound handler validates this). The body follows the `completion` variant:
    /// `Succeeded` sends the result `Payloads` (payload-serializer content-type);
    /// `Failed`/`Canceled` send the JSON Nexus failure with `Content-Type:
    /// application/json`. The `url` is always a resolved `http(s)://` address — the runtime
    /// resolves `SYSTEM_CALLBACK_URL` to the configured local listener before calling.
    ///
    /// `links` are best-effort `Nexus-Link` headers and are **not essential to
    /// resolution** (design.md §5). Wave 3 carries them in the signature but does not
    /// yet emit the headers — link encoding lands with its producer (the firing handler
    /// that builds the workflow-event start link from the close event) in Wave 4/5.
    ///
    /// Returns the delivery outcome (never an `Err` for an HTTP/transport result — those
    /// are folded into `RetryableError`/`NonRetryableError`); the outer `Err` is
    /// reserved for un-classifiable pre-flight failures.
    async fn complete_operation(
        &self,
        url: &str,
        token: &str,
        completion: NexusCompletion,
        links: &[Link],
    ) -> Result<CompletionDeliveryOutcome>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum NexusTaskRequest {
    StartOperation {
        service: String,
        operation: String,
        request_id: String,
        payload: Option<Payload>,
        scheduled_time: Option<OffsetDateTime>,
        /// Callback URL the handler attaches to its backing workflow so the eventual
        /// async outcome is delivered back here. `Some(SYSTEM_CALLBACK_URL)` for a
        /// Worker-target dispatch; `None` when no completion callback is attached
        /// (e.g. the External arm, deferred to `nexus-multi-cluster`). The edge
        /// poll-response translation emits this as `StartOperationRequest.callback`.
        callback_url: Option<String>,
        /// Encoded [`NexusCompletionToken`] sent in the `Temporal-Callback-Token`
        /// header alongside `callback_url`, so the completion can be routed back to
        /// this originator's pending operation. `Some` iff `callback_url` is.
        callback_token: Option<String>,
    },
    CancelOperation {
        service: String,
        operation: String,
        operation_id: String,
        /// Handler-issued async operation token, sent as the wire `operation_token` so a
        /// WorkflowRunOperation handler can unmarshal it. Falls back to `operation_id` when
        /// the op never started (no handler token).
        operation_token: String,
    },
    /// Caller-facing HTTP request admitted and normalized by the compatibility
    /// edge. Every field is transport-neutral; public protobuf construction
    /// remains in `tokeira-edge`.
    Http(NexusHttpTaskRequest),
}

/// Neutral caller-facing Nexus request envelope carried by the disposable
/// runtime delivery broker.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusHttpTaskRequest {
    /// Eligible caller headers, normalized to lower-case keys.
    pub header: BTreeMap<String, String>,
    /// Time at which the edge admitted the request.
    pub scheduled_time: OffsetDateTime,
    /// Whether the caller accepts Temporal failure envelopes.
    pub temporal_failure_responses: bool,
    /// Resolved endpoint name, empty for namespace/task-queue dispatch.
    pub endpoint: String,
    /// Deadline used to compute the remaining worker request timeout at poll.
    pub dispatch_deadline: OffsetDateTime,
    /// Start or cancellation request data.
    pub variant: NexusHttpTaskRequestVariant,
}

/// Variant-specific fields for a neutral caller-facing Nexus request.
#[derive(Clone, Debug, PartialEq)]
pub enum NexusHttpTaskRequestVariant {
    /// Start a Nexus operation.
    StartOperation {
        /// Nexus service name.
        service: String,
        /// Nexus operation name.
        operation: String,
        /// Caller idempotency key.
        request_id: String,
        /// Callback URL for asynchronous completion.
        callback: String,
        /// Converted Temporal payload, populated only after authorization.
        payload: Option<Payload>,
        /// Callback headers with the `Nexus-Callback-` prefix removed.
        callback_header: BTreeMap<String, String>,
        /// Caller links.
        links: Vec<NexusTaskLink>,
    },
    /// Cancel a previously-started Nexus operation.
    CancelOperation {
        /// Nexus service name.
        service: String,
        /// Nexus operation name.
        operation: String,
        /// Compatibility copy of the operation token.
        operation_id: String,
        /// Handler-authored operation token.
        operation_token: String,
    },
}

/// Neutral Nexus link represented as the protocol URL and type strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NexusTaskLink {
    /// Link target URL.
    pub url: String,
    /// Nexus link type.
    pub link_type: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusTask {
    pub token: NexusTaskToken,
    pub request: NexusTaskRequest,
    /// Exact final server-authored delivery origin.
    pub origin: WorkerTaskOrigin,
}

/// Exact disposable delivery identity for one Nexus task-queue partition.
///
/// Deployment coordinates are absent together for unversioned work. They are
/// deliberately excluded from [`NexusTaskToken`], whose public bytes remain the
/// v1.31.0 namespace/queue/task-ID contract.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NexusQueueKey {
    /// Namespace containing the task queue.
    pub namespace_id: NamespaceId,
    /// Logical task-queue family.
    pub task_queue: TaskQueueName,
    /// Worker Deployment name for exact-version delivery.
    pub deployment: Option<DeploymentId>,
    /// Build ID for exact-version delivery.
    pub build_id: Option<BuildId>,
}

impl NexusQueueKey {
    /// Construct an unversioned Nexus queue identity.
    #[must_use]
    pub const fn unversioned(namespace_id: NamespaceId, task_queue: TaskQueueName) -> Self {
        Self {
            namespace_id,
            task_queue,
            deployment: None,
            build_id: None,
        }
    }

    /// Construct an exact-version or unversioned identity from workflow state.
    #[must_use]
    pub fn from_version(
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        version: Option<&WorkerDeploymentVersionRef>,
    ) -> Self {
        Self {
            namespace_id,
            task_queue,
            deployment: version.map(|version| DeploymentId(version.deployment_name.clone())),
            build_id: version.map(|version| BuildId(version.build_id.clone())),
        }
    }

    /// Convert the final broker key into Worker authorization evidence.
    #[must_use]
    pub fn worker_task_origin(&self) -> WorkerTaskOrigin {
        WorkerTaskOrigin {
            namespace_id: self.namespace_id,
            normal_task_queue: self.task_queue.clone(),
            task_class: WorkerTaskClass::Nexus,
            deployment: self
                .deployment
                .clone()
                .unwrap_or_else(|| DeploymentId(String::new())),
            build_id: self
                .build_id
                .clone()
                .unwrap_or_else(|| BuildId(String::new())),
        }
    }
}

#[derive(Clone)]
pub struct NexusTaskBroker {
    inner: Arc<AsyncMutex<NexusBrokerState>>,
    config_store: Arc<RwLock<Arc<dyn TaskQueueConfigStore>>>,
    observation_sink: Arc<RwLock<Arc<dyn crate::DemandObservationSink>>>,
    queue_metrics: Arc<RwLock<crate::WorkerComputeQueueMetrics>>,
    waiters: Arc<Mutex<HashMap<NexusQueueKey, usize>>>,
    worker_compute_waiters: Arc<
        Mutex<
            HashMap<
                (uuid::Uuid, u64),
                oneshot::Sender<crate::worker_compute::WorkerComputeProviderCompletion>,
            >,
        >,
    >,
}

impl std::fmt::Debug for NexusTaskBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusTaskBroker").finish_non_exhaustive()
    }
}

impl Default for NexusTaskBroker {
    fn default() -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(NexusBrokerState::default())),
            config_store: Arc::new(RwLock::new(Arc::new(
                InMemoryTaskQueueConfigStore::default(),
            ))),
            observation_sink: Arc::new(RwLock::new(Arc::new(crate::DisabledWorkerComputeSink))),
            queue_metrics: Arc::new(RwLock::new(crate::WorkerComputeQueueMetrics::default())),
            waiters: Arc::new(Mutex::new(HashMap::new())),
            worker_compute_waiters: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
struct NexusBrokerState {
    ready: HashMap<NexusQueueKey, VecDeque<NexusTask>>,
    /// Private result routes keyed by the UUID carried in the protobuf token.
    /// A response atomically removes its entry so duplicate and late responses
    /// cannot resolve a different delivery.
    outstanding: HashMap<String, NexusTaskCorrelation>,
    /// Per-queue wake handles (see `tokeira-runtime::broker`'s per-queue wake
    /// pattern): a publish wakes only pollers on that namespace+task-queue.
    wakes: HashMap<NexusQueueKey, Arc<Notify>>,
    /// Worker identities whose current and subsequent long polls must return
    /// empty after an enabled `ShutdownWorker` cancellation.
    denied_workers: HashSet<(NamespaceId, TaskQueueName, WorkerIdentity)>,
    rate_origin: tokio::time::Instant,
    rate_limits: DispatchRateLimits,
}

impl Default for NexusBrokerState {
    fn default() -> Self {
        Self {
            ready: HashMap::new(),
            outstanding: HashMap::new(),
            wakes: HashMap::new(),
            denied_workers: HashSet::new(),
            rate_origin: tokio::time::Instant::now(),
            rate_limits: DispatchRateLimits::default(),
        }
    }
}

enum NexusTakeOutcome {
    Ready(NexusTask),
    WaitUntil(tokio::time::Instant),
    Blocked,
    Empty,
}

fn nexus_config_key(key: &NexusQueueKey) -> TaskQueueConfigKey {
    TaskQueueConfigKey {
        namespace_id: key.namespace_id,
        task_queue: key.task_queue.clone(),
        kind: TaskQueueConfigKind::Nexus,
    }
}

struct NexusWaiterGuard {
    waiters: Arc<Mutex<HashMap<NexusQueueKey, usize>>>,
    key: NexusQueueKey,
}

impl NexusWaiterGuard {
    fn register(waiters: Arc<Mutex<HashMap<NexusQueueKey, usize>>>, key: NexusQueueKey) -> Self {
        *waiters
            .lock()
            .expect("Nexus waiter-count lock poisoned")
            .entry(key.clone())
            .or_default() += 1;
        Self { waiters, key }
    }
}

impl Drop for NexusWaiterGuard {
    fn drop(&mut self) {
        let mut waiters = self
            .waiters
            .lock()
            .expect("Nexus waiter-count lock poisoned");
        let Some(count) = waiters.get_mut(&self.key) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            waiters.remove(&self.key);
        }
    }
}

impl NexusTaskBroker {
    /// Share the live volatile task-queue configuration store.
    pub fn set_task_queue_config_store(&self, store: Arc<dyn TaskQueueConfigStore>) {
        *self
            .config_store
            .write()
            .expect("Nexus task queue config lock poisoned") = store;
    }

    /// Share the process-local worker-compute observation sink with broker clones.
    pub fn set_demand_observation_sink(&self, sink: Arc<dyn crate::DemandObservationSink>) {
        *self
            .observation_sink
            .write()
            .expect("Nexus observation-sink lock poisoned") = sink;
    }

    /// Share the process-local periodic queue-metrics recorder with broker clones.
    pub fn set_worker_compute_queue_metrics(&self, metrics: crate::WorkerComputeQueueMetrics) {
        *self
            .queue_metrics
            .write()
            .expect("Nexus queue-metrics lock poisoned") = metrics;
    }

    /// Publish an already-authored task without a completion route.
    ///
    /// This remains for poll-only tests and one-way fixtures. Production
    /// workflow dispatch uses [`Self::publish_workflow`] so a response route is
    /// registered before the task becomes visible.
    pub async fn publish(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        mut task: NexusTask,
    ) {
        let key = NexusQueueKey::unversioned(namespace_id, task_queue);
        task.origin = key.worker_task_origin();
        let mut inner = self.inner.lock().await;
        inner.ready.entry(key.clone()).or_default().push_back(task);
        let wake = inner.wakes.entry(key.clone()).or_default().clone();
        drop(inner);
        wake.notify_waiters();
    }

    /// Publish a workflow-originated task and retain its private response route.
    ///
    /// Correlation insertion and queue visibility happen under the same lock;
    /// therefore even an immediately responding worker cannot beat registration.
    pub async fn publish_workflow(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        request: NexusTaskRequest,
    ) {
        self.publish_workflow_versioned(
            NexusQueueKey::unversioned(namespace_id, task_queue),
            run_key,
            operation_id,
            scheduled_event_id,
            request,
        )
        .await;
    }

    /// Publish workflow-originated work to one exact Deployment Version.
    ///
    /// The version affects disposable handout identity only. Worker-visible token
    /// bytes and private response correlation remain identical to unversioned work.
    pub async fn publish_workflow_versioned(
        &self,
        key: NexusQueueKey,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        request: NexusTaskRequest,
    ) {
        let task_kind = match &request {
            NexusTaskRequest::StartOperation { .. } => NexusWorkflowTaskKind::StartOperation,
            NexusTaskRequest::CancelOperation { .. } => NexusWorkflowTaskKind::CancelOperation,
            NexusTaskRequest::Http(request) => match request.variant {
                NexusHttpTaskRequestVariant::StartOperation { .. } => {
                    NexusWorkflowTaskKind::StartOperation
                }
                NexusHttpTaskRequestVariant::CancelOperation { .. } => {
                    NexusWorkflowTaskKind::CancelOperation
                }
            },
        };
        let has_waiter = self
            .waiters
            .lock()
            .expect("Nexus waiter-count lock poisoned")
            .get(&key)
            .copied()
            .unwrap_or(0)
            > 0;
        let mut inner = self.inner.lock().await;
        let task_id = loop {
            let candidate = uuid::Uuid::new_v4().to_string();
            if !inner.outstanding.contains_key(&candidate) {
                break candidate;
            }
        };
        let token = NexusTaskToken {
            namespace_id: key.namespace_id.0.to_string(),
            task_queue: key.task_queue.0.clone(),
            task_id: task_id.clone(),
        };
        inner.outstanding.insert(
            task_id,
            NexusTaskCorrelation::Workflow {
                run_key,
                operation_id,
                scheduled_event_id,
                task_kind,
                origin: key.worker_task_origin(),
            },
        );
        inner
            .ready
            .entry(key.clone())
            .or_default()
            .push_back(NexusTask {
                token,
                request,
                origin: key.worker_task_origin(),
            });
        let wake = inner.wakes.entry(key.clone()).or_default().clone();
        drop(inner);
        if let (Some(deployment_name), Some(build_id)) =
            (key.deployment.clone(), key.build_id.clone())
        {
            let observation = crate::DemandObservation {
                namespace_id: key.namespace_id,
                task_queue: key.task_queue,
                task_type: WorkerComputeTaskType::Nexus,
                deployment_name,
                build_id,
                match_kind: if has_waiter {
                    crate::DemandMatchKind::Sync
                } else {
                    crate::DemandMatchKind::NoSync
                },
            };
            self.queue_metrics
                .read()
                .expect("Nexus queue-metrics lock poisoned")
                .record_add(observation.queue_key());
            let sink = self
                .observation_sink
                .read()
                .expect("Nexus observation-sink lock poisoned")
                .clone();
            let _ = sink.try_observe(observation);
        }
        wake.notify_waiters();
    }

    /// Publish a caller-facing HTTP task and return its delivery lease.
    ///
    /// The opaque waiter route is inserted under the same lock that makes the
    /// task pollable. This Tokeira-native atomic publication ensures an
    /// immediately responding worker can always find the caller correlation,
    /// preserving the v1.31.0 observable dispatch contract.
    pub async fn publish_http(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        waiter_id: String,
        request: NexusTaskRequest,
    ) -> NexusHttpDispatchLease {
        let task_id = uuid::Uuid::new_v4().to_string();
        let token = NexusTaskToken {
            namespace_id: namespace_id.0.to_string(),
            task_queue: task_queue.0.clone(),
            task_id: task_id.clone(),
        };
        let key = NexusQueueKey::unversioned(namespace_id, task_queue);
        let mut inner = self.inner.lock().await;
        inner.outstanding.insert(
            task_id.clone(),
            NexusTaskCorrelation::Http {
                waiter_id,
                origin: key.worker_task_origin(),
            },
        );
        inner
            .ready
            .entry(key.clone())
            .or_default()
            .push_back(NexusTask {
                token,
                request,
                origin: key.worker_task_origin(),
            });
        let wake = inner.wakes.entry(key).or_default().clone();
        drop(inner);
        wake.notify_waiters();
        NexusHttpDispatchLease {
            task_id: Some(task_id),
            broker: self.clone(),
        }
    }

    /// Register one attempt-scoped worker-compute completion receiver.
    ///
    /// Registration precedes task visibility, so an immediately responding worker
    /// cannot beat the waiter. A duplicate live claim is rejected rather than
    /// silently replacing the receiver that owns its result.
    pub fn register_worker_compute_waiter(
        &self,
        action_id: uuid::Uuid,
        claim_epoch: u64,
    ) -> Option<oneshot::Receiver<crate::worker_compute::WorkerComputeProviderCompletion>> {
        let (sender, receiver) = oneshot::channel();
        let mut waiters = self
            .worker_compute_waiters
            .lock()
            .expect("worker-compute Nexus waiter lock poisoned");
        if waiters.contains_key(&(action_id, claim_epoch)) {
            return None;
        }
        waiters.insert((action_id, claim_epoch), sender);
        Some(receiver)
    }

    /// Publish one synchronous Worker-target provider attempt.
    pub async fn publish_worker_compute(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        action_id: uuid::Uuid,
        claim_epoch: u64,
        request: NexusTaskRequest,
    ) -> NexusWorkerComputeDispatchLease {
        let task_id = uuid::Uuid::new_v4().to_string();
        let token = NexusTaskToken {
            namespace_id: namespace_id.0.to_string(),
            task_queue: task_queue.0.clone(),
            task_id: task_id.clone(),
        };
        let key = NexusQueueKey::unversioned(namespace_id, task_queue);
        let mut inner = self.inner.lock().await;
        inner.outstanding.insert(
            task_id.clone(),
            NexusTaskCorrelation::WorkerCompute {
                action_id,
                claim_epoch,
                origin: key.worker_task_origin(),
            },
        );
        inner
            .ready
            .entry(key.clone())
            .or_default()
            .push_back(NexusTask {
                token,
                request,
                origin: key.worker_task_origin(),
            });
        let wake = inner.wakes.entry(key).or_default().clone();
        drop(inner);
        wake.notify_waiters();
        NexusWorkerComputeDispatchLease {
            task_id: Some(task_id),
            action_id,
            claim_epoch,
            broker: self.clone(),
        }
    }

    /// Resolve the current worker-compute waiter after edge translation.
    #[must_use]
    pub fn complete_worker_compute(
        &self,
        action_id: uuid::Uuid,
        claim_epoch: u64,
        completion: crate::worker_compute::WorkerComputeProviderCompletion,
    ) -> bool {
        self.worker_compute_waiters
            .lock()
            .expect("worker-compute Nexus waiter lock poisoned")
            .remove(&(action_id, claim_epoch))
            .is_some_and(|sender| sender.send(completion).is_ok())
    }

    /// Remove a waiter whose attempt timed out or was cancelled.
    pub fn cancel_worker_compute_waiter(&self, action_id: uuid::Uuid, claim_epoch: u64) {
        self.worker_compute_waiters
            .lock()
            .expect("worker-compute Nexus waiter lock poisoned")
            .remove(&(action_id, claim_epoch));
    }

    /// Atomically consume the response route for `task_id`.
    ///
    /// Unknown, expired, and repeated responses return `None` without touching
    /// any other outstanding task.
    pub async fn consume(&self, task_id: &str) -> Option<NexusTaskCorrelation> {
        self.inner.lock().await.outstanding.remove(task_id)
    }

    /// Read one response route without consuming its single-use fence.
    ///
    /// The compatibility edge uses this before authorization; only a
    /// subsequently authorized response may call [`Self::consume`].
    pub async fn correlation(&self, task_id: &str) -> Option<NexusTaskCorrelation> {
        self.inner.lock().await.outstanding.get(task_id).cloned()
    }

    async fn expire_http(&self, task_id: &str) {
        let mut inner = self.inner.lock().await;
        if !matches!(
            inner.outstanding.get(task_id),
            Some(NexusTaskCorrelation::Http { .. })
        ) {
            return;
        }
        inner.outstanding.remove(task_id);
        for ready in inner.ready.values_mut() {
            ready.retain(|task| task.token.task_id != task_id);
        }
        inner.ready.retain(|_, ready| !ready.is_empty());
    }

    async fn expire_worker_compute(&self, task_id: &str, action_id: uuid::Uuid, claim_epoch: u64) {
        let mut inner = self.inner.lock().await;
        if !matches!(
            inner.outstanding.get(task_id),
            Some(NexusTaskCorrelation::WorkerCompute {
                action_id: current_action,
                claim_epoch: current_epoch,
                ..
            }) if *current_action == action_id && *current_epoch == claim_epoch
        ) {
            return;
        }
        inner.outstanding.remove(task_id);
        for ready in inner.ready.values_mut() {
            ready.retain(|task| task.token.task_id != task_id);
        }
        inner.ready.retain(|_, ready| !ready.is_empty());
    }

    pub async fn poll(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        wait_for: tokio::time::Duration,
    ) -> Option<NexusTask> {
        self.poll_versioned(
            NexusQueueKey::unversioned(namespace_id, task_queue),
            wait_for,
        )
        .await
    }

    /// Poll one exact Deployment-Version Nexus queue.
    pub async fn poll_versioned(
        &self,
        key: NexusQueueKey,
        wait_for: tokio::time::Duration,
    ) -> Option<NexusTask> {
        self.poll_versioned_for_worker(key, &WorkerIdentity(String::new()), wait_for)
            .await
    }

    /// Poll one exact Deployment-Version Nexus queue for a Worker identity.
    ///
    /// The identity remains disposable broker evidence. It exists only so an
    /// enabled `ShutdownWorker` request can wake the SDK's parked Nexus long
    /// poll and prevent a zombie re-poll from taking new work, matching
    /// `cancelOutstandingWorkerPolls` in
    /// `service/frontend/workflow_handler.go @ v1.31.0`.
    pub async fn poll_versioned_for_worker(
        &self,
        key: NexusQueueKey,
        worker: &WorkerIdentity,
        wait_for: tokio::time::Duration,
    ) -> Option<NexusTask> {
        if self.is_denied(&key, worker).await {
            return None;
        }
        match self.try_take(&key).await {
            NexusTakeOutcome::Ready(task) => return Some(task),
            NexusTakeOutcome::WaitUntil(_)
            | NexusTakeOutcome::Blocked
            | NexusTakeOutcome::Empty => {}
        }

        // The cancellation-safe guard is intentionally separate from async broker
        // state: dropping a gRPC poll future must immediately remove its demand
        // without spawning cleanup work or risking a stale sync-match observation.
        let _waiter = NexusWaiterGuard::register(self.waiters.clone(), key.clone());
        // Per-queue wake + deadline loop: a publish on another nexus queue must
        // not end this poll, and a wake is a hint to re-check, not a result (we
        // wait until the deadline if there is still nothing for this queue).
        let wake = self.queue_wake(&key).await;
        let config_key = nexus_config_key(&key);
        let config_store = self
            .config_store
            .read()
            .expect("Nexus task queue config lock poisoned")
            .clone();
        let config_changed = config_store.changed(&config_key);
        let deadline = tokio::time::Instant::now() + wait_for;
        loop {
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let changed = config_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            if self.is_denied(&key, worker).await {
                return None;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let rate_wait = match self.try_take(&key).await {
                NexusTakeOutcome::Ready(task) => return Some(task),
                NexusTakeOutcome::WaitUntil(eligible_at) => {
                    eligible_at.saturating_duration_since(tokio::time::Instant::now())
                }
                NexusTakeOutcome::Blocked | NexusTakeOutcome::Empty => remaining,
            };
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = changed.as_mut() => {}
                _ = tokio::time::sleep(rate_wait.min(remaining)) => {}
            }
        }
    }

    /// Cancel current and reject future Nexus polls for one shutting-down
    /// Worker identity on a task-queue family.
    pub async fn deny_worker(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        worker: WorkerIdentity,
    ) {
        let mut inner = self.inner.lock().await;
        inner
            .denied_workers
            .insert((namespace_id, task_queue, worker));
        // Shutdown identifies a queue family rather than an exact versioned
        // broker key. Wake all Nexus waiters; only the denied identity exits
        // after each poll re-checks the deny set.
        let wakes = inner.wakes.values().cloned().collect::<Vec<_>>();
        drop(inner);
        for wake in wakes {
            wake.notify_waiters();
        }
    }

    async fn is_denied(&self, key: &NexusQueueKey, worker: &WorkerIdentity) -> bool {
        !worker.0.is_empty()
            && self.inner.lock().await.denied_workers.contains(&(
                key.namespace_id,
                key.task_queue.clone(),
                worker.clone(),
            ))
    }

    /// Whether a Nexus poll that just claimed one task left runnable work on
    /// the same queue. Nexus tasks have no durable matching backlog in
    /// v1.31.0, so its poller scaler uses add/dispatch pressure instead
    /// (`physical_task_queue_manager.go:878-883 @ v1.31.0`); remaining ready
    /// tasks are Tokeira's direct equivalent signal.
    pub async fn has_runnable_backlog(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
    ) -> bool {
        self.has_runnable_backlog_versioned(&NexusQueueKey::unversioned(
            namespace_id,
            task_queue.clone(),
        ))
        .await
    }

    /// Whether one exact-version Nexus queue has remaining runnable work.
    pub async fn has_runnable_backlog_versioned(&self, key: &NexusQueueKey) -> bool {
        self.inner
            .lock()
            .await
            .ready
            .get(key)
            .is_some_and(|ready| !ready.is_empty())
    }

    /// Snapshot live exact-version Nexus backlog in deterministic queue order.
    ///
    /// This is advisory matching state. The queue sampler combines it with
    /// authoritative pending-operation reconstruction so broker loss cannot hide
    /// persistent demand from capacity policy.
    pub async fn versioned_backlog_counts(
        &self,
    ) -> BTreeMap<tokeira_types::WorkerComputeQueueKey, u64> {
        self.inner
            .lock()
            .await
            .ready
            .iter()
            .filter_map(|(key, ready)| {
                let (Some(deployment_name), Some(build_id)) =
                    (key.deployment.clone(), key.build_id.clone())
                else {
                    return None;
                };
                Some((
                    tokeira_types::WorkerComputeQueueKey {
                        namespace_id: key.namespace_id,
                        deployment_name,
                        build_id,
                        task_type: WorkerComputeTaskType::Nexus,
                        task_queue: key.task_queue.clone(),
                    },
                    u64::try_from(ready.len()).unwrap_or(u64::MAX),
                ))
            })
            .collect()
    }

    async fn queue_wake(&self, key: &NexusQueueKey) -> Arc<Notify> {
        self.inner
            .lock()
            .await
            .wakes
            .entry(key.clone())
            .or_default()
            .clone()
    }

    async fn try_take(&self, key: &NexusQueueKey) -> NexusTakeOutcome {
        let config_key = nexus_config_key(key);
        let config_store = self
            .config_store
            .read()
            .expect("Nexus task queue config lock poisoned")
            .clone();
        let config = config_store
            .get(&config_key)
            .await
            .expect("hydrated task-queue cache reads are infallible");
        let mut inner = self.inner.lock().await;
        if inner.ready.get(key).is_none_or(VecDeque::is_empty) {
            return NexusTakeOutcome::Empty;
        }
        let effective = EffectivePriority {
            priority_key: 3,
            fairness_key: String::new(),
            fairness_weight: 1.0,
        };
        let now = inner.rate_origin.elapsed();
        match inner
            .rate_limits
            .inspect(&config_key, &effective, config.as_ref(), now)
        {
            DispatchEligibility::Blocked => return NexusTakeOutcome::Blocked,
            DispatchEligibility::At(offset) => {
                return NexusTakeOutcome::WaitUntil(inner.rate_origin + offset);
            }
            DispatchEligibility::Ready => {}
        }
        let task = inner.ready.get_mut(key).and_then(VecDeque::pop_front);
        let metric_key = task.as_ref().and_then(|_| {
            let (Some(deployment_name), Some(build_id)) = (&key.deployment, &key.build_id) else {
                return None;
            };
            Some(
                crate::DemandObservation {
                    namespace_id: key.namespace_id,
                    task_queue: key.task_queue.clone(),
                    task_type: WorkerComputeTaskType::Nexus,
                    deployment_name: deployment_name.clone(),
                    build_id: build_id.clone(),
                    match_kind: crate::DemandMatchKind::NoSync,
                }
                .queue_key(),
            )
        });
        if task.is_some() {
            inner
                .rate_limits
                .consume(&config_key, &effective, config.as_ref(), now);
        }
        let outcome = task.map_or(NexusTakeOutcome::Empty, NexusTakeOutcome::Ready);
        drop(inner);
        if let Some(metric_key) = metric_key {
            self.queue_metrics
                .read()
                .expect("Nexus queue-metrics lock poisoned")
                .record_dispatch(metric_key);
        }
        outcome
    }

    /// Remove all not-yet-delivered Nexus tasks owned by a deleted run.
    pub async fn remove_run(&self, run_key: RunKey) {
        let mut inner = self.inner.lock().await;
        let removed_task_ids: HashSet<_> = inner
            .outstanding
            .iter()
            .filter_map(|(task_id, correlation)| match correlation {
                NexusTaskCorrelation::Workflow { run_key: owner, .. } if *owner == run_key => {
                    Some(task_id.clone())
                }
                _ => None,
            })
            .collect();
        inner
            .outstanding
            .retain(|task_id, _| !removed_task_ids.contains(task_id));
        for ready in inner.ready.values_mut() {
            ready.retain(|task| !removed_task_ids.contains(&task.token.task_id));
        }
        inner.ready.retain(|_, ready| !ready.is_empty());
    }
}

/// Lease for one in-flight caller-facing Nexus dispatch.
///
/// Dropping the HTTP request removes its private route and any not-yet-polled
/// task. This is transport cleanup only: caller-facing Nexus dispatch has no
/// durable workflow fact to preserve or recover.
#[derive(Debug)]
pub struct NexusHttpDispatchLease {
    task_id: Option<String>,
    broker: NexusTaskBroker,
}

/// Attempt-scoped cleanup for a Worker-target provider task.
#[derive(Debug)]
pub struct NexusWorkerComputeDispatchLease {
    task_id: Option<String>,
    action_id: uuid::Uuid,
    claim_epoch: u64,
    broker: NexusTaskBroker,
}

impl NexusWorkerComputeDispatchLease {
    /// Deterministically remove an unfinished route and waiter.
    pub async fn cancel(mut self) {
        self.broker
            .cancel_worker_compute_waiter(self.action_id, self.claim_epoch);
        if let Some(task_id) = self.task_id.take() {
            self.broker
                .expire_worker_compute(&task_id, self.action_id, self.claim_epoch)
                .await;
        }
    }
}

impl Drop for NexusWorkerComputeDispatchLease {
    fn drop(&mut self) {
        self.broker
            .cancel_worker_compute_waiter(self.action_id, self.claim_epoch);
        let Some(task_id) = self.task_id.take() else {
            return;
        };
        let broker = self.broker.clone();
        let action_id = self.action_id;
        let claim_epoch = self.claim_epoch;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                broker
                    .expire_worker_compute(&task_id, action_id, claim_epoch)
                    .await;
            });
        }
    }
}

impl NexusHttpDispatchLease {
    /// Deterministically remove this caller-facing delivery route.
    ///
    /// HTTP handlers use this on every normal return, including timeout. The
    /// `Drop` fallback remains necessary when client cancellation drops the
    /// handler future before it can run this finalizer.
    pub async fn cancel(mut self) {
        if let Some(task_id) = self.task_id.take() {
            self.broker.expire_http(&task_id).await;
        }
    }
}

impl Drop for NexusHttpDispatchLease {
    fn drop(&mut self) {
        let Some(task_id) = self.task_id.take() else {
            return;
        };
        let broker = self.broker.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                broker.expire_http(&task_id).await;
            });
        }
    }
}

#[derive(Debug)]
pub struct NoopNexusHttpClient;

#[async_trait]
impl NexusHttpClient for NoopNexusHttpClient {
    async fn start_operation(
        &self,
        _address: &str,
        _operation_id: &str,
        _service: &str,
        _operation: &str,
        _input: &Payloads,
        _schedule_to_close_timeout: Option<Duration>,
        _trace_headers: &[KeyValue],
    ) -> Result<NexusStartResult> {
        Err(anyhow!("nexus http client not configured"))
    }

    async fn cancel_operation(
        &self,
        _address: &str,
        _service: &str,
        _operation: &str,
        _operation_token: &str,
        _trace_headers: &[KeyValue],
    ) -> Result<NexusCancelResult> {
        Err(anyhow!("nexus http client not configured"))
    }
}

/// Test completion client: performs no I/O and reports every completion as
/// `Delivered`, so firing-path tests advance to `CompletionCallbackAttempted(Succeeded)`
/// without standing up an HTTP listener.
///
/// This is deliberately the opposite philosophy from [`NoopNexusHttpClient`] (which
/// errors to surface a missing client loudly): it silently "succeeds". It is intended
/// as an explicit **test** double — the production runtime must wire the real
/// `HttpNexusCompletionClient`. The runtime-constructor default (loud-vs-silent) is
/// decided when delivery is wired (Wave 4).
#[derive(Debug)]
pub struct NoopNexusCompletionClient;

#[async_trait]
impl NexusCompletionClient for NoopNexusCompletionClient {
    async fn complete_operation(
        &self,
        _url: &str,
        _token: &str,
        _completion: NexusCompletion,
        _links: &[Link],
    ) -> Result<CompletionDeliveryOutcome> {
        Ok(CompletionDeliveryOutcome::Delivered)
    }
}

/// Volatile index of which open Nexus operations to watch for timeouts.
///
/// This is a derived index, not authority: it lists *which* `(run, operation)`
/// pairs the scanner must check. The timeout deadlines, `started`/`started_at`
/// anchors, and current liveness are read from the durable `PendingNexusOperation`
/// at scan time (history is authority, AGENTS §3). On shard takeover the index is
/// rebuilt from durable state by `crate::recovery::sweep_shard`; losing it only
/// delays firing until the rebuild, never changes the outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusTimeoutEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub operation_id: String,
    pub scheduled_event_id: i64,
    pub scheduled_at: OffsetDateTime,
}

#[derive(Clone, Default, Debug)]
pub struct NexusTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, String), NexusTimeoutEntry>>>,
}

impl NexusTimeoutTrackingState {
    pub fn insert(&self, entry: NexusTimeoutEntry) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .insert((entry.run_key, entry.operation_id.clone()), entry);
    }

    pub fn remove(&self, run_key: RunKey, operation_id: &str) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .remove(&(run_key, operation_id.to_string()));
    }

    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .retain(|(candidate, _), _| *candidate != run_key);
    }

    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    pub fn snapshot(&self) -> Vec<NexusTimeoutEntry> {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<NexusTimeoutEntry> {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .values()
            .filter(|entry| entry.shard_id == shard_id)
            .cloned()
            .collect()
    }
}

/// Volatile index of which `BackingOff` completion callbacks the completion-callback
/// scanner must re-fire.
///
/// Like [`NexusTimeoutTrackingState`] this is a derived index, not authority: it lists
/// *which* `(run, callback_index)` pairs are backing off. The `next_attempt_at`
/// deadline and current `CallbackState` are read from the durable `CompletionCallback`
/// at scan time (history is authority, AGENTS §3). On shard takeover the index is
/// rebuilt from durable state by `crate::recovery::sweep_shard`; losing it only
/// delays a retry until the rebuild, never changes the outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionCallbackTrackingEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub callback_index: usize,
}

#[derive(Clone, Default, Debug)]
pub struct CompletionCallbackTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, usize), CompletionCallbackTrackingEntry>>>,
}

impl CompletionCallbackTrackingState {
    pub fn insert(&self, entry: CompletionCallbackTrackingEntry) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .insert((entry.run_key, entry.callback_index), entry);
    }

    pub fn remove(&self, run_key: RunKey, callback_index: usize) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .remove(&(run_key, callback_index));
    }

    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .retain(|(candidate, _), _| *candidate != run_key);
    }

    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    pub fn snapshot(&self) -> Vec<CompletionCallbackTrackingEntry> {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<CompletionCallbackTrackingEntry> {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .values()
            .filter(|entry| entry.shard_id == shard_id)
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NexusTimeoutScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_timeouts_per_scan: usize,
}

impl Default for NexusTimeoutScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_timeouts_per_scan: 100,
        }
    }
}

/// Cadence + per-tick bound for the completion-callback retry scanner (mirrors
/// [`NexusTimeoutScannerConfig`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionCallbackScannerConfig {
    pub scan_interval: tokio::time::Duration,
    pub max_per_scan: usize,
}

impl Default for CompletionCallbackScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: tokio::time::Duration::from_secs(1),
            max_per_scan: 100,
        }
    }
}

/// Runtime-side completion-delivery knobs (a runtime-local mirror of
/// `tokeira_config::NexusCompletionConfig`; the kernel stays config-free and the
/// runtime crate does not depend on `tokeira-config`). `tokeirad` copies the policy
/// values into this when constructing the runtime; tests use [`Default`].
#[derive(Clone, Debug, PartialEq)]
pub struct NexusCompletionRuntimeConfig {
    /// The address `temporal://system` resolves to when a callback fires (the local
    /// `/nexus/callback` listener).
    pub system_callback_url: String,
    /// First retry backoff interval.
    pub retry_initial_interval: Duration,
    /// Cap on the backoff interval.
    pub retry_max_interval: Duration,
    /// Per-attempt backoff multiplier.
    pub retry_backoff_coefficient: f64,
    /// Max delivery attempts; `0` = unbounded (v1.31.0 `NoInterval` semantics).
    pub retry_max_attempts: u32,
}

impl Default for NexusCompletionRuntimeConfig {
    fn default() -> Self {
        // Mirrors tokeira_config::NexusCompletionConfig defaults (async-completion Wave 0):
        // 1s initial / 1h max / 2.0 coefficient, unbounded attempts.
        Self {
            system_callback_url: "http://127.0.0.1:7253".to_string(),
            retry_initial_interval: Duration::seconds(1),
            retry_max_interval: Duration::hours(1),
            retry_backoff_coefficient: 2.0,
            retry_max_attempts: 0,
        }
    }
}

/// Bundle of the completion-delivery dependencies the runtime threads from its
/// constructors to the publisher + the completion-callback scanner: the outbound
/// client, the delivery/backoff config, and the scanner cadence. Defaults to a
/// non-delivering [`NoopNexusCompletionClient`] so the many `TokeiraRuntime::new`
/// call sites (and tests that don't exercise delivery) need no changes; `tokeirad`
/// injects the real [`HttpNexusCompletionClient`](crate::nexus_http::HttpNexusCompletionClient).
#[derive(Clone)]
pub struct NexusCompletionDeps {
    pub client: Arc<dyn NexusCompletionClient>,
    pub config: NexusCompletionRuntimeConfig,
    pub scanner: CompletionCallbackScannerConfig,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for NexusCompletionDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusCompletionDeps")
            .finish_non_exhaustive()
    }
}

impl Default for NexusCompletionDeps {
    fn default() -> Self {
        Self {
            client: Arc::new(NoopNexusCompletionClient),
            config: NexusCompletionRuntimeConfig::default(),
            scanner: CompletionCallbackScannerConfig::default(),
        }
    }
}

/// Next backoff delay for a completion-callback retry: `initial * coefficient^(attempt-1)`,
/// capped at `max_interval`, then jittered. `attempt` is the 1-based number of the attempt
/// that just failed (so the first failure backs off by ~`initial`). `jitter_seed` is a
/// per-callback value (derived from the run key + callback index) that de-correlates
/// different callbacks' retries. The kernel performs no backoff math (`command.rs`
/// `CallbackAttemptOutcome` doc); the runtime computes this and passes `next_attempt_at`
/// into `CompletionCallbackAttempted`.
///
/// Jitter: the interval is scaled by a factor in `[0.8, 1.0)`, mirroring v1.31.0's
/// `addJitter` (`nextInterval*0.8 + rand(0..0.2*nextInterval)`,
/// `common/backoff/retrypolicy.go:178-187 @ v1.31.0`) — same anti-synchronized-retry-storm
/// goal. The factor is **deterministic** (a hash of `(jitter_seed, attempt)`), not random:
/// tokeira has no `rand` dependency and a deterministic, sleep-free-testable scanner is
/// preferred; different callbacks still de-correlate via distinct seeds. Mechanism
/// deviation only (no randomness), not a goal or wire-contract deviation.
///
/// The result is floored at a strictly positive minimum so the scanner can never hot-loop
/// on a zero/negative deadline (v1.31.0 stops retrying on a non-positive interval,
/// `retrypolicy.go:154-170 @ v1.31.0`; tokeira returns a plain `Duration`, so a
/// misconfigured non-positive `initial` degrades to retrying at the floor). Under the
/// validated config (`tokeira_config::NexusCompletionConfig`: `initial > 0`,
/// `coefficient >= 1.0`, `max >= initial`) the floor is a no-op.
pub fn nexus_completion_backoff(
    config: &NexusCompletionRuntimeConfig,
    attempt: u32,
    jitter_seed: u64,
) -> Duration {
    use std::hash::{Hash, Hasher};

    let exp = attempt.saturating_sub(1) as f64;
    let secs =
        config.retry_initial_interval.as_seconds_f64() * config.retry_backoff_coefficient.powf(exp);
    let capped = secs.min(config.retry_max_interval.as_seconds_f64());

    // Deterministic jitter factor in [0.8, 1.0) from a hash of (seed, attempt).
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    jitter_seed.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let frac = (hasher.finish() >> 11) as f64 / ((1u64 << 53) as f64); // [0, 1)
    let jittered = capped * (0.8 + 0.2 * frac);

    // Hard positive floor: a degenerate (non-positive) config must not yield a zero
    // deadline that hot-loops the scanner.
    Duration::seconds_f64(jittered.max(0.001))
}

/// Decide which Nexus timeout, if any, the live operation has breached at `now`.
///
/// Mirrors v1.31.0's three independent timeout tasks
/// (`components/nexusoperations/statemachine.go:144-167 @ v1.31.0`):
/// schedule-to-close is anchored at the scheduled time and applies in any state;
/// schedule-to-start is anchored at the scheduled time but only while the
/// operation has not started; start-to-close is anchored at the started time and
/// only once started. A zero/unset timeout means "no deadline" (v1.31.0 only
/// emits the task when `AsDuration() != 0`), which is the opposite of the
/// activity scanner's zero-means-immediate convention — Nexus has no schedule
/// command default, so an unset timeout must not fire. When more than one
/// deadline has passed, the earliest-anchored applicable one is reported; since
/// the conformance suite sets exactly one timeout per operation, precedence only
/// affects the rare multi-timeout case and never suppresses a real firing.
pub fn evaluate_nexus_timeout(
    op: &PendingNexusOperation,
    now: OffsetDateTime,
) -> Option<NexusTimeoutType> {
    let mut fired: Option<(OffsetDateTime, NexusTimeoutType)> = None;
    let mut consider = |deadline: OffsetDateTime, kind: NexusTimeoutType| {
        if now >= deadline {
            match fired {
                Some((existing, _)) if existing <= deadline => {}
                _ => fired = Some((deadline, kind)),
            }
        }
    };

    if let Some(timeout) = op.schedule_to_close_timeout
        && !timeout.is_zero()
    {
        consider(op.scheduled_at + timeout, NexusTimeoutType::ScheduleToClose);
    }
    if !op.started
        && let Some(timeout) = op.schedule_to_start_timeout
        && !timeout.is_zero()
    {
        consider(op.scheduled_at + timeout, NexusTimeoutType::ScheduleToStart);
    }
    if let Some(started_at) = op.started_at
        && let Some(timeout) = op.start_to_close_timeout
        && !timeout.is_zero()
    {
        consider(started_at + timeout, NexusTimeoutType::StartToClose);
    }

    fired.map(|(_, kind)| kind)
}

/// Evaluate the watched Nexus operations once and resolve any that have timed
/// out, capped at `max_timeouts_per_scan`.
///
/// Each entry's run is reloaded from `repo` because the live `PendingNexusOperation`
/// is the authority for the current timeouts, the started anchor, and whether the
/// operation still exists (mirrors the activity scanner). An entry whose run is
/// absent, or whose operation is no longer pending, is dropped (it resolved by
/// another path); a load error is transient and leaves the entry for the next scan.
pub(crate) async fn scan_nexus_timeouts_once<R>(
    repo: &R,
    tracking: &NexusTimeoutTrackingState,
    shard_id: Option<ShardId>,
    lanes: &[LaneHandle],
    lane_count: usize,
    config: &NexusTimeoutScannerConfig,
) where
    R: RunRepository + 'static,
{
    let now = OffsetDateTime::now_utc();
    let entries = match shard_id {
        Some(shard_id) => tracking.snapshot_for_shard(shard_id),
        None => tracking.snapshot(),
    };
    let mut submitted = 0usize;

    for entry in entries {
        if submitted >= config.max_timeouts_per_scan {
            break;
        }

        let state = match repo.load_run(entry.run_key).await {
            Ok(LoadedRun::Existing(state)) => state,
            Ok(LoadedRun::Absent) => {
                tracking.remove(entry.run_key, &entry.operation_id);
                continue;
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    run_key = ?entry.run_key,
                    operation_id = entry.operation_id,
                    "nexus timeout scanner failed to load run"
                );
                continue;
            }
        };

        let Some(operation) = state.pending_nexus_operations.get(&entry.operation_id) else {
            tracking.remove(entry.run_key, &entry.operation_id);
            continue;
        };

        let Some(timeout_type) = evaluate_nexus_timeout(operation, now) else {
            // Not timed out. If the operation is backing off and its next attempt is
            // due, re-dispatch it. Schedule-to-close (checked just above) dominates: had
            // it fired we would have taken the timeout branch instead (Invariant 3). The
            // kernel fences against a double re-dispatch, so this is fire-and-forget and
            // the entry stays tracked (the op is still pending until a terminal outcome).
            if operation.next_attempt_at.is_some_and(|next| now >= next) {
                runtime_metrics::record_scanner_dispatched(
                    "nexus_retry",
                    shard_id.map(|s| s.0).unwrap_or(0),
                );
                let lane = pick_lane_for_run_key(lanes, lane_count, entry.run_key).clone();
                let _ = lane
                    .submit(
                        entry.run_key,
                        Command::NexusOperationRetry(NexusOperationRetryRequest {
                            operation_id: entry.operation_id.clone(),
                            scheduled_event_id: entry.scheduled_event_id,
                            now,
                        }),
                    )
                    .await;
                submitted += 1;
            }
            if operation
                .cancellation
                .as_ref()
                .and_then(|cancellation| {
                    cancellation
                        .next_attempt_at
                        .map(|next| (cancellation.requested_event_id, next))
                })
                .is_some_and(|(_, next)| now >= next)
                && submitted < config.max_timeouts_per_scan
            {
                let requested_event_id = operation
                    .cancellation
                    .as_ref()
                    .map(|cancellation| cancellation.requested_event_id)
                    .unwrap_or_default();
                runtime_metrics::record_scanner_dispatched(
                    "nexus_cancel_retry",
                    shard_id.map(|s| s.0).unwrap_or(0),
                );
                let lane = pick_lane_for_run_key(lanes, lane_count, entry.run_key).clone();
                let _ = lane
                    .submit(
                        entry.run_key,
                        Command::NexusCancellationRetry(NexusCancellationRetryRequest {
                            operation_id: entry.operation_id.clone(),
                            scheduled_event_id: entry.scheduled_event_id,
                            requested_event_id,
                            now,
                        }),
                    )
                    .await;
                submitted += 1;
            }
            continue;
        };

        runtime_metrics::record_scanner_dispatched(
            "nexus_timeout",
            shard_id.map(|s| s.0).unwrap_or(0),
        );
        let lane = pick_lane_for_run_key(lanes, lane_count, entry.run_key).clone();
        let result = lane
            .submit(
                entry.run_key,
                Command::NexusOperationResolved(NexusOperationResolvedRequest {
                    operation_id: entry.operation_id.clone(),
                    scheduled_event_id: entry.scheduled_event_id,
                    resolution: NexusResolution::TimedOut { timeout_type },
                    now,
                }),
            )
            .await
            .map(|_| ());

        match result {
            Ok(()) => tracking.remove(entry.run_key, &entry.operation_id),
            Err(error) => {
                let message = error.to_string();
                // Kernel rejection means the operation already resolved or advanced
                // past the state this timeout was computed against, so the entry is
                // stale: drop it. Other errors are transient and keep the entry.
                if message.contains("kernel rejected") {
                    tracing::debug!(
                        ?error,
                        run_key = ?entry.run_key,
                        operation_id = entry.operation_id,
                        "nexus timeout scanner timeout rejected by kernel"
                    );
                    tracking.remove(entry.run_key, &entry.operation_id);
                } else {
                    tracing::warn!(
                        ?error,
                        run_key = ?entry.run_key,
                        operation_id = entry.operation_id,
                        "nexus timeout scanner failed to submit timeout"
                    );
                }
            }
        }
        submitted += 1;
    }
}

pub(crate) async fn run_nexus_timeout_scanner<R>(
    repo: Arc<R>,
    tracking: NexusTimeoutTrackingState,
    lanes: Vec<LaneHandle>,
    lane_count: usize,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: NexusTimeoutScannerConfig,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner
            .read()
            .expect("shard_owner lock poisoned")
            .active_shards()
            .collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("nexus_timeout", shard_id.0);
            scan_nexus_timeouts_once(
                repo.as_ref(),
                &tracking,
                Some(shard_id),
                &lanes,
                lane_count,
                &config,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokio::{runtime::Runtime, sync::mpsc};
    use uuid::Uuid;

    use super::*;

    fn versioned_key(
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        deployment: &str,
        build_id: &str,
    ) -> NexusQueueKey {
        NexusQueueKey {
            namespace_id,
            task_queue: task_queue.clone(),
            deployment: Some(DeploymentId(deployment.to_owned())),
            build_id: Some(BuildId(build_id.to_owned())),
        }
    }

    fn cancel_request(operation_id: &str) -> NexusTaskRequest {
        NexusTaskRequest::CancelOperation {
            service: "service".to_owned(),
            operation: "cancel".to_owned(),
            operation_id: operation_id.to_owned(),
            operation_token: "operation-token".to_owned(),
        }
    }

    async fn await_nexus_waiter(broker: &NexusTaskBroker, key: &NexusQueueKey) {
        loop {
            if broker
                .waiters
                .lock()
                .expect("Nexus waiter-count lock poisoned")
                .get(key)
                .copied()
                .unwrap_or(0)
                > 0
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn versioned_nexus_publication_is_isolated_and_observed_after_waiter_lookup() {
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("nexus".to_owned());
        let key = versioned_key(namespace_id, &task_queue, "payments", "build-a");
        let other = versioned_key(namespace_id, &task_queue, "payments", "build-b");
        let broker = NexusTaskBroker::default();
        let (sender, mut receiver) = mpsc::channel(2);
        broker.set_demand_observation_sink(Arc::new(crate::ChannelDemandObservationSink::new(
            sender,
        )));

        let poll = {
            let broker = broker.clone();
            let key = key.clone();
            tokio::spawn(async move {
                broker
                    .poll_versioned(key, tokio::time::Duration::from_secs(5))
                    .await
            })
        };
        await_nexus_waiter(&broker, &key).await;
        broker
            .publish_workflow_versioned(
                key.clone(),
                RunKey::new(),
                "operation".to_owned(),
                7,
                cancel_request("operation"),
            )
            .await;

        assert!(
            broker
                .poll_versioned(other, tokio::time::Duration::ZERO)
                .await
                .is_none()
        );
        let task = poll.await.expect("poll task").expect("versioned task");
        assert_eq!(task.token.namespace_id, namespace_id.0.to_string());
        assert_eq!(task.token.task_queue, task_queue.0);
        assert_eq!(
            receiver.try_recv().expect("Nexus observation"),
            crate::DemandObservation {
                namespace_id,
                task_queue,
                task_type: WorkerComputeTaskType::Nexus,
                deployment_name: DeploymentId("payments".to_owned()),
                build_id: BuildId("build-a".to_owned()),
                match_kind: crate::DemandMatchKind::Sync,
            }
        );
    }

    #[tokio::test]
    async fn denied_nexus_worker_is_woken_and_cannot_take_future_work() {
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("nexus".to_owned());
        let key = versioned_key(namespace_id, &task_queue, "payments", "build-a");
        let denied_worker = WorkerIdentity("worker-a".to_owned());
        let broker = NexusTaskBroker::default();

        let poll = {
            let broker = broker.clone();
            let key = key.clone();
            let worker = denied_worker.clone();
            tokio::spawn(async move {
                broker
                    .poll_versioned_for_worker(key, &worker, tokio::time::Duration::from_secs(60))
                    .await
            })
        };
        await_nexus_waiter(&broker, &key).await;
        broker
            .deny_worker(namespace_id, task_queue.clone(), denied_worker.clone())
            .await;

        let cancelled = tokio::time::timeout(tokio::time::Duration::from_secs(1), poll)
            .await
            .expect("shutdown must wake the parked Nexus poll")
            .expect("poll task");
        assert!(cancelled.is_none());

        broker
            .publish_workflow_versioned(
                key.clone(),
                RunKey::new(),
                "operation".to_owned(),
                7,
                cancel_request("operation"),
            )
            .await;
        assert!(
            broker
                .poll_versioned_for_worker(
                    key.clone(),
                    &denied_worker,
                    tokio::time::Duration::ZERO,
                )
                .await
                .is_none()
        );
        assert!(
            broker
                .poll_versioned_for_worker(
                    key,
                    &WorkerIdentity("worker-b".to_owned()),
                    tokio::time::Duration::ZERO,
                )
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn nexus_zero_rate_blocks_and_live_unset_releases_ready_work() {
        let broker = NexusTaskBroker::default();
        let repository = Arc::new(tokeira_storage::InMemoryStore::default());
        let store = Arc::new(crate::RepositoryBackedTaskQueueConfigStore::new(repository));
        store.hydrate().await.expect("hydrate durable policy");
        broker.set_task_queue_config_store(store.clone());
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("rate-limited".to_string());
        let key = nexus_config_key(&NexusQueueKey::unversioned(
            namespace_id,
            task_queue.clone(),
        ));
        let metadata = crate::TaskQueueConfigMetadata {
            reason: "test".to_string(),
            update_identity: "test".to_string(),
            update_time: OffsetDateTime::UNIX_EPOCH,
        };
        store
            .apply(
                key.clone(),
                crate::TaskQueueConfigPatch {
                    queue_rate_limit: crate::TaskQueueConfigFieldPatch::Set((
                        Some(0.0),
                        metadata.clone(),
                    )),
                    ..crate::TaskQueueConfigPatch::default()
                },
                1_000,
            )
            .await
            .expect("zero is a valid blocking rate");
        broker
            .publish(
                namespace_id,
                task_queue.clone(),
                NexusTask {
                    token: NexusTaskToken {
                        namespace_id: namespace_id.0.to_string(),
                        task_queue: task_queue.0.clone(),
                        task_id: "task".to_string(),
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "service".to_string(),
                        operation: "operation".to_string(),
                        operation_id: "operation-id".to_string(),
                        operation_token: "operation-token".to_string(),
                    },
                    origin: NexusQueueKey::unversioned(namespace_id, task_queue.clone())
                        .worker_task_origin(),
                },
            )
            .await;

        assert!(
            broker
                .poll(
                    namespace_id,
                    task_queue.clone(),
                    tokio::time::Duration::ZERO
                )
                .await
                .is_none()
        );
        store
            .apply(
                key,
                crate::TaskQueueConfigPatch {
                    queue_rate_limit: crate::TaskQueueConfigFieldPatch::Set((None, metadata)),
                    ..crate::TaskQueueConfigPatch::default()
                },
                1_000,
            )
            .await
            .expect("unset is valid");
        assert!(
            broker
                .poll(namespace_id, task_queue, tokio::time::Duration::ZERO)
                .await
                .is_some()
        );
    }

    // Feature: edge-nexus-task-transport, Property 1: Task token round-trip
    proptest! {
        #[test]
        fn property_task_token_roundtrip(
            namespace in any::<u128>(),
            task_queue in "[a-z0-9_-]{1,24}",
            task_id in "[a-z0-9_-]{1,24}",
        ) {
            let token = NexusTaskToken {
                namespace_id: Uuid::from_u128(namespace).to_string(),
                task_queue,
                task_id,
            };
            let encoded = token.encode().expect("token should encode");
            let decoded = NexusTaskToken::decode(&encoded).expect("token should decode");
            prop_assert_eq!(decoded, token);
        }
    }

    proptest! {
        #[test]
        fn property_nexus_version_isolation_preserves_response_identity(
            namespace_seed in any::<u128>(),
            queue_suffix in "[a-z]{1,8}",
            deployment in "[a-z]{1,8}",
            build_id in "[a-z0-9]{1,8}",
            operation_id in "[a-z0-9_-]{1,16}",
            versioned in any::<bool>(),
        ) {
            // Feature: worker-compute-controller, Property 6: Nexus version isolation preserves response identity
            let rt = Runtime::new().expect("runtime");
            rt.block_on(async move {
                let broker = NexusTaskBroker::default();
                let namespace_id = NamespaceId(Uuid::from_u128(namespace_seed));
                let task_queue = TaskQueueName(format!("queue-{queue_suffix}"));
                let run_key = RunKey::new();
                let key = if versioned {
                    versioned_key(namespace_id, &task_queue, &deployment, &build_id)
                } else {
                    NexusQueueKey::unversioned(namespace_id, task_queue.clone())
                };
                let wrong = if versioned {
                    NexusQueueKey::unversioned(namespace_id, task_queue.clone())
                } else {
                    versioned_key(namespace_id, &task_queue, &deployment, &build_id)
                };
                let expected_origin = key.worker_task_origin();

                broker
                    .publish_workflow_versioned(
                        key.clone(),
                        run_key,
                        operation_id.clone(),
                        11,
                        cancel_request(&operation_id),
                    )
                    .await;
                prop_assert!(
                    broker
                        .poll_versioned(wrong, tokio::time::Duration::ZERO)
                        .await
                        .is_none()
                );
                let task = broker
                    .poll_versioned(key, tokio::time::Duration::ZERO)
                    .await
                    .expect("exact key receives task");
                let encoded = task.token.encode().expect("token encodes");
                let decoded = NexusTaskToken::decode(&encoded).expect("token decodes");
                prop_assert_eq!(decoded.namespace_id, namespace_id.0.to_string());
                prop_assert_eq!(decoded.task_queue, task_queue.0);
                prop_assert_eq!(&decoded.task_id, &task.token.task_id);
                prop_assert_eq!(
                    broker.consume(&decoded.task_id).await,
                    Some(NexusTaskCorrelation::Workflow {
                        run_key,
                        operation_id,
                        scheduled_event_id: 11,
                        task_kind: NexusWorkflowTaskKind::CancelOperation,
                        origin: expected_origin,
                    })
                );
                Ok(())
            })?;
        }
    }

    // Feature: edge-nexus-task-transport, Property 3: Correlation single consumption
    proptest! {
        #[test]
        fn property_correlation_single_consumption(
            namespace_seed in any::<u128>(),
            queue_suffix in "[a-z]{1,8}",
            first_operation in "[a-z0-9_-]{1,16}",
            second_operation in "[a-z0-9_-]{1,16}",
        ) {
            let rt = Runtime::new().expect("runtime");
            rt.block_on(async move {
                let broker = NexusTaskBroker::default();
                let namespace_id = NamespaceId(Uuid::from_u128(namespace_seed));
                let task_queue = TaskQueueName(format!("queue-{queue_suffix}"));
                let first_run = RunKey::new();
                let second_run = RunKey::new();
                let expected_origin =
                    NexusQueueKey::unversioned(namespace_id, task_queue.clone())
                        .worker_task_origin();

                broker
                    .publish_workflow(
                        namespace_id,
                        task_queue.clone(),
                        first_run,
                        first_operation.clone(),
                        1,
                        NexusTaskRequest::CancelOperation {
                            service: "svc".to_owned(),
                            operation: "cancel".to_owned(),
                            operation_id: first_operation.clone(),
                            operation_token: "token-a".to_owned(),
                        },
                    )
                    .await;
                broker
                    .publish_workflow(
                        namespace_id,
                        task_queue.clone(),
                        second_run,
                        second_operation.clone(),
                        2,
                        NexusTaskRequest::CancelOperation {
                            service: "svc".to_owned(),
                            operation: "cancel".to_owned(),
                            operation_id: second_operation.clone(),
                            operation_token: "token-b".to_owned(),
                        },
                    )
                    .await;

                let first = broker
                    .poll(namespace_id, task_queue.clone(), tokio::time::Duration::ZERO)
                    .await
                    .expect("first task");
                let second = broker
                    .poll(namespace_id, task_queue, tokio::time::Duration::ZERO)
                    .await
                    .expect("second task");

                prop_assert_eq!(
                    broker.consume(&first.token.task_id).await,
                    Some(NexusTaskCorrelation::Workflow {
                        run_key: first_run,
                        operation_id: first_operation,
                        scheduled_event_id: 1,
                        task_kind: NexusWorkflowTaskKind::CancelOperation,
                        origin: expected_origin.clone(),
                    })
                );
                prop_assert_eq!(broker.consume(&first.token.task_id).await, None);
                prop_assert_eq!(
                    broker.consume(&second.token.task_id).await,
                    Some(NexusTaskCorrelation::Workflow {
                        run_key: second_run,
                        operation_id: second_operation,
                        scheduled_event_id: 2,
                        task_kind: NexusWorkflowTaskKind::CancelOperation,
                        origin: expected_origin,
                    })
                );
                Ok(())
            })?;
        }
    }

    // Feature: edge-nexus-task-transport, Property 6: workflow and caller-facing
    // deliveries retain disjoint private result routes behind identical public tokens.
    proptest! {
        #[test]
        fn property_correlation_route_separation(
            namespace_seed in any::<u128>(),
            queue_suffix in "[a-z]{1,8}",
            operation_id in "[a-z0-9_-]{1,16}",
            waiter_id in "[a-z0-9_-]{1,16}",
        ) {
            let rt = Runtime::new().expect("runtime");
            rt.block_on(async move {
                let broker = NexusTaskBroker::default();
                let namespace_id = NamespaceId(Uuid::from_u128(namespace_seed));
                let task_queue = TaskQueueName(format!("queue-{queue_suffix}"));
                let run_key = RunKey::new();
                let expected_origin =
                    NexusQueueKey::unversioned(namespace_id, task_queue.clone())
                        .worker_task_origin();
                broker
                    .publish_workflow(
                        namespace_id,
                        task_queue.clone(),
                        run_key,
                        operation_id.clone(),
                        7,
                        NexusTaskRequest::CancelOperation {
                            service: "svc".to_owned(),
                            operation: "cancel".to_owned(),
                            operation_id: operation_id.clone(),
                            operation_token: "workflow-token".to_owned(),
                        },
                    )
                    .await;
                let _http_lease = broker
                    .publish_http(
                        namespace_id,
                        task_queue.clone(),
                        waiter_id.clone(),
                        NexusTaskRequest::CancelOperation {
                            service: "svc".to_owned(),
                            operation: "cancel".to_owned(),
                            operation_id: "http-op".to_owned(),
                            operation_token: "http-token".to_owned(),
                        },
                    )
                    .await;

                let workflow = broker
                    .poll(namespace_id, task_queue.clone(), tokio::time::Duration::ZERO)
                    .await
                    .expect("workflow task");
                let http = broker
                    .poll(namespace_id, task_queue, tokio::time::Duration::ZERO)
                    .await
                    .expect("HTTP task");
                prop_assert_eq!(
                    broker.consume(&workflow.token.task_id).await,
                    Some(NexusTaskCorrelation::Workflow {
                        run_key,
                        operation_id,
                        scheduled_event_id: 7,
                        task_kind: NexusWorkflowTaskKind::CancelOperation,
                        origin: expected_origin.clone(),
                    })
                );
                prop_assert_eq!(
                    broker.consume(&http.token.task_id).await,
                    Some(NexusTaskCorrelation::Http {
                        waiter_id,
                        origin: expected_origin,
                    })
                );
                Ok(())
            })?;
        }
    }

    #[test]
    fn canceling_http_delivery_lease_removes_undelivered_task_and_route() {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(async move {
            let broker = NexusTaskBroker::default();
            let namespace_id = NamespaceId(Uuid::new_v4());
            let task_queue = TaskQueueName("queue".to_owned());
            let lease = broker
                .publish_http(
                    namespace_id,
                    task_queue.clone(),
                    "waiter".to_owned(),
                    NexusTaskRequest::CancelOperation {
                        service: "svc".to_owned(),
                        operation: "cancel".to_owned(),
                        operation_id: "op".to_owned(),
                        operation_token: "token".to_owned(),
                    },
                )
                .await;
            let task_id = lease.task_id.clone().expect("task ID");
            lease.cancel().await;

            assert!(
                broker
                    .poll(namespace_id, task_queue, tokio::time::Duration::ZERO)
                    .await
                    .is_none()
            );
            assert_eq!(broker.consume(&task_id).await, None);
        });
    }

    #[test]
    fn nexus_completion_backoff_jitters_deterministically_within_bounds() {
        let config = NexusCompletionRuntimeConfig::default(); // 1s initial, 1h max, 2.0 coeff
        // Deterministic: same (attempt, seed) -> same delay.
        let a = nexus_completion_backoff(&config, 1, 42);
        let b = nexus_completion_backoff(&config, 1, 42);
        assert_eq!(a, b);
        // Jitter keeps the first-attempt delay in [0.8s, 1.0s) of the 1s nominal interval.
        assert!(
            a.as_seconds_f64() >= 0.8 && a.as_seconds_f64() < 1.0,
            "attempt-1 delay {a:?} must be jittered within [0.8s, 1.0s)"
        );
        // Distinct seeds de-correlate (different callbacks back off at different offsets).
        let other = nexus_completion_backoff(&config, 1, 43);
        assert_ne!(a, other, "different seeds should jitter differently");
        // Exponential growth holds across attempts (attempt 3 ~ 4x attempt 1's nominal).
        let third = nexus_completion_backoff(&config, 3, 42);
        assert!(
            third.as_seconds_f64() >= 3.2,
            "attempt-3 delay {third:?} >= 0.8*4s"
        );
    }

    #[test]
    fn nexus_completion_backoff_floors_a_degenerate_config() {
        // A non-positive initial interval must not yield a zero deadline (scanner hot-loop):
        // the hard floor keeps it strictly positive.
        let config = NexusCompletionRuntimeConfig {
            retry_initial_interval: Duration::ZERO,
            retry_max_interval: Duration::ZERO,
            retry_backoff_coefficient: 2.0,
            retry_max_attempts: 0,
            system_callback_url: "http://127.0.0.1:7253".to_string(),
        };
        let delay = nexus_completion_backoff(&config, 1, 7);
        assert!(
            delay > Duration::ZERO,
            "floor must keep the delay positive, got {delay:?}"
        );
    }

    // Feature: edge-nexus-task-transport, Property 2: Broker queue isolation
    proptest! {
        #[test]
        fn property_broker_queue_isolation(
            namespace_seed in any::<u128>(),
            queue_suffix in "[a-z]{1,8}",
            first_operation in "[a-z0-9_-]{1,16}",
            second_operation in "[a-z0-9_-]{1,16}",
            third_operation in "[a-z0-9_-]{1,16}",
        ) {
            let rt = Runtime::new().expect("runtime");
            rt.block_on(async move {
                let broker = NexusTaskBroker::default();
                let namespace_a = NamespaceId(Uuid::from_u128(namespace_seed));
                let namespace_b = NamespaceId(Uuid::from_u128(namespace_seed.wrapping_add(1)));
                let queue_a = TaskQueueName(format!("queue-a-{queue_suffix}"));
                let queue_b = TaskQueueName(format!("queue-b-{queue_suffix}"));

                let first = NexusTask {
                    token: NexusTaskToken {
                        namespace_id: namespace_a.0.to_string(),
                        task_queue: queue_a.0.clone(),
                        task_id: first_operation.clone(),
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: first_operation.clone(),
                        operation_token: String::new(),
                    },
                    origin: NexusQueueKey::unversioned(namespace_a, queue_a.clone())
                        .worker_task_origin(),
                };
                let second = NexusTask {
                    token: NexusTaskToken {
                        namespace_id: namespace_b.0.to_string(),
                        task_queue: queue_b.0.clone(),
                        task_id: second_operation.clone(),
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: second_operation.clone(),
                        operation_token: String::new(),
                    },
                    origin: NexusQueueKey::unversioned(namespace_b, queue_b.clone())
                        .worker_task_origin(),
                };
                let third = NexusTask {
                    token: NexusTaskToken {
                        namespace_id: namespace_a.0.to_string(),
                        task_queue: queue_a.0.clone(),
                        task_id: third_operation.clone(),
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: third_operation.clone(),
                        operation_token: String::new(),
                    },
                    origin: NexusQueueKey::unversioned(namespace_a, queue_a.clone())
                        .worker_task_origin(),
                };

                broker
                    .publish(namespace_a, queue_a.clone(), first.clone())
                    .await;
                broker
                    .publish(namespace_b, queue_b.clone(), second.clone())
                    .await;
                broker
                    .publish(namespace_a, queue_a.clone(), third.clone())
                    .await;

                let polled_second = broker
                    .poll(namespace_b, queue_b.clone(), tokio::time::Duration::from_millis(1))
                    .await
                    .expect("queue b task");
                let polled_first = broker
                    .poll(namespace_a, queue_a.clone(), tokio::time::Duration::from_millis(1))
                    .await
                    .expect("first queue a task");
                let polled_third = broker
                    .poll(namespace_a, queue_a.clone(), tokio::time::Duration::from_millis(1))
                    .await
                    .expect("second queue a task");
                let empty = broker
                    .poll(namespace_a, queue_b, tokio::time::Duration::from_millis(1))
                    .await;

                prop_assert_eq!(polled_second, second);
                prop_assert_eq!(polled_first, first);
                prop_assert_eq!(polled_third, third);
                prop_assert_eq!(empty, None);
                Ok(())
            })?;
        }
    }
}

#[cfg(test)]
mod endpoint_store_tests {
    use proptest::prelude::*;

    use super::{
        InMemoryNexusEndpointStore, NexusEndpointSpec, NexusEndpointSpecTarget, NexusEndpointStore,
        NexusEndpointStoreError,
    };

    fn external_spec(name: &str) -> NexusEndpointSpec {
        NexusEndpointSpec {
            name: name.to_owned(),
            description: Vec::new(),
            target: NexusEndpointSpecTarget::External {
                url: "https://example.test".to_owned(),
            },
        }
    }

    // Feature: api-conformance-nexus-admin, Property 1: CRUD round trip.
    // Create authors id + version 1; get/find return the same fields.
    #[test]
    fn create_then_get_round_trips_with_server_authored_id_and_version() {
        let store = InMemoryNexusEndpointStore::new();
        let created = store.create(external_spec("ep-one"), 100).expect("create");
        assert_eq!(created.version, 1, "create authors version 1");
        assert!(!created.id.is_empty(), "create authors a non-empty id");
        assert_eq!(created.created_time_unix_nanos, 100);
        assert_eq!(
            created.last_modified_time_unix_nanos, 0,
            "unset until modified"
        );

        let fetched = store.get(&created.id).expect("get");
        assert_eq!(fetched, created);
        assert_eq!(store.find_by_name("ep-one"), Some(created));
    }

    // Feature: api-conformance-nexus-admin, Property 1: duplicate name rejected.
    #[test]
    fn duplicate_name_is_rejected() {
        let store = InMemoryNexusEndpointStore::new();
        store.create(external_spec("dup"), 0).expect("first create");
        assert_eq!(
            store.create(external_spec("dup"), 0),
            Err(NexusEndpointStoreError::DuplicateName("dup".to_owned()))
        );
    }

    // Feature: api-conformance-nexus-admin, Property 2: optimistic update safety.
    // A matching version mutates and bumps; a stale version is a FAILED_PRECONDITION
    // mismatch that does not mutate.
    #[test]
    fn update_is_version_fenced() {
        let store = InMemoryNexusEndpointStore::new();
        let created = store.create(external_spec("ep"), 0).expect("create");

        // Stale version → mismatch, no mutation.
        assert_eq!(
            store.update(&created.id, created.version + 9, external_spec("ep"), 1),
            Err(NexusEndpointStoreError::VersionMismatch {
                received: created.version + 9,
                expected: created.version,
            })
        );
        assert_eq!(
            store.get(&created.id).unwrap().version,
            1,
            "no mutation on mismatch"
        );

        // Matching version → mutate, bump to 2, set last-modified.
        let updated = store
            .update(&created.id, created.version, external_spec("ep"), 50)
            .expect("update");
        assert_eq!(updated.version, 2);
        assert_eq!(updated.last_modified_time_unix_nanos, 50);
        assert_eq!(
            updated.created_time_unix_nanos,
            created.created_time_unix_nanos
        );
    }

    // Feature: api-conformance-nexus-admin, Property 2: missing id outcomes.
    #[test]
    fn update_and_delete_missing_id_not_found() {
        let store = InMemoryNexusEndpointStore::new();
        assert_eq!(
            store.update(
                "00000000-0000-0000-0000-000000000000",
                1,
                external_spec("x"),
                0
            ),
            Err(NexusEndpointStoreError::NotFound(
                "00000000-0000-0000-0000-000000000000".to_owned()
            ))
        );
        assert!(matches!(
            store.delete("nope"),
            Err(NexusEndpointStoreError::NotFound(_))
        ));
    }

    // Delete removes by id (no version CAS — v1.31.0 matching deletes by id), and a
    // deleted name can be re-created.
    #[test]
    fn delete_removes_and_frees_name() {
        let store = InMemoryNexusEndpointStore::new();
        let created = store.create(external_spec("gone"), 0).expect("create");
        store.delete(&created.id).expect("delete");
        assert!(matches!(
            store.get(&created.id),
            Err(NexusEndpointStoreError::NotFound(_))
        ));
        // Name is free again.
        store.create(external_spec("gone"), 0).expect("recreate");
    }

    // A page token that no longer names an entry is FAILED_PRECONDITION (not a panic
    // or silent empty), matching the table owner's behaviour.
    #[test]
    fn list_unknown_page_token_is_page_token_not_found() {
        let store = InMemoryNexusEndpointStore::new();
        store.create(external_spec("a"), 0).expect("create");
        assert_eq!(
            store.list(Some("not-an-id"), 10).err(),
            Some(NexusEndpointStoreError::PageTokenNotFound)
        );
    }

    // Feature: api-conformance-nexus-admin, Property 1: pagination stability.
    // Paging by id with page_size covers every endpoint exactly once, in id order,
    // regardless of insertion order or page size.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn prop_pagination_covers_all_in_id_order(
            count in 1usize..20,
            page_size in 1usize..8,
        ) {
            let store = InMemoryNexusEndpointStore::new();
            for i in 0..count {
                store.create(external_spec(&format!("ep-{i}")), 0).expect("create");
            }

            let mut seen_ids: Vec<String> = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let page = store.list(token.as_deref(), page_size).expect("list page");
                prop_assert!(page.entries.len() <= page_size);
                for entry in &page.entries {
                    seen_ids.push(entry.id.clone());
                }
                match page.next_page_token {
                    Some(next) => token = Some(next),
                    None => break,
                }
            }

            prop_assert_eq!(seen_ids.len(), count, "every endpoint paged exactly once");
            let mut sorted = seen_ids.clone();
            sorted.sort();
            prop_assert_eq!(&seen_ids, &sorted, "pages are in id order");
            sorted.dedup();
            prop_assert_eq!(sorted.len(), count, "no duplicates across pages");
        }
    }
}
