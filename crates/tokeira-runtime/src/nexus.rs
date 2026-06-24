//! Nexus operation types, endpoint registry, and timeout scanning.
//!
//! Contains the HTTP client trait for Nexus operations, endpoint configuration
//! and registry, timeout tracking state, and the background scanner that
//! detects timed-out Nexus operations.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    Command, Link, LoadedRun, NexusOperationResolvedRequest, NexusResolution, NexusTimeoutType,
    PendingNexusOperation,
};
use tokeira_storage::RunRepository;
use tokeira_types::{NamespaceId, Payload, Payloads, RunKey, ShardId, TaskQueueName};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    lane::LaneHandle, metrics as runtime_metrics, scanner::pick_lane_for_run_key, shard::ShardOwner,
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

/// In-cluster callback URL sentinel attached to a Worker-target `StartOperation`.
/// Verbatim from `SystemCallbackURL = "temporal://system"`
/// (`common/nexus/constants.go @ v1.31.0`). The runtime resolves it to the
/// configured local HTTP listener (`NexusCompletionConfig.system_callback_url`)
/// when firing — the loopback v1.31.0 performs via `routeSystemCallbackRequest`.
pub const SYSTEM_CALLBACK_URL: &str = "temporal://system";

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
    ///
    /// NOTE (deferred): cancel-request *retry* and the `NexusOperationCancelRequest`
    /// {Failed,Completed} history-event lifecycle are not modelled here yet; this
    /// is a single best-effort attempt (Req 5.3 + the cancel-lifecycle follow-up).
    async fn cancel_operation(
        &self,
        address: &str,
        service: &str,
        operation: &str,
        operation_token: &str,
        trace_headers: &[KeyValue],
    ) -> Result<()>;
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
        let mut state = self.state.lock().unwrap();
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
            .unwrap()
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let state = self.state.lock().unwrap();
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
        let state = self.state.lock().unwrap();
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusTaskToken {
    pub run_key: RunKey,
    pub operation_id: String,
    pub scheduled_event_id: i64,
}

impl NexusTaskToken {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|error| anyhow!("invalid nexus task token: {error}"))
    }
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
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct NexusTask {
    pub token: NexusTaskToken,
    pub request: NexusTaskRequest,
}

#[derive(Default, Clone)]
pub struct NexusTaskBroker {
    inner: Arc<AsyncMutex<NexusBrokerState>>,
}

#[derive(Default)]
struct NexusBrokerState {
    ready: HashMap<(NamespaceId, TaskQueueName), VecDeque<NexusTask>>,
    /// Per-queue wake handles (see `tokeira-runtime::broker`'s per-queue wake
    /// pattern): a publish wakes only pollers on that namespace+task-queue.
    wakes: HashMap<(NamespaceId, TaskQueueName), Arc<Notify>>,
}

impl NexusTaskBroker {
    pub async fn publish(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        task: NexusTask,
    ) {
        let key = (namespace_id, task_queue);
        let mut inner = self.inner.lock().await;
        inner.ready.entry(key.clone()).or_default().push_back(task);
        let wake = inner.wakes.entry(key).or_default().clone();
        drop(inner);
        wake.notify_waiters();
    }

    pub async fn poll(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        wait_for: tokio::time::Duration,
    ) -> Option<NexusTask> {
        if let Some(task) = self.try_take(namespace_id, &task_queue).await {
            return Some(task);
        }

        // Per-queue wake + deadline loop: a publish on another nexus queue must
        // not end this poll, and a wake is a hint to re-check, not a result (we
        // wait until the deadline if there is still nothing for this queue).
        let wake = self.queue_wake(namespace_id, &task_queue).await;
        let deadline = tokio::time::Instant::now() + wait_for;
        loop {
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(task) = self.try_take(namespace_id, &task_queue).await {
                return Some(task);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }

    async fn queue_wake(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Arc<Notify> {
        self.inner
            .lock()
            .await
            .wakes
            .entry((namespace_id, task_queue.clone()))
            .or_default()
            .clone()
    }

    async fn try_take(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Option<NexusTask> {
        let mut inner = self.inner.lock().await;
        inner
            .ready
            .get_mut(&(namespace_id, task_queue.clone()))
            .and_then(VecDeque::pop_front)
    }
}

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
    ) -> Result<()> {
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
/// rebuilt from durable state by [`crate::recovery::sweep_shard`]; losing it only
/// delays firing until the rebuild, never changes the outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NexusTimeoutEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub operation_id: String,
    pub scheduled_event_id: i64,
    pub scheduled_at: OffsetDateTime,
}

#[derive(Clone, Default)]
pub struct NexusTimeoutTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, String), NexusTimeoutEntry>>>,
}

impl NexusTimeoutTrackingState {
    pub fn insert(&self, entry: NexusTimeoutEntry) {
        self.inner
            .lock()
            .unwrap()
            .insert((entry.run_key, entry.operation_id.clone()), entry);
    }

    pub fn remove(&self, run_key: RunKey, operation_id: &str) {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, operation_id.to_string()));
    }

    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.inner
            .lock()
            .unwrap()
            .retain(|(candidate, _), _| *candidate != run_key);
    }

    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .unwrap()
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    pub fn snapshot(&self) -> Vec<NexusTimeoutEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<NexusTimeoutEntry> {
        self.inner
            .lock()
            .unwrap()
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
/// rebuilt from durable state by [`crate::recovery::sweep_shard`]; losing it only
/// delays a retry until the rebuild, never changes the outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionCallbackTrackingEntry {
    pub run_key: RunKey,
    pub shard_id: ShardId,
    pub callback_index: usize,
}

#[derive(Clone, Default)]
pub struct CompletionCallbackTrackingState {
    inner: Arc<Mutex<HashMap<(RunKey, usize), CompletionCallbackTrackingEntry>>>,
}

impl CompletionCallbackTrackingState {
    pub fn insert(&self, entry: CompletionCallbackTrackingEntry) {
        self.inner
            .lock()
            .unwrap()
            .insert((entry.run_key, entry.callback_index), entry);
    }

    pub fn remove(&self, run_key: RunKey, callback_index: usize) {
        self.inner
            .lock()
            .unwrap()
            .remove(&(run_key, callback_index));
    }

    pub fn remove_all_for_run(&self, run_key: RunKey) {
        self.inner
            .lock()
            .unwrap()
            .retain(|(candidate, _), _| *candidate != run_key);
    }

    pub fn remove_all_for_shard(&self, shard_id: ShardId) {
        self.inner
            .lock()
            .unwrap()
            .retain(|_, entry| entry.shard_id != shard_id);
    }

    pub fn snapshot(&self) -> Vec<CompletionCallbackTrackingEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn snapshot_for_shard(&self, shard_id: ShardId) -> Vec<CompletionCallbackTrackingEntry> {
        self.inner
            .lock()
            .unwrap()
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
/// capped at `max_interval`. `attempt` is the 1-based number of the attempt that just
/// failed (so the first failure backs off by `initial`). The kernel performs no backoff
/// math (`command.rs` `CallbackAttemptOutcome` doc); the runtime computes this and passes
/// `next_attempt_at` into `CompletionCallbackAttempted`.
pub fn nexus_completion_backoff(config: &NexusCompletionRuntimeConfig, attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(1) as f64;
    let secs =
        config.retry_initial_interval.as_seconds_f64() * config.retry_backoff_coefficient.powf(exp);
    let capped = secs.min(config.retry_max_interval.as_seconds_f64());
    Duration::seconds_f64(capped.max(0.0))
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

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
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
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    use super::*;

    // Feature: edge-nexus-task-transport, Property 1: Task token round-trip
    proptest! {
        #[test]
        fn property_task_token_roundtrip(
            run in any::<u128>(),
            operation_id in "[a-z0-9_-]{1,24}",
            scheduled_event_id in 0i64..10_000,
        ) {
            let token = NexusTaskToken {
                run_key: RunKey(Uuid::from_u128(run)),
                operation_id,
                scheduled_event_id,
            };
            let encoded = token.encode().expect("token should encode");
            let decoded = NexusTaskToken::decode(&encoded).expect("token should decode");
            prop_assert_eq!(decoded, token);
        }
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
                        run_key: RunKey::new(),
                        operation_id: first_operation.clone(),
                        scheduled_event_id: 1,
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: first_operation.clone(),
                    },
                };
                let second = NexusTask {
                    token: NexusTaskToken {
                        run_key: RunKey::new(),
                        operation_id: second_operation.clone(),
                        scheduled_event_id: 2,
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: second_operation.clone(),
                    },
                };
                let third = NexusTask {
                    token: NexusTaskToken {
                        run_key: RunKey::new(),
                        operation_id: third_operation.clone(),
                        scheduled_event_id: 3,
                    },
                    request: NexusTaskRequest::CancelOperation {
                        service: "svc".to_string(),
                        operation: "cancel".to_string(),
                        operation_id: third_operation.clone(),
                    },
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
