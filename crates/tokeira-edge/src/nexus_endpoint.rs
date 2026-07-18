//! The Nexus endpoint admin: the OperatorService `*NexusEndpoint(s)` CRUD/list
//! operations, ground-truthed to Temporal `v1.31.0` (`AGENTS §8`).
//!
//! This is the thin admission/translation seam over the neutral
//! [`NexusEndpointStore`] (the table
//! owner). It validates requests (the verbatim v1.31.0 issues), translates the
//! public Nexus API spec to the stored form and back, and maps the table owner's
//! outcomes to the exact gRPC codes. It owns no durable state and implements no
//! dispatch — runtime dispatch reads the same store via the registry.
//!
//! ## Internal-topology deviation (observable contract preserved)
//!
//! v1.31.0 splits this work: the frontend validates and forwards create/update/
//! delete to the matching service that owns the `nexus_endpoints` table, while
//! get/list read straight from persistence for read-after-write consistency
//! (`service/frontend/nexus_endpoint_client.go:30-34 @ v1.31.0`). Tokeira collapses
//! the table owner into one `NexusEndpointStore`; the error codes, messages,
//! server-authored id/version semantics, and read-after-write behaviour still match
//! v1.31.0 (the deviation is internal only).
//!
//! ## Authorization
//!
//! Endpoints are **global** resources, so the admin itself carries no namespace;
//! the calling [`OperatorService`](crate::operator_service::OperatorService) gates
//! every operation through the operator interceptor (`OperatorWrite` for create/
//! update/delete, `OperatorRead` for get/list) before delegating here — the same
//! authz path as the other OperatorService RPCs.

use std::sync::Arc;

use async_trait::async_trait;
use prost::Message as _;
use tokeira_proto::{common, operatorservice, public::temporal::api::nexus::v1 as nexus_v1};
use tokeira_runtime::nexus::{
    NexusEndpointRecord, NexusEndpointSpec, NexusEndpointSpecTarget, NexusEndpointStore,
    NexusEndpointStoreError,
};

use crate::{
    errors::{EdgeError, EdgeResult},
    namespace_cache::NamespaceCache,
    translate::to_internal::namespace_id_for,
};

/// `^[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9]$` — the endpoint-name regex
/// (`EndpointNameRegex @ v1.31.0`). Matched by hand (no `regex` dep): first char
/// ASCII-alpha, last char ASCII-alphanumeric, interior ASCII-alphanumeric or `-`,
/// minimum length 2.
fn endpoint_name_matches_regex(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() < 2 {
        return false;
    }
    let is_alpha = |b: u8| b.is_ascii_alphabetic();
    let is_alnum = |b: u8| b.is_ascii_alphanumeric();
    if !is_alpha(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    bytes[1..bytes.len() - 1]
        .iter()
        .all(|&b| is_alnum(b) || b == b'-')
}

/// The six Nexus endpoint admin limits (the runtime values; the config surface that
/// carries the v1.31.0-faithful defaults lives in `tokeira-config`, mapped in at the
/// bootstrap so the edge stays config-agnostic — the same pattern as the activity
/// `max_id_length`). Defaults here mirror `common/dynamicconfig/constants.go @
/// v1.31.0` so a directly-constructed admin (tests) is still faithful.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NexusEndpointLimits {
    pub name_max_length: usize,
    pub external_url_max_length: usize,
    pub description_max_size: usize,
    pub task_queue_max_length: usize,
    pub list_default_page_size: usize,
    pub list_max_page_size: usize,
}

impl Default for NexusEndpointLimits {
    fn default() -> Self {
        Self {
            name_max_length: 200,
            external_url_max_length: 4 * 1024,
            description_max_size: 20_000,
            task_queue_max_length: 1000,
            list_default_page_size: 100,
            list_max_page_size: 1000,
        }
    }
}

/// Resolves a namespace name for the Worker-target check. `namespace_id` returns the
/// namespace's id when it exists, or `None` when it does not (the
/// `FAILED_PRECONDITION` "could not verify namespace…" branch). A trait so the admin
/// is testable without a full namespace cache.
#[async_trait]
pub trait NexusNamespaceResolver: Send + Sync {
    /// The id for `name`, or `None` if the namespace does not exist.
    async fn namespace_id(&self, name: &str) -> Option<String>;
}

/// [`NexusNamespaceResolver`] backed by the edge namespace cache: a namespace exists
/// iff the cache has it, and its id is the edge's canonical `namespace_id_for(name)`
/// (the deterministic id the rest of the edge routes on, so dispatch resolves the
/// same namespace).
pub struct CacheNamespaceResolver {
    cache: Arc<dyn NamespaceCache>,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for CacheNamespaceResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheNamespaceResolver")
            .finish_non_exhaustive()
    }
}

impl CacheNamespaceResolver {
    pub fn new(cache: Arc<dyn NamespaceCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl NexusNamespaceResolver for CacheNamespaceResolver {
    async fn namespace_id(&self, name: &str) -> Option<String> {
        match self.cache.get(name).await {
            Ok(Some(_)) => Some(namespace_id_for(name).0.to_string()),
            _ => None,
        }
    }
}

/// The Nexus endpoint admin over a [`NexusEndpointStore`].
pub struct NexusEndpointAdmin {
    store: Arc<dyn NexusEndpointStore>,
    namespaces: Arc<dyn NexusNamespaceResolver>,
    limits: NexusEndpointLimits,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for NexusEndpointAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusEndpointAdmin").finish_non_exhaustive()
    }
}

impl NexusEndpointAdmin {
    pub fn new(
        store: Arc<dyn NexusEndpointStore>,
        namespaces: Arc<dyn NexusNamespaceResolver>,
        limits: NexusEndpointLimits,
    ) -> Self {
        Self {
            store,
            namespaces,
            limits,
        }
    }

    /// Create an endpoint (`Create @ v1.31.0`): validate the spec, translate, then
    /// the store authors the id/version and returns the persisted entry.
    pub async fn create(
        &self,
        req: operatorservice::CreateNexusEndpointRequest,
    ) -> EdgeResult<operatorservice::CreateNexusEndpointResponse> {
        let spec = self.validate_and_translate_spec(req.spec).await?;
        let record = self
            .store
            .create(spec, now_unix_nanos())
            .map_err(|e| map_store_error(e, StoreOp::Create))?;
        Ok(operatorservice::CreateNexusEndpointResponse {
            endpoint: Some(self.record_to_proto(record)),
        })
    }

    /// Read an endpoint by id (`Get @ v1.31.0`, read-after-write).
    pub async fn get(
        &self,
        req: operatorservice::GetNexusEndpointRequest,
    ) -> EdgeResult<operatorservice::GetNexusEndpointResponse> {
        validate_endpoint_id(&req.id)?;
        let record = self
            .store
            .get(&req.id)
            .map_err(|e| map_store_error(e, StoreOp::Get))?;
        Ok(operatorservice::GetNexusEndpointResponse {
            endpoint: Some(self.record_to_proto(record)),
        })
    }

    /// Update an endpoint (`Update @ v1.31.0`): validate the spec, then the store
    /// applies the mutation iff the supplied version matches.
    pub async fn update(
        &self,
        req: operatorservice::UpdateNexusEndpointRequest,
    ) -> EdgeResult<operatorservice::UpdateNexusEndpointResponse> {
        let spec = self.validate_and_translate_spec(req.spec).await?;
        let record = self
            .store
            .update(&req.id, req.version, spec, now_unix_nanos())
            .map_err(|e| map_store_error(e, StoreOp::Update))?;
        Ok(operatorservice::UpdateNexusEndpointResponse {
            endpoint: Some(self.record_to_proto(record)),
        })
    }

    /// Delete an endpoint (`Delete @ v1.31.0`): validate id + `version > 0`, then
    /// delete by id. The version is **not** CAS-checked — v1.31.0's matching delete
    /// removes by id without comparing the version (`service/matching/
    /// nexus_endpoint_client.go:200-220 @ v1.31.0`); the `> 0` guard is the only
    /// version rule.
    pub async fn delete(
        &self,
        req: operatorservice::DeleteNexusEndpointRequest,
    ) -> EdgeResult<operatorservice::DeleteNexusEndpointResponse> {
        validate_delete_request(&req)?;
        self.store
            .delete(&req.id)
            .map_err(|e| map_store_error(e, StoreOp::Delete))?;
        Ok(operatorservice::DeleteNexusEndpointResponse {})
    }

    /// List endpoints (`List @ v1.31.0`). A non-empty `name` is the list-and-filter
    /// path: it ignores `page_size`/`next_page_token` and returns the single match
    /// or an empty list. Otherwise it paginates by id with the page-size bounds.
    pub async fn list(
        &self,
        req: operatorservice::ListNexusEndpointsRequest,
    ) -> EdgeResult<operatorservice::ListNexusEndpointsResponse> {
        if !req.name.is_empty() {
            let endpoints = self
                .store
                .find_by_name(&req.name)
                .map(|record| vec![self.record_to_proto(record)])
                .unwrap_or_default();
            return Ok(operatorservice::ListNexusEndpointsResponse {
                next_page_token: Vec::new(),
                endpoints,
            });
        }

        // page_size == 0 is "unset" and defaults; only a set page size is validated.
        let page_size = if req.page_size == 0 {
            self.limits.list_default_page_size
        } else {
            self.validate_page_size(req.page_size)?
        };
        // The next-page token is the last-returned endpoint id, as raw bytes
        // (`[]byte(entry.Id)`, `service/matching/nexus_endpoint_client.go:289 @ v1.31.0`).
        let start_after = if req.next_page_token.is_empty() {
            None
        } else {
            Some(String::from_utf8(req.next_page_token.clone()).map_err(|_| {
                EdgeError::FailedPrecondition(
                    "could not find endpoint indicated by nexus list endpoints next page token"
                        .to_owned(),
                )
            })?)
        };
        let page = self
            .store
            .list(start_after.as_deref(), page_size)
            .map_err(|e| map_store_error(e, StoreOp::List))?;
        Ok(operatorservice::ListNexusEndpointsResponse {
            next_page_token: page
                .next_page_token
                .map(String::into_bytes)
                .unwrap_or_default(),
            endpoints: page
                .entries
                .into_iter()
                .map(|record| self.record_to_proto(record))
                .collect(),
        })
    }

    /// `validateUpsertSpec @ v1.31.0`: accumulate name/target/description issues into
    /// a single `INVALID_ARGUMENT` (not fail-fast), then translate the spec to the
    /// stored form. The one exception is the Worker namespace-existence check, which
    /// returns `FAILED_PRECONDITION` immediately rather than accumulating.
    async fn validate_and_translate_spec(
        &self,
        spec: Option<nexus_v1::EndpointSpec>,
    ) -> EdgeResult<NexusEndpointSpec> {
        let spec = spec.unwrap_or_default();
        let mut issues: Vec<String> = Vec::new();

        // Name (`getEndpointNameIssues`): empty short-circuits the rest of the name
        // checks (matching the Go `return issues` after the empty append).
        let name = spec.name.clone();
        if name.is_empty() {
            issues.push("endpoint name not set".to_owned());
        } else {
            if name.len() > self.limits.name_max_length {
                issues.push(format!(
                    "endpoint name exceeds length limit of {}",
                    self.limits.name_max_length
                ));
            }
            if !endpoint_name_matches_regex(&name) {
                // %q-quoted regex literal, matching Go's
                // `Appendf("...regex: %q", EndpointNameRegex.String())`.
                issues.push(
                    r#"endpoint name must match the regex: "^[a-zA-Z][a-zA-Z0-9\\-]*[a-zA-Z0-9]$""#
                        .to_owned(),
                );
            }
        }

        // Target variant. An unset variant short-circuits (returns the issues so far),
        // mirroring the `return issues.GetError()` after appending "empty target
        // variant".
        let target = match spec.target.as_ref().and_then(|t| t.variant.as_ref()) {
            None => {
                issues.push("empty target variant".to_owned());
                return Err(invalid_argument(issues));
            }
            Some(variant) => variant,
        };

        let translated_target = match target {
            nexus_v1::endpoint_target::Variant::Worker(worker) => {
                if worker.namespace.is_empty() {
                    issues.push("target namespace not set".to_owned());
                    // No id to resolve; leave a placeholder, the issues abort below.
                    NexusEndpointSpecTarget::Worker {
                        namespace_name: String::new(),
                        namespace_id: String::new(),
                        task_queue: worker.task_queue.clone(),
                    }
                } else {
                    match self.namespaces.namespace_id(&worker.namespace).await {
                        // Namespace-not-found is returned IMMEDIATELY as
                        // FAILED_PRECONDITION, not accumulated (`validateUpsertSpec @
                        // v1.31.0`).
                        None => {
                            return Err(EdgeError::FailedPrecondition(format!(
                                "could not verify namespace referenced by target exists: namespace {} not found",
                                worker.namespace
                            )));
                        }
                        Some(namespace_id) => NexusEndpointSpecTarget::Worker {
                            namespace_name: worker.namespace.clone(),
                            namespace_id,
                            task_queue: worker.task_queue.clone(),
                        },
                    }
                }
            }
            nexus_v1::endpoint_target::Variant::External(external) => {
                NexusEndpointSpecTarget::External {
                    url: external.url.clone(),
                }
            }
        };

        // Task queue validation accumulates (Worker only).
        if let NexusEndpointSpecTarget::Worker { task_queue, .. } = &translated_target
            && let Err(message) = validate_task_queue(task_queue, self.limits.task_queue_max_length)
        {
            issues.push(format!("invalid target task queue: {message:?}"));
        }
        // External URL validation accumulates.
        if let NexusEndpointSpecTarget::External { url } = &translated_target
            && let Err(issue) = validate_external_url(url, self.limits.external_url_max_length)
        {
            issues.push(issue);
        }

        // Description: opaque encoded Payload bytes; the size limit is the proto
        // `Size()` of the Payload (`spec.GetDescription().Size() @ v1.31.0`).
        let description = spec
            .description
            .as_ref()
            .map(|payload| payload.encode_to_vec())
            .unwrap_or_default();
        if description.len() > self.limits.description_max_size {
            issues.push(format!(
                "description size exceeds limit of {}",
                self.limits.description_max_size
            ));
        }

        if !issues.is_empty() {
            return Err(invalid_argument(issues));
        }

        Ok(NexusEndpointSpec {
            name,
            description,
            target: translated_target,
        })
    }

    /// `validatePageSize @ v1.31.0`: a negative page size is rejected, and one over
    /// the max is rejected; otherwise the (positive) size is used as-is.
    fn validate_page_size(&self, page_size: i32) -> EdgeResult<usize> {
        if page_size < 0 {
            return Err(EdgeError::BadRequest("page_size is negative".to_owned()));
        }
        if page_size as usize > self.limits.list_max_page_size {
            return Err(EdgeError::BadRequest(format!(
                "page_size exceeds limit of {}",
                self.limits.list_max_page_size
            )));
        }
        Ok(page_size as usize)
    }

    /// Translate a stored record to the external `Endpoint`
    /// (`endpointPersistedEntryToExternalAPI @ v1.31.0`): the Worker target echoes
    /// the stored namespace *name*; `last_modified_time` is set only when the
    /// endpoint has been modified (`version > 1`); `url_prefix` is the dispatch route
    /// for the id.
    fn record_to_proto(&self, record: NexusEndpointRecord) -> nexus_v1::Endpoint {
        let target = match record.spec.target {
            NexusEndpointSpecTarget::External { url } => nexus_v1::EndpointTarget {
                variant: Some(nexus_v1::endpoint_target::Variant::External(
                    nexus_v1::endpoint_target::External { url },
                )),
            },
            NexusEndpointSpecTarget::Worker {
                namespace_name,
                task_queue,
                ..
            } => nexus_v1::EndpointTarget {
                variant: Some(nexus_v1::endpoint_target::Variant::Worker(
                    nexus_v1::endpoint_target::Worker {
                        namespace: namespace_name,
                        task_queue,
                    },
                )),
            },
        };
        let description = (!record.spec.description.is_empty())
            .then(|| common::Payload::decode(record.spec.description.as_slice()).ok())
            .flatten();
        let spec = nexus_v1::EndpointSpec {
            name: record.spec.name,
            description,
            target: Some(target),
        };
        nexus_v1::Endpoint {
            version: record.version,
            id: record.id.clone(),
            spec: Some(spec),
            created_time: unix_nanos_to_timestamp(record.created_time_unix_nanos),
            // Only set once the endpoint has actually been modified (`version > 1`).
            last_modified_time: (record.version > 1)
                .then(|| unix_nanos_to_timestamp(record.last_modified_time_unix_nanos))
                .flatten(),
            url_prefix: format!("/nexus/endpoints/{}/services", record.id),
        }
    }
}

/// Which operation a store error came from — selects the verbatim "not found"
/// message, which differs between update and delete (`service/matching/
/// nexus_endpoint_client.go:152,218 @ v1.31.0`).
#[derive(Clone, Copy)]
enum StoreOp {
    Create,
    Get,
    Update,
    Delete,
    List,
}

fn map_store_error(error: NexusEndpointStoreError, op: StoreOp) -> EdgeError {
    match error {
        NexusEndpointStoreError::DuplicateName(name) => EdgeError::AlreadyExists(format!(
            "error creating Nexus endpoint. Endpoint with name {name} already registered"
        )),
        NexusEndpointStoreError::NotFound(id) => {
            let message = match op {
                StoreOp::Update => {
                    format!("error updating Nexus endpoint. endpoint ID {id} not found")
                }
                StoreOp::Delete => format!("error deleting nexus endpoint with ID: {id}"),
                // Get/Create/List not-found surfaces a generic lookup message; the
                // suite exercises not-found on update/delete.
                _ => format!("error looking up Nexus endpoint with ID `{id}`"),
            };
            EdgeError::NotFound(message)
        }
        // Version mismatch is FAILED_PRECONDITION — never ABORTED (`:155-156 @ v1.31.0`).
        NexusEndpointStoreError::VersionMismatch { received, expected } => {
            EdgeError::FailedPrecondition(format!(
                "nexus endpoint version mismatch. received: {received} expected {expected}"
            ))
        }
        NexusEndpointStoreError::PageTokenNotFound => EdgeError::FailedPrecondition(
            "could not find endpoint indicated by nexus list endpoints next page token".to_owned(),
        ),
    }
}

/// `getEndpointIDIssues @ v1.31.0`: id non-empty and a parseable UUID.
fn validate_endpoint_id(id: &str) -> EdgeResult<()> {
    let mut issues: Vec<String> = Vec::new();
    if id.is_empty() {
        issues.push("endpoint ID not set".to_owned());
    } else if let Err(err) = uuid::Uuid::parse_str(id) {
        issues.push(format!("malformed endpoint ID: {err:?}"));
    }
    if issues.is_empty() {
        Ok(())
    } else {
        Err(invalid_argument(issues))
    }
}

/// `validateDeleteRequest @ v1.31.0`: id issues plus `version > 0`.
fn validate_delete_request(req: &operatorservice::DeleteNexusEndpointRequest) -> EdgeResult<()> {
    let mut issues: Vec<String> = Vec::new();
    if req.id.is_empty() {
        issues.push("endpoint ID not set".to_owned());
    } else if let Err(err) = uuid::Uuid::parse_str(&req.id) {
        issues.push(format!("malformed endpoint ID: {err:?}"));
    }
    if req.version <= 0 {
        issues.push("endpoint version is non-positive".to_owned());
    }
    if issues.is_empty() {
        Ok(())
    } else {
        Err(invalid_argument(issues))
    }
}

/// Mirror of `common/tqid.Validate(taskQueue, maxLength) @ v1.31.0`: non-empty,
/// length-bounded, and not starting with the reserved `/_sys/` prefix. Returns the
/// inner message Go produces (the endpoint client wraps it as
/// `invalid target task queue: %q`).
fn validate_task_queue(task_queue: &str, max_length: usize) -> Result<(), String> {
    if task_queue.is_empty() {
        return Err("taskQueue is not set".to_owned());
    }
    if task_queue.len() > max_length {
        return Err("taskQueue length exceeds limit".to_owned());
    }
    if task_queue.starts_with("/_sys/") {
        return Err("task queue name cannot start with reserved prefix /_sys/".to_owned());
    }
    Ok(())
}

/// Mirror of the External-target URL checks in `validateUpsertSpec @ v1.31.0`:
/// empty → "empty target URL"; over-length → limit; unparseable → "invalid target
/// URL: parse …"; scheme ∉ {http, https} → scheme error. Parsing uses the real
/// `url` crate (WHATWG) rather than a bespoke parser: the dispatch HTTP client
/// re-parses this same string, so validating with a spec-compliant parser keeps
/// admission and use in agreement (no parser differential / SSRF-via-scheme-confusion
/// bypass). The error-message envelope mimics Go's `url.Error` shape so the corpus's
/// "invalid target URL: parse" prefix matches, but the *decision* is the parser's.
fn validate_external_url(url: &str, max_length: usize) -> Result<(), String> {
    if url.is_empty() {
        return Err("empty target URL".to_owned());
    }
    if url.len() > max_length {
        return Err(format!("target URL length exceeds limit of {max_length}"));
    }
    match url::Url::parse(url) {
        Err(err) => Err(format!("invalid target URL: parse {url:?}: {err}")),
        Ok(parsed) => {
            let scheme = parsed.scheme();
            if scheme != "http" && scheme != "https" {
                Err(format!(
                    "invalid target URL scheme: {scheme:?}, expected http or https"
                ))
            } else {
                Ok(())
            }
        }
    }
}

/// Join accumulated validation issues into one `INVALID_ARGUMENT`
/// (`RequestIssues.GetError() @ v1.31.0` joins with ", ").
fn invalid_argument(issues: Vec<String>) -> EdgeError {
    EdgeError::BadRequest(issues.join(", "))
}

fn now_unix_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn unix_nanos_to_timestamp(nanos: i64) -> Option<prost_types::Timestamp> {
    (nanos > 0).then_some(prost_types::Timestamp {
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

#[cfg(test)]
mod tests {
    use tokeira_runtime::nexus::InMemoryNexusEndpointStore;

    use super::*;

    /// A resolver that knows a fixed set of namespace names.
    struct FakeResolver {
        existing: Vec<String>,
    }

    #[async_trait]
    impl NexusNamespaceResolver for FakeResolver {
        async fn namespace_id(&self, name: &str) -> Option<String> {
            self.existing
                .iter()
                .any(|n| n == name)
                .then(|| format!("ns-id-{name}"))
        }
    }

    fn admin(existing_namespaces: &[&str]) -> NexusEndpointAdmin {
        NexusEndpointAdmin::new(
            Arc::new(InMemoryNexusEndpointStore::new()),
            Arc::new(FakeResolver {
                existing: existing_namespaces.iter().map(|s| s.to_string()).collect(),
            }),
            NexusEndpointLimits::default(),
        )
    }

    fn external_spec(name: &str, url: &str) -> nexus_v1::EndpointSpec {
        nexus_v1::EndpointSpec {
            name: name.to_owned(),
            description: None,
            target: Some(nexus_v1::EndpointTarget {
                variant: Some(nexus_v1::endpoint_target::Variant::External(
                    nexus_v1::endpoint_target::External {
                        url: url.to_owned(),
                    },
                )),
            }),
        }
    }

    fn worker_spec(name: &str, namespace: &str, task_queue: &str) -> nexus_v1::EndpointSpec {
        nexus_v1::EndpointSpec {
            name: name.to_owned(),
            description: None,
            target: Some(nexus_v1::EndpointTarget {
                variant: Some(nexus_v1::endpoint_target::Variant::Worker(
                    nexus_v1::endpoint_target::Worker {
                        namespace: namespace.to_owned(),
                        task_queue: task_queue.to_owned(),
                    },
                )),
            }),
        }
    }

    async fn create(
        admin: &NexusEndpointAdmin,
        spec: nexus_v1::EndpointSpec,
    ) -> EdgeResult<nexus_v1::Endpoint> {
        admin
            .create(operatorservice::CreateNexusEndpointRequest { spec: Some(spec) })
            .await
            .map(|r| r.endpoint.expect("endpoint"))
    }

    // Feature: api-conformance-nexus-admin, Property 1: CRUD round trip.
    #[tokio::test]
    async fn create_round_trips_with_authored_id_version_and_url_prefix() {
        let admin = admin(&[]);
        let endpoint = create(&admin, external_spec("my-endpoint", "https://h"))
            .await
            .expect("create");
        assert_eq!(endpoint.version, 1);
        assert!(!endpoint.id.is_empty());
        assert_eq!(
            endpoint.url_prefix,
            format!("/nexus/endpoints/{}/services", endpoint.id)
        );
        assert!(
            endpoint.last_modified_time.is_none(),
            "unmodified: no last_modified"
        );
        let spec = endpoint.spec.expect("spec");
        assert_eq!(spec.name, "my-endpoint");

        // Get returns the same record (read-after-write).
        let got = admin
            .get(operatorservice::GetNexusEndpointRequest {
                id: endpoint.id.clone(),
            })
            .await
            .expect("get")
            .endpoint
            .expect("endpoint");
        assert_eq!(got.id, endpoint.id);
        assert_eq!(got.version, 1);
    }

    // Worker target round-trips the namespace NAME (not the resolved id).
    #[tokio::test]
    async fn worker_target_echoes_namespace_name() {
        let admin = admin(&["payments"]);
        let endpoint = create(&admin, worker_spec("wep", "payments", "tq"))
            .await
            .expect("create");
        let variant = endpoint
            .spec
            .unwrap()
            .target
            .unwrap()
            .variant
            .expect("variant");
        match variant {
            nexus_v1::endpoint_target::Variant::Worker(w) => {
                assert_eq!(w.namespace, "payments");
                assert_eq!(w.task_queue, "tq");
            }
            other => panic!("expected worker target, got {other:?}"),
        }
    }

    fn expect_bad_request(err: EdgeError) -> String {
        match err {
            EdgeError::BadRequest(message) => message,
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    // Feature: api-conformance-nexus-admin, Property 3: validation code/message fidelity.
    #[tokio::test]
    async fn empty_name_is_invalid_argument_with_verbatim_message() {
        let admin = admin(&[]);
        let err = create(&admin, external_spec("", "https://h"))
            .await
            .unwrap_err();
        assert_eq!(expect_bad_request(err), "endpoint name not set");
    }

    #[tokio::test]
    async fn bad_name_regex_is_rejected() {
        let admin = admin(&[]);
        // Leading digit violates the regex.
        let err = create(&admin, external_spec("1bad", "https://h"))
            .await
            .unwrap_err();
        let message = expect_bad_request(err);
        assert!(
            message.contains("endpoint name must match the regex:"),
            "got: {message}"
        );
    }

    #[tokio::test]
    async fn unset_target_variant_is_rejected() {
        let admin = admin(&[]);
        let spec = nexus_v1::EndpointSpec {
            name: "ok-name".to_owned(),
            description: None,
            target: Some(nexus_v1::EndpointTarget { variant: None }),
        };
        let err = create(&admin, spec).await.unwrap_err();
        assert_eq!(expect_bad_request(err), "empty target variant");
    }

    #[tokio::test]
    async fn worker_namespace_not_found_is_failed_precondition() {
        let admin = admin(&[]); // "missing" does not exist
        let err = create(&admin, worker_spec("wep", "missing", "tq"))
            .await
            .unwrap_err();
        match err {
            EdgeError::FailedPrecondition(message) => assert!(
                message.contains("could not verify namespace referenced by target exists"),
                "got: {message}"
            ),
            other => panic!("expected FailedPrecondition, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn external_url_scheme_and_empty_and_parse_errors() {
        let admin = admin(&[]);
        // Empty URL.
        let err = create(&admin, external_spec("ep1", "")).await.unwrap_err();
        assert_eq!(expect_bad_request(err), "empty target URL");
        // Non-http scheme.
        let err = create(&admin, external_spec("ep2", "smtp://foo"))
            .await
            .unwrap_err();
        assert!(expect_bad_request(err).contains("invalid target URL scheme:"));
        // Unparseable (matches the corpus's "invalid target URL: parse" prefix).
        let err = create(&admin, external_spec("ep3", "-http://foo"))
            .await
            .unwrap_err();
        assert!(expect_bad_request(err).contains("invalid target URL: parse"));
    }

    // Feature: api-conformance-nexus-admin, Property 3: id validators.
    #[tokio::test]
    async fn get_malformed_id_is_invalid_argument() {
        let admin = admin(&[]);
        let err = admin
            .get(operatorservice::GetNexusEndpointRequest {
                id: "not-a-uuid".to_owned(),
            })
            .await
            .unwrap_err();
        assert!(expect_bad_request(err).contains("malformed endpoint ID:"));
    }

    #[tokio::test]
    async fn delete_non_positive_version_is_invalid_argument() {
        let admin = admin(&[]);
        let created = create(&admin, external_spec("ep", "https://h"))
            .await
            .unwrap();
        let err = admin
            .delete(operatorservice::DeleteNexusEndpointRequest {
                id: created.id,
                version: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(expect_bad_request(err), "endpoint version is non-positive");
    }

    // Feature: api-conformance-nexus-admin, Property 2: optimistic update safety.
    #[tokio::test]
    async fn update_version_mismatch_is_failed_precondition() {
        let admin = admin(&[]);
        let created = create(&admin, external_spec("ep", "https://h"))
            .await
            .unwrap();
        let err = admin
            .update(operatorservice::UpdateNexusEndpointRequest {
                id: created.id,
                version: created.version + 5,
                spec: Some(external_spec("ep", "https://h2")),
            })
            .await
            .unwrap_err();
        match err {
            EdgeError::FailedPrecondition(message) => {
                assert_eq!(
                    message,
                    format!(
                        "nexus endpoint version mismatch. received: {} expected {}",
                        created.version + 5,
                        created.version
                    )
                );
            }
            other => panic!("expected FailedPrecondition, got {other:?}"),
        }
    }

    // Duplicate name on create → AlreadyExists with the verbatim message.
    #[tokio::test]
    async fn duplicate_name_is_already_exists() {
        let admin = admin(&[]);
        create(&admin, external_spec("dup", "https://h"))
            .await
            .unwrap();
        let err = create(&admin, external_spec("dup", "https://h2"))
            .await
            .unwrap_err();
        match err {
            EdgeError::AlreadyExists(message) => assert_eq!(
                message,
                "error creating Nexus endpoint. Endpoint with name dup already registered"
            ),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    // List by name ignores paging and returns the single match (or empty).
    #[tokio::test]
    async fn list_by_name_returns_single_match() {
        let admin = admin(&[]);
        create(&admin, external_spec("found", "https://h"))
            .await
            .unwrap();
        let resp = admin
            .list(operatorservice::ListNexusEndpointsRequest {
                page_size: 0,
                next_page_token: Vec::new(),
                name: "found".to_owned(),
            })
            .await
            .expect("list");
        assert_eq!(resp.endpoints.len(), 1);
        assert_eq!(resp.endpoints[0].spec.as_ref().unwrap().name, "found");

        let empty = admin
            .list(operatorservice::ListNexusEndpointsRequest {
                page_size: 0,
                next_page_token: Vec::new(),
                name: "absent".to_owned(),
            })
            .await
            .expect("list");
        assert!(empty.endpoints.is_empty());
    }

    #[tokio::test]
    async fn list_page_size_over_max_is_invalid_argument() {
        let admin = admin(&[]);
        let err = admin
            .list(operatorservice::ListNexusEndpointsRequest {
                page_size: NexusEndpointLimits::default().list_max_page_size as i32 + 1,
                next_page_token: Vec::new(),
                name: String::new(),
            })
            .await
            .unwrap_err();
        assert!(expect_bad_request(err).contains("page_size exceeds limit of"));
    }
}
