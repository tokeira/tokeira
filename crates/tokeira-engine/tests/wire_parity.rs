//! Wire-parity goldens for the gRPC stack.
//!
//! Every probe here asks the running engine a question whose answer depends on
//! the HTTP and gRPC libraries underneath the edge rather than on workflow
//! semantics: which status a missing method gets, what an oversized message
//! is told, how compression is negotiated, what reflection lists, how gRPC-Web
//! frames a reply. The answers were captured on the stack in use when this file
//! was written and live as fixtures under `tests/fixtures/wire-parity/`; a
//! later stack must reproduce them byte for byte. Running with
//! `WIRE_PARITY_CAPTURE=1` rewrites the fixtures instead of comparing, which is
//! only correct on the stack whose answers are the reference.
//!
//! Three surfaces answer: the in-process endpoint and a host-attached listener
//! of one embedded engine, and the public listener of an in-memory `tokeirad`,
//! which carries the CORS, gRPC-Web, HTTP API, and Nexus HTTP layers the host
//! listener does not.

mod listener_support;

use std::{collections::BTreeMap, fs, net::SocketAddr, path::PathBuf};

use anyhow::{Context as _, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use listener_support::{
    RawCall, RawResponse, STEP, Transport, WORKFLOW_SERVICE, execution, task_queue,
};
use proptest::prelude::*;
use prost::Message;
use serde_json::{Value, json};
use tokeira_engine::{Engine, TokeiradHandle};
use tokeira_proto::{
    common::{Payload, Payloads, WorkflowType},
    enums::WorkflowIdReusePolicy,
    workflowservice::{
        DeprecateNamespaceRequest, DescribeNamespaceRequest, DescribeWorkflowExecutionRequest,
        GetSystemInfoRequest, GetSystemInfoResponse, PollWorkflowTaskQueueRequest,
        QueryWorkflowRequest, RegisterNamespaceRequest, RespondWorkflowTaskCompletedRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, TerminateWorkflowExecutionRequest,
        UpdateWorkflowExecutionRequest,
    },
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_util::task::AbortOnDropHandle;
use tonic::{Code, Status};

const NAMESPACE: &str = "default";
/// The decode limit the edge inherits from the stack's default today and
/// states explicitly after the migration.
const DECODE_LIMIT: usize = 4 * 1024 * 1024;
const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

// ---------------------------------------------------------------------------
// Fixtures: capture or compare
// ---------------------------------------------------------------------------

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wire-parity")
}

fn capturing() -> bool {
    std::env::var_os("WIRE_PARITY_CAPTURE").is_some()
}

/// Write `observed` as the fixture in capture mode; otherwise require the
/// fixture to equal it and print both on mismatch.
fn check_fixture(name: &str, observed: &Value) -> Result<()> {
    let path = fixture_dir().join(format!("{name}.json"));
    let rendered = format!("{}\n", serde_json::to_string_pretty(observed)?);
    if capturing() {
        fs::create_dir_all(fixture_dir())?;
        fs::write(&path, rendered).with_context(|| format!("write {}", path.display()))?;
        return Ok(());
    }
    let expected = fs::read_to_string(&path)
        .with_context(|| format!("read golden {} (capture it first)", path.display()))?;
    let expected: Value = serde_json::from_str(&expected)?;
    ensure!(
        &expected == observed,
        "wire-parity golden `{name}` differs\n--- expected\n{}\n--- observed\n{rendered}",
        serde_json::to_string_pretty(&expected)?
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Surfaces
// ---------------------------------------------------------------------------

struct Surfaces {
    engine: Engine,
    listener: tokeira_engine::EngineListener,
    tokeirad: TokeiradHandle,
    /// Label, transport, and whether the surface is a network socket.
    targets: Vec<(&'static str, Transport)>,
}

impl Surfaces {
    async fn start() -> Result<Self> {
        let engine = Engine::start().await?;
        let listener = engine.listen("127.0.0.1:0".parse()?).await?;
        let tokeirad = TokeiradHandle::start_in_memory("127.0.0.1:0".parse()?).await?;
        let targets = vec![
            ("in-process", Transport::InProcess(engine.endpoint())),
            (
                "host-listener",
                Transport::network(listener.bound_addr()).await?,
            ),
            (
                "public-listener",
                Transport::network(tokeirad.bound_addr()).await?,
            ),
        ];
        Ok(Self {
            engine,
            listener,
            tokeirad,
            targets,
        })
    }

    fn public_addr(&self) -> SocketAddr {
        self.tokeirad.bound_addr()
    }

    fn network_targets(&self) -> impl Iterator<Item = &(&'static str, Transport)> {
        self.targets
            .iter()
            .filter(|(_, transport)| matches!(transport, Transport::Network(_)))
    }

    async fn shutdown(self) -> Result<()> {
        self.listener.shutdown().await?;
        self.engine.shutdown().await?;
        self.tokeirad.shutdown().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Status catalogue
// ---------------------------------------------------------------------------

/// Header keys that vary per response or carry the status itself.
const VOLATILE_HEADERS: [&str; 6] = [
    "date",
    "content-type",
    "content-length",
    "grpc-status",
    "grpc-message",
    "grpc-status-details-bin",
];

fn stable_headers(headers: BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .into_iter()
        .filter(|(name, _)| !VOLATILE_HEADERS.contains(&name.as_str()))
        .collect()
}

fn status_headers(status: &Status) -> BTreeMap<String, String> {
    stable_headers(
        status
            .metadata()
            .clone()
            .into_headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
    )
}

/// Replace every run id the scenario created with the zero uuid, in text and
/// in encoded detail bytes (a uuid string keeps its length, so the protobuf
/// framing around it stays valid).
fn normalize(text: &str, run_ids: &[String]) -> String {
    run_ids.iter().fold(text.to_owned(), |acc, run_id| {
        acc.replace(run_id.as_str(), ZERO_UUID)
    })
}

fn normalize_bytes(bytes: &[u8], run_ids: &[String]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for run_id in run_ids {
        let needle = run_id.as_bytes();
        let mut index = 0;
        while index + needle.len() <= out.len() {
            if &out[index..index + needle.len()] == needle {
                out[index..index + needle.len()].copy_from_slice(ZERO_UUID.as_bytes());
                index += needle.len();
            } else {
                index += 1;
            }
        }
    }
    out
}

fn status_entry(status: &Status, run_ids: &[String]) -> Value {
    json!({
        "code": status.code() as i32,
        "message": normalize(status.message(), run_ids),
        "details_base64": STANDARD.encode(normalize_bytes(status.details(), run_ids)),
        "metadata": status_headers(status),
    })
}

fn ok_entry(response: &RawResponse) -> Value {
    json!({
        "code": 0,
        "message": "",
        "details_base64": "",
        "metadata": stable_headers(response.headers.clone()),
    })
}

fn raw(body: Vec<u8>) -> RawCall {
    RawCall {
        body: Bytes::from(body),
        ..Default::default()
    }
}

fn raw_with_headers(body: Vec<u8>, headers: &[(&str, &str)]) -> RawCall {
    RawCall {
        body: Bytes::from(body),
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        ..Default::default()
    }
}

/// A fixed list of calls whose answers come from the edge's error mapping or
/// from the stack itself. Every entry is deterministic once run ids are
/// normalized.
fn catalogue_calls(
    duplicate_start: &StartWorkflowExecutionRequest,
) -> Vec<(&'static str, &'static str, RawCall)> {
    vec![
        (
            "describe_missing_execution",
            "DescribeWorkflowExecution",
            raw(DescribeWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                execution: Some(execution("wire-parity-missing", "")),
            }
            .encode_to_vec()),
        ),
        (
            "describe_missing_namespace",
            "DescribeNamespace",
            raw(DescribeNamespaceRequest {
                namespace: "wire-parity-no-such-namespace".to_owned(),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "start_missing_workflow_type",
            "StartWorkflowExecution",
            raw(StartWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_id: "wire-parity-no-type".to_owned(),
                task_queue: Some(task_queue("wire-parity")),
                request_id: "wire-parity-no-type".to_owned(),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "start_duplicate_rejected",
            "StartWorkflowExecution",
            raw(StartWorkflowExecutionRequest {
                request_id: "wire-parity-duplicate-2".to_owned(),
                ..duplicate_start.clone()
            }
            .encode_to_vec()),
        ),
        (
            "signal_missing_execution",
            "SignalWorkflowExecution",
            raw(SignalWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_execution: Some(execution("wire-parity-missing", "")),
                signal_name: "bump".to_owned(),
                request_id: "wire-parity-signal".to_owned(),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "query_missing_execution",
            "QueryWorkflow",
            raw(QueryWorkflowRequest {
                namespace: NAMESPACE.to_owned(),
                execution: Some(execution("wire-parity-missing", "")),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "terminate_missing_execution",
            "TerminateWorkflowExecution",
            raw(TerminateWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_execution: Some(execution("wire-parity-missing", "")),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "update_missing_execution",
            "UpdateWorkflowExecution",
            raw(UpdateWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_execution: Some(execution("wire-parity-missing", "")),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "respond_workflow_task_bad_token",
            "RespondWorkflowTaskCompleted",
            raw(RespondWorkflowTaskCompletedRequest {
                task_token: b"not-a-token".to_vec(),
                namespace: NAMESPACE.to_owned(),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "poll_empty_namespace",
            "PollWorkflowTaskQueue",
            raw(PollWorkflowTaskQueueRequest {
                task_queue: Some(task_queue("wire-parity")),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "register_namespace_duplicate",
            "RegisterNamespace",
            raw(RegisterNamespaceRequest {
                namespace: NAMESPACE.to_owned(),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "deprecate_namespace_unimplemented",
            "DeprecateNamespace",
            raw(DeprecateNamespaceRequest {
                namespace: NAMESPACE.to_owned(),
                ..Default::default()
            }
            .encode_to_vec()),
        ),
        (
            "unknown_method",
            "NoSuchMethod",
            raw(GetSystemInfoRequest::default().encode_to_vec()),
        ),
        (
            "malformed_request_body",
            "GetSystemInfo",
            raw(vec![0xff, 0xff, 0xff, 0xff]),
        ),
        (
            "unsupported_request_encoding",
            "GetSystemInfo",
            raw_with_headers(
                GetSystemInfoRequest::default().encode_to_vec(),
                &[("grpc-encoding", "br")],
            ),
        ),
    ]
}

fn duplicate_start_request() -> StartWorkflowExecutionRequest {
    StartWorkflowExecutionRequest {
        namespace: NAMESPACE.to_owned(),
        workflow_id: "wire-parity-duplicate".to_owned(),
        workflow_type: Some(WorkflowType {
            name: "wire-parity".to_owned(),
        }),
        task_queue: Some(task_queue("wire-parity")),
        request_id: "wire-parity-duplicate-1".to_owned(),
        workflow_id_reuse_policy: WorkflowIdReusePolicy::RejectDuplicate as i32,
        ..Default::default()
    }
}

/// Seed the running workflow the duplicate-start case collides with. One seed
/// per engine: the in-process endpoint and the host listener share an engine,
/// so the second surface reuses the first surface's run id.
async fn seed_duplicate(transport: &Transport) -> Result<Vec<String>> {
    let seeded: StartWorkflowExecutionResponse = transport
        .unary("StartWorkflowExecution", duplicate_start_request(), &[])
        .await
        .context("seed the duplicate start")?;
    Ok(vec![seeded.run_id])
}

/// Run the catalogue on one surface; `run_ids` are the seed's ids to normalize.
async fn run_catalogue(
    transport: &Transport,
    run_ids: &[String],
) -> Result<BTreeMap<String, Value>> {
    let mut entries = BTreeMap::new();
    for (case, rpc, call) in catalogue_calls(&duplicate_start_request()) {
        // The tonic client owns `grpc-encoding` and replaces whatever metadata
        // names it, so only the in-process bridge can deliver the raw header.
        if case == "unsupported_request_encoding" && matches!(transport, Transport::Network(_)) {
            continue;
        }
        let entry = match tokio::time::timeout(STEP, transport.unary_raw(rpc, call))
            .await
            .with_context(|| format!("{case} did not return"))?
        {
            Ok(response) => ok_entry(&response),
            Err(status) => status_entry(&status, run_ids),
        };
        entries.insert(case.to_owned(), entry);
    }
    Ok(entries)
}

/// The parts of an entry every surface must agree on. Metadata is compared
/// per surface against the golden instead: the public listener's CORS layer
/// adds headers the host listener never sends.
fn decision(entry: &Value) -> Value {
    json!({
        "code": entry["code"],
        "message": entry["message"],
        "details_base64": entry["details_base64"],
    })
}

// Feature: tonic-0-14-grpc-stack, Property 2: status parity across transports
#[tokio::test]
async fn status_catalogue_matches_the_golden_on_every_surface() -> Result<()> {
    let surfaces = Surfaces::start().await?;
    let mut per_surface = BTreeMap::new();
    let mut shared_engine_run_ids = None;
    for (label, transport) in &surfaces.targets {
        let run_ids = match *label {
            "public-listener" => seed_duplicate(transport).await?,
            _ => match &shared_engine_run_ids {
                Some(run_ids) => Vec::clone(run_ids),
                None => {
                    let run_ids = seed_duplicate(transport).await?;
                    shared_engine_run_ids = Some(run_ids.clone());
                    run_ids
                }
            },
        };
        let entries = run_catalogue(transport, &run_ids).await?;
        per_surface.insert((*label).to_owned(), entries);
    }

    // Every surface decides every case identically before the golden is consulted.
    let reference = per_surface
        .get("in-process")
        .context("in-process surface ran")?;
    for (label, entries) in &per_surface {
        for (case, entry) in entries {
            let expected = reference.get(case).map(decision);
            ensure!(
                expected.as_ref() == Some(&decision(entry)),
                "{label}: case {case} differs from the in-process decision\n{}\nvs\n{}",
                serde_json::to_string_pretty(entry)?,
                serde_json::to_string_pretty(reference.get(case).unwrap_or(&Value::Null))?
            );
        }
    }

    let mut cases: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for (label, entries) in &per_surface {
        for (case, entry) in entries {
            cases
                .entry(case.clone())
                .or_default()
                .insert(label.clone(), entry.clone());
        }
    }
    check_fixture("status-catalogue", &json!({ "cases": cases }))?;
    surfaces.shutdown().await
}

// ---------------------------------------------------------------------------
// Decode-limit boundary
// ---------------------------------------------------------------------------

/// A start request whose encoded length is exactly `target` bytes: the payload
/// is grown until the varint-prefixed encoding lands on the target.
fn start_of_exact_size(workflow_id: &str, target: usize) -> Result<Vec<u8>> {
    let mut data_len = target.saturating_sub(256);
    for _ in 0..8 {
        let request = StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: workflow_id.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "wire-parity".to_owned(),
            }),
            task_queue: Some(task_queue("wire-parity-size")),
            request_id: format!("{workflow_id}-request"),
            input: Some(Payloads {
                payloads: vec![Payload {
                    metadata: BTreeMap::from([("encoding".to_owned(), b"binary/plain".to_vec())]),
                    data: vec![b'x'; data_len],
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let encoded = request.encode_to_vec();
        if encoded.len() == target {
            return Ok(encoded);
        }
        data_len = (data_len + target).saturating_sub(encoded.len());
    }
    bail!("could not size a start request to exactly {target} bytes")
}

// Feature: tonic-0-14-grpc-stack, Property 5: decode-limit boundary
#[tokio::test]
async fn decode_limit_boundaries_match_the_golden() -> Result<()> {
    let surfaces = Surfaces::start().await?;
    let sizes = [DECODE_LIMIT - 1, DECODE_LIMIT, DECODE_LIMIT + 1];
    let mut per_size = BTreeMap::new();
    for size in sizes {
        let mut per_surface = BTreeMap::new();
        for (label, transport) in &surfaces.targets {
            let body = start_of_exact_size(&format!("wire-parity-size-{size}-{label}"), size)?;
            let entry = match tokio::time::timeout(
                STEP,
                transport.unary_raw("StartWorkflowExecution", raw(body)),
            )
            .await
            .with_context(|| format!("{label}: {size}-byte start did not return"))?
            {
                Ok(_) => json!({ "code": 0, "message": "" }),
                Err(status) => json!({ "code": status.code() as i32, "message": status.message() }),
            };
            per_surface.insert((*label).to_owned(), entry);
        }
        let reference = per_surface["in-process"].clone();
        for (label, entry) in &per_surface {
            ensure!(
                entry == &reference,
                "{label}: {size}-byte start differs: {entry}"
            );
        }
        per_size.insert(size.to_string(), reference);
    }
    ensure!(
        per_size[&(DECODE_LIMIT + 1).to_string()]["code"] != json!(0),
        "the over-limit start must be refused"
    );
    ensure!(
        per_size[&DECODE_LIMIT.to_string()]["code"] == json!(0),
        "a start at the limit must be accepted"
    );
    check_fixture(
        "decode-limit",
        &json!({ "limit": DECODE_LIMIT, "sizes": per_size }),
    )?;
    surfaces.shutdown().await
}

// ---------------------------------------------------------------------------
// Compression negotiation (network surfaces)
// ---------------------------------------------------------------------------

// Feature: tonic-0-14-grpc-stack, Property 6: compression negotiation
#[tokio::test]
async fn compression_matrix_matches_the_golden() -> Result<()> {
    let surfaces = Surfaces::start().await?;
    let mut per_surface = BTreeMap::new();
    for (label, transport) in surfaces.network_targets() {
        let mut cases = BTreeMap::new();
        for (send_gzip, accept_gzip) in [(false, false), (true, false), (false, true), (true, true)]
        {
            let call = RawCall {
                body: Bytes::from(GetSystemInfoRequest::default().encode_to_vec()),
                send_gzip,
                accept_gzip,
                ..Default::default()
            };
            let case = format!(
                "send_{}_accept_{}",
                if send_gzip { "gzip" } else { "identity" },
                if accept_gzip { "gzip" } else { "none" }
            );
            let entry = match tokio::time::timeout(STEP, transport.unary_raw("GetSystemInfo", call))
                .await
                .with_context(|| format!("{label}: {case} did not return"))?
            {
                Ok(response) => json!({
                    "code": 0,
                    "message": "",
                    "response_encoding": response.headers.get("grpc-encoding"),
                    "accept_encoding": response.headers.get("grpc-accept-encoding"),
                }),
                Err(status) => json!({
                    "code": status.code() as i32,
                    "message": status.message(),
                    "response_encoding": status_headers(&status).get("grpc-encoding"),
                    "accept_encoding": status_headers(&status).get("grpc-accept-encoding"),
                }),
            };
            cases.insert(case, entry);
        }
        per_surface.insert((*label).to_owned(), cases);
    }
    let reference = per_surface["host-listener"].clone();
    ensure!(
        per_surface["public-listener"] == reference,
        "the public listener negotiates compression differently from the host listener"
    );
    check_fixture("compression", &json!({ "cases": reference }))?;
    surfaces.shutdown().await
}

// ---------------------------------------------------------------------------
// Reflection inventory (network surfaces)
// ---------------------------------------------------------------------------

/// One probe per reflection protocol; the generated client types differ only
/// by module, so the body is written once.
macro_rules! reflection_probe {
    ($name:ident, $version:ident) => {
        async fn $name(addr: SocketAddr) -> Result<Value> {
            use tonic_reflection::pb::$version::{
                ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
                server_reflection_request::MessageRequest,
                server_reflection_response::MessageResponse,
            };

            let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))?
                .connect()
                .await?;
            let mut client = ServerReflectionClient::new(channel);

            let list = ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            };
            let mut responses = client
                .server_reflection_info(tokio_stream::once(list))
                .await?
                .into_inner();
            let response = tokio::time::timeout(STEP, responses.message())
                .await
                .context("reflection did not list services")??
                .context("reflection stream ended")?;
            let Some(MessageResponse::ListServicesResponse(listed)) = response.message_response
            else {
                bail!("reflection must answer ListServices with a service list");
            };
            let mut services: Vec<String> = listed
                .service
                .into_iter()
                .map(|service| service.name)
                .collect();
            services.sort();

            let symbol = ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::FileContainingSymbol(
                    WORKFLOW_SERVICE.to_owned(),
                )),
            };
            let mut responses = client
                .server_reflection_info(tokio_stream::once(symbol))
                .await?
                .into_inner();
            let response = tokio::time::timeout(STEP, responses.message())
                .await
                .context("reflection did not resolve the symbol")??
                .context("reflection stream ended")?;
            let Some(MessageResponse::FileDescriptorResponse(files)) = response.message_response
            else {
                bail!("reflection must answer FileContainingSymbol with descriptors");
            };
            let mut names: Vec<String> = files
                .file_descriptor_proto
                .iter()
                .map(|bytes| prost_types::FileDescriptorProto::decode(bytes.as_slice()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|file| file.name.unwrap_or_default())
                .collect();
            names.sort();

            Ok(json!({ "services": services, "workflow_service_files": names }))
        }
    };
}

reflection_probe!(reflection_inventory_v1alpha, v1alpha);
reflection_probe!(reflection_inventory_v1, v1);

// Feature: tonic-0-14-grpc-stack, Property 7: reflection inventory
#[tokio::test]
async fn reflection_listing_matches_the_golden() -> Result<()> {
    let surfaces = Surfaces::start().await?;
    let host_addr = surfaces.listener.bound_addr();
    let public_addr = surfaces.public_addr();

    // `v1alpha` is the protocol the golden was captured with; it must answer
    // exactly as before on both listeners.
    let host_v1alpha = reflection_inventory_v1alpha(host_addr).await?;
    let public_v1alpha = reflection_inventory_v1alpha(public_addr).await?;
    ensure!(
        host_v1alpha == public_v1alpha,
        "v1alpha reflection differs between the host and public listeners"
    );

    // `v1` is served next to it and lists the same services, with its own
    // name in place of the v1alpha service's, over the same descriptors.
    let host_v1 = reflection_inventory_v1(host_addr).await?;
    let public_v1 = reflection_inventory_v1(public_addr).await?;
    ensure!(
        host_v1 == public_v1,
        "v1 reflection differs between the host and public listeners"
    );
    let mut expected_v1 = host_v1alpha.clone();
    if let Some(Value::Array(services)) = expected_v1.get_mut("services") {
        for service in services.iter_mut() {
            if service == "grpc.reflection.v1alpha.ServerReflection" {
                *service = json!("grpc.reflection.v1.ServerReflection");
            }
        }
        services.sort_by_key(|service| service.to_string());
    }
    ensure!(
        host_v1 == expected_v1,
        "v1 reflection must mirror v1alpha:\n{}\nvs\n{}",
        serde_json::to_string_pretty(&host_v1)?,
        serde_json::to_string_pretty(&expected_v1)?
    );

    check_fixture("reflection", &json!({ "v1alpha": host_v1alpha }))?;
    surfaces.shutdown().await
}

// ---------------------------------------------------------------------------
// gRPC-Web framing (public listener)
// ---------------------------------------------------------------------------

struct HttpReply {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

/// One HTTP/1.1 POST written by hand, so the probe sees exactly what a
/// browser client would: status line, headers, and the framed body.
async fn http1_post(
    addr: SocketAddr,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> Result<HttpReply> {
    let request = http::Request::post(path)
        .header("host", addr.to_string())
        .header("content-type", content_type)
        .header("x-grpc-web", "1")
        .header("connection", "close")
        .body(Full::new(Bytes::copy_from_slice(body)))?;
    let stream = tokio::time::timeout(STEP, tokio::net::TcpStream::connect(addr))
        .await
        .context("gRPC-Web connection did not complete")??;
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await?;
    let driver = AbortOnDropHandle::new(tokio::spawn(connection));
    // HTTP framing defines completion. An early reply can close a socket with
    // unread request bytes and reset it after sending a complete response; an
    // EOF-based probe turns that teardown race into a false golden failure.
    // Hyper still rejects a body truncated before its declared boundary.
    let reply = tokio::time::timeout(STEP, async {
        let response = sender
            .send_request(request)
            .await
            .context("HTTP/1 request failed")?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let body = response
            .into_body()
            .collect()
            .await
            .context("incomplete HTTP/1 response")?
            .to_bytes()
            .to_vec();
        Ok::<_, anyhow::Error>(HttpReply {
            status,
            headers,
            body,
        })
    })
    .await
    .context("gRPC-Web reply did not complete");
    driver.abort();
    if let Err(error) = driver.await
        && !error.is_cancelled()
    {
        return Err(error.into());
    }
    reply?
}

#[tokio::test]
async fn http1_probe_completes_at_the_message_boundary() -> Result<()> {
    for (response, complete) in [
        ("HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\npong", true),
        (
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n4\r\npong\r\n0\r\n\r\n",
            true,
        ),
        ("HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\npo", false),
        (
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n4\r\npo",
            false,
        ),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await?);
            }
            stream.write_all(response.as_bytes()).await?;
            if !complete {
                drop(stream);
            }
            // A complete HTTP message must not wait for the peer's TCP teardown.
            let _ = released.await;
            Ok::<_, anyhow::Error>(())
        });
        let reply = http1_post(addr, "/probe", "application/grpc-web+proto", &[]).await;
        let _ = release.send(());
        tokio::time::timeout(STEP, server).await???;
        if complete {
            let reply = reply?;
            assert_eq!(reply.status, 200);
            assert_eq!(reply.body, b"pong");
        } else {
            assert!(reply.is_err(), "truncated response was accepted");
        }
    }
    Ok(())
}

fn grpc_web_frame(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(flag);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Split a gRPC-Web body into (data payload, trailer text).
fn split_grpc_web(body: &[u8]) -> Result<(Vec<u8>, String)> {
    let mut data = Vec::new();
    let mut trailers = String::new();
    let mut rest = body;
    while rest.len() >= 5 {
        let flag = rest[0];
        let len = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
        ensure!(rest.len() >= 5 + len, "truncated gRPC-Web frame");
        let payload = &rest[5..5 + len];
        if flag & 0x80 == 0x80 {
            trailers.push_str(&String::from_utf8_lossy(payload));
        } else {
            data.extend_from_slice(payload);
        }
        rest = &rest[5 + len..];
    }
    ensure!(
        rest.is_empty(),
        "trailing bytes after the last gRPC-Web frame"
    );
    Ok((data, trailers))
}

// Feature: tonic-0-14-grpc-stack, Property 11: gRPC-Web parity
#[tokio::test]
async fn grpc_web_framing_matches_the_golden() -> Result<()> {
    let surfaces = Surfaces::start().await?;
    let addr = surfaces.public_addr();
    let public = Transport::network(addr).await?;

    // A unary success: the framed payload equals the native response.
    let request = GetSystemInfoRequest::default().encode_to_vec();
    let native: GetSystemInfoResponse = public
        .unary("GetSystemInfo", GetSystemInfoRequest::default(), &[])
        .await?;
    let reply = http1_post(
        addr,
        &format!("/{WORKFLOW_SERVICE}/GetSystemInfo"),
        "application/grpc-web+proto",
        &grpc_web_frame(0, &request),
    )
    .await?;
    let (data, trailers) = split_grpc_web(&reply.body)?;
    let decoded = GetSystemInfoResponse::decode(data.as_slice())?;
    ensure!(
        decoded == native,
        "gRPC-Web payload differs from the native response"
    );
    let success = json!({
        "http_status": reply.status,
        "content_type": reply.headers.get("content-type"),
        "trailers": trailers,
    });

    // A missing method: a trailers-only answer carried in the HTTP headers.
    let reply = http1_post(
        addr,
        &format!("/{WORKFLOW_SERVICE}/NoSuchMethod"),
        "application/grpc-web+proto",
        &grpc_web_frame(0, &request),
    )
    .await?;
    let (data, trailers) = split_grpc_web(&reply.body)?;
    let unknown = json!({
        "http_status": reply.status,
        "content_type": reply.headers.get("content-type"),
        "grpc_status_header": reply.headers.get("grpc-status"),
        "grpc_message_header": reply.headers.get("grpc-message"),
        "data_bytes": data.len(),
        "trailers": trailers,
    });

    check_fixture(
        "grpc-web",
        &json!({ "get_system_info": success, "unknown_method": unknown }),
    )?;
    surfaces.shutdown().await
}

// ---------------------------------------------------------------------------
// Property 2 at the type level: a status survives its own HTTP encoding
// ---------------------------------------------------------------------------

fn arb_metadata_pair() -> impl Strategy<Value = (String, String)> {
    ("[a-z][a-z0-9-]{0,12}", "[ -~]{0,24}").prop_filter("reserved keys", |(name, _)| {
        !name.starts_with("grpc-") && name != "content-type" && name != "te"
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    // Feature: tonic-0-14-grpc-stack, Property 2: status parity across transports
    #[test]
    fn status_round_trips_through_its_http_form(
        code in 1i32..=16,
        // A literal percent sign is left out: the stack in use when this golden
        // was captured does not escape it on the way out, so a message such as
        // `%aa` comes back as an UNKNOWN decoding error. That quirk is not a
        // behaviour the edge relies on and is not protected here.
        message in "[ -$&-~]{0,48}",
        details in prop::collection::vec(any::<u8>(), 0..64),
        pairs in prop::collection::vec(arb_metadata_pair(), 0..4),
    ) {
        let mut metadata = tonic::metadata::MetadataMap::new();
        for (name, value) in &pairs {
            if let (Ok(key), Ok(value)) = (
                tonic::metadata::MetadataKey::from_bytes(name.as_bytes()),
                tonic::metadata::MetadataValue::try_from(value.as_str()),
            ) {
                metadata.insert(key, value);
            }
        }
        let status = Status::with_details_and_metadata(
            Code::from_i32(code),
            message.clone(),
            Bytes::from(details.clone()),
            metadata,
        );
        let response = status.into_http::<tonic::body::Body>();
        let decoded = Status::from_header_map(response.headers()).expect("a status is present");
        prop_assert_eq!(decoded.code(), Code::from_i32(code));
        prop_assert_eq!(decoded.message(), message.as_str());
        prop_assert_eq!(decoded.details(), details.as_slice());
        for (name, _) in &pairs {
            if let Ok(key) = tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(name.as_bytes()) {
                prop_assert_eq!(
                    decoded.metadata().get(key.as_str()).map(|value| value.as_bytes().to_vec()),
                    status_metadata_value(&pairs, name)
                );
            }
        }
    }
}

fn status_metadata_value(pairs: &[(String, String)], name: &str) -> Option<Vec<u8>> {
    pairs
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == name)
        .and_then(|(_, value)| tonic::metadata::MetadataValue::try_from(value.as_str()).ok())
        .map(|value| value.as_bytes().to_vec())
}
