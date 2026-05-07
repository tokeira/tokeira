//! Local `tokeirad` bootstrap.
//!
//! This binary wires the dev in-memory store, runtime, projection workers, and
//! edge services into one process. It is intentionally explicit so developers
//! can see which pieces are authoritative, which are transport-only, and where
//! background tasks such as projection and history notification are attached.

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tonic_web::GrpcWebLayer;
use tower_http::cors::CorsLayer;
use tracing::info;

mod correlation_format;
mod observability;

use tokeira_config::{Cli, TokeiraConfig};
use tokeira_edge::{
    CacheBackedRouter, EdgeInterceptors, EdgeRoutingConfig, HistoryNotifyingRepository,
    HistoryWaitRegistry, InMemoryNamespaceCache, InMemoryOperatorApi, LocalOnlyRouter,
    LongPollConfig, LongPollGate, NamespaceCache, OperatorService, PendingQueryStore,
    PollerRegistry, ResolvedNamespace, RoutingCache, WorkflowExecutionDescription, WorkflowService,
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    run_routing_subscription,
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_projection::{
    InMemoryVisibilityStore, ProjectionWorker, VisibilityQueryService, VisibilitySink,
};
use tokeira_runtime::{
    ConnectionBudgetApplier, EndpointTarget, MembershipConfig, NexusEndpointConfig,
    NexusEndpointRegistry, NoopNexusHttpClient, RuntimeConfig, ScheduleEngineConfig, ScheduleStore,
    TokeiraRuntime, VersioningRuleStore, run_schedule_engine,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, IncarnationId, NamespaceId, NodeEndpoint, PlacementConfig, ProjectionCursor,
    WorkflowId,
};

#[allow(dead_code)]
#[derive(Clone, Debug)]
enum BootstrapNexusEndpointTarget {
    External {
        address: String,
    },
    Worker {
        namespace_name: String,
        task_queue: String,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct BootstrapNexusEndpointConfig {
    target: BootstrapNexusEndpointTarget,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let (effective_config, config_source) = TokeiraConfig::resolve(cli.config.as_deref())?;
    if cli.dump_config {
        println!("{}", effective_config.to_toml()?);
        return Ok(());
    }
    let observability = observability::ObservabilityConfig::from_tokeira_config(&effective_config)?;
    let metrics_handle = observability::install_metrics(&observability)?;
    let log_reload = observability::install_tracing(&observability)?;
    for warning in effective_config.emergency_warnings() {
        tracing::warn!("{warning}");
    }
    let effective_config = Arc::new(effective_config);
    let _observability_server = observability::spawn_observability_server(
        &observability,
        effective_config.clone(),
        metrics_handle,
        log_reload,
    );

    let addr: std::net::SocketAddr = effective_config
        .infrastructure
        .network
        .grpc_addr
        .parse()
        .with_context(|| {
            format!(
                "invalid infrastructure.network.grpc_addr value: {}",
                effective_config.infrastructure.network.grpc_addr
            )
        })?;
    info!(config_source, "loaded tokeirad configuration");

    // Build the authoritative dev store first, then wrap it with the
    // history-notifying repository used by edge long-poll.
    let node_id = IncarnationId::new();
    let node_endpoint = configured_node_endpoint(&effective_config, addr);
    let store = InMemoryStore::default();
    let history_waits = HistoryWaitRegistry::default();
    let repo = Arc::new(HistoryNotifyingRepository::new(
        Arc::new(store.clone()),
        history_waits.clone(),
    ));
    let default_namespace = ResolvedNamespace::active("default");
    let default_namespace_id = namespace_id_for("default");

    // Bootstrap edge-facing namespace/operator state.
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces.insert(default_namespace).await?;
    let nexus_registry = build_nexus_endpoint_registry(
        namespaces.as_ref(),
        HashMap::<String, BootstrapNexusEndpointConfig>::new(),
    )
    .await?;

    // The runtime owns execution orchestration, scanners, brokers, and all
    // run-local in-memory coordination such as buffered consistent queries.
    let versioning_rule_store = Arc::new(VersioningRuleStore::default());
    let schedule_store = Arc::new(ScheduleStore::default());
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        RuntimeConfig::default().lane_count,
        RuntimeConfig::default().lane,
        RuntimeConfig::default().timer_scanner,
        RuntimeConfig::default().workflow_timeout_scanner,
        RuntimeConfig::default().backlog,
        RuntimeConfig::default().activity_timeout_scanner,
        RuntimeConfig::default().nexus_timeout_scanner,
        nexus_registry,
        Arc::new(NoopNexusHttpClient),
        effective_config.infrastructure.placement.shard_count,
        node_id.to_string(),
        node_endpoint.as_authority(),
        true,
        versioning_rule_store.clone(),
    ));
    let membership_cancel = CancellationToken::new();
    let _membership_client = effective_config
        .infrastructure
        .placement
        .controller_endpoint
        .clone()
        .map(|controller_endpoint| {
            runtime.spawn_membership_client(
                membership_config(
                    &effective_config,
                    node_id,
                    node_endpoint.clone(),
                    controller_endpoint,
                ),
                Arc::new(NoopConnectionBudgetApplier),
                membership_cancel.clone(),
            )
        });
    let schedule_engine_cancel = CancellationToken::new();
    let _schedule_engine = run_schedule_engine(
        schedule_store.clone(),
        runtime.clone(),
        versioning_rule_store.clone(),
        ScheduleEngineConfig::default(),
        schedule_engine_cancel,
    );

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let routing_subscription_cancel = CancellationToken::new();
    let router: Arc<dyn tokeira_edge::EdgeRouter> = if let Some(controller_endpoint) =
        effective_config
            .infrastructure
            .placement
            .controller_endpoint
            .clone()
    {
        let routing_cache = Arc::new(RoutingCache::new(placement_config(&effective_config)));
        let routing_config = EdgeRoutingConfig {
            controller_endpoint,
            max_retries: effective_config
                .infrastructure
                .placement
                .routing_max_retries,
        };
        let subscription_cache = routing_cache.clone();
        let subscription_cancel = routing_subscription_cancel.clone();
        let _routing_subscription = tokio::spawn(async move {
            if let Err(error) =
                run_routing_subscription(subscription_cache, routing_config, subscription_cancel)
                    .await
            {
                tracing::warn!(?error, "routing subscription exited");
            }
        });
        Arc::new(CacheBackedRouter::new(routing_cache, node_id))
    } else {
        Arc::new(LocalOnlyRouter)
    };
    let workflow_broker = runtime.broker();
    let buffered_queries = runtime.buffered_queries();
    let worker_registry = runtime.worker_registry();
    let nexus_task_broker = runtime.nexus_task_broker();
    let runtime_adapter = Arc::new(RuntimeAdapter::new(runtime.clone()));
    let resolver = Arc::new(StoreExecutionResolver::new(
        repo.clone(),
        default_namespace_id,
    ));
    let visibility_store = InMemoryVisibilityStore::default();
    let visibility = Arc::new(VisibilityQueryService::new(visibility_store.clone()));
    let long_polls = LongPollGate::new(LongPollConfig::default());
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
    // Visibility is populated from the projection log, not from ad hoc edge
    // mutation hooks. One worker per partition keeps checkpoint ownership and
    // replay boundaries explicit.
    for partition_id in 0..16 {
        let projection_worker = ProjectionWorker {
            log: store.clone(),
            sink: VisibilitySink::new(
                visibility_store.clone(),
                format!("visibility-{partition_id}"),
            ),
            batch_size: 256,
        };
        tokio::spawn(async move {
            let cancel = CancellationToken::new();
            if let Err(error) = projection_worker
                .run_from_cursor(
                    &format!("visibility-{partition_id}"),
                    cancel,
                    ProjectionCursor::beginning(partition_id, 1),
                )
                .await
            {
                tracing::warn!(?error, partition_id, "projection worker exited");
            }
        });
    }

    // Assemble the transport services once shared runtime/read-side components
    // exist. The edge stays thin: it delegates workflow semantics back into the
    // runtime and uses visibility/operator helpers only for read APIs.
    let workflow_service =
        WorkflowService::new_with_versioning_and_buffered_queries_and_history_wait_registry(
            runtime_adapter,
            resolver,
            visibility,
            repo.clone(),
            operator_api.clone(),
            namespaces,
            interceptors.clone(),
            PollerRegistry::default(),
            PendingQueryStore::default(),
            buffered_queries,
            workflow_broker,
            nexus_task_broker,
            long_polls,
            router,
            history_waits,
            versioning_rule_store,
            worker_registry,
            schedule_store,
            Arc::new(tokeira_runtime::BatchOperationStore::default()),
        );
    let operator_service = OperatorService::new(operator_api, interceptors);

    let workflow_grpc = WorkflowServiceGrpc::new(workflow_service);
    let operator_grpc = OperatorServiceGrpc::new(operator_service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .build()
        .context("failed to build gRPC reflection service")?;

    info!("tokeirad gRPC server listening on {addr}");

    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(CorsLayer::permissive())
        .layer(GrpcWebLayer::new())
        .add_service(workflow_grpc.into_service())
        .add_service(operator_grpc.into_service())
        .add_service(reflection)
        .serve(addr)
        .await
        .with_context(|| format!("failed to bind or serve gRPC transport on {addr}"))?;

    Ok(())
}

async fn build_nexus_endpoint_registry(
    namespaces: &dyn NamespaceCache,
    configs: HashMap<String, BootstrapNexusEndpointConfig>,
) -> Result<NexusEndpointRegistry> {
    let mut resolved = HashMap::with_capacity(configs.len());
    for (endpoint_name, config) in configs {
        let target = match config.target {
            BootstrapNexusEndpointTarget::External { address } => {
                EndpointTarget::External { address }
            }
            BootstrapNexusEndpointTarget::Worker {
                namespace_name,
                task_queue,
            } => {
                let namespace = namespaces
                    .get(&namespace_name)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "failed to register nexus worker endpoint `{endpoint_name}`: namespace `{namespace_name}` not found"
                        )
                    })?;
                EndpointTarget::Worker {
                    namespace_id: namespace_id_for(&namespace.name),
                    task_queue: tokeira_types::TaskQueueName(task_queue),
                }
            }
        };
        resolved.insert(endpoint_name, NexusEndpointConfig { target });
    }
    Ok(NexusEndpointRegistry::new(resolved))
}

fn configured_node_endpoint(
    config: &TokeiraConfig,
    listen_addr: std::net::SocketAddr,
) -> NodeEndpoint {
    NodeEndpoint {
        host: config.infrastructure.placement.node_host.clone(),
        port: config
            .infrastructure
            .placement
            .node_port
            .unwrap_or_else(|| listen_addr.port()),
    }
}

fn placement_config(config: &TokeiraConfig) -> PlacementConfig {
    let placement = &config.infrastructure.placement;
    PlacementConfig {
        shard_count: placement.shard_count,
        bundle_count: placement.bundle_count,
        partition_count: placement.partition_count,
        hash_version: placement.hash_version,
    }
}

fn membership_config(
    config: &TokeiraConfig,
    node_id: IncarnationId,
    node_endpoint: NodeEndpoint,
    controller_endpoint: String,
) -> MembershipConfig {
    let placement = &config.infrastructure.placement;
    MembershipConfig {
        controller_endpoint,
        heartbeat_interval: std::time::Duration::from_millis(placement.heartbeat_interval_ms),
        reconnect_base_delay: std::time::Duration::from_millis(placement.reconnect_base_delay_ms),
        reconnect_max_delay: std::time::Duration::from_millis(placement.reconnect_max_delay_ms),
        node_id,
        node_endpoint,
        zone: None,
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_id: env!("CARGO_PKG_VERSION").to_string(),
    }
}

#[derive(Debug)]
struct NoopConnectionBudgetApplier;

impl ConnectionBudgetApplier for NoopConnectionBudgetApplier {
    fn apply_budget(
        &self,
        _rate_per_second: f64,
        _capacity: u64,
        _max_reservoir_size: u32,
    ) -> Result<()> {
        Ok(())
    }
}

struct StoreExecutionResolver<R> {
    repo: Arc<R>,
    namespace_id: NamespaceId,
}

impl<R> StoreExecutionResolver<R> {
    fn new(repo: Arc<R>, namespace_id: NamespaceId) -> Self {
        Self { repo, namespace_id }
    }
}

#[async_trait]
impl<R> ExecutionResolver for StoreExecutionResolver<R>
where
    R: RunRepository + 'static,
{
    async fn current_run_key(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<tokeira_types::RunKey>> {
        // Try current open run first, then fall back to latest run (including closed)
        let result = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?;
        if result.is_some() {
            return Ok(result);
        }
        self.repo
            .find_latest_run(self.namespace_id, &WorkflowId(workflow_id.to_string()))
            .await
    }

    async fn describe_execution(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        // Try current open run first, then fall back to latest run (including closed)
        let run_key = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?
        {
            Some(rk) => rk,
            None => {
                // Workflow may be closed — find the latest run by scanning all runs
                match self
                    .repo
                    .find_latest_run(self.namespace_id, &WorkflowId(workflow_id.to_string()))
                    .await?
                {
                    Some(rk) => rk,
                    None => return Ok(None),
                }
            }
        };

        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(Some(WorkflowExecutionDescription {
                namespace: "default".to_string(),
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                workflow_type: state.workflow_type.0,
                task_queue: state.task_queue.0,
                status: state.status,
                start_time: Some(state.started_at),
                close_time: state.closed_at,
                history_length: state.last_event_id,
                state_transition_count: state.transition_seq.0 as i64,
                memo: state.memo,
                search_attributes: state.search_attributes,
                pending_activities: state
                    .activities
                    .values()
                    .map(
                        |activity| tokeira_edge::translate::PendingActivityDescription {
                            activity_id: activity.activity_id.clone(),
                            activity_type: activity.activity_type.clone(),
                            is_started: activity.started_at.is_some(),
                            attempt: activity.attempt,
                            maximum_attempts: activity
                                .retry_policy
                                .as_ref()
                                .map(|policy| policy.maximum_attempts)
                                .unwrap_or_default(),
                            scheduled_at: activity.scheduled_at,
                            started_at: activity.started_at,
                        },
                    )
                    .collect(),
                pending_children: state
                    .children
                    .values()
                    .map(|child| tokeira_edge::translate::PendingChildDescription {
                        workflow_id: child.child_workflow_id.0.clone(),
                        run_id: child
                            .child_run_id
                            .as_ref()
                            .map(|run_id| run_id.0.to_string()),
                        workflow_type: String::new(),
                        initiated_event_id: child.initiated_event_id,
                        parent_close_policy: child.parent_close_policy,
                    })
                    .collect(),
                pending_workflow_task: state.pending_workflow_task.as_ref().map(|task| {
                    tokeira_edge::translate::PendingWorkflowTaskDescription {
                        is_started: task.started_event_id.is_some(),
                        scheduled_at: task.scheduled_at,
                        started_at: task.started_at,
                        attempt: task.attempt,
                    }
                }),
            })),
            LoadedRun::Absent => Err(anyhow!("resolved run missing from storage: {:?}", run_key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_nexus_endpoint_registry_resolves_worker_namespace_names() {
        let cache = InMemoryNamespaceCache::new();
        let namespace = ResolvedNamespace::active("payments");
        let namespace_id = namespace_id_for("payments");
        cache.insert(namespace).await.expect("namespace insert");

        let registry = build_nexus_endpoint_registry(
            &cache,
            HashMap::from([(
                "payments-endpoint".to_string(),
                BootstrapNexusEndpointConfig {
                    target: BootstrapNexusEndpointTarget::Worker {
                        namespace_name: "payments".to_string(),
                        task_queue: "nexus-q".to_string(),
                    },
                },
            )]),
        )
        .await
        .expect("registry should build");

        let config = registry.resolve("payments-endpoint").expect("endpoint");
        assert_eq!(
            config,
            &NexusEndpointConfig {
                target: EndpointTarget::Worker {
                    namespace_id,
                    task_queue: tokeira_types::TaskQueueName("nexus-q".to_string()),
                },
            }
        );
    }

    #[tokio::test]
    async fn build_nexus_endpoint_registry_rejects_unknown_worker_namespace() {
        let cache = InMemoryNamespaceCache::new();

        let result = build_nexus_endpoint_registry(
            &cache,
            HashMap::from([(
                "payments-endpoint".to_string(),
                BootstrapNexusEndpointConfig {
                    target: BootstrapNexusEndpointTarget::Worker {
                        namespace_name: "missing".to_string(),
                        task_queue: "nexus-q".to_string(),
                    },
                },
            )]),
        )
        .await;

        assert!(result.is_err(), "missing namespace should fail");
        let error = result.err().expect("error");

        assert!(error.to_string().contains("namespace `missing` not found"));
    }

    #[test]
    fn placement_helpers_build_runtime_and_edge_config_from_server_config() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.placement.controller_endpoint =
            Some("http://127.0.0.1:7240".to_string());
        config.infrastructure.placement.node_host = "10.0.0.9".to_string();
        config.infrastructure.placement.node_port = Some(8123);
        config.infrastructure.placement.shard_count = 8;
        config.infrastructure.placement.bundle_count = 4;
        config.infrastructure.placement.partition_count = 64;
        config.infrastructure.placement.hash_version = 2;
        let listen_addr = "127.0.0.1:7233".parse().unwrap();
        let node_id = IncarnationId::new();
        let endpoint = configured_node_endpoint(&config, listen_addr);
        let membership = membership_config(
            &config,
            node_id,
            endpoint.clone(),
            "http://127.0.0.1:7240".into(),
        );

        assert_eq!(endpoint.as_authority(), "10.0.0.9:8123");
        assert_eq!(membership.node_id, node_id);
        assert_eq!(membership.node_endpoint, endpoint);
        assert_eq!(
            placement_config(&config),
            PlacementConfig {
                shard_count: 8,
                bundle_count: 4,
                partition_count: 64,
                hash_version: 2,
            }
        );
    }
}
