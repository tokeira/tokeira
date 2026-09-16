//! Independent recovery fixture over durable roots and the three shipped executors.
#![cfg(test)]

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, Ordering},
};

use prost::Message;
use tokeira_chasm::{ExecutionKey, Registry, archetype_id_for_fqn};
use tokeira_chasm_acceptance::{AcceptanceLibrary, Resource};
use tokeira_chasm_activity::{ActivityConfig, ActivityExecution, ActivityLibrary, ActivityState};
use tokeira_edge::{
    chasm_activity::{ActivityBridge, ActivityDispatchQueue, PolledActivityTask},
    chasm_executors::{ActivityDispatchExecutor, DeliverCallbackExecutor, StartActivityExecutor},
    namespace_cache::InMemoryNamespaceCache,
};
use tokeira_engine::chasm::{Component, ComponentRef, DeploymentVersionTarget, TypedEngine};
use tokeira_proto::failure::{ApplicationFailureInfo, Failure, failure::FailureInfo};
use tokeira_runtime::{
    chasm::{
        ChasmEngine, ChasmTimerSweeper, CollectingVisibilitySink, DispatchMultiplexer, Engine,
        OutboxRebuildScanner, VisibilitySink,
    },
    nexus::{NexusCompletionRuntimeConfig, NoopNexusCompletionClient},
};
use tokeira_storage::InMemoryChasmNodeStore;

pub(crate) const SECOND: i64 = 1_000_000_000;
pub(crate) const QUEUE: &str = "acceptance-queue";

pub(crate) fn target() -> DeploymentVersionTarget {
    DeploymentVersionTarget {
        deployment_name: "acceptance".into(),
        build_id: "v1".into(),
    }
}

pub(crate) fn key() -> ExecutionKey {
    ExecutionKey::new(
        "00000000-0000-0000-0000-000000000001",
        "resource",
        "00000000-0000-0000-0000-000000000002",
    )
}

pub(crate) fn reference(key: ExecutionKey) -> ComponentRef {
    ComponentRef::new(
        key,
        archetype_id_for_fqn(Resource::FQN),
        Default::default(),
        vec![],
        Default::default(),
    )
}

#[derive(Clone)]
pub(crate) struct Clock {
    value: Arc<AtomicI64>,
    reads: Arc<Mutex<Vec<i64>>>,
}

impl Clock {
    pub(crate) fn new(now: i64) -> Self {
        Self {
            value: Arc::new(AtomicI64::new(now)),
            reads: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(crate) fn now(&self) -> i64 {
        self.value.load(Ordering::SeqCst)
    }
    pub(crate) fn set(&self, now: i64) {
        self.value.store(now, Ordering::SeqCst);
    }
    pub(crate) fn source(&self) -> Arc<dyn Fn() -> i64 + Send + Sync> {
        let clock = self.clone();
        Arc::new(move || {
            let now = clock.now();
            clock.reads.lock().unwrap().push(now);
            now
        })
    }
    pub(crate) fn readings(&self) -> Vec<i64> {
        self.reads.lock().unwrap().clone()
    }
}

pub(crate) struct Stack {
    pub(crate) engine: Arc<ChasmEngine>,
    pub(crate) bridge: Arc<ActivityBridge>,
    pub(crate) queue: Arc<ActivityDispatchQueue>,
    pub(crate) sweeper: ChasmTimerSweeper,
    pub(crate) rebuild: OutboxRebuildScanner,
}

impl Stack {
    pub(crate) fn new(
        store: Arc<InMemoryChasmNodeStore>,
        clock: &Clock,
        deliver_callbacks: bool,
    ) -> Self {
        Self::with_visibility(
            store,
            clock,
            deliver_callbacks,
            Arc::new(CollectingVisibilitySink::default()),
        )
    }

    pub(crate) fn with_visibility(
        store: Arc<InMemoryChasmNodeStore>,
        clock: &Clock,
        deliver_callbacks: bool,
        visibility: Arc<dyn VisibilitySink>,
    ) -> Self {
        let mut registry = Registry::builder();
        registry
            .register_library::<ActivityLibrary>()
            .unwrap()
            .seal_built_ins();
        registry.register_library::<AcceptanceLibrary>().unwrap();
        let mux = Arc::new(DispatchMultiplexer::default());
        let engine = Arc::new(
            ChasmEngine::new(
                store.clone(),
                Arc::new(registry.build()),
                mux.clone(),
                visibility,
            )
            .with_clock(clock.source()),
        );
        let config = ActivityConfig {
            enable_standalone: true,
            ..Default::default()
        };
        let queue = Arc::new(ActivityDispatchQueue::new());
        let dispatch = Arc::new(ActivityDispatchExecutor::new(
            Arc::downgrade(&engine),
            queue.clone(),
        ));
        mux.register(dispatch.clone()).unwrap();
        mux.register(Arc::new(StartActivityExecutor::new(
            Arc::downgrade(&engine),
            config.clone(),
            1000,
        )))
        .unwrap();
        if deliver_callbacks {
            mux.register(Arc::new(DeliverCallbackExecutor::new(
                Arc::downgrade(&engine),
                Arc::new(NoopNexusCompletionClient),
                NexusCompletionRuntimeConfig::default(),
                Arc::new(InMemoryNamespaceCache::new()),
            )))
            .unwrap();
        }
        let bridge = Arc::new(
            ActivityBridge::new(engine.clone(), config, 1000).with_dispatch_executor(dispatch),
        );
        let sweeper = ChasmTimerSweeper::new(engine.clone())
            .with_evaluator(archetype_id_for_fqn(ActivityExecution::FQN), bridge.clone());
        let rebuild = OutboxRebuildScanner::new(store, engine.clone(), mux.clone());
        Self {
            engine,
            bridge,
            queue,
            sweeper,
            rebuild,
        }
    }

    pub(crate) fn handle(&self) -> TypedEngine<Resource> {
        TypedEngine::new(self.engine.clone())
    }

    pub(crate) async fn poll(&self) -> Option<PolledActivityTask> {
        self.bridge
            .poll_activity_task(QUEUE, "worker-v1", Some(&target()))
            .await
            .unwrap()
    }

    pub(crate) async fn complete(&self, task: &PolledActivityTask, failed: bool) {
        if failed {
            let failure = failure();
            self.bridge
                .respond_activity_task_failed(
                    &task.task_token,
                    &key().namespace_id,
                    failure.message.clone(),
                    failure.encode_to_vec(),
                    vec![],
                    "worker-v1".into(),
                )
                .await
                .unwrap();
        } else {
            self.bridge
                .respond_activity_task_completed(
                    &task.task_token,
                    &key().namespace_id,
                    vec![],
                    "worker-v1".into(),
                )
                .await
                .unwrap();
        }
    }

    pub(crate) async fn activity_state(&self, task: &PolledActivityTask) -> ActivityState {
        let key = ExecutionKey::new(
            key().namespace_id,
            task.activity_id.clone(),
            task.run_id.clone(),
        );
        let snapshot = self.engine.read_component(&key).await.unwrap();
        ActivityState::decode(snapshot.data.unwrap().as_slice()).unwrap()
    }
}

pub(crate) fn failure() -> Failure {
    Failure {
        message: "reconcile failed".into(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "acceptance".into(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}
