//! Memory adapter for the shared storage contracts. Direct inspection includes
//! complete private row values; production selectors remain the operations tested.

use super::*;
use crate::contract_tests::{
    self as contract, ContractBackend, FixtureScope, Rows, SHARD_COUNT, history, recovery,
};

#[async_trait]
impl ContractBackend for InMemoryStore {
    fn repo(&self) -> &dyn RunRepository {
        self
    }
    fn log(&self) -> &dyn ProjectionLog {
        self
    }
    fn leases(&self) -> &dyn LeaseRepository {
        self
    }

    async fn rows(&self, scope: &FixtureScope) -> Result<Rows> {
        let store = self.inner.lock().await;
        let mut rows = Rows::new();
        macro_rules! capture {
            ($name:literal, $values:expr) => {{
                let mut values: Vec<String> = $values.map(|row| format!("{row:?}")).collect();
                values.sort();
                rows.insert($name.into(), values);
            }};
        }
        capture!(
            "state",
            store.runs.iter().filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "placement",
            store
                .run_shard_map
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "history",
            store
                .history
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "principals",
            store
                .history_principals
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "history_size",
            store
                .history_size
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "audit",
            store
                .transition_audit
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "pointer",
            store
                .current_execution
                .iter()
                .filter(|(_, key)| scope.contains(**key))
        );
        capture!(
            "open",
            store
                .current_open
                .iter()
                .filter(|(_, key)| scope.contains(**key))
        );
        capture!(
            "execution_index",
            store
                .execution_index
                .iter()
                .filter(|(_, key)| scope.contains(**key))
        );
        capture!(
            "dedupe",
            store
                .request_dedupe
                .iter()
                .filter(|(_, value)| scope.contains(value.run_key))
        );
        capture!(
            "timers",
            store
                .timer_bucket
                .iter()
                .filter(|(key, _)| scope.contains(key.run_key))
        );
        capture!(
            "activity_dispatch",
            store
                .activity_dispatch
                .iter()
                .filter(|((key, _), _)| scope.contains(*key))
        );
        capture!(
            "workflow_dispatch",
            store
                .workflow_dispatch
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        capture!(
            "backlog",
            store
                .dispatch_backlog
                .iter()
                .filter(|row| scope.contains(row.run_key))
        );
        capture!(
            "operations",
            store
                .outbound_operations
                .iter()
                .filter(|(key, _)| scope.contains(key.source_run))
        );
        capture!(
            "intents",
            store
                .outbound_intents
                .iter()
                .filter(|(key, _)| scope.contains(key.source_run))
        );
        capture!(
            "projection",
            store
                .projection_log
                .iter()
                .filter(|(_, row)| scope.contains(row.run_key))
        );
        capture!(
            "retained",
            store
                .retained_runs
                .iter()
                .filter(|(key, _)| scope.contains(**key))
        );
        Ok(rows)
    }

    async fn cleanup(&self, scope: &FixtureScope) -> Result<()> {
        let mut store = self.inner.lock().await;
        for state in &scope.states {
            let key = state.run_key;
            store.runs.remove(&key);
            store.history.remove(&key);
            store.history_principals.remove(&key);
            store.history_size.remove(&key);
            store.transition_audit.remove(&key);
            store.run_shard_map.remove(&key);
            store.workflow_dispatch.remove(&key);
            store.latest_projection_keys.remove(&key);
            store.retained_runs.remove(&key);
        }
        store.current_open.retain(|_, key| !scope.contains(*key));
        store
            .current_execution
            .retain(|_, key| !scope.contains(*key));
        store.execution_index.retain(|_, key| !scope.contains(*key));
        store
            .request_dedupe
            .retain(|_, row| !scope.contains(row.run_key));
        store
            .timer_bucket
            .retain(|key, _| !scope.contains(key.run_key));
        store
            .activity_dispatch
            .retain(|(key, _), _| !scope.contains(*key));
        store
            .dispatch_backlog
            .retain(|row| !scope.contains(row.run_key));
        store
            .outbound_operations
            .retain(|key, _| !scope.contains(key.source_run));
        store
            .outbound_intents
            .retain(|key, _| !scope.contains(key.source_run));
        store
            .projection_log
            .retain(|_, row| !scope.contains(row.run_key));
        drop(store);
        anyhow::ensure!(
            self.rows(scope).await?.values().all(Vec::is_empty),
            "owned memory rows survived cleanup"
        );
        Ok(())
    }

    async fn remove_hot(&self, key: RunKey) -> Result<()> {
        self.inner.lock().await.runs.remove(&key);
        Ok(())
    }

    async fn expire_lease(&self, home: ShardId) -> Result<()> {
        self.inner.lock().await.bundle_lease_expiries.insert(
            home,
            tokio::time::Instant::now() - std::time::Duration::from_secs(1),
        );
        Ok(())
    }

    async fn clear_lease(&self, home: ShardId, original_owner: &str) -> Result<()> {
        let mut store = self.inner.lock().await;
        if let Some((Some(owner), _, _)) = store.bundle_leases.get(&home) {
            anyhow::ensure!(
                owner == original_owner || owner == &format!("{original_owner}-new"),
                "refusing to clean another fixture's lease"
            );
        }
        store.bundle_leases.remove(&home);
        store.bundle_lease_expiries.remove(&home);
        Ok(())
    }

    async fn seed_image(&self, image: &ProjectionRecord) -> Result<()> {
        self.inner
            .lock()
            .await
            .projection_log
            .insert(projection_log_key(image), image.clone());
        Ok(())
    }

    fn seeded_image_time(&self) -> Option<OffsetDateTime> {
        None
    }

    async fn workflow_row(&self, key: RunKey) -> Result<Option<WorkflowDispatchRow>> {
        Ok(self.inner.lock().await.workflow_dispatch.get(&key).cloned())
    }
}

macro_rules! shared_property {
    ($name:ident, $property:expr, $strategy:path, $case:path) => {
        #[test]
        fn $name() -> Result<()> {
            let runtime = tokio::runtime::Runtime::new()?;
            contract::run_cases(
                stringify!($name),
                "memory",
                $property,
                $strategy(),
                |case| {
                    runtime.block_on(Box::pin($case(
                        &InMemoryStore::with_shard_count(SHARD_COUNT),
                        case,
                    )))
                },
            )
        }
    };
}

shared_property!(
    storage_contract_memory_commit_outcomes,
    5,
    contract::commits::strategy,
    contract::commits::outcomes
);
shared_property!(
    storage_contract_memory_competing_commits,
    6,
    contract::commits::strategy,
    contract::commits::competing
);
shared_property!(
    storage_contract_memory_lease_fence,
    7,
    contract::commits::strategy,
    contract::commits::fence
);

shared_property!(
    storage_contract_memory_projection_pages,
    3,
    contract::projection::strategy,
    contract::projection::pages
);
shared_property!(
    storage_contract_memory_projection_deletion,
    4,
    contract::projection::strategy,
    contract::projection::deletion
);

#[test]
fn storage_contract_memory_history() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    contract::run_cases(
        "storage_contract_memory_history",
        "memory",
        1,
        history::strategy(),
        |case| {
            runtime.block_on(history::case(
                &InMemoryStore::with_shard_count(SHARD_COUNT),
                case,
            ))
        },
    )?;
    runtime.block_on(history::boundaries(&InMemoryStore::with_shard_count(
        SHARD_COUNT,
    )))
}

#[test]
fn storage_contract_memory_recovery() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    contract::run_cases(
        "storage_contract_memory_recovery",
        "memory",
        2,
        recovery::strategy(),
        |case| {
            runtime.block_on(recovery::case(
                &InMemoryStore::with_shard_count(SHARD_COUNT),
                case,
            ))
        },
    )
}
