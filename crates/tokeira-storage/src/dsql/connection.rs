//! DSQL connection acquisition and operation-class admission control.
//!
//! Runtime storage is not a single homogeneous workload. Transition commits,
//! recovery reads, projection reads, and control-plane maintenance have
//! different failure impact and latency sensitivity. This module wraps the
//! physical connection reservoir with per-class semaphores so a large read or
//! projection burst cannot starve commits.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use sqlx::PgConnection;
use tokio::{
    sync::{OwnedSemaphorePermit, RwLock, Semaphore},
    task::JoinHandle,
};

use crate::{ConnectionDirector, DbClass, metrics};

use super::{
    DsqlPoolConfig, Reservoir, ReservoirEntry, ReturnedConnection, config::ReservoirConfig,
};

#[derive(Debug)]
pub struct ClassBudgets {
    /// Per-operation-class semaphores.
    ///
    /// The map is behind an `RwLock` so reconfiguration can atomically replace
    /// all class budgets without stopping in-flight operations that already
    /// hold permits from the old semaphores.
    budgets: RwLock<HashMap<DbClass, Arc<Semaphore>>>,
    /// Configured totals kept separately from semaphore state for metrics.
    totals: RwLock<HashMap<DbClass, usize>>,
    /// Fast aggregate used by tests and observability.
    total_budget: AtomicUsize,
}

impl ClassBudgets {
    /// Construct class budgets from a complete allocation map.
    pub fn new(allocations: &HashMap<DbClass, usize>) -> Result<Self> {
        let total_budget = validate_allocations(allocations)?;
        let budgets = build_budget_map(allocations);
        let class_budgets = Self {
            budgets: RwLock::new(budgets),
            totals: RwLock::new(allocations.clone()),
            total_budget: AtomicUsize::new(total_budget),
        };
        Ok(class_budgets)
    }

    pub async fn acquire(&self, class: DbClass) -> Result<OwnedSemaphorePermit> {
        let label = db_class_label(class);
        metrics::increment_dsql_pool_waiting(label);
        let started = Instant::now();
        let semaphore = {
            let budgets = self.budgets.read().await;
            let Some(semaphore) = budgets.get(&class) else {
                metrics::decrement_dsql_pool_waiting(label);
                bail!("missing budget for class {class:?}");
            };
            semaphore.clone()
        };
        self.record_class_metric(class, &semaphore).await;
        let result = semaphore.acquire_owned().await.map_err(Into::into);
        metrics::decrement_dsql_pool_waiting(label);
        if result.is_ok() {
            metrics::record_dsql_class_permit_wait_duration(label, started.elapsed());
        }
        result
    }

    pub async fn reconfigure(&self, allocations: &HashMap<DbClass, usize>) -> Result<()> {
        let total_budget = validate_allocations(allocations)?;
        let mut budgets = self.budgets.write().await;
        *budgets = build_budget_map(allocations);
        let mut totals = self.totals.write().await;
        *totals = allocations.clone();
        self.total_budget.store(total_budget, Ordering::Release);
        drop(totals);
        drop(budgets);
        self.record_metrics().await;
        Ok(())
    }

    pub fn total_budget(&self) -> usize {
        self.total_budget.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub async fn class_available(&self, class: DbClass) -> Option<usize> {
        let budgets = self.budgets.read().await;
        budgets.get(&class).map(|budget| budget.available_permits())
    }

    async fn record_metrics(&self) {
        let budgets = self.budgets.read().await;
        for (class, semaphore) in budgets.iter() {
            self.record_class_metric(*class, semaphore).await;
        }
    }

    async fn record_class_metric(&self, class: DbClass, semaphore: &Semaphore) {
        let totals = self.totals.read().await;
        let total = totals.get(&class).copied().unwrap_or_default();
        let available = semaphore.available_permits();
        let in_use = total.saturating_sub(available);
        metrics::record_dsql_pool_class_budget(db_class_label(class), total, in_use, 0);
    }
}

#[derive(Debug)]
pub struct DsqlConnectionDirector {
    /// Warm physical connections managed independently of operation class.
    reservoir: Arc<Reservoir>,
    /// Logical admission limits layered on top of the reservoir.
    class_budgets: Arc<ClassBudgets>,
    /// Number of permits currently holding a physical connection.
    in_flight: Arc<AtomicUsize>,
    /// Periodic reporter for class budget and reservoir snapshots.
    reporter_handle: JoinHandle<()>,
}

impl DsqlConnectionDirector {
    /// Start the background connection reservoir and class-budget controller.
    pub fn start(config: DsqlPoolConfig, reservoir: Reservoir) -> Result<Self> {
        let reservoir = Arc::new(reservoir);
        let class_budgets = Arc::new(ClassBudgets::new(&default_allocations(&config.reservoir))?);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let reporter_handle = spawn_periodic_reporter(
            Arc::clone(&class_budgets),
            Arc::clone(&reservoir),
            Arc::clone(&in_flight),
        );
        Ok(Self {
            reservoir,
            class_budgets,
            in_flight,
            reporter_handle,
        })
    }

    pub async fn reconfigure_class_budgets(
        &self,
        allocations: &HashMap<DbClass, usize>,
    ) -> Result<()> {
        self.class_budgets.reconfigure(allocations).await
    }
}

#[async_trait]
impl ConnectionDirector for DsqlConnectionDirector {
    type Permit = DsqlPermit;

    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
        let started = Instant::now();
        let class_guard = self.class_budgets.acquire(class).await?;
        let entry = self.reservoir.checkout()?;
        let in_flight = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::record_dsql_pool_checkout_duration(db_class_label(class), started.elapsed());
        metrics::set_dsql_reservoir_in_flight(in_flight);
        metrics::set_dsql_reservoir_utilization_ratio(in_flight, self.reservoir.ready_count());
        Ok(DsqlPermit::new(
            class,
            entry,
            class_guard,
            self.reservoir.return_sender(),
            self.reservoir.slot_manager(),
            Arc::clone(&self.in_flight),
        ))
    }
}

impl Drop for DsqlConnectionDirector {
    fn drop(&mut self) {
        self.reporter_handle.abort();
    }
}

#[async_trait]
pub(crate) trait DsqlConnectionAcquirer: std::fmt::Debug + Send + Sync {
    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit>;
}

#[async_trait]
impl DsqlConnectionAcquirer for DsqlConnectionDirector {
    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
        ConnectionDirector::acquire(self, class).await
    }
}

#[derive(Debug)]
pub struct DsqlPermit {
    /// Operation class that consumed this permit.
    pub class: DbClass,
    /// Checked-out connection.
    ///
    /// The `Option` is required because `Drop` only receives `&mut self`; taking
    /// the connection allows the permit to return ownership to the reservoir
    /// without unsafe code or an explicit close API.
    connection: Option<PgConnection>,
    /// Creation timestamp of the physical connection, not the checkout time.
    created_at: std::time::Instant,
    /// Maximum lifetime assigned when the connection was created.
    max_lifetime: std::time::Duration,
    /// Semaphore permit enforcing operation-class admission.
    _class_guard: OwnedSemaphorePermit,
    /// Synchronous return path into the reservoir return processor.
    reservoir_return: tokio::sync::mpsc::UnboundedSender<ReturnedConnection>,
    /// Slot accounting owner for discard paths that bypass the return processor.
    slot_manager: Arc<super::SlotBlockManager>,
    /// Shared in-flight counter owned by the director.
    director_in_flight: Arc<AtomicUsize>,
    /// Caller-set flag that causes return processing to discard the connection.
    marked_bad: bool,
}

impl DsqlPermit {
    fn new(
        class: DbClass,
        entry: ReservoirEntry,
        class_guard: OwnedSemaphorePermit,
        reservoir_return: tokio::sync::mpsc::UnboundedSender<ReturnedConnection>,
        slot_manager: Arc<super::SlotBlockManager>,
        director_in_flight: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            class,
            connection: Some(entry.connection),
            created_at: entry.created_at,
            max_lifetime: entry.max_lifetime,
            _class_guard: class_guard,
            reservoir_return,
            slot_manager,
            director_in_flight,
            marked_bad: false,
        }
    }

    pub fn connection(&mut self) -> Result<&mut PgConnection> {
        let Some(connection) = self.connection.as_mut() else {
            bail!("DSQL permit connection already returned");
        };
        Ok(&mut *connection)
    }

    pub fn mark_bad(&mut self) {
        self.marked_bad = true;
    }
}

impl Drop for DsqlPermit {
    fn drop(&mut self) {
        let previous = self.director_in_flight.fetch_sub(1, Ordering::AcqRel);
        metrics::set_dsql_reservoir_in_flight(previous.saturating_sub(1));
        // Dropping the permit is the storage-layer "return connection" API.
        // Expired connections are intentionally discarded here because handing
        // them back to the ready pool would create rare mid-transaction expiry
        // failures that are hard to diagnose.
        if let Some(connection) = self.connection.take() {
            if self.created_at.elapsed() <= self.max_lifetime {
                if self
                    .reservoir_return
                    .send(ReturnedConnection {
                        entry: ReservoirEntry {
                            connection,
                            created_at: self.created_at,
                            max_lifetime: self.max_lifetime,
                        },
                        marked_bad: self.marked_bad,
                    })
                    .is_err()
                {
                    self.slot_manager.release_slot();
                    metrics::record_dsql_pool_connection_retired("return_channel_closed");
                }
            } else {
                metrics::record_dsql_reservoir_connection_age("expired", self.created_at.elapsed());
                metrics::record_dsql_pool_connection_retired("expired");
                self.slot_manager.release_slot();
            }
        }
    }
}

fn spawn_periodic_reporter(
    class_budgets: Arc<ClassBudgets>,
    reservoir: Arc<Reservoir>,
    in_flight: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            class_budgets.record_metrics().await;
            let ready = reservoir.ready_count();
            let in_flight = in_flight.load(Ordering::Acquire);
            metrics::record_dsql_pool_connections_total(ready);
            metrics::set_dsql_reservoir_in_flight(in_flight);
            metrics::set_dsql_reservoir_utilization_ratio(in_flight, ready);
        }
    })
}

fn validate_allocations(allocations: &HashMap<DbClass, usize>) -> Result<usize> {
    let total_budget: usize = allocations.values().sum();
    if total_budget == 0 {
        bail!("class budget total must be greater than zero");
    }
    for class in all_classes() {
        if allocations.get(&class).copied().unwrap_or_default() == 0 {
            bail!("missing positive budget for class {class:?}");
        }
    }
    Ok(total_budget)
}

fn build_budget_map(allocations: &HashMap<DbClass, usize>) -> HashMap<DbClass, Arc<Semaphore>> {
    allocations
        .iter()
        .map(|(class, permits)| (*class, Arc::new(Semaphore::new(*permits))))
        .collect()
}

/// Compute default per-class allocations that sum exactly to `target_ready`.
///
/// Priority ratios: Control 10%, Commit 50%, Read 20%, Projection 10%,
/// Maintenance gets the remainder. Each class gets at least 1 permit.
fn default_allocations(config: &ReservoirConfig) -> HashMap<DbClass, usize> {
    let total = config.target_ready.max(5);
    let control = (total / 10).max(1);
    let commit = (total / 2).max(1);
    let read = (total / 5).max(1);
    let projection = (total / 10).max(1);
    let allocated = control + commit + read + projection;
    // Maintenance absorbs the remainder so the sum equals total.
    // If the fixed allocations already exceed total (small target_ready),
    // maintenance gets 1 and the total is allowed to exceed target_ready
    // by a small amount — the reservoir will simply have more permits
    // than ready connections, causing natural backpressure.
    let maintenance = if allocated >= total {
        1
    } else {
        total - allocated
    };

    let mut allocations = HashMap::new();
    allocations.insert(DbClass::Control, control);
    allocations.insert(DbClass::Commit, commit);
    allocations.insert(DbClass::Read, read);
    allocations.insert(DbClass::Projection, projection);
    allocations.insert(DbClass::Maintenance, maintenance);
    allocations
}

fn all_classes() -> [DbClass; 5] {
    [
        DbClass::Control,
        DbClass::Commit,
        DbClass::Read,
        DbClass::Projection,
        DbClass::Maintenance,
    ]
}

fn db_class_label(class: DbClass) -> &'static str {
    match class {
        DbClass::Control => "control",
        DbClass::Commit => "commit",
        DbClass::Read => "read",
        DbClass::Projection => "projection",
        DbClass::Maintenance => "maintenance",
    }
}

#[cfg(test)]
fn test_allocations(commit: usize) -> HashMap<DbClass, usize> {
    let mut allocations = HashMap::new();
    allocations.insert(DbClass::Control, 1);
    allocations.insert(DbClass::Commit, commit);
    allocations.insert(DbClass::Read, 1);
    allocations.insert(DbClass::Projection, 1);
    allocations.insert(DbClass::Maintenance, 1);
    allocations
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[tokio::test]
    async fn class_budget_acquire_and_release() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let permit = budgets.acquire(DbClass::Commit).await.unwrap();
        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(0));
        drop(permit);
        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(1));
    }

    #[tokio::test]
    async fn class_budget_reconfiguration_replaces_allocations() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        budgets.reconfigure(&test_allocations(3)).await.unwrap();
        assert_eq!(budgets.total_budget(), 7);
        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(3));
    }

    proptest! {
        #[test]
        fn class_budget_sum_invariant(commit in 1usize..128) {
            let allocations = test_allocations(commit);
            let expected: usize = allocations.values().sum();
            let budgets = ClassBudgets::new(&allocations).unwrap();
            prop_assert_eq!(budgets.total_budget(), expected);
        }
    }
}
