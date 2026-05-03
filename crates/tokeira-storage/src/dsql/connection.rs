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
use aurora_dsql_sqlx_connector::DsqlConnectOptions;
use sqlx::{PgConnection, PgPool, Pool, Postgres, pool::PoolConnection};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::{ConnectionDirector, DbClass, metrics};

use super::{
    DsqlAuthConfig, DsqlPoolConfig, Reservoir, ReservoirEntry, TokenBucketRateLimiter,
    config::ReservoirConfig,
};

#[derive(Clone, Debug)]
pub struct DsqlConnector {
    pool: PgPool,
}

impl DsqlConnector {
    /// Wrap an existing SQLx pool.
    ///
    /// Tests and embedding applications can use this to provide their own pool
    /// policy while still exercising Tokeira's reservoir and class budgets.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Build a DSQL-aware SQLx pool from IAM auth settings.
    ///
    /// The current connector uses the same PostgreSQL username for all roles;
    /// the role distinction is captured in the auth config and left available
    /// for future assume-role plumbing.
    pub async fn connect(
        auth: &DsqlAuthConfig,
        config: &DsqlPoolConfig,
        role: super::DsqlRole,
    ) -> Result<Self> {
        let user = match role {
            super::DsqlRole::Admin => "admin",
            super::DsqlRole::Runtime => "admin", // same user, different IAM role via assume-role
            super::DsqlRole::Readonly => "admin",
        };
        let connection_string = auth.connection_string(user, "postgres")?;
        let options = DsqlConnectOptions::from_connection_string(&connection_string)?;
        let pool =
            aurora_dsql_sqlx_connector::pool::connect_with(&options, auth.pool_options(config))
                .await?;
        Ok(Self { pool })
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Postgres>> {
        self.pool.acquire().await.map_err(Into::into)
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

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
        let semaphore = {
            let budgets = self.budgets.read().await;
            let Some(semaphore) = budgets.get(&class) else {
                bail!("missing budget for class {class:?}");
            };
            semaphore.clone()
        };
        self.record_class_metric(class, &semaphore).await;
        semaphore.acquire_owned().await.map_err(Into::into)
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
    reservoir: Reservoir,
    /// Logical admission limits layered on top of the reservoir.
    class_budgets: ClassBudgets,
}

impl DsqlConnectionDirector {
    /// Start the background connection reservoir and class-budget controller.
    pub async fn start(config: DsqlPoolConfig, connector: DsqlConnector) -> Result<Self> {
        let rate_limiter =
            TokenBucketRateLimiter::new(config.connection_rate_per_second, config.burst_capacity);
        let reservoir = Reservoir::start(config.reservoir.clone(), connector, rate_limiter).await?;
        let class_budgets = ClassBudgets::new(&default_allocations(&config.reservoir))?;
        Ok(Self {
            reservoir,
            class_budgets,
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
        let entry = self.reservoir.checkout().await?;
        metrics::record_dsql_pool_checkout_duration(db_class_label(class), started.elapsed());
        Ok(DsqlPermit::new(
            class,
            entry,
            class_guard,
            self.reservoir.return_sender(),
        ))
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
    connection: Option<PoolConnection<Postgres>>,
    /// Creation timestamp of the physical connection, not the checkout time.
    created_at: std::time::Instant,
    /// Maximum lifetime assigned when the connection was created.
    max_lifetime: std::time::Duration,
    /// Semaphore permit enforcing operation-class admission.
    _class_guard: OwnedSemaphorePermit,
    /// Synchronous return path into the reservoir return processor.
    reservoir_return: tokio::sync::mpsc::UnboundedSender<ReservoirEntry>,
}

impl DsqlPermit {
    fn new(
        class: DbClass,
        entry: ReservoirEntry,
        class_guard: OwnedSemaphorePermit,
        reservoir_return: tokio::sync::mpsc::UnboundedSender<ReservoirEntry>,
    ) -> Self {
        Self {
            class,
            connection: Some(entry.connection),
            created_at: entry.created_at,
            max_lifetime: entry.max_lifetime,
            _class_guard: class_guard,
            reservoir_return,
        }
    }

    pub fn connection(&mut self) -> Result<&mut PgConnection> {
        let Some(connection) = self.connection.as_mut() else {
            bail!("DSQL permit connection already returned");
        };
        Ok(&mut *connection)
    }
}

impl Drop for DsqlPermit {
    fn drop(&mut self) {
        // Dropping the permit is the storage-layer "return connection" API.
        // Expired connections are intentionally discarded here because handing
        // them back to the ready pool would create rare mid-transaction expiry
        // failures that are hard to diagnose.
        if let Some(connection) = self.connection.take() {
            if self.created_at.elapsed() <= self.max_lifetime {
                let _ = self.reservoir_return.send(ReservoirEntry {
                    connection,
                    created_at: self.created_at,
                    max_lifetime: self.max_lifetime,
                });
            } else {
                metrics::record_dsql_pool_connection_retired("expired");
            }
        }
    }
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
