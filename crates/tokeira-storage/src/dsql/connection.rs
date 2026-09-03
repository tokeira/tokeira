//! DSQL connection acquisition and operation-class admission control.
//!
//! Runtime storage is not a single homogeneous workload. Transition commits,
//! recovery reads, projection reads, and control-plane maintenance have
//! different failure impact and latency sensitivity. This module wraps the
//! physical connection reservoir with per-class semaphores so a large read or
//! projection burst cannot starve commits.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex, RwLock as StdRwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use sqlx::PgConnection;
use tokeira_observability::DbClassLabel;
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore},
    task::JoinHandle,
};

use crate::{ConnectionDirector, DbClass, metrics};

use super::{
    DsqlPoolConfig, Reservoir, ReservoirEntry, ReturnedConnection,
    config::ReservoirConfig,
    connection_coordinator::EmbeddedConnectionCoordinator,
    control_lease::{OwnershipAdmissionGate, OwnershipAdmissionPermit, OwnershipAdmissionState},
    embedded_reservoir,
    embedded_reservoir::EmbeddedReservoir,
};

const LEAK_SUSPECT_AFTER: StdDuration = StdDuration::from_secs(30);
const LEAK_SCAN_INTERVAL: StdDuration = StdDuration::from_secs(5);
const EMBEDDED_SHUTDOWN_TIMEOUT: StdDuration = StdDuration::from_secs(30);
const CLASS_BUDGET_PRESSURE_AFTER: StdDuration = StdDuration::from_secs(5);
const CLASS_BUDGET_WARNING_COOLDOWN: StdDuration = StdDuration::from_secs(60);

#[derive(Debug, Default)]
struct ClassPressure {
    // Equal clock readings still represent distinct acquisitions; dropping one
    // registration must not hide another waiter with the same start time.
    waiting: BTreeMap<Instant, usize>,
    // Observations also run on checkout: avoid scanning a saturated queue.
    waiter_count: usize,
    last_warning: Option<Instant>,
}

#[derive(Debug)]
struct ClassPressureSample {
    waiters: usize,
    longest_wait: StdDuration,
    warn: bool,
}

impl ClassPressure {
    fn register(&mut self, started: Instant) {
        *self.waiting.entry(started).or_default() += 1;
        self.waiter_count += 1;
    }

    fn complete(&mut self, started: Instant) {
        let count = self
            .waiting
            .get_mut(&started)
            .expect("a class waiter must complete its own registration");
        *count -= 1;
        if *count == 0 {
            self.waiting.remove(&started);
        }
        self.waiter_count -= 1;
    }

    fn observe(&mut self, now: Instant) -> ClassPressureSample {
        let longest_wait = self
            .waiting
            .first_key_value()
            .map_or(StdDuration::ZERO, |(started, _)| {
                now.saturating_duration_since(*started)
            });
        let warn = longest_wait >= CLASS_BUDGET_PRESSURE_AFTER
            && self.last_warning.is_none_or(|last| {
                now.saturating_duration_since(last) >= CLASS_BUDGET_WARNING_COOLDOWN
            });
        if warn {
            // Observation and reservation happen under one class mutex. Keep
            // this history when the queue drains, or flapping bypasses the cap.
            self.last_warning = Some(now);
        }
        ClassPressureSample {
            waiters: self.waiter_count,
            longest_wait,
            warn,
        }
    }
}

#[derive(Debug)]
struct ClassWaiter {
    pressure: Arc<Mutex<ClassPressure>>,
    started: Instant,
    label: DbClassLabel,
}

impl ClassWaiter {
    fn new(pressure: Arc<Mutex<ClassPressure>>, label: DbClassLabel) -> Self {
        let started = Instant::now();
        pressure
            .lock()
            .expect("class pressure lock poisoned")
            .register(started);
        metrics::increment_dsql_pool_waiting(label);
        Self {
            pressure,
            started,
            label,
        }
    }
}

impl Drop for ClassWaiter {
    fn drop(&mut self) {
        // Acquisition futures can be cancelled without returning through
        // `acquire`; stale registrations would manufacture permanent pressure.
        self.pressure
            .lock()
            .expect("class pressure lock poisoned")
            .complete(self.started);
        metrics::decrement_dsql_pool_waiting(self.label);
    }
}

/// Class-local admission permits and cancellation-safe pressure observations.
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
    /// Separate from semaphore generations so reconfiguration cannot reset
    /// cooldowns or orphan registrations still waiting on an old semaphore.
    pressure: HashMap<DbClass, Arc<Mutex<ClassPressure>>>,
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
            pressure: all_classes()
                .into_iter()
                .map(|class| (class, Arc::new(Mutex::new(ClassPressure::default()))))
                .collect(),
            total_budget: AtomicUsize::new(total_budget),
        };
        Ok(class_budgets)
    }

    /// Wait for this class's permit without consuming another class's budget.
    /// Cancellation removes pressure accounting; a closed semaphore returns an error.
    pub async fn acquire(&self, class: DbClass) -> Result<OwnedSemaphorePermit> {
        let label = db_class_label(class);
        let started = Instant::now();
        let semaphore = {
            let budgets = self.budgets.read().await;
            let Some(semaphore) = budgets.get(&class) else {
                bail!("missing budget for class {class:?}");
            };
            semaphore.clone()
        };
        self.record_class_metric(class, &semaphore, Instant::now())
            .await;
        // Wait time is measured around only the class semaphore. Reservoir
        // empty backpressure is intentionally a separate signal emitted after
        // admission.
        let result = if let Ok(permit) = semaphore.clone().try_acquire_owned() {
            // Most operations need no wait registration or ordered-map allocation.
            Ok(permit)
        } else {
            let waiter = ClassWaiter::new(Arc::clone(&self.pressure[&class]), label);
            let result = semaphore.acquire_owned().await.map_err(Into::into);
            drop(waiter);
            result
        };
        if result.is_ok() {
            metrics::record_dsql_class_permit_wait_duration(label, started.elapsed());
        }
        result
    }

    /// Replace allocations while preserving old permits, waiters, and cooldowns.
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

    async fn close(&self) {
        let budgets = self.budgets.read().await;
        for semaphore in budgets.values() {
            semaphore.close();
        }
    }

    /// Sum of the current generation's configured class allocations.
    pub fn total_budget(&self) -> usize {
        self.total_budget.load(Ordering::Acquire)
    }

    #[cfg(test)]
    /// Available permits in the current generation for a test observation.
    pub async fn class_available(&self, class: DbClass) -> Option<usize> {
        let budgets = self.budgets.read().await;
        budgets.get(&class).map(|budget| budget.available_permits())
    }

    async fn record_metrics(&self) {
        self.record_metrics_at(Instant::now()).await;
    }

    async fn record_metrics_at(&self, now: Instant) {
        let budgets = self.budgets.read().await;
        for (class, semaphore) in budgets.iter() {
            self.record_class_metric(*class, semaphore, now).await;
        }
    }

    async fn record_class_metric(&self, class: DbClass, semaphore: &Semaphore, now: Instant) {
        let totals = self.totals.read().await;
        let total = totals.get(&class).copied().unwrap_or_default();
        let available = semaphore.available_permits();
        let in_use = total.saturating_sub(available);
        let pressure = self.pressure[&class]
            .lock()
            .expect("class pressure lock poisoned")
            .observe(now);
        metrics::record_dsql_pool_class_budget(
            db_class_label(class),
            total,
            in_use,
            pressure.waiters,
        );
        // Full utilization is routine for a single-permit class, and idle
        // partition polls can briefly queue. Only a still-pending wait of at
        // least five seconds signals pressure. The per-class reservation caps
        // warnings at one per minute even across recovery/reconfiguration;
        // periodic reporting also catches acquisitions that never complete.
        if pressure.warn {
            tracing::warn!(
                class = db_class_label(class).as_str(),
                total_permits = total,
                in_use_permits = in_use,
                waiters = pressure.waiters,
                longest_wait_seconds = pressure.longest_wait.as_secs_f64(),
                pressure_after_seconds = CLASS_BUDGET_PRESSURE_AFTER.as_secs(),
                warning_cooldown_seconds = CLASS_BUDGET_WARNING_COOLDOWN.as_secs(),
                "DSQL class permit acquisition is under sustained pressure"
            );
        }
    }
}

/// Selects an unchanged distributed reservoir or the isolated embedded pool.
///
/// Keeping the variants separate prevents embedded lifecycle and refill
/// mechanics from changing the DynamoDB-backed `tokeirad` path.
#[derive(Debug)]
enum ReservoirController {
    Distributed(Reservoir),
    Embedded(EmbeddedReservoir),
}

impl ReservoirController {
    fn ready_count(&self) -> usize {
        match self {
            Self::Distributed(reservoir) => reservoir.ready_count(),
            Self::Embedded(reservoir) => reservoir.ready_count(),
        }
    }

    /// Live warm-connection target. Only the distributed reservoir can be
    /// capped by a controller budget; the embedded pool's target is its
    /// configuration.
    fn target_ready(&self) -> usize {
        match self {
            Self::Distributed(reservoir) => reservoir.target_ready(),
            Self::Embedded(reservoir) => reservoir.config().target_ready,
        }
    }
}

#[derive(Debug)]
pub struct DsqlConnectionDirector {
    /// Warm physical connections managed independently of operation class.
    reservoir: Arc<ReservoirController>,
    /// Logical admission limits layered on top of the reservoir.
    class_budgets: Arc<ClassBudgets>,
    /// Number of permits currently holding a physical connection.
    in_flight: Arc<AtomicUsize>,
    /// Embedded-only drain notification; distributed shutdown ignores it.
    in_flight_changed: Arc<Notify>,
    /// Embedded-only admission state; distributed acquisition retains its
    /// established immediate-checkout behavior.
    accepting: AtomicBool,
    admission_lock: AsyncMutex<()>,
    /// Managed embedded ownership admission. Distributed mode never installs
    /// this gate and therefore retains its established acquisition path.
    ownership_gate: StdRwLock<Option<OwnershipAdmissionGate>>,
    /// Tracks long-lived checkouts without using stack traces or raw call-site
    /// strings as labels. The call-site dimension is derived from `DbClass`.
    leak_tracker: Arc<CheckoutLeakTracker>,
    /// Periodic reporter for class budget and reservoir snapshots.
    reporter_handle: JoinHandle<()>,
    leak_detector_handle: JoinHandle<()>,
}

impl DsqlConnectionDirector {
    /// Start the background connection reservoir and class-budget controller.
    pub fn start(config: DsqlPoolConfig, reservoir: Reservoir) -> Result<Self> {
        let reservoir = Arc::new(ReservoirController::Distributed(reservoir));
        let class_budgets = Arc::new(ClassBudgets::new(&default_allocations(&config.reservoir))?);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let in_flight_changed = Arc::new(Notify::new());
        let leak_tracker = Arc::new(CheckoutLeakTracker::default());
        let reporter_handle = spawn_periodic_reporter(
            Arc::clone(&class_budgets),
            Arc::clone(&reservoir),
            Arc::clone(&in_flight),
        );
        let leak_detector_handle = spawn_leak_detector(Arc::clone(&leak_tracker));
        Ok(Self {
            reservoir,
            class_budgets,
            in_flight,
            in_flight_changed,
            accepting: AtomicBool::new(true),
            admission_lock: AsyncMutex::new(()),
            ownership_gate: StdRwLock::new(None),
            leak_tracker,
            reporter_handle,
            leak_detector_handle,
        })
    }

    /// Start the embedded-only pool without changing distributed reservoir mechanics.
    pub(crate) fn start_embedded(
        config: &ReservoirConfig,
        reservoir: EmbeddedReservoir,
    ) -> Result<Self> {
        let reservoir = Arc::new(ReservoirController::Embedded(reservoir));
        let class_budgets = Arc::new(ClassBudgets::new(&default_allocations(config))?);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let in_flight_changed = Arc::new(Notify::new());
        let leak_tracker = Arc::new(CheckoutLeakTracker::default());
        let reporter_handle = spawn_periodic_reporter(
            Arc::clone(&class_budgets),
            Arc::clone(&reservoir),
            Arc::clone(&in_flight),
        );
        let leak_detector_handle = spawn_leak_detector(Arc::clone(&leak_tracker));
        Ok(Self {
            reservoir,
            class_budgets,
            in_flight,
            in_flight_changed,
            accepting: AtomicBool::new(true),
            admission_lock: AsyncMutex::new(()),
            ownership_gate: StdRwLock::new(None),
            leak_tracker,
            reporter_handle,
            leak_detector_handle,
        })
    }

    pub async fn reconfigure_class_budgets(
        &self,
        allocations: &HashMap<DbClass, usize>,
    ) -> Result<()> {
        self.class_budgets.reconfigure(allocations).await
    }

    /// Apply a placement-controller connection budget to the reservoir: cap
    /// the warm target at `min(configured target_ready, max_reservoir_size)`
    /// and retire idle connections above it. Returns how many were retired.
    ///
    /// A directive can only lower the target, never raise it above the
    /// operator's configuration; the cluster-wide share is a ceiling on this
    /// node, not a licence to exceed local limits. Only the distributed
    /// reservoir is budgeted: the embedded engine never joins a placement
    /// controller, so a directive reaching an embedded director is a wiring
    /// error and is reported as one.
    pub fn apply_connection_budget(&self, max_reservoir_size: u32) -> Result<usize> {
        let ReservoirController::Distributed(reservoir) = self.reservoir.as_ref() else {
            bail!("connection budgets apply only to the distributed DSQL reservoir");
        };
        let target = usize::try_from(max_reservoir_size)
            .unwrap_or(usize::MAX)
            .min(reservoir.config().target_ready)
            .max(1);
        reservoir.reconfigure_target(u32::try_from(target).unwrap_or(u32::MAX));
        Ok(reservoir.retire_excess(u32::try_from(target).unwrap_or(u32::MAX)))
    }

    /// Warm connections available for immediate checkout, reported to the
    /// placement controller as this node's connection headroom.
    pub fn ready_connections(&self) -> usize {
        self.reservoir.ready_count()
    }

    /// Live warm-connection target after any controller cap.
    pub fn reservoir_target(&self) -> usize {
        self.reservoir.target_ready()
    }

    /// Install the singleton-owner gate on an embedded director exactly once.
    ///
    /// The shared gate makes loss of the DSQL owner claim close both the
    /// in-process RPC boundary and subsequent physical connection checkouts.
    pub fn install_ownership_gate(&self, gate: OwnershipAdmissionGate) -> Result<()> {
        if !matches!(self.reservoir.as_ref(), ReservoirController::Embedded(_)) {
            bail!("ownership admission is available only for embedded DSQL");
        }
        let mut installed = self
            .ownership_gate
            .write()
            .expect("embedded ownership gate lock poisoned");
        if installed.is_some() {
            bail!("embedded DSQL ownership admission is already installed");
        }
        *installed = Some(gate);
        Ok(())
    }

    /// Acquire the control connection needed to conditionally release the
    /// owner claim after ordinary ownership admission has closed.
    ///
    /// This narrow shutdown seam bypasses only the owner gate. Director
    /// shutdown admission and every class/physical pool bound still apply.
    pub async fn acquire_shutdown_control(&self) -> Result<DsqlPermit> {
        let ReservoirController::Embedded(reservoir) = self.reservoir.as_ref() else {
            bail!("shutdown control acquisition is available only for embedded DSQL");
        };
        self.acquire_embedded(DbClass::Control, reservoir, false)
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        match self.reservoir.as_ref() {
            ReservoirController::Distributed(reservoir) => {
                // Preserve the pre-embedded distributed shutdown path exactly:
                // stop diagnostics, then let the original reservoir release its
                // DynamoDB slot blocks.
                self.reporter_handle.abort();
                self.leak_detector_handle.abort();
                reservoir.shutdown().await
            }
            ReservoirController::Embedded(_) => {
                self.shutdown_with_deadline(Instant::now() + EMBEDDED_SHUTDOWN_TIMEOUT)
                    .await
            }
        }
    }

    /// Close and drain only the embedded pool within a caller-owned deadline.
    pub async fn shutdown_with_deadline(&self, deadline: Instant) -> Result<()> {
        let ReservoirController::Embedded(reservoir) = self.reservoir.as_ref() else {
            self.reporter_handle.abort();
            self.leak_detector_handle.abort();
            let ReservoirController::Distributed(reservoir) = self.reservoir.as_ref() else {
                unreachable!("reservoir controller variant changed during shutdown");
            };
            return reservoir.shutdown().await;
        };

        {
            let _guard = self.admission_lock.lock().await;
            self.accepting.store(false, Ordering::Release);
        }
        self.class_budgets.close().await;
        self.reporter_handle.abort();
        self.leak_detector_handle.abort();
        reservoir.begin_shutdown();

        while self.in_flight.load(Ordering::Acquire) != 0 {
            let notified = self.in_flight_changed.notified();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            let now = Instant::now();
            if now >= deadline
                || tokio::time::timeout(deadline.saturating_duration_since(now), notified)
                    .await
                    .is_err()
            {
                reservoir.abort_return_processing();
                bail!(
                    "timed out draining {} checked-out embedded DSQL connections",
                    self.in_flight.load(Ordering::Acquire)
                );
            }
        }

        let now = Instant::now();
        if now >= deadline {
            reservoir.abort_return_processing();
            bail!("timed out shutting down embedded DSQL connection resources");
        }
        tokio::time::timeout(
            deadline.saturating_duration_since(now),
            reservoir.finish_shutdown(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out shutting down embedded DSQL resources"))?
    }
}

#[async_trait]
impl ConnectionDirector for DsqlConnectionDirector {
    type Permit = DsqlPermit;

    async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
        match self.reservoir.as_ref() {
            ReservoirController::Distributed(reservoir) => {
                self.acquire_distributed(class, reservoir).await
            }
            ReservoirController::Embedded(reservoir) => {
                self.acquire_embedded(class, reservoir, true).await
            }
        }
    }
}

impl DsqlConnectionDirector {
    async fn acquire_distributed(
        &self,
        class: DbClass,
        reservoir: &Reservoir,
    ) -> Result<DsqlPermit> {
        // This is the pre-embedded `tokeirad` acquisition sequence: class
        // permit, immediate checkout, and the existing slot-block-backed permit.
        let started = Instant::now();
        let class_guard = self.class_budgets.acquire(class).await?;
        let entry = reservoir.checkout()?;
        // From this point until `DsqlPermit::drop`, the connection is out of
        // the ready reservoir but still owns exactly one slot reservation.
        let in_flight = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::record_dsql_pool_checkout_duration(db_class_label(class), started.elapsed());
        metrics::set_dsql_reservoir_in_flight(in_flight);
        metrics::set_dsql_reservoir_utilization_ratio(in_flight, reservoir.ready_count());
        let leak_checkout = self.leak_tracker.track(class, Instant::now());
        Ok(DsqlPermit::new(
            class,
            entry,
            class_guard,
            reservoir.return_sender(),
            reservoir.slot_manager(),
            Arc::clone(&self.in_flight),
            Arc::clone(&self.leak_tracker),
            leak_checkout,
        ))
    }

    async fn acquire_embedded(
        &self,
        class: DbClass,
        reservoir: &EmbeddedReservoir,
        enforce_ownership: bool,
    ) -> Result<DsqlPermit> {
        if !self.accepting.load(Ordering::Acquire) {
            bail!("embedded DSQL connection director is closed");
        }
        let ownership_gate = enforce_ownership
            .then(|| {
                self.ownership_gate
                    .read()
                    .expect("embedded ownership gate lock poisoned")
                    .clone()
            })
            .flatten();
        let ownership_guard = ownership_gate
            .as_ref()
            .map(OwnershipAdmissionGate::admit)
            .transpose()?;
        let started = Instant::now();
        let class_guard = self.class_budgets.acquire(class).await?;
        let entry = reservoir.checkout_wait().await?;
        let admission_guard = self.admission_lock.lock().await;
        if !self.accepting.load(Ordering::Acquire)
            || ownership_gate
                .as_ref()
                .is_some_and(|gate| gate.state() != OwnershipAdmissionState::Open)
        {
            reservoir.coordinator().release_slot();
            drop(entry);
            bail!("embedded DSQL connection admission is closed");
        }
        let Some(return_sender) = reservoir.return_sender() else {
            reservoir.coordinator().release_slot();
            drop(entry);
            bail!("embedded DSQL return path is closed");
        };
        let in_flight = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        drop(admission_guard);
        metrics::record_dsql_pool_checkout_duration(db_class_label(class), started.elapsed());
        metrics::set_dsql_reservoir_in_flight(in_flight);
        metrics::set_dsql_reservoir_utilization_ratio(in_flight, reservoir.ready_count());
        let leak_checkout = self.leak_tracker.track(class, Instant::now());
        Ok(DsqlPermit::new_embedded(
            class,
            entry,
            class_guard,
            return_sender,
            reservoir.coordinator(),
            Arc::clone(&self.in_flight),
            Arc::clone(&self.in_flight_changed),
            Arc::clone(&self.leak_tracker),
            leak_checkout,
            ownership_guard,
        ))
    }
}

impl Drop for DsqlConnectionDirector {
    fn drop(&mut self) {
        self.reporter_handle.abort();
        self.leak_detector_handle.abort();
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

#[derive(Clone, Copy, Debug)]
struct CheckoutLeak {
    id: u64,
    class: DbClass,
    call_site: &'static str,
}

#[derive(Debug)]
struct CheckoutLeakEntry {
    checkout: CheckoutLeak,
    started_at: Instant,
    suspected: bool,
}

#[derive(Debug, Default)]
struct CheckoutLeakTracker {
    next_id: AtomicU64,
    entries: Mutex<HashMap<u64, CheckoutLeakEntry>>,
}

impl CheckoutLeakTracker {
    fn track(&self, class: DbClass, started_at: Instant) -> CheckoutLeak {
        let checkout = CheckoutLeak {
            id: self.next_id.fetch_add(1, Ordering::AcqRel),
            class,
            call_site: checkout_call_site(class),
        };
        let mut entries = self.entries.lock().expect("checkout leak tracker poisoned");
        entries.insert(
            checkout.id,
            CheckoutLeakEntry {
                checkout,
                started_at,
                suspected: false,
            },
        );
        checkout
    }

    fn scan(&self, now: Instant, suspect_after: StdDuration) -> Vec<CheckoutLeak> {
        let mut entries = self.entries.lock().expect("checkout leak tracker poisoned");
        let mut newly_suspected = Vec::new();
        for entry in entries.values_mut() {
            if !entry.suspected && now.duration_since(entry.started_at) >= suspect_after {
                entry.suspected = true;
                newly_suspected.push(entry.checkout);
            }
        }
        newly_suspected
    }

    fn complete(&self, checkout: CheckoutLeak) {
        let removed = {
            let mut entries = self.entries.lock().expect("checkout leak tracker poisoned");
            entries.remove(&checkout.id)
        };
        let Some(entry) = removed else {
            return;
        };
        if entry.suspected {
            metrics::resolve_dsql_connection_leak_suspect(
                db_class_label(entry.checkout.class),
                entry.checkout.call_site,
                entry.started_at.elapsed(),
            );
        }
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.entries
            .lock()
            .expect("checkout leak tracker poisoned")
            .len()
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
    /// Mode-specific return and slot-accounting path. The distributed variant
    /// retains the original DynamoDB slot manager; embedded uses only local state.
    reservoir_return: PermitReturn,
    /// Shared in-flight counter owned by the director.
    director_in_flight: Arc<AtomicUsize>,
    in_flight_changed: Option<Arc<Notify>>,
    leak_tracker: Arc<CheckoutLeakTracker>,
    leak_checkout: CheckoutLeak,
    /// Caller-set flag that causes return processing to discard the connection.
    marked_bad: bool,
    /// Managed embedded owner admission held for the checkout lifetime.
    _ownership_guard: Option<OwnershipAdmissionPermit>,
}

#[derive(Debug)]
enum PermitReturn {
    Distributed {
        sender: tokio::sync::mpsc::UnboundedSender<ReturnedConnection>,
        slot_manager: Arc<super::SlotBlockManager>,
    },
    Embedded {
        sender: tokio::sync::mpsc::UnboundedSender<embedded_reservoir::ReturnedConnection>,
        coordinator: Arc<dyn EmbeddedConnectionCoordinator>,
    },
}

impl PermitReturn {
    fn release_slot(&self) {
        match self {
            Self::Distributed { slot_manager, .. } => slot_manager.release_slot(),
            Self::Embedded { coordinator, .. } => coordinator.release_slot(),
        }
    }
}

impl DsqlPermit {
    fn new(
        class: DbClass,
        entry: ReservoirEntry,
        class_guard: OwnedSemaphorePermit,
        reservoir_return: tokio::sync::mpsc::UnboundedSender<ReturnedConnection>,
        slot_manager: Arc<super::SlotBlockManager>,
        director_in_flight: Arc<AtomicUsize>,
        leak_tracker: Arc<CheckoutLeakTracker>,
        leak_checkout: CheckoutLeak,
    ) -> Self {
        Self {
            class,
            connection: Some(entry.connection),
            created_at: entry.created_at,
            max_lifetime: entry.max_lifetime,
            _class_guard: class_guard,
            reservoir_return: PermitReturn::Distributed {
                sender: reservoir_return,
                slot_manager,
            },
            director_in_flight,
            in_flight_changed: None,
            leak_tracker,
            leak_checkout,
            marked_bad: false,
            _ownership_guard: None,
        }
    }

    fn new_embedded(
        class: DbClass,
        entry: embedded_reservoir::ReservoirEntry,
        class_guard: OwnedSemaphorePermit,
        reservoir_return: tokio::sync::mpsc::UnboundedSender<
            embedded_reservoir::ReturnedConnection,
        >,
        coordinator: Arc<dyn EmbeddedConnectionCoordinator>,
        director_in_flight: Arc<AtomicUsize>,
        in_flight_changed: Arc<Notify>,
        leak_tracker: Arc<CheckoutLeakTracker>,
        leak_checkout: CheckoutLeak,
        ownership_guard: Option<OwnershipAdmissionPermit>,
    ) -> Self {
        Self {
            class,
            connection: Some(entry.connection),
            created_at: entry.created_at,
            max_lifetime: entry.max_lifetime,
            _class_guard: class_guard,
            reservoir_return: PermitReturn::Embedded {
                sender: reservoir_return,
                coordinator,
            },
            director_in_flight,
            in_flight_changed: Some(in_flight_changed),
            leak_tracker,
            leak_checkout,
            marked_bad: false,
            _ownership_guard: ownership_guard,
        }
    }

    pub fn connection(&mut self) -> Result<&mut PgConnection> {
        let Some(connection) = self.connection.as_mut() else {
            bail!("DSQL permit connection already returned");
        };
        Ok(&mut *connection)
    }

    pub fn mark_bad(&mut self) {
        // Consumers call this when SQL usage indicates the connection should
        // not be reused. The actual discard happens in `Drop`/return
        // processing so callers keep a simple RAII API.
        self.marked_bad = true;
    }
}

impl Drop for DsqlPermit {
    fn drop(&mut self) {
        let previous = self.director_in_flight.fetch_sub(1, Ordering::AcqRel);
        metrics::set_dsql_reservoir_in_flight(previous.saturating_sub(1));
        if let Some(changed) = &self.in_flight_changed {
            changed.notify_waiters();
        }
        self.leak_tracker.complete(self.leak_checkout);
        // Dropping the permit is the storage-layer "return connection" API.
        // Expired connections are intentionally discarded here because handing
        // them back to the ready pool would create rare mid-transaction expiry
        // failures that are hard to diagnose.
        if let Some(connection) = self.connection.take() {
            if self.created_at.elapsed() <= self.max_lifetime {
                let return_failed = match &self.reservoir_return {
                    PermitReturn::Distributed { sender, .. } => sender
                        .send(ReturnedConnection {
                            entry: ReservoirEntry {
                                connection,
                                created_at: self.created_at,
                                max_lifetime: self.max_lifetime,
                            },
                            marked_bad: self.marked_bad,
                        })
                        .is_err(),
                    PermitReturn::Embedded { sender, .. } => sender
                        .send(embedded_reservoir::ReturnedConnection {
                            entry: embedded_reservoir::ReservoirEntry {
                                connection,
                                created_at: self.created_at,
                                max_lifetime: self.max_lifetime,
                            },
                            marked_bad: self.marked_bad,
                        })
                        .is_err(),
                };
                if return_failed {
                    self.reservoir_return.release_slot();
                    metrics::record_dsql_pool_connection_retired("return_channel_closed");
                }
            } else {
                metrics::record_dsql_reservoir_connection_age("expired", self.created_at.elapsed());
                metrics::record_dsql_pool_connection_retired("expired");
                self.reservoir_return.release_slot();
            }
        }
    }
}

fn spawn_periodic_reporter(
    class_budgets: Arc<ClassBudgets>,
    reservoir: Arc<ReservoirController>,
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
            metrics::set_dsql_reservoir_ready_connections(ready);
            // The live target, not the configured one: after a controller
            // budget cap the two differ, and re-reporting the configured value
            // every tick would make the gauge flap against the cap.
            metrics::set_dsql_reservoir_target_connections(reservoir.target_ready());
            metrics::set_dsql_reservoir_in_flight(in_flight);
            metrics::set_dsql_reservoir_utilization_ratio(in_flight, ready);
            let total = ready + in_flight;
            if total > 0 && in_flight as f64 / total as f64 > 0.8 {
                tracing::warn!(
                    ready_connections = ready,
                    in_flight_connections = in_flight,
                    "DSQL reservoir utilization is above 80%"
                );
            }
        }
    })
}

fn spawn_leak_detector(leak_tracker: Arc<CheckoutLeakTracker>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(LEAK_SCAN_INTERVAL);
        loop {
            interval.tick().await;
            for checkout in leak_tracker.scan(Instant::now(), LEAK_SUSPECT_AFTER) {
                metrics::record_dsql_connection_leak_detected(
                    db_class_label(checkout.class),
                    checkout.call_site,
                );
                tracing::warn!(
                    class = db_class_label(checkout.class).as_str(),
                    call_site = checkout.call_site,
                    suspect_after_seconds = LEAK_SUSPECT_AFTER.as_secs(),
                    "DSQL connection checkout exceeded leak suspicion deadline"
                );
            }
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

fn checkout_call_site(class: DbClass) -> &'static str {
    match class {
        DbClass::Control => "db_class_control",
        DbClass::Commit => "db_class_commit",
        DbClass::Read => "db_class_read",
        DbClass::Projection => "db_class_projection",
        DbClass::Maintenance => "db_class_maintenance",
    }
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
    // Allocate one permit to every class first, then distribute the remaining
    // capacity by priority. This preserves liveness for maintenance/control
    // while biasing the pool toward commit throughput.
    let remaining = total - 5;
    let control = 1 + remaining / 10;
    let commit = 1 + remaining / 2;
    let read = 1 + remaining / 5;
    let projection = 1 + remaining / 10;
    let allocated = control + commit + read + projection;
    let maintenance = total - allocated;

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

fn db_class_label(class: DbClass) -> DbClassLabel {
    match class {
        DbClass::Control => DbClassLabel::Control,
        DbClass::Commit => DbClassLabel::Commit,
        DbClass::Read => DbClassLabel::Read,
        DbClass::Projection => DbClassLabel::Projection,
        DbClass::Maintenance => DbClassLabel::Maintenance,
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
    use std::{
        future::Future,
        task::{Context, Waker},
    };

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

    #[tokio::test]
    async fn class_budget_exhaustion_is_class_local() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let _commit = budgets.acquire(DbClass::Commit).await.unwrap();

        assert_eq!(budgets.class_available(DbClass::Commit).await, Some(0));
        assert_eq!(budgets.class_available(DbClass::Read).await, Some(1));
        let _read = budgets.acquire(DbClass::Read).await.unwrap();
        assert_eq!(budgets.class_available(DbClass::Read).await, Some(0));
    }

    #[test]
    fn class_pressure_requires_a_current_five_second_wait() {
        let start = Instant::now();
        let mut pressure = ClassPressure::default();
        for poll in 0..100 {
            let now = start + StdDuration::from_secs(poll);
            pressure.register(now);
            assert!(!pressure.observe(now + StdDuration::from_millis(999)).warn);
            pressure.complete(now);
        }
        let now = start + StdDuration::from_secs(100);
        assert!(!pressure.observe(now).warn);
        pressure.register(now);
        assert!(!pressure.observe(now + StdDuration::from_millis(4_999)).warn);
        let sample = pressure.observe(now + StdDuration::from_secs(5));
        assert!(sample.warn);
        assert_eq!(sample.waiters, 1);
        assert_eq!(sample.longest_wait, StdDuration::from_secs(5));
    }

    #[test]
    fn class_pressure_cooldown_survives_recovery() {
        let start = Instant::now();
        let mut pressure = ClassPressure::default();
        pressure.register(start);
        assert!(pressure.observe(start + StdDuration::from_secs(5)).warn);
        pressure.complete(start);
        let recovered = pressure.observe(start + StdDuration::from_secs(6));
        assert_eq!(recovered.waiters, 0);
        assert_eq!(recovered.longest_wait, StdDuration::ZERO);
        assert!(!recovered.warn);

        let restarted = start + StdDuration::from_secs(7);
        pressure.register(restarted);
        for millis in [12_000, 20_000, 40_000, 64_999] {
            assert!(
                !pressure
                    .observe(start + StdDuration::from_millis(millis))
                    .warn
            );
        }
        assert!(pressure.observe(start + StdDuration::from_secs(65)).warn);
        assert!(!pressure.observe(start + StdDuration::from_secs(65)).warn);
        assert!(pressure.observe(start + StdDuration::from_secs(125)).warn);
    }

    #[test]
    fn class_pressure_tracks_equal_start_times_and_oldest_waiter_departure() {
        let start = Instant::now();
        let later = start + StdDuration::from_secs(4);
        let mut pressure = ClassPressure::default();
        pressure.register(start);
        pressure.register(start);
        pressure.register(later);
        pressure.complete(start);
        assert_eq!(pressure.observe(later).waiters, 2);
        pressure.complete(start);
        let sample = pressure.observe(start + StdDuration::from_secs(5));
        assert_eq!(sample.waiters, 1);
        assert_eq!(sample.longest_wait, StdDuration::from_secs(1));
        assert!(!sample.warn);
        assert!(pressure.observe(start + StdDuration::from_secs(9)).warn);
    }

    #[tokio::test]
    async fn class_budget_reports_sustained_waits_and_recovers_after_acquisition() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let holder = budgets.acquire(DbClass::Projection).await.unwrap();
        budgets
            .record_metrics_at(Instant::now() + StdDuration::from_secs(120))
            .await;
        assert_eq!(
            budgets.pressure[&DbClass::Projection]
                .lock()
                .unwrap()
                .last_warning,
            None
        );

        let mut waiter = Box::pin(budgets.acquire(DbClass::Projection));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let started = *budgets.pressure[&DbClass::Projection]
            .lock()
            .unwrap()
            .waiting
            .first_key_value()
            .unwrap()
            .0;
        budgets
            .record_metrics_at(started + StdDuration::from_millis(4_999))
            .await;
        assert_eq!(
            budgets.pressure[&DbClass::Projection]
                .lock()
                .unwrap()
                .last_warning,
            None
        );
        for seconds in [5, 65, 125] {
            let now = started + StdDuration::from_secs(seconds);
            budgets.record_metrics_at(now).await;
            assert_eq!(
                budgets.pressure[&DbClass::Projection]
                    .lock()
                    .unwrap()
                    .last_warning,
                Some(now)
            );
            budgets
                .record_metrics_at(now + StdDuration::from_secs(59))
                .await;
            assert_eq!(
                budgets.pressure[&DbClass::Projection]
                    .lock()
                    .unwrap()
                    .last_warning,
                Some(now)
            );
        }

        drop(holder);
        let _acquired = waiter.await.unwrap();
        budgets
            .record_metrics_at(started + StdDuration::from_secs(200))
            .await;
        let pressure = budgets.pressure[&DbClass::Projection].lock().unwrap();
        assert!(pressure.waiting.is_empty());
        assert_eq!(
            pressure.last_warning,
            Some(started + StdDuration::from_secs(125))
        );
    }

    #[tokio::test]
    async fn class_budget_cancelled_and_failed_acquisitions_clear_pressure() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let _holder = budgets.acquire(DbClass::Commit).await.unwrap();
        let mut cancelled = Box::pin(budgets.acquire(DbClass::Commit));
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(
            budgets.pressure[&DbClass::Commit]
                .lock()
                .unwrap()
                .waiting
                .len(),
            1
        );
        drop(cancelled);
        assert!(
            budgets.pressure[&DbClass::Commit]
                .lock()
                .unwrap()
                .waiting
                .is_empty()
        );

        let mut closed = Box::pin(budgets.acquire(DbClass::Commit));
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        budgets.close().await;
        assert!(closed.await.is_err());
        budgets
            .record_metrics_at(Instant::now() + StdDuration::from_secs(120))
            .await;
        let pressure = budgets.pressure[&DbClass::Commit].lock().unwrap();
        assert!(pressure.waiting.is_empty());
        assert_eq!(pressure.last_warning, None);
    }

    #[tokio::test]
    async fn class_budget_reconfiguration_preserves_waiters_and_cooldown() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let holder = budgets.acquire(DbClass::Commit).await.unwrap();
        let mut waiter = Box::pin(budgets.acquire(DbClass::Commit));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let started = *budgets.pressure[&DbClass::Commit]
            .lock()
            .unwrap()
            .waiting
            .first_key_value()
            .unwrap()
            .0;
        let warned_at = started + StdDuration::from_secs(5);
        budgets.record_metrics_at(warned_at).await;
        budgets.reconfigure(&test_allocations(3)).await.unwrap();
        let _new_generation = budgets.acquire(DbClass::Commit).await.unwrap();
        budgets
            .record_metrics_at(warned_at + StdDuration::from_secs(59))
            .await;
        {
            let pressure = budgets.pressure[&DbClass::Commit].lock().unwrap();
            assert_eq!(pressure.waiting.len(), 1);
            assert_eq!(pressure.last_warning, Some(warned_at));
        }
        drop(holder);
        let _old_generation = waiter.await.unwrap();
        assert!(
            budgets.pressure[&DbClass::Commit]
                .lock()
                .unwrap()
                .waiting
                .is_empty()
        );
    }

    #[test]
    fn class_pressure_warning_reservation_is_atomic_and_class_local() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let start = Instant::now();
        let now = start + StdDuration::from_secs(5);
        let commit = &budgets.pressure[&DbClass::Commit];
        commit.lock().unwrap().register(start);
        let warnings = std::thread::scope(|scope| {
            let observers: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| commit.lock().unwrap().observe(now).warn))
                .collect();
            observers
                .into_iter()
                .map(|observer| observer.join().unwrap())
                .filter(|warn| *warn)
                .count()
        });
        assert_eq!(warnings, 1);
        let mut read = budgets.pressure[&DbClass::Read].lock().unwrap();
        read.register(start);
        assert!(read.observe(now).warn);
    }

    #[tokio::test]
    async fn dsql_permit_mark_bad_and_connection_error_paths_are_stable() {
        let budgets = ClassBudgets::new(&test_allocations(1)).unwrap();
        let class_guard = budgets.acquire(DbClass::Commit).await.unwrap();
        let (return_tx, _return_rx) = tokio::sync::mpsc::unbounded_channel();
        let in_flight = Arc::new(AtomicUsize::new(1));
        let leak_tracker = Arc::new(CheckoutLeakTracker::default());
        let leak_checkout = leak_tracker.track(DbClass::Commit, Instant::now());
        let mut permit = DsqlPermit {
            class: DbClass::Commit,
            connection: None,
            created_at: std::time::Instant::now(),
            max_lifetime: std::time::Duration::from_secs(60),
            _class_guard: class_guard,
            reservoir_return: PermitReturn::Distributed {
                sender: return_tx,
                slot_manager: crate::dsql::SlotBlockManager::local_for_tests(1),
            },
            director_in_flight: Arc::clone(&in_flight),
            in_flight_changed: None,
            leak_tracker: Arc::clone(&leak_tracker),
            leak_checkout,
            marked_bad: false,
            _ownership_guard: None,
        };

        assert!(permit.connection().is_err());
        permit.mark_bad();
        assert!(permit.marked_bad);
        drop(permit);
        assert_eq!(in_flight.load(Ordering::Acquire), 0);
        assert_eq!(leak_tracker.active_count(), 0);
    }

    fn distributed_director(target_ready: usize) -> DsqlConnectionDirector {
        let config = DsqlPoolConfig {
            reservoir: ReservoirConfig {
                target_ready,
                ..ReservoirConfig::default()
            },
            ..DsqlPoolConfig::default()
        };
        let reservoir = Reservoir::idle_for_tests(config.reservoir.clone());
        DsqlConnectionDirector::start(config, reservoir).unwrap()
    }

    #[tokio::test]
    async fn connection_budget_caps_distributed_reservoir_target() {
        let director = distributed_director(8);
        assert_eq!(director.reservoir_target(), 8);

        assert_eq!(director.apply_connection_budget(3).unwrap(), 0);
        assert_eq!(director.reservoir_target(), 3);
        assert_eq!(director.ready_connections(), 0);
    }

    #[tokio::test]
    async fn connection_budget_never_raises_target_above_configuration() {
        let director = distributed_director(8);

        director.apply_connection_budget(3).unwrap();
        director.apply_connection_budget(100).unwrap();
        assert_eq!(director.reservoir_target(), 8);

        // The expiry reset a runtime applies after a dropped controller
        // stream: a share of one keeps the node alive at the floor.
        director.apply_connection_budget(0).unwrap();
        assert_eq!(director.reservoir_target(), 1);
    }

    #[test]
    fn checkout_leak_tracker_marks_and_resolves_bounded_call_sites() {
        let tracker = CheckoutLeakTracker::default();
        let started_at = Instant::now() - StdDuration::from_secs(60);
        let checkout = tracker.track(DbClass::Projection, started_at);

        let suspected = tracker.scan(Instant::now(), StdDuration::from_secs(30));

        assert_eq!(suspected.len(), 1);
        assert_eq!(suspected[0].call_site, "db_class_projection");
        tracker.complete(checkout);
        assert_eq!(tracker.active_count(), 0);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: observability-production, Property 4: pressure eligibility and cooldown survive waiter lifecycles.
        #[test]
        fn class_pressure_matches_waiter_lifecycle_model(
            events in prop::collection::vec((0u8..3, 0u64..70_000, any::<usize>()), 1..256),
        ) {
            let start = Instant::now();
            let mut pressure = ClassPressure::default();
            let mut pending = Vec::new();
            let mut elapsed = 0u64;
            let mut last_warning = None;
            for (action, advance_ms, index) in events {
                elapsed += advance_ms;
                let now = start + StdDuration::from_millis(elapsed);
                match action {
                    0 => {
                        pressure.register(now);
                        pending.push(elapsed);
                    }
                    1 if !pending.is_empty() => {
                        let finished = pending.swap_remove(index % pending.len());
                        pressure.complete(start + StdDuration::from_millis(finished));
                    }
                    _ => {}
                }
                let expected_wait = pending.iter().map(|started| elapsed - started).max().unwrap_or(0);
                let expected_warning = expected_wait >= 5_000
                    && last_warning.is_none_or(|previous| elapsed - previous >= 60_000);
                let sample = pressure.observe(now);
                prop_assert_eq!(sample.waiters, pending.len());
                prop_assert_eq!(sample.longest_wait, StdDuration::from_millis(expected_wait));
                prop_assert_eq!(sample.warn, expected_warning);
                if sample.warn {
                    last_warning = Some(elapsed);
                }
            }
        }
    }

    proptest! {
        #[test]
        fn class_budget_sum_invariant(commit in 1usize..128) {
            let allocations = test_allocations(commit);
            let expected: usize = allocations.values().sum();
            let budgets = ClassBudgets::new(&allocations).unwrap();
            prop_assert_eq!(budgets.total_budget(), expected);
        }

        #[test]
        fn default_class_budget_sum_invariant(target_ready in 5usize..500) {
            let config = ReservoirConfig {
                target_ready,
                ..ReservoirConfig::default()
            };
            let allocations = default_allocations(&config);
            prop_assert_eq!(allocations.values().sum::<usize>(), target_ready);
            prop_assert!(allocations.values().all(|permits| *permits >= 1));
        }
    }
}
