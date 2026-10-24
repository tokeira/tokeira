//! Test-only attribution of logical run loads and their physical SQL statements.
//!
//! The observer is keyed by run, so a test's attempt ID follows that run across
//! RPC and lane task boundaries without adding a wire field. Tests register one
//! attempt at a time per run. The ordinary build has no registry or counters;
//! scopes then only await their original future.

use std::future::Future;
use tokeira_types::RunKey;

/// Attribute reads in this future to a start phase or excluded hydration phase.
pub async fn scope<T>(purpose: &'static str, future: impl Future<Output = T>) -> T {
    #[cfg(feature = "test-support")]
    {
        enabled::PURPOSE.scope(purpose, future).await
    }
    #[cfg(not(feature = "test-support"))]
    {
        let _ = purpose;
        future.await
    }
}

pub(crate) fn logical(run_key: RunKey) {
    record(run_key, "logical");
}

#[cfg(feature = "dsql")]
pub(crate) fn sql(run_key: RunKey, statement: &'static str) {
    record(run_key, statement);
}

fn record(run_key: RunKey, kind: &'static str) {
    #[cfg(feature = "test-support")]
    enabled::record(run_key, kind);
    #[cfg(not(feature = "test-support"))]
    let _ = (run_key, kind);
}

// The injected failure models a rejected OCC attempt, not DSQL's competing
// writer behaviour (which has separate live fence contracts).
pub(crate) fn reject_commit(run_key: RunKey) -> bool {
    #[cfg(feature = "test-support")]
    {
        enabled::reject_commit(run_key)
    }
    #[cfg(not(feature = "test-support"))]
    {
        let _ = run_key;
        false
    }
}

#[cfg(feature = "test-support")]
pub use enabled::{ReadProbe, ReadReport};

#[cfg(feature = "test-support")]
mod enabled {
    use super::*;
    use std::{
        collections::{BTreeMap, HashMap},
        sync::{Arc, LazyLock, Mutex},
    };

    tokio::task_local! { pub(super) static PURPOSE: &'static str; }
    type Counts = BTreeMap<(&'static str, &'static str), usize>;
    static PROBES: LazyLock<Mutex<HashMap<RunKey, Arc<Mutex<ReadReport>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    static CONFLICTS: LazyLock<Mutex<HashMap<RunKey, usize>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    /// Executed reads for one correlated attempt; SQL and logical counts differ.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ReadReport {
        /// Test-assigned identity, shared by both runtime nodes and lane tasks.
        pub attempt: String,
        /// Counts by phase and operation (`logical` or a SQL statement purpose).
        pub counts: Counts,
    }

    /// Registers one attempt for a run, removing its observer on every exit path.
    #[derive(Debug)]
    pub struct ReadProbe {
        run_key: RunKey,
        report: Arc<Mutex<ReadReport>>,
    }

    impl ReadProbe {
        /// Reject this many commit attempts before they write, to measure the
        /// runtime's mandatory reload path independently from a real OCC race.
        pub fn inject_conflicts(&self, count: usize) {
            CONFLICTS
                .lock()
                .expect("read probe conflicts poisoned")
                .insert(self.run_key, count);
        }
        /// Begin observing. Panics if another attempt already observes this run.
        pub fn start(run_key: RunKey, attempt: impl Into<String>) -> Self {
            let report = Arc::new(Mutex::new(ReadReport {
                attempt: attempt.into(),
                counts: BTreeMap::new(),
            }));
            let mut probes = PROBES.lock().expect("read probe registry poisoned");
            assert!(
                !probes.contains_key(&run_key),
                "one observed attempt per run"
            );
            probes.insert(run_key, report.clone());
            Self { run_key, report }
        }

        /// Snapshot the counts without ending observation.
        pub fn report(&self) -> ReadReport {
            self.report.lock().expect("read probe poisoned").clone()
        }
    }

    impl Drop for ReadProbe {
        fn drop(&mut self) {
            CONFLICTS
                .lock()
                .expect("read probe conflicts poisoned")
                .remove(&self.run_key);
            PROBES
                .lock()
                .expect("read probe registry poisoned")
                .remove(&self.run_key);
        }
    }

    pub(super) fn reject_commit(run_key: RunKey) -> bool {
        let mut conflicts = CONFLICTS.lock().expect("read probe conflicts poisoned");
        if let Some(left) = conflicts.get_mut(&run_key)
            && *left > 0
        {
            *left -= 1;
            record(run_key, "injected-conflict");
            return true;
        }
        false
    }

    pub(super) fn record(run_key: RunKey, kind: &'static str) {
        let report = PROBES
            .lock()
            .expect("read probe registry poisoned")
            .get(&run_key)
            .cloned();
        if let Some(report) = report {
            let purpose = PURPOSE
                .try_with(|purpose| *purpose)
                .unwrap_or("outside-start");
            *report
                .lock()
                .expect("read probe poisoned")
                .counts
                .entry((purpose, kind))
                .or_default() += 1;
        }
    }
}
