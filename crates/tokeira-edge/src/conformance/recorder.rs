//! In-memory wire-coverage recorder for Tier-2 functional conformance.
//!
//! Tier 2 runs Temporal's own functional Go suite, unmodified, over the real gRPC
//! wire against a running `tokeirad` (see `.kiro/specs/temporal-functional-conformance`).
//! While that suite runs, the edge observes every `(wire_method, status_code)` pair it
//! serves so the run can be joined against the compatibility matrix and turned into an
//! interpretable coverage report. This module owns the *live aggregation* of those
//! observations — the recorder — and materializes them into the stable on-disk shape
//! owned by [`super::record`].
//!
//! ## Why aggregate live rather than log every call
//!
//! The report only needs *how many times* each distinct `(wire_method, status_code)`
//! pair occurred, not the per-call timeline. Aggregating into a counter at record time
//! keeps memory bounded by the number of distinct pairs (small — the public surface is a
//! few dozen RPCs × a handful of status codes) regardless of how many calls the suite
//! drives, and makes [`WireCoverageRecorder::snapshot`] a direct projection into
//! [`WireCoverageRecord`] with no post-processing. The atomic unit the report consumes is
//! exactly `(wire_method, status_code, count)`, so the recorder's internal counter is
//! keyed by `(wire_method, status_code)` and values are the count.
//!
//! ## Why `Mutex<HashMap>` and not a lock-free / sharded map
//!
//! The recorder is only ever live under the conformance flag during a Tier-2 run — never
//! on a production hot path (the zero-overhead-when-off invariant is upheld one layer up,
//! in the `WireCoverageLayer` tower layer, which is mounted on the gRPC server only when a
//! conformance run constructs a recorder; production never installs the layer, so the
//! recorder is never invoked). A run drives at most a modest call rate against an
//! in-memory `tokeirad`, so a single `std::sync::Mutex` guarding a `HashMap` is more than
//! fast enough and adds no new dependency. A concurrent map (`DashMap`) would buy nothing
//! here and is not a current dependency of this crate, so it is deliberately not
//! introduced (AGENTS.md §1: every dependency must earn its place). The recorder is shared
//! as `Arc<WireCoverageRecorder>` so every served call increments the same counter.
//!
//! ## Why the snapshot is deterministically ordered
//!
//! A `HashMap` yields its entries in an unspecified, run-to-run-varying order.
//! [`WireCoverageRecord`] is serialized as evidence and compared across runs, so
//! [`WireCoverageRecorder::snapshot`] sorts the rows by `(wire_method, status_code)`
//! before materializing them. This makes the emitted record a stable function of the
//! observed counts alone — two runs that observed the same multiset of calls produce
//! byte-identical evidence — which is what lets the report (and any diff of the evidence
//! file) treat the output as canonical.

use std::{collections::HashMap, sync::Mutex};

use super::record::{WireCoverageRecord, WireCoverageRow};

/// Live, thread-safe aggregator of `(wire_method, status_code)` observations over a
/// single Tier-2 run.
///
/// The recorder is the behavioural counterpart to the [`WireCoverageRecord`] data model:
/// it accumulates counts as the suite runs and materializes them into that record on
/// demand. It is intended to be held as `Arc<WireCoverageRecorder>` and shared across
/// every edge service handler so all calls feed one counter.
///
/// Counts are keyed by `(wire_method, status_code)`: a method served with two distinct
/// status codes accumulates into two separate keys, which is exactly what the report
/// needs to decide whether the observed outcomes agree with the matrix claim.
#[derive(Debug, Default)]
pub struct WireCoverageRecorder {
    /// Occurrence counts keyed by the observed `(wire_method, status_code)` pair.
    ///
    /// Guarded by a `Mutex` because every edge handler may record concurrently; see the
    /// module docs for why a plain mutex is the right tradeoff for a conformance-only,
    /// never-production-hot-path recorder.
    counts: Mutex<HashMap<(String, i32), u64>>,
}

impl WireCoverageRecorder {
    /// Create an empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one served call: increment the count for `(wire_method, status_code)`.
    ///
    /// `wire_method` is the gRPC path as observed on the wire
    /// (`/package.Service/Method`) and `status_code` is `tonic::Code as i32`; both are
    /// carried verbatim into the snapshot so the report can resolve and compare them
    /// without re-parsing (see [`WireCoverageRow`]). The owned key is materialized via
    /// the entry API; this is a conformance-only path, never a production hot path, so
    /// the per-call key allocation is deliberately not optimized away (a tuple-keyed
    /// `HashMap` cannot be probed by `(&str, i32)` without a custom `Borrow`, which would
    /// be more machinery than this off-by-default recorder warrants).
    pub fn record(&self, wire_method: &str, status_code: i32) {
        // A poisoned lock means a previous holder panicked while mutating the map.
        // Recover the guard rather than propagating the panic: a corrupt count is a
        // harmless loss of conformance *evidence*, never a correctness concern, and the
        // recorder must not be able to take down a request path it is only observing.
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        *counts
            .entry((wire_method.to_owned(), status_code))
            .or_insert(0) += 1;
    }

    /// Materialize the current counts into a [`WireCoverageRecord`].
    ///
    /// Rows are sorted by `(wire_method, status_code)` so the output is a stable function
    /// of the observed counts alone — see the module docs on why deterministic ordering
    /// matters for evidence. Snapshotting does not reset the counter; a recorder may be
    /// snapshotted more than once over its life.
    pub fn snapshot(&self) -> WireCoverageRecord {
        let counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut rows: Vec<WireCoverageRow> = counts
            .iter()
            .map(|((wire_method, status_code), count)| WireCoverageRow {
                wire_method: wire_method.clone(),
                status_code: *status_code,
                count: *count,
            })
            .collect();

        rows.sort_by(|a, b| {
            a.wire_method
                .cmp(&b.wire_method)
                .then(a.status_code.cmp(&b.status_code))
        });

        WireCoverageRecord { rows }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: &str = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution";
    const DESCRIBE: &str =
        "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution";

    #[test]
    fn record_aggregates_repeated_pairs_into_one_row() {
        let recorder = WireCoverageRecorder::new();
        recorder.record(START, 0);
        recorder.record(START, 0);
        recorder.record(START, 0);

        let record = recorder.snapshot();

        assert_eq!(record.rows.len(), 1);
        assert_eq!(record.rows[0].wire_method, START);
        assert_eq!(record.rows[0].status_code, 0);
        assert_eq!(record.rows[0].count, 3);
    }

    #[test]
    fn distinct_status_codes_for_same_method_are_distinct_rows() {
        let recorder = WireCoverageRecorder::new();
        recorder.record(DESCRIBE, 0);
        recorder.record(DESCRIBE, 5);
        recorder.record(DESCRIBE, 5);

        let record = recorder.snapshot();

        assert_eq!(record.rows.len(), 2);
        // Sorted by (wire_method, status_code), so status 0 precedes status 5.
        assert_eq!(record.rows[0].status_code, 0);
        assert_eq!(record.rows[0].count, 1);
        assert_eq!(record.rows[1].status_code, 5);
        assert_eq!(record.rows[1].count, 2);
    }

    #[test]
    fn snapshot_is_deterministically_ordered_regardless_of_insertion_order() {
        // Insert in a deliberately non-sorted order; the snapshot must still come back
        // sorted by (wire_method, status_code) so the evidence is canonical.
        let recorder = WireCoverageRecorder::new();
        recorder.record(START, 5);
        recorder.record(DESCRIBE, 5);
        recorder.record(START, 0);
        recorder.record(DESCRIBE, 0);

        let record = recorder.snapshot();
        let keys: Vec<(String, i32)> = record
            .rows
            .iter()
            .map(|row| (row.wire_method.clone(), row.status_code))
            .collect();

        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(keys, expected);
    }

    #[test]
    fn snapshot_does_not_reset_counts() {
        let recorder = WireCoverageRecorder::new();
        recorder.record(START, 0);

        let first = recorder.snapshot();
        let second = recorder.snapshot();

        assert_eq!(first, second);
        assert_eq!(second.rows[0].count, 1);
    }

    #[test]
    fn empty_recorder_snapshots_to_no_rows() {
        let recorder = WireCoverageRecorder::new();
        assert!(recorder.snapshot().rows.is_empty());
    }
}
