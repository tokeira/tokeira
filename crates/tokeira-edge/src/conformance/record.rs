//! Wire-coverage record data model for Tier-2 functional conformance.
//!
//! Tier 2 runs Temporal's own functional Go suite, unmodified, over the real gRPC
//! wire against a running `tokeirad` (see `.kiro/specs/temporal-functional-conformance`).
//! While that suite runs, a wire-coverage recorder in the edge observes every
//! `(wire_method, status_code)` pair served, so the run can be joined against the
//! compatibility matrix and turned into an interpretable coverage report rather than a
//! raw status dump. This module owns the persistent shape of those observations — the
//! wire-coverage record — and nothing else.
//!
//! ## Boundary
//!
//! This is the *data model only*. It deliberately contains no recorder logic (the
//! interceptor extension that populates these records is task 2.x) and no report join
//! against the compatibility matrix (task 9.x). Keeping the model free of behaviour
//! lets the recorder, the JSON evidence on disk, and the Rust report all agree on one
//! schema without sharing code.
//!
//! Downstream, each [`WireCoverageRow`] is resolved through
//! `tokeira_compatibility::coverage::resolve(wire_path, dynamic_config, namespace)` —
//! the single tested join between an observed wire method and its matrix
//! `RpcClassification` — so the report does not re-implement wire↔matrix matching. The
//! record here is purely the observation `(method, status, count)`; all classification
//! (`agrees` / `contradicts` / `uncovered` / `unknown-to-matrix`) is a property of the
//! report, never baked into the record.

use serde::{Deserialize, Serialize};

/// One observed `(wire_method, status_code)` pair and how many times it occurred over a run.
///
/// This is the atomic unit of wire coverage: the recorder increments `count` each time
/// the edge serves a call to `wire_method` that completes with `status_code`. A single
/// method appears in multiple rows when it was served with multiple distinct status
/// codes over the run (e.g. a method that returned `OK` for some calls and `NOT_FOUND`
/// for others yields two rows), which is exactly what the report needs to decide whether
/// the observed outcomes agree with the matrix claim.
///
/// `wire_method` is the gRPC path as observed on the wire (`/package.Service/Method`); it
/// is carried verbatim so it can be fed straight to
/// `tokeira_compatibility::coverage::resolve`, which is total over arbitrary path strings
/// and reports any path it cannot classify as `unknown-to-matrix` rather than dropping it.
///
/// `status_code` is stored as the raw `i32` produced by `tonic::Code as i32`, not as a
/// typed enum. The rationale is fidelity over prettiness: the record is wire evidence,
/// and the recorder must faithfully serialize whatever code the `EdgeError → tonic::Status`
/// mapping produced — including a code outside the set `tonic::Code` currently models —
/// without lossy normalization or a fallible conversion at record time. `i32` is also the
/// canonical gRPC `status` representation (`tonic::Code: From<i32>` / `Into<i32>`), so a
/// consumer wanting the typed code can recover it losslessly, while a consumer comparing
/// against the matrix's expected outcome can compare codes directly. Keeping it `i32`
/// avoids coupling the on-disk evidence format to the exact variant set of a third-party
/// enum at the version we happen to build against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCoverageRow {
    /// The gRPC wire path served, as observed (`/package.Service/Method`). Carried
    /// verbatim so it can be resolved through `tokeira_compatibility::coverage::resolve`
    /// without pre-parsing.
    pub wire_method: String,

    /// The gRPC status code the call completed with, as `tonic::Code as i32`. See the
    /// type-level doc for why this is a raw `i32` rather than a typed enum.
    pub status_code: i32,

    /// How many times `(wire_method, status_code)` was observed over the run.
    pub count: u64,
}

/// The aggregate wire-coverage evidence the recorder emits over a single Tier-2 run.
///
/// This is what gets serialized at the end of a conformance run and handed to the report
/// generator (task 9.x), which resolves each [`WireCoverageRow`] through
/// `tokeira_compatibility::coverage::resolve` and marks it `agrees` / `contradicts` /
/// `uncovered` / `unknown-to-matrix` against the compatibility matrix.
///
/// The record is intentionally minimal: the rows *are* the essence of the evidence, so
/// this type is a thin container around them rather than a place for run bookkeeping.
/// Run-level metadata (pin versions, the per-test ledger) is owned by other Tier-2
/// artifacts (the pin gate, [`super::ledger`]) and joined in the report, not duplicated
/// here — keeping the wire-coverage record a clean, stable evidence shape with a single
/// responsibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCoverageRecord {
    /// Every distinct `(wire_method, status_code)` observed over the run, with its
    /// occurrence count. Each row is resolved independently through the compatibility
    /// surface in the report.
    pub rows: Vec<WireCoverageRow>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_coverage_record_round_trips_through_json() {
        let record = WireCoverageRecord {
            rows: vec![
                WireCoverageRow {
                    wire_method:
                        "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
                            .to_owned(),
                    status_code: 0,
                    count: 3,
                },
                WireCoverageRow {
                    wire_method:
                        "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution"
                            .to_owned(),
                    status_code: 5,
                    count: 1,
                },
            ],
        };

        let json = serde_json::to_string(&record).expect("record serializes to JSON");
        let decoded: WireCoverageRecord =
            serde_json::from_str(&json).expect("record deserializes from JSON");

        assert_eq!(record, decoded);
    }

    #[test]
    fn rows_with_distinct_status_codes_are_distinct() {
        let ok = WireCoverageRow {
            wire_method: "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
                .to_owned(),
            status_code: 0,
            count: 1,
        };
        let not_found = WireCoverageRow {
            status_code: 5,
            ..ok.clone()
        };

        assert_ne!(ok, not_found);
    }
}
