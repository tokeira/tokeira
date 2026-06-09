//! Wire-coverage report: the three-way join of observed traffic against the matrix.
//!
//! Tier 2 runs Temporal's functional corpus over the real gRPC wire against
//! `tokeirad`, and the wire-coverage recorder ([`super::record`]) emits, for the run, a
//! [`WireCoverageRecord`] of `(wire_method, status_code, count)` observations. This
//! module turns that raw evidence into an *interpretable* verdict per RPC by joining it
//! against the compatibility matrix through the single tested entry point
//! `tokeira_compatibility::coverage::resolve` — it never re-implements wire↔matrix
//! matching.
//!
//! Each verdict is one of four (Requirements 5.3–5.6):
//!
//! - **Agrees** — the observed status is consistent with what the matrix claims for the
//!   RPC (the RPC is served when the matrix says it is implemented; it answers
//!   `UNIMPLEMENTED` when the matrix says it is stubbed; it answers
//!   `FAILED_PRECONDITION` when the matrix says it is an experimental feature whose gate
//!   is off).
//! - **Contradicts** — the observed status conflicts with the matrix claim (an
//!   "implemented" RPC answered `UNIMPLEMENTED`; a "stubbed" RPC was actually served; a
//!   gated-off feature answered success). A contradiction is a matrix-is-stale signal,
//!   surfaced rather than hidden.
//! - **Uncovered** — the matrix claims the RPC is `Implemented`/`Partial`, but the run
//!   never drove it to success, so the claim is undemonstrated by this run
//!   (Requirement 5.5).
//! - **UnknownToMatrix** — the observed wire path is outside the matrix's vocabulary
//!   (`AdminService`, gRPC `Health`, an unparseable path). Surfaced, never dropped
//!   (Requirement 5.6).
//!
//! ## What this module owns and does not
//!
//! It owns the observation↔matrix join only (task 9.1). It deliberately does **not**
//! join the per-test ledger (task 9.2), derive the out-of-public-scope internal-client
//! surface (task 9.3), or implement the report gates (task 10). Keeping the wire join
//! standalone lets it be tested in isolation against the matrix and reused by the
//! ledger-aware report without entangling the two data sources.
//!
//! ## Verdict is config-aware by construction
//!
//! `resolve` (and therefore this report) takes a `DynamicConfigReader` and namespace
//! because an `Experimental` feature's expected outcome depends on whether its gate is
//! on. The caller must pass the *same* dynamic configuration the observed `tokeirad`
//! ran with, otherwise an experimental feature's verdict is judged against the wrong
//! baseline. This module reads no configuration of its own; it threads the caller's
//! reader straight through to `resolve`.

use serde::{Deserialize, Serialize};
use tonic::Code;

use tokeira_compatibility::{
    FEATURE_MATRIX, FeatureState,
    coverage::{ExpectedOutcome, RpcClassification, resolve},
    dispatch::DynamicConfigReader,
    rpc_id_to_wire_path,
};

use super::record::WireCoverageRecord;

/// The verdict for a single RPC in the joined report.
///
/// The four variants partition every reportable RPC — whether it was observed on the
/// wire or only claimed by the matrix — into exactly one bucket. `Agrees`/`Contradicts`
/// arise from observed traffic the matrix classifies; `Uncovered` arises from a matrix
/// claim no successful observation backs; `UnknownToMatrix` arises from observed traffic
/// the matrix has no claim about. The set is closed and the report assigns precisely one
/// per RPC, so no observation and no claim is ever silently dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WireCoverageVerdict {
    /// Observed status is consistent with the matrix claim for this RPC.
    Agrees,
    /// Observed status conflicts with the matrix claim (matrix-is-stale signal).
    Contradicts,
    /// Matrix claims `Implemented`/`Partial` but the run never drove the RPC to success.
    Uncovered,
    /// Observed wire path is outside the matrix's vocabulary.
    UnknownToMatrix,
}

/// One row of the join keyed to an *observed* wire method.
///
/// Every [`WireCoverageRow`](super::record::WireCoverageRow) in the input record yields
/// exactly one `ObservedVerdict`, so the observed side of the report is total over the
/// recorder's evidence. The matrix detail (`rpc_id`, `feature_id`, `state`, `expected`)
/// is populated only when the path resolves to [`RpcClassification::Known`]; for an
/// `UnknownToMatrix` path those are `None` and the verdict is
/// [`WireCoverageVerdict::UnknownToMatrix`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedVerdict {
    /// The observed gRPC wire path (`/package.Service/Method`), verbatim from the record.
    pub wire_method: String,
    /// The gRPC status the call completed with, as `tonic::Code as i32` (record fidelity).
    pub status_code: i32,
    /// How many times `(wire_method, status_code)` was observed.
    pub count: u64,
    /// The verdict for this observation: `Agrees`, `Contradicts`, or `UnknownToMatrix`.
    /// (`Uncovered` never arises from an observation — it is a property of an
    /// unobserved matrix claim and lives in [`WireCoverageReport::uncovered`].)
    pub verdict: WireCoverageVerdict,
    /// The matrix's dotted rpc id, when the path is `Known`.
    pub rpc_id: Option<String>,
    /// The owning feature id, when the path is `Known`.
    pub feature_id: Option<String>,
    /// The owning feature's declared state, when the path is `Known` (preserves the
    /// `Partial` label as reporting detail).
    pub state: Option<FeatureState>,
    /// The wire outcome the matrix state implied, when the path is `Known`.
    pub expected: Option<ExpectedOutcome>,
}

/// One row of the join keyed to a *matrix-claimed* RPC the run never drove to success.
///
/// An `Uncovered` row means the matrix declares the RPC `Implemented` or `Partial`, but
/// no observation in the record shows it completing with `OK`. The claim is therefore
/// undemonstrated by this run — not contradicted (nothing observed says it is broken),
/// just unbacked. This is the honest "we say we do this but never showed it" signal
/// (Requirement 5.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UncoveredClaim {
    /// The matrix's dotted rpc id that was claimed implemented/partial but never driven.
    pub rpc_id: String,
    /// The owning feature id.
    pub feature_id: String,
    /// The owning feature's declared state (`Implemented` or `Partial`).
    pub state: FeatureState,
}

/// The joined wire-coverage report for one Tier-2 run.
///
/// Two collections, two iteration sources, one join:
/// - [`observed`](Self::observed) — one entry per recorded observation, classified
///   `Agrees` / `Contradicts` / `UnknownToMatrix` against the matrix.
/// - [`uncovered`](Self::uncovered) — one entry per matrix `Implemented`/`Partial` RPC
///   that no observation drove to success.
///
/// Together they cover both directions of the join: "what did we observe, and does it
/// match the claim?" and "what did we claim, but never demonstrate?". The report carries
/// no gate logic (task 10) and no ledger join (task 9.2); it is pure interpreted
/// evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCoverageReport {
    /// One verdict per recorded observation, in the record's row order.
    pub observed: Vec<ObservedVerdict>,
    /// Matrix `Implemented`/`Partial` RPCs never driven to success this run.
    pub uncovered: Vec<UncoveredClaim>,
}

/// Join a wire-coverage record against the matrix into an interpreted report.
///
/// This is task 9.1's entry point. It walks the record's observations once, resolving
/// each through `tokeira_compatibility::coverage::resolve` and assigning a verdict, then
/// scans the matrix once for `Implemented`/`Partial` RPCs that no observation drove to
/// success and records those as `Uncovered`.
///
/// `dynamic_config` and `namespace` must describe the configuration the observed
/// `tokeirad` ran under (see the module docs): they are threaded into `resolve` so an
/// `Experimental` feature's expected outcome is judged against the gate state that was
/// actually in effect.
///
/// The function is total: every observation produces exactly one [`ObservedVerdict`] and
/// every unbacked matrix claim produces exactly one [`UncoveredClaim`]; nothing is
/// dropped.
pub fn generate_wire_coverage_report(
    record: &WireCoverageRecord,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> WireCoverageReport {
    let observed: Vec<ObservedVerdict> = record
        .rows
        .iter()
        .map(|row| {
            classify_observation(
                &row.wire_method,
                row.status_code,
                row.count,
                dynamic_config,
                namespace,
            )
        })
        .collect();

    let uncovered = derive_uncovered(record, dynamic_config, namespace);

    WireCoverageReport {
        observed,
        uncovered,
    }
}

/// Classify a single observation against the matrix.
///
/// Resolves the wire path, then — for a `Known` RPC — compares the observed status code
/// against the matrix's expected outcome via [`verdict_for_known`]. An `UnknownToMatrix`
/// path is reported verbatim with no matrix detail.
fn classify_observation(
    wire_method: &str,
    status_code: i32,
    count: u64,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> ObservedVerdict {
    match resolve(wire_method, dynamic_config, namespace) {
        RpcClassification::Known {
            rpc_id,
            feature_id,
            state,
            expected,
        } => ObservedVerdict {
            wire_method: wire_method.to_owned(),
            status_code,
            count,
            verdict: verdict_for_known(&expected, status_code),
            rpc_id: Some(rpc_id.to_owned()),
            feature_id: Some(feature_id.to_owned()),
            state: Some(state),
            expected: Some(expected),
        },
        RpcClassification::UnknownToMatrix { wire_path } => ObservedVerdict {
            wire_method: wire_path,
            status_code,
            count,
            verdict: WireCoverageVerdict::UnknownToMatrix,
            rpc_id: None,
            feature_id: None,
            state: None,
            expected: None,
        },
    }
}

/// Decide whether an observed status agrees with or contradicts the matrix's expected
/// outcome for a `Known` RPC.
///
/// The comparison is deliberately framed around *served-ness*, not exact status equality,
/// because an implemented RPC legitimately returns business errors (`NOT_FOUND`,
/// `ALREADY_EXISTS`, `INVALID_ARGUMENT`, …) while still being served:
///
/// - **`Ok` / `OkWhenEnabled`** (matrix says the RPC is implemented / experimental-enabled):
///   any status *except* `UNIMPLEMENTED` agrees — the RPC was served. Observing
///   `UNIMPLEMENTED` contradicts: the matrix claims it works but the wire says it does not.
/// - **`Unimplemented`** (matrix says stubbed/unsupported): `UNIMPLEMENTED` agrees; any
///   other status contradicts — the RPC was actually served despite the stub claim
///   (matrix is stale and *under*-claims).
/// - **`DisabledPrecondition`** (experimental feature, gate off): `FAILED_PRECONDITION`
///   agrees; anything else contradicts — a served success means the gate was not actually
///   enforced, and `UNIMPLEMENTED` means the disabled-state contract is not honoured.
///
/// This never yields `Uncovered` (that is a property of unobserved claims) nor
/// `UnknownToMatrix` (the caller has already resolved the path to `Known`).
fn verdict_for_known(expected: &ExpectedOutcome, status_code: i32) -> WireCoverageVerdict {
    let is = |code: Code| status_code == code as i32;

    match expected {
        ExpectedOutcome::Ok | ExpectedOutcome::OkWhenEnabled => {
            if is(Code::Unimplemented) {
                WireCoverageVerdict::Contradicts
            } else {
                WireCoverageVerdict::Agrees
            }
        }
        ExpectedOutcome::Unimplemented => {
            if is(Code::Unimplemented) {
                WireCoverageVerdict::Agrees
            } else {
                WireCoverageVerdict::Contradicts
            }
        }
        ExpectedOutcome::DisabledPrecondition => {
            if is(Code::FailedPrecondition) {
                WireCoverageVerdict::Agrees
            } else {
                WireCoverageVerdict::Contradicts
            }
        }
    }
}

/// Scan the matrix for `Implemented`/`Partial` RPCs the run never drove to success.
///
/// An RPC is *covered* when some observation shows it completing with `OK`; otherwise a
/// claimed-implemented RPC is `Uncovered`. Only `Implemented` and `Partial` features can
/// be uncovered — a `Stubbed`/`Unsupported`/`Experimental` feature makes no
/// "this works" claim for a successful call to demonstrate, so its absence from the run
/// is not a coverage gap (Requirement 5.5).
///
/// Coverage is decided on the *observed success set*: the set of matrix rpc ids for which
/// the record holds at least one `OK` observation. We resolve each observed `OK` row back
/// to its matrix rpc id (via `resolve`) rather than string-matching wire paths, so the
/// success set and the matrix scan share one canonical identity and cannot drift.
fn derive_uncovered(
    record: &WireCoverageRecord,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> Vec<UncoveredClaim> {
    // The set of matrix rpc ids observed completing with OK at least once. Built by
    // resolving each successful observation back to its canonical matrix id so the
    // membership test below matches on the same identity the scan iterates.
    let mut driven_ok: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in &record.rows {
        if row.status_code != Code::Ok as i32 {
            continue;
        }
        if let RpcClassification::Known { rpc_id, .. } =
            resolve(&row.wire_method, dynamic_config, namespace)
        {
            driven_ok.insert(rpc_id.to_owned());
        }
    }

    let mut uncovered = Vec::new();
    for entry in FEATURE_MATRIX {
        if !matches!(
            entry.state,
            FeatureState::Implemented | FeatureState::Partial
        ) {
            continue;
        }
        for rpc in entry.rpcs {
            // A matrix rpc id maps to a wire path and back to the same id; membership is
            // tested on the matrix id directly so it cannot diverge from `driven_ok`.
            if !driven_ok.contains(*rpc) {
                uncovered.push(UncoveredClaim {
                    rpc_id: (*rpc).to_owned(),
                    feature_id: entry.id.to_owned(),
                    state: entry.state,
                });
            }
        }
    }
    uncovered
}

/// Build the wire path for a matrix rpc id, used by tests and callers that want to drive
/// the canonical path for a claimed RPC. Thin pass-through to the compatibility crate's
/// own mapping so there is one definition of the wire shape.
pub fn wire_path_for_rpc(rpc_id: &str) -> Option<String> {
    rpc_id_to_wire_path(rpc_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_compatibility::dispatch::StaticDynamicConfig;

    use crate::conformance::record::WireCoverageRow;

    fn ok_row(wire_method: &str) -> WireCoverageRow {
        WireCoverageRow {
            wire_method: wire_method.to_owned(),
            status_code: Code::Ok as i32,
            count: 1,
        }
    }

    fn row(wire_method: &str, code: Code) -> WireCoverageRow {
        WireCoverageRow {
            wire_method: wire_method.to_owned(),
            status_code: code as i32,
            count: 1,
        }
    }

    // An implemented RPC observed succeeding agrees with the matrix claim.
    #[test]
    fn implemented_rpc_observed_ok_agrees() {
        let start = wire_path_for_rpc("WorkflowService.StartWorkflowExecution")
            .expect("known matrix rpc has a wire path");
        let record = WireCoverageRecord {
            rows: vec![ok_row(&start)],
        };
        let report = generate_wire_coverage_report(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        let verdict = &report.observed[0];
        assert_eq!(verdict.verdict, WireCoverageVerdict::Agrees);
        assert_eq!(
            verdict.rpc_id.as_deref(),
            Some("WorkflowService.StartWorkflowExecution")
        );
    }

    // An implemented RPC observed answering UNIMPLEMENTED contradicts the matrix claim.
    #[test]
    fn implemented_rpc_observed_unimplemented_contradicts() {
        let start = wire_path_for_rpc("WorkflowService.StartWorkflowExecution")
            .expect("known matrix rpc has a wire path");
        let record = WireCoverageRecord {
            rows: vec![row(&start, Code::Unimplemented)],
        };
        let report = generate_wire_coverage_report(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        assert_eq!(report.observed[0].verdict, WireCoverageVerdict::Contradicts);
    }

    // An implemented RPC observed returning a business error (NOT_FOUND) still agrees:
    // the RPC was served, which is all the matrix claims.
    #[test]
    fn implemented_rpc_observed_business_error_agrees() {
        let describe = wire_path_for_rpc("WorkflowService.DescribeWorkflowExecution")
            .expect("known matrix rpc has a wire path");
        let record = WireCoverageRecord {
            rows: vec![row(&describe, Code::NotFound)],
        };
        let report = generate_wire_coverage_report(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        assert_eq!(report.observed[0].verdict, WireCoverageVerdict::Agrees);
    }

    // A path the matrix does not classify is reported verbatim as unknown-to-matrix.
    #[test]
    fn unclassified_path_is_unknown_to_matrix() {
        let record = WireCoverageRecord {
            rows: vec![ok_row(
                "/temporal.server.api.adminservice.v1.AdminService/DescribeMutableState",
            )],
        };
        let report = generate_wire_coverage_report(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        let verdict = &report.observed[0];
        assert_eq!(verdict.verdict, WireCoverageVerdict::UnknownToMatrix);
        assert!(verdict.rpc_id.is_none());
    }

    // A claimed implemented RPC never driven to success appears as uncovered; one that
    // was driven to OK does not.
    #[test]
    fn uncovered_lists_undriven_implemented_rpcs_only() {
        let start = wire_path_for_rpc("WorkflowService.StartWorkflowExecution")
            .expect("known matrix rpc has a wire path");
        let record = WireCoverageRecord {
            rows: vec![ok_row(&start)],
        };
        let report = generate_wire_coverage_report(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        // The driven RPC must not be uncovered.
        assert!(
            !report
                .uncovered
                .iter()
                .any(|u| u.rpc_id == "WorkflowService.StartWorkflowExecution"),
            "an OK-driven rpc must not be reported uncovered"
        );
        // Some other implemented/partial rpc that was never driven must be uncovered,
        // proving the scan reports undriven claims.
        assert!(
            !report.uncovered.is_empty(),
            "implemented rpcs never driven this run must be reported uncovered"
        );
        for claim in &report.uncovered {
            assert!(
                matches!(
                    claim.state,
                    FeatureState::Implemented | FeatureState::Partial
                ),
                "only implemented/partial rpcs can be uncovered, got {:?} for {}",
                claim.state,
                claim.rpc_id
            );
        }
    }
}
