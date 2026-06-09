//! Out-of-public-scope surface derivation from wire observations.
//!
//! When a Tier-2 test fails because it reaches a surface outside Tokeira's public
//! claim — an internal client the Shape-2 onebox does not front — the ledger
//! classifies it `OutOfPublicScope` and must cite *which* internal surface it
//! touched ([`super::ledger::EvidenceRef::InternalSurface`], Requirement 3.6). This
//! module derives that surface **mechanically from the wire-coverage record**, so the
//! citation is a recorder-observed fact rather than hand-judgement (task 9.3).
//!
//! ## What "internal surface" means here
//!
//! The compatibility matrix's vocabulary is exactly the public `WorkflowService` and
//! `OperatorService` surface. Any other gRPC service observed on the wire — Temporal's
//! internal `AdminService`, `HistoryService`, `MatchingService`, the gRPC `Health`
//! protocol, or an `OperatorService` method the matrix does not classify — is
//! beyond-claim. The wire-coverage join ([`super::report`]) already marks such paths
//! [`RpcClassification::UnknownToMatrix`](tokeira_compatibility::coverage::RpcClassification);
//! this module names *which* internal client each one belongs to, by reading the
//! service segment of the gRPC path. The mapping is structural (derived from the path),
//! not a hardcoded allow-list of methods, so a newly-observed internal method is named
//! by its service without a code change.
//!
//! ## Honest scope limit: run-level, not per-test
//!
//! The wire-coverage recorder ([`super::recorder`]) aggregates `(wire_method,
//! status_code)` counts over the *whole run*; it does not attribute calls to the
//! individual test that made them. So this module derives the set of internal surfaces
//! touched **by the run**, not per-test. That is sufficient for the report's purpose —
//! it answers "did this run reach `AdminService`/`HistoryService`/… , and which
//! methods?" — and it is the faithful limit of the current recorder. True per-test
//! attribution would require the recorder to carry a per-call test correlation
//! (a recorder change, not a report change); until then the report cites the run-level
//! surface set and is explicit that the attribution is run-scoped. This is called out
//! so a future reader does not mistake the run-level set for per-test evidence.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tokeira_compatibility::{
    coverage::{RpcClassification, resolve},
    dispatch::DynamicConfigReader,
};

use super::record::WireCoverageRecord;

/// An internal client surface beyond Tokeira's public compatibility claim.
///
/// Each variant names a gRPC service (or protocol) that the public matrix does not
/// own. The variants are the surfaces the design enumerates as out-of-public-scope
/// signals (Requirement 3.6): Temporal's internal cross-service clients, the gRPC
/// health protocol, and a catch-all for any other unclassified service so the
/// derivation stays total over arbitrary observed paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InternalSurface {
    /// Temporal's internal `AdminService` (cluster/shard administration).
    AdminClient,
    /// Temporal's internal `HistoryService`.
    HistoryClient,
    /// Temporal's internal `MatchingService`.
    MatchingClient,
    /// An `OperatorService` method the matrix does not classify — i.e. operator
    /// surface beyond the claimed subset. Distinguished from the other internal
    /// services because `OperatorService` is *partly* public, so reaching an
    /// unclassified method on it is "beyond the claimed subset" specifically.
    OperatorBeyondClaim,
    /// The standard gRPC Health Checking Protocol (`grpc.health.v1.Health`).
    Health,
    /// Any other service the matrix does not classify, named verbatim by its
    /// service segment so the derivation never drops an observation it cannot bucket.
    Other(String),
}

impl InternalSurface {
    /// The stable tag string used as the ledger's `InternalSurface` evidence value.
    ///
    /// This is what a `OutOfPublicScope` entry cites; keeping it derived from the enum
    /// (rather than re-typed at each call site) means the evidence string and the
    /// classifier can never drift.
    pub fn tag(&self) -> String {
        match self {
            InternalSurface::AdminClient => "AdminClient".to_owned(),
            InternalSurface::HistoryClient => "HistoryClient".to_owned(),
            InternalSurface::MatchingClient => "MatchingClient".to_owned(),
            InternalSurface::OperatorBeyondClaim => "OperatorClient(beyond-claim)".to_owned(),
            InternalSurface::Health => "Health".to_owned(),
            InternalSurface::Other(service) => format!("Other({service})"),
        }
    }
}

/// One internal surface touched by the run, with the methods observed on it and how
/// many calls hit them.
///
/// `methods` lists the distinct wire paths observed for this surface (sorted, for
/// stable evidence), and `call_count` is the total observations across them. This is
/// the mechanical detail a `OutOfPublicScope` classification cites: not just "an
/// internal client", but which one and which methods.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalSurfaceUsage {
    /// The internal surface this usage belongs to.
    pub surface: InternalSurface,
    /// Distinct observed wire paths on this surface, sorted for canonical output.
    pub methods: Vec<String>,
    /// Total observations (summed counts) across this surface's methods.
    pub call_count: u64,
}

/// The run-level set of internal surfaces the corpus reached, derived from the
/// wire-coverage record.
///
/// This is the report's out-of-public-scope evidence (task 9.3): the mechanical answer
/// to "which beyond-claim internal clients did this run touch, and via which methods?".
/// It is run-scoped, not per-test — see the module docs on why, and on the recorder
/// change true per-test attribution would require.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutOfScopeSurfaces {
    /// One entry per distinct internal surface observed, sorted by surface for
    /// canonical output. Empty when the run touched no beyond-claim surface.
    pub surfaces: Vec<InternalSurfaceUsage>,
}

/// Extract the gRPC service segment from a wire path (`/package.Service/Method`).
///
/// Returns the bare `Service` (the final dotted segment of the qualified name), or
/// `None` for a path that is not the gRPC method-path shape. Structural and total, so
/// it can be fed arbitrary observed paths without guarding — mirroring the discipline
/// of `tokeira_compatibility::coverage::normalize`.
fn service_segment(wire_path: &str) -> Option<&str> {
    let mut parts = wire_path.split('/');
    let leading = parts.next()?;
    let qualified = parts.next()?;
    let method = parts.next()?;
    if parts.next().is_some() || !leading.is_empty() || method.is_empty() {
        return None;
    }
    let (package, service) = qualified.rsplit_once('.')?;
    if package.is_empty() || service.is_empty() {
        return None;
    }
    Some(service)
}

/// Classify an `UnknownToMatrix` wire path into the internal surface it belongs to.
///
/// The mapping is by gRPC service name (the structural segment of the path), which is
/// why it stays correct for methods the matrix has never seen. `Health` is recognised
/// by its well-known service name; an `OperatorService` path reaching here is
/// beyond-claim by construction (the matrix would have classified a claimed operator
/// method as `Known`); anything else is named verbatim via [`InternalSurface::Other`]
/// so nothing is dropped.
fn surface_for_unknown_path(wire_path: &str) -> InternalSurface {
    match service_segment(wire_path) {
        Some("AdminService") => InternalSurface::AdminClient,
        Some("HistoryService") => InternalSurface::HistoryClient,
        Some("MatchingService") => InternalSurface::MatchingClient,
        // OperatorService reaching the unknown bucket means the matrix did not claim
        // this method — operator surface beyond the claimed subset.
        Some("OperatorService") => InternalSurface::OperatorBeyondClaim,
        Some("Health") => InternalSurface::Health,
        Some(other) => InternalSurface::Other(other.to_owned()),
        // An unparseable path still must not be dropped; name it verbatim.
        None => InternalSurface::Other(wire_path.to_owned()),
    }
}

/// Derive the run-level out-of-public-scope surfaces from a wire-coverage record.
///
/// Walks the record once, resolving each row through
/// `tokeira_compatibility::coverage::resolve`; rows that resolve to
/// [`RpcClassification::Known`] are public surface and ignored, rows that resolve to
/// [`RpcClassification::UnknownToMatrix`] are bucketed by internal surface. The result
/// aggregates distinct methods and total call counts per surface, with deterministic
/// ordering throughout so the evidence is canonical across runs.
///
/// `dynamic_config` and `namespace` are threaded into `resolve` only to keep the
/// classification consistent with the wire-coverage report (an experimental feature's
/// `Known`-ness does not change its public-vs-internal status, but using the same
/// `resolve` call guarantees the two reports partition the same paths identically).
pub fn derive_out_of_scope_surfaces(
    record: &WireCoverageRecord,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> OutOfScopeSurfaces {
    // Accumulate per surface: the set of distinct methods (with their summed counts).
    // BTreeMap keys give canonical ordering for free.
    let mut by_surface: BTreeMap<InternalSurface, BTreeMap<String, u64>> = BTreeMap::new();

    for row in &record.rows {
        // Only beyond-claim (unknown-to-matrix) traffic is out-of-scope evidence;
        // public surface resolves to Known and is skipped.
        if let RpcClassification::Known { .. } =
            resolve(&row.wire_method, dynamic_config, namespace)
        {
            continue;
        }
        let surface = surface_for_unknown_path(&row.wire_method);
        let methods = by_surface.entry(surface).or_default();
        *methods.entry(row.wire_method.clone()).or_insert(0) += row.count;
    }

    let surfaces = by_surface
        .into_iter()
        .map(|(surface, methods)| {
            let call_count = methods.values().sum();
            InternalSurfaceUsage {
                surface,
                methods: methods.into_keys().collect(),
                call_count,
            }
        })
        .collect();

    OutOfScopeSurfaces { surfaces }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_compatibility::dispatch::StaticDynamicConfig;

    use crate::conformance::record::WireCoverageRow;

    fn row(wire_method: &str, count: u64) -> WireCoverageRow {
        WireCoverageRow {
            wire_method: wire_method.to_owned(),
            status_code: 0,
            count,
        }
    }

    // AdminService traffic is bucketed as the AdminClient internal surface.
    #[test]
    fn admin_service_is_admin_client() {
        let record = WireCoverageRecord {
            rows: vec![row(
                "/temporal.server.api.adminservice.v1.AdminService/DescribeMutableState",
                2,
            )],
        };
        let scope = derive_out_of_scope_surfaces(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        assert_eq!(scope.surfaces.len(), 1);
        assert_eq!(scope.surfaces[0].surface, InternalSurface::AdminClient);
        assert_eq!(scope.surfaces[0].call_count, 2);
        assert_eq!(scope.surfaces[0].methods.len(), 1);
    }

    // History and Matching internal services map to their respective clients.
    #[test]
    fn history_and_matching_services_map_to_clients() {
        let record = WireCoverageRecord {
            rows: vec![
                row(
                    "/temporal.server.api.historyservice.v1.HistoryService/GetMutableState",
                    1,
                ),
                row(
                    "/temporal.server.api.matchingservice.v1.MatchingService/PollWorkflowTaskQueue",
                    3,
                ),
            ],
        };
        let scope = derive_out_of_scope_surfaces(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        let surfaces: Vec<&InternalSurface> = scope.surfaces.iter().map(|u| &u.surface).collect();
        assert!(surfaces.contains(&&InternalSurface::HistoryClient));
        assert!(surfaces.contains(&&InternalSurface::MatchingClient));
    }

    // Public WorkflowService traffic is NOT out-of-scope; it resolves to Known and is
    // excluded from the surfaces entirely.
    #[test]
    fn public_workflow_service_is_not_out_of_scope() {
        let record = WireCoverageRecord {
            rows: vec![row(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
                5,
            )],
        };
        let scope = derive_out_of_scope_surfaces(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        assert!(
            scope.surfaces.is_empty(),
            "public surface must not appear as out-of-scope, got {:?}",
            scope.surfaces
        );
    }

    // The gRPC health protocol is recognised as its own surface.
    #[test]
    fn health_protocol_is_its_own_surface() {
        let record = WireCoverageRecord {
            rows: vec![row("/grpc.health.v1.Health/Check", 1)],
        };
        let scope = derive_out_of_scope_surfaces(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        assert_eq!(scope.surfaces.len(), 1);
        assert_eq!(scope.surfaces[0].surface, InternalSurface::Health);
    }

    // Distinct methods on one surface aggregate under that surface with summed counts
    // and sorted method lists (canonical evidence).
    #[test]
    fn methods_aggregate_per_surface_with_canonical_order() {
        let record = WireCoverageRecord {
            rows: vec![
                row(
                    "/temporal.server.api.adminservice.v1.AdminService/ZebraMethod",
                    1,
                ),
                row(
                    "/temporal.server.api.adminservice.v1.AdminService/AlphaMethod",
                    4,
                ),
            ],
        };
        let scope = derive_out_of_scope_surfaces(
            &record,
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );

        assert_eq!(scope.surfaces.len(), 1);
        let usage = &scope.surfaces[0];
        assert_eq!(usage.surface, InternalSurface::AdminClient);
        assert_eq!(usage.call_count, 5);
        // Sorted: Alpha before Zebra.
        assert_eq!(usage.methods.len(), 2);
        assert!(usage.methods[0].ends_with("/AlphaMethod"));
        assert!(usage.methods[1].ends_with("/ZebraMethod"));
    }

    #[test]
    fn surface_tag_strings_are_stable() {
        assert_eq!(InternalSurface::AdminClient.tag(), "AdminClient");
        assert_eq!(InternalSurface::HistoryClient.tag(), "HistoryClient");
        assert_eq!(InternalSurface::MatchingClient.tag(), "MatchingClient");
        assert_eq!(
            InternalSurface::OperatorBeyondClaim.tag(),
            "OperatorClient(beyond-claim)"
        );
        assert_eq!(InternalSurface::Health.tag(), "Health");
        assert_eq!(
            InternalSurface::Other("FooService".to_owned()).tag(),
            "Other(FooService)"
        );
    }
}
