//! Wire-path ↔ rpc-id normalization, ground-truthed to the vendored protos.
//!
//! This submodule owns the bidirectional mapping between an observed gRPC method path
//! (`/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution`) and the matrix's
//! dotted rpc id (`WorkflowService.StartWorkflowExecution`), so coverage joins are exact rather
//! than fuzzy string matches (Requirement 1).
//!
//! The package prefix differs per service (`WorkflowService` lives in
//! `temporal.api.workflowservice.v1`, `OperatorService` in `temporal.api.operatorservice.v1`).
//! Rather than hardcode that mapping — which would silently rot if the vendored protos moved — it
//! is derived at runtime from the `package` and `service` declarations in the vendored protos
//! (crate-local verbatim copies of `proto/upstream/`, kept in lockstep by `proto-sync` and a
//! parity test), the same source the matrix-completeness test
//! (`matrix_classifies_every_upstream_rpc`) trusts. Per AGENTS.md §8 the vendored protos are the
//! authoritative wire shape; generated artifacts under `target/` are never read, as they can be
//! stale. The parse is `include_str!` + line scanning: no new dependency, no I/O at runtime, no
//! async — consistent with the crate's pure contract (Requirement 5).

/// Vendored `WorkflowService` proto, the authoritative source for its package prefix.
///
/// Sliced at compile time via `include_str!` so the package mapping is ground-truthed to
/// the vendored proto tree and never to a possibly-stale generated descriptor under
/// `target/` (AGENTS.md §8). The include reads the crate-local verbatim copy under
/// `data/` rather than `proto/upstream/` directly, because the published archive cannot
/// reach outside its own package; `proto-sync` refreshes the copies on every sync and a
/// parity test fails the workspace build if they drift from `proto/upstream/`.
const WORKFLOW_SERVICE_PROTO: &str = include_str!("../../data/workflowservice.service.proto");

/// Vendored `OperatorService` proto, the authoritative source for its package prefix.
const OPERATOR_SERVICE_PROTO: &str = include_str!("../../data/operatorservice.service.proto");

/// The vendored service protos scanned to build the service→package mapping.
///
/// Each proto declares exactly one `service` and one `package`; the matrix vocabulary spans these
/// two services, so this list is the complete ground truth for the inverse mapping. Adding a third
/// matrix service is a one-line change here, keeping the mapping in lockstep with the protos.
const SERVICE_PROTOS: &[&str] = &[WORKFLOW_SERVICE_PROTO, OPERATOR_SERVICE_PROTO];

/// Maps an observed gRPC wire method path to its matrix rpc id, or `None` if it does not parse.
///
/// A valid wire path is `/<package>.<Service>/<Method>`; the rpc id is `<Service>.<Method>` where
/// the service is the final dotted segment of the qualified name. The function is deliberately
/// structural — it derives the service from the path itself rather than validating the package
/// against the protos — so it stays total over arbitrary observed input. Any path that does not
/// match the `/package.Service/Method` shape (wrong segment count, missing leading slash, no
/// package dot, empty method) yields `None` rather than panicking, so a recorder can feed it
/// every observed path without guarding (Requirement 1.1, 1.4).
pub fn wire_path_to_rpc_id(wire_path: &str) -> Option<String> {
    // Split into exactly three parts: the empty string before the leading slash, the qualified
    // `package.Service`, and the method. Anything else is not the gRPC method-path shape.
    let mut parts = wire_path.split('/');
    let leading = parts.next()?;
    let qualified = parts.next()?;
    let method = parts.next()?;
    if parts.next().is_some() || !leading.is_empty() || method.is_empty() {
        return None;
    }

    // The service is the last dotted segment; everything before it is the package. Requiring a
    // non-empty package enforces the `package.Service` half of the shape (Requirement 1.4).
    let (package, service) = qualified.rsplit_once('.')?;
    if package.is_empty() || service.is_empty() {
        return None;
    }

    Some(format!("{service}.{method}"))
}

/// Maps a matrix rpc id back to its gRPC wire method path, or `None` for an unknown service.
///
/// This is the inverse of [`wire_path_to_rpc_id`]. The rpc id carries only `<Service>.<Method>`,
/// so the package prefix must be recovered from the vendored protos — that lookup is what makes an
/// unknown service (one no vendored proto declares) return `None` rather than fabricate a path
/// (Requirement 1.2, 1.3). A malformed rpc id (no dot, empty service or method) also yields
/// `None`, keeping the function total.
pub fn rpc_id_to_wire_path(rpc_id: &str) -> Option<String> {
    let (service, method) = rpc_id.split_once('.')?;
    if service.is_empty() || method.is_empty() {
        return None;
    }

    let package = package_for_service(service)?;
    Some(format!("/{package}.{service}/{method}"))
}

/// Resolves a service name to its proto package, scanning the vendored protos.
///
/// Returns `None` for any service not declared by a vendored proto, which is what propagates an
/// unknown service up through [`rpc_id_to_wire_path`]. Re-parsing the (small) protos on each call
/// avoids a cached static and therefore a new dependency such as `once_cell`; the mapping stays
/// purely a projection of the vendored protos (Requirement 1.3, 5.2).
fn package_for_service(service: &str) -> Option<&'static str> {
    SERVICE_PROTOS.iter().copied().find_map(|proto| {
        let (declared_service, package) = parse_service_package(proto)?;
        (declared_service == service).then_some(package)
    })
}

/// Extracts `(service_name, package)` from a single vendored service proto.
///
/// Reads the `service <Name> {` and `package <dotted>;` declarations directly. Returns `None` if
/// either is absent rather than panicking, so a malformed or unexpected proto degrades to "unknown
/// service" instead of crashing — the same total-function discipline the public API upholds.
fn parse_service_package(proto: &'static str) -> Option<(&'static str, &'static str)> {
    let package = proto
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("package "))
        .map(|rest| rest.trim_end().trim_end_matches(';'))?;

    let service = proto.lines().find_map(|line| {
        line.trim_start()
            .strip_prefix("service ")?
            .split_whitespace()
            .next()
    })?;

    Some((service, package))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_workflow_service_wire_path_to_rpc_id() {
        assert_eq!(
            wire_path_to_rpc_id(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
            ),
            Some("WorkflowService.StartWorkflowExecution".to_string())
        );
    }

    #[test]
    fn maps_operator_service_wire_path_to_rpc_id() {
        assert_eq!(
            wire_path_to_rpc_id(
                "/temporal.api.operatorservice.v1.OperatorService/CreateNexusEndpoint"
            ),
            Some("OperatorService.CreateNexusEndpoint".to_string())
        );
    }

    #[test]
    fn maps_workflow_rpc_id_to_wire_path() {
        assert_eq!(
            rpc_id_to_wire_path("WorkflowService.StartWorkflowExecution"),
            Some(
                "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
                    .to_string()
            )
        );
    }

    #[test]
    fn maps_operator_rpc_id_to_wire_path() {
        assert_eq!(
            rpc_id_to_wire_path("OperatorService.CreateNexusEndpoint"),
            Some(
                "/temporal.api.operatorservice.v1.OperatorService/CreateNexusEndpoint".to_string()
            )
        );
    }

    #[test]
    fn round_trips_through_both_directions() {
        let rpc_id = "WorkflowService.StartWorkflowExecution";
        let wire_path = rpc_id_to_wire_path(rpc_id).expect("known rpc id maps to a wire path");
        assert_eq!(wire_path_to_rpc_id(&wire_path).as_deref(), Some(rpc_id));
    }

    #[test]
    fn package_prefix_is_derived_from_the_vendored_protos() {
        let wire_path = rpc_id_to_wire_path("WorkflowService.StartWorkflowExecution")
            .expect("known rpc id maps to a wire path");
        assert!(wire_path.starts_with("/temporal.api.workflowservice.v1.WorkflowService/"));

        let operator = rpc_id_to_wire_path("OperatorService.CreateNexusEndpoint")
            .expect("known rpc id maps to a wire path");
        assert!(operator.starts_with("/temporal.api.operatorservice.v1.OperatorService/"));
    }

    #[test]
    fn unknown_service_has_no_wire_path() {
        assert_eq!(
            rpc_id_to_wire_path("AdminService.DescribeMutableState"),
            None
        );
    }

    #[test]
    fn malformed_wire_paths_return_none() {
        assert_eq!(wire_path_to_rpc_id(""), None);
        assert_eq!(wire_path_to_rpc_id("not-a-path"), None);
        assert_eq!(wire_path_to_rpc_id("/NoPackage/Method"), None);
        assert_eq!(
            wire_path_to_rpc_id("temporal.api.workflowservice.v1.WorkflowService/Start"),
            None
        );
        assert_eq!(
            wire_path_to_rpc_id("/temporal.api.workflowservice.v1.WorkflowService/Start/Extra"),
            None
        );
        assert_eq!(
            wire_path_to_rpc_id("/temporal.api.workflowservice.v1.WorkflowService/"),
            None
        );
    }

    #[test]
    fn malformed_rpc_ids_return_none() {
        assert_eq!(rpc_id_to_wire_path(""), None);
        assert_eq!(rpc_id_to_wire_path("NoMethod"), None);
        assert_eq!(rpc_id_to_wire_path("WorkflowService."), None);
        assert_eq!(rpc_id_to_wire_path(".StartWorkflowExecution"), None);
    }

    // Feature: temporal-compatibility-surface, Property 1
    //
    // Normalization round-trip: every matrix-classified rpc id survives a trip out to its wire
    // path and back unchanged, and the forward wire path is exactly `/<package>.<Service>/<Method>`
    // with the package read from the vendored proto for that service. The matrix rpc set is a fixed
    // finite list, so an exhaustive loop over `FEATURE_MATRIX` is the complete (strongest) form of
    // the property rather than a random sample.
    #[test]
    fn every_matrix_rpc_round_trips_and_matches_the_vendored_package() {
        for entry in crate::FEATURE_MATRIX {
            for rpc_id in entry.rpcs {
                let wire_path =
                    rpc_id_to_wire_path(rpc_id).expect("matrix rpc id maps to a wire path");

                // Round-trip: out to the wire path and back yields the same rpc id.
                assert_eq!(
                    wire_path_to_rpc_id(&wire_path),
                    Some((*rpc_id).to_owned()),
                    "round-trip mismatch for {rpc_id}"
                );

                // Forward direction: the wire path is `/<package>.<Service>/<Method>` with the
                // package derived from the vendored proto that declares the service.
                let (service, method) = rpc_id
                    .split_once('.')
                    .expect("matrix rpc id is Service.Method");
                let package = package_for_service(service)
                    .expect("matrix service is declared by a vendored proto");
                assert_eq!(
                    wire_path,
                    format!("/{package}.{service}/{method}"),
                    "wire path shape mismatch for {rpc_id}"
                );
            }
        }
    }

    #[test]
    fn crate_local_proto_copies_match_the_vendored_tree() {
        // The packaged crate carries verbatim copies under data/; in the
        // workspace the vendored tree is present and the copies must be
        // byte-identical (refresh with `cargo run -p proto-sync -- generate`).
        // A packaged test run has no vendored tree and skips.
        let upstream = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../proto/upstream/temporal/api"
        );
        for (copy, upstream_rel) in [
            (WORKFLOW_SERVICE_PROTO, "workflowservice/v1/service.proto"),
            (OPERATOR_SERVICE_PROTO, "operatorservice/v1/service.proto"),
        ] {
            let path = format!("{upstream}/{upstream_rel}");
            let Ok(vendored) = std::fs::read_to_string(&path) else {
                return;
            };
            assert_eq!(copy, vendored, "stale data/ copy of {upstream_rel}");
        }
    }
}
