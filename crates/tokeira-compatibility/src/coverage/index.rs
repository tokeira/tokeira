//! Runtime RPC→feature index over `FEATURE_MATRIX`.
//!
//! This submodule owns the runtime lookup from a dotted rpc id to its `&'static FeatureEntry`,
//! promoting today's test-only RPC index into real API so each observed RPC resolves
//! deterministically to its feature and state. It is a projection over the existing matrix const
//! — built by scanning `FEATURE_MATRIX`, never a second source of truth — and inherits the
//! "every rpc owned exactly once" guarantee the matrix completeness test already enforces.

use crate::{FEATURE_MATRIX, FeatureEntry};

/// Resolve a dotted rpc id (e.g. `WorkflowService.StartWorkflowExecution`) to the matrix entry
/// that owns it, or `None` if the matrix does not classify it.
///
/// This is the runtime counterpart to the `#[cfg(test)]` ownership map in `matrix.rs`: a coverage
/// consumer feeds it the rpc id produced by wire-path normalization and gets back the single
/// `FeatureEntry` that declares the RPC, from which it reads the feature's state and id. Returning
/// `Option` rather than panicking keeps the function total over arbitrary input — an id the matrix
/// has never heard of (a future proto method observed before the matrix catches up) yields `None`,
/// which `resolve` maps to the unknown-to-matrix bucket rather than dropping the observation.
///
/// A linear scan over `FEATURE_MATRIX` returning the first match is correct because the matrix is
/// small and the existing `every_rpc_is_owned_once` test guarantees each rpc id is owned by exactly
/// one entry; there is therefore no ambiguity to resolve and no benefit to a precomputed map that
/// would add a dependency or lazy-init complexity for no measurable gain.
pub fn feature_for_rpc(rpc_id: &str) -> Option<&'static FeatureEntry> {
    feature_and_rpc_for(rpc_id).map(|(entry, _)| entry)
}

/// Resolve a dotted rpc id to its owning entry *and* the `&'static str` the matrix stores for that
/// rpc, or `None` if the matrix does not classify it.
///
/// This exists because [`resolve`](super::resolve) reports `rpc_id` as a `&'static str` borrowed
/// from the matrix, whereas wire-path normalization yields an *owned* `String`. Rather than have
/// `resolve` re-scan `entry.rpcs` to recover the `&'static` form after the lookup already found it,
/// this returns both halves of the match in one pass: the owning entry and the matrix's own
/// `&'static str` for the rpc (the element of `entry.rpcs` equal to `rpc_id`). The returned rpc id
/// borrows from `FEATURE_MATRIX`, which is `const` for the life of the program, so it is genuinely
/// `&'static` — no allocation and no lifetime tied to the caller's `rpc_id` argument.
///
/// [`feature_for_rpc`] is the entry-only projection of this; both share the single linear scan so
/// the "every rpc owned exactly once" guarantee (proven by `every_rpc_is_owned_once`) governs both.
pub fn feature_and_rpc_for(rpc_id: &str) -> Option<(&'static FeatureEntry, &'static str)> {
    FEATURE_MATRIX.iter().find_map(|entry| {
        entry
            .rpcs
            .iter()
            .copied()
            .find(|&owned_rpc| owned_rpc == rpc_id)
            .map(|owned_rpc| (entry, owned_rpc))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_a_known_rpc_to_its_owning_feature() {
        let entry = feature_for_rpc("WorkflowService.StartWorkflowExecution")
            .expect("StartWorkflowExecution is classified by the matrix");
        assert_eq!(entry.id, "workflow-start");
    }

    #[test]
    fn resolves_an_operator_service_rpc() {
        let entry = feature_for_rpc("OperatorService.CreateNexusEndpoint")
            .expect("CreateNexusEndpoint is classified by the matrix");
        assert_eq!(entry.id, "nexus-admin");
    }

    #[test]
    fn returns_none_for_an_unclassified_rpc_id() {
        assert!(feature_for_rpc("WorkflowService.NotARealRpc").is_none());
    }

    #[test]
    fn returns_none_for_an_empty_rpc_id() {
        assert!(feature_for_rpc("").is_none());
    }

    #[test]
    fn resolves_every_classified_rpc_to_exactly_one_entry() {
        for entry in FEATURE_MATRIX {
            for rpc in entry.rpcs {
                let resolved =
                    feature_for_rpc(rpc).expect("every classified rpc resolves to an entry");
                assert_eq!(
                    resolved.id, entry.id,
                    "rpc {rpc} resolved to the wrong owning feature"
                );
            }
        }
    }

    // Feature: temporal-compatibility-surface, Property 2
    //
    // Index totality and uniqueness, exercised *through* `feature_for_rpc`: every classified rpc id
    // resolves to exactly the one entry that declares it (totality), no *other* entry claims the
    // same rpc (uniqueness verified via the index, not just the raw matrix), and an id the matrix
    // never classifies resolves to `None`. This mirrors the matrix's own `every_rpc_is_owned_once`
    // intent but proves the runtime lookup — the surface coverage consumers actually call — inherits
    // that guarantee rather than re-deriving it.
    #[test]
    fn feature_for_rpc_resolves_each_rpc_to_its_sole_owner_and_none_otherwise() {
        for owner in FEATURE_MATRIX {
            for rpc in owner.rpcs {
                let resolved = feature_for_rpc(rpc)
                    .expect("every classified rpc id resolves to an owning entry");
                assert_eq!(
                    resolved.id, owner.id,
                    "rpc {rpc} resolved to the wrong owning feature via the index"
                );

                // Uniqueness through the index: the resolved owner is the *only* entry whose
                // `rpcs` contains this id, so no two features can both claim it.
                let claimants = FEATURE_MATRIX
                    .iter()
                    .filter(|candidate| candidate.rpcs.contains(rpc))
                    .count();
                assert_eq!(
                    claimants, 1,
                    "rpc {rpc} is claimed by {claimants} entries; the index must resolve a sole owner"
                );
            }
        }

        assert!(
            feature_for_rpc("WorkflowService.NotARealRpc").is_none(),
            "a clearly-unclassified rpc id must resolve to None"
        );
    }
}
