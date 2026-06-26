//! Queryable coverage surface over the compatibility matrix.
//!
//! This module turns `FEATURE_MATRIX` from a *declaration* of Tokeira's Temporal
//! compatibility into a *queryable spine* that conformance tooling can join a stream of
//! observed gRPC calls against. The matrix already classifies every WorkflowService and
//! OperatorService RPC exactly once (state, capability field, dynamic-config key, evidence);
//! what consumers lack is the small set of runtime query functions needed to map an observed
//! wire method to a matrix-backed verdict. This module owns that query layer and nothing else.
//!
//! It sits in `tokeira-compatibility` (not edge or runtime) on purpose: the matrix is the
//! single source of truth for compatibility policy, the crate is pure (no I/O, no async), and
//! kernel/edge/CLI all depend on it freely. Adding the coverage API here keeps one policy in
//! one place rather than re-deriving matching logic in each downstream consumer. Every function
//! is a total projection over the existing `const` data plus the vendored descriptor set — it
//! introduces no new source of truth and never mutates the matrix.
//!
//! The module is split into submodules so independent pieces compose without entangling:
//! [`normalize`] maps wire paths to/from rpc ids, [`index`] resolves an rpc id to its
//! `FeatureEntry`, [`expected`] projects a feature's state to the wire outcome it implies, and
//! this file owns the shared result types ([`ExpectedOutcome`], [`RpcClassification`]) plus the
//! `resolve` entry point that composes the three. This task establishes only the skeleton and
//! result types; the query functions are filled in by later tasks.

pub mod expected;
pub mod index;
pub mod normalize;

use serde::{Deserialize, Serialize};

use crate::{FeatureState, dispatch::DynamicConfigReader};

/// The wire outcome a feature's matrix state implies for one of its RPCs.
///
/// This is the verdict a coverage consumer compares an *observed* gRPC status against: it
/// answers "given what the matrix claims about this feature, what should calling this RPC
/// produce?". It is deliberately a small closed set rather than a raw status code so the same
/// projection serves both conformance tiers without either re-deriving the mapping. The variants
/// mirror the existing `dispatch_rpc` policy (one policy, two views) so an expected outcome and
/// an actual dispatch decision can never silently disagree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedOutcome {
    /// The feature is `Implemented` or `Partial`: expect a successful call (or a legitimate
    /// business error). `Partial` is treated as `Implemented`-equivalent for verdict purposes;
    /// the `Partial` label is preserved only as reporting detail (Requirement 6).
    Ok,
    /// The feature is `Experimental` with its dynamic-config gate enabled: expect success. Kept
    /// distinct from [`Ok`](Self::Ok) so a report can show that the success is contingent on a
    /// gate being on rather than unconditional.
    OkWhenEnabled,
    /// The feature is `Experimental` with its gate disabled: expect a disabled-state contract
    /// failure (`FAILED_PRECONDITION`) rather than success or `UNIMPLEMENTED`.
    DisabledPrecondition,
    /// The feature is `Stubbed` or `Unsupported`: expect `UNIMPLEMENTED`. The RPC exists on the
    /// wire but Tokeira does not yet honour it.
    Unimplemented,
}

/// The classification of a single observed wire method.
///
/// `resolve` returns this for *every* observed wire path so that no call is ever silently
/// dropped from a coverage report (Requirement 4). The two variants partition observed traffic
/// into "the matrix owns this RPC" ([`Known`](Self::Known)) and "this is outside the matrix's
/// vocabulary" ([`UnknownToMatrix`](Self::UnknownToMatrix), e.g. `AdminService`, gRPC `Health`,
/// or an unparseable path). It is data only — serializable so consumers can embed it directly in
/// a report — and carries no logic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcClassification {
    /// The matrix classifies this RPC. Carries everything a report row needs without a second
    /// lookup: the dotted rpc id, the owning feature id, the feature's state (so the `Partial`
    /// label survives per Requirement 6), and the expected wire outcome.
    ///
    /// `rpc_id` and `feature_id` are `&'static str` because they are borrowed directly from the
    /// `FEATURE_MATRIX` entries (which are `const` for the life of the program); copying them
    /// into owned strings would add allocation for no benefit.
    Known {
        /// The matrix's dotted identifier for the RPC, e.g. `WorkflowService.StartWorkflowExecution`.
        rpc_id: &'static str,
        /// The id of the `FeatureEntry` that owns this RPC.
        feature_id: &'static str,
        /// The owning feature's declared state, preserved as reporting detail.
        state: FeatureState,
        /// The wire outcome the feature's state (and dynamic config, for `Experimental`) implies.
        expected: ExpectedOutcome,
    },
    /// A served wire method the matrix has no claim about. `wire_path` is an *owned* `String`
    /// because it is arbitrary observed input — it does not necessarily correspond to any
    /// `&'static` matrix entry, and the recorder must be able to report a path the matrix has
    /// never heard of.
    UnknownToMatrix {
        /// The raw observed gRPC method path, retained verbatim so the report can name it.
        wire_path: String,
    },
}

/// Classify a single observed gRPC wire method against the compatibility matrix.
///
/// This is the one call a coverage consumer makes per observed RPC: it composes the three
/// submodule pieces — [`normalize::wire_path_to_rpc_id`] to turn the wire path into the matrix's
/// dotted id, [`index::feature_and_rpc_for`] to find the owning [`FeatureEntry`](crate::FeatureEntry), and
/// [`expected::expected_outcome`] to project that entry's state to the wire outcome it implies —
/// into the single [`RpcClassification`] verdict a report row needs.
///
/// It is **total** and never panics (Requirement 4.4): every input wire path yields either
/// [`RpcClassification::Known`] or [`RpcClassification::UnknownToMatrix`], so no observed call is
/// ever silently dropped from a report. Two distinct paths reach `UnknownToMatrix`, and the
/// distinction is deliberately *not* surfaced because a report treats them identically:
///
/// 1. **Unparseable wire path** — normalization returns `None` for anything that is not
///    `/package.Service/Method` (gRPC `Health`, a malformed path, etc.).
/// 2. **Parseable but unclassified** — the path parses to a clean rpc id, but no matrix entry owns
///    it (`AdminService`, or a future proto method observed on the wire before the matrix catches
///    up). A parseable id the matrix does not classify is still unknown *to the matrix*, which is
///    exactly what this verdict means.
///
/// `dynamic_config` and `namespace` are threaded straight through to `expected_outcome` (they only
/// matter for `Experimental` features, whose verdict depends on whether their gate is on); this
/// function reads no configuration of its own.
///
/// The reported `rpc_id` is the matrix's own `&'static str` (recovered via `feature_and_rpc_for`),
/// not the owned `String` produced by normalization, so the `Known` variant can hold a `&'static`
/// id with no allocation — matching its field type without copying.
pub fn resolve(
    wire_path: &str,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> RpcClassification {
    // An unparseable path is reported verbatim rather than dropped: the recorder must be able to
    // feed arbitrary observed paths without guarding (Requirement 4.2).
    let Some(rpc_id) = normalize::wire_path_to_rpc_id(wire_path) else {
        return RpcClassification::UnknownToMatrix {
            wire_path: wire_path.to_owned(),
        };
    };

    // A parseable id the matrix has never heard of is still unknown-to-matrix, not an error: a
    // proto method can appear on the wire before the matrix classifies it (Requirement 4.2).
    let Some((entry, static_rpc_id)) = index::feature_and_rpc_for(&rpc_id) else {
        return RpcClassification::UnknownToMatrix {
            wire_path: wire_path.to_owned(),
        };
    };

    RpcClassification::Known {
        rpc_id: static_rpc_id,
        feature_id: entry.id,
        state: entry.state,
        expected: expected::expected_outcome(entry, dynamic_config, namespace),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FEATURE_MATRIX, dispatch::StaticDynamicConfig, rpc_id_to_wire_path};
    use proptest::prelude::*;

    // Feature: temporal-compatibility-surface, Property 4
    //
    // `resolve` is total: for ARBITRARY input strings it returns a value and never panics. The
    // two-variant return is trivially total by type, so the real content of this property is "the
    // parser/index composition does not panic on adversarial input" — proptest catches a panic as a
    // test failure automatically. A plausible-path strategy exercises the `/pkg.Service/Method`
    // parser more deeply than `.*` alone would, since uniformly random strings almost never hit the
    // method-path shape.
    proptest! {
        #[test]
        fn resolve_is_total_over_arbitrary_wire_paths(
            wire_path in prop_oneof![
                ".*",
                "/[a-z.]{1,20}\\.[A-Za-z]{1,15}/[A-Za-z]{1,20}",
            ],
        ) {
            // The assertion is reaching this line without a panic; the result is one of the two
            // variants by construction. We still bind it to make the call observable and to forbid
            // the compiler from optimising the (pure) call away.
            let classification = resolve(&wire_path, &StaticDynamicConfig::disabled(), Some("default"));
            match classification {
                RpcClassification::Known { .. } | RpcClassification::UnknownToMatrix { .. } => {}
            }
        }
    }

    // Feature: temporal-compatibility-surface, Property 4
    //
    // A KNOWN wire path — one built from a real `FEATURE_MATRIX` rpc via `rpc_id_to_wire_path` —
    // resolves to `Known` with the matching dotted `rpc_id`, confirming the totality property's
    // happy path: matrix-owned traffic is classified as `Known`, never silently dropped to
    // `UnknownToMatrix`.
    proptest! {
        #[test]
        fn resolve_classifies_generated_known_wire_paths_as_known(
            index in 0usize..usize::MAX,
            rpc_index in 0usize..usize::MAX,
        ) {
            // Pick an arbitrary (entry, rpc) pair from the matrix using the generated indices,
            // skipping entries with no rpcs. This drives `resolve` across the whole known surface
            // rather than a single hand-picked rpc.
            let entry = &FEATURE_MATRIX[index % FEATURE_MATRIX.len()];
            prop_assume!(!entry.rpcs.is_empty());
            let rpc_id = entry.rpcs[rpc_index % entry.rpcs.len()];

            let wire_path = rpc_id_to_wire_path(rpc_id)
                .expect("a matrix rpc id maps to a wire path");
            let classification =
                resolve(&wire_path, &StaticDynamicConfig::disabled(), Some("default"));
            match classification {
                RpcClassification::Known {
                    rpc_id: resolved_rpc_id,
                    feature_id,
                    ..
                } => {
                    prop_assert_eq!(resolved_rpc_id, rpc_id);
                    prop_assert_eq!(feature_id, entry.id);
                }
                other => prop_assert!(
                    false,
                    "a generated known wire path must resolve to Known, got {:?}",
                    other
                ),
            }
        }
    }

    // Feature: temporal-compatibility-surface, Property 4
    //
    // Exhaustive companion to the generated-path arm: every rpc in `FEATURE_MATRIX` resolves to
    // `Known` with `rpc_id` equal to that rpc and `feature_id` equal to its owning entry, under the
    // disabled dynamic config. This pins the "no observed (known) RPC is dropped" guarantee across
    // the entire matrix, not just sampled indices.
    #[test]
    fn resolve_classifies_every_matrix_rpc_as_known() {
        for entry in FEATURE_MATRIX {
            for rpc in entry.rpcs {
                let wire_path =
                    rpc_id_to_wire_path(rpc).expect("a matrix rpc id maps to a wire path");
                let classification = resolve(
                    &wire_path,
                    &StaticDynamicConfig::disabled(),
                    Some("default"),
                );
                match classification {
                    RpcClassification::Known {
                        rpc_id, feature_id, ..
                    } => {
                        assert_eq!(rpc_id, *rpc, "resolved rpc_id mismatch for {rpc}");
                        assert_eq!(
                            feature_id, entry.id,
                            "rpc {rpc} resolved to the wrong owning feature"
                        );
                    }
                    other => panic!("rpc {rpc} must resolve to Known, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn classifies_a_known_implemented_rpc() {
        let classification = resolve(
            "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
            &StaticDynamicConfig::disabled(),
            Some("default"),
        );
        match classification {
            RpcClassification::Known {
                rpc_id, feature_id, ..
            } => {
                assert_eq!(rpc_id, "WorkflowService.StartWorkflowExecution");
                assert_eq!(feature_id, "workflow-start");
            }
            other => panic!("expected a Known classification, got {other:?}"),
        }
    }

    #[test]
    fn unparseable_wire_path_is_unknown_to_matrix() {
        let classification = resolve("not-a-path", &StaticDynamicConfig::disabled(), None);
        assert_eq!(
            classification,
            RpcClassification::UnknownToMatrix {
                wire_path: "not-a-path".to_owned(),
            }
        );
    }

    #[test]
    fn parseable_but_unclassified_wire_path_is_unknown_to_matrix() {
        let classification = resolve(
            "/temporal.server.api.adminservice.v1.AdminService/DescribeMutableState",
            &StaticDynamicConfig::disabled(),
            None,
        );
        assert_eq!(
            classification,
            RpcClassification::UnknownToMatrix {
                wire_path: "/temporal.server.api.adminservice.v1.AdminService/DescribeMutableState"
                    .to_owned(),
            }
        );
    }
}
