# Requirements Document

## Introduction

This feature turns the existing `tokeira-compatibility` matrix from a declaration into a queryable
coverage spine. It adds a small, pure set of runtime functions — wire-path normalization, an RPC→feature
index, an expected-outcome projection, and a single `resolve` entry point — that let conformance tooling
turn a stream of observed gRPC calls into a clear, matrix-backed coverage verdict.

It is the first of two ordered steps. This spec owns the matrix coverage API. The sibling
`temporal-functional-conformance` spec consumes it to run Temporal's functional suite against `tokeirad`
and produce the coverage report; the `conformance-harness` (Tier 1) spec reuses the same API. The
additions are purely additive over the existing `FEATURE_MATRIX` const and the vendored descriptor set —
no new crate, no new third-party dependency, no I/O — consistent with the crate's pure contract and
ground-truthed to the vendored protos per AGENTS.md §8.

## Glossary

- **The crate:** `tokeira-compatibility`.
- **Matrix:** `FEATURE_MATRIX`, the existing const slice of `FeatureEntry`.
- **rpc id:** the matrix's dotted identifier, e.g. `WorkflowService.StartWorkflowExecution`.
- **wire path:** the gRPC method path, e.g. `/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution`.
- **Expected outcome:** the wire result a feature's state implies (OK, disabled-precondition, unimplemented).
- **Coverage consumer:** Tier 2 (functional conformance) and Tier 1 (conformance-harness).

## Requirements

### Requirement 1: Wire-path normalization

**User Story:** As a coverage consumer, I want to map an observed gRPC method path to the matrix's rpc
id (and back), so that observed calls join exactly against matrix claims rather than by fuzzy matching.

#### Acceptance Criteria

1. THE crate SHALL provide a function mapping a gRPC wire method path to its matrix rpc id.
2. THE crate SHALL provide the inverse mapping from rpc id to wire method path.
3. THE mapping SHALL be derived from the vendored proto/descriptor set, not hardcoded, and SHALL NOT
   read generated artifacts under `target/`.
4. WHEN a wire path does not parse as `/package.Service/Method`, THE normalization SHALL return a
   not-recognized result rather than panicking.

### Requirement 2: Runtime RPC→feature index

**User Story:** As a coverage consumer, I want a runtime lookup from rpc id to its feature entry, so
that each observed RPC resolves deterministically to its feature and state.

#### Acceptance Criteria

1. THE crate SHALL provide a runtime function `feature_for_rpc(rpc_id) -> Option<&FeatureEntry>`.
2. WHEN an rpc id is not classified by the matrix, THE function SHALL return `None`.
3. THE index SHALL resolve every rpc id classified in `FEATURE_MATRIX` to exactly one feature entry.

### Requirement 3: Expected-outcome projection

**User Story:** As a coverage consumer, I want to know what wire outcome a feature's state implies, so
that I can judge an observed status code as agreeing with or contradicting the matrix claim.

#### Acceptance Criteria

1. THE crate SHALL provide a function projecting a feature entry plus a dynamic-config reader to an
   expected outcome (OK, OK-when-enabled, disabled-precondition, or unimplemented).
2. THE expected outcome SHALL be consistent with the existing `dispatch_rpc` policy for the same inputs
   (single policy, two views).
3. WHERE a feature is `Experimental`, THE expected outcome SHALL depend on its dynamic-config gate:
   enabled ⟹ OK, disabled ⟹ disabled-precondition.
4. WHERE a feature is `Stubbed` or `Unsupported`, THE expected outcome SHALL be unimplemented.
5. WHERE a feature is `Implemented` or `Partial`, THE expected outcome SHALL be OK.

### Requirement 4: Resolve and the unknown-to-matrix bucket

**User Story:** As a coverage consumer, I want a single call that classifies any observed wire method,
so that no observed RPC is ever silently dropped from the report.

#### Acceptance Criteria

1. THE crate SHALL provide a `resolve(wire_path)` function returning a classification for any input.
2. WHEN an observed wire method is outside the matrix's vocabulary (e.g. `AdminService`, gRPC `Health`,
   or a service beyond Workflow/Operator), THE classification SHALL be an explicit unknown-to-matrix
   result, not an omission or error.
3. WHEN an observed wire method is matrix-owned, THE classification SHALL carry its rpc id, feature id,
   state, and expected outcome.
4. THE `resolve` function SHALL be total: it returns a value for every input and never panics.

### Requirement 5: Purity and additive contract

**User Story:** As a maintainer, I want these additions to preserve the crate's pure, dependency-light
contract, so that kernel, edge, and CLI can keep depending on it freely.

#### Acceptance Criteria

1. THE additions SHALL introduce no I/O and no async.
2. THE additions SHALL introduce no new third-party dependency and no new crate.
3. THE additions SHALL NOT modify the content of `FEATURE_MATRIX`; they SHALL be projections over it.
4. THE result types SHALL be serializable so consumers can embed them in a coverage report.

### Requirement 6: Taxonomy reconciliation

**User Story:** As a coverage consumer, I want one expected-outcome function usable by both tiers, so
that the matrix's five states and the harness's four-state axis-K do not require two policies.

#### Acceptance Criteria

1. THE expected-outcome projection SHALL treat `Partial` as `Implemented`-equivalent (expect OK) for
   verdict purposes.
2. THE `Partial` label SHALL be preserved as reporting detail and SHALL NOT be erased from the matrix.
