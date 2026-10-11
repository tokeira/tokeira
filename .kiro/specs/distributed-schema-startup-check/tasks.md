# Implementation Plan

## Overview

First show the defect, with tests that fail on the current code. Then:
- check the schema at distributed startup, refusing before anything starts;
- honour the migration policy setting;
- let `tkr schema` follow the deployment's `tokeirad.toml`;
- prove it live, on an ephemeral cluster.

## Tasks

- [x] 1. Exploration, before the fix
  - [x] 1.1 Write the tests:
    - the live test of the design's Testing Strategy;
    - the offline test of a distributed configuration with `automatic`;
    - the offline test of `tkr schema setup` for a deployment whose metadata records in-memory storage and whose `tokeirad.toml` selects DSQL.
    - _Requirements: 1.1-1.4_
  - [x] 1.2 Run them on the current code, the live test on an ephemeral cluster with two ephemeral DynamoDB tables, and confirm each fails as 1.1 to 1.4 predict
    - _Requirements: 1.1-1.4_
    - DONE. On the code before the fix, on an ephemeral cluster with two ephemeral DynamoDB tables, a distributed node started and bound its listener on a schema one migration behind its target and on one past what its release reads, while the target schema started and served. Offline, a distributed configuration with `automatic` went on to need an endpoint. `tkr schema setup` refused, with "schema commands require dsql storage", a deployment whose metadata records in-memory storage and whose `tokeirad.toml` selects DSQL.

- [x] 2. The check
  - [x] 2.1 Refuse `automatic` in `build_and_serve`'s DSQL branch, before any I/O
    - _Requirements: 2.6_
  - [x] 2.2 Add `check_distributed_schema` and the mapping of the assessment's decision. Run the check after `DsqlStore::connect` and before `build_dsql_stack`, and shut the store down on a refusal.
    - _Requirements: 2.1-2.5_

- [x] 3. Document what each value of `infrastructure.dsql.migration_policy` does, on the field and in its catalog entry
  - _Requirements: 2.6_

- [x] 4. Make `tkr schema` decide DSQL from the deployment's `tokeirad.toml`
  - _Requirements: 2.7, 3.2_

- [x] 5. Write property tests for Properties 1, 3 and 4, and check Property 2 in the live test
  - **Property 1: A node starts only on a schema its release can run on**
  - **Property 2: A refused start leaves nothing started**
  - **Property 3: The policy setting selects the check**
  - **Property 4: `tkr schema` follows the server configuration**
  - **Validates: Requirements 2.1-2.7, 3.2**

- [x] 6. Write the unit tests listed in the design
  - _Requirements: 2.3, 2.4_

- [x] 7. Update the documentation:
  - the deployment configuration guide's schema paragraph;
  - the live test's row in `docs/testing/dsql-live-suites.md`.
  - _Requirements: 2.3, 2.7_

- [x] 8. Align managed-embedded-dsql (criteria 5.3, 5.12 and a new 5.14, Evidence item 9, the design's paragraph on the policy and its Property 1) and tkr-cli (Requirement 7.1)
  - _Requirements: 2.6, 2.7_

- [x] 9. Checkpoint: the exploration tests pass, each negative control fails its test, the live test passes on an ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes
  - DONE at `e8a7d8a2`. The exploration tests pass. Properties 1, 3 and 4 pass offline, and Property 2 passes in the live suite, which passed on an ephemeral cluster with two ephemeral DynamoDB tables. Each of the seven negative controls failed its tests: four offline and three live. The bar passed (3,848 tests passed, 2 skipped).

## Notes

- Property tests use `proptest`, tagged `// Feature: distributed-schema-startup-check, Property N: <title>`.
- The live run follows `docs/testing/dsql-live-suites.md`: an ephemeral cluster and two ephemeral DynamoDB tables, deleted when the run ends.
- The release already says "Upgrade with every node stopped.", so this change adds no second note.
