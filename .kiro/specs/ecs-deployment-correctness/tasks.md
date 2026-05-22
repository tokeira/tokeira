# Implementation Plan: ECS Deployment Correctness

## Overview

Fix five deployment correctness bugs in the ECS platform layer that cause silent failures across the deployment lifecycle: task definition rollout gap, missing execution role, resource arithmetic underflow, and VPC endpoint inventory drift. The environment variable mapping defect has already been fixed in the current codebase; this plan keeps regression coverage for it.

Target files:

- `crates/tokeira-aws/src/resources/ecs_service.rs` — fix `diff()` and `update()` for task definition rollout, add `execution_role_dependency` to `TaskDefinitionSpec`, preserve existing environment mapping regression coverage
- `platforms/ecs/src/config.rs`, `platforms/ecs/src/services.rs` — validate service resource sufficiency in `EcsConfig::validate()` and replace `saturating_sub` with plain subtraction after validation
- `platforms/ecs/src/modules/services.rs` — wire execution role dependency in `to_aws_task_definition()`, preserve existing environment mapping
- `platforms/ecs/src/modules/networking.rs`, `platforms/ecs/src/modules/dsql.rs`, `platforms/ecs/src/config.rs` — add endpoint inventory tests for static networking endpoints and DSQL logical endpoint categories

Ordering rationale: tests must pass at every commit boundary. For bugs that are already fixed, tests are regression coverage and pass immediately. For active bugs, write each bug-condition test in the same task as its fix. Preservation tests capture non-buggy behavior before and after implementation, then the full checkpoint verifies the workspace.

## Tasks

- [x] 1. Establish regression and preservation test boundaries
  - **Property 1: Bug Condition** - ECS Deployment Correctness Defects
  - **IMPORTANT**: Tests must pass at every commit boundary. Do not commit failing exploration tests separately.
  - For Bug 3, write the regression test now because the fix already exists and the test passes on the current codebase.
  - For Bugs 1, 2, 4, and 5, write the bug-condition test in the same task as the fix (`3.1`, `3.2`, `3.4`, `3.5`).
  - **GOAL**: Encode the bug conditions as tests without leaving the repository in a failing state.
  - **Scoped PBT Approach**: Each property targets a specific bug condition from the design
  - Property 1a — Task Definition Rollout Gap: Generate random task definition ARN pairs where `current_td_arn != desired_td_arn`. Assert `EcsServiceResource.diff()` returns `InternalChange::Update`. Add this test with task `3.1`.
  - Property 1b — Missing Execution Role: Generate ECS service resources through `ServicesModule.resources()` and assert module-produced task definitions with non-empty `secrets` vectors have `execution_role_dependency = Some(...)`. Add this test with task `3.2`.
  - Property 1c — Container Environment Mapping Regression: Generate `ContainerSpec` with non-empty `environment` vectors. Assert `to_aws_container(spec).environment.len() == spec.environment.len()`. This test passes on the current codebase and serves as regression coverage confirming the fix already exists.
  - Property 1d — Zero-Resource Primary Container: Generate `(task_cpu, sidecar_cpu, wait_cpu)` tuples where `task_cpu <= sidecar_cpu + wait_cpu`. Assert `workload_from_parts()` returns `Err`. Add this test with task `3.4`.
  - Property 1e — VPC Endpoint Inventory Drift: Assert `networking::required_endpoint_specs(region)` covers all static/generic endpoints from `config::required_vpc_endpoints(region)` excluding DSQL entries. Separately assert `DsqlModule::resources()` produces VPC endpoint resources for logical categories `dsql-control` and `dsql-connection`. Do not assert exact AWS service names for DSQL connection endpoints; the connection service name is resolved dynamically at apply time. Add this test with task `3.5`.
  - Mark task complete when the regression test for already-fixed Bug 3 is written and the active-bug tests are assigned to their implementation tasks.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 2. Write preservation property tests (BEFORE implementing fixes)
  - **Property 2: Preservation** - Non-Buggy ECS Deployment Behavior
  - **IMPORTANT**: Follow observation-first methodology
  - **IMPORTANT**: Use `proptest` as required by AGENTS.md
  - Cover non-buggy inputs (cases where bug conditions do NOT hold) and keep these tests passing throughout implementation
  - Property 2a — Service Diff Preservation: Generate random task definition ARN pairs where `current_td_arn == desired_td_arn`. Write property asserting `diff()` returns `NoChange` for all matching ARN pairs.
  - Property 2b — No-Secrets Task Definition Preservation: Generate module-produced task definitions where all containers have empty `secrets` and use public images. Write property asserting `execution_role_dependency` is `None`.
  - Property 2c — Empty Environment Preservation: Generate `ContainerSpec` with empty `environment` vector. Write property asserting `to_aws_container()` produces a valid container definition with no environment entries.
  - Property 2d — Sufficient Resources Preservation: Generate `(task_cpu, task_memory, sidecar_cpu, sidecar_memory, wait_cpu, wait_memory)` tuples where `task_cpu > sidecar_cpu + wait_cpu` AND `task_memory > sidecar_memory + wait_memory`. Observe `workload_from_parts()` returns `Ok` with `primary.cpu == task_cpu - (sidecar_cpu + wait_cpu)` and `primary.memory_mb == task_memory - (sidecar_memory + wait_memory)`. Write property asserting correct subtraction.
  - Run tests after each task that touches the relevant behavior
  - **EXPECTED OUTCOME**: All preservation tests PASS (this confirms baseline behavior to preserve)
  - Mark task complete when tests are written, run, and passing
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 3. Fix for ECS deployment correctness bugs

  - [x] 3.1 Fix Bug 1 — Task Definition Rollout Gap
    - In `crates/tokeira-aws/src/resources/ecs_service.rs`: store `task_definition_arn` in `ResourceState.properties` during `create()`
    - Fix `EcsServiceResource.diff()` (line 352): read task definition dependency's current `physical_id` from `ProvisionContext`, compare against `current.properties["task_definition_arn"]`, return `InternalChange::Update` if they differ
    - Fix `EcsServiceResource.update()` (line 319): call `UpdateService` with new task definition ARN and `force_new_deployment(true)`, return updated `ResourceState` with new ARN in properties
    - Handle missing `task_definition_arn` in existing state: if the field is absent in `current.properties`, treat as needing update so apply backfills the field
    - Add a test: given service state without `task_definition_arn`, `diff()` returns `InternalChange::Update`
    - _Bug_Condition: isBugCondition_1(input) where current_td_arn != desired_td_arn AND diff() returns NoChange_
    - _Expected_Behavior: diff() returns InternalChange::Update, update() calls UpdateService with new ARN_
    - _Preservation: When current_td_arn == desired_td_arn, diff() continues to return NoChange_
    - _Requirements: 2.1, 3.1_

  - [x] 3.2 Fix Bug 2 — Missing Execution Role
    - Add `execution_role_dependency: Option<ResourceId>` field to `TaskDefinitionSpec` in `crates/tokeira-aws/src/resources/ecs_service.rs` (line 24 area)
    - In `TaskDefinitionResource.create()` (line 112): if `execution_role_dependency` is `Some`, resolve role ARN from state and set `.execution_role_arn(role_arn)` on `RegisterTaskDefinition` request
    - In `platforms/ecs/src/modules/services.rs` `to_aws_task_definition()`: accept `execution_role_dependency: Option<ResourceId>` parameter
    - In `ServicesModule.resources()`: for workloads with secrets or private ECR images, create execution role resource and pass its `ResourceId` as execution role dependency
    - Test that `ServicesModule.resources()` produces task definitions with `execution_role_dependency = Some(...)` for workloads that have secrets (Grafana)
    - Test that module-produced task definitions without secrets have `execution_role_dependency = None`
    - Do NOT write a property test over arbitrary `TaskDefinitionSpec` values; the invariant is enforced at the module level, not the raw struct level
    - If `TaskDefinitionResource.create()` receives a raw spec with secrets/private ECR and no execution role, emit a `tracing::warn!` indicating the task definition may fail at runtime; do not reject the spec
    - Preserve the existing `task_role_dependency`; it provides in-container credentials for DSQL auth, Alloy SSM config reads, and ECS Exec
    - Verify Grafana task definition has both `execution_role_dependency` for secret injection and `task_role_dependency` for in-container access
    - Add a test confirming the Alloy sidecar's SSM parameter read remains authorized through the task role, not the execution role
    - _Bug_Condition: isBugCondition_2(input) where (has_secrets OR uses_private_ecr) AND execution_role_dependency IS NONE_
    - _Expected_Behavior: TaskDefinitionSpec carries execution_role_dependency, RegisterTaskDefinition includes execution_role_arn_
    - _Preservation: When no secrets and public images, no execution role required_
    - _Requirements: 2.2, 3.2_

  - [x] 3.3 Verify Bug 3 — Container Environment Mapping Already Fixed
    - Confirm the current code already has `environment: Vec<EnvironmentSpec>` and `EnvironmentSpec { name: String, value: String }` in AWS-layer `ContainerSpec`
    - Confirm `container_definition()` maps `spec.environment` to `.set_environment(Some(...))` using `aws_sdk_ecs::types::KeyValuePair`
    - Confirm `to_aws_container()` maps platform-layer `spec.environment` entries to `aws_ecs::EnvironmentSpec` structs
    - Write or keep a regression test confirming `to_aws_container` maps environment variables correctly
    - No implementation change is expected for this bug; the fix already exists in `crates/tokeira-aws/src/resources/ecs_service.rs` and `platforms/ecs/src/modules/services.rs`
    - _Bug_Condition: isBugCondition_3(input) where input.environment IS NOT EMPTY AND to_aws_container(input).environment IS EMPTY_
    - _Expected_Behavior: to_aws_container produces AWS ContainerSpec with environment field containing all { name, value } pairs_
    - _Preservation: When environment is empty, valid container definition with no environment entries_
    - _Requirements: 2.3, 3.3_

  - [x] 3.4 Fix Bug 4 — Resource Sizing Uses saturating_sub
    - Add resource sufficiency checks to `EcsConfig::validate()`
    - For each service in `ServiceConfigs`, compute sidecar/init overhead (Alloy CPU/memory plus wait-for CPU/memory) and verify `service.cpu > overhead_cpu` and `service.memory_mb > overhead_memory`
    - Return `EcsConfigError::InsufficientTaskResources { service, task_cpu, overhead_cpu, task_memory, overhead_memory }` on failure
    - Replace `saturating_sub` in `workload_from_parts` with plain subtraction; this is safe because config validation already proved the result is positive
    - Keep `workload_from_parts` and `Deployment::services()` infallible after validated config
    - Add tests: (a) config with insufficient CPU fails validation, (b) config with sufficient CPU passes validation, (c) `workload_from_parts` produces correct subtraction after validated config
    - _Bug_Condition: isBugCondition_4(input) where primary_cpu == 0 OR primary_memory == 0 after saturating_sub_
    - _Expected_Behavior: `EcsConfig::validate()` returns `EcsConfigError::InsufficientTaskResources` when resources are insufficient_
    - _Preservation: When task resources exceed sidecar overhead, correct subtraction result identical to saturating_sub_
    - _Requirements: 2.4, 3.4_

  - [x] 3.5 Fix Bug 5 — VPC Endpoint Inventory Drift
    - Write a two-layer endpoint inventory test
    - Layer 1: assert `networking::required_endpoint_specs(region)` covers all static/generic endpoints from `config::required_vpc_endpoints(region)` excluding DSQL entries
    - Layer 2: assert `DsqlModule::resources()` produces VPC endpoint resources for both `dsql-control` and `dsql-connection` logical categories
    - Do NOT assert exact AWS service names for DSQL; the connection endpoint service name is resolved dynamically by `GetVpcEndpointServiceName`
    - Do NOT move DSQL endpoints into the networking module; they are owned by `DsqlModule`
    - _Bug_Condition: isBugCondition_5(input) where expected endpoints != actual endpoints (missing dsql, dsql-control)_
    - _Expected_Behavior: Static networking endpoints match the static expected endpoint subset, and DSQL module resources cover the DSQL logical endpoint categories_
    - _Preservation: Existing module endpoint ownership unchanged_
    - _Requirements: 2.5, 3.5_

  - [x] 3.6 Verify bug condition exploration tests now pass
    - **Property 1: Expected Behavior** - ECS Deployment Correctness Fixes Validated
    - **IMPORTANT**: Re-run the SAME tests from task 1 — do NOT write new tests
    - The tests from task 1 encode the expected behavior for all five bugs
    - When these tests pass, it confirms the expected behavior is satisfied
    - Run bug condition exploration tests from step 1
    - **EXPECTED OUTCOME**: All five property tests PASS (confirms bugs are fixed)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x] 3.7 Verify preservation tests still pass
    - **Property 2: Preservation** - Non-Buggy Behavior Unchanged
    - **IMPORTANT**: Re-run the SAME tests from task 2 — do NOT write new tests
    - Run preservation property tests from step 2
    - **EXPECTED OUTCOME**: All preservation tests PASS (confirms no regressions)
    - Confirm all tests still pass after fixes (no regressions introduced)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 4. Checkpoint - Ensure all tests pass
  - Run `cargo +nightly fmt --all --check` to verify formatting
  - Run `cargo lint` to verify clippy passes on workspace + all targets
  - Run `cargo test-lint` to verify clippy passes on tests
  - Run `cargo test --workspace` to verify all unit and property tests pass
  - Verify no `.unwrap()` outside test code
  - Verify `thiserror` used for new error types in library crates
  - Verify `tracing` used for any new structured logging
  - Ensure all tests pass, ask the user if questions arise.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1", "2"] },
    { "id": 1, "tasks": ["3.1", "3.2", "3.3", "3.4", "3.5"] },
    { "id": 2, "tasks": ["3.6", "3.7"] },
    { "id": 3, "tasks": ["4"] }
  ]
}
```

## Notes

- Task 1 defines where bug-condition tests live; active-bug tests are written with their fixes to avoid committing failing tests
- Task 2 preservation tests can be written before or alongside fixes as long as they pass at each commit boundary
- Tasks 3.1–3.5 are independent bug fixes; each active fix includes its own bug-condition test
- Tasks 3.6 and 3.7 re-run existing tests to verify fixes work and don't regress — they depend on all fixes being complete
- Task 4 is the final workspace checkpoint depending on all prior tasks
- All property-based tests use `proptest` per AGENTS.md conventions
- Error types in library crates (`tokeira-aws`, `platforms/ecs`) use `thiserror` per AGENTS.md
- Resource sufficiency validation belongs in `EcsConfig::validate()` so `workload_from_parts` and `Deployment::services()` remain infallible after validated config
