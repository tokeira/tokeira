# Implementation Plan: ECS Deployment Correctness

## Overview

Fix five deployment correctness bugs in the ECS platform layer that cause silent failures across the deployment lifecycle: task definition rollout gap, missing execution role, environment variable mapping drop, resource arithmetic underflow, and VPC endpoint inventory drift.

Target files:

- `crates/tokeira-aws/src/resources/ecs_service.rs` — fix `diff()` and `update()` for task definition rollout, add `execution_role_dependency` to `TaskDefinitionSpec`, add `environment` field to AWS-layer `ContainerSpec`
- `platforms/ecs/src/services.rs` — replace `saturating_sub` with checked subtraction + error in `workload_from_parts`
- `platforms/ecs/src/modules/services.rs` — map `environment` in `to_aws_container()`, wire execution role dependency in `to_aws_task_definition()`
- `platforms/ecs/src/modules/networking.rs` — add DSQL endpoints to `required_endpoint_specs()`

Ordering rationale: exploration tests first (confirm all five bugs exist on unfixed code), preservation tests second (capture non-buggy behavior baseline), then implementation of all five fixes, then verification that exploration tests pass and preservation tests still pass, then full workspace checkpoint.

## Tasks

- [ ] 1. Write bug condition exploration tests
  - **Property 1: Bug Condition** - ECS Deployment Correctness Defects
  - **IMPORTANT**: Write these property-based tests BEFORE implementing the fixes
  - **CRITICAL**: These tests MUST FAIL on unfixed code — failure confirms the bugs exist
  - **DO NOT attempt to fix the tests or the code when they fail**
  - **NOTE**: These tests encode the expected behavior — they will validate the fixes when they pass after implementation
  - **GOAL**: Surface counterexamples that demonstrate each of the five bugs exists
  - **Scoped PBT Approach**: Each property targets a specific bug condition from the design
  - Property 1a — Task Definition Rollout Gap: Generate random task definition ARN pairs where `current_td_arn != desired_td_arn`. Assert `EcsServiceResource.diff()` returns `InternalChange::Update`. On unfixed code, `diff()` unconditionally returns `NoChange` — test FAILS confirming bug exists.
  - Property 1b — Missing Execution Role: Generate `TaskDefinitionSpec` with containers having non-empty `secrets` vectors. Assert `execution_role_dependency` is `Some`. On unfixed code, the field doesn't exist — test FAILS confirming bug exists.
  - Property 1c — Container Environment Not Mapped: Generate `ContainerSpec` with non-empty `environment` vectors. Assert `to_aws_container(spec).environment.len() == spec.environment.len()`. On unfixed code, the AWS-layer struct has no `environment` field — test FAILS confirming bug exists.
  - Property 1d — Zero-Resource Primary Container: Generate `(task_cpu, sidecar_cpu, wait_cpu)` tuples where `task_cpu <= sidecar_cpu + wait_cpu`. Assert `workload_from_parts()` returns `Err`. On unfixed code, `saturating_sub` produces zero without error — test FAILS confirming bug exists.
  - Property 1e — VPC Endpoint Inventory Drift: Assert `required_endpoint_specs(region).service_names == config::required_vpc_endpoints(region)` as sets. On unfixed code, networking module is missing `dsql` and `dsql-control` — test FAILS confirming bug exists.
  - Run tests on UNFIXED code
  - **EXPECTED OUTCOME**: All five property tests FAIL (this is correct — it proves the bugs exist)
  - Document counterexamples found to understand root causes
  - Mark task complete when tests are written, run, and failures are documented
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ] 2. Write preservation property tests (BEFORE implementing fixes)
  - **Property 2: Preservation** - Non-Buggy ECS Deployment Behavior
  - **IMPORTANT**: Follow observation-first methodology
  - **IMPORTANT**: Use `proptest` as required by AGENTS.md
  - Observe behavior on UNFIXED code for non-buggy inputs (cases where bug conditions do NOT hold)
  - Property 2a — Service Diff Preservation: Generate random task definition ARN pairs where `current_td_arn == desired_td_arn`. Observe `diff()` returns `NoChange` on unfixed code. Write property asserting this holds for all matching ARN pairs.
  - Property 2b — No-Secrets Task Definition Preservation: Generate `TaskDefinitionSpec` where all containers have empty `secrets` and use public images. Observe no execution role is required on unfixed code. Write property asserting `execution_role_dependency` is `None`.
  - Property 2c — Empty Environment Preservation: Generate `ContainerSpec` with empty `environment` vector. Observe `to_aws_container()` produces valid container definition with no environment entries. Write property asserting output has no environment entries.
  - Property 2d — Sufficient Resources Preservation: Generate `(task_cpu, task_memory, sidecar_cpu, sidecar_memory, wait_cpu, wait_memory)` tuples where `task_cpu > sidecar_cpu + wait_cpu` AND `task_memory > sidecar_memory + wait_memory`. Observe `workload_from_parts()` returns `Ok` with `primary.cpu == task_cpu - (sidecar_cpu + wait_cpu)` and `primary.memory_mb == task_memory - (sidecar_memory + wait_memory)`. Write property asserting correct subtraction.
  - Run tests on UNFIXED code
  - **EXPECTED OUTCOME**: All preservation tests PASS (this confirms baseline behavior to preserve)
  - Mark task complete when tests are written, run, and passing on unfixed code
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 3. Fix for ECS deployment correctness bugs

  - [ ] 3.1 Fix Bug 1 — Task Definition Rollout Gap
    - In `crates/tokeira-aws/src/resources/ecs_service.rs`: store `task_definition_arn` in `ResourceState.properties` during `create()`
    - Fix `EcsServiceResource.diff()` (line 352): read task definition dependency's current `physical_id` from `ProvisionContext`, compare against `current.properties["task_definition_arn"]`, return `InternalChange::Update` if they differ
    - Fix `EcsServiceResource.update()` (line 319): call `UpdateService` with new task definition ARN and `force_new_deployment(true)`, return updated `ResourceState` with new ARN in properties
    - _Bug_Condition: isBugCondition_1(input) where current_td_arn != desired_td_arn AND diff() returns NoChange_
    - _Expected_Behavior: diff() returns InternalChange::Update, update() calls UpdateService with new ARN_
    - _Preservation: When current_td_arn == desired_td_arn, diff() continues to return NoChange_
    - _Requirements: 2.1, 3.1_

  - [ ] 3.2 Fix Bug 2 — Missing Execution Role
    - Add `execution_role_dependency: Option<ResourceId>` field to `TaskDefinitionSpec` in `crates/tokeira-aws/src/resources/ecs_service.rs` (line 24 area)
    - In `TaskDefinitionResource.create()` (line 112): if `execution_role_dependency` is `Some`, resolve role ARN from state and set `.execution_role_arn(role_arn)` on `RegisterTaskDefinition` request
    - In `platforms/ecs/src/modules/services.rs` `to_aws_task_definition()`: accept `execution_role_dependency: Option<ResourceId>` parameter
    - In `ServicesModule.resources()`: for workloads with secrets or private ECR images, create execution role resource and pass its `ResourceId` as execution role dependency
    - _Bug_Condition: isBugCondition_2(input) where (has_secrets OR uses_private_ecr) AND execution_role_dependency IS NONE_
    - _Expected_Behavior: TaskDefinitionSpec carries execution_role_dependency, RegisterTaskDefinition includes execution_role_arn_
    - _Preservation: When no secrets and public images, no execution role required_
    - _Requirements: 2.2, 3.2_

  - [ ] 3.3 Fix Bug 3 — Container Environment Not Mapped
    - Add `environment: Vec<EnvironmentSpec>` field and `EnvironmentSpec { name: String, value: String }` struct to AWS-layer `ContainerSpec` in `crates/tokeira-aws/src/resources/ecs_service.rs`
    - In `container_definition()`: map `spec.environment` to `.set_environment(Some(...))` using `aws_sdk_ecs::types::KeyValuePair`
    - In `to_aws_container()` (`platforms/ecs/src/modules/services.rs`, line 108): map platform-layer `spec.environment` entries to `aws_ecs::EnvironmentSpec` structs
    - _Bug_Condition: isBugCondition_3(input) where input.environment IS NOT EMPTY AND to_aws_container(input).environment IS EMPTY_
    - _Expected_Behavior: to_aws_container produces AWS ContainerSpec with environment field containing all { name, value } pairs_
    - _Preservation: When environment is empty, valid container definition with no environment entries_
    - _Requirements: 2.3, 3.3_

  - [ ] 3.4 Fix Bug 4 — Resource Sizing Uses saturating_sub
    - In `platforms/ecs/src/services.rs` `workload_from_parts` (line 389): replace `saturating_sub` with checked subtraction
    - If `task_cpu <= sidecar_cpu + wait_cpu` OR `task_memory_mb <= sidecar_memory + wait_memory`, return an `IacError` with descriptive message indicating task-level resources are insufficient for sidecar overhead
    - Change `workload_from_parts` return type from `EcsWorkload` to `Result<EcsWorkload, IacError>` and propagate through callers
    - Use `thiserror` for the error variant (library crate per AGENTS.md)
    - _Bug_Condition: isBugCondition_4(input) where primary_cpu == 0 OR primary_memory == 0 after saturating_sub_
    - _Expected_Behavior: workload_from_parts returns Err when resources insufficient_
    - _Preservation: When task resources exceed sidecar overhead, correct subtraction result identical to saturating_sub_
    - _Requirements: 2.4, 3.4_

  - [ ] 3.5 Fix Bug 5 — VPC Endpoint Inventory Drift
    - In `platforms/ecs/src/modules/networking.rs` `required_endpoint_specs()` (line 168): add `("dsql", "dsql", EndpointType::Interface)` and `("dsql-control", "dsql-control", EndpointType::Interface)` to the endpoint list
    - Add inventory completeness unit test: assert `required_endpoint_specs(region).map(|e| e.service_name)` equals `config::required_vpc_endpoints(region)` as a set
    - _Bug_Condition: isBugCondition_5(input) where expected endpoints != actual endpoints (missing dsql, dsql-control)_
    - _Expected_Behavior: Union of all module-created endpoints equals config::required_vpc_endpoints(region)_
    - _Preservation: Existing module endpoint ownership unchanged_
    - _Requirements: 2.5, 3.5_

  - [ ] 3.6 Verify bug condition exploration tests now pass
    - **Property 1: Expected Behavior** - ECS Deployment Correctness Fixes Validated
    - **IMPORTANT**: Re-run the SAME tests from task 1 — do NOT write new tests
    - The tests from task 1 encode the expected behavior for all five bugs
    - When these tests pass, it confirms the expected behavior is satisfied
    - Run bug condition exploration tests from step 1
    - **EXPECTED OUTCOME**: All five property tests PASS (confirms bugs are fixed)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [ ] 3.7 Verify preservation tests still pass
    - **Property 2: Preservation** - Non-Buggy Behavior Unchanged
    - **IMPORTANT**: Re-run the SAME tests from task 2 — do NOT write new tests
    - Run preservation property tests from step 2
    - **EXPECTED OUTCOME**: All preservation tests PASS (confirms no regressions)
    - Confirm all tests still pass after fixes (no regressions introduced)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 4. Checkpoint - Ensure all tests pass
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

- Tasks 1 and 2 are independent exploration/preservation tests that can be written in parallel before any fixes
- Tasks 3.1–3.5 are independent bug fixes that can be implemented in parallel after tests are written
- Tasks 3.6 and 3.7 re-run existing tests to verify fixes work and don't regress — they depend on all fixes being complete
- Task 4 is the final workspace checkpoint depending on all prior tasks
- All property-based tests use `proptest` per AGENTS.md conventions
- Error types in library crates (`tokeira-aws`, `platforms/ecs`) use `thiserror` per AGENTS.md
- The `workload_from_parts` return type change (Bug 4) will require propagating `Result` through callers in `platforms/ecs`
