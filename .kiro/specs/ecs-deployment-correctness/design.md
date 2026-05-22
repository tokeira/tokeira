# ECS Deployment Correctness Bugfix Design

## Overview

The ECS platform layer has five deployment correctness bugs that cause silent failures across the deployment lifecycle. The bugs span two crates (`tokeira-aws` and `platforms/ecs`) and affect task definition rollout, execution role assignment, environment variable mapping, resource arithmetic, and VPC endpoint inventory completeness. The fix strategy is minimal and targeted: each bug has a well-defined condition, a clear expected behavior, and a preservation boundary that ensures non-buggy paths remain unchanged.

## Glossary

- **Bug_Condition (C)**: The set of inputs/states that trigger one of the five deployment correctness defects
- **Property (P)**: The desired correct behavior when the bug condition holds — services update, roles are assigned, environment is mapped, resources are validated, endpoints are complete
- **Preservation**: Existing behavior for non-buggy inputs that must remain unchanged by the fixes
- **EcsServiceResource**: The IaC resource in `crates/tokeira-aws/src/resources/ecs_service.rs` that manages ECS service create/update/delete lifecycle
- **TaskDefinitionResource**: The IaC resource that registers new task definition revisions with ECS
- **ProvisionContext**: The execution context carrying resource state and AWS client handles during IaC operations
- **ResourceState**: The persisted state of a provisioned resource, including `physical_id` and `properties`
- **saturating_sub**: Rust's subtraction that clamps at zero instead of panicking on underflow

## Bug Details

### Bug 1: Task Definition Rollout Gap

The bug manifests when a task definition is updated (new image, config change) but the ECS service continues running the old revision. `EcsServiceResource.diff()` unconditionally returns `InternalChange::NoChange` and `update()` returns the current state unchanged, so the IaC engine never calls `UpdateService` with the new task definition ARN.

**Formal Specification:**
```
FUNCTION isBugCondition_1(input)
  INPUT: input of type { current_service_state: ResourceState, task_definition_state: ResourceState }
  OUTPUT: boolean

  current_td_arn := current_service_state.properties["task_definition_arn"]
                    OR infer from task_definition_dependency physical_id at create time
  desired_td_arn := task_definition_state.physical_id

  RETURN current_td_arn != desired_td_arn
         AND EcsServiceResource.diff() returns NoChange
END FUNCTION
```

### Bug 2: Missing Execution Role

The bug manifests when a task definition includes containers with `secrets` (Secrets Manager references) or uses private ECR images, but no execution role is set on the `RegisterTaskDefinition` call. ECS requires an execution role with `secretsmanager:GetSecretValue` and ECR pull permissions for the ECS agent to inject secrets and pull images.

**Formal Specification:**
```
FUNCTION isBugCondition_2(input)
  INPUT: input of type TaskDefinitionSpec
  OUTPUT: boolean

  has_secrets := ANY container IN input.containers WHERE container.secrets IS NOT EMPTY
  uses_private_ecr := ANY container IN input.containers
                      WHERE container.image MATCHES private ECR pattern

  RETURN (has_secrets OR uses_private_ecr)
         AND input.execution_role_dependency IS NONE
END FUNCTION
```

### Bug 3: Container Environment Not Mapped

The bug manifests when a `ContainerSpec` in `platforms/ecs` has a non-empty `environment` vector but `to_aws_container` in `platforms/ecs/src/modules/services.rs` does not map it to the AWS-layer `ContainerSpec`. The AWS-layer struct in `crates/tokeira-aws` lacks an `environment` field entirely, so the mapping silently drops all plain environment variables.

**Formal Specification:**
```
FUNCTION isBugCondition_3(input)
  INPUT: input of type platforms::ecs::ContainerSpec
  OUTPUT: boolean

  RETURN input.environment IS NOT EMPTY
         AND to_aws_container(input).environment IS EMPTY (field missing)
END FUNCTION
```

### Bug 4: Resource Sizing Uses saturating_sub

The bug manifests when task-level CPU or memory is less than or equal to the sum of sidecar (Alloy) and init-container (wait-for) reservations. `saturating_sub` produces zero instead of an error, so the primary container is created with `cpu: 0` or `memory_mb: 0`, passing validation while being resource-starved at runtime.

**Formal Specification:**
```
FUNCTION isBugCondition_4(input)
  INPUT: input of type { task_cpu: u32, task_memory_mb: u32, sidecar_cpu: u32,
                         sidecar_memory: u32, wait_cpu: u32, wait_memory: u32 }
  OUTPUT: boolean

  primary_cpu := task_cpu.saturating_sub(sidecar_cpu + wait_cpu)
  primary_memory := task_memory_mb.saturating_sub(sidecar_memory + wait_memory)

  RETURN primary_cpu == 0 OR primary_memory == 0
END FUNCTION
```

### Bug 5: VPC Endpoint Inventory Drift

The bug manifests when `EcsConfig::required_vpc_endpoints(region)` includes DSQL endpoints (`dsql`, `dsql-control`) but `networking::required_endpoint_specs` only covers core ECS/ECR/S3/SSM/Cloud Map endpoints. No test asserts that the union of all module-created endpoints matches the expected inventory, so private-only deployments can fail due to missing endpoints.

**Formal Specification:**
```
FUNCTION isBugCondition_5(input)
  INPUT: input of type { region: String }
  OUTPUT: boolean

  expected := config::required_vpc_endpoints(region)
  actual := networking::required_endpoint_specs(region)
             UNION dsql_module_endpoints(region)
             -- mapped to service_name format

  RETURN expected != actual
END FUNCTION
```

### Examples

- **Bug 1**: Deploy with image tag `v1.2.3`, then update to `v1.2.4`. Task definition registers revision 2, but service stays on revision 1. Operator sees "no changes" in plan output.
- **Bug 2**: Grafana container has `secrets: [{ name: "ADMIN_PASSWORD", value_from: "arn:aws:secretsmanager:..." }]`. Task starts, ECS agent cannot pull the secret, task fails with "unable to pull secrets or registry auth".
- **Bug 3**: Primary container has `environment: [{ name: "TOKEIRA_ROLE", value: "runtime" }]`. Container starts with no `TOKEIRA_ROLE` env var set.
- **Bug 4**: Task CPU = 256, Alloy sidecar = 128, Alloy init = 64, wait-for = 32. Total overhead = 224. Primary gets 32 CPU. But if task CPU = 192, primary gets 0 CPU.
- **Bug 5**: Private deployment in `eu-west-2`. Networking module creates 11 endpoints. Config expects 13 (includes `dsql`, `dsql-control`). DSQL module doesn't create its own endpoints. Private connectivity to DSQL fails.

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- When a task definition has NOT changed between plan cycles, `EcsServiceResource.diff()` SHALL continue to report no change and `update()` SHALL NOT call `UpdateService` (Requirement 3.1)
- When a task definition has no `secrets` and uses public images, the system SHALL continue to register the task definition without requiring an execution role (Requirement 3.2)
- When a `ContainerSpec` has an empty `environment` vector, the system SHALL continue to produce a valid AWS container definition with no environment key-value pairs (Requirement 3.3)
- When task-level CPU/memory is sufficiently larger than sidecar reservations, the system SHALL continue to compute primary container resources correctly using subtraction (Requirement 3.4)
- When VPC endpoints are created by individual modules, the system SHALL continue to allow modules to own their respective endpoint resources independently (Requirement 3.5)

**Scope:**
All inputs that do NOT trigger the five bug conditions should be completely unaffected by these fixes. This includes:
- Services where the task definition ARN has not changed
- Task definitions without secrets or private ECR images
- Containers with empty environment vectors
- Workloads with sufficient CPU/memory headroom above sidecar reservations
- Deployments where all required endpoints are already created by modules

## Hypothesized Root Cause

Based on code analysis, the root causes are confirmed (not hypothesized):

1. **Task Definition Rollout Gap**: `EcsServiceResource.diff()` at line 352 unconditionally returns `InternalChange::NoChange`. It does not read the task definition dependency's current state to compare ARNs. The `update()` method at line 319 returns `Ok(current.clone())` without calling `UpdateService`.

2. **Missing Execution Role**: `TaskDefinitionResource.create()` at line 112 sets `task_role_arn` (for in-container credentials) but never sets `execution_role_arn` (for ECS agent credentials). The `TaskDefinitionSpec` struct has `task_role_dependency` but no `execution_role_dependency` field.

3. **Container Environment Not Mapped**: `to_aws_container` in `platforms/ecs/src/modules/services.rs` at line 108 maps `secrets`, `port_mappings`, `mount_points`, and `depends_on` but does not map `environment`. The AWS-layer `ContainerSpec` in `crates/tokeira-aws/src/resources/ecs_service.rs` at line 24 has no `environment` field.

4. **Resource Sizing saturating_sub**: `workload_from_parts` at line 389 uses `cpu.saturating_sub(sidecar_cpu + wait_cpu)` and `memory_mb.saturating_sub(sidecar_memory + wait_memory)`. When the result is zero, no error is raised and the zero value flows into `primary_container()`.

5. **VPC Endpoint Inventory Drift**: `required_endpoint_specs` in `networking.rs` at line 168 lists 11 endpoints (ECS, ECR, S3, autoscaling, servicediscovery, SSM). `required_vpc_endpoints` in `config.rs` at line 484 lists 13 (adds `dsql`, `dsql-control`). No module creates the DSQL endpoints, and no test asserts the union matches.

## Correctness Properties

Property 1: Bug Condition — Task Definition Rollout Triggers Service Update

_For any_ ECS service resource where the task definition dependency's `physical_id` (the current revision ARN) differs from the task definition ARN recorded at service creation time, the fixed `EcsServiceResource.diff()` SHALL return `InternalChange::Update` and the fixed `update()` SHALL call `UpdateService` with the new task definition ARN, producing a `ResourceState` reflecting the updated ARN.

**Validates: Requirements 2.1**

Property 2: Bug Condition — Execution Role Assigned When Secrets or Private ECR Present

_For any_ `TaskDefinitionSpec` where at least one container has a non-empty `secrets` vector or references a private ECR image, the fixed `TaskDefinitionResource.create()` SHALL include an `execution_role_arn` in the `RegisterTaskDefinition` call, and the `TaskDefinitionSpec` struct SHALL carry an `execution_role_dependency` field.

**Validates: Requirements 2.2**

Property 3: Bug Condition — Environment Variables Mapped to AWS Container Definition

_For any_ `ContainerSpec` with a non-empty `environment` vector, the fixed `to_aws_container` SHALL produce an AWS-layer `ContainerSpec` with a corresponding `environment` field containing all `{ name, value }` pairs, and the `container_definition` function SHALL emit them as AWS ECS `KeyValuePair` entries.

**Validates: Requirements 2.3**

Property 4: Bug Condition — Zero-Resource Primary Container Rejected

_For any_ workload configuration where `task_cpu - (sidecar_cpu + wait_cpu) <= 0` OR `task_memory_mb - (sidecar_memory + wait_memory) <= 0`, the fixed `workload_from_parts` SHALL return an error instead of producing a container with zero resources. Validation SHALL verify `primary.cpu > 0` AND `primary.memory_mb > 0`.

**Validates: Requirements 2.4**

Property 5: Bug Condition — VPC Endpoint Inventory Completeness

_For any_ region, the union of VPC endpoints created by all infrastructure modules SHALL equal `EcsConfig::required_vpc_endpoints(region)`. A test SHALL assert this equality, preventing endpoint inventory drift.

**Validates: Requirements 2.5**

Property 6: Preservation — Unchanged Service When Task Definition Unchanged

_For any_ ECS service resource where the task definition dependency's `physical_id` equals the task definition ARN stored in the service's state, the fixed `diff()` SHALL return `InternalChange::NoChange` and `update()` SHALL NOT call `UpdateService`, preserving the current no-op behavior.

**Validates: Requirements 3.1**

Property 7: Preservation — No Execution Role When Not Needed

_For any_ `TaskDefinitionSpec` where all containers have empty `secrets` vectors and use public images, the fixed system SHALL continue to register the task definition without an execution role, preserving the current behavior.

**Validates: Requirements 3.2**

Property 8: Preservation — Empty Environment Produces Valid Container

_For any_ `ContainerSpec` with an empty `environment` vector, the fixed `to_aws_container` SHALL produce a valid AWS container definition with no environment entries, preserving the current behavior.

**Validates: Requirements 3.3**

Property 9: Preservation — Correct Resource Subtraction When Headroom Sufficient

_For any_ workload configuration where `task_cpu > (sidecar_cpu + wait_cpu)` AND `task_memory_mb > (sidecar_memory + wait_memory)`, the fixed system SHALL compute primary container resources identically to the current `saturating_sub` behavior (since the result is positive and identical to checked subtraction).

**Validates: Requirements 3.4**

Property 10: Preservation — Module Endpoint Ownership Unchanged

_For any_ infrastructure module that creates VPC endpoints, the fixed system SHALL continue to allow that module to own its endpoint resources independently. The fix adds DSQL endpoints to the networking module (or a dedicated module) without changing ownership of existing endpoints.

**Validates: Requirements 3.5**

## Fix Implementation

### Changes Required

**File**: `crates/tokeira-aws/src/resources/ecs_service.rs`

**Bug 1 — Task Definition Rollout:**
1. **Add `task_definition_arn` to `service_state` properties**: Store the task definition ARN used at service creation so `diff()` can compare against it.
2. **Fix `EcsServiceResource.diff()`**: Read the task definition dependency's current `physical_id` from `ProvisionContext`, compare against `current.properties["task_definition_arn"]`. Return `InternalChange::Update` if they differ, `NoChange` otherwise.
3. **Fix `EcsServiceResource.update()`**: Call `UpdateService` with the new task definition ARN and `force_new_deployment(true)`. Return updated `ResourceState` with the new task definition ARN in properties.

**Bug 2 — Execution Role:**
4. **Add `execution_role_dependency: Option<ResourceId>` to `TaskDefinitionSpec`** (AWS-layer struct).
5. **In `TaskDefinitionResource.create()`**: If `execution_role_dependency` is `Some`, resolve the role ARN from state and set `.execution_role_arn(role_arn)` on the `RegisterTaskDefinition` request.

**Bug 3 — Environment Mapping:**
6. **Add `environment: Vec<EnvironmentSpec>` to AWS-layer `ContainerSpec`** with a new `EnvironmentSpec { name, value }` struct.
7. **In `container_definition()`**: Map `spec.environment` to `.set_environment(Some(...))` using `aws_sdk_ecs::types::KeyValuePair`.
8. **In `to_aws_container()` (platforms/ecs)**: Map `spec.environment` entries to `aws_ecs::EnvironmentSpec` structs.

**File**: `platforms/ecs/src/services.rs`

**Bug 4 — Resource Sizing Validation:**
9. **Replace `saturating_sub` with checked subtraction + error**: In `workload_from_parts`, compute `primary_cpu` and `primary_memory` using checked subtraction. If either result is zero or would underflow, return an `IacError` with a descriptive message indicating the task-level resources are insufficient for the sidecar overhead.
10. **Propagate the error**: Change `workload_from_parts` return type from `EcsWorkload` to `Result<EcsWorkload, IacError>` and propagate through callers.

**File**: `platforms/ecs/src/modules/services.rs`

**Bug 2 — Execution Role Wiring:**
11. **In `to_aws_task_definition()`**: Accept an `execution_role_dependency: Option<ResourceId>` parameter and set it on the AWS-layer `TaskDefinitionSpec`.
12. **In `ServicesModule.resources()`**: For workloads with secrets or private ECR images, create an execution role resource and pass its `ResourceId` as the execution role dependency.

**File**: `platforms/ecs/src/modules/networking.rs`

**Bug 5 — VPC Endpoint Inventory:**
13. **Add DSQL endpoints to `required_endpoint_specs()`**: Add `("dsql", "dsql", EndpointType::Interface)` and `("dsql-control", "dsql-control", EndpointType::Interface)` to the endpoint list.
14. **Add inventory completeness test**: Assert that `required_endpoint_specs(region).map(|e| e.service_name)` equals `config::required_vpc_endpoints(region)` (as a set).

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that demonstrate the bugs on unfixed code, then verify the fixes work correctly and preserve existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bugs BEFORE implementing the fix. Confirm the root cause analysis.

**Test Plan**: Write unit tests that exercise each bug condition on the current (unfixed) code and observe the incorrect behavior.

**Test Cases**:
1. **Rollout Gap Test**: Create an `EcsServiceResource` with a `task_definition_dependency` whose state has a different `physical_id` than what was used at creation. Call `diff()` — observe it returns `NoChange` (will fail after fix, confirming the bug).
2. **Missing Execution Role Test**: Create a `TaskDefinitionSpec` with containers that have secrets. Call `create()` with a mock context — observe no `execution_role_arn` is set (will fail after fix).
3. **Environment Drop Test**: Create a platform-layer `ContainerSpec` with `environment: vec![EnvironmentVar { name: "FOO", value: "bar" }]`. Call `to_aws_container()` — observe the result has no environment field (will fail after fix).
4. **Zero Resource Test**: Call `workload_from_parts` with `cpu = 128` (less than sidecar overhead). Observe it produces a container with `cpu: 0` (will fail after fix).
5. **Endpoint Drift Test**: Compare `required_endpoint_specs("eu-west-2")` service names against `required_vpc_endpoints("eu-west-2")` — observe they don't match (will fail after fix).

**Expected Counterexamples**:
- `diff()` returns `NoChange` even when task definition ARN has changed
- `RegisterTaskDefinition` request has no execution role despite secrets being present
- AWS container definition has no environment entries despite platform spec having them
- Primary container has `cpu: 0` without any error
- Endpoint sets differ by 2 entries (`dsql`, `dsql-control`)

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds, the fixed functions produce the expected behavior.

**Pseudocode:**
```
FOR ALL input WHERE isBugCondition_1(input) DO
  result := EcsServiceResource_fixed.diff(current_state, ctx)
  ASSERT result == InternalChange::Update
END FOR

FOR ALL input WHERE isBugCondition_2(input) DO
  spec := build_task_definition_spec(input)
  ASSERT spec.execution_role_dependency IS SOME
END FOR

FOR ALL input WHERE isBugCondition_3(input) DO
  aws_container := to_aws_container_fixed(input)
  ASSERT aws_container.environment.len() == input.environment.len()
  FOR EACH (expected, actual) IN zip(input.environment, aws_container.environment) DO
    ASSERT expected.name == actual.name AND expected.value == actual.value
  END FOR
END FOR

FOR ALL input WHERE isBugCondition_4(input) DO
  result := workload_from_parts_fixed(input)
  ASSERT result IS Err
END FOR

FOR ALL regions DO
  expected := config::required_vpc_endpoints(region)
  actual := networking::required_endpoint_specs(region).map(service_name)
  ASSERT expected == actual (as sets)
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold, the fixed functions produce the same result as the original functions.

**Pseudocode:**
```
FOR ALL input WHERE NOT isBugCondition_1(input) DO
  ASSERT EcsServiceResource_fixed.diff(input) == InternalChange::NoChange
END FOR

FOR ALL input WHERE NOT isBugCondition_2(input) DO
  spec := build_task_definition_spec(input)
  ASSERT spec.execution_role_dependency IS NONE
END FOR

FOR ALL input WHERE NOT isBugCondition_3(input) DO
  aws_container := to_aws_container_fixed(input)
  ASSERT aws_container.environment IS EMPTY
END FOR

FOR ALL input WHERE NOT isBugCondition_4(input) DO
  result := workload_from_parts_fixed(input)
  ASSERT result IS Ok
  ASSERT result.primary.cpu == input.task_cpu - (input.sidecar_cpu + input.wait_cpu)
  ASSERT result.primary.memory_mb == input.task_memory_mb - (input.sidecar_memory + input.wait_memory)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- It generates many test cases automatically across the input domain
- It catches edge cases that manual unit tests might miss (e.g., boundary values for resource arithmetic)
- It provides strong guarantees that behavior is unchanged for all non-buggy inputs

**Test Plan**: Observe behavior on UNFIXED code first for non-buggy inputs, then write property-based tests capturing that behavior.

**Test Cases**:
1. **Service Diff Preservation**: For any service where task definition ARN matches, verify `diff()` returns `NoChange`
2. **No-Secrets Task Definition Preservation**: For any task definition without secrets, verify no execution role is required
3. **Empty Environment Preservation**: For any container with empty environment, verify valid container definition produced
4. **Sufficient Resources Preservation**: For any workload where CPU/memory exceeds sidecar overhead, verify correct subtraction result

### Unit Tests

- Test `EcsServiceResource.diff()` returns `Update` when task definition ARN differs
- Test `EcsServiceResource.diff()` returns `NoChange` when task definition ARN matches
- Test `EcsServiceResource.update()` produces state with new task definition ARN
- Test `TaskDefinitionResource.create()` sets execution role when secrets present
- Test `TaskDefinitionResource.create()` omits execution role when no secrets
- Test `to_aws_container` maps environment variables correctly
- Test `to_aws_container` handles empty environment
- Test `workload_from_parts` returns error when primary CPU would be zero
- Test `workload_from_parts` returns error when primary memory would be zero
- Test `workload_from_parts` succeeds when resources are sufficient
- Test `required_endpoint_specs` includes DSQL endpoints
- Test endpoint inventory matches `required_vpc_endpoints`

### Property-Based Tests

- Generate random `(task_cpu, sidecar_cpu, wait_cpu)` tuples and verify: if `task_cpu > sidecar_cpu + wait_cpu` then result is `Ok` with correct subtraction; if `task_cpu <= sidecar_cpu + wait_cpu` then result is `Err`
- Generate random `ContainerSpec` with varying environment lengths and verify: environment length in output equals environment length in input
- Generate random task definition ARN pairs and verify: `diff()` returns `Update` iff ARNs differ, `NoChange` iff ARNs match
- Generate random `TaskDefinitionSpec` with varying secrets presence and verify: execution role dependency is `Some` iff any container has secrets

### Integration Tests

- Test full workload generation pipeline with realistic ECS config produces valid task definitions with correct resource allocation
- Test that `ServicesModule.resources()` produces task definitions with execution roles for workloads that have secrets (e.g., Grafana)
- Test that the networking module's endpoint list stays synchronized with config's expected endpoint list across multiple regions
