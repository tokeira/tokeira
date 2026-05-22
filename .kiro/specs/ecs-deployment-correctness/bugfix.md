# Bugfix Requirements Document

## Introduction

The ECS platform layer (`platforms/ecs/`, `crates/tokeira-aws/`) has five confirmed deployment correctness bugs that can cause silent deployment failures, secret injection failures, and resource starvation. These bugs affect the core deployment lifecycle: task definition rollout, execution role assignment, environment variable mapping, resource arithmetic, and VPC endpoint inventory completeness. Left unfixed, deployments appear successful while old code continues running, secrets fail to inject at task startup, environment configuration is silently dropped, containers are starved of resources, and private-only deployments fail due to missing endpoints.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a task definition is updated (new image tag, config change, container spec change) THEN `TaskDefinitionResource` registers a new revision but `EcsServiceResource.diff()` always returns `NoChange` and `EcsServiceResource.update()` returns the current state unchanged, leaving the ECS service pinned to the old task definition revision

1.2 WHEN a task definition includes containers with `secrets` referencing Secrets Manager ARNs THEN the task definition is registered without an execution role, causing ECS to fail task startup with "unable to pull secrets" because the ECS agent lacks `secretsmanager:GetSecretValue` permission

1.3 WHEN a `ContainerSpec` has a non-empty `environment` vector THEN the `to_aws_container` function in `platforms/ecs/src/modules/services.rs` maps `secrets` but ignores `environment`, silently dropping all plain environment variables from the AWS container definition

1.4 WHEN task-level CPU/memory is less than or equal to the sum of sidecar and init-container reservations THEN `saturating_sub` produces a primary container with zero CPU or zero memory without any error, passing config validation while the actual container is resource-starved

1.5 WHEN `EcsConfig::required_vpc_endpoints(region)` includes DSQL endpoints (`dsql`, `dsql-control`) but `networking::required_endpoint_specs` only covers core ECS/ECR/S3/SSM/Cloud Map endpoints THEN there is no test proving the union of all module-created endpoints matches the expected inventory, allowing private-only deployments to fail due to missing endpoints

### Expected Behavior (Correct)

2.1 WHEN the desired task definition ARN/revision (from `TaskDefinitionResource` state) differs from the task definition ARN stored in the current `EcsServiceResource` state THEN the service resource SHALL detect the change in `diff()` and `update()` SHALL call `UpdateService` with the new task definition ARN, triggering a rolling deployment

2.2 WHEN a task definition includes containers with `secrets` referencing Secrets Manager ARNs or uses private ECR images THEN the task definition SHALL include an execution role with permissions for `secretsmanager:GetSecretValue`, `ecr:GetAuthorizationToken`, `ecr:BatchGetImage`, and `ecr:GetDownloadUrlForLayer`

2.3 WHEN a `ContainerSpec` has a non-empty `environment` vector THEN `to_aws_container` SHALL convert each `EnvironmentVar { name, value }` entry into an AWS ECS container definition `environment` key-value pair alongside the existing `secrets` mapping

2.4 WHEN task-level CPU/memory minus sidecar and init-container reservations would result in a primary container with insufficient resources THEN the system SHALL return a configuration error instead of silently producing a zero-resource container; validation SHALL verify that `primary.cpu > 0` and `primary.memory_mb > 0` after subtraction

2.5 WHEN the ECS platform is deployed in private-only mode THEN a test SHALL verify that the union of VPC endpoints created by all modules (networking + DSQL) equals `EcsConfig::required_vpc_endpoints(region)`, preventing endpoint inventory drift

### Unchanged Behavior (Regression Prevention)

3.1 WHEN a task definition has not changed between plan cycles THEN `EcsServiceResource.diff()` SHALL CONTINUE TO report no change and `update()` SHALL NOT call `UpdateService`

3.2 WHEN a task definition has no `secrets` and uses public images THEN the system SHALL CONTINUE TO register the task definition without requiring an execution role

3.3 WHEN a `ContainerSpec` has an empty `environment` vector THEN the system SHALL CONTINUE TO produce a valid AWS container definition with no environment key-value pairs

3.4 WHEN task-level CPU/memory is sufficiently larger than sidecar reservations THEN the system SHALL CONTINUE TO compute primary container resources correctly using subtraction

3.5 WHEN VPC endpoints are created by individual modules (networking creates core endpoints, DSQL creates DSQL-specific endpoints) THEN the system SHALL CONTINUE TO allow modules to own their respective endpoint resources independently
