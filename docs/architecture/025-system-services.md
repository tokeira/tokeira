# 025 System Services

**Status:** draft for architecture review  
**Related docs:** [000-overview](000-overview.md), [035-placement-and-membership](035-placement-and-membership.md), [045-autoscaling-on-ecs-ec2](045-autoscaling-on-ecs-ec2.md), [055-admission-control](055-admission-control.md), [075-archival-to-s3](075-archival-to-s3.md)

## Purpose

Temporal includes a **Worker Service** for internal background Workflows.[^temporal-server] Tokeira needs an equivalent capability, but it should not simply reproduce Worker Service as a fourth core correctness plane.

The question is not "should Tokeira have background work?" It obviously should. The question is **which background work belongs in plain services, and which belongs in durable internal workflows?**

## Design claim

Tokeira should have a **System Services layer**, not a monolithic Worker Service.

That layer should contain two categories of jobs:

1. **control services** for hot-path or near-hot-path mechanics,
2. **system workflows** for long-running, auditable, restart-friendly platform operations.

The distinction matters because durable workflows are excellent for orchestration and audit, but they are the wrong tool for everything.

## What should remain plain services

The following concerns should stay as ordinary services or loops, not durable workflows:

- shard/bundle placement control,
- lease acquisition and renewal,
- edge admission and load shedding,
- delivery-broker control loops,
- DSQL connection-budget control,
- runtime auto-tune,
- autoscaling decisions.

These are short-cycle control problems. They need low-latency response and should not depend on going through the durable execution engine to keep the durable execution engine healthy.

## What should become system workflows

The following concerns are a good fit for durable internal workflows:

- namespace bootstrap or delayed namespace teardown,
- batch operations that span many executions,
- archival orchestration,
- history/export verification and repair,
- large projection backfills,
- tenant migration or repartitioning jobs,
- compliance or retention jobs that run for a long time and need auditable progress.

These jobs benefit from:

- retries,
- resumability,
- explicit history,
- operator visibility,
- long waits,
- human or external approvals.

If DSQL storage cost is acceptable, that is a strong argument for using Tokeira itself for these system jobs rather than building a separate, less observable job framework.

## Architectural rule

The clean rule is:

> **Do not put hot-path mechanics into system workflows.**  
> **Do put long-running operational orchestration into system workflows when durability and auditability matter.**

This keeps the runtime/control plane fast while still taking advantage of the platform’s own durable execution model.

## Recommended service definitions

### `tokeira-controller`

Plain service.

Responsibilities:

- placement planning,
- bundle movement,
- routing-map publication,
- admission policy distribution,
- autoscaler coordination.

This service does not own correctness; DSQL lease fencing still does.

### `tokeira-system`

Durable internal workflow service.

Responsibilities:

- run internal system workflows,
- host system task queues,
- expose operator-visible progress,
- isolate internal workflow execution from tenant workloads.

This is the nearest conceptual equivalent to Temporal’s Worker Service, but it is narrower and more deliberate.

### `tokeira-archival`

Plain service, optionally using system workflows for orchestration.

Responsibilities:

- consume archive candidates,
- export closed execution data to S3,
- verify archive manifests,
- mark archival completion,
- coordinate later hot-state pruning.

This is separated because archival touches external object storage and should be paced independently of workflow execution.

## Why not one generic worker service

Temporal’s current Worker Service is documented as the place for internal background Workflows.[^temporal-server] That makes sense in Temporal’s service topology, but Tokeira is already trying to reduce internal service coupling.

A single generic worker service would tend to blur three distinct things:

- control loops,
- durable internal orchestration,
- external data movement.

Those three should scale, fail, and be admitted differently.

## Scheduling and isolation

System workflows should run in a dedicated namespace and on dedicated task queues.

Recommended defaults:

- namespace: `__tokeira_system`
- task queues:
  - `system/default`
  - `system/archival`
  - `system/backfill`
  - `system/maintenance`

That gives clean observability and lets the platform enforce separate admission and concurrency limits for system work.

## Storage implications

Using Tokeira itself for system workflows means those workflows also write history to DSQL. That is acceptable if the platform is honest about what belongs there.

The right trade is:

- system workflows for low-volume, high-value operations,
- plain services for high-frequency mechanics.

This keeps DSQL storage growth moderate while still capturing the important operational jobs in a durable, inspectable form.

## Failure semantics

System workflows should be treated like tenant workflows in one important sense: they should survive restarts and operator interruptions.

But they should be treated differently in two other senses:

- they should not be allowed to starve tenant traffic,
- they should be easier to pause, drain, or reschedule during maintenance.

That implies dedicated admission classes and queue isolation.

## Recommended first system workflows

The first internal workflows I would actually implement are:

1. archival orchestration,
2. large visibility/projection backfills,
3. batch workflow operations,
4. namespace decommission jobs.

These are all jobs where progress tracking and resumability matter more than raw dispatch latency.

## Review questions

1. Should `tokeira-system` be present from day one, or should the first milestone keep system jobs as plain services only?
2. Do we want a single system namespace, or separate namespaces for platform and operator-run jobs?
3. Which maintenance jobs should never be allowed to use durable workflows, even if they would be convenient?

## References

[^temporal-server]: Temporal Server, official docs: https://docs.temporal.io/temporal-service/temporal-server
