# ecs-idealized — the ECS onboarding shape

Direction, not contract: the ECS platform authored as a multi-document
definition, with `dsql.tkd` translated faithfully from the real 679-line
`platforms/ecs/src/modules/dsql.rs`. Deliberately non-compiling; the value
is the shape. The parts mechanism itself is adopted contract and lives in
this spec's `design.md`; where this sketch and the spec documents disagree,
the spec documents win.

## What the translation surfaces

- **Validation by shape.** `validate_preexisting` (runtime field checking)
  dissolves into `DsqlMode::Preexisting(PreexistingDsql)` whose fields are
  simply required.
- **Stable logical identity across modes.** `dsql.cluster` names the same
  engine identity whether managed or adopted; the mode lives in the
  payload.
- **Reference-carrying payloads.** Today's `vpc_dependency: ResourceId`
  config fields become payload fields holding a resource handle — the typed
  sibling of the `output(...)` references writeback already uses.
- **Platform-neutral kinds.** The role wrapper hardcodes the
  `ecs-tasks.amazonaws.com` trust principal today; as a kind it takes
  `assumed_by`, so EKS authors the same kind.
- Presumed kinds (each wraps an existing `tokeira-aws` resource; the
  wrappers are onboarding work): `S3Bucket`, `VpcEndpoint`,
  `DsqlConnectionEndpoint`, `DsqlRole`, `AdoptedDsqlEndpoint`,
  `AdoptedIamRole`. `DsqlCluster` exists.

## Onboarding consequences

- The ECS platform authors one part file per module (networking, dsql,
  cluster, observability, …), each declaring its resources against the
  deployment the root wires; `EcsConfig` dissolves into the definition's
  authored configuration shape.
- The `.tkdp` peer mirrors one-to-one under the adopted parts mechanism;
  its authored surface follows `../tkdp-parts-idealized/`.
