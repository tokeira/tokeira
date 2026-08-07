# ecs-idealized — multi-file definitions, judged against the hardest case

A sketch of the `mod` mechanism: a root `definition.tkd` referencing part
files, with `dsql.tkd` translated faithfully from the real 679-line
`platforms/ecs/src/modules/dsql.rs`. Deliberately non-compiling; the value
is the shape.

## The mechanism

- `mod networking;` in the root resolves to a sibling `networking.tkd`,
  loaded through the same definition-dir-anchored source resolution
  companions use — no upward paths, ever. `syn` already parses the
  declaration (`Item::Mod`, `content: None`); the subset gate flips one arm
  from refusal to resolution.
- A part is a namespace of items — functions and structs, the same
  language. No new graph concepts: the root's `deployment()` still builds
  the one graph.

## The rules

1. **One level.** The root declares parts; parts declare no `mod`.
   Identity, revision retention, and the retarget set stay enumerable from
   the root.
2. **All wiring flows through the root.** Parts never reference each
   other's items. A part receives what it needs as parameters (config
   values, resource handles, its module dependencies) and returns a struct
   of handles; the root passes pieces onward. Consequence: the root reads
   as the deployment's wiring diagram, one screen, while each part
   encapsulates its resource detail.
   - Type visibility follows: the root's types are visible to parts (the
     shared configuration language); a part's types are visible to the
     root under its namespace (`dsql::Handles`); parts see nothing of each
     other.

## What the translation surfaces

- **Validation by shape.** `validate_preexisting` (runtime field checking)
  dissolves into `DsqlMode::Preexisting(PreexistingDsql)` whose fields are
  simply required.
- **Stable logical identity across modes.** `dsql.cluster` names the same
  engine identity whether managed or adopted; the mode lives in the
  payload.
- **Reference-carrying payloads.** Today's `vpc_dependency: ResourceId`
  config fields become payload fields holding a resource handle — the
  typed sibling of the `output(...)` references writeback already uses.
- **Platform-neutral kinds.** The role wrapper hardcodes the
  `ecs-tasks.amazonaws.com` trust principal today; as a kind it takes
  `assumed_by`, so EKS authors the same kind.
- Presumed kinds (each wraps an existing `tokeira-aws` resource; the
  wrappers are onboarding work): `S3Bucket`, `VpcEndpoint`,
  `DsqlConnectionEndpoint`, `DsqlRole`, `AdoptedDsqlEndpoint`,
  `AdoptedIamRole`. `DsqlCluster` exists.

## The tkdp mirror

`import networking` at the top of a root `definition.tkdp`, resolving to a
sibling `networking.tkdp` under the same bounds; part functions and returned
records mirror one-to-one.
