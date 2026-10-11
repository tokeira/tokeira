# Distributed Schema Startup Check — Bugfix Design

## Overview

A listener-backed distributed node checks the DSQL schema before it builds anything:
- **The check.** After the node connects to DSQL, and before it builds the runtime or binds a listener, it assesses the schema against its release's schema contract under the validate-only policy. This is the assessment the embedded engine runs. The node starts only when the assessment finds the schema compatible, and it applies no migration.
- **A refusal.** Otherwise the node shuts its DSQL store down, which closes its connections and releases its DynamoDB connection-slot blocks. It then exits with an error that says what it found and what to do.
- **The setting.** An absent or `validate_only` `infrastructure.dsql.migration_policy` selects the check; `automatic` is refused before the node connects.
- **The remedy works.** `tkr schema setup`, which the refusal names, now runs for every deployment whose `tokeirad.toml` selects DSQL storage. Compose and ECS deployments were refused before.

## Glossary

- **Schema contract:** a release's minimum supported, target and maximum readable schema versions, and its migration-set digest (`crates/tokeira-storage/schema-contract.toml`, `MigrationRunner::compatibility_contract`).
- **Assessment:** `MigrationRunner::assess_connection`. It reads the migration ledger (`schema_version`) and the compatibility record (`schema_compatibility`) without writing either, and applies managed-embedded-dsql's decision table (`assess_schema_compatibility`, `crates/tokeira-storage/src/dsql/schema_compatibility.rs`).
- **Listener-backed distributed node:** `tokeirad` with `infrastructure.storage = "dsql"`, built by `build_and_serve` (`crates/tokeira-engine/src/lib.rs`), with DynamoDB coordination of its DSQL connections.
- **Connection-slot blocks:** the blocks of DSQL connection slots that a distributed node leases in its DynamoDB connection-lease table (`SlotBlockManager`, `crates/tokeira-storage/src/dsql/slot_block_manager.rs`).

## Bug Details

### Bug Condition

The bug condition holds when a listener-backed distributed node starts and either of these is true:
- a validate-only assessment of the schema would not find it compatible: the schema is missing, below the target version, below the minimum supported version or past the maximum readable version, or its ledger doesn't match the release's migrations;
- `infrastructure.dsql.migration_policy` is `automatic`.

### Examples

- **Before setup.** A new cluster on which `tkr schema setup` hasn't run. Today the node connects, takes connection-slot blocks, and fails while it prepares execution placement, with a missing-table error. After the fix, it refuses to start: the database has no Tokeira schema, and `tkr schema setup` applies it.
- **One migration behind.** A schema at V081 under a release whose target is V082. Today the node serves without the unique index on `shard_lease (shard_id, epoch)` that V082 adds and the bundle lease fence needs. After the fix, it refuses, naming V081 and V082.
- **A later release's schema.** A schema at V083 under a release that reads up to V082. Today the node serves, and fails when it uses what V083 changed. After the fix, it refuses with the assessment's report of a future schema.
- **`automatic`.** Today the node ignores it. After the fix, it refuses before it connects.
- **Compose or ECS.** Today `tkr schema setup` refuses these deployments with "schema commands require dsql storage", even though their `tokeirad.toml` selects DSQL. After the fix, it migrates them.

## Expected Behavior

### Preservation Requirements

- The embedded engine's managed and existing modes keep their own assessment and migration (3.1).
- `tkr schema setup` keeps applying the release's migrations, and still refuses a deployment that doesn't use DSQL (3.2).
- An in-memory listener-backed node starts without a schema check (3.3).
- A distributed node whose schema passes the check builds and serves the same stack as before (3.4).
- The schema contract and the migrations are unchanged (3.5).

## Root Cause

- **No check.** `build_and_serve`'s DSQL branch goes from `DsqlStore::connect` straight to `build_dsql_stack`, and `build_dsql_stack` drops the store's migration runner unused. Only `apply_embedded_schema`, on the embedded path, runs the assessment.
- **An unread setting.** Managed-embedded-dsql added `DsqlInfraConfig.migration_policy` for this startup, but nothing wired it.
- **A gate on the wrong record.** `tkr schema` decides whether a deployment uses DSQL from the deployment's metadata (`apps/tkr/src/commands/schema.rs`). Every Compose and ECS deployment's metadata records in-memory storage. An infrastructure apply then writes `infrastructure.storage = "dsql"` into the deployment's `tokeirad.toml` (`platforms/ecs/deployment.tkd`), and the metadata keeps saying in-memory. The command already reads the endpoint from `tokeirad.toml`, which its module documentation calls authoritative.

## Correctness Properties

Property 1: A node starts only on a schema its release can run on

_For any_ validate-only assessment of a schema, the distributed startup SHALL go on to build the stack exactly when the assessment finds the schema compatible. Otherwise it SHALL refuse:
- a schema behind the target, or no schema, with an error that names the schema's version, the target version and `tkr schema setup`;
- a rejected schema, with an error that carries the assessment's incompatibility report.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4**

Property 2: A refused start leaves nothing started

_For any_ refused start, the node SHALL have bound no listener, taken no shard lease and changed no row of the schema's ledger or any other table, and SHALL have released its DynamoDB connection-slot blocks.

**Validates: Requirements 2.5**

Property 3: The policy setting selects the check

_For any_ distributed configuration, an absent or `validate_only` migration policy SHALL lead to the check, and `automatic` SHALL refuse the start before any connection is made.

**Validates: Requirements 2.6**

Property 4: `tkr schema` follows the server configuration

_For any_ deployment, `tkr schema` SHALL treat the deployment as using DSQL exactly when its `tokeirad.toml` selects `infrastructure.storage = "dsql"`, whatever the deployment's metadata records.

**Validates: Requirements 2.7, 3.2**

## Fix Implementation

### Engine

- **The policy.** `build_and_serve`'s DSQL branch refuses `automatic` next to its refusal of a snapshot policy, before any I/O.
- **The check.** A new `check_distributed_schema` takes the connected `DsqlStore`. It acquires one control-class connection from the store's director and assesses the schema against `MigrationRunner::compatibility_contract()` under the validate-only policy, then releases the connection.
  - It assesses with `MigrationRunner::embedded()`, because the release's contract is computed from the compiled-in migrations. The store's own runner reads a migrations directory.
  - It runs after `DsqlStore::connect`, so its connection goes through the coordinated reservoir and its rate limits, and before `build_dsql_stack`.
- **The outcome.** A pure function maps the assessment's decision:
  - **compatible:** start, logging the schema's version and the contract's versions;
  - **migration required:** refuse, naming the schema's version (or no schema), the target version and `tkr schema setup`;
  - **rejected:** refuse with the incompatibility report. A one-line hint by category says what to do: a future schema needs the release that migrated it or a later one; a schema below the minimum needs an intermediate release first; a ledger that doesn't match this release's migrations needs investigating before any node runs on it.
  - **initialize or migrate:** a validate-only assessment never returns these, so the node refuses as on an internal error.
- **A refusal.** The node calls `DsqlStore::shutdown`. Shutdown aborts the reservoir's background tasks and releases its connection-slot blocks. If shutdown fails, the node logs that and still returns the refusal.

### Configuration

- `DsqlInfraConfig.migration_policy`'s documentation and its catalog entry (`crates/tokeira-config/src/documentation.rs`) say what each value does on this path.

### tkr

- `tkr schema` reads the deployment's `tokeirad.toml` first, and refuses unless its `infrastructure.storage` is `dsql`. The refusal names the file and the storage it selects. The deployment's metadata no longer decides.

### Platform paths

No path applies migrations before it starts `tokeirad`. Only an operator running `tkr schema setup` does, and after this change a node started first refuses, saying so.

| Path | How the schema is migrated | This change |
|---|---|---|
| Local with DSQL | The operator runs `tkr schema setup` ([deployment configuration](../../../docs/provisioning/deployment-configuration.md)); `tkr deploy apply` then starts `tokeirad` | A node started first refuses |
| Compose with DSQL | No step in the platform's lifecycle; `tkr schema setup` refused the deployment | `tkr schema setup` runs |
| ECS (always DSQL) | No step; `tkr schema setup` refused the deployment | `tkr schema setup` runs |
| EKS | The platform accepts no new deployments | None |
| Embedded managed or existing DSQL | The engine migrates under its own policy | None |

These gaps stay, recorded here rather than fixed:
- The [Compose](../../../docs/platforms/compose/README.md) and [ECS](../../../docs/platforms/ecs/README.md) guides don't list a `tkr schema setup` step before `tkr deploy apply`.
- `tkr diagnostics` decides DSQL from the deployment's metadata too (`apps/tkr/src/commands/diagnostics.rs`).
- ECS `tkr admin schema setup` has no command to run: it runs `schema setup` in the admin task's container, whose image holds only `tokeirad`, which has no subcommands. The admin task is itself a DSQL-configured `tokeirad`, so it now refuses on a schema it can't run on.
- `tkr schema setup` connects with the operator's own credentials; the written-back admin role is never assumed.
- The placement controller and the autoscaler connect to DSQL without a schema check.
- [The platforms guide](../../../docs/platforms/README.md) offers `--storage dsql` for Compose and ECS deployments, which those platforms refuse when a deployment is created.

### Specs this changes

- [managed-embedded-dsql](../managed-embedded-dsql/requirements.md):
  - Criterion 5.3 gives an absent distributed policy the validate-only policy.
  - Criterion 5.12 keeps automatic migration for existing clusters only.
  - A new criterion 5.14 refuses `automatic` in distributed mode.
  - The design's paragraph on `DsqlInfraConfig.migration_policy` and its Property 1 follow.
  - Evidence item 9 says `tkr schema setup` is how distributed DSQL is migrated.
- [tkr-cli](../tkr-cli/requirements.md): Requirement 7.1 decides DSQL storage from the deployment's `tokeirad.toml`.

## Testing Strategy

### Exploratory Bug Condition Checking

Written and run on the code before the fix, each failing there:
- **live:** a distributed node started against a schema one migration behind its target, and one started against a schema past its maximum readable version, each refused;
- **offline:** a distributed configuration with `automatic` refused before any I/O;
- **offline:** `tkr schema setup` for a deployment whose metadata records in-memory storage and whose `tokeirad.toml` selects DSQL reaches the endpoint lookup.

### Property-Based Tests

- Property 1 runs offline over generated migration sets and ledgers: each is assessed by `assess_schema_compatibility` under the validate-only policy, and the outcome is mapped as the node maps it.
- Property 3 runs offline over the three policy values.
- Property 4 runs offline over every pairing of metadata storage and `tokeirad.toml` storage.
- Property 2 is checked live, after each refusal.

### Unit Tests

- Each refusal's error names what Property 1 says it names.
- The decisions a validate-only assessment can't return refuse.

### Live Test

One test, on an ephemeral cluster with two ephemeral DynamoDB tables for the coordination. It can run again on the same cluster:
1. **Set up.** Delete ledger rows above the target, then apply the migrations, so the schema is at the target whatever an earlier run left.
2. **Below the target.** Delete the target's ledger row. A start is refused, naming both versions. Nothing is started (Property 2). The migrations are applied again.
3. **At the target.** A start succeeds and answers `GetSystemInfo` on its listener, then shuts down.
4. **Past the maximum.** Record a ledger row one above the maximum readable version. A start is refused with a future-schema report. Nothing is started. The row is deleted.

After each refusal, the test checks that nothing was started:
- the gRPC and Nexus completion addresses can be bound again;
- the ledger, the compatibility record and the shard leases are unchanged;
- no item in the connection-lease table has an owner.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test that covers it:
- **offline:** a schema behind its target admitted;
- **offline:** a rejected schema admitted;
- **offline:** `automatic` accepted;
- **offline:** `tkr schema` gated on the deployment's metadata again;
- **live:** the check skipped;
- **live:** a refusal that leaves the store running;
- **live:** a refusal that leaves its listener bound.

### Preservation Checking

The engine's embedded DSQL tests, the in-memory listener tests and `tkr`'s schema tests stay green.
