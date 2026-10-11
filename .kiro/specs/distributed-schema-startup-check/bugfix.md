# Bugfix Requirements Document

## Introduction

A Tokeira release runs on the DSQL schemas between two versions of its schema contract (`crates/tokeira-storage/schema-contract.toml`):
- its **target version**, the version its migrations reach;
- its **maximum readable version**, the newest version it can read.

The embedded engine checks the schema against that range before it serves (`apply_embedded_schema`, `crates/tokeira-engine/src/lib.rs`), as [managed-embedded-dsql](../managed-embedded-dsql/requirements.md) Requirement 4 requires. A listener-backed distributed node, `tokeirad` with `infrastructure.storage = "dsql"`, checks nothing. It connects, builds the runtime and binds its listener whatever the schema holds (`build_and_serve`, `crates/tokeira-engine/src/lib.rs`). So:
- **A node started before its release's migrations are applied** goes on starting whatever is missing. It fails at the first step that uses a missing table, after it has connected and taken DynamoDB connection-slot blocks. When no step uses one, it serves without what the migrations add. A schema at V081 is an example: the node serves without V082's unique index on `shard_lease (shard_id, epoch)`, which the bundle lease fence needs to refuse a stale write ([bundle-lease-fence](../bundle-lease-fence/bugfix.md)).
- **A node of an earlier release, started against a schema a later release migrated,** serves, then fails when it first uses what the later migrations changed. The schema change that drops the `activity_state` table is one: the release before it still deletes a run's `activity_state` rows when it deletes the run.

The node's configuration also documents `infrastructure.dsql.migration_policy` as its schema policy at startup (`crates/tokeira-config/src/lib.rs`, `crates/tokeira-config/src/documentation.rs`). Managed-embedded-dsql criteria 5.3, 5.12 and 5.13 give that setting rules, but nothing reads it.

An operator migrates a distributed node's schema with `tkr schema setup`, and no platform does it for them. But `tkr schema` decides whether a deployment uses DSQL from the deployment's metadata (`apps/tkr/src/commands/schema.rs`), and every Compose or ECS deployment's metadata records in-memory storage. So the command refuses those deployments even once their `tokeirad.toml` selects DSQL, as an ECS deployment's always does.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a listener-backed distributed node starts against a DSQL schema below its release's target version, or with no Tokeira schema at all, THEN it goes on starting. It fails at the first step that uses what the missing migrations add, after it has connected to DSQL and taken DynamoDB connection-slot blocks. When no step uses it, the node binds its listener and serves without it.

1.2 WHEN such a node starts against a schema past its release's maximum readable version THEN it binds its listener and serves, and fails later, when it first uses what the newer migrations changed.

1.3 WHEN such a node starts THEN it ignores `infrastructure.dsql.migration_policy`, whether the setting is absent, `validate_only` or `automatic`.

1.4 WHEN `tkr schema` runs for a deployment whose `tokeirad.toml` selects DSQL storage and whose metadata records in-memory storage THEN it refuses with "schema commands require dsql storage".

### Expected Behavior (Correct)

2.1 WHEN a listener-backed distributed node starts THEN it SHALL check the DSQL schema against its release's schema contract, without changing the schema, before it builds the runtime or binds a listener.

2.2 WHEN the check finds the schema at or above the target version and at or below the maximum readable version, with every migration the release knows recorded as the release has it, THEN the node SHALL start.

2.3 WHEN the check finds the schema below the target version, or no Tokeira schema, THEN the node SHALL refuse to start. Its error SHALL name the schema's version and the target version, and say to apply the migrations with `tkr schema setup`.

2.4 WHEN the check rejects the schema, because its version is past the maximum readable version or below the minimum supported one, or because its migration ledger doesn't match the release's migrations, THEN the node SHALL refuse to start with the check's incompatibility report and what to do about it.

2.5 WHEN the node refuses to start THEN it SHALL leave nothing started:
- no listener bound;
- no runtime or other background work, and no shard lease taken;
- no change to the schema, its ledger or any other table;
- its DSQL connections closed and its DynamoDB connection-slot blocks released.

2.6 WHEN `infrastructure.dsql.migration_policy` is absent or `validate_only` THEN the node SHALL check the schema as 2.1 to 2.5 say. WHEN it is `automatic` THEN the node SHALL refuse to start before it connects to DSQL, saying that a distributed node never migrates the schema and naming `tkr schema setup`.

2.7 WHEN `tkr schema` runs for a deployment THEN it SHALL treat the deployment as using DSQL exactly when the deployment's `tokeirad.toml` selects `infrastructure.storage = "dsql"`, whatever the deployment's metadata records.

### Unchanged Behavior (Regression Prevention)

3.1 The embedded engine's managed and existing DSQL modes SHALL CONTINUE TO check and migrate the schema under their own policies, as managed-embedded-dsql specifies.

3.2 `tkr schema setup` SHALL CONTINUE TO apply the release's migrations, and SHALL CONTINUE TO refuse a deployment that doesn't use DSQL.

3.3 A listener-backed node with in-memory storage SHALL CONTINUE TO start without a schema check.

3.4 A distributed node whose schema passes the check SHALL CONTINUE TO build and serve the same stack as before.

3.5 This change SHALL add no migration and change no version in the schema contract.

### Out of Scope

- **Nodes of releases before this change.** They don't check the schema, so only the release note's "Upgrade with every node stopped." keeps one of them off a schema a later release migrated.
- **Migrating at distributed startup.** A distributed node refuses `automatic` (2.6). Migrations stay an explicit operator step, `tkr schema setup`, as root `AGENTS.md` §4 asks of every mutation.
- **The other platform gaps.** The design records what stays as it is: `tkr diagnostics`'s matching gate, ECS `tkr admin schema setup`, the credentials `tkr schema setup` connects with, the placement controller and autoscaler, and the platforms guide's `--storage dsql`.
