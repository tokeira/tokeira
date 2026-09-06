# DSQL Connector 0.2 and SQLx 0.9 — Requirements

## Introduction

Tokeira reaches Aurora DSQL through `aurora-dsql-sqlx-connector` 0.1.2 on SQLx 0.8. The
connector's manifest takes `aws-sdk-dsql`'s default features, and that crate's `rustls`
default enables the AWS SDK's legacy HTTPS client: hyper 0.14, h2 0.3.27, hyper-rustls
0.24, tokio-rustls 0.24, rustls 0.21, and rustls-webpki 0.101.7. The connector is the only
root of that client left in the workspace. Every other AWS SDK dependency already opts out
of the defaults in favour of the modern client, and the tonic 0.14 move removed the legacy
HTTP stack from every server surface. `deny.toml` carries four advisories against that
client (RUSTSEC-2026-0098, -0099, -0104 for rustls-webpki 0.101.7 and RUSTSEC-2026-0258 for
h2 0.3.27) as declared stop-gaps whose stated exit is this move.

Connector 0.2.2 pins `aws-sdk-dsql` with default features off, so the legacy client has no
enabler once the pin moves. It requires SQLx 0.9, a major release whose one broad change is
that every `query*()` function takes a SQL string proven safe by its type: only
`&'static str` qualifies, and any other string must be wrapped in `AssertSqlSafe` by a
caller who has audited it. Thirteen call sites in the storage, projection, and `tkr`
crates pass built strings today. The connector also gates its OCC-retry error behind a
feature Tokeira does not enable and reports through the `log` facade instead of `tracing`.

This feature moves the three SQLx users to 0.9 and the connector to 0.2.2, attests every
dynamic SQL site with the reason it cannot carry request data, removes the legacy client
from the lock and the four advisories from `deny.toml`, re-scopes the engine's lock
invariant now that the Connector Exception ends, makes the `log`-to-`tracing` bridge an
explicit dependency, and proves the migrated connection path against a live DSQL cluster.

Public `tokeira-storage` signatures name SQLx types (`PgConnection`, `PgPool`,
`sqlx::Error`), so a SQLx major is a breaking change for every downstream crate. It ships in
the 0.3.0 train the gRPC stack move already requires; it adds no bump of its own.

Authorities for this feature are the crate sources in the pinned registry, not their
documentation: `aurora-dsql-sqlx-connector` 0.2.2 (with 0.1.2 for comparison), `sqlx`,
`sqlx-core`, and `sqlx-postgres` 0.9.0, `aws-sdk-dsql` 1.55.0 as locked, `aws-smithy-runtime`
1.12.1, `aws-smithy-http-client` 1.2.0, `aws-runtime` 1.9.1, and `tracing-subscriber` 0.3.23.
Temporal behaviour is untouched: the feature lives entirely below the storage repository
contract. Sibling specs:
[dsql-schema-connection](../dsql-schema-connection/requirements.md) introduced the
connector; [dsql-reservoir-redesign](../dsql-reservoir-redesign/requirements.md) made the
reservoir the sole owner of connections and left `connect_with` as the connector's one
job; [tonic-0-14-grpc-stack](../tonic-0-14-grpc-stack/requirements.md) owns the lock
invariant and defined the Connector Exception this feature ends;
[managed-embedded-dsql](../managed-embedded-dsql/requirements.md) owns the billable live
lifecycle run this feature reuses as evidence.

## Glossary

- **Connector:** the `aurora-dsql-sqlx-connector` crate. Tokeira uses one function of it,
  `connection::connect_with`, from the Connection Factory.
- **Connection Factory:** `crates/tokeira-storage/src/dsql/connection_factory.rs`, the single
  module that names the Connector and maps its errors to Failure Categories.
- **Reservoir:** the storage-owned connection pool. There is no SQLx `PgPool` in the runtime
  path; `PgPool` appears only in test helpers and in two public `MigrationRunner` and
  `ControlLeaseRepository` signatures.
- **Legacy Client Set:** the crates that exist in the lock only because the AWS SDK's legacy
  HTTPS client is enabled: hyper 0.14, h2 0.3, hyper-rustls 0.24, tokio-rustls 0.24, rustls
  0.21, rustls-webpki 0.101, and webpki-roots 0.26 (the last is SQLx 0.8's root store; SQLx
  0.9 uses webpki-roots 1).
- **SDK Type Dependencies:** http 0.2 and http-body 0.4. The AWS SDK crates require them
  unconditionally for their own types (`aws-sdk-dsql` names `http = "0.2.9"` without a
  feature gate; `aws-smithy-runtime` names `http-body` 0.4 without a gate and always enables
  `aws-smithy-types/http-body-0-4-x`). No HTTP client or server implementation stands behind
  them, and they remain after the Legacy Client Set leaves.
- **Connector Exception:** the clause in the engine's lock invariant that allows the Legacy
  Client Set while the Connector is pinned below 0.2. This feature ends it.
- **Lock Invariant:** the engine test `workspace_resolves_one_grpc_and_http_stack` in
  `crates/tokeira-engine/tests/embedded_architecture.rs`, which reads `Cargo.lock` and
  asserts which lines may exist.
- **SQL Safety Contract:** SQLx 0.9's `SqlSafeStr` trait
  (`sqlx-core-0.9.0/src/sql_str.rs`), implemented for `&'static str` and for
  `AssertSqlSafe<T>` over `&str`, `String`, `Box<str>`, `Arc<str>`, `Arc<String>`, and
  `Cow<'static, str>`. Every `query`, `query_as`, `query_scalar`, and `raw_sql` function takes
  `impl SqlSafeStr`.
- **Attested Site:** a `query*()` call whose SQL argument is wrapped in `AssertSqlSafe`,
  carrying an inline comment that names the source of the text and states why it cannot
  contain request data.
- **Failure Category:** the label `ConnectionFactoryError::kind()` returns, recorded by
  `record_dsql_connection_error` when a physical connection attempt fails: `config`,
  `token`, `connection`, `database`, and today also `occ_retry`.
- **Log Bridge:** `tracing-log`'s `LogTracer`, which `SubscriberInitExt::try_init` installs
  when `tracing-subscriber`'s `tracing-log` feature is enabled, forwarding `log` records to
  the tracing subscriber.
- **Live Evidence:** test runs against a real DSQL cluster that this feature requires before
  it is accepted. They never run in CI and are gated by the `dsql-integration` feature and
  environment variables.
- **Bar:** the finishing commands in root `AGENTS.md` §10.4, plus `cargo deny check`, which
  CI runs as a merge gate although the Bar does not include it.

## Target State

- **Manifests.** `crates/tokeira-storage/Cargo.toml` pins
  `aurora-dsql-sqlx-connector = "=0.2.2"` with no features (the `pool` feature is dropped
  because no code path uses the Connector's pool) and `sqlx = "0.9"` with default features
  off and the present feature list. `crates/tokeira-projection/Cargo.toml` and
  `apps/tkr/Cargo.toml` move their `sqlx` lines to 0.9 with their present feature lists. The
  root `Cargo.toml` names `tracing-log` in the `tracing-subscriber` feature list.
- **Lock.** Exactly one line each of `sqlx`, `sqlx-core`, `sqlx-postgres` (0.9) and
  `aurora-dsql-sqlx-connector` (0.2.2). None of the Legacy Client Set. The SDK Type
  Dependencies remain and are named as such by the Lock Invariant. Every other line moves
  only if the resolver requires it for the two new versions; the lock diff is reviewed.
- **Advisories.** `deny.toml` drops RUSTSEC-2026-0098, -0099, -0104, and -0258. The two
  unrelated entries (RUSTSEC-2023-0089, RUSTSEC-2026-0097) stay.
- **SQL sites.** Every `query*()` call passes a `&'static str` or is an Attested Site. The
  eleven sites that pass built strings are attested; the two sites that iterate a static
  slice pass the element by value. No new dynamic SQL and no `raw_sql` is introduced.
- **Connection Factory.** `connect_with` remains the only Connector call. The error mapping is
  exhaustive over the Connector's variants with no wildcard arm, and the `occ_retry`
  Failure Category is removed: the only producer was the Connector's `OCCRetryExhausted`
  variant, which its `retry_on_occ` helper alone constructs, which Tokeira never calls, and
  which 0.2.2 compiles only under the `occ` feature.
- **Diagnostics.** Connector records, now emitted through `log`, reach the process tracing
  subscriber through the Log Bridge, which is declared rather than inherited from a default.
- **Live Evidence.** A new endpoint-gated test drives the Connector's IAM path end to end;
  the existing URL-gated storage and projection suites run on SQLx 0.9; the managed
  lifecycle test runs once on the migrated stack. The environment variables that gate them
  are documented under `docs/testing/`.
- **Release notes.** A `changed` fragment (public storage signatures move to SQLx 0.9 types;
  the connector moves to 0.2.2) and a `security` fragment (the legacy AWS client and its four
  advisories leave the workspace).
- **Out of scope.** Adopting the Connector's `occ` feature; consolidating `sqlx` into
  `[workspace.dependencies]`; changing Reservoir behaviour; moving AWS SDK crate versions
  beyond what the resolver requires; SQLx's `sqlx.toml` configuration; changing `tkr`'s
  narrower `sqlx` feature list (feature unification makes the difference moot inside the
  workspace).

## Evidence From Current Code

Manifests and lock (state at `origin/main` 6f974fe1):

- `crates/tokeira-storage/Cargo.toml:22` pins `aurora-dsql-sqlx-connector = "=0.1.2"` with
  `features = ["pool"]`, optional under `dsql`; `:31` pins `sqlx = "0.8"` with
  `["runtime-tokio", "tls-rustls", "postgres", "time", "uuid"]`.
  `crates/tokeira-projection/Cargo.toml:25` carries the same `sqlx` line;
  `apps/tkr/Cargo.toml:28` carries `sqlx = "0.8"` with `["runtime-tokio", "tls-rustls",
  "postgres"]`, not optional. No `sqlx` entry exists in `[workspace.dependencies]`.
- `Cargo.lock` resolves `sqlx` 0.8.6, the connector 0.1.2, `aws-sdk-dsql` 1.55.0,
  `aws-smithy-runtime` 1.12.1, `aws-smithy-http-client` 1.2.0, and both lines of hyper
  (0.14.32, 1.10.1), h2 (0.3.27, 0.4.16), hyper-rustls (0.24.2, 0.27.9), rustls (0.21.12,
  0.23.38), rustls-webpki (0.101.7, 0.103.13), and webpki-roots (0.26.11, 1.0.7).
- `cargo tree -i aws-sdk-dsql -e features` shows the connector as the only enabler of
  `aws-sdk-dsql/rustls`; `aws-sdk-dsql` 1.55.0 defines `rustls = ["aws-smithy-runtime/tls-rustls"]`
  and `default = ["rustls", "default-https-client", "rt-tokio"]`;
  `aws-smithy-runtime/tls-rustls` enables `connector-hyper-0-14-x` and
  `aws-smithy-http-client/legacy-rustls-ring`, which is where hyper 0.14, hyper-rustls 0.24,
  tokio-rustls 0.24, and rustls 0.21 enter. `webpki-roots` 0.26.11 has one parent,
  `sqlx-core` 0.8.6. `tokeira-aws` and `tokeira-managed-dsql` already depend on
  `aws-sdk-dsql` with `default-features = false, features = ["default-https-client",
  "rt-tokio"]`.
- `aws-sdk-dsql-1.55.0/Cargo.toml:112` names `http = "0.2.9"` with no feature gate;
  `aws-smithy-runtime-1.12.1/Cargo.toml:153` names `http-body` 0.4.6 with no gate and `:137`
  always enables `aws-smithy-types/http-body-0-4-x`. `aws-runtime-1.9.1/Cargo.toml:114`
  names `http-body` 0.4 as optional. The SDK Type Dependencies therefore survive the move.
- A scratch project depending only on `aurora-dsql-sqlx-connector = "0.2"` and
  `sqlx = "0.9"` resolves the connector 0.2.2, `sqlx` 0.9.0, `aws-sdk-dsql` 1.69.0, hyper
  1.11.1, h2 0.4.19, hyper-rustls 0.27.9, rustls 0.23.43, rustls-webpki 0.103.15, and no
  copy of any Legacy Client Set crate.

Connector 0.2.2 against 0.1.2 (registry sources):

- `Cargo.toml.orig`: `aws-sdk-dsql = { version = "1.0", default-features = false }` (0.1.2:
  `aws-sdk-dsql = "1.0"`); `sqlx = { version = "0.9", features = ["runtime-tokio",
  "postgres", "tls-rustls-ring"] }`; `log = "0.4"` replaces `tracing = "0.1"`;
  `aws-credential-types = "1"` added; `rust-version = "1.94"`; features `default = []`,
  `pool`, `occ`.
- `CHANGELOG.md`, entry `rust/sqlx/v0.2.1`: "Breaking change: upgraded sqlx from 0.8 to
  0.9", "Dynamic SQL strings now require `AssertSqlSafe()` wrapping per sqlx 0.9", MSRV 1.85
  to 1.94.
- `src/connection.rs`: `connect(url)` and `connect_with(&DsqlConnectOptions) ->
  Result<PgConnection>` are byte-identical between the two versions.
- `src/error.rs`: `DsqlError::{ConfigError, TokenError, ConnectionError, DatabaseError}` keep
  their shapes; `OCCRetryExhausted` gains an `occ_type` field and `#[cfg(feature = "occ")]`.
- `src/config.rs` and `src/pool.rs`: `tracing::warn!`/`tracing::error!` become `log::debug!`/
  `log::error!`; `DsqlConnectOptions` gains an optional `credentials_provider`;
  `from_connection_string` and `authenticated_pg_options` keep their signatures.

SQLx 0.9.0 against 0.8.6 (registry sources; `sqlx-0.9.0/CHANGELOG.md` section `0.9.0`):

- `#3723` `SqlSafeStr`: `query`, `query_as`, `query_scalar`, and `raw_sql` take
  `impl SqlSafeStr` (`sqlx-core-0.9.0/src/query.rs:653`, `query_as.rs:341`,
  `query_scalar.rs:322`, `raw_sql.rs:119`); the trait is implemented only for
  `&'static str` and `AssertSqlSafe` (`sql_str.rs:47-114`); `AssertSqlSafe<&str>` copies the
  string, the owned forms do not.
- `#3821`: MSRV 1.94; combined runtime-and-TLS features deleted. The workspace uses
  `runtime-tokio` and `tls-rustls`, which survive unchanged: `tls-rustls = ["tls-rustls-ring"]`,
  `tls-rustls-ring = ["tls-rustls-ring-webpki"]` in both `sqlx-0.8.6/Cargo.toml:501-514` and
  `sqlx-0.9.0/Cargo.toml:221-234`.
- `#3960`: the `Arguments` trait loses its lifetime (`sqlx-core-0.9.0/src/arguments.rs:12`);
  `Query<'q, DB, A>` (`query.rs:18`), `Encode<'q, DB>` (`encode.rs:30`), and `PgArguments`
  (`sqlx-postgres-0.9.0/src/arguments.rs:70`) keep their shapes.
- `DatabaseError` (`sqlx-core-0.9.0/src/error.rs`) has the same required methods as 0.8.6:
  `message`, `as_error`, `as_error_mut`, `into_error`, `kind`; `ErrorKind::Other` remains
  (`error.rs:209`).
- `#4042`: webpki-roots 1 replaces 0.26 under `tls-rustls-ring-webpki`.
- `#3486`: the tracing field `aquired_after_secs` is respelled; `#3800`:
  `PgConnectOptions::options()` escapes automatically; `#4008`: `#[derive(sqlx::Type)]`
  emits `PgHasArrayType` for newtypes; `#4077`: the `offline` feature becomes optional;
  `#3613`: `RawSql` methods gain a `DB` parameter; `#3383`: the `Migrate` trait changes.
- The workspace's default feature set for `sqlx` (`any`, `macros`, `migrate`, `json`) is
  enabled by the connector, which does not turn defaults off, in both 0.1.2 and 0.2.2;
  the workspace's own manifests turn them off.

Workspace usage (anchors at `origin/main` 6f974fe1):

- `crates/tokeira-storage/src/dsql/connection_factory.rs:9` imports `DsqlConnectOptions` and
  `DsqlError`; `:28` calls `from_connection_string`; `:35` calls
  `aurora_dsql_sqlx_connector::connection::connect_with`, the only Connector call in the
  workspace; `:60-70` maps `DsqlError` variants including `OCCRetryExhausted { source, .. }`
  behind a wildcard arm; `:72-80` returns the five Failure Category labels; `:126-156` is
  the unit test constructing the four non-OCC variants by value. No code names
  `aurora_dsql_sqlx_connector::pool` or `DsqlConnectOptionsBuilder`.
- Built SQL strings reach `query*()` at `crates/tokeira-storage/src/dsql/migration.rs:347`,
  `:403`, `:1037`, `:1041` (`&migration.sql`, a `String` copied from the embedded migration
  corpus at `:697` and `:623`);
  `crates/tokeira-storage/src/dsql/worker_compute_repository.rs:889`, `:954`, `:1002`
  (`format!` over the `ACTION_COLUMNS` constant); and
  `crates/tokeira-projection/src/dsql_store.rs:300`, `:1577`, `:1607`, `:1666` (the SQL
  compiler's output). `migration.rs:324` and `:511` iterate `&'static [&'static str]`
  slices and pass `&&'static str`, which coerces today and fails the generic bound under
  0.9. Sixteen further non-literal arguments are `const` items or `const fn` results of
  type `&'static str` and need no change. No `raw_sql`, `query!`, `sqlx::migrate!`,
  `Migrator`, `PgHasArrayType`, `#[derive(sqlx::Type)]`, `Arguments`, `TransactionManager`,
  `PgAdvisoryLock`, `PgListener`, `PgConnectOptions::options`, or `.sqlx/` directory exists.
- `crates/tokeira-projection/src/dsql_store.rs:1542` (`bind_sql_values`) names
  `Query<'q, Postgres, PgArguments>`; `crates/tokeira-storage/src/dsql/run_repository/mod.rs:2001`
  is a `#[cfg(test)]` `impl sqlx::error::DatabaseError`; both compile unchanged on 0.9.
- Public items naming SQLx types: `DsqlPermit::connection() -> Result<&mut PgConnection>`
  (`connection.rs:854`), `PhysicalConnectionFactory::create_connection`
  (`connection_factory.rs:84-86`), `MigrationRunner::{apply, status}(&PgPool)` and
  `{assess_connection, bootstrap_migration_coordination, apply_connection, apply_decision,
  status_connection}(&mut PgConnection)` (`migration.rs:294-669`),
  `SchemaCompatibilityError::Database(#[from] sqlx::Error)` (`migration.rs:71`),
  `ControlLeaseError::Database(sqlx::Error)` with `From<sqlx::Error>` (`control_lease.rs:209,
  222`), `ControlLeaseRepository::{new, with_clock}(PgPool, ..)` (`control_lease.rs:407, 416`),
  and `WorkerComputeRepository::list_health_with_connection(&mut sqlx::PgConnection, ..)`
  (`worker_compute_repository.rs:71`). `tokeira-projection` and `tokeira-engine` expose none.
  `apps/tkr/src/commands/schema.rs:8` names `sqlx::PgConnection` only to call the
  `MigrationRunner` connection methods.
- `crates/tokeira-engine/tests/embedded_architecture.rs:171-194` is the Connector Exception:
  hyper 0.14, http 0.2, http-body 0.4, h2 0.3, hyper-rustls 0.24, and rustls 0.21 may exist
  only while `aurora-dsql-sqlx-connector` starts with `0.1.`. `:48-78` is the guard test
  asserting `dsql-integration` is not a default feature, that
  `live_managed_dsql.rs` and `dsql_embedded_ownership.rs` open with the feature gate, and
  that neither they nor `embedded_telemetry.rs` sleep.
- `deny.toml:25-41` carries the four advisories with the exit "Drop these three when the
  connector pin moves to a release without the service defaults (0.2.x, which needs SQLx
  0.9)" and "Drop this entry with the rustls-webpki ones when the connector pin moves";
  `:73-79` sets `multiple-versions = "warn"` and `wildcards = "deny"`.
- Root `Cargo.toml:147`: `tracing-subscriber = { version = "0.3", features = ["fmt",
  "env-filter", "json"] }` with defaults on; `Cargo.lock` lists `tracing-log` 0.2.0 under
  `tracing-subscriber` 0.3.23; `crates/tokeira-observability/src/tracing.rs:175` and `:188`
  install the subscriber with `try_init`, which `tracing-subscriber-0.3.23/src/util.rs:61-77`
  documents as also installing the `log` compatibility layer under that feature. No
  workspace crate names `log`, `tracing-log`, or `LogTracer`.
- Live tests: `crates/tokeira-storage/tests/dsql_shard_leasing.rs` (four tests),
  `crates/tokeira-storage/tests/dsql_embedded_ownership.rs` (one), and
  `crates/tokeira-projection/tests/dsql_projection_persistence.rs` (five) are gated by
  `#![cfg(feature = "dsql-integration")]` and no-op unless `TOKEIRA_DSQL_TEST_DATABASE_URL`
  or `DATABASE_URL` is set; they connect through `DatabaseUrlConnectionFactory` or
  `PgPoolOptions`, not through the Connector.
  `crates/tokeira-storage/tests/dsql_schema_bootstrap.rs` additionally requires
  `TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_DATABASE_URL` and an acknowledgement variable.
  `crates/tokeira-engine/tests/live_managed_dsql.rs` creates and destroys a cluster and
  exercises `DsqlStore::connect_embedded`, hence the Connector, per
  [managed-embedded-dsql-live-aws.md](../../../docs/testing/managed-embedded-dsql-live-aws.md).
  `TOKEIRA_DSQL_TEST_DATABASE_URL` appears in no document under `docs/`.
- `docs/architecture/060-connection-management.md:8` and `:14` describe the Reservoir as the
  sole connection owner using `connect_with`; `docs/crates/storage.md` and
  `docs/crates/projection.md` name no SQLx or Connector type. Neither needs an edit.
- `.changie.yaml` requires fragments named `{kind}-{slice}.yaml` under `.changes/unreleased/`
  with a 36-character `custom.Slice` and a body of 8 to 180 characters.

## Dependency Policy

One row per dependency line this feature owns.

| Line | Target | If violated | Persistence or side-effect impact |
|------|--------|-------------|-----------------------------------|
| `tokeira-storage` → `aurora-dsql-sqlx-connector` | `"=0.2.2"`, no features, optional under `dsql` | build fails under `--locked`; Lock Invariant fails on any Legacy Client Set line | none; connections are created the same way |
| `tokeira-storage` → `sqlx` | `"0.9"`, defaults off, `["runtime-tokio", "tls-rustls", "postgres", "time", "uuid"]` | compile error at every built-string site (`SqlSafeStr`) | none |
| `tokeira-projection` → `sqlx` | same line as storage | as above | none |
| `tkr` → `sqlx` | `"0.9"`, defaults off, `["runtime-tokio", "tls-rustls", "postgres"]` | compile error in `commands/schema.rs` | none |
| root → `tracing-subscriber` | features gain `"tracing-log"` | the Log Bridge property fails | Connector diagnostics would vanish if the default were ever turned off |
| `Cargo.lock` | one line each of `sqlx*` (0.9) and the connector (0.2.2); Legacy Client Set absent; SDK Type Dependencies allowed | Lock Invariant fails | none |
| `deny.toml` advisories | RUSTSEC-2026-0098, -0099, -0104, -0258 removed; RUSTSEC-2023-0089 and RUSTSEC-2026-0097 kept | `cargo deny check advisories` fails on an unused or missing ignore | none |

## SQLx 0.9 Change Policy

Every breaking entry in the 0.9.0 changelog section, with Tokeira's exposure. "Not
exposed" rows are verified by the searches recorded in the evidence above.

| Change | Exposure | Policy |
|--------|----------|--------|
| `SqlSafeStr` on `query*()` and `raw_sql` (#3723) | 13 sites | attest 11, pass the 2 slice elements by value (Requirement 3) |
| MSRV 1.94 (#3821) | workspace `rust-version = "1.97"`, toolchain 1.97.1 | none |
| combined runtime-and-TLS features deleted (#3821) | not used; `runtime-tokio` and `tls-rustls` survive | none |
| `Arguments` lifetime removed (#3960) | trait not named; `Query<'q, Postgres, PgArguments>` unchanged | none |
| `RawSql` gains `DB` parameter, `fetch_optional` return change (#3613, #3924) | not used | none |
| `Decode for Cow` decodes owned (#3674) | not decoded | none |
| `PgHasArrayType` derived for newtypes (#4008) | no derives, no manual impls | none |
| `offline` optional (#4077) | no `query!` macros, no `.sqlx/` | none |
| `PgConnectOptions::options()` escaping (#3800) | not called; the Connector builds options without it | none |
| `Migrate` trait and `Migrator` setters (#3383, #3526) | migrations use Tokeira's own `schema_version` runner | none |
| `PgAdvisoryLockGuard` lifetime (#3495) | not used | none |
| tracing field respelled (#3486) | no log parsing | none |
| `Pool::close` closes all connections first (#3952) | pools exist only in tests | none |
| `DatabaseError` trait | one `#[cfg(test)]` impl; required methods unchanged | none |
| webpki-roots 1 (#4042) | root store for DSQL's public certificate chain | verified by Live Evidence |
| unnamed statements when not persistent, non-prepared metadata queries (#3863, #4226) | protocol-level; DSQL speaks the PostgreSQL extended protocol | verified by Live Evidence |

## Requirements

### Requirement 1: One SQLx line and one Connector line

**User Story:** As a maintainer, I want the workspace to resolve one SQLx and one Connector
version with the legacy AWS client gone, so that the advisories against that client close
and no second TLS stack is compiled into the server.

#### Acceptance Criteria

1. THE `tokeira-storage` manifest SHALL pin `aurora-dsql-sqlx-connector` at exactly `0.2.2`,
   optional under the `dsql` feature, with no Connector features enabled.
2. THE `tokeira-storage`, `tokeira-projection`, and `tkr` manifests SHALL name `sqlx` at
   `0.9` with default features off and their present feature lists unchanged.
3. THE workspace lock SHALL contain exactly one version each of `sqlx`, `sqlx-core`,
   `sqlx-postgres`, and `aurora-dsql-sqlx-connector`, on the 0.9 and 0.2 lines respectively.
4. THE workspace lock SHALL contain no version of hyper 0.14, h2 0.3, hyper-rustls 0.24,
   tokio-rustls 0.24, rustls 0.21, rustls-webpki 0.101, or webpki-roots 0.26.
5. THE workspace lock MAY contain http 0.2 and http-body 0.4 as SDK Type Dependencies, and
   THE Lock Invariant SHALL record them as such rather than as part of any exception. This
   supersedes acceptance criterion 1.3 of
   [tonic-0-14-grpc-stack](../tonic-0-14-grpc-stack/requirements.md), which expected both to
   leave with the Connector; the AWS SDK requires them regardless of its HTTP client.
6. THE Lock Invariant SHALL assert criterion 4 unconditionally: the Connector Exception
   clause is removed, so a future Connector or AWS SDK move that re-enables the legacy
   client fails the test rather than being tolerated.
7. WHEN the lock is regenerated for this feature, THE only lines that move SHALL be the
   SQLx and Connector crates and the dependencies the resolver requires for their new
   versions; every other movement is reported in the change and justified.
8. THE workspace SHALL build, lint, and test with `--locked` at every checkpoint.

### Requirement 2: Advisory policy follows the lock

**User Story:** As an operator reviewing supply-chain posture, I want `deny.toml` to carry
no advisory whose stated exit has arrived, so that the file states the true exposure.

#### Acceptance Criteria

1. THE `deny.toml` advisory ignore list SHALL no longer contain RUSTSEC-2026-0098,
   RUSTSEC-2026-0099, RUSTSEC-2026-0104, or RUSTSEC-2026-0258, nor the comments that
   justified them.
2. THE `deny.toml` advisory ignore list SHALL keep RUSTSEC-2023-0089 and RUSTSEC-2026-0097
   with their comments unchanged.
3. WHEN `cargo deny check advisories bans licenses sources` runs against the migrated lock,
   THE check SHALL pass without reporting the four removed advisories as encountered.

### Requirement 3: Every SQL string is proven or attested

**User Story:** As a reviewer, I want every dynamic SQL string that reaches SQLx to state
where its text comes from and why request data cannot be in it, so that SQLx's safety
contract is met deliberately rather than by blanket wrapping.

#### Acceptance Criteria

1. THE SQL argument of every `sqlx::query`, `query_as`, and `query_scalar` call in the
   workspace SHALL be either a `&'static str` or an `AssertSqlSafe` value.
2. EACH Attested Site SHALL carry an inline comment naming the source of the SQL text and
   the reason that text cannot carry request data.
3. THE four migration-runner sites SHALL attest that the text is a migration from the
   embedded corpus whose checksum the runner verifies against the ledger, and THE runner
   SHALL execute the embedded bytes unchanged.
4. THE three worker-compute sites SHALL attest that the only interpolated value is the
   `ACTION_COLUMNS` constant.
5. THE four projection sites SHALL attest that the text is the SQL compiler's output, and
   FOR ANY filter, sort, or grouping input, THE compiled SQL SHALL contain request values
   only as `$n` bind placeholders, with every value carried in the bind list.
6. THE two sites that iterate `&'static [&'static str]` slices SHALL pass each element by
   value as `&'static str` and SHALL NOT be attested.
7. THE feature SHALL introduce no `raw_sql` call and no new dynamic SQL site.
8. THE `tokeira-storage`, `tokeira-projection`, and `tkr` sources SHALL contain no
   `AssertSqlSafe` use other than the eleven Attested Sites, and a static check SHALL fail
   when an `AssertSqlSafe` appears without the comment criterion 2 requires.

### Requirement 4: The Connector seam keeps its contract

**User Story:** As an operator, I want connection failures classified exactly as before and
Connector diagnostics still visible, so that the move changes what is compiled and nothing
about what I observe.

#### Acceptance Criteria

1. THE Connection Factory SHALL remain the only module naming the Connector, and
   `connection::connect_with` SHALL remain its only Connector call.
2. THE Failure Categories `config`, `token`, `connection`, and `database` SHALL keep their
   labels and their mapping from `DsqlError::ConfigError`, `TokenError`,
   `ConnectionError`, and `DatabaseError`.
3. THE `occ_retry` Failure Category and the `ConnectionFactoryError::OccRetry` variant SHALL
   be removed, because no path in Tokeira could produce them and the Connector no longer
   compiles the source variant without its `occ` feature.
4. THE Connector error mapping SHALL match the Connector's variants exhaustively with no
   wildcard arm, so that a variant added by a future Connector release is a compile error
   that forces a classification decision rather than a silent `connection` label.
5. THE root manifest SHALL name `tracing-log` in the `tracing-subscriber` feature list, and
   THE workspace lock SHALL resolve `tracing-log` as a dependency of `tracing-subscriber`.
6. WHEN a subscriber is installed through `SubscriberInitExt::try_init` under the
   workspace's `tracing-subscriber` features, THEN a `log` record at or above the
   subscriber's level SHALL be delivered to that subscriber as a tracing event.
7. THE per-connection behaviour of the Connector path (an IAM token minted for each
   physical connection, TLS with `verify-full`, the admin role and the region taken from the
   configured endpoint) SHALL be unchanged, as verified by Live Evidence.

### Requirement 5: Public types and release notes

**User Story:** As a downstream consumer of `tokeira-storage`, I want the SQLx major to be
declared as the breaking change it is, so that my pin moves with the 0.3.0 train and not by
surprise.

#### Acceptance Criteria

1. THE public `tokeira-storage` items that name `PgConnection`, `PgPool`, or `sqlx::Error`
   SHALL keep their names and shapes, now over SQLx 0.9 types.
2. THE feature SHALL ship in the 0.3.0 release and SHALL NOT change the workspace version
   itself; the release train owns the bump.
3. THE feature SHALL add a `changed` release-note fragment stating that public storage
   signatures move to SQLx 0.9 types and the Connector to 0.2.2, and a `security` fragment
   stating that the legacy AWS HTTPS client and its four advisories leave the workspace.
4. THE `tkr` binary SHALL compile and its schema commands SHALL keep their behaviour.

### Requirement 6: Live Evidence and verification

**User Story:** As the integration seat, I want the migrated connection path proven against a
real DSQL cluster before this feature is accepted, so that a driver or connector regression
cannot reach a release on the strength of unit tests alone.

#### Acceptance Criteria

1. THE Bar and `cargo deny check` SHALL pass at the feature's head.
2. THE feature SHALL add a test, gated by `dsql-integration` and skipped unless
   `TOKEIRA_DSQL_TEST_ENDPOINT` and `TOKEIRA_DSQL_TEST_REGION` are set, that builds a
   Connection Factory from those values, creates a connection through the Connector, and
   round-trips a query on it.
3. THE guard test `credentialed_sql_and_aws_tests_are_non_default_and_sleep_free` SHALL cover
   the new test file: the feature gate on its first line and the absence of sleeps.
4. WHEN the feature is accepted, THE new test, the ten URL-gated storage and projection
   tests, and the managed lifecycle test SHALL each have run green on the migrated stack
   against a live cluster, and THE task ledger SHALL record each run with its date.
5. THE documentation under `docs/testing/` SHALL describe how to run the URL-gated suites
   and the new endpoint-gated test, naming every environment variable and its fallback,
   without naming any cluster, host, or account.
6. THE architecture and crate documents that describe connection management SHALL remain
   accurate without edits, as confirmed in the change.
