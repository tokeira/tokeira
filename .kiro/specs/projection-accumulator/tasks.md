# Implementation Plan: projection accumulator in run state

Implement the [approved design](design.md) and [bugfix requirements](bugfix.md).
This is a storage refactor preserving the complete projection images produced by
Tokeira `689e89a8622d114de1dd80232a22bb63705d50d1`. The four documented Temporal
compatibility corrections remain separate work.

Every property-test task below is required. Use existing `proptest` support with
at least **100 valid cases per property per executed backend**, retained shrinking
and deterministic race coordination. Tags identify the design property; each
property test also states its invariant in a one-line comment. Checkpoints are
marked complete only after their commands pass; execution evidence is recorded below.

## Tasks

- [x] 1. Write the bug-condition exploration property test before changing production code
  - Generate valid ordinary transition sequences and observe preceding-image
    lookups in the baseline commit path. Assert that commits make no such lookup;
    run the property against the unfixed code and record its shrunk failing
    sequence. Observe actual lookup calls through narrowly scoped test
    instrumentation, not a source-text match or connection-acquisition count.
  - Start with the in-memory repository so this reproduction needs no database.
    Cover DSQL statement observation when the existing SQL harness supports it.
    If the test unexpectedly passes, investigate the premise or instrumentation
    before implementing the fix. Keep the regression assertion for Task 7.1.
  - Freeze the baseline merge and raw projection derivation as test-only Tokeira
    code with its commit provenance. Do not copy Temporal implementation code or
    delegate the oracle's accumulator derivation to the new production helper.
  - Tag: `// Feature: projection-accumulator, Property 1: no projection reads in commits`
  - _Requirements: 1.1, 1.2, 2.1, 3.1_

- [x] 2. Add the per-run field, initialization and frozen extension payload
  - [x] 2.1 Add and document the serde-skipped field in `tokeira-kernel/src/state.rs`
    - Add `used_worker_deployment_versions: Option<Vec<String>>` directly to
      `WorkflowState`. Keep it outside `WorkflowVersioningInfo`; compaction must
      preserve it, and routing inheritance must not copy another run's list.
    - Initialize fresh start, signal-with-start and replay-constructed states to
      `Some(Vec::new())`. Update-with-start keeps its delegated start path.
      Replay and completion helpers carry the field without accumulating events.
    - Update affected struct literals and fixtures across the workspace. Specify
      whether each fixture represents fresh, ready or legacy state; do not hide
      the distinction behind a blanket default that makes all fixtures ready.
    - _Requirements: 2.5, 2.6, 2.17, 3.2, 3.3, 3.4_
  - [x] 2.2 Extend the existing codec and snapshot section handling
    - Add tag 3 with the frozen postcard `Vec<String>` payload. Emit it for every
      `Some`, including empty; absence decodes to `None`. Preserve the positional
      state, envelope version, snapshot format and existing section tags.
    - Decode tag 3 with exact-payload validation and the existing extension error
      surface. Preserve seeded strings and duplicates verbatim. Keep framing
      checks, ordered unique emitted tags and unknown-tag skipping.
    - Reuse the existing per-run snapshot extension without another snapshot tag,
      eager seeding or a new snapshot representation. Document the new payload in
      the codec's section contract.
    - _Requirements: 2.16, 2.17, 2.18, 2.19_
  - [x] 2.3 Required property test: Property 5 — extension compatibility
    - Generate absent, ready-empty and nonempty fields, including repeated and
      empty strings. Check hot-state and snapshot round trips and unchanged
      positional bytes against the frozen baseline layout.
    - Cover coexistence with existing known sections, skipped unknown tags,
      ordering, duplicate tags, invalid magic, truncated payloads and trailing
      payload bytes. Assert errors return no usable decoded/restored result.
    - Tag: `// Feature: projection-accumulator, Property 5: extension compatibility`
    - _Requirements: 2.16, 2.17, 2.18, 2.19_

- [x] 3. Implement the shared pure preparation and seed helpers in storage
  - [x] 3.1 Add `prepare_workflow_projection` and `ProjectionAccumulatorError`
    - Require readiness, derive the baseline raw context, append only new
      observations in encounter order, overlay a nonempty result, then update
      the local field. Preserve raw absent, explicitly empty and differently
      typed search attributes when the accumulated list is empty.
    - Complete fallible context construction before updating the field. Keep
      deletion on the existing redacting builder, and do not mutate workflow
      search attributes, history, routing or effects.
    - Correct the nearby comments that currently imply completion-only or fully
      Temporal-equivalent derivation; cite the preservation baseline and deferred
      cases rather than carrying those inaccurate claims into the new helper.
    - _Requirements: 2.1, 2.4, 3.1, 3.4, 3.5, 3.7_
  - [x] 3.2 Add `seed_workflow_projection_accumulator`
    - For an unseeded local copy, validate record/run identity and reject a row
      sequence newer than the state. Accept older rows and copy the keyword list
      verbatim. Missing images, missing attributes and non-keyword values seed
      empty; corrupt data fails with run and defect context.
    - Leave ready inputs unchanged. Unit-test error precedence and identity
      mismatches without introducing a new public RPC error classification.
    - _Requirements: 2.7, 2.9, 2.10, 2.11, 2.12, 2.13_
  - [x] 3.3 Required property test: Property 2 — complete baseline image equivalence
    - Compare the entire `ProjectionContext` against the frozen oracle over
      generated transitions and accepted search-attribute values. Include
      repeated/changing versions, unversioned transitions, unspecified behaviour,
      versioning-info compaction and lists exceeding the later trimming limit.
    - Assert the resulting field equals the emitted image's extracted list and
      preparation is idempotent. Task 7.2 extends this property to persisted and
      returned repository state; Task 11 runs its DSQL instance.
    - Tag: `// Feature: projection-accumulator, Property 2: complete baseline image equivalence`
    - _Requirements: 2.2, 2.3, 3.1, 3.4, 3.5_

- [x] 4. Seed legacy state at commit-capable load boundaries
  - [x] 4.1 Implement consistent DSQL legacy loads in `dsql/run_repository/load.rs`
    - Retain the single-read fast path for absent or ready state. On unseeded
      state, use the same acquired read permit to begin the design's explicit
      read-only repeatable-read transaction and reread hot state and statistics.
      Discard values from the initial read. Handle disappearance or newly
      persisted readiness before deciding whether an image lookup is needed.
    - Fetch the latest image by run key across all partitions, decode and validate
      it, and seed the local state in that transaction. Do not restrict the query
      to today's partition or filter out newer-than-state rows before validation.
    - Finish the transaction before returning state and statistics. Perform no
      hot-state update, readiness backfill or commit-path fallback read.
    - _Requirements: 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 2.14, 2.15_
  - [x] 4.2 Implement in-memory seeding under the existing store mutex
    - Clone state and statistics and obtain the legacy image under one lock.
      Seed only the returned clone; preserve `store.runs` and its durable
      readiness. Keep `load_run` delegated to `load_run_with_stats`.
    - Keep the latest-image index for legacy loads and the cursor index for
      projection consumers; restore continues to rebuild disposable indexes.
    - _Requirements: 2.7, 2.9, 2.10, 2.11, 2.12, 2.13, 2.14, 2.15, 2.19_
  - [x] 4.3 Required property test: Property 4 — consistent, non-durable legacy seeding
    - Generate legacy states and images with empty, malformed, differently typed
      and repeated values; older/newer sequences; invalid identities; and absent
      images. Compare the helper and memory load with the preservation model.
    - Assert loading never changes stored readiness, including a snapshot taken
      after load but before persistence. Task 11 supplies the required real
      SQLx snapshot schedules and historical-partition cases for this property.
    - Tag: `// Feature: projection-accumulator, Property 4: consistent non-durable legacy seeding`
    - _Requirements: 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.15_

- [x] 5. Fold once in both commit implementations before writes and growth measurement
  - [x] 5.1 Refactor the DSQL internal writer and projection insertion
    - Keep public repository methods and bundle wrapping unchanged. Preserve
      fence, OCC, dedupe and current-execution outcome precedence, then prepare
      the image against a mutable local clone of the transition state.
    - Compute history accounting as before, encode the folded state once and use
      those exact bytes for growth measurement and persistence. Encode the prepared
      context before writes and pass its bytes to an INSERT-only projection helper;
      remove its preceding-image SELECT and decode.
    - Return that persisted state only after commit succeeds. Discard local
      changes on any failure and retain existing serialization-conflict mapping.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.21, 2.22, 3.6, 3.8_
  - [x] 5.2 Refactor the in-memory commit under its existing mutex
    - Use the same pure preparation after existing admission checks and before
      mutating any durable map, including history statistics. Measure the folded
      state using the existing codec and growth policy.
    - Persist and return the same state with the prepared image. Remove the
      latest-image lookup from commits while retaining index maintenance.
    - Apply the readiness guard to both entry points, including existing reset
      runs at expected sequence zero; never silently seed during a commit.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.21, 2.22, 3.6, 3.8_

- [x] 6. Checkpoint: the exploration regression is green and storage foundations pass
  - Rerun Task 1's unchanged no-read assertion and the new codec, fold and load
    tests. Run `cargo +nightly fmt --all`,
    `cargo check --workspace --locked`,
    `cargo clippy -p tokeira-kernel -p tokeira-storage --all-targets --locked`
    and `cargo nextest run -p tokeira-kernel -p tokeira-storage --locked`.
    Resolve affected fixtures; do not suppress the original regression.
  - _Requirements: 2.1, 2.4, 2.7, 2.16, 2.19, 3.1_

- [x] 7. Complete the shared repository correction and preservation contracts
  - [x] 7.1 Required property test: Property 1 — no projection reads in commits
    - Extend the exploration test to ready-empty and nonempty states and both
      public commit methods. Observe zero preceding-image lookups in commits
      and ready loads; an otherwise applying unseeded existing input must fail
      before state, history, dispatch, dedupe or projection writes.
    - Include a ready run with deliberately undecodable old projection data to
      catch accidental reads. Keep SQL statement-count assertions distinct from
      director acquisition-class assertions; Task 11 covers the SQL backend.
    - Tag: `// Feature: projection-accumulator, Property 1: no projection reads in commits`
    - _Requirements: 1.1, 1.2, 2.1, 2.4, 2.13_
  - [x] 7.2 Extend Property 2 through successful repository commits
    - Drive equivalent generated commands through the frozen baseline model and
      the changed repository. After every successful commit, compare every image
      field and verify the persisted, loaded and `Applied.new_state` accumulators
      agree. Share the generated contract with the DSQL tests in Task 11.
    - _Requirements: 2.2, 2.3, 3.1, 3.4, 3.5_
  - [x] 7.3 Required property test: Property 7 — failure isolation and existing commit contract
    - Generate fenced, conflicting, duplicate, no-op and invalid transitions.
      Assert the established outcome precedence and unchanged durable maps;
      losing folds cannot overwrite a winner or advance the durable image.
    - Include failed preparation, encoding/growth rejection and transaction
      rollback where supported by the backend. Check deletion retains the
      baseline redacted tombstone regardless of accumulator contents.
    - Tag: `// Feature: projection-accumulator, Property 7: failure isolation and existing commit contract`
    - _Requirements: 2.4, 3.6, 3.7, 3.8_
  - [x] 7.4 Required property test: Property 8 — exact state growth accounting
    - Compare measured size with actual encoded length less the existing activity
      input exclusion. Generate ready-empty and large lists; count all section
      framing. For otherwise eligible existing/open transitions, pin equality
      at the error threshold and rejection one byte over, keeping other limits
      clear. Verify unchanged exemptions and warning/error policies.
    - Exercise both repository entry points and existing runtime breach handling;
      a rejected transition changes no durable state. Add no independent list
      limit, configuration field or trimming.
    - Tag: `// Feature: projection-accumulator, Property 8: exact state growth accounting`
    - _Requirements: 2.21, 2.22, 3.8_

- [x] 8. Verify run boundaries and every runtime commit consumer
  - [x] 8.1 Required property test: Property 3 — new run boundaries
    - Generate parent/predecessor lists and replay prefixes. Verify normal,
      signal-with-start and update-with-start construction begins ready empty;
      continue-as-new, child, retry and cron use their actual production routing
      inputs without copying a predecessor's accumulator.
    - Check both reset materializers persist ready empty without an image.
      Pin prefix v1 then v2 followed by a first ordinary image of `[v2]`, and a
      first ordinary transition changing v2 to v3 producing `[v3]`. Include
      repeated resets and legacy reset materialization with no section/image.
    - Tag: `// Feature: projection-accumulator, Property 3: new run boundaries`
    - _Requirements: 2.5, 2.6, 3.2, 3.3_
  - [x] 8.2 Add focused lane and direct-activity regression tests
    - Exercise cold loads, cache eviction, OCC reload and post-reset bookkeeping.
      Assert the lane caches the returned committed list and discards a losing
      local fold before reloading the winner.
    - Cover heartbeat/retry loads, forced starts for completion, activity starts,
      activity retry commits and dispatch-publication preparation from the
      design's state-source inventory. Start from legacy state where meaningful
      so a test would expose a bypass of seed-capable repository loading.
    - Verify recovery-derived work reloads before committing. Keep visibility,
      dispatch and recovery scans classified by their actual read-only consumers;
      do not add a cache protocol or seed every decoded scan row.
    - _Requirements: 2.3, 2.4, 2.7, 2.13, 2.15, 2.20, 3.6, 3.8_

- [x] 9. Required property test: Property 6 — restart, legacy writers and pruning
  - Build a generated survival contract for eviction before/after seed persistence,
    snapshot/restore, repository recreation and interleaved old/new writers.
    Use the actual frozen pre-accumulator reader/writer path: ignore tag 3, merge
    from the preceding image, and rewrite without tag 3 before a new-reader load.
  - Cover ordinary version changes, inherited starts and reset materializations.
    Verify seed-only loads, no-ops and failed commits leave durable readiness
    unchanged. After a successful new commit, the returned state and restored
    state must carry the same list.
  - Once old writers are excluded and readiness is persisted, remove applied
    images only in the test fixture and compare later images with a retained-log
    control. Add negative controls for each missing prerequisite: an unseeded
    run whose image is removed, and a ready run rewritten by a legacy writer
    after image removal. These expose loss; they do not introduce a deleter.
  - Tag: `// Feature: projection-accumulator, Property 6: restart legacy writers and pruning`
  - _Requirements: 1.3, 2.14, 2.15, 2.20, 4.1, 4.2_

- [x] 10. Checkpoint: default backend and runtime contracts pass
  - Run `cargo +nightly fmt --all`,
    `cargo clippy -p tokeira-kernel -p tokeira-storage -p tokeira-runtime --all-targets --locked`
    and `cargo nextest run -p tokeira-kernel -p tokeira-storage -p tokeira-runtime --locked`.
    All eight properties must have passing default-suite coverage for their
    pure/in-memory portions. Database-dependent portions remain required at the
    next checkpoint, not implicitly passed by this one.
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.15, 2.19, 2.20, 2.21, 3.1, 3.6_

- [x] 11. Implement and run the real SQLx/DSQL integration contracts
  - [x] 11.1 Verify the exact transaction path and controlled legacy-load races
    - Use the existing `dsql-integration` fixture and director/connection
      patterns on a disposable database. Exercise
      `begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")` and bound
      parameters through SQLx; do not substitute a browser or plain `BEGIN` test.
    - Coordinate two connections with channels/barriers. Schedule a writer
      between the fast read and transaction reread, then separately between
      the hot-state and projection reads inside the transaction. Assert returned
      state, statistics and seed share one snapshot, and a stale subsequent
      commit follows the existing OCC result. Include disappearance and another
      writer persisting readiness before the transactional reread.
    - Execute Property 4 with generated sequences and historical partition counts.
      Assert corrupt/newer seeds fail, older seeds remain valid, ready loads
      skip the image even if it is malformed, and seeding never issues an UPDATE.
    - _Requirements: 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 2.15, 3.8_
  - [x] 11.2 Run the shared correction, preservation and survival contracts against DSQL
    - Run the repository portions of Properties 1, 2, 3, 6, 7 and 8 with at least
      100 valid generated cases each, both commit entry points, persisted blobs,
      real projection rows and repository recreation. Reuse the default-suite
      generators and frozen oracle instead of divergent SQL-only expectations.
    - Observe statement-level absence of projection reads where the SQL harness
      exposes statements. If it cannot, report that specific coverage as
      unavailable; never substitute acquisition counts or call a skipped test a
      pass. Keep any harness coordination confined to test code, with no new
      dependency, production switch or shared configuration change.
    - Wait for fixture ASYNC index builds with `CALL sys.wait_for_job($1)` before
      property execution. Retry only explicit catalog conflicts during fixture
      migration and the repository's existing normalized serialization conflict
      during otherwise valid test commits; retain all admission-failure assertions.
    - Scope data setup and cleanup to test-owned runs. Do not change production
      tables, indexes, migrations, connection-pool settings or partition config.
    - _Requirements: 1.1, 1.3, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.13, 2.14, 2.15, 2.20, 2.21, 2.22, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 4.1, 4.2_

- [x] 12. Final checkpoint: integration evidence and the full repository bar
  - Run the focused new integration contracts with
    `cargo nextest run -p tokeira-storage --features dsql-integration --locked`
    and the existing fixture's configured test filter/environment. Record the
    actual command and backend used; a PostgreSQL run does not establish DSQL
    execution, and missing database access leaves that portion unverified.
  - Run all six commands from AGENTS.md §10.4: `cargo +nightly fmt --all`,
    `cargo lint --locked`, `cargo check --workspace --locked`,
    `cargo nextest run --workspace --locked`,
    `cargo test --workspace --doc --locked` and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  - Check spec links and requirement/property/task traceability. Inspect the
    final diff for changed positional layouts, accidental dependencies, unsafe
    retention claims or any deferred semantic correction. Report commands not
    run and why; complete the implementation's changie fragment under the
    repository's existing convention.
  - _Requirements: 2.1, 2.2, 2.3, 2.7, 2.16, 2.19, 2.20, 2.21, 3.1, 3.8, 4.1, 4.2_

## Task Dependency Graph

Each entry lists the tasks that must complete first. A top-level task with
subtasks completes only after all of its subtasks; their listed order is binding.
This graph describes implementation dependencies, not permission to delegate work.

```json
{
  "1": [],
  "2": ["1"],
  "3": ["2"],
  "4": ["3"],
  "5": ["4"],
  "6": ["5"],
  "7": ["6"],
  "8": ["7"],
  "9": ["8"],
  "10": ["9"],
  "11": ["10"],
  "12": ["11"]
}
```

## Notes

- The exploration failure in Task 1 is expected only before the fix. No failing
  exploration test is removed, ignored or committed as a finished slice.
- All properties are required, including the database portions. The default
  suite remains credential-free. Keep SQL execution coverage and statement
  observation coverage separately reported when the environment limits either.
- The [AWS DSQL playground](https://playground.dsql.demo.aws/) checks on 2026-10-07
  accepted the exact read-only BEGIN, reported repeatable-read/read-only settings,
  rejected an UPDATE with SQLSTATE `25006`, and selected the latest synthetic
  image across partition assignments. A controlled reader retained version 10
  while another session committed version 11, then saw 11 after its transaction.
  That concurrency check used plain `BEGIN`: the playground did not retain the
  expanded BEGIN across separate commands. An attempted `SET TRANSACTION` form
  returned `0A000` and was rolled back. None of these browser checks substitutes
  for Task 11's exact SQLx transaction, bound parameters or real state codec.
- Use the existing module and public-item documentation conventions. Explain
  storage ownership, readiness, the consistent legacy-load snapshot, persistence
  before deletion eligibility, and why replay does not fold copied completions.
- There is no rollout, backfill, retention worker, readiness scanner or deletion
  eligibility API in this plan. Later deletion must prove both Requirements 4.1
  and 4.2; these tests do not grant permission to delete production images.
- Requirements, design and this task plan are approved. Completed checkboxes record
  implementation and executed validation, rather than design-only approval.

## Execution evidence (2026-10-07)

- The unfixed Property 1 shrank to `versions = [None]`: one ordinary commit made
  one previous-image lookup when the assertion required zero. The same no-read
  assertion now passes, extended to both commit entry points and ready loads.
- Checkpoint 6 passed the workspace check, focused kernel/storage clippy and 578
  kernel/storage tests. Checkpoint 10 passed focused kernel/storage/runtime clippy
  and all 1,205 tests. All builds used the native `protoc` selected through the
  per-command `PROTOC` environment variable; no shared toolchain configuration changed.
- Seven accumulator contracts passed on a fresh Aurora DSQL cluster in `eu-west-1`:
  100 generated cases for each applicable repository property, both commit entry
  points, actual SQLx statement observation, persisted blobs and full projection
  comparisons, plus six deterministic reader/writer schedules. The command was:

  ```bash
  cargo nextest run --config-file /tmp/projection-accumulator-nextest.toml \
    --profile dsql-live --locked --test-threads 1 -p tokeira-storage \
    --features dsql-integration --lib --run-ignored only \
    -E 'test(dsql_projection_accumulator)'
  ```

  The temporary nextest profile allowed 30 minutes per live test; it did not
  change repository or shared configuration. The fixture's later index-completion
  barrier was verified by rerunning the 100-case legacy-seeding contract on another
  fresh cluster. Production commit and load behavior was unchanged by that harness fix.
- The existing live suites also passed: shard leasing (4), embedded ownership (1),
  projection persistence (6), CHASM (3), and IAM connector (1). The standalone
  projection command explicitly enabled both `dsql-integration` and
  `tokeira-storage/dsql-integration` to expose its existing test constructor.
  URL and IAM environment gates were populated for the actual DSQL runs; no
  PostgreSQL substitute was used. Cluster lifecycle times are reported in the PR,
  with endpoints, tokens and cluster identities excluded.
- All six AGENTS.md §10.4 commands passed. The complete workspace nextest run
  used `--test-threads 4`: 3,655 passed and two existing tests remained ignored.
  An initial default-concurrency timeout in the unchanged backlog-routing property
  and its successful isolated rerun are reported in the PR. Doctests and
  `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` passed.
  The offline spec-link check and final whitespace check passed; dependencies,
  lockfile, migrations and shared configuration were unchanged.
