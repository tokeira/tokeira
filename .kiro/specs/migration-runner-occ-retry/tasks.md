# Implementation Plan

- [x] 1. Show the defect before changing the runner
  - [x] 1.1 Add the executor double and a unit test for Property 1 against `apply_connection`: one `40001`
    on commit, then success. Run it and see it fail on today's code.
    - Done as a negative control once the step was shared (2.1). `apply_connection` takes a `PgConnection`,
      so the double, `ScriptedSession`, implements the private `MigrationSession` trait under the shared
      loop. With the loop cut to a single attempt, which is all the plain paths made,
      `conflicts_below_the_limit_are_retried` fails.
    - _Requirements: 1.1, 2.1_

- [x] 2. Share the guarded path's step
  - [x] 2.1 Extract the retry loop and the index-build wait from `execute_migration_step` into shared
    functions. Keep `execute_migration_step` as the idempotency check, then the shared step, then the wait.
    - Done: the loop is `attempt_migration_with_retries`, over `MigrationSession::attempt`. The wait was
      already `wait_for_async_index`, which both kinds of path now call.
    - _Requirements: 2.1, 2.2, 3.1_
  - [x] 2.2 Rewire `apply_connection` and `apply` (one connection per migration) through the shared
    functions, keeping their error context, their metrics and the order in which they record migrations.
    - Done through `apply_plain_migration`. `apply` returns its connection to the pool before recording the
      migration, so it still holds one connection at a time. A failed wait stops the run with "failed
      waiting for the index build of migration V…" and records nothing, as on the guarded path.
    - _Requirements: 2.1, 2.2, 2.3, 3.2, 3.3, 3.4_

- [x] 3. Prove it
  - [x] 3.1 Property 1, a conflict is retried and the migration is recorded once: unit tests over the
    double, with failures on the statement and on the commit, below and at the limit.
    - Done: `conflicts_below_the_limit_are_retried` and `conflicts_past_the_limit_stop_with_the_last`,
      100 cases each.
    - _Requirements: 2.1, 2.3_
  - [x] 3.2 Property 2, other failures stop the run: unit test with a non-`40001` failure.
    - Done: `a_failure_that_is_not_a_conflict_is_not_retried`, on the statement and on the commit, and
      `a_failure_to_begin_is_not_retried`.
    - _Requirements: 3.3_
  - [x] 3.3 Check that the guarded path still refuses a non-idempotent migration, and that its tests still
    pass.
    - Done: `execute_migration_step` still checks idempotency first, and its tests pass. The retry tests
      run a migration that check refuses (`the_retried_migration_is_one_the_guarded_step_refuses`).
    - _Requirements: 3.1_
  - [x] 3.4 Live run on an ephemeral DSQL cluster (profile `default`, `eu-west-1`, no deletion
    protection, tagged `purpose=tokeira-test`, deleted when the run ends):
    - `apply_connection` and `apply` each migrate the full corpus on a new cluster;
    - a second run applies nothing.

    Record when the cluster was created and deleted, without identifiers.
    - Done with `dsql_migration_runner` on 7 October 2026, one new cluster per path:
      - `apply_connection` first: created 21:02:38Z, passed in 371.69 s, deleted 21:10:27Z;
      - `apply` first: created 21:20:07Z, passed in 397.82 s, deleted 21:28:27Z.
    - _Requirements: 2.1, 2.2, 3.4_

- [x] 4. Checkpoint: run the full `AGENTS.md` §10.4 bar, then the live run in 3.4.
