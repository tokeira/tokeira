# Implementation Plan

- [ ] 1. Show the defect before changing the runner
  - [ ] 1.1 Add the executor double and a unit test for Property 1 against `apply_connection`: one `40001`
    on commit, then success. Run it and see it fail on today's code.
    - _Requirements: 1.1, 2.1_

- [ ] 2. Share the guarded path's step
  - [ ] 2.1 Extract the retry loop and the index-build wait from `execute_migration_step` into shared
    functions. Keep `execute_migration_step` as the idempotency check, then the shared step, then the wait.
    - _Requirements: 2.1, 2.2, 3.1_
  - [ ] 2.2 Rewire `apply_connection` and `apply` (one connection per migration) through the shared
    functions, keeping their error context, their metrics and the order in which they record migrations.
    - _Requirements: 2.1, 2.2, 2.3, 3.2, 3.3, 3.4_

- [ ] 3. Prove it
  - [ ] 3.1 Property 1, a conflict is retried and the migration is recorded once: unit tests over the
    double, with failures on the statement and on the commit, below and at the limit.
    - _Requirements: 2.1, 2.3_
  - [ ] 3.2 Property 2, other failures stop the run: unit test with a non-`40001` failure.
    - _Requirements: 3.3_
  - [ ] 3.3 Check that the guarded path still refuses a non-idempotent migration, and that its tests still
    pass.
    - _Requirements: 3.1_
  - [ ] 3.4 Live run on an ephemeral DSQL cluster (profile `default`, `eu-west-1`, no deletion
    protection, tagged `purpose=tokeira-test`, deleted when the run ends):
    - `apply_connection` and `apply` each migrate the full corpus on a new cluster;
    - a second run applies nothing.

    Record when the cluster was created and deleted, without identifiers.
    - _Requirements: 2.1, 2.2, 3.4_

- [ ] 4. Checkpoint: run the full `AGENTS.md` §10.4 bar, then the live run in 3.4.
