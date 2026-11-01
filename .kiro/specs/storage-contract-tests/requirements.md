# Requirements: Storage Contract Tests

## Introduction

Make the shared semantic contracts of `RunRepository` and `ProjectionLog`
executable against both authoritative storage implementations. Memory-only
success cannot establish that a DSQL query pages completely, resumes correctly,
or commits atomically. Conversely, the in-memory store is intentionally free of
DSQL's physical limits; teaching it to reject DSQL-sized writes would contradict
the existing contract.

This is a verification feature, not a proposal to redesign persistence. The
authority is the current storage interface and its owning specifications, checked
against the implementations at `757ea24589ca8208d1a58f0801e03dd3a127816b`.
Public Temporal behavior remains targeted at v1.31.0. These internal storage
interfaces have no corresponding Temporal in-memory backend; this specification
introduces no new public field semantics or status mapping. Any such decision
discovered during implementation must be checked against the pinned Temporal
source before changing its owning specification.

Primary scope is `tokeira-storage`, with a focused controller-managed acquisition
regression in `tokeira-runtime` and the live-suite documentation. No dependency,
schema, production configuration, CI schedule or deployment change is proposed.

## Glossary

- **Shared case:** one scenario and its assertions executed through adapters for
  both stores. An adapter supplies setup, access and cleanup, not a different oracle.
- **Oracle:** expected semantic results derived from the scenario's inputs and
  specified rules, independently of the production query or derivation being tested.
- **Trace:** generated inputs plus a sequence of commits, reads, resumes or
  competing writes whose observations can be replayed.
- **Observation:** a returned state, event, attribution, image, cursor, typed
  outcome or durable side effect visible through the tested contract.
- **Live evidence:** an executed test against an ephemeral Aurora DSQL database,
  with its exact source, binary, test selection and result recorded.
- **Physical case:** a DSQL-only boundary or SQL/concurrency behavior that the
  in-memory store does not emulate.
- **Legacy case:** a deliberately seeded older storage representation covered by
  an existing migration or compatibility specification.

## Target State

One documented coverage matrix connects each contract below to its shared case,
memory entry point, DSQL entry point and actual execution evidence. Existing
shared suites are reused. Missing ordering, resumption and commit-conflict cases
gain independent oracles and generated traces. Live DSQL checks cover physical
boundaries and controller-managed acquisition through the real repository.

The suite compares semantics, not physical tables or arbitrary backend error
strings. It preserves the documented differences in legacy seeding, diagnostic
timestamps, DSQL encodings and transaction limits. It does not claim that every
storage trait, visibility query, CHASM component or Temporal functional test is
covered by this feature. The matrix identifies existing sibling coverage instead
of silently treating those areas as newly verified.

Out of scope: a persistence-limits facade, activity reconciliation, queue-home
transport, projection-worker redesign, query optimization, new runtime behavior,
and moving functional conformance from memory to DSQL.

## Evidence From Current Code

- [`api.rs`](../../../crates/tokeira-storage/src/api.rs), `RunRepository` and
  `ProjectionLog`, defines semantic writes, ordered history, recovery and
  projection reads. `read_history_to_end` and its attributed counterpart use
  bounded pages; a short history page means the end.
- [`history-pagination`](../history-pagination/bugfix.md), criteria 2.1–2.3,
  owns exact requested history pages and bounded whole-history reads.
  [`load.rs`](../../../crates/tokeira-storage/src/dsql/run_repository/load.rs)
  implements DSQL batch reads and split-value hydration;
  [`memory.rs`](../../../crates/tokeira-storage/src/memory.rs) implements the
  matching event reads.
- [`recovery_index.rs`](../../../crates/tokeira-storage/src/recovery_index.rs),
  `read_candidate_page`, advances through legacy then flagged rows and normalizes
  a zero limit to one. [`recovery-index`](../recovery-index/requirements.md),
  Requirements 2, 3 and 6, distinguishes DSQL legacy rows from current-format
  memory rows. A short legacy page is not necessarily the end of the listing.
- [`projection_log.rs`](../../../crates/tokeira-storage/src/dsql/projection_log.rs)
  reads by `(run_key, transition_seq)`, not insertion time. `ProjectionLog` and
  [`projection-passes`](../projection-passes/bugfix.md), criteria 2.1–2.4, require
  a new pass from the head and exact image deletion. Their legacy-seed rules
  deliberately differ between stores. Memory returns no oldest-written timestamp.
- [`commit.rs`](../../../crates/tokeira-storage/src/dsql/run_repository/commit.rs)
  checks the execution-home fence and expected sequence before applying its write
  set. `CommitResult`, `WriteFence` and
  [`bundle-lease-fence`](../bundle-lease-fence/bugfix.md) define outcome and
  fencing contracts. The successful returned state includes the projection fold.
- Shared helpers already exist in
  [`history_event_tests.rs`](../../../crates/tokeira-storage/src/history_event_tests.rs),
  [`lease_fence_tests.rs`](../../../crates/tokeira-storage/src/lease_fence_tests.rs),
  [`bulk_write_tests.rs`](../../../crates/tokeira-storage/src/bulk_write_tests.rs),
  [`large_value_tests.rs`](../../../crates/tokeira-storage/src/large_value_tests.rs)
  and [`workflow_dispatch_tests.rs`](../../../crates/tokeira-storage/src/workflow_dispatch_tests.rs).
  Their coverage is real but distributed across feature-specific entry points;
  it is not evidence that an unexecuted live selection passed.
- [`bounded-bulk-writes`](../bounded-bulk-writes/bugfix.md), criterion 2.12, and
  [`large-value-parts`](../large-value-parts/bugfix.md), criterion 3.5, explicitly
  keep DSQL's limits out of memory. Existing live tests cover DSQL transaction
  limits, split values and bounded maintenance.
- [`runtime/dsql_tests.rs`](../../../crates/tokeira-runtime/src/runtime/dsql_tests.rs)
  exercises controller-managed acquisition with a superseded recovery generation.
  [`DSQL live suites`](../../../docs/testing/dsql-live-suites.md) prescribes
  preparation-before-leases ordering. Several existing live entry points return
  successfully when their URL is unset; a default nextest pass is therefore not
  proof of live execution.

## Contract Policy

The table specifies test treatment of the existing surface, not new validation
rules. Other `RunRepository` methods are inventoried as sibling coverage; they are
not implicitly given new semantics here.

| Surface or value | Test policy | Invalid/refused outcome | Observable impact |
|---|---|---|---|
| `read_history`, `read_attributed_history`: `run_key` | Use known, absent and isolated run identities | Preserve existing absent-run read behavior | No other run's events |
| Same methods: `after_event_id`, `limit` | Exclusive event cursor; zero, one, exact boundary and larger finite pages | DSQL-only representability refusals remain physical cases | No read mutates history; no silent smaller page |
| Whole-history helpers and `read_history_event` | Read the expected suffix or exact event, including batch edges | Preserve absent event as `None` | Equality to fixture-authored history |
| History events and attribution | Compare complete event values and optional principals | Corrupt stored representations retain existing errors | No lost or shifted attribution |
| Recovery: `shard_id`, `cursor`, `limit` | Home isolation, opaque phase/position cursor, effective limit `max(1, limit)` | Invalid storage data remains an error | Ordered candidates within each phase; `next` controls completion |
| Recovery output: `states`, `next` | Compare all state values and traversal completeness for an unchanged set | No false terminal page | Every eligible current-format run appears once |
| Projection read: `partition_id`, `fanout`, `after`, `limit` | Exact scope; exclusive image key; zero reads empty | DSQL conversion restrictions remain physical cases | Sorted, bounded images; no mutation |
| Projection image key/context | Compare full semantic values including authority and transition sequence | Preserve existing decode/seed failures | Images reflect committed post-transition state |
| `ProjectionPage.oldest_written_at` | Check DSQL against its seeded timestamps; allow memory's documented `None` | No cross-backend timestamp equality assumption | Diagnostic only |
| `delete_images`: scope and keys | Delete only named keys in the named partition/fanout | Preserve legacy retention policy | Repeat deletion is idempotent; count equals actual removals |
| Commit: run key, transition, optional explicit execution home | Use well-formed fixture transitions and explicit incumbent identities | Preserve existing mismatch validation and typed outcomes | State, history, audit, dedupe, derived rows and projection form one write set |
| `WriteFence::Unfenced`, `WriteFence::Lease` | Separate unfenced contract fixtures from controller-owned fixtures | Lost lease yields `LeaseFenceLost`; wrong-home validation is tested separately | Refused writes expose no part of their write set |
| `CommitResult::Applied` | Compare returned state with reloaded state and expected writes | Not applicable | Exactly the accepted transition becomes visible |
| `CommitResult::Conflict` | Assert category and invariant, not backend prose | Sequence/concurrent-write conflict | No partial writes |
| `CommitResult::CurrentExecutionConflict` | Compare incumbent run, status and request-id contents | Existing open execution wins | Losing start creates no orphan state/history/image |
| `CommitResult::Duplicate` | Exercise an applicable same-run dedupe precondition | Existing request wins | No additional writes; stale-sequence precedence remains as implemented |
| DSQL physical representation | Exercise current SQL and codecs on live DSQL | Assert the existing recognized limit/error category | Refused transactions remain unapplied; supported large values hydrate whole |
| Legacy representations | Seed only through dedicated fixture hooks | Preserve owning-spec rejection rules | Verify legacy behavior independently, not by inventing memory limits |

## Requirements

### Requirement 1: Explicit coverage and one semantic oracle

**User Story:** As a storage maintainer, I want both stores to execute the same
contract cases, so backend drift cannot hide behind different assertions.

1. THE suite SHALL maintain a matrix covering history, recovery, projection,
   commits, existing bounded-write contracts, physical DSQL checks and runtime
   acquisition, identifying the exact memory and DSQL test entry points.
2. WHEN a contract has an existing shared case, THE suite SHALL reuse that case
   or document the concrete missing invariant before adding another.
3. WHEN both backends execute a shared case, THE suite SHALL use the same
   scenario and semantic oracle for both.
4. THE oracle SHALL derive expected ordering, membership and write effects from
   fixture inputs rather than the production selector or predicate under test.
5. WHERE an observation deliberately differs by backend, THE matrix SHALL name
   the difference and cite its owning contract.

### Requirement 2: Complete history and exact resumption

**User Story:** As a runtime maintainer, I want every history page to obey one
contract, so a successful read cannot silently truncate a workflow.

1. WHEN a shared trace writes a history in varying batch sizes, THE suite SHALL
   verify ordered exclusive-cursor reads for every selected page boundary.
2. WHEN more events remain than a requested finite page can hold, THE suite SHALL
   verify that the page contains exactly the requested number of events.
3. WHEN a whole-history helper reads a suffix, THE suite SHALL compare it with
   the complete expected suffix, including deterministic histories above 1,000
   events and exact multiples of `HISTORY_READ_PAGE`.
4. WHEN the history contains event attribution, THE suite SHALL verify that the
   attributed and unattributed readers return the same events with the correct
   optional principal attached to each attributed event.
5. WHEN the selected event is at a batch boundary, absent, or in a supported
   split-value batch, THE suite SHALL verify the exact-event lookup against the
   fixture-authored event sequence.
6. WHEN a read has a zero limit or starts at or after the last event, THE suite
   SHALL verify an empty result without mutation.

### Requirement 3: Recovery pages that finish without losing candidates

**User Story:** As a runtime maintainer, I want recovery to traverse all eligible
runs of its home, so acquisition does not activate with missing work.

1. WHEN a fixed set of current-format runs spans multiple homes and pages, THE
   shared suite SHALL verify complete, duplicate-free traversal of the selected
   home's eligible runs in run-key order.
2. WHEN a recovery page returns `next`, THE suite SHALL resume using that cursor
   even when the page is shorter than its effective limit.
3. WHEN a recovery read requests zero entries, THE suite SHALL verify the
   existing effective limit of one and eventual completion.
4. WHERE DSQL holds legacy and flagged rows, THE live suite SHALL verify legacy
   traversal before flagged traversal, including empty and exact-full phases.
5. WHEN a legacy row becomes flagged during its legacy-phase traversal, THE live
   suite SHALL verify that the subsequent flagged phase can recover it; this
   changing-set case SHALL NOT assert global exactly-once visitation.
6. WHEN a traversal resumes after deleting its cursor row, THE suite SHALL verify
   that later eligible keys remain reachable.

### Requirement 4: Projection ordering and exact retirement

**User Story:** As a projection maintainer, I want a shared log contract, so
insertion order cannot conceal a missing image on DSQL.

1. WHEN a shared trace commits runs in an order different from their keys, THE
   suite SHALL verify pages ordered by `(run_key, transition_seq)` within the
   selected partition and fanout.
2. WHEN an image is inserted below an active page cursor, THE suite SHALL verify
   that a subsequent pass from the head reads that image.
3. WHEN selected image keys are deleted, THE suite SHALL verify that only those
   removable keys disappear and that repeating the deletion removes zero more.
4. WHEN another image is committed after a page was read, THE suite SHALL verify
   that deleting the observed page cannot delete that unobserved image.
5. WHERE a legacy seed requires retention or applied-value fallback, THE suite
   SHALL execute the corresponding owning-spec case for each backend.
6. WHEN a projection read has limit zero, THE suite SHALL verify an empty page
   without changing the log.

### Requirement 5: Commit outcomes and atomic observations

**User Story:** As a runtime maintainer, I want failed and competing commits to
leave coherent storage, so retry logic can trust the repository outcome.

1. WHEN a valid transition applies, THE shared suite SHALL verify its returned
   and reloaded states against the expected post-transition state.
2. WHEN a transition conflicts, duplicates an existing request, or loses its
   lease, THE shared suite SHALL verify that none of its attempted history,
   audit, dedupe, pointer, timer, dispatch, outbound or projection changes became
   visible.
3. WHEN two distinct transitions compete at the same expected sequence, THE
   suite SHALL verify that exactly one applies and that stored observations
   contain only the winner's write set.
4. WHEN two valid starts compete for one open-current pointer, THE suite SHALL
   verify that at most one owns that pointer and that the losing start leaves
   no orphan authoritative or derived rows.
5. WHEN commit preconditions select `CurrentExecutionConflict` or `Duplicate`,
   THE suite SHALL verify the corresponding semantic result without requiring
   identical backend diagnostic text.
6. WHEN ownership changes between a caller's observation and commit, THE suite
   SHALL verify the original lease fence's rejection using deterministic
   synchronization rather than sleeps.
7. WHEN exercising both commit entry points, THE suite SHALL verify that
   `commit_transition_for_bundle` preserves the same atomicity and fencing
   guarantees as `commit_transition`.

### Requirement 6: Physical boundaries stay on DSQL

**User Story:** As a persistence maintainer, I want database constraints tested on
the database, so memory remains useful without making false DSQL claims.

1. THE implementation SHALL preserve the in-memory store's absence of DSQL row,
   transaction-byte, value-size and SQL binding limits.
2. WHEN a repository method binds the caller's page limit directly to SQL and
   that limit is outside its supported binding range, THE live suite SHALL
   verify a diagnostic error rather than silent truncation; history readers
   that bind a fixed internal batch size are not subject to this assertion.
3. WHEN supported data crosses the existing split-value or bounded-write
   threshold, THE delivery SHALL execute the owning live cases and verify full
   readback, interruption/resumption and atomic visible publication.
4. WHEN a write is refused by a recognized DSQL transaction limit, THE live suite
   SHALL verify the existing refusal classification and absence of partial writes.
5. THE coverage matrix SHALL distinguish shared semantic evidence from physical
   DSQL evidence, including existing reset, deletion, backlog and successor cases.

### Requirement 7: Real controller-managed acquisition

**User Story:** As an operator, I want the real acquisition path exercised on
DSQL, so a successful memory startup does not conceal a broken first sweep.

1. WHEN the live runtime acquires an execution home in controller-managed mode,
   THE check SHALL use the real DSQL repository and lease path after required
   storage preparation has completed.
2. WHEN that home holds more recoverable runs than one recovery page, THE check
   SHALL verify reconstruction of the seeded work before the acquisition is
   reported active.
3. WHEN an acquisition is superseded during recovery, THE check SHALL retain the
   existing assertion that its old generation cannot install tracker state.
4. WHEN acquisition completes or fails, THE fixture SHALL stop its runtime and
   release only the leases and data it owns.

### Requirement 8: Reproducible properties and honest execution evidence

**User Story:** As a reviewer, I want replayable failures and exact test results,
so a green report means the required backend checks actually ran.

1. THE design SHALL derive required generated properties from the variable
   ordering, pagination and commit invariants above.
2. WHEN a shared generated property is verified, THE delivery SHALL execute at
   least 100 cases on each backend and record the seed or replayable trace.
3. WHEN an oracle claims to detect truncation, wrong ordering, skipped resumption
   or a partial write, THE tests SHALL demonstrate rejection of a deliberately
   corrupted observation without modifying production behavior.
4. WHEN a new live test is explicitly selected without its required connection
   configuration, THE entry point SHALL fail with a clear configuration error;
   it SHALL NOT report an unexecuted case as passed.
5. THE ordinary offline suite SHALL remain credential-free through explicit
   feature/ignore selection of live entry points.
6. WHEN live verification runs, THE runner SHALL use an immutable nextest archive
   from the recorded source and the existing serial `dsql-live` profile.
7. THE live run record SHALL list selected tests, executed counts, passed/failed/
   skipped outcomes, generated case counts and applicable feature flags.
8. THE live fixtures SHALL isolate their keys, homes and partitions and preserve
   the documented preparation-before-leases order.
9. WHEN the live run ends, THE delivery SHALL record ephemeral-cluster cleanup
   and keep resource identifiers and credentials out of committed artifacts.
10. THE implementation SHALL add no automatic CI or nightly live DSQL runs.

## Review Status

Requirements, design and tasks approved; implementation and verification are
complete. The full workspace bar, all 66 selected live executions and all 17
generated receipts pass. Ephemeral-resource cleanup is verified. The
[runbook](../../../docs/testing/storage-contract-tests.md) records archive
provenance and outcomes. Existing sibling specifications remain unchanged.

## Requirement 9: Cancellation-safe DSQL transaction startup

A live runtime acquisition check exposed a production connection-reuse defect
while cleaning its fixture. This narrowly scoped repair belongs to the existing
reservoir bad-flag contract; the memory backend and Temporal API remain unchanged.

1. WHEN a reservoir-backed transaction-start future is first polled, THE
   DSQL permit SHALL mark its physical connection unsafe for reuse until SQLx
   successfully returns the transaction handle.
2. IF transaction startup is cancelled or fails before returning that handle,
   THEN THE permit SHALL retain the bad flag and retire the connection through
   the existing return path, releasing its slot exactly once.
3. WHEN transaction startup succeeds, THE permit SHALL restore its previous
   bad-flag state without clearing an earlier explicit bad mark.
4. THE repair SHALL cover read-only and read-write transaction starts on
   reservoir-owned connections, including helpers that begin their own snapshots.
5. THE healthy return path SHALL preserve the existing no-network rule, pool
   limits, admission, ownership fencing, connection creation rate and slot accounting.
6. THE tests SHALL demonstrate cancellation during real SQLx startup and prove
   that the following borrower can execute a write transaction without inheriting
   the cancelled transaction. Generated cancellation-state preservation coverage
   SHALL execute at least 100 cases without sleeps.
7. THE implementation SHALL preserve original errors, successful transaction
   semantics, existing cleanup assertions and all storage-contract case minimums;
   it SHALL NOT add dependencies, schema changes or connection configuration.
