# Queue-home delivery implementation evidence

## Series

The foundations add executed defect exploration, run-load attribution, the typed offer origin and bounded sticky
storage pages. The following PR defines peer credentials and the wire contract without activating offer admission.
Delivery then activates the handler with shared admission, retirement, push and sticky recovery. Start-read
consolidation and the remaining proof matrix follow. Unfinished tasks stay open.

The foundations' `WorkflowOfferHint` is a typed envelope constructed from durable intent. Its execution home,
schedule time, priority, exact/live routing and original sticky coordinates survive destination changes. Existing
speculative, eager and query envelopes stay compatible. Wiring the new normal envelope into publication,
discovery and remote start belongs to the delivery and read-consolidation changes; this PR activates no push path.

## Part 2: credentials and wire contract

Tasks 3.1–3.3 and 3.7 implement the runtime credential and wire contract. Provisioning belongs to deployment tooling
outside this repository; tasks 3.4–3.6 are removed. No admission, sender, retirement or sticky fallback is activated.
The handler authenticates and validates then returns `Unavailable`, without calling the legacy publisher. Start's
128 MiB response limit is preserved.

The existing no-controller decision in `build_and_serve` (`crates/tokeira-engine/src/lib.rs`) gates peer bootstrap
and the unchanged single-node claim. Embedded construction passes no peer capability, including durable modes and
an attached public listener. No-peer startup builds no file loader, signer, renewal work or offer sender/endpoint.
Controller-managed startup requires valid material even with one replica.

`infrastructure.peer.path` names an absolute protected file. `tokeirad peer-credential generate --out <path>` creates
it once with mode 0600, refuses overwrite and prints only path configuration. The versioned JSON contains fresh
cluster identity, ES256 PKCS#8 DER encoded as base64url and its public profile; there is no provider reference,
sidecar or deployment adoption state. File opens reject symlinks component by component; loading bounds size and
rejects group/world permissions. Key/profile agreement and signing are checked before readiness. Missing and
inconsistent failures are distinct and redacted, with the generator/install/stopped-node restart remedy.
Restarts do not rewrite the file; explicit generation at a new path and installation on every stopped node rotates
trust. Public authentication is unchanged. See [operator guidance](../../../docs/operations/peer-credentials.md).

Dependency changes are edges to packages already in the lockfile: auth uses `p256`/`uuid`, config uses `rustix`, and
engine tests use `tempfile`. The existing `jsonwebtoken` ring backend and P-256/ECDSA family are reused. The lockfile
is refreshed offline after removing provider-related edges. No new crates, migrations or upstream proto edits.

### Regression inventory

| Property | Generated test | Configured cases |
| --- | --- | --- |
| 7 | auth `generated_peer_policy` | 100 |
| 7 | auth `generated_material_agreement` | 100 |
| 10 | auth `generated_renewal_and_rotation` | 100 |
| 7 | config `generated_bounded_file_loading` | 100 |
| 7 | edge `generated_wire_shapes` | 100 |
| 7 | edge `generated_bounds_and_handler_saturation` | 100 |
| 10 | engine `generated_peer_file_restart_rotation` | 100 |
| 10 | engine `generated_upgrade_bootstrap` | 100 |
| 10 | engine `generated_no_peer_startup` | 100 |
| 10 | engine `generated_durable_peer_exemption` | 100 |

Examples cover validity/leeway and byte boundaries, corrupt/mismatched keys, token redaction, create-only generation,
mode 0600, overwrite refusal, symlink rejection, missing/inconsistent startup messages, frame bounds, absent routes
and Start's larger response. Durable bootstrap tests use the existing provider/connection fakes and common embedded
router; they preserve ownership safeguards and do not constitute live DSQL readiness evidence.

### Validation

Locally, nightly formatting and warning-denied workspace/all-targets clippy pass, and `cargo check` refreshed the
lockfile without package or version changes. All six generated auth/config/edge properties above pass at 100 cases
each. `generated_upgrade_bootstrap` and `generated_peer_file_restart_rotation` each pass 100 cases, as do the
create-only CLI generation, mode and overwrite refusal, symlink and public-mode rejection, and both startup
diagnostics.

The response-frame example exposed a Tonic trailers-only decoding-limit bypass. The dormant offer client now rejects
a malformed initial success status and bounds the unary response body independently. Exact/one-over response-frame
and unchanged Start-limit regressions pass. No sender or handler is activated.

The full bar passes on the final head, on a fresh Linux devbox: fmt, lint, check, test (3,978 passed, 2 skipped),
doctest and doc, with `generated_no_peer_startup` and `generated_durable_peer_exemption` among the tests run.
`cargo clippy --features dsql-integration` is clean for `tokeira-engine` and `tokeira-edge`. The binding inventory
fixture gains only this part's internal messages, enums and the `OfferWorkflowTask` method. No live provider or
network validation is claimed.

The changed fragment is: “Controller-managed clusters need a peer credential file on every node: generate one with
tokeirad, stop every node, and set it in each node's configuration before restarting.” The changie dry run, offline
Markdown links, `git diff --check` and a scan of the diff for key material pass.

## Part 3: admission and retirement

### Activated behavior

Normal local publication and discovery now converge on the broker's bounded admission path. A directly invoked
pushed source exercises that same path, but the authenticated offer handler still returns `Unavailable`. There is
no outbound sender, remote push, new sticky sweep, or start-read consolidation in this change. The remaining
sticky routing/fallback conditions and distributed delivery wiring belong to the subsequent push change.
Discovery resolves each row before admission, and a row retained in this acquisition answers `Known` after
resolution, because another home's record cannot suppress it and same-home retargeting needs the current routing.

The receiver captures its own queue-home acquisition and rechecks it under the broker lock before either
`Known` or admission. Cancellation and the mutable admission deadline are checked there and at dequeue; a stale
page reservation cannot authorize the next acquisition. Retention keys include home, generation and incarnation.
Revisions distinguish same-tick replacement and same-home retargeting, and completion releases only its revision.
A second index maps `(OfferKey, revision)` to the retained scope, keeping take and completion constant-time.
Per-key scope counts keep speculative publication's `contains` check constant-time through retargeting and removal.
A current local routing/availability decision permits retargeting; an unvalidated pushed hint cannot retarget by
arrival order. A conflicting hint without retarget authority is `Known`, with no saturation outcome. Authorized
retargeting replaces the selected hint atomically and cancels the old attempt if its start has not been handed over.

Ready retention and in-flight leases are capped by the absolute sticky deadline. Neither a take nor `Known`
extends the existing retention limit. The local broker preserves that deadline on its existing normal fallback,
consistent with `service/history/transfer_queue_active_task_executor.go @ v1.31.0`, `processWorkflowTask`.
This includes the local half of requirement 2.7: a repeated fallback retains the same deadline and expires at it.
The remaining sticky fallback conditions and remote push work remain open.
Admission adds a 64 MiB node-wide serialized-envelope budget to the existing 256-per-queue and 8,192-per-home
limits. Counting uses a bounded writer rather than allocating a serialized copy. Page guards release unused
reservations on capacity or ownership refusal and on cancellation. Known and expired rows preserve the remaining
slots for later candidates in the same slice; retained and in-flight entries both consume capacity.

The runtime starts a separate retirement task owned by its shutdown tracker. It watches acquisition cancellation
(including lease loss) and the shared admission deadline, re-reading deadlines after renewal. It does not wait for
a discovery tick. Only first registration of an acquisition wakes retirement; repeated reservations, admissions
and polls do not. Expiry compares retention times only: acquisition watchers remove lost generations, and the
dequeue guard checks the captured acquisition while cleanup is pending, including after replacement.
A take skips retired or newly expired entries and continues selecting eligible work under the same broker lock.
Cleanup removes only the matching generation's records, counters and discovery registrations, and cancels starts
before handoff. The claimant carries its cancellation token from dequeue to that check. After invoking the start
path, the start completes under the poll's context and its result reaches the poller even if retirement precedes
the commit or reply. This matches `service/matching/matching_engine.go @ v1.31.0`, `PollWorkflowTaskQueue` and
`recordWorkflowTaskStarted`, which pass the poll's request context to the history client.
Sticky polls cannot consume normal offers through an alias
when their own queue home is not locally serving. Activity accounting and delivery, speculative/eager/query paths,
discovery period and per-pass admission limit, durable storage, migrations and protobuf contracts are unchanged.

Admission counters use finite source/outcome labels, saturation uses count/bytes, and retirement uses
cancelled/deadline/shutdown. No run, queue identity or credential is a label on these new counters.

### Generated regression inventory

Every property below is configured for 100 generated cases. Broker tests exercise the production admission and
retirement implementations with real placement acquisitions. Accounting tests compare bounded retention against
an independent finite-trace model; they do not claim distributed start or live DSQL evidence.

| Property | Test | Cases |
| --- | --- | --- |
| 2 | `property_2_colocated_distinct_homes` | 100 |
| 2 | `property_2_publication_discovery_races` | 100 |
| 2 | `property_2_same_home_retarget_and_late_result` | 100 |
| 2 | `property_2_full_releases_reservations` | 100 |
| 2 | `property_2_expiry_and_delayed_results` | 100 |
| 2 | `property_2_count_and_byte_exhaustion` | 100 |
| 2 | `property_2_retention_traces_match_model` | 100 |
| 5 | `property_5_cancelled_generation_cannot_dequeue` | 100 |
| 5 | `property_5_admission_rechecks_acquisition` | 100 |
| 5 | `property_5_cleanup_without_discovery` | 100 |
| 5 | `property_5_sticky_poll_requires_its_home` | 100 |
| 5 | `property_5_renewed_deadline` | 100 |
| 5 | `property_5_stale_page_reservation` | 100 |

On the unchanged implementation at `af53084e44719c4d23c2c336faad8f379d514820`, the new cancelled-generation dequeue
property failed with one publication, and the colocated-home property failed with one incarnation on two homes.
Both shrank to queue index zero and failed their named invariants. These are executed pre-fix failures, not inferred
counterexamples. The existing retirement exploration is now a passing stored-intent regression; the split-home
publication explorations remain expected failures until push is activated.

The cleanup property never starts a discovery scheduler: it awaits retirement's capacity notification and verifies
removal directly, without invoking dequeue/expiry as cleanup. It checks retained attempts, page reservations and
demand registrations, then invokes old-generation cleanup and a late result against the replacement. Retargeting
covers both ready and in-flight entries, acknowledges stale peer hints without retargeting, and proves a late
result cannot release the new normal-queue revision.

The review regressions first failed on the preceding implementation: `retirement_wakes_only_for_new_acquisitions`
observed a redundant reservation wake; `expiry_does_not_query_placement_per_offer` observed 32 provider calls;
`property_2_same_home_retarget_and_late_result` observed `Full` instead of `Known`; and
`retirement_during_owner_start_delivers_committed_result` observed an empty poll after a real durable start.
The latter holds the owner response and tests retirement both before and after the commit.
`local_fallback_preserves_absolute_sticky_deadline` already passed before review fixes: fallback keeps the deadline,
a repeated offer leaves its revision and deadline unchanged, and the entry expires at that deadline.
The finite-trace accounting property also checks that every retained revision has exactly one index entry.

The subsequent discovery and dequeue regressions also failed before their fixes:
`expired_row_preserves_reservation_for_following_rows` returned `Capacity` at an expired normal row instead of
admitting the three rows behind it in the same slice; `poll_skips_retired_entries_before_current_generation`
returned an empty poll with two retired entries ahead of current work. The latter covers both general and sticky
ready queues. `contains_survives_scope_removal_and_retargeting` passed before the lookup optimization and preserves
lookup behavior across homes, generations, duplicate admission, retargeting, ready removal, completion, expiry,
retirement and run removal. The finite-trace model additionally checks the per-key scope counts.

### Validation

All thirteen generated properties passed at 100 cases each (1,300 generated cases). The latest focused review run
also passed the three new reservation, dequeue and lookup examples: 16 tests passed. Pre-handoff cancellation
follows the existing bounded retry path, without asserting that the authoritative incarnation is obsolete. After
handoff, both retirement schedules deliver the owner-committed task. The fallback example preserves its original
deadline through repetition and expiry.

| Command | Result |
| --- | --- |
| `cargo nextest run -p tokeira-runtime -p tokeira-storage -p tokeira-edge --locked --offline --no-fail-fast` | 2,033 passed (one marked leaky), 21 failed, one skipped. Every failure was a known sandbox socket failure: six edge wire tests and fifteen runtime HTTP tests. There were no other failures, and no test was excluded. |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | Passed on the final code. |
| `cargo clippy -p tokeira-storage -p tokeira-runtime -p tokeira-engine -p tokeira-edge -p tokeira-projection --features dsql-integration --all-targets --locked --offline -- -D warnings` | Passed on the final code. |
| `cargo +nightly fmt --all -- --check` | Passed. |
| `git diff --check` | Passed. |

The passing `unpause_workflow_routes_through_submit_and_resumes` test was marked leaky in the full run. Repeating
the same package command with `-E 'test(unpause_workflow_routes_through_submit_and_resumes)'` passed normally:
one passed, with no leak warning.

Disk space was checked before every build and remained above 12 GiB. Logs are under the existing target directory.
The sandbox permits neither the affected socket operations nor approval escalation, so the wire failures remain
unverified here. No live DSQL execution is claimed. The offer handler, dependency graph, lockfile, migrations and
protobuf definitions are unchanged.

## Exploration

Base: `ad71615c129e5d9c85417b6553ea62e42dfc6d21`. The runtime tests invoke the existing publisher and broker over
real stored intent. The normal case separates execution and queue homes; the sticky case separates execution,
sticky and normal homes and registers a recent completed sticky poll. Both have a colocated positive control.
The retirement case cancels the home acquisition with discovery stopped.

Each generated property uses ChaCha seed `[17; 32]`, varies duplicate publications from one through seven, and
shrinks to one publication. The explicitly executed wrapper accepts only its named desired-contract failure:

| Case | Retained counterexample |
| --- | --- |
| Normal publication | One committed pending task, split homes, no discovery tick: queue home receives no offer. |
| Sticky publication | One committed sticky task, three distinct homes, recent sticky worker, deadline still ahead: sticky home receives no offer. |
| Acquisition retirement | One ready offer, acquisition cancelled, no discovery tick: the retired offer remains claimable. |

Each property stops at its first failure and shrinks it; the configured 100-case ceiling is not a claim that 100
passing traces ran. An unexpected pass, another failure reason or a panic fails the harness. Convert the relevant
case to an ordinary passing regression when its fix lands.

Implementation revision: `420441131d532a438750e272daa3fe98cafa4fdb`.

## Start-read baseline

Two actual runtimes share a repository, own distinct homes, and start through the existing gRPC service with its
test worker authenticator. This measures the current start path; provisioned peer credentials are covered by later
delivery tests. The queue offer is already admitted. Warm cases prime the owner's lane with a committed signal; cold
cases use a new lane over persisted state. Each retry case rejects one commit before it writes, then observes the
runtime's fresh reload. This injection measures retry cost; it is not a competing-writer DSQL proof.

The eight memory cases and sixteen DSQL cases passed, with exact phase attribution. DSQL repeats every case
with inline and split state. Each table cell gives no retry / one injected conflict:

| Start | Cache | Logical loads | Inline read SQL | Split read SQL |
| --- | --- | ---: | ---: | ---: |
| Normal | Warm | 3 / 4 | 3 / 4 | 15 / 20 |
| Normal | Cold | 4 / 5 | 4 / 5 | 20 / 25 |
| Sticky | Warm | 4 / 5 | 4 / 5 | 20 / 25 |
| Sticky | Cold | 5 / 6 | 5 / 6 | 25 / 30 |

One inline load executes the fast hot-state SELECT. One split load executes that SELECT, then BEGIN, a hot-state
snapshot SELECT, one value-parts SELECT and COMMIT: three SELECTs and two transaction statements. The split
fixture carries a 1 MiB completion result. No legacy projection-seed read occurs in these fixtures. Counts are
asserted per phase as well as per attempt, so retry or admission-cache work cannot disappear inside a total.

Each attempt loads once at the queue home for resolution, once at the owner for metadata, and once at the owner
for resolution. Sticky authorization adds one owner load. A cold admission cache adds one load and supplies the
lane; one rejected commit adds one lane reload. Both the zero queue-home target and owner preparation target fail
explicitly in every case.
Owner preparation remains provisionally one warm or two cold loads, plus necessary retry reloads.

`test-support` registers an observer by run and attempt, following spawned lanes without changing a wire field.
`load_run_with_stats` counts once. DSQL attribution names each state SELECT, part read, legacy projection seed
read, and explicit read-snapshot transaction statement. Commit SQL and lease/registry/history queries are outside
the run-load metric. This fixture performs no publication or discovery hydration within its measured attempt;
their excluded counts are zero here, not a claim that those paths never load. The later consolidation must keep
their cost separately visible. Ordinary builds have no observer registry or conflict injection.

## Sticky page contract

The shared memory/DSQL contract generates 100 cases of sticky and non-sticky rows across homes, live and exact
routing (including present empty coordinates), requested bounds below and above 64, and keyset continuation.
An independent fixture-coordinate oracle checks complete sorted coverage. Mutation cases delete the cursor row,
insert ahead and behind it, then check continued traversal and a fresh head pass. Every returned candidate counts
as examined, independent of whether a later consumer admits it. Fixtures remove only their own keys.

The query first selects at most 64 keys through the sticky-home partial index, then fetches their payloads in the
same statement snapshot, without an explicit BEGIN/COMMIT pair. The live plan check passed against the actual
builder with 8,192 fixture rows and 1,024 sticky rows in the selected home. It verified exactly 64 returned keys
at offsets 0, 512 and 960. The first page uses
`idx_workflow_dispatch_sticky_home` with `shard_id` equality as its index condition. Both continuation plans put
`run_key > cursor` in that same index condition, rather than applying it as a filter after scanning the prefix.
The payload side uses `workflow_dispatch_pkey` with `run_key = candidates.run_key` under a batched nested loop.
The final sort contains only the bounded candidate page. Normalized continuation shape:

```text
CTE candidates
  Limit
    Index Only Scan using idx_workflow_dispatch_sticky_home
      Index Cond: ((shard_id = home) AND (run_key > cursor))
Nested Loop (Batched Join)
  CTE Scan on candidates
  Index Only Scan using workflow_dispatch_pkey
    Index Cond: (run_key = candidates.run_key)
```

The first development plan run hit the unchanged live-suite timeout while using per-row fixture writes and
schema bootstrap. Its cluster was deleted and absence verified. The fixture now seeds and removes rows in
bounded batches; the rerun passed on a fresh ephemeral Aurora DSQL cluster. Only the rerun supplies plan evidence.

At implementation revision `420441131d532a438750e272daa3fe98cafa4fdb`, the following focused live tests passed:

| Test | Executed evidence |
| --- | --- |
| `queue_home_delivery_live_sticky_query_plans` | Three exact-builder plans and 64-key page assertions over the fixture above. |
| `queue_home_delivery_live_read_baseline` | Sixteen real two-runtime gRPC starts, covering normal/sticky, warm/cold, inline/split and zero/one injected conflict. |
| `queue_home_delivery_live_sticky_pages` | All 100 generated shared storage cases; the same contract also passed on memory. |
| `workflow_dispatch_live_ordered_pages` | Existing ordered-dispatch preservation contract. |

Shard-zero recovery-candidate / dispatch counts were 0 / 0 after each new suite and 31 / 28 after the existing
ordered-page suite. The new storage fixtures use reserved homes and remove their own keys; the start baseline
terminates and removes its runs and relinquishes its leases. Placement preparation ran before lease-taking work.
Both original validation clusters were deleted and their absence verified. The ephemeral validation devbox
was expired and its absence from the active inventory verified.

## Non-live validation

The full finish bar passed on the ephemeral Linux devbox: pinned-nightly formatting, `cargo lint --locked`,
`cargo check --workspace --locked`, `cargo nextest run --workspace --locked` (3,940 passed), workspace doctests,
and workspace documentation with `RUSTDOCFLAGS="-D warnings"`. Local nightly formatting passed.

The two default-suite skips are the existing opt-in SDK worker roundtrips
`sdk_echo_roundtrip_against_post_sync_tokeirad` and
`sdk_scoped_worker_roundtrip_uses_production_authorization_bootstrap`. No exploration property is ignored.
URL-gated tests without credentials contribute no live evidence.

Three negative controls ran separately: removing sticky selection, removing the continuation bound, and making
the exploration property unexpectedly succeed. Each failed its named invariant, without a timeout; each changed
file was restored byte for byte. The schema checksum/digest audit preserves the complete merged migration prefix.
The scratch changie dry run, offline Markdown link check, `git diff --check`, and dependency bans/licenses/sources
checks also passed. No dependency version or lockfile changed.

## Review follow-up

The review fix in `d2b365e086efc21ae24fc2a6619c8613d89dc9de` is based on
`0053c6ac03351580b08f64982744d9b95300b35a`. `HistoryNotifyingRepository`, which the engine places around
the runtime repository, now forwards the sticky-page operation. The regression uses committed sticky and
non-sticky runs across distinct homes and checks populated pages, the requested limit, cursor continuation,
exhaustion and payload equality with the underlying store.

The regression passed locally and in the full workspace suite. A negative control removed only that forwarding
method: the regression failed with the precise unsupported-operation error. Restoring the source byte for byte
made it pass again. The DSQL fetch now executes the unchanged key-page/payload-join statement directly on its
connection; its single statement supplies the shared snapshot without an explicit transaction.

The complete finish bar passed on the rebased tree, including 3,948 workspace tests and the same two existing
opt-in skips. Formatting, lint, workspace check, doctests, warning-clean documentation, the offline Markdown
link check and scratch changie dry run passed. The source used remotely was verified byte for byte against the
local review tree.

All four focused live suites listed above passed again with the review fix: three first/deep-continuation plans
over 8,192 rows, all sixteen remote-start read cases, all 100 generated sticky-page cases and the existing
ordered-page contract. The single-statement pages retained index-bound continuation and primary-key payload
lookups. Every recorded inline/split read total matched the baseline matrix. Shard-zero counts were again 0 / 0
after each new suite and 31 / 28 after the existing ordered-page suite.
The additional ephemeral Aurora DSQL cluster was deleted and its absence verified. The review devbox was
expired and its absence from the active inventory verified.
