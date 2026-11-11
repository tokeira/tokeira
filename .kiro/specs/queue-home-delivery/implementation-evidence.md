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

All thirteen generated properties passed at 100 cases each (1,300 generated cases). Pre-handoff cancellation
follows the existing bounded retry path, without asserting that the authoritative incarnation is obsolete. After
handoff, both retirement schedules deliver the owner-committed task. The fallback example preserves its original
deadline through repetition and expiry, an expired row leaves the rest of its page admissible, and a poll reaches
current work behind retired entries.

The full bar passes on the final code, on macOS: fmt, lint, check, doctest and doc, and test (4,578 passed,
3 skipped). One `tkr` test, `discovered_selection_creates_and_checks_with_the_generated_compose_provisioner`, failed:
it builds a nested workspace and expects its binary in that workspace's default `target` directory, and a
`CARGO_TARGET_DIR` override for the whole run placed the binary elsewhere. `tkr` does not depend on
`tokeira-runtime`, the only crate this change touches, so that test's code is the same as on `main`. `cargo clippy
--features dsql-integration` is clean for the storage, runtime, engine, edge and projection crates. The changie dry run,
offline Markdown links and `git diff --check` pass.

No live DSQL or network validation is claimed. The offer handler, dependency graph, lockfile, migrations and
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


## Part 4: push and sticky fallbacks

### Activated behavior

Committed normal workflow tasks now reach their queue home by local admission or authenticated push. The runtime
owns the bounded sender; the edge implements its provider-agnostic sink using trusted placement, pooled channels
and peer tokens. The handler acknowledges retention or an incarnation already known in this acquisition, without
starting work or waiting for a lane. Engine wiring installs the sender only with a validated peer endpoint.
No-peer modes deliver locally without constructing a sender, sink or endpoint.

The sticky home checks its ten-second recent-poller window first, including completed polls without a waiter and
worker shutdown. With a poller present it checks deployment change, using the same current/ramping selection as
Tokeira's start path. Pinned tasks and directives with an assigned build bypass deployment fallback; a nil target
is not unavailability. The internal request adds workflow identity and the initial ramping choice. Upstream protos
and the existing routing hash are unchanged. Only no recent poller and deployment change are produced, following
the [worker-versioning decision](../worker-deployments/reference/v1-v2-conformance-decision.md); wire reasons 3 and
4 remain reserved. Poller-first ordering follows `AddWorkflowTask`, `service/matching/matching_engine.go @ v1.31.0`.

Local publication uses committed state and its existing deployment decision, with no run reload or second policy
lookup. One state-based resolver supplies the normal fallback target to both publication and the off-lane sender.
The sender's fallback reload is attributed to `publication-fallback`, separately from start reads. Routing read
failure is transient only after an available poller; it cannot override `NoRecentPoller`.

Fallback reaches the original normal queue's home and keeps the sticky origin and absolute deadline, including
the publisher's deployment-change branch. Same-home retargeting requires the receiving home's own decision;
arrival order alone answers `Known`. Obsolete incarnations have their own outcome, distinct from expiry.
No hint outcome writes authoritative state. Activity, speculative, eager, query and start delivery are unchanged.

### Bounds and recovery seam

The sender retains at most 1,024 entries and 16 MiB per node, counting active work as well as queued work and
coalescing by incarnation. Sender and broker use the same capped serialized-envelope byte counter. Up to 32
attempts run per node, at most eight per destination; source alternation and round-robin homes share capacity
between publication and recovery. Each attempt includes routing, credentials and RPC within 500 ms; residence,
retries and fallback share one second. At most three delivery attempts use bounded jitter, with one trusted
placement refresh after `NotHome`. Acquisition loss cancels queued and active work. Fallback re-enters scheduling
for its second destination. Saturation and delivery counters use bounded outcome/source/reason labels.

The recovery source is available to the subsequent sticky sweep. That sweep will traverse durable sticky intent
and republish through this sender after lost publication or placement changes. It is not active here. Ordinary
discovery's period and limits, acquisition-driven admission retirement, storage formats and migrations are unchanged.

### Regression inventory

All generated properties below configure 100 cases each: sixteen runtime properties and three edge properties,
1,900 cases in total. They use injected time or synchronization barriers, with no sleeps.

| Property | Generated tests |
| --- | --- |
| 4 | `off_home_sticky_never_selects_local_fallback`, `sticky_conditions_and_controls`, `same_home_fallback_and_absolute_deadline`, `distinct_homes_fallback_and_lost_reply` |
| 4 | `receiver_ramping_and_recent_window`, `local_fallback_with_failed_run_loads`, `no_poller_precedes_routing_failure`, `local_publication_reuses_routing` |
| 7 | `receipts_and_interleaved_discovery`, `publishing_lane_paused_until_receipt`, `full_sticky_home_never_selects_fallback` |
| 7 | `sender_retention_bounds`, `sender_fairness_and_concurrency`, `sender_budgets_refresh_and_failures`, `sender_acquisition_loss`, `obsolete_fallback_outcome` |
| 4 / 7 | Edge `wire_routing_and_sticky_origin`, `handler_and_sink_in_process`, `sink_placement_and_destination_cap` |

Test names carry the corresponding `property_4_` or `property_7_` prefix. The edge tests call the real handler and
sink preparation in-process; they do not claim multi-runtime socket integration. Routing properties include each
sticky cause alone and together, pinned/assigned controls, completed polls, worker shutdown, unavailable policy,
repeated fallback and deadline-versus-selection races. Durable snapshots remain unchanged by delivery outcomes.

Before implementation, the off-home sticky property admitted at the wrong home and the existing
`sticky_dispatch_moves_to_normal_queue_when_current_target_changes` test lost its deadline. Both failed against
main before their fixes. Before the review corrections, local fallback was lost when every run load failed and
no poller with failed routing returned `Routing` instead of `StickyUnavailable(NoRecentPoller)`.


### Validation

The four-crate nextest run (`-p tokeira-runtime -p tokeira-storage -p tokeira-edge -p tokeira-engine --locked
--offline --no-fail-fast`) passes on macOS: 2,472 tests passed and one was skipped. All nineteen generated
routing/scheduling properties passed at 100 cases each, including the paused lane, ramping window and full broker.
The two main-first regressions also pass. In-process edge checks need no listener. Real multi-runtime gRPC and live
DSQL verification are recorded in Parts 7 and 8.

Warning-denied workspace/all-targets clippy and the five-crate `dsql-integration` clippy command pass with
`--locked --offline`. Nightly formatting and `git diff --check` pass. There is no dependency change.

## Part 5: sticky recovery

### Acquisition lifecycle and bounds

Every successfully repaired execution-home acquisition installs one continuing sticky sweep, after activation.
Controller acquisition, self-assigned recovery, seeded recovery and empty-store startup all use the same hook.
Duplicate installation for one generation is ignored. The runtime shutdown tracker owns each sweep; cancellation,
lease deadline or runtime shutdown drops its pending read, permit and cursor. A delayed predecessor cannot install
work for its replacement. Only volatile per-home query pacing survives a local reacquisition; traversal restarts
at the beginning.

The sweep reads `list_sticky_workflow_dispatch(home, after, 64)`. FIFO node-wide capacity admits at most eight
pages concurrently, including their serial hydration, and releases capacity before a home's next period. Each
home starts at most one query per second, including across replacement. Each completed row advances the cursor,
including skipped and refused candidates. A failed page query preserves the cursor; a failed hydration leaves
the exclusive cursor immediately before that row, retaining progress through the completed prefix. Retries use
two-second exponential backoff capped at 32 seconds. Individual reads time out after one second; there is no
whole-page timeout that could repeatedly prevent later rows from being examined. After the last row the page's
last examined key is used; exhaustion wraps on the following pass. No cursor, delivery bit, repair or cross-home
write is persisted.

Before hydration, rows with an expired deadline are skipped as `expired`, and rows younger than the sender's
one-second total budget are skipped as `recent`. Both advance the cursor; wrapping revisits recent rows after
publication has had its retry budget. Eligible candidates are hydrated off-lane and compared with their freshly
derived dispatch row, including incarnation and intent. Current candidates become `WorkflowOffer`s with
`PublicationSource::Recovery`. The existing sender coalesces and bounds publication and recovery together:
1,024 entries, 16 MiB, 32 active attempts and eight per
destination, alternating sources and scheduling homes fairly. No-sender modes invoke the same local receiver and
resolve normal fallback with `fallback_target` using the already hydrated state, with one run load per pass.
Both paths re-decide sticky availability and deployment policy even after an earlier normal fallback, and
retain the original absolute deadline. This preserves the fallback/TTL rule in
`service/history/transfer_queue_active_task_executor.go @ v1.31.0` and recent-poller check in
`service/matching/matching_engine.go @ v1.31.0`.

Hydration loads use `sticky-recovery`; only the sender's later fallback reloads under `sticky-recovery-fallback`,
separate from `publication-fallback` and start reads. The three `tokeira_runtime_workflow_offer_sweep_*_total` counters record
examined candidates, retained/queued/coalesced offers, and skips/incomplete reads by bounded reason. They carry no
run or queue labels. Ordinary normal discovery, timeout transitions, admission/retirement, other task kinds,
storage and migrations are unchanged.

### Generated runtime evidence

Fifteen properties configure 100 cases each (1,500 generated cases), using paused clocks and channel barriers.
They exercise real memory-store pages, delivery components and the runtime activation hooks. Remote attempts use
an in-process fault sink; real multi-runtime gRPC and live DSQL execution are recorded in Parts 7 and 8.

| Property | Tests and coverage |
| --- | --- |
| 1 | `property_1_lost_sticky_row_recovered_before_deadline`: a full runtime recovers a committed sticky row without publication or normal discovery demand, in empty-start and self-assigned modes. |
| 1 | `property_1_recovery_finite_fault_prefix_fair_suffix`: finite page/load failures and full broker capacity followed by a fair suffix offer every pending task; durable snapshots are unchanged. |
| 4 | `property_4_recovery_lost_sticky_push` and `property_4_recovery_lost_normal_fallback_push`: separately dropped transport attempts, sender restart over a memory snapshot, queue-home replacement and finite delivery faults. Normal fallback has no useful sticky demand. |
| 4 | `property_4_recovery_expiry_and_cleared_affinity`: expiry leaves state unchanged; cleared affinity and an authoritative timeout successor are recovered by normal discovery alone. |
| 4 | `property_4_recovery_local_fallback_reuses_hydration`: sticky-unavailable rows reach the normal queue without a sender, with exactly one run load per pass and the original deadline. |
| 8 | `property_8_recovery_cursor_wrap_and_saturation`: multiple pages, saturated sender, strict continuation past refused rows, and insertion behind the cursor found after wrapping. |
| 8 | `property_8_recovery_fair_bounded_scheduler`: 9–24 homes, duplicate activation notifications, FIFO progress, eight-query bound, one-second pacing and tracked shutdown. |
| 8 | `property_8_recovery_incomplete_read_retries`: page failures preserve the continuation; middle-row hydration failures retry that row after backoff without loading or offering completed rows again. Load counts and offer counts cover the fair suffix. |
| 8 | `property_8_recovery_recent_rows_skip_hydration`: recent rows cause no run loads, advance the cursor and recover on a later pass at the sender budget boundary. |
| 8 | `property_8_recovery_expired_rows_skip_hydration`: expired rows cause no run loads or offers and leave durable state unchanged for timeout processing. |
| 8 | `property_8_recovery_cancelled_acquisition`: cancellation/deadline during a held read cannot populate replacement delivery; replacement starts from the beginning. |
| 8 | `property_8_recovery_stale_candidate`: incarnation and affinity changes after page selection are skipped while cursor progress and other candidates are preserved. |
| 8 | `property_8_recovery_reacquisition_restarts_at_head`: replacing a continued traversal resets its cursor while preserving one-second home pacing. |
| 8 | `property_8_sticky_sweep_waits_for_repair`: seeded and explicit repair barriers prevent premature reads; interrupted repair never starts a sweep. |

The main-first regression was executed against an archived `origin/main` at
`b62607d69cf744ad0095ceea4ad3a6cdee4f84fa`, with only the new test and its module declaration added.
`property_1_lost_sticky_row_recovered_before_deadline` failed: no normal offer arrived within three seconds,
before the original 30-second deadline. The shrunk counterexample was `index = 0, seeded = false`, with seed
`c7ae6fa1d9a36b220f97d3f742f169ed03b2fe746d4ab89d0d7d2eab1fe82728`. It passes with the sweep.
The other new tests are implementation coverage, not additional claims of execution against main.

The existing `restart_preserves_wft_completion_routing_metadata` test passed on main but assumed cold sticky work
remained unavailable until timeout. Its manual timeout could now leave an older recovery hint ahead of the
replacement task, which its task-only polling helper reports as a stale-sequence error. The updated test shuts
down the old runtime, polls through the public activation path and asserts normal fallback of the original
incarnation before its deadline, with full-history delivery. Start and timeout implementations are unchanged.

### Validation

- `cargo nextest run -p tokeira-runtime -p tokeira-storage -p tokeira-edge -p tokeira-engine --locked --offline --no-fail-fast`: 2,487 passed and one skipped, including the socket tests. All fifteen generated recovery properties pass at 100 cases each, as does the updated restart test.
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`: passed.
- `cargo clippy -p tokeira-storage -p tokeira-runtime -p tokeira-engine -p tokeira-edge -p tokeira-projection --features dsql-integration --all-targets --locked --offline -- -D warnings`: passed; this is compile/lint coverage, not live DSQL execution.
- `cargo +nightly fmt --all -- --check`: passed.
- `git diff --check`: passed.

No dependency, lockfile, storage or migration change is included.


## Part 6: start reads

### Owner preparation and the disposable route

Publication, pushed admission, normal discovery, sticky fallback and sticky recovery carry the execution home
through the broker's volatile `TimestampedWorkflowTask` and `WorkflowOfferStamp`. The queue home forwards from
that hint without hydrating the run. `DispatchableWorkflowTask`, durable backlog encodings, storage and migrations
are unchanged. Legacy in-process callers and speculative offers without the hint retain their resolver, still
attributed to `queue-home/resolve`. The start wire contract is unchanged: the queue home uses its volatile home
hint to choose a destination, and the owner's serving gate checks the home derived from authoritative state.
A node that does not serve that home refuses the request; a request reaching the real owner can start regardless
of the hint used to reach it. The binding inventory fixture now also records offer fields 13 and 14 from Part 4,
which it omitted.

The authenticated owner calls `load_run_with_stats` once under `execution-owner/preparation`. Its non-serializable
`PreparedWorkflowStart` supplies namespace and authoritative normal-queue binding, repeated sticky worker policy,
metadata bounds, history size and target/version resolution. Query and update snapshots are frozen before the
start; metadata, attachment bytes and protobuf framing must all fit before submission. Refusal does not mark an
update sent or consume a query waiter. Successful delivery marks only the frozen update selection and binds the
frozen queries on the execution owner, preserving their waiters.

The existing submit path retains its admission-cache lookup. The lane uses its own state, serving acquisition and
commit fence; it never installs the prepared image. The check captures registry routing config and stored routing
config before submission. On every attempt the lane synchronously re-derives queue/sticky binding, version target
and deployment transition using its own state and those captured inputs, with no routing read or await. A changed
run identity or decision is refused, while an unrelated signal, activity completion or update admission can advance
the transition sequence without refusing the start. The started event records the prepared history size.

`service/history/api/recordworkflowtaskstarted/api.go @ v1.31.0` checks that the run is running, then checks the task's
scheduled event ID, stamp and whether it has already started; it does not reject every mutable-state version change.
Tokeira retains the kernel's task-incarnation check and the execution component's serving/fence checks. Every OCC
conflict still evicts and reloads, followed by synchronous decision revalidation against that fresh state. Routing
changes after preparation are not caught in the lane, matching main's pre-submit routing lookup. Pair commits,
successor construction and update reconciliation are unchanged.

### Memory matrix and read accounting

The baseline below is the previously executed two-runtime gRPC measurement. The new eight-case matrix runs the
same two runtimes, repository and authenticated owner handler twice: with an in-process `WorkflowTaskStarter`, and
over real gRPC in `queue_home_delivery_exploration_read_baseline`, with the same assertions.
Each cell is **zero conflicts / one rejected commit**. The target has not changed.

| Phase | Normal warm before → after | Normal cold before → after | Sticky warm before → after | Sticky cold before → after |
| --- | --- | --- | --- | --- |
| `queue-home/resolve` | 1/1 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 |
| `execution-owner/sticky-binding` | 0/0 → 0/0 | 0/0 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 |
| `execution-owner/metadata` | 1/1 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 |
| `execution-owner/resolve` | 1/1 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 | 1/1 → 0/0 |
| `execution-owner/preparation` | 0/0 → 1/1 | 0/0 → 1/1 | 0/0 → 1/1 | 0/0 → 1/1 |
| `execution-owner/admission-cache` | 0/0 → 0/0 | 1/1 → 1/1 | 0/0 → 0/0 | 1/1 → 1/1 |
| `execution-owner/lane-cache-or-retry` | 0/1 → 0/1 | 0/1 → 0/1 | 0/1 → 0/1 | 0/1 → 0/1 |
| **All logical loads** | **3/4 → 1/2** | **4/5 → 2/3** | **4/5 → 1/2** | **5/6 → 2/3** |

No run read moved to publication or hydration. Immediate local publication still uses committed state with zero
run reloads; normal discovery's existing live-routing hydration is now explicitly scoped as `normal-discovery`
(exact routing requires no hydration). Sticky sweep hydration remains `sticky-recovery`; later sender fallbacks
remain `publication-fallback` or `sticky-recovery-fallback`. These scopes are excluded and separately visible in
the reports. Unexpected scopes fail the per-phase assertions. Lane ownership-generation refreshes, previously
unscoped, now also count as `execution-owner/lane-cache-or-retry`; concurrent acquisition changes are separate from
the stable-cache matrix. Live registry/policy queries and history fetches remain outside the run-load metric.
When configured, preparation performs the existing `workflow_task_routing_config` and `load_deployment` lookups
before submission and captures their results. Lane revalidation performs zero registry/deployment reads, including
on OCC retry; the mandatory fresh run reload is unchanged. No routing read moved into the lane or into an excluded
hydration scope. The unversioned baseline has no configured deployment repository.

The live DSQL variant's per-phase logical and SQL expectations use the new matrix.
One inline load still means one fast hot-state SELECT; one split load still means that SELECT plus BEGIN,
snapshot SELECT, value-parts SELECT and COMMIT. Consequently the expected SQL totals are 1/2 warm and 2/3 cold
for inline state, or 5/10 warm and 10/15 cold for split state, for both normal and sticky starts. The live run
in Part 8 measured exactly these totals.

### Regression evidence

`queue_home_delivery_prepared_reads_in_process` was executed against an archived `origin/main` at
`b62607d69cf744ad0095ceea4ad3a6cdee4f84fa`, with only the new socket-free test harness added. It failed on the first
cold normal case: `(queue loads, owner loads) = (1, 3)`, expected `(0, 2)`. The report attributed one each to queue
resolution, owner metadata, owner resolution and owner admission cache. This is the executed main-first regression;
the additional generated properties are implementation coverage, not claims of execution against main.

Six Property 6 tests configure 100 cases each:

- `property_6_prepared_read_budget`: normal/sticky, warm/cold and zero/one injected conflict, asserting every phase.
- `property_6_prepared_bindings_and_envelope`: a misrouted request is refused by the non-owner's serving gate without
  a home wire field; eight forged request coordinates per case; query and update attachments one byte over a bounded
  limit and exactly at it; refusal before commit, retained query waiter and one-time update send.
- `property_6_prepared_state_and_policy_changes`: a signal committed after preparation, through the lane or a
  competing durable writer, still allows the pending task to start. Warm stale state reloads after the real OCC
  conflict. Per-phase read counts stay unchanged and the start records the prepared history size. A barrier changes
  worker policy before repeated sticky authorization, which must use the authoritative normal queue.
- `property_6_prepared_lane_uses_captured_routing`: routing changes after target resolution and before lane execution;
  normal and sticky starts retain their captured policy, matching main's off-lane lookup.
- `property_6_prepared_lane_refuses_changed_decision`: a real sticky reset or versioning override between preparation
  and execution still refuses the start, on warm/cold state and after competing writes requiring OCC reload.
- `property_6_offer_sources_carry_execution_home`: local publication, pushed admission, discovery and sticky fallback
  carry the original execution home through dequeue. The existing 100-case
  `property_4_recovery_local_fallback_reuses_hydration` additionally checks the sweep's home hint through dequeue.

`prepared_start_does_not_read_deployments_in_lane` covers both normal and sticky starts. After preparation, a gated
repository proves that a deployment read blocks. While that gate stays closed, the prepared start and a signal on
another run using the same one-lane runtime both commit; any additional deployment read fails the test.

The first generated test run exposed two fixture errors, both corrected: reusing an empty signal request ID
caused legitimate deduplication instead of a state change, and `snapshot_pending_updates` deliberately retains
sent-but-unaccepted transport. The corrected tests use unique signal request IDs and assert that a second
non-retry send selection is empty while the transport and waiter remain available.

### Validation

- `cargo nextest run -p tokeira-runtime -p tokeira-storage -p tokeira-edge -p tokeira-engine -p tokeira-proto --locked --offline --no-fail-fast`: 2,505 passed and one skipped, including the socket tests. The eight-case in-process and gRPC matrices, all six 100-case Property 6 tests, the two-scenario gated-repository test and the extended 100-case sweep property pass.
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`: passed.
- `cargo clippy -p tokeira-storage -p tokeira-runtime -p tokeira-engine -p tokeira-edge -p tokeira-projection --features dsql-integration --all-targets --locked --offline -- -D warnings`: passed; compile/lint coverage only.
- `cargo +nightly fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo run -p proto-sync --locked --offline -- generate`: passed. `WIRE_PARITY_CAPTURE=1 cargo nextest run -p tokeira-proto --test binding_inventory --locked --offline` updated the inventory; rerunning without the capture flag passed its one test.

The task-specific socket tests are
`grpc::workflow_dispatch::read_baseline::queue_home_delivery_exploration_read_baseline` and
`grpc::workflow_dispatch::tests::forwarded_start_and_replies_use_owner_waiters_and_reauthenticate` in
`tokeira-edge`. `queue_home_delivery_live_read_baseline` has updated logical/SQL expectations; its live run is
recorded in Part 8. No dependency, lockfile, durable storage format or migration changed.


## Part 7: fencing, preservation and multi-runtime integration

`PeerOfferEndpoint` owns a system-clock default and a clock builder; its Unix-seconds method supplies verification
and all three peer signing purposes. Engine startup installs the runtime clock before sharing the endpoint, so
runtime deadlines and peer credentials use the same time without changing the 300-second lifetime, renewal below
60 seconds, 30-second leeway or verification policy.

### Fencing and preservation

The engine's `peer::delivery_tests` harness follows the authenticated runtime/placement setup in
`workflow_dispatch_read_baseline`. Real memory repositories retain their transition and lease checks; a forwarding
repository adds independent recovery-reader gates, counted loads and a one-shot commit barrier. The generated
fencing case takes execution ownership before a start or while its commit is held, checks that the former writer
cannot commit, replays its still-current hint through the peer gate and starts under the new epoch. It checks
repeated starts, forged future sequences and an obsolete sequence after a real worker failure creates a successor.
The successor is completed before counting durable history: transient retry starts materialize at completion
(`service/history/workflow/workflow_task_state_machine.go @ v1.31.0`). The movement case holds an offer on the wire
while its queue acquisition is retired, then delivers the same current hint to the replacement home.

`queue_home_delivery_live_fenced_starts` runs those same two helpers with a real DSQL repository, 100 schedules
per helper, behind `dsql-integration` and `TOKEIRA_DSQL_TEST_DATABASE_URL`. Placement is prepared before leases;
owned runs are terminated and purged and owned leases are relinquished. Its live run is recorded in Part 8.

Five generated preservation tests use committed memory state and injected clocks: local priority ordering,
eager/reserved start suppression, speculative update transport and rejection, query/update owner attachments with
completion/failure forwarding, and acquisition reconstruction of sticky timeout/dispatch state. The deferred
production delivery paths are unchanged; activity and outbound senders only use the endpoint's time source.

### Wire scenarios

Each node loads the protected file produced by the daemon's `generate` function using the path configuration it
prints. The runtimes, interceptors and peer endpoints share a controllable clock. Advancing it by 331 seconds
makes the original token fail at the actual wire gate while renewed tokens continue delivering between the same
running nodes; the credential file remains unchanged. Public polling and remote starts use a worker authenticator
and real gRPC, without an external identity provider. Offer fault injection drops requests or replies outside the
unchanged authorization gate; all delivered requests traverse that gate.

The focused wire set, including the generated fencing and attachment cases and the token boundary test, passes
in 4.193 seconds. Per-test wall times from that run are:

| Test | Seconds | Evidence |
| --- | ---: | --- |
| `grpc_normal_push_remote_start_and_renewal` | 0.231 | Two runtimes, both recovery readers blocked, remote start with zero queue loads and one warm owner load, expiry and renewal. |
| `grpc_three_homes_no_recent_poller` | 0.286 | Separate execution, sticky and normal homes; immediate normal fallback retains affinity and deadline. |
| `grpc_three_homes_deployment_changed` | 0.294 | Recent sticky poll; a barrier changes deployment between send and receive; a versioned public poll remotely starts the current version. |
| `grpc_dropped_sticky_push_recovered_by_sweep` | 0.325 | Sticky attempts are lost; only the execution-home reader is released; sticky delivery recovers. |
| `grpc_dropped_fallback_push_recovered_by_sweep` | 0.391 | Normal fallback attempts are lost; the sticky reader re-evaluates and republishes the original durable intent. |
| `grpc_dropped_acknowledgement_recovered_by_sweep` | 0.442 | Admitted normal fallback loses its replies and volatile offer; the sticky reader recovers it. |
| `grpc_dropped_normal_push_recovered_by_discovery` | 0.263 | Only normal discovery is released after all normal attempts are lost. |
| `grpc_peer_policy_capacity_statuses_and_retirement` | 0.432 | Unauthenticated, wrong-purpose, invalid shape and size refusals; admitted/known/full/expired/not-home replies; 64 held authenticated handlers plus immediate retirement and replacement admission. |
| `property_3_fenced_starts_and_delayed_hints` | 4.119 | 100 generated fenced takeover/incarnation schedules through gRPC, including cold and warm owner load counters. |
| `property_3_queue_movement_mid_push` | 2.960 | 100 generated queue-movement schedules through gRPC. |
| `property_9_query_update_and_reply_routing` | 2.024 | 100 generated attachment and reply-routing cases through the existing authenticated owner service. |
| `peer_clock_expiry_and_renewal_through_gate` | 0.060 | Seven validity/renewal boundary requests plus a negative-epoch refusal. |

The normal/sticky, warm/cold, zero/one-conflict socket read matrix additionally checks zero queue-home loads,
one warm or two cold owner loads, and one fresh owner load per conflict. Reader barriers isolate prompt-push
claims; they are released explicitly for recovery claims. No test sleeps or waits for real token expiry.

### Property coverage and checkpoint

Every generated test below configures 100 cases. Names grouped by prefix refer only to queue-home-delivery tests
in the indicated modules; other features' similarly numbered tests are not included. Additional example and matrix
coverage is stated separately.

| Property | Executable coverage | Generated cases |
| --- | --- | ---: |
| 1 | `property_1_lost_sticky_row_recovered_before_deadline`, `property_1_recovery_finite_fault_prefix_fair_suffix`, `queue_home_delivery_exploration_normal_split_home`; normal push and discovery wire scenarios above | 3 × 100 |
| 2 | Seven `property_2_*` tests in `broker::admission_tests` and `workflow_offers::tests`; wire capacity/deduplication scenario | 7 × 100 |
| 3 | `property_3_fenced_starts_and_delayed_hints`, `property_3_queue_movement_mid_push` | 2 × 100 |
| 4 | Eight runtime routing properties, edge `property_4_wire_routing_and_sticky_origin`, four `property_4_recovery_*` properties and `queue_home_delivery_exploration_sticky_split_home`; all five sticky wire scenarios | 14 × 100 |
| 5 | Six `property_5_*` tests in `broker::admission_tests`; `queue_home_delivery_exploration_retirement_without_discovery` adds 100 deterministic schedules; wire retirement/replacement scenario | 6 × 100 |
| 6 | `property_6_prepared_read_budget`, `property_6_prepared_bindings_and_envelope`, `property_6_prepared_state_and_policy_changes`, `property_6_prepared_lane_uses_captured_routing`, `property_6_prepared_lane_refuses_changed_decision`, `property_6_offer_sources_carry_execution_home`; socket and in-process matrices each add eight cases, lane routing gate adds two | 6 × 100 |
| 7 | Eight runtime sender/admission scheduling properties, two edge sink properties, `generated_peer_policy`, `generated_material_agreement`, `generated_bounded_file_loading`, `generated_wire_shapes`, `generated_bounds_and_handler_saturation`; wire policy/capacity scenario | 15 × 100 |
| 8 | Nine `property_8_*` recovery/repair properties and `queue_home_delivery_sticky_pages_memory` | 10 × 100 |
| 9 | `property_9_local_delivery_priority`, `property_9_eager_and_reserved`, `property_9_speculative_updates`, `property_9_timeout_reconstruction_and_repair`, `property_9_query_update_and_reply_routing` | 5 × 100 |
| 10 | `generated_renewal_and_rotation`, `generated_peer_file_restart_rotation`, `generated_upgrade_bootstrap`, `generated_no_peer_startup`, `generated_durable_peer_exemption`; expiry/renewal wire and gate examples | 5 × 100 |

The connected normal and sticky exploration properties pass. Their explicit disconnected-sender negative controls
fail the desired delivery assertion, and accept only the expected reason and the shrunk one-publication trace
with ChaCha seed `[17; 32]`; these are expected-failure controls, not 100 passing cases. Retirement exploration
passes with discovery stopped. Reader gates, commit/transport barriers and receipts synchronize the new races.

Adjacent tracked `.proptest-regressions` files retain the generated fixture counterexamples. The corrected fixture
assumptions concern request-ID deduplication, a cache warmed by preceding refusals, transient retry history and
consistent clock stamping. They do not identify production defects. The only production change in this part is
the shared peer clock; there is no new main-first defect claim. The earlier executed main-first regressions
for lost sticky recovery and start-read budgets remain listed with their counterexamples above.


### Validation

All requested commands complete successfully:

| Command | Result |
| --- | --- |
| `cargo nextest run -p tokeira-runtime -p tokeira-storage -p tokeira-edge -p tokeira-engine -p tokeira-proto --locked --offline --no-fail-fast` | 2,523 passed, one existing ignored measurement; about 100 seconds of test execution. |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | Passed. |
| `cargo clippy -p tokeira-storage -p tokeira-runtime -p tokeira-engine -p tokeira-edge -p tokeira-projection --features dsql-integration --all-targets --locked --offline -- -D warnings` | Passed, including compilation of the live fencing/movement variant. |
| `cargo +nightly fmt --all -- --check` | Passed after `cargo +nightly fmt --all`. |
| `git diff --check` | Passed. |
| `cargo nextest run -p tokeira-auth -p tokeira-config --locked --offline --no-fail-fast -E 'test(peer)'` | Eight passed; 2.497 seconds. |
| `cargo nextest run -p tokeira-engine -p tokeira-edge --lib --locked --offline --no-fail-fast -E 'test(peer::delivery_tests) \| test(property_9_) \| test(peer_clock_expiry)'` | Twelve passed; 4.193 seconds. |

The affected contract suites executed in the full run include storage `memory::workflow_dispatch_tests` and
`workflow_dispatch::tests`; runtime `broker::admission_tests`, `workflow_offers::tests`, `workflow_delivery` routing,
sender and sweep tests, `runtime::repair_tests`, completion eager/inline/heartbeat tests, prepared-start and sticky
recovery tests, and `runtime_query`, `runtime_update` and `runtime_admitted_updates_after_restart`; edge
`grpc::workflow_dispatch` read/prepared/attachment tests, `grpc::workflow_offer` gate/delivery tests and workflow
service contracts; engine `peer::tests`, `embedded_dsql_tests` and the new wire harness; and the proto suite.
`generated_no_peer_startup` and `generated_durable_peer_exemption` each pass 100 cases. The existing ignored test is
`outbound::bug_condition_exploration_tests::child_fan_out_starts_every_child`, an opt-in measurement. The live DSQL
runs are recorded in Part 8.


## Part 8: live DSQL

### Run and outcomes

The live suites ran from a nextest archive of `c5f8df3dac8f4b325f0e58162bdbb420c16e2b60`, the Part 7 commit, on
one ephemeral Aurora DSQL cluster created for this run. Each step was a separate nextest process under the
`dsql-live` profile, one test at a time, with the URL gate set. All five tests passed:

| Step | Test | Seconds | Executed evidence |
| --- | --- | ---: | --- |
| 1 | Edge `queue_home_delivery_live_read_baseline` | 1,138.6 | Sixteen real two-runtime gRPC starts, each logging its executed report. The time includes migrating the new database, whose 44 asynchronous index builds took 1,042.8 seconds. |
| 2 | Storage `queue_home_delivery_live_sticky_pages` | 54.6 | All 100 generated bounded traversal and cursor-mutation cases on reserved homes. |
| 2 | Storage `queue_home_delivery_live_sticky_query_plans` | 14.0 | Exact-builder plans at offsets 0, 512 and 960 over 8,192 fixture rows, 1,024 of them sticky in the selected home, with 64-key page assertions. |
| 3 | Runtime `workflow_dispatch_live_acquisition_rejects_superseded_tracker_installs` | 8.3 | The unchanged acquisition-hook regression: real lease acquisition, interrupted recovery and refused old-generation installs. This change starts the sticky sweep from that hook. |
| 4 | Engine `queue_home_delivery_live_fenced_starts` | 590.0 | 100 generated fencing schedules and 100 queue-home movements through real gRPC between runtimes with peer credentials. |

The durations, the sixteen executed reports and the written plans show that every live body ran; none returned
at its gate. The read baseline ran first, on the new database, as the runbook's isolated check, and deleted each
run and released both leases between scenarios. The sticky fixtures then wrote and removed only their own keys on
reserved homes. The acquisition check and the fenced starts each prepared the same eight homes, which placement
accepts only while no lease is live, and each relinquished its leases; the fenced starts terminated and purged
every run they made. After the last step the cluster was deleted and its absence confirmed.

The rest of the shared-cluster campaign was not rerun. This change alters no storage code, schema or migration,
and the two storage suites above cover the only storage read the sweep uses.

### Start reads

All sixteen scenarios measured exactly the Part 6 expectations. Each cell gives zero conflicts / one rejected
commit:

| Start | Cache | Logical loads | Inline read SQL | Split read SQL |
| --- | --- | ---: | ---: | ---: |
| Normal | Warm | 1 / 2 | 1 / 2 | 5 / 10 |
| Normal | Cold | 2 / 3 | 2 / 3 | 10 / 15 |
| Sticky | Warm | 1 / 2 | 1 / 2 | 5 / 10 |
| Sticky | Cold | 2 / 3 | 2 / 3 | 10 / 15 |

Every load was the owner's: preparation once, the admission cache once when cold, and one lane reload per
rejected commit. The queue home loaded nothing. Before this change the same matrix measured 3/4 to 5/6 logical
loads ([start-read baseline](#start-read-baseline)). Reads outside the start were reported separately, as
designed: each normal scenario recorded one `normal-discovery` hydration, and five of the eight sticky scenarios
recorded one `sticky-recovery` hydration by the execution home's sweep, depending on when it reached the row.
Neither fallback scope recorded a read.

### Sticky plans

The three plans have the shapes recorded under [sticky page contract](#sticky-page-contract). Each selects
candidates with an Index Only Scan of `idx_workflow_dispatch_sticky_home` under the limit. The first page has
`shard_id` alone as its index condition; both continuations add `run_key > cursor` to that index condition.
Each candidate's payload comes from an Index Only Scan of `workflow_dispatch_pkey` on `run_key`, and the final
sort holds only the bounded page. The planner chose a plain nested loop here, where the earlier run showed a
batched join; either way at most 64 payloads are fetched by primary key.

### Limitations

Each suite ran once. The first migrating test used 1,138.6 of the profile's 1,200 seconds, almost all of it
waiting for index builds; the runbook now records that margin. The read counts come from a two-runtime fixture
with one injected conflict, not from competing writers under load.
