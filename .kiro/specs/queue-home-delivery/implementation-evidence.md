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
Both task-owned Aurora DSQL clusters were deleted and their absence verified. The ephemeral validation devbox
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
