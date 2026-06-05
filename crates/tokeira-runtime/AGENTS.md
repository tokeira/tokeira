# AGENTS — tokeira-runtime

Crate-local rules. The root `AGENTS.md` still applies; this refines it for the runtime.
On conflict, the stricter rule wins.

## The one boundary: history is authority; everything else is a derived effect

The runtime owns correctness *execution*: lane-local processing, shard/bundle ownership,
durable transitions, and the derived effects (dispatch, timers, projection) the kernel's
transitions call for. The load-bearing rule (root §3):

- **Every state-changing request becomes a durable per-run transition, committed under a
  fence.** Persistence — including activity heartbeat details and dispatch progress —
  goes through `repo.commit_transition` with the authoritative state, not through a
  volatile side channel and not through a kernel command. Volatile trackers (e.g. the
  heartbeat liveness tracker, timeout/cancel state) are fine for *liveness*, but they are
  never the source of truth.
- **Do not put correctness weight on a queue.** The broker (`broker.rs`), publisher, and
  backlog are *disposable delivery optimisers*: losing them must lose no work and complete
  nothing. Authoritative pending state lives with the run, and a sweeper reconstructs
  delivery from it after a crash. If a design makes a queue write load-bearing for
  correctness, the design is wrong. (This is the exact claim the `tools/simulation/broker`
  simulator falsifies — keep the two aligned when broker semantics change.)

## Fencing, idempotency, and the transport/waiter split

- Commits are fenced (CAS/OCC). A stale delivery id, a lapsed lease, or a re-delivered
  task must be rejected at commit, never allowed to double-apply.
- The update registry separates the *transport payload* (what the worker needs) from the
  *waiter* (the current API call). A caller soft-timeout clears the waiter only; it must
  not erase the admitted update. See `update.rs` (`clear_waiter` vs `remove`).

## Determinism

Sweepers, scanners, and drain logic that iterate a `HashMap` while drawing RNG or
producing ordered effects are a determinism hazard. Iterate ordered, or sort before
emitting. (This bit `placement` in the simulators; the same class of bug applies here.)

## Behaviour ground truth

Observable behaviour follows the targeted Temporal release (root §8). Read the local
`../temporal` checkout at the `TEMPORAL_SERVER_COMPAT` tag; cite path + tag in comments
for non-obvious decisions.

## Where things belong instead

- Semantic transition logic (what the new state *is*) → `tokeira-kernel` (pure).
- The actual DSQL/in-memory persistence mechanism → `tokeira-storage`. The runtime decides
  *when* and *under what fence* to commit; storage performs the write.
- Request translation / public API shape → `tokeira-edge`.
