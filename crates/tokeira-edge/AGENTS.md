# AGENTS — tokeira-edge

Crate-local rules. The root `AGENTS.md` still applies; this refines it for the edge.
On conflict, the stricter rule wins.

## The one boundary: the edge is thin — it translates, it does not decide

`tokeira-edge` is the compatibility shell for the public Temporal APIs. It admits,
authenticates, validates, and translates requests, then hands them to the runtime. It
does NOT own workflow semantics.

- **No workflow correctness logic here.** Defaulting that changes outcomes, lifecycle
  ordering, fencing, dedupe — those are runtime/kernel concerns. The edge maps wire types
  to internal calls and back (`translate/`, `workflow_service.rs`, `operator_service.rs`).
- **Thread request fields through faithfully; do not silently drop them.** A request's
  `run_id`, wait policy, and lifecycle stage carry meaning. Targeting the exact run when a
  valid non-empty `run_id` is present (current-run fallback only for empty `run_id`),
  honoring `WaitPolicy` blocking semantics, and per-RPC stage defaulting are behaviour the
  edge must preserve — not normalize away. (The update-lifecycle work exists because these
  were being dropped.)

## Behaviour ground truth (this is where conformance is won or lost)

Public-API behaviour follows the targeted Temporal release (root §8), verified in order:

1. `proto/upstream/` for wire shape (messages, field numbers, enums, oneofs).
2. The §8 reference checkout at the `TEMPORAL_SERVER_COMPAT` tag for runtime
   behaviour the proto does not specify (error/status mapping, defaulting, NOT_FOUND vs
   INVALID_ARGUMENT, blocking contracts).

Resolve a behaviour question against ground truth *before* implementing, and cite the
source path + tag in a comment. Do not infer API behaviour from SDK docs, blog posts, or
memory. A "definitive Understand-Temporal-behaviour" question is resolved, never left as
"honour or defer".

## Where the deciding belongs instead

- Workflow semantics, fencing, durable transitions → `tokeira-runtime` / `tokeira-kernel`.
- Visibility/read-model shape → `tokeira-projection` (the edge re-exports its types).
- The edge's job ends at a faithful translation and a correct status/error mapping.
