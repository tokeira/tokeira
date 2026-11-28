# Independent activity reference decisions

This unpublished crate derives expectations from Temporal server v1.31.0. Its pure
functions accept upstream failure/policy values and explicit time. They have no
access to Tokeira's execution state or service implementations. Consumers use a
path-only development dependency, except the unpublished behaviour harness, which
alone has a normal dependency on the model. No production dependency chain may
reach either crate.

`src/activity.rs` owns retry precedence, retryability, backoff, retry windows,
policy defaulting and validation, options rescheduling, completion-time provenance,
timeout selection and outcomes, and activity cancellation. Times are signed Unix
nanoseconds in the protobuf Timestamp domain. Durations use signed 64-bit
nanoseconds. Positive floating-point overflow in the exponential function explicitly
selects arm64 conversion semantics; only its signed-duration bound is portable
across Go architectures. The exponential function takes its whole power as Go's
`math.Pow` does (`src/math/pow.go:131-165 @ go1.26.2`); the source index cannot
check this citation because it refers to Go's source, not Temporal's.

Timeout names in policy validation follow
`enums/v1/workflow.go-helpers.pb.go:199-217 @ go.temporal.io/api v1.62.8`.
Duration conversion follows
`types/known/durationpb/duration.pb.go:171-187 @ google.golang.org/protobuf v1.36.10`:
seconds-product overflow remains flagged before nanos are added, even if the exact
sum would fit. Both module versions are pinned in the server's `go.mod`; the source
index cannot check these dependency citations. Validation preserves raw protobuf
duration fields unless capped; conversion to signed nanoseconds is a separate
observation.

`rules.json` records the ordered branches, their input domains and observable
outputs, and evidence for every branch. Ordinary rules await corpus calibration;
passing implementation tests do not calibrate them. `activity.retry-window` awaits
calibration; only `activity.retry-expiration-equality` covers the unrecordable exact
instant at which the next attempt starts at expiration. `activity.timeout-due` is
also source-only, scoped to exact clock boundaries and submillisecond precision.
Each source-only rule states its restricted domain and reason. The coverage API
maps absent and provisional rules to invariants-only. `compare_exact` reports the
first unequal or missing observation without dropping IDs or timestamps.

`source-index.json` pins the resolved tag commit and records each cited file's line
count and SHA-256, plus SHA-256 for every inclusive line range. Hashes include exact
newline bytes. No upstream implementation text is stored here. Regenerate after
changing citations. Check mode is the default:

```sh
cargo nextest run --offline --locked -p tokeira-behaviour-model --run-ignored ignored-only -E 'test(source_index_matches_tag)'
TOKEIRA_MODEL_SOURCE_WRITE=1 cargo nextest run --offline --locked -p tokeira-behaviour-model --run-ignored ignored-only -E 'test(source_index_matches_tag)'
```

The default checkout resolver reads linked-worktree metadata to find the sibling of
the main checkout. `TOKEIRA_MODEL_TEMPORAL_CHECKOUT=/path/to/temporal` selects another
read-only clone. Only `git show` and `git rev-parse` run there. The ignored Rust test
in `tests/source_index.rs` needs that local checkout; CI never reads Temporal's
source and instead checks the embedded catalog against the embedded index.
Keeping regeneration in an integration test also keeps filesystem access and the
workspace `sha2` development dependency outside the pure model. Check mode names
either JSON file whose generated contents differ; write mode regenerates both.
Hash validation establishes consistency with the generated index; semantic support
still requires reading the cited lines when a rule changes.

The evidence tests reject missing citations, invalid ranges, wrong tags or commits,
altered digests, duplicate keys, and forbidden transitive dependencies. Cargo's
resolved graph is checked using actual package identities, including development
and build edges; production incoming edges to this crate are rejected as well.
Only the upstream bindings and basic value/serialization libraries are permitted
as implementation dependencies. Model rules never import the kernel, runtime,
edge, storage, or compatibility policy.

```sh
cargo nextest run --offline --locked -p tokeira-behaviour-model
```
