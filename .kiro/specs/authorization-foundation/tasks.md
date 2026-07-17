# Implementation Plan: Authorization Foundation

## Overview

Deliver the configured auth layer per `requirements.md` / `design.md`: the `tokeira-auth` crate
(Claims/Role, multi-issuer JWT authenticator + JWKS providers, grants engine, AWS IAM presigned-
STS authenticator, default authorizer), the presence-enables `[policy.authorization]` config
section, edge enforcement parity (deny shape, deny-before-existence ordering, coverage closure),
token-namespace enforcement, Principal Attribution end-to-end (RequestContext → kernel stamp →
sidecar persistence → serializer field 303), cross-namespace command authorization, and the four
conformance-only override keys. Every upstream behaviour cites `v1.31.0`. Stock-default
behaviour must remain byte-identical throughout (Invariant I1).

## Tasks

- [x] 1. Configuration surface
  - [x] 1.1 Model `[policy.authorization]` in `crates/tokeira-config`: presence-enables (no
    `enabled` flag), `principal_attribution`/`expose_authorizer_errors`, `[[jwt.issuers]]`
    (name/issuer/jwks_uri/audience/refresh_interval/permissions_claim/grants), `[aws_iam]`
    (grants). Crate conventions (`deny_unknown_fields`, default fns, hand-written `Default`).
    - Validation → boot failure naming the field: missing/blank issuer fields, duplicate
      `name`/`issuer`, malformed `grant` strings (shared grammar, strict), empty/invalid match
      patterns (pinned grammar — Requirement 1.5), `aws_iam` with empty grants. Absent section =
      permissive; flags inert without a source.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 2. `tokeira-auth` crate (new)
  - [x] 2.1 `Role` bitmask + `Claims` + `AuthPrincipal` + `ClaimMapper`/`Authorizer` traits;
    stock default stays the permissive short-circuit (observably upstream's noop pair).
    - _Requirements: 2.1, 2.2, 2.3, 4.5_
  - [x] 2.2 `GrantRules` engine: `match_sub`/`match_arn` patterns (pinned grammar: full-string,
    case-sensitive, `*` the only metacharacter and it crosses separators, empty invalid) →
    `grant` list; grant strings parsed by the same `ns:role` parser as the permissions claim
    (strict at config validation); OR-union with claim-derived roles; matcher property-tested
    against the anchored-regex reference (`*` → `(?s:.*)`, dot-matches-newline).
    - _Requirements: 1.5, 3.5, 11.4_
  - [x] 2.3 `JwtAuthenticator`: bearer parse → `iss`-routed issuer profile (unmatched → deny) →
    JWT verify against the profile (allow-list, `kid`, signature, time claims, per-profile
    audience) → `sub` → permissions grammar (SplitN `:` 2, `temporal-system` pseudo-namespace,
    case rules, OR-accumulation, skip-with-warning) ∪ matched grants rules. Extras-header data
    accepted and ignored. Exact upstream error strings.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.8_
  - [x] 2.4 `JwksKeyProvider` (one per issuer profile): `reqwest` fetch, `kid` index,
    `RS256`/`ES256` allow-list, non-fatal initial failure, per-profile atomic-swap interval
    refresh with cross-profile isolation. New workspace deps `jsonwebtoken` + `quick-xml`
    (deny.toml review; xml is for task 2.6).
    - _Requirements: 3.2, 3.6_
  - [x] 2.5 `DefaultAuthorizer`: health-check pre-claims allow; nil-claims deny; system|namespace
    union; numeric `>=`; unknown-scope deny; `AuthPrincipal{type, name}` on role-check allow.
    - _Requirements: 4.1, 4.2, 4.3, 4.5, 2.4_
  - [x] 2.6 `StsAuthenticator`: `tokeira-aws-v1.` decode → pure URL validator (the full 11.2
    checklist: https / no userinfo / no fragment / 443-only / path `/`; host = commercial or
    GovCloud STS pattern; `Action=GetCallerIdentity` exactly once; `X-Amz-Algorithm`
    AWS4-HMAC-SHA256; credential scope `…/sts/aws4_request` region-consistent; `X-Amz-Date`
    valid, now within window, `X-Amz-Expires ≤ 900`; no duplicated security-critical params) →
    reconstructed request to the validated host (never the client URL; redirects disabled;
    200-only; bounded body; internal base-URL seam for tests) → `Arn` extraction →
    `Claims{subject: arn, auth_type: "aws-iam"}` ∪ grants. Digest-keyed allow-verdict cache,
    TTL ≤ presigned validity; denials uncached; fail-closed on STS errors.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

- [x] 3. Edge classification + pipeline
  - [x] 3.1 Extend `Action` with the missing per-RPC variants; implement
    `Action::classification()` total per `metadata.go:70-201 @ v1.31.0`; unit-test the
    non-negotiable rows (Nexus-endpoint five = cluster/Admin incl. Get/List;
    `ListSearchAttributes` ns/RO; namespace CRUD ns/Admin; polls Write; AdminService blanket).
    - _Requirements: 4.4_
  - [x] 3.2 Reshape `Authenticator` to claims-based; reorder `begin` to
    authenticate → authorize(namespace **name**) → resolve; replace `EdgeContext.principal` with
    `claims` + `auth_principal`; update the `start_batch_operation` identity fallback.
    Evaluation happens once at admission — no re-check while a long poll is parked.
    - _Requirements: 5.1, 5.6, 2.2_
  - [x] 3.3 Deny mapping: `EdgeError::PermissionDenied { reason }` →
    `PERMISSION_DENIED` `Request unauthorized.` + `PermissionDeniedFailure` detail (generalize
    `grpc/errors.rs:382-405` builder); claim-mapper errors always generic; authorizer errors
    verbatim iff `expose_authorizer_errors`.
    - _Requirements: 5.2, 5.3, 3.7_
  - [x] 3.4 Coverage closure: the six bridge-direct CHASM standalone-activity handlers
    (start/describe/poll/request-cancel/terminate/delete,
    `grpc/workflow_service.rs:2827-3181`; list/count already begin-guarded) + AdminService
    `DescribeMutableState` through `begin`. Fix the stale "Describe/poll/list/count stay
    deferred" comment at `grpc/workflow_service.rs:2820-2826` in passing.
    - _Requirements: 5.4_
  - [x] 3.5 Authorization metrics (latency timer + error/deny counters) at the `begin` seam.
    - _Requirements: 5.5_

- [x] 4. tokeirad wiring
  - [x] 4.1 Build the configured authenticator from `[policy.authorization]` at
    `apps/tokeirad/src/lib.rs:902` (replace `EdgeInterceptors::permissive`); validation
    failures fatal at boot
    (`cmd/server/main.go:197-221 @ v1.31.0` precedent). Absent `[policy.authorization]` (or no
    identity source configured) ⇒ byte-identical permissive behaviour.
    - _Requirements: 1.4, 1.2, 1.3_
  - [x] 4.2 Deployment documentation: the bearer-only/TLS-upstream stance and its operational
    consequence (bearer tokens — JWT and presigned-STS alike — MUST traverse an
    upstream-TLS-protected path); JWKS-over-HTTPS recommendation; presigned-URL possession
    semantics. Update `docs/readiness/configuration.md`'s surface enumeration + "genuine
    deployment knobs" with `[policy.authorization]`. When the canonical `config.example.toml`
    (tracked by that doc) is created, the exact-issuer-routing migration warning MUST sit beside
    `issuer`.
    - _Requirements: 3.2, 10.3, 11.7_

- [x] 5. Token-namespace identity + enforcement
  - [x] 5.1 Edge-owned `NamespacedTaskToken<T>` JSON envelope with
    `#[serde(default)] namespace_id: Option<NamespaceId>` (the **stable ID** — deletion renames
    but preserves it, `operator_service.rs:324-346`) and a flattened internal token for
    `WorkflowTaskToken`/`ActivityTaskToken` (`tokens.rs:24-60`); keep those runtime/kernel
    fencing structs unchanged. Populate the envelope at workflow/activity poll-response
    serialization. Keep `NexusTaskToken`'s existing exact v1.31.0 three-field protobuf shape,
    whose field 1 already carries the stable namespace (`nexus.rs:582-610`). Legacy
    workflow/activity JSON tokens decode to `None` → today's behaviour; fixture-test that decode.
    - _Requirements: 6.4_
  - [x] 5.2 Two-branch admission flow in the token-bearing handlers (today they authorize with
    `namespace = None` then decode — `workflow_service.rs:3437` pattern): empty request ns →
    decode + `get_by_id` resolution **before** authentication (malformed token →
    `INVALID_ARGUMENT` pre-auth) → authenticate → authorize resolved name; non-empty request
    ns → authenticate → authorize → resolve, then post-load check
    (`validate_task_token_namespace`, stable-ID compare) behind the
    `enableTokenNamespaceEnforcement` accessor (constant `true`); two present, disagreeing
    identities → `INVALID_ARGUMENT`
    `Operation requested with a token from a different namespace.`
    - _Requirements: 6.1, 6.2, 6.3_

- [x] 6. Principal Attribution
  - [x] 6.1 `EventPrincipal` in `tokeira-types`; `RequestContext.principal` beside
    `caller_identity`; thread from `EdgeContext` through the `to_internal.rs` builders and
    `workflow_service.rs` construction sites, gated by `principal_propagation_enabled(configured)`.
    - _Requirements: 7.1, 7.4_
  - [x] 6.2 Kernel stamp: `TransitionBuilder` holds the request principal;
    `Transition.event_principals` (new in-memory field, index-aligned with `history_events`,
    length-asserted at commit) filled by `emit`/`emit_at`; `buffer` stores the principal into
    `BufferedEvent.principal` (new trailing field on the postcard-persisted `WorkflowState` —
    pre-baseline shape change) so it survives restart; flush emits the buffered event's own
    principal (preservation for free); both-empty principal never stamped, half-empty stamped
    (`sub: ""` → `{jwt, ""}` regression). Kernel purity: no new kernel dependency, `apply`
    unchanged.
    - _Requirements: 7.2, 7.3_
  - [x] 6.3 Storage sidecar: nullable `principals_data BYTEA` **folded into
    `V005__history_batch.sql`** (pre-baseline rule: no ALTER; checksum changes), holding
    batch-aligned `postcard(Vec<Option<EventPrincipal>>)`; commit binds it atomically with
    `events_data` (`run_repository/commit.rs:496`); loads pair the decode
    (`run_repository/load.rs:139,235,296`; NULL → all-`None`; length mismatch → corruption
    error); repository read API returns the attributed pair; in-memory store mirrors the
    pairing. Old-shaped postcard fixtures pin decode behaviour for both the NULL-sidecar row
    and the pre-change `WorkflowState` blob (documented pre-baseline outcome).
    - _Requirements: 7.7_
  - [x] 6.4 Serializer: set `HistoryEvent.principal` (field 303) in `history_event_to_proto`;
    verify all three read paths (poll history, get-history, reverse).
    - _Requirements: 7.5_
  - [x] 6.5 Spoof-impossibility test: client-supplied `temporal-principal-*` metadata / crafted
    request fields cannot influence the stamp.
    - _Requirements: 7.6_

- [x] 7. Cross-namespace command authorization
  - [x] 7.1 In `RespondWorkflowTaskCompleted`: when gate on, dedupe
    `(target namespace, api)` over SignalExternal/StartChild/RequestCancelExternal and
    re-authorize each; deny fails the respond with the 5.2 shape. Gate = constant `false` +
    conformance override.
    - _Requirements: 8.1, 8.2_

- [x] 8. Conformance override keys (harness-only; never production)
  - [x] 8.1 Wire the four keys per the honesty rule — each key's `Wired` entry in
    `KEY_CLASSIFICATION` lands **inside the change that adds its consult-site accessor**: the
    expose-errors key with task 3.3, token-namespace with 5.2, propagation with 6.1,
    cross-namespace with 7.1 (config-fallback idiom for the two config-backed knobs; constants
    for the two behaviour gates). This task is the cross-cutting completion check that all four
    entries + accessors exist; document the namespace→global collapse on
    `frontend.enablePrincipalPropagation` at its entry.
    - _Requirements: 9.1, 9.2, 9.4_
  - [x] 8.2 Off-feature equivalence + lifecycle tests (registry set/clear/reset per key).
    - _Requirements: 9.3_

- [x] 9. Verification suite
  - [x] 9.1 Property tests: permissions grammar (P2), decision table incl. Worker quirk (P3),
    attribution lifecycle incl. buffering interleavings (P5), override lifecycle (P7), issuer
    routing over arbitrary issuer sets × tokens (P8), pure STS URL-validator over adversarial
    URL corpora (P9, no I/O), grants pattern matcher vs the anchored-regex reference (1.5).
    ≥100 iterations each.
    - _Requirements: 1.3, 1.5, 2.4, 3.2, 3.4, 3.5, 4.1-4.4, 7.1-7.5, 7.7, 9.1-9.3, 11.2, 11.3, 11.5_
  - [x] 9.2 Exact-string unit tests: every Error Handling row; JWKS rotation semantics; config
    validation failures (each Requirement 1.3 arm).
    - _Requirements: 3.1-3.3, 3.6, 5.2, 5.3, 6.1, 1.3, 11.3_
  - [x] 9.3 Integration: JWT end-to-end against a local JWKS fixture (allow/deny per role);
    principal read-back via `GetWorkflowExecutionHistory` (P5); begin-coverage regression with
    enforcement on and a grant-less caller (CHASM + AdminService paths deny); anonymous-caller
    health-check regression (`GetSystemInfo` + `Health/Check` succeed with no token under
    enforcement); multi-issuer routing (two fixtures; cross-issuer and absent `iss` deny);
    grants-rule mapping for an EKS-shaped token (no permissions claim); STS end-to-end against a
    local fixture via the internal base-URL seam (allow + `aws-iam` attribution; adversarial
    URLs deny with zero outbound requests; outage denies; cache honours expiry); the half-empty
    principal regression (`sub: ""` → `{jwt, ""}` stamped).
    - _Requirements: 1.2, 4.1, 5.1, 5.4, 7.1-7.5, 3.2, 3.5, 11.1-11.6_
  - [x] 9.4 Structural: kernel dependency-graph assertion extended (`tokeira-auth`,
    `tokeira-conformance` absent from kernel deps); the tokeira `AuthInfo`-equivalent carries no
    TLS peer material and `[policy.authorization]` rejects TLS-listener-shaped keys via
    `deny_unknown_fields`.
    - _Requirements: 10.1, 10.2_

- [ ] 10. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test --workspace` (campaign convention).
  - Run `cargo test --workspace --features conformance` equivalent for the gated crates.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["2.2", "2.4", "3.1"] },
    { "id": 2, "tasks": ["2.3", "2.5", "2.6", "3.2", "3.3"] },
    { "id": 3, "tasks": ["3.4", "3.5", "4.1", "4.2", "5.1", "6.1"] },
    { "id": 4, "tasks": ["5.2", "6.2", "6.3", "7.1", "8.1"] },
    { "id": 5, "tasks": ["6.4", "6.5", "8.2"] },
    { "id": 6, "tasks": ["9.1", "9.2", "9.3", "9.4"] },
    { "id": 7, "tasks": ["10"] }
  ]
}
```

## Notes

- **Stock-default invariance is the standing regression bar**: after every wave, a config with no
  identity source (section absent) serves byte-identical behaviour (no deny, no principal).
  Invariant I1 is not a final test — it gates each merge.
- The deny is `PERMISSION_DENIED` + `Request unauthorized.` **never `UNAUTHENTICATED`** — the
  existing `Unauthorized → unauthenticated` mapping must not be reached from the auth path
  (`interceptor.go:43-51 @ v1.31.0`).
- The persisted `HistoryEvent` postcard encoding is untouchable (positional; no field count) —
  attribution is sidecar-only. Any change to that plan needs a storage-evolution review first.
- The `conformance` feature is the Temporal-harness override shim only; it is never enabled for
  production. Production knobs: static TOML + the two constants.
- The STS authenticator's non-negotiables: the server **never fetches the client-supplied URL**
  (validate, then reconstruct against the static host pattern — SSRF containment, Property 9),
  fail-closed on STS errors, allow-verdicts cached no longer than the presigned validity.
- Upstream has no functional-corpus auth suites; this spec's bar is the tokeira-native suite in
  task 9 (unit parity with `interceptor_test.go` / `mutable_state_impl_test.go` semantics), plus
  the tokeira-native multi-issuer/STS coverage.
