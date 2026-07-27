# Requirements Document

## Introduction

This spec delivers the **configured layer** of the authentication/authorization conformance
decision ([`reference/v1.31.0-conformance-decision.md`](./reference/v1.31.0-conformance-decision.md)):
a real JWT authenticator (claim mapper + default authorizer + role model), the
`[policy.authorization]` static-config section (curated to the close-to-zero-configuration claim
of `docs/readiness/configuration.md`), the four related dynamic-config keys wired through the
conformance-only override registry, **Principal Attribution** end-to-end (edge → kernel event
stamp → history read-back), and the tokeira-native **AWS IAM authenticator** (presigned STS
`GetCallerIdentity` bearer, Requirement 11). The transport stance is fixed by the decision
record: **bearer-only at the edge, TLS terminated upstream** — no TLS listener configuration, no
mTLS-derived identity.

Every behaviour below is verified against the pinned Temporal source and cited inline
(`AGENTS §8`). The authoritative loci:

- `common/authorization/interceptor.go @ v1.31.0` — AuthInfo construction, deny path,
  `exposeAuthorizerErrors`, principal strip/propagate, cross-namespace command authorization.
- `common/authorization/default_jwt_claim_mapper.go @ v1.31.0` — token pipeline, permissions
  grammar, exact error strings.
- `common/authorization/default_token_key_provider.go @ v1.31.0` — JWKS fetch/rotation,
  algorithm allow-list.
- `common/authorization/default_authorizer.go`, `roles.go`, `frontend_api.go`,
  `common/api/metadata.go @ v1.31.0` — role model, decision procedure, per-RPC classification.
- `common/config/config.go:626-651 @ v1.31.0` — the static `Authorization` config surface.
- `common/dynamicconfig/constants.go @ v1.31.0` — the four dynamic-config keys and defaults.
- `common/headers/headers.go`, `service/history/workflow/mutable_state_impl.go:7105-7124
  @ v1.31.0` — principal headers, strip-then-propagate, event stamping semantics.
- `common/rpc/interceptor/namespace_validator.go @ v1.31.0` — token-namespace enforcement.

A fix whose justification is "it makes the test pass" rather than "v1.31.0 does X, verified at
`<path> @ v1.31.0`" is not acceptable (the Implementer mandate,
`temporal-functional-conformance/reference/FINDINGS.md`).

## Glossary

- **Claims:** the authenticated caller's identity + role grants — `Subject`, cluster-wide
  `System` role, per-namespace `Namespaces` roles, and `AuthType` (the authentication method,
  e.g. `"jwt"`) (`common/authorization/roles.go:25-36 @ v1.31.0`).
- **Role:** an `int16` bitmask — `Worker=1`, `Reader=2`, `Writer=4`, `Admin=8`, `Undefined=0`
  (`roles.go:3-14 @ v1.31.0`).
- **Claim mapper:** turns request auth material (bearer token) into `Claims`
  (`claim_mapper.go:29-31 @ v1.31.0`).
- **Authorizer:** decides Allow/Deny for `(Claims, CallTarget)` and, on Allow, computes the
  **Principal** (`authorizer.go:38-56 @ v1.31.0`).
- **Principal (proto):** `temporal.api.common.v1.Principal { type, name }` — the server-computed
  caller identity stamped on history events as `HistoryEvent.principal` (field 303,
  `proto/upstream/temporal/api/history/v1/message.proto:1174`). Distinct from the edge's current
  `Principal { subject, scopes }` struct, which this spec **replaces with `Claims`**.
- **Permissions claim:** the JWT array claim (default name `"permissions"`) of
  `namespace:role` grants; `temporal-system` is the pseudo-namespace for the System role
  (`default_jwt_claim_mapper.go:17-20,112-147 @ v1.31.0`).
- **Principal propagation:** the gate (`frontend.enablePrincipalPropagation`, namespace-scoped
  bool, default `false`) that lets the computed principal reach history stamping
  (`constants.go:848-853 @ v1.31.0`). There is **no** `system.enablePrincipalAttribution` key in
  v1.31.0 (decision-record correction).

## Target State

**Implemented for the decision-owned configured surface**: configured-path parity for the
**default** claim mapper + **default** authorizer + Principal Attribution, plus stock-default
parity preserved (the absent section stays permissive and stamps nothing), plus the
tokeira-native AWS IAM bearer (Requirement 11 — outside Temporal's surface, inside tokeira's).
Out of scope, per the decision record:

- mTLS-derived identity at the edge (bearer-only stance; upstream's shipped mapper ignores
  `TLSSubject` anyway — `default_jwt_claim_mapper.go:78-82 @ v1.31.0`).
- Custom `ClaimMapper`/`Authorizer`/`JWTAudienceMapper` plugins (Go server options); tokeira's
  extension seam remains the edge `Authenticator` trait. Static `audience` validation **is** in
  scope.
- Nexus-dispatch principal propagation (upstream discards it —
  `service/frontend/nexus_handler.go:161 @ v1.31.0`).
- Multi-cluster/standby stamping semantics (tokeira has no replication; the active-only rule is
  trivially satisfied).

### Conformance scope (set expectations)

Upstream v1.31.0 has **no functional-corpus suites** for the authorizer or principal attribution —
coverage there is unit-level (`common/authorization/interceptor_test.go`,
`service/history/workflow/mutable_state_impl_test.go:6154-6290 @ v1.31.0`). Executing this spec
therefore clears no corpus tier; its verification bar is tokeira-native tests mirroring the
upstream unit semantics (exact error strings, decision table, stamping rules) plus an end-to-end
JWT integration test. The four dynamic-config keys are wired so any future corpus leaf that flips
them is honoured rather than skipped (honesty boundary, `conformance-config-override`).

## Deliberate deviations from v1.31.0 internal topology (observable contract preserved)

1. **Single-process collapse of principal propagation.** Upstream propagates the principal
   frontend→history as gRPC metadata (`temporal-principal-type`/`-name`,
   `common/headers/headers.go:24-25 @ v1.31.0`) because history is a separate role. Tokeira has
   no internode hop: the principal rides the internal `RequestContext` from edge to kernel. The
   observable contract — which events carry which principal — MUST match upstream exactly.
   The spoof-defense obligation (upstream: unconditional `StripPrincipal`,
   `interceptor.go:153-155`) is met structurally: tokeira's typed internal DTOs carry no
   client-writable principal field, so there is nothing to strip; this MUST be asserted by test.
2. **Static TOML instead of dynamic config for deployment knobs.** Upstream gates propagation and
   error exposure via dynamic config; tokeira has no production dynamic config, so these become
   `[policy.authorization]` TOML fields with the same defaults, overridable at runtime only in
   conformance builds (Requirement 9). Upstream's namespace-scoped granularity for
   `enablePrincipalPropagation` collapses to a global bool (the conformance registry is
   global-only by prior decision — `conformance-config-override` design non-goal); revisit only
   if a corpus leaf ever needs per-namespace granularity.
3. **Config shape is tokeira-native, not Temporal-spelled.** Temporal's `global.authorization`
   YAML (two plugin selectors, one merged key provider, one global audience/claim grammar,
   renameable headers, `permissionsRegex`) is collapsed into issuer profiles under
   `[policy.authorization]` (below). What is preserved is **behaviour**: a deployment configured
   with one issuer, the default `permissions` claim, and no grants rules observes exactly the
   v1.31.0 `authorizer: "default"` / `claimMapper: "default"` behaviour — same token pipeline,
   same grammar, same deny shape, same attribution. What is deliberately dropped: the
   noop/default selector matrix (its mixed combos have no operational value and existed for Go
   plugin injection), header-name overrides (every SDK sends `authorization`), and
   `permissionsRegex` (per-issuer grants rules subsume its use cases). One strictness beyond
   upstream, required for multi-issuer: tokens are routed to an issuer profile by their `iss`
   claim and MUST match a configured issuer (upstream's single merged keyring never validates
   `iss`) — a deliberate, documented improvement.
4. **The AWS IAM bearer (Requirement 11) is a tokeira-native extension**, not part of the
   v1.31.0 surface — a deployment using it is outside Temporal-config equivalence, while the JWT
   path's parity is unaffected. It is recorded in the decision record as tokeira product surface,
   not in `supported.md`.

## Configuration Surface (raise, never hardcode — Implementer mandate rule 3)

New `[policy.authorization]` section (`crates/tokeira-config`), curated to the
close-to-zero-configuration claim (`docs/readiness/configuration.md`): **presence-enables** —
the section absent is the stock-default permissive server; configuring an identity source *is*
the enforcement switch. No `enabled` boolean, no selector strings, zero required fields when the
section is absent; the empty `tokeirad.toml` stays valid.

```toml
[policy.authorization]
principal_attribution = false        # stamp HistoryEvent.principal (field 303)
expose_authorizer_errors = false     # v1.31.0 exposeAuthorizerErrors semantics

[[policy.authorization.jwt.issuers]] # zero or more; ≥1 issuer or aws_iam ⇒ enforcement on
name = "eks-prod"                    # unique; logs/metrics label
issuer = "https://oidc.eks.eu-west-1.amazonaws.com/id/ABC"   # matched against token `iss`
jwks_uri = "https://oidc.eks.eu-west-1.amazonaws.com/id/ABC/keys"
audience = "tokeira"                 # optional; empty = check skipped (upstream semantics)
refresh_interval = "1m"              # optional; absent = fetch once, no background refresh
permissions_claim = "permissions"    # optional; the v1.31.0 ns:role grammar, verbatim
[[policy.authorization.jwt.issuers.grants]]
match_sub = "system:serviceaccount:prod:*"   # glob on the token subject
grant = ["prod:worker", "prod:write"]        # SAME ns:role grammar as the permissions claim

[policy.authorization.aws_iam]       # presence enables the STS bearer (Requirement 11)
[[policy.authorization.aws_iam.grants]]
match_arn = "arn:aws:sts::123456789012:assumed-role/tokeira-worker-*"
grant = ["prod:worker", "prod:write"]
```

> **Migration warning — exact issuer routing.** `issuer` is not a profile label. It must exactly
> and case-sensitively match the IdP token's `iss` claim, including scheme, path, and
> trailing-slash form. Unlike Temporal v1.31.0's merged keyring, tokeira rejects missing or
> unmatched issuers; a mismatch causes every JWT from that IdP to be denied as
> `Request unauthorized.` This warning belongs beside `issuer` in the canonical
> `config.example.toml` when it is created (`docs/readiness/configuration.md` tracks it).

| Field | Default | Invalid input (boot) | Runtime side-effect when set | Semantics source |
|---|---|---|---|---|
| `principal_attribution` | `false` | non-bool → parse error | events gain durable field-303 attribution (`principals_data` writes begin); inert with no identity source | `frontend.enablePrincipalPropagation`, `constants.go:848-853 @ v1.31.0` (tokeira-named; the conformance key keeps the upstream name) |
| `expose_authorizer_errors` | `false` | non-bool → parse error | authorizer *errors* (never claim-mapper errors or denials) reach callers verbatim; inert with no identity source | `frontend.exposeAuthorizerErrors`, `constants.go:842-847 @ v1.31.0` |
| `jwt.issuers[].name` / `.issuer` / `.jwks_uri` | required per issuer | missing/blank/duplicate → `ValidationError::Field` | first issuer turns enforcement on for every RPC | issuer routing is tokeira strictness (Deviation 3); JWKS mechanics `default_token_key_provider.go @ v1.31.0` |
| `jwt.issuers[].audience` | `""` (check skipped) | — (any string) | non-blank adds a required exact `aud` match for that profile | `default_jwt_claim_mapper.go:197 @ v1.31.0` |
| `jwt.issuers[].refresh_interval` | none (fetch once) | unparseable duration → parse error | background JWKS refresh task per profile | zero-value semantics, `default_token_key_provider.go:49 @ v1.31.0` |
| `jwt.issuers[].permissions_claim` | `"permissions"` | — (blank ⇒ the default, upstream parity: `default_jwt_claim_mapper.go:38-41 @ v1.31.0`) | reads a different claim name for that profile | `default_jwt_claim_mapper.go:17,38-41 @ v1.31.0` |
| `*.grants[]` (`match_sub` / `match_arn` → `grant`) | `[]` | empty pattern, or `grant` failing the `ns:role` grammar → `ValidationError::Field` (strict — Requirement 1.5) | matched identities gain the granted roles (union); no persistence — evaluated per request | tokeira-native; grammar Requirements 1.5 + 3.4 |
| `aws_iam` (table presence) | absent | present with empty `grants` → `ValidationError::Field` | enables the STS bearer path (outbound STS dependency on auth path) | Requirement 11 |

Header names are **constants** (`authorization`, `authorization-extras` —
`interceptor.go:46-47 @ v1.31.0`), not config. Behaviour constants (config-as-constant;
conformance-overridable only): `frontend.enableTokenNamespaceEnforcement` = `true`
(`constants.go:832-836 @ v1.31.0`), `system.enableCrossNamespaceCommands` = `false`
(`constants.go:148-152 @ v1.31.0`).

## Requirements

### Requirement 1: Static configuration surface

**User Story:** As an operator, I want enforcement to follow from declaring my identity sources —
nothing more — so that tokeira's close-to-zero-configuration claim holds for auth too.

#### Acceptance Criteria

1. THE config crate SHALL model the `[policy.authorization]` section above with those defaults,
   following crate conventions (`deny_unknown_fields`, per-field `default =` fns, hand-written
   `Default`, validation in `TokeiraConfig::validate()`).
2. WHEN the section is absent (or present with no `jwt.issuers` entry and no `aws_iam` table),
   THE served behaviour SHALL be byte-identical to today's permissive edge: every call allowed,
   no principal on any event — enforcement is active iff at least one identity source is
   configured.
3. VALIDATION SHALL fail startup, naming the offending field, when: an issuer omits `name`,
   `issuer`, or `jwks_uri`, or a blank value is supplied; two issuers share a `name` or an
   `issuer` URL; a `grant` string does not parse under the `ns:role` grammar (Requirement 3.4);
   a `match_sub`/`match_arn` glob is empty; or `aws_iam` is present with an empty `grants` list
   (an STS source that can never grant anything is a misconfiguration, surfaced at boot — the
   deliberate-early-surface principle; upstream reaches equivalent dead ends lazily, cf.
   `HasSourceURIsConfigured`, `config.go:714-724 @ v1.31.0`).
4. WHEN `principal_attribution` or `expose_authorizer_errors` is set with no identity source
   configured, THE config SHALL be accepted (the flags are inert until a source exists) — the
   section's presence alone never changes served behaviour.
5. THE `match_sub`/`match_arn` pattern grammar SHALL be exactly: **full-string**, case-sensitive
   match; `*` is the only metacharacter, matching any (possibly empty) sequence of characters
   **including** separators and newlines (`:`, `/`, `-` are not special — a `*` crosses them,
   so patterns must be written anchored, e.g.
   `arn:aws:sts::123456789012:assumed-role/tokeira-worker-*`); no other metacharacters (`?`,
   classes, alternation) exist; the empty pattern is invalid (boot error per 1.3). The matcher
   is property-tested against a reference implementation (the pattern compiled to an anchored
   regex with `*` → `(?s:.*)` — dot-matches-newline, honouring the any-characters contract —
   and all else escaped; task 2.2), preventing accidental substring or case-insensitive
   over-granting.

### Requirement 2: Claims and role model

**User Story:** As a security engineer, I want tokeira's role semantics to be exactly Temporal's,
so that a permissions claim authored for Temporal grants identical access on tokeira.

#### Acceptance Criteria

1. THE auth model SHALL define `Role` as a bitmask with `Worker=1`, `Reader=2`, `Writer=4`,
   `Admin=8`, `Undefined=0`, and validity = no bits outside that set
   (`roles.go:3-21 @ v1.31.0`).
2. THE auth model SHALL define `Claims { subject, system: Role, namespaces: Map<String, Role>,
   auth_type }` (`roles.go:25-36 @ v1.31.0`), replacing the edge's current
   `Principal { subject, scopes }` (the name `Principal` is reserved for the proto
   `Principal{type,name}`).
3. WHEN no identity source is configured (Requirement 1.2), THE effective behaviour SHALL match
   upstream's stock noop pair — every caller treated as `Claims { system: Admin }` with empty
   subject/auth_type, consulted even with no auth material, allowed with no principal
   (`claim_mapper.go:52-59`, `noop_authorizer.go:12-14 @ v1.31.0`) — whether implemented as
   literal claims or as the existing permissive short-circuit; the two are observably identical.
4. THE role-requirement comparison SHALL be numeric `>=` on the bitmask (Reader 2 < Writer 4 <
   Admin 8), preserving the upstream quirk that `Worker=1` alone satisfies **no** access level
   under the default authorizer (`default_authorizer.go:59,68-77 @ v1.31.0`) — mirrored
   faithfully, documented as such.

### Requirement 3: JWT authentication (default claim mapper + JWKS key provider)

**User Story:** As a platform team, I want tokeira to validate the same bearer JWTs Temporal
validates, with the same failure messages, so that SDK auth suppliers and issuer infrastructure
work unmodified.

#### Acceptance Criteria

1. WHEN at least one JWT issuer is configured, THE edge SHALL read the raw value of the
   `authorization` header (constant name) from request metadata and process it as: empty →
   `Claims { auth_type: "jwt" }` with no subject/roles and **no error**; not exactly two
   space-separated parts → deny with message `unexpected authorization token format`; scheme not
   case-insensitively `bearer` → deny with `unexpected name in authorization token`
   (`default_jwt_claim_mapper.go:76-92 @ v1.31.0`). A bearer value carrying the
   `tokeira-aws-v1.` prefix routes to the AWS IAM authenticator instead (Requirement 11).
   (Upstream short-circuits header-less requests before the mapper — `GetAuthInfo` returns nil
   when there is no auth header and no TLS subject, so claims stay nil and the authorizer's
   nil-claims deny fires, `interceptor.go:204-212 @ v1.31.0`. Tokeira folds this into the
   mapper's empty-token branch: role-less jwt-typed claims and nil claims deny identically
   everywhere except the health-check set, so the collapse is observably equivalent.)
2. THE token verification SHALL proceed as, in order (any failure → deny):
   - route to an issuer profile by exact match of the `iss` claim against configured `issuer`
     values; absent or unmatched `iss` denies generically (tokeira strictness beyond upstream's
     merged keyring — Deviation 3);
   - enforce an algorithm allow-list of exactly `RS256` and `ES256`;
   - require a `kid` header (missing → error `malformed token - no "kid" header`) and resolve
     the key from that profile's JWKS set by `kid`;
   - verify signature and time-based claims;
   - only when the profile's `audience` is configured non-blank, require an exact audience
     match (mismatch → `audience mismatch`)
   (`default_jwt_claim_mapper.go:151-199`, `default_token_key_provider.go:90-92 @ v1.31.0`).
   Upstream routes `config.Audience` through an always-installed static audience mapper
   (`audience_mapper.go:21-28`, `cmd/server/main.go:217 @ v1.31.0`); tokeira passes the profile
   string directly — observably identical; custom audience mappers are out of scope per the
   decision record.
3. THE subject SHALL come from the `sub` claim, with a missing or non-string `sub` denying with
   `unexpected value type of "sub" claim` (`default_jwt_claim_mapper.go:19,97-100 @ v1.31.0`).
4. THE permissions grammar SHALL match upstream exactly: claim named by the profile's
   `permissions_claim`; absent or non-array claim silently contributes nothing; each entry split
   `SplitN(":", 2)` into `namespace:role`; role matched case-insensitively to `read`→Reader,
   `write`→Writer, `admin`→Admin, `worker`→Worker, else `Undefined`; namespace matched
   case-sensitively; `temporal-system` (exact) ORs into the System role; repeated grants
   OR-accumulate; malformed entries (non-string, no `:`) are skipped with a warning, never an
   error (`default_jwt_claim_mapper.go:102-147,203-215 @ v1.31.0`; case tests
   `default_jwt_claim_mapper_test.go:142-161 @ v1.31.0`). This grammar is THE single role
   language: `grants[].grant` strings (Requirement 3.5, Requirement 11) parse with the same
   parser — but strict at **config-validation** time (a malformed grant is a boot error per
   Requirement 1.3, not a runtime skip; the operator wrote it, so it must be right).
5. FOR each of the matched profile's `grants` rules whose `match_sub` pattern (grammar per
   Requirement 1.5) matches the token's subject, THE mapper SHALL OR the rule's `grant` entries into the claims exactly as if they
   had appeared in the permissions claim (union with claim-derived roles). This replaces
   upstream's `permissionsRegex` escape hatch (`config.go:630-632 @ v1.31.0` — collapsed per
   Deviation 3): identity providers that cannot mint a permissions claim (EKS projected
   service-account tokens, Cognito access tokens) get their role mapping here instead.
6. EACH issuer profile SHALL own an independent JWKS provider for its single required
   `jwks_uri`, with these behaviours (mechanics per
   `default_token_key_provider.go:43-53,110-166 @ v1.31.0`, re-scoped from upstream's one
   multi-URI provider to per-profile providers):
   - index RSA/ECDSA public keys by `kid`;
   - log-and-continue on initial-fetch failure (the profile serves an empty key set — every
     token then fails key lookup, upstream behaviour);
   - when the profile's `refresh_interval` is set, refresh on that interval with an **atomic
     swap**: a failed fetch keeps the profile's previous key set;
   - profiles are isolated — one profile's refresh failure never affects another profile's keys;
   - the configured URI scheme is accepted as-is (upstream enforces none — plain `http.Get`,
     `default_token_key_provider.go:141 @ v1.31.0`), with the docs recommending HTTPS.
7. All claim-mapper failures above SHALL surface to the caller only as the generic deny of
   Requirement 5.3 — upstream logs the specific error and returns the generic one regardless of
   `expose_authorizer_errors` (`interceptor.go:141-149 @ v1.31.0`).
8. THE edge SHALL read the raw value of the `authorization-extras` header (constant name) and
   pass it to the authenticator as extra data (`interceptor.go:197,218 @ v1.31.0`), which the
   JWT and AWS IAM authenticators ignore (upstream's default mapper consumes only the token and
   audience — `default_jwt_claim_mapper.go:76-93 @ v1.31.0`). It exists for custom
   `Authenticator` implementations.

### Requirement 4: Default authorizer and per-RPC classification

**User Story:** As an operator, I want per-RPC allow/deny decisions identical to Temporal's, so
that a role that can(not) do something on Temporal can(not) do it on tokeira.

#### Acceptance Criteria

1. THE authorizer SHALL allow the health-check set — `grpc.health.v1.Health/Check` and
   `WorkflowService/GetSystemInfo` — **before** the claims-nil check (anonymous callers pass)
   (`frontend_api.go:8-28`, `default_authorizer.go:37-43 @ v1.31.0`).
2. WHEN claims are absent for any non-health-check API, THE authorizer SHALL deny
   (`default_authorizer.go:41-43 @ v1.31.0`).
3. THE decision SHALL be: look up the target RPC's `(Scope, Access)` classification; for
   namespace-scoped APIs the effective role is `claims.system | claims.namespaces[ns]` (missing
   entry = 0); for cluster-scoped APIs it is `claims.system`; unknown scope denies; allow iff
   effective role `>=` required role (ReadOnly→Reader, Write→Writer, Admin and unknown→Admin)
   (`default_authorizer.go:45-77 @ v1.31.0`).
4. THE edge's `Action`-level classification SHALL be per-RPC faithful to
   `common/api/metadata.go:70-201 @ v1.31.0`, splitting or adding `Action` variants where today's
   granularity conflates distinct upstream classes. Non-negotiable rows: all five OperatorService
   Nexus-endpoint RPCs are cluster/**Admin** (including Get/List); `ListSearchAttributes` is
   namespace/ReadOnly while Add/Remove are namespace/Admin; `Register/Update/DeprecateNamespace`
   are namespace/Admin; `ListNamespaces`, `GetSearchAttributes`, `GetClusterInfo`, `GetSystemInfo`
   are cluster/ReadOnly; every task-queue poll and task respond (`PollWorkflowTaskQueue`,
   `PollActivityTaskQueue`, `PollNexusTaskQueue`, all `Respond*`) is namespace/Write — while the
   non-task-queue polls `PollActivityExecution` (`metadata.go:94`) and
   `PollWorkflowExecutionUpdate` (`metadata.go:139`) are namespace/**ReadOnly**;
   remote-cluster registry RPCs are cluster/Admin; the entire AdminService surface is
   cluster/Admin blanket (`metadata.go:181-194,206-219 @ v1.31.0`).
5. ON an allow decided by the role comparison, THE authorizer SHALL produce
   `Principal { type: claims.auth_type, name: claims.subject }`; the health-check pre-claims
   allow produces **none** (`default_authorizer.go:22,37-40 @ v1.31.0`), and the no-op authorizer
   produces none (`default_authorizer.go:59-62`, `noop_authorizer.go:12-14 @ v1.31.0`).

### Requirement 5: Edge enforcement pipeline (interceptor parity)

**User Story:** As an SDK user, I want auth failures to look exactly like Temporal's, so that
client error handling and retry classification behave identically.

#### Acceptance Criteria

1. THE `begin` pipeline SHALL authorize against the **namespace name** before resolving namespace
   existence, so an unauthorized caller cannot distinguish existing from non-existing namespaces
   (upstream chain: authorization at position 8, namespace state validation at 13 —
   `service/frontend/fx.go:256-290 @ v1.31.0`; today's `begin` resolves first,
   `crates/tokeira-edge/src/interceptors.rs:260-296`).
2. EVERY authn/authz failure SHALL map to gRPC `PERMISSION_DENIED` with message
   `Request unauthorized.` and a `temporal.api.errordetails.v1.PermissionDeniedFailure` detail —
   never `UNAUTHENTICATED` (upstream `errUnauthorized`, `interceptor.go:43-51 @ v1.31.0`;
   detail-builder precedent `crates/tokeira-edge/src/grpc/errors.rs:382-405`) — where the
   detail's `reason` is empty except when the authorizer denies with a non-empty `Reason`, which
   the detail carries while the message stays `Request unauthorized.`
   (`interceptor.go:260-263 @ v1.31.0`).
3. THE caller SHALL receive, on an auth-component failure (as opposed to a decision):
   - for a claim-mapper error: the generic deny, regardless of `expose_authorizer_errors`
     (Requirement 3.7);
   - for an **authorizer** error: the authorizer's error verbatim iff
     `expose_authorizer_errors` is true, else the generic deny
     (`interceptor.go:250-257 @ v1.31.0`).
4. THE begin chokepoint SHALL cover the currently-bypassing public surfaces:
   - the six standalone-activity CHASM handlers that go bridge-direct — `start`/`describe`/
     `poll`/`request_cancel`/`terminate`/`delete_activity_execution`
     (`crates/tokeira-edge/src/grpc/workflow_service.rs:2827-3181`; `list`/`count` already
     delegate to the begin-guarded domain layer) — calling `begin` with their per-RPC actions
     (classification per `metadata.go:91-98 @ v1.31.0`);
   - AdminService `DescribeMutableState` (`crates/tokeira-edge/src/grpc/admin_service.rs:39-77`)
     calling `begin` under the cluster/Admin blanket.
   The Nexus completion callback listener keeps its token-based auth (out of scope here; owned
   by the Nexus specs).
5. THE authorization metrics SHALL be emitted: an authorization latency timer on every call, a
   counter for authorizer errors, a counter for denials
   (`service_authorization_latency`, `service_errors_authorize_failed`,
   `service_errors_unauthorized` — `interceptor.go:247-259`,
   `common/metrics/metric_defs.go:666-672 @ v1.31.0`), following tokeira's edge metrics
   conventions.
6. AUTHENTICATION and authorization SHALL be evaluated once at request admission; token
   time-based claims are not re-checked while a long poll is parked (upstream runs the
   interceptor once per RPC — `interceptor.go:126-182 @ v1.31.0`; a JWT expiring mid-poll does
   not abort the poll).

### Requirement 6: Token-namespace enforcement

**User Story:** As a security engineer, I want a task token minted for one namespace to be
unusable against another, so that a leaked token's blast radius is bounded.

#### Acceptance Criteria

1. WHEN a request carries both a namespace and a task token whose namespace resolves differently,
   THE edge SHALL reject with `INVALID_ARGUMENT` and message
   `Operation requested with a token from a different namespace.`
   (`namespace_validator.go:38,348-357 @ v1.31.0`), with the token's namespace taking priority
   for handler execution when they match (`namespace_validator.go:226-250 @ v1.31.0`).
2. THE gate SHALL be the behaviour constant `frontend.enableTokenNamespaceEnforcement = true`,
   overridable only through the conformance registry (Requirement 9)
   (`constants.go:832-836 @ v1.31.0` — default `true`).
3. WHEN a token-bearing request (`Respond*`, `RecordActivityTaskHeartbeat`) carries an **empty**
   namespace and a decodable task token, THE edge SHALL back-fill the namespace from the token
   **before** authorization, so the call is authorized and executed against the token's namespace
   (upstream's namespace validator back-fills at interceptor position 5, ahead of authorization
   at 8 — `namespace_validator.go:104-148`, `service/frontend/fx.go:256-290 @ v1.31.0`).
   Without this, namespace-scoped roles cannot authorize SDK requests that omit `namespace`.
4. AS the prerequisite for 6.1–6.3 (Tokeira's workflow/activity wire tokens do not carry a
   namespace today — `crates/tokeira-types/src/tokens.rs:24-60` — while upstream tokens carry
   `NamespaceId`, `common/tasktoken/token.go:21,47 @ v1.31.0`), THE edge SHALL serialize
   `WorkflowTaskToken` and `ActivityTaskToken` inside a self-describing JSON envelope with
   `#[serde(default)] namespace_id: Option<NamespaceId>` and a flattened internal fencing token.
   The namespace identity is edge wire metadata and SHALL NOT be added to the runtime/kernel
   token structs: those structs fence authoritative transitions and do not own public admission.
   `NexusTaskToken` already carries the required stable namespace ID as field 1 of the exact
   v1.31.0 three-field protobuf token (`crates/tokeira-runtime/src/nexus.rs:582-610`) and SHALL
   retain that wire shape rather than being wrapped in JSON. The identity is the **stable ID,
   not the name**: namespace deletion
   atomically renames the record while preserving its ID
   (`crates/tokeira-edge/src/operator_service.rs:324-346`), and the cache resolves the stable ID
   across that rename (`NamespaceCache::get_by_id`,
   `crates/tokeira-edge/src/namespace_cache.rs:54-62`), so a name-carrying token would dangle.
   Further:
   - **issuance**: the edge populates the workflow/activity envelope whenever it serializes a
     poll response; Nexus issuance continues populating protobuf field 1;
   - **decode/legacy**: workflow/activity tokens are serde_json (self-describing), so the
     envelope field is `#[serde(default)]` and the internal fields are flattened — a pre-change
     in-flight token decodes with the identity absent and receives today's behaviour (no
     back-fill; the existing post-load check only). Nexus has no legacy namespace-less shape;
   - **admission flow (two branches, preserving upstream error precedence)**:
     - *empty request namespace*: decode the token and resolve its `namespace_id` to the current
       name via `NamespaceCache::get_by_id` **before** authentication/authorization (upstream's
       validator back-fills at position 5, ahead of the auth interceptor at 8 —
       `namespace_validator.go:104-148`, `fx.go:256-290 @ v1.31.0`; a malformed token on this
       branch errors `INVALID_ARGUMENT` pre-auth, matching that precedence), then authenticate →
       authorize against the resolved name → resolve namespace state;
     - *non-empty request namespace*: authenticate and authorize **that name first**; token
       decode and mismatch validation follow post-authorization (upstream's mismatch check runs
       in `StateValidationIntercept` at position 13, after auth at 8 — an *unauthorized* caller
       with disagreeing namespaces receives `PERMISSION_DENIED`, never the mismatch error);
   - **mismatch**: when both identities are present and disagree, the 6.1 rejection applies —
     compared by stable ID, mirroring upstream's registry-id comparison
     (`namespace_validator.go:348-357 @ v1.31.0`).

### Requirement 7: Principal Attribution

**User Story:** As an auditor, I want every history event to carry the authenticated identity
that caused it, so that history is trustworthy attribution, not client-asserted identity.

#### Acceptance Criteria

1. WHEN `principal_attribution` is true (statically or via the conformance override of
   `frontend.enablePrincipalPropagation`) AND the authorizer produced a principal, THE principal
   SHALL travel on the internal request context (alongside, not replacing, the client-supplied
   `caller_identity` — `crates/tokeira-types/src/request.rs:27-39`) into kernel transaction
   processing.
2. THE kernel SHALL stamp the transaction's principal onto every history event it emits whose
   principal is not already set — with events buffered under an outstanding workflow task
   stamped **at buffer time**, so a later flush by a different caller preserves the original
   (`mutable_state_impl.go:7105-7124 @ v1.31.0`; preservation semantics
   `mutable_state_impl_test.go:6207-6290 @ v1.31.0`). Worker-initiated transactions
   (e.g. `RespondWorkflowTaskCompleted`) stamp the **worker's** principal on the events they emit.
3. A principal SHALL be suppressed only when **both** `type` and `name` are empty — upstream's
   header round-trip drops empty values individually but `GetPrincipal` returns nil only when
   both are empty (`headers.go:148-152,162 @ v1.31.0`). A half-empty principal IS stamped: an
   allowed JWT with `sub: ""` yields `Principal{type: "jwt", name: ""}` on events (regression
   test pinning this exact case: task 9).
4. WHEN attribution is off, or no identity source is configured, THE emitted events SHALL carry
   no principal (proto field 303 absent) — the stock-default baseline.
5. THE history read-back SHALL emit `HistoryEvent.principal` (field 303) from the stored
   attribution on every read path (`GetWorkflowExecutionHistory(Reverse)`, poll-response history)
   — the serializer currently leaves it unset
   (`crates/tokeira-edge/src/translate/history_serializer.rs:104-116`).
6. CLIENT-supplied principal injection SHALL be impossible by construction — no internal request
   DTO exposes a principal field writable from proto requests — asserted by a test that a
   request carrying `temporal-principal-*`-style metadata or crafted fields cannot influence the
   stamp (upstream strips headers unconditionally, `interceptor.go:153-155 @ v1.31.0`; tokeira's
   structural equivalent per Deviation 1).
7. THE persisted representation SHALL NOT break decode of existing history rows: history batches
   are positional postcard blobs (`crates/tokeira-storage/src/dsql/codec.rs:48-56`), so
   attribution is persisted alongside — not inside — the existing `HistoryEvent` encoding,
   following the decode-only-legacy discipline
   (`crates/tokeira-kernel/src/event.rs:703-714` precedent). Old rows read back as
   principal-absent. (The buffered-event carrier — `BufferedEvent.principal` inside the
   postcard-persisted `WorkflowState` — is a **pre-baseline** shape change executed under the
   storage build-phase rules, with old-shaped fixtures pinning the documented decode outcome;
   the design's Durable-representation section owns the exact mechanics.)

### Requirement 8: Cross-namespace command authorization

**User Story:** As a security engineer, I want workflow commands that reach into other namespaces
to be authorized against those namespaces, so that a foothold in one namespace does not pivot.

#### Acceptance Criteria

1. THE gate SHALL be the behaviour constant `system.enableCrossNamespaceCommands = false`
   (`constants.go:148-152 @ v1.31.0`), overridable only through the conformance registry.
2. WHEN enabled, on `RespondWorkflowTaskCompleted`, THE edge SHALL re-authorize each
   `SignalExternal` / `StartChild` / `RequestCancelExternal` command whose target namespace is
   non-empty and differs from the source, against the corresponding API classification
   (`SignalWorkflowExecution` / `StartWorkflowExecution` / `RequestCancelWorkflowExecution`),
   deduplicating by `(target namespace, API)`; any deny fails the respond call with the
   Requirement 5.2 shape (`interceptor.go:287-353 @ v1.31.0`).

### Requirement 9: Conformance dynamic-config wiring

**User Story:** As a conformance engineer, I want the Temporal harness to flip the four auth keys
on a live tokeirad, so future corpus leaves governed by them run instead of skipping.

> **Scope guard.** The `conformance` feature exists solely to support the Temporal test harness's
> `OverrideDynamicConfig`; it is never enabled in a production build. Nothing in this requirement
> adds production-runtime configurability: production behaviour is fixed by the static
> `[policy.authorization]` TOML (Requirement 1) and the two behaviour constants (Requirements 6.2, 8.1).

#### Acceptance Criteria

1. THE four keys SHALL be added to `KEY_CLASSIFICATION` as `Wired` **in the same change** as
   their consult sites (honesty rule, `crates/tokeira-conformance/src/lib.rs:124-131`):
   `frontend.enablePrincipalPropagation` (Bool), `frontend.exposeAuthorizerErrors` (Bool),
   `frontend.enableTokenNamespaceEnforcement` (Bool), `system.enableCrossNamespaceCommands`
   (Bool).
2. EACH consult site SHALL follow the twin-accessor idiom: off-feature it evaluates to the static
   config value (for the two config-backed knobs) or the pinned constant (for the two behaviour
   constants); on-feature it consults the registry falling back to that same value (precedent:
   the accessor-with-configured-fallback at `crates/tokeira-edge/src/chasm_activity.rs:67-104`).
3. IN a build without the `conformance` feature, THE artifact SHALL contain no override surface
   for these keys and behave identically to the static configuration (Property 1 of
   `conformance-config-override`).
4. THE upstream namespace scoping of `frontend.enablePrincipalPropagation` SHALL be documented at
   the classification entry as collapsed-to-global (registry limitation; Deviation 2), along with
   the name mapping to the static field `principal_attribution` (the registry keeps upstream key
   names; the TOML uses tokeira's).

### Requirement 10: Transport stance (bearer-only at the edge)

**User Story:** As a deployment owner, I want the auth surface to stay transport-independent, so
that TLS/mTLS remain load-balancer concerns and tokeirad stays a plain-listener binary.

#### Acceptance Criteria

1. THE authenticator SHALL consume only request metadata (headers), reading or modeling no TLS
   peer material (tokeira's `AuthInfo` equivalent carries no `TLSSubject`/`TLSConnection`).
2. THE `[policy.authorization]` section SHALL add no TLS listener configuration, leaving the
   gRPC listener plaintext-terminated-upstream, as documented in the decision record.
3. THE deployment documentation SHALL state the stance and its operational consequence: bearer
   tokens MUST reach tokeirad over an upstream-TLS-protected path.

### Requirement 11: AWS IAM authenticator (presigned STS `GetCallerIdentity` bearer)

**User Story:** As an ECS (or any IAM-credentialed) worker fleet owner, I want workers to
authenticate with their task-role credentials and no distributed secrets, so that tokeira is
deployable on AWS compute that has no OIDC issuer.

> Tokeira-native extension (Deviation 4) — the pattern is the EKS `aws-iam-authenticator` /
> Vault AWS auth method: the client presigns an STS `GetCallerIdentity` call with its IAM
> credentials and presents the presigned URL as a bearer; the server has STS execute it and
> reads back the caller's ARN. Precedent in-repo: Aurora DSQL IAM auth tokens are this exact
> shape (`crates/tokeira-storage/src/dsql/connection_factory.rs:23-38`).

#### Acceptance Criteria

1. WHEN `[policy.authorization.aws_iam]` is configured, THE edge SHALL treat a bearer value of
   the form `tokeira-aws-v1.<base64url(presigned STS GetCallerIdentity URL)>` as an AWS IAM
   credential; the prefix is the dispatch discriminator (no heuristics against JWT shapes).
   Absent the config table, the prefix is not special and such a bearer fails JWT parsing
   generically.
2. THE server SHALL parse the decoded URL — never fetching it verbatim — and reject (generic
   deny) unless **all** of the following hold, then reconstruct the request
   itself against the validated STS host (SSRF containment — the outbound target derives from
   the static host pattern, never from unvalidated token content):
   - scheme is `https`; no userinfo component; no fragment; no explicit port other than 443;
     path is exactly `/`;
   - host is exactly `sts.amazonaws.com` or `sts.<region>.amazonaws.com` (the `aws` and
     `aws-us-gov` partitions; `.amazonaws.com.cn` and ISO partitions are out of scope and
     documented as such);
   - `Action=GetCallerIdentity` is present exactly once;
   - `X-Amz-Algorithm` is exactly `AWS4-HMAC-SHA256`; `X-Amz-Credential`'s scope ends
     `/sts/aws4_request` and its region component is consistent with the host;
   - `X-Amz-Date` parses as a valid SigV4 timestamp, `X-Amz-Expires` ≤ 900, and the current
     time lies within `[X-Amz-Date, X-Amz-Date + X-Amz-Expires]`;
   - no security-critical query parameter (`Action`, `Version`, any `X-Amz-*`) occurs more than
     once.
3. THE server SHALL execute the reconstructed call with redirects disabled (any 3xx response
   denies), accept only HTTP 200, bound the response body it will read (constant cap, e.g.
   64 KiB) before parsing, and extract the caller `Arn` from the `GetCallerIdentityResponse`;
   with any STS error, non-200, oversized body, or unreachable STS denying generically
   (fail-closed), under the claim-mapper error-containment rule (Requirement 3.7 —
   never exposed, regardless of `expose_authorizer_errors`).
4. THE resulting claims SHALL be `Claims { subject: <ARN>, auth_type: "aws-iam" }` plus the union
   of `grant` entries from every `aws_iam.grants` rule whose `match_arn` pattern (grammar per
   Requirement 1.5) matches the ARN (grant strings and OR-accumulation per Requirement 3.4/3.5). An ARN matching no rule yields
   role-less claims — authenticated but denied everywhere by the authorizer (no config-level
   reject list needed).
5. VERIFICATION verdicts SHALL be cached keyed on a digest of the full bearer value, with TTL
   bounded by the presigned URL's remaining validity (≤ 15 minutes); cache size/TTL are
   constants, not config. A cached deny is not stored (failures re-verify).
6. PRINCIPAL attribution SHALL work unchanged: on allow, `Principal { type: "aws-iam", name:
   <ARN> }` flows through Requirement 7 identically to JWT principals.
7. THE documentation SHALL state the trust consequences: possession of the presigned URL is
   possession of the identity for its validity window (mirroring the EKS authenticator model),
   so the upstream-TLS requirement of Requirement 10.3 applies with the same force.
