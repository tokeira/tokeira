# Platform Source-Sets Requirements

## Introduction

Tokeira currently binds each deployment to a provenance-bearing platform provisioner (`tkp`) and
retains a definition plus selected companions as configuration history. That model does not yet make
the complete platform-authored input tree a first-class artifact: dashboards, alert rules, and
templates are not uniformly owned, identified, persisted, checked out, edited, applied, recovered,
or audited as one source revision.

This feature introduces the **platform source-set** as that missing abstraction. A platform publishes
an immutable, format-specific **Prototypical Platform Source-Set** alongside a compatible `tkp`.
Deployment creation materializes one selected definition format and its companion files as ordinary
operator-editable files. Each successful apply binds provider state to an immutable **Deployment
Source Revision** without making those source bytes part of `EngineIdentity` or requiring `tkp` to be
rebuilt after an operator edit.

This is a Tokeira-owned provisioning contract with no Temporal analogue. Target behavior is
therefore authoritative in `docs/provisioning/platform-source-sets.md`; current implementation
evidence is listed below. This specification depends on the platform-provisioner binding and
engine-versioning contracts, preserves both the Rust/syn `.tkd` and Monty/Python `.tkdp` frontends,
and extends the shared operator-output contract.

The feature is foundational and cross-cutting: it adds durable source identity, authenticated origin
evidence, deployment-envelope fields, immutable-object persistence, operator workflow, and
interrupted-apply recovery. Physical garbage collection, an independently publishable desired-source
lifecycle, source merging, in-place definition-format conversion, and the mechanics used to project
multiple frontends from one platform composition are outside scope.

## Glossary

- **Platform Source-Set:** The complete layout-admitted tree of platform-authored deployment inputs
  interpreted by a compatible `tkp`.
- **Prototypical Platform Source-Set:** An immutable, format-specific source-set published by a
  platform as the origin from which a deployment is created.
- **Deployment Source Revision:** An immutable snapshot of one deployment's complete source-set,
  including operator edits and origin attribution.
- **Source-Set Layout:** The fixed platform-relative path convention that determines source membership
  without a source-authored metadata file.
- **Source Inventory:** The deterministic derived list of Source Member paths, byte lengths, and
  Content Digests produced from a Source-Set Layout; it is metadata, not a Source Member.
- **Source Member:** A regular file admitted by the Source-Set Layout, including the selected
  definition.
- **Reserved Path:** A deployment-relative path owned by deployment management, local state, or
  derived publication and therefore unavailable to source members.
- **Derived Published Artifact:** A generated provider manifest, rendered deployment artifact,
  provisioner binary, metadata file, or other output derived from source rather than authored as
  source.
- **Definition Format:** The selected operator frontend identity, currently `tkd` or `tkdp`.
- **Tree Digest:** The exact-content SHA-256 identity of a complete Source-Set tree.
- **Revision Digest:** The SHA-256 history identity of a Deployment Source Revision descriptor.
- **Content Digest:** The SHA-256 digest of one source member's exact bytes.
- **Release Evidence Envelope:** An immutable signed inception envelope authenticating the original
  binding among a Prototypical Platform Source-Set, compatible engine, exact `tkp` artifacts, build
  evidence, and release authority.
- **Release Authority:** An identity trusted by configured admission policy to authorize a canonical
  platform release.
- **Release-Authority Policy:** The configured trust roots, accepted signature schemes, key states,
  and admission rules used to verify Release Evidence Envelopes.
- **Build Authority:** Existing provenance data describing who built `tkp` and under which controls;
  it is not self-authenticating proof.
- **Deployment Origin:** The immutable deployment-inception record identifying the original admitted
  Prototypical Platform Source-Set, engine binding, and retained Release Evidence; ordinary apply
  references but never changes it.
- **Applied Source Reference:** The deployment envelope's sole resting mutable reference to the
  Deployment Source Revision that successfully converged.
- **Apply Operation Marker:** The durable confirmed apply intent opened before provider mutation; it
  names the exact confirmed plan, target and prior source revisions, engine identity, starting state
  heads, operation identifier, confirmation attribution, and recovery phase.
- **Apply Result:** An immutable post-convergence record binding the confirmed plan to completed
  actions, resulting source and configuration revision, final state heads, and completion attribution.
- **Operation Lease:** The renewable CAS-managed deployment lock serializing cooperating mutations.
- **Version Token:** A non-empty opaque value returned by a state backend for one mutable logical
  object and accepted only as the expected version for a later CAS on that same object and backend.
- **Observed Version:** Either explicit absence or a present Version Token; absence is never encoded as
  an empty Version Token.
- **Plan Basis:** The local and remote identities and Observed Versions whose equality makes a
  confirmed plan safe to apply.
- **Immutable Object Reference:** A class-specific digest that identifies retained immutable content
  independently of its physical storage location.
- **Retrieval Locator:** Optional catalog or transport metadata used to fetch bytes; it is not durable
  object identity and is excluded from signed and canonical identity.
- **Checkout Base:** The immutable applied revision from which a local editable checkout was
  materialized or, before first convergence, the prototype Tree Digest authenticated by Deployment
  Origin.
- **Dirty Checkout:** A checkout whose current Tree Digest differs from its Checkout Base.
- **Protected Root:** A durable reference that prevents its complete reachable immutable object
  graph from collection.
- **Canonical Publication:** Publication admitted for shared or production use through canonical
  Dagger build evidence and Release-Authority Policy.

## Target State

A catalog selection keyed by `(platform, definition format, engine version)` resolves a compatible
`tkp`, one Prototypical Platform Source-Set, canonical build evidence, and a Release Evidence
Envelope. Creation validates and retains that inception binding, records the immutable Deployment
Origin, and atomically materializes the source tree directly at the deployment root without a
`platform/` or `source/` wrapper. A deployment contains exactly one selected definition format for its
lifetime.

A fixed Source-Set Layout, not a source-authored metadata file, determines membership. A
format-specific source-set contains exactly one root `definition.tkd` or `definition.tkdp`. Compose
admits its other authored inputs only beneath `alerts/`, `dashboards/`, and `templates/`. Canonical
Dagger publication derives the prototype Source Inventory and Tree Digest; deployment creation
re-derives them from the received contents before admission. Generated provider manifests and
rendered outputs are never Source Members.

A Tree Digest identifies exact source content. A separate Revision Digest identifies history and
provenance. Source files are persisted as immutable content-addressed objects; revision descriptors
and admitted release evidence are create-only. The deployment envelope's Applied Source Reference is
the only resting mutable source head. There is no `desired_source_ref`, `source/head.json`, or
independent source-publication command.

With remote state selected, the deployment directory contains source and derived published artifacts
but no provisioned state. Without remote state, local state artifacts coexist only under Reserved
Paths and never affect source identity. The same source validation and identity rules apply to both
backends.

Operators use checkout, status, pull, plan, apply, and marker-scoped recovery. Pull never overwrites a
Dirty Checkout and has no force path. Apply publishes source only as part of the confirmed mutation,
retains the exact revalidated plan, opens its durable confirmed-intent marker before provider changes,
and advances the Applied Source Reference only after an immutable Apply Result is durable. Recovery
resumes the recorded target or reconciles the prior applied source from a separately confirmed
recovery plan; it never merely clears a marker.

Deployment inception requires authenticated Release Evidence. Ordinary apply records
operator-attributed source lineage and may re-evaluate current origin admissibility, but never creates
or modifies Release Evidence. Normal deployment commands never garbage-collect immutable source
objects. A separate, dry-run-first maintenance utility is deferred.

Every in-scope command produces a structured result model rendered as deterministic Markdown by
default or complete JSON under `--json`. Success, refusal, and operational failure have prescribed
stdout/stderr and exit behavior.

## Evidence From Current Code

- **Target behavior:** `docs/provisioning/platform-source-sets.md` records the evaluated architecture,
  annotated apply protocol, operator workflow, and specification boundaries.
- **Deployment materialization:** `apps/tkr/src/deployment_dir.rs`
  (`PendingDeployment::publish`, `DeploymentResolver`) currently stages and atomically renames a
  deployment directory containing a definition, `tokeirad.toml`, metadata, `tkp`, and local `state/`.
- **Retained desired source:** `crates/tokeira-provisioner-cli/src/config_history.rs` currently retains
  one definition, `source.json`, `tokeirad.toml`, and `explanation.json` under
  `state/config-revisions/<revision>/`; it does not retain a complete layout-defined source-set.
- **Deployment authority:** `crates/tokeira-provisioner/src/lib.rs`
  (`DeploymentStateEnvelope`, `Operation`, `RollbackCheckpoint`) currently binds engine provenance,
  configuration revision, state heads, checkpoints, and upgrade/rollback markers, but has no Applied
  Source Reference or source origin.
- **Remote CAS state:** `crates/tokeira-state/src/s3_store.rs` (`S3StateStore`) already implements one
  mutable `manifest.json`, immutable snapshots, `If-None-Match: *` creation, `If-Match` updates, and
  post-read SHA-256 verification.
- **Operation serialization:** `crates/tokeira-state/src/operation_lock.rs` (`OperationLock`) already
  provides renewable CAS acquisition, token-bound renewal, released-record retention, and takeover of
  expired or released leases.
- **Engine identity and build trust:** `crates/tokeira-provisioner/src/identity.rs`
  (`EngineIdentity`, `BuildAuthority`) separates executable interchangeability from authority tier;
  `crates/tokeira-provisioner/src/admission.rs` verifies authority, revocation, and artifact bytes but
  does not authenticate a source-set-to-engine release binding.
- **Catalog resolution:** `.kiro/specs/engine-versioning/` defines exact
  `(platform, format, engine)` publication and resolution. This feature adds the external source-set
  artifact and authenticated release binding without changing that key.
- **Frontend accessibility:** `.kiro/specs/tkdp-frontend/` and the `tokeira-tkd`/`tokeira-tkdp` crates
  establish `.tkd` and `.tkdp` as peer operator-facing frontends.
- **Compose authored inputs:** `platforms/compose/definition.tkd` and `definition.tkdp` are the two
  format projections. The currently misplaced companion source is one alert file under
  `crates/tokeira-compose/alerts/`, ten JSON dashboards under `crates/tokeira-compose/dashboards/`,
  and five live templates under `crates/tokeira-compose/templates/`; the Compose implementation
  embeds those bytes at compile time.
- **Output behavior:** `docs/platforms/operator-output-contract.md`,
  `crates/tokeira-provisioner-cli/src/render.rs`, and
  `crates/tokeira-provisioner-cli/src/cli.rs` establish data-first reports, deterministic Markdown,
  complete `--json`, stdout reports, stderr advisories, exit `0` success, and exit `1` typed refusal.
- **Launcher propagation:** `apps/tkr/src/launcher.rs` inherits `tkp` stdout/stderr and propagates its
  process status, so the two binaries can expose one command contract.

## Contract Policy

### Source-Set Layout

There is no `source-set.toml` or other source-authored membership metadata. The selected catalog
binding supplies platform id and Definition Format; the platform layout supplies the admissible paths.

| Path | Compose policy | Membership / side effect |
|---|---|---|
| `definition.tkd` | Required only for a `tkd` projection | Selected definition and Source Member |
| `definition.tkdp` | Required only for a `tkdp` projection | Selected definition and Source Member |
| `alerts/**` | Zero or more regular files | Every file is a Source Member |
| `dashboards/**` | Zero or more regular files | Every file is a Source Member |
| `templates/**` | Zero or more regular files | Every file is a Source Member |
| Any other non-reserved path | Not admitted by Compose | `unsupported_source_path` |

The current Compose prototype contains one selected definition, one alert file, ten dashboards, and
five templates: seventeen Source Members for each format projection. It has no authored manifests,
generic assets, or runtime configuration. `tokeirad.toml` and `docker-compose.yml` are Reserved Path
derivatives rather than Source Members.

### Reserved Path Authority

The shared scanner enforces the union of deployment-wide Reserved Paths and the bound platform's
compiled derived-output declarations. Source content cannot add, remove, or override a reservation.
A directory reservation covers the named directory and every descendant.

| Authority | Reserved path | Current purpose |
|---|---|---|
| Shared deployment layer | `metadata.json` | Deployment identity and binding metadata |
| Shared deployment layer | `tkp` | Deployment-local provisioner executable |
| Shared deployment layer | `tokeirad.toml` | Generated server configuration |
| Shared deployment layer | `state/` | Local state when selected; remains reserved when remote state omits it |
| Compose platform binding | `config/` | Generated observability and service configuration |
| Compose platform binding | `docker-compose.yml` | Non-authoritative inspection projection |

### Derived Source Inventory Entry

| Field | Derivation | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `path` | Canonical relative path admitted by the platform layout | `invalid_source_path` | Materialized path and canonical ordering key |
| `byte_length` | Exact current file byte length | `invalid_revision_entry` | Included in Tree Digest and revision descriptor |
| `content_digest` | SHA-256 of exact current file bytes | `content_digest_mismatch` | Immutable blob identity |

The Source Inventory is derived at canonical publication, deployment creation, and apply by the same
shared scanner. It is not materialized into the source tree. Revision-descriptor entries are generated
from this inventory and cannot assert independent roles, content types, or membership.

### Deployment Source Revision Descriptor

The descriptor is immutable and rejects unknown fields.

| Field | Target policy | Error if invalid | Identity / persistence impact |
|---|---|---|---|
| `schema` | Supported revision schema | `unsupported_revision_schema` | Included in Revision Digest |
| `platform_id` | Matches catalog and deployment binding | `platform_mismatch` | Included in Revision Digest |
| `definition_format` | Matches immutable deployment format | `definition_format_mismatch` | Included in Revision Digest |
| `definition_path` | Matches Source-Set Layout and admitted frontend | `invalid_definition_path` | Included in Revision Digest |
| `tree_digest` | Recomputed Tree Digest of all entries | `tree_digest_mismatch` | Content identity referenced by descriptor |
| `parent_revision` | Prior applied revision, or absent only for first revision | `invalid_parent_revision` | Included in Revision Digest |
| `entries[]` | Exact complete Source Inventory with path, byte length, and Content Digest | `invalid_revision_entry` | Resolves immutable blobs |
| `created_at` | UTC creation instant | `invalid_attribution` | Included in Revision Digest |
| `created_by` | Non-empty operator/service identity | `invalid_attribution` | Included in Revision Digest |
| `message` | Optional operator attribution text | `invalid_attribution` | Included in Revision Digest when present |
| `origin` | Immutable Deployment Origin defined below | `invalid_origin_evidence` | Included in Revision Digest and unchanged by ordinary apply |

The descriptor does not contain its own Revision Digest; the immutable object key is that digest.

### Deployment Origin

The Deployment Origin is established at deployment inception. Ordinary source apply copies this
record into revision identity by reference and never creates, replaces, or amends its Release Evidence.

| Field | Target policy | Error if invalid | Identity / persistence impact |
|---|---|---|---|
| `prototypical_source_set_digest` | Equals the originally admitted prototype Tree Digest | `source_origin_mismatch` | Included in Deployment Origin and carried by descendant revisions |
| `release_record_digest` | SHA-256 of the complete retained evidence envelope | `release_digest_mismatch` | Sole durable evidence reference; deterministically keys retained evidence |
| `release_authority` | Authority id admitted at inception | `untrusted_release_authority` | Included in Deployment Origin for inspection and policy evaluation |
| `engine_version` | Original admitted human engine release | `release_binding_mismatch` | Included in Deployment Origin |
| `engine_identity_digest` | Original admitted `tkp` identity | `release_binding_mismatch` | Included in Deployment Origin |

### Inception Release Evidence Envelope

The signed canonical payload consists exactly of the rows marked **signed payload**. `signature` and
`verification_material` are outer-envelope fields. `release_record_digest` is computed over the
complete envelope after those fields are present and is stored in Deployment Origin and the evidence
object key rather than recursively inside the envelope.

| Field | Target policy | Error if invalid | Signature / persistence impact |
|---|---|---|---|
| `schema` | Supported release-evidence schema | `unsupported_release_schema` | **signed payload** |
| `release_authority` | Authority id admitted by policy | `untrusted_release_authority` | **signed payload** |
| `platform` / `format` / `engine_version` | Match catalog selection and inception binding | `release_binding_mismatch` | **signed payload** |
| `engine_identity_digest` | Matches the originally admitted `tkp` identity | `release_binding_mismatch` | **signed payload** |
| `tkp_artifacts[]` | Exact target, size, and SHA-256 for published executables | `artifact_mismatch` | **signed payload** |
| `build_evidence_digest` | Digest of canonical Dagger evidence | `build_evidence_mismatch` | **signed payload** |
| `build_authority` | Existing Build Authority claim | `build_authority_insufficient` | **signed payload**, not self-authenticating |
| `source_set_tree_digest` | Equals the originally admitted prototype Tree Digest | `source_origin_mismatch` | **signed payload** |
| `issued_at` | Valid UTC instant within key policy | `release_time_invalid` | **signed payload** |
| `signature_scheme` | Versioned scheme supported by policy | `unsupported_signature_scheme` | **signed payload** and verifier selection |
| `key_id` | Trusted key for authority and issuance time | `untrusted_release_key` | **signed payload** and key selection |
| `signature` | Valid over exact canonical signed-payload bytes | `release_signature_invalid` | Outer envelope |
| `verification_material` | Fingerprint matches the policy-selected trusted key | `verification_material_mismatch` | Outer envelope retained for historical verification |

### Deployment Envelope and Apply Marker Additions

| Field | Target policy | Error if invalid | Mutation rule |
|---|---|---|---|
| `platform_source_origin` | Immutable Deployment Origin established from inception evidence | `invalid_origin_evidence` | Set at creation; never changed by ordinary apply |
| `definition_format` | Deployment's immutable admitted format | `definition_format_mismatch` | Set at creation; never changed |
| `applied_source_ref` | Revision Digest that last converged, or absent before first apply | `invalid_applied_source` | Changed only by successful final apply/recovery CAS |
| `config_revision` | Monotonic applied revision counter | `stale_config_revision` | Advanced in same CAS as Applied Source Reference |
| `last_apply_result_ref` | Digest of the last committed Apply Result, or absent before first apply | `invalid_apply_result` | Changed only by successful final apply/recovery CAS |
| `operation.operation_id` | Unique non-empty id | `operation_id_mismatch` | Required while apply/recovery is in flight |
| `operation.kind` | Includes `apply_in_flight` | `operation_kind_mismatch` | Gates ordinary mutation |
| `operation.confirmed_plan_digest` | Digest of the exact revalidated and confirmed apply plan | `invalid_confirmed_plan` | Set in confirmed intent before provider mutation |
| `operation.confirmed_by` | Non-empty operator or automation identity | `invalid_attribution` | Set with confirmed plan; immutable |
| `operation.confirmed_at` | UTC confirmation instant | `invalid_attribution` | Set with confirmed plan; immutable |
| `operation.recovery_strategy` | Absent during ordinary apply; `resume` or `restore-applied` after recovery confirmation | `recovery_strategy_mismatch` | Set once before recovery mutation |
| `operation.recovery_plan_digest` | Required when recovery strategy is set | `invalid_confirmed_plan` | Set with recovery strategy before recovery mutation |
| `operation.recovery_confirmed_by` / `operation.recovery_confirmed_at` | Required recovery confirmation attribution when strategy is set | `invalid_attribution` | Set once before recovery mutation |
| `operation.phase` | Versioned idempotent apply/recovery phase | `invalid_operation_phase` | Advanced only by guarded envelope CAS |
| `operation.target_source_ref` | Exact target Revision Digest | `invalid_operation_target` | Set before provider mutation |
| `operation.prior_applied_source_ref` | Prior applied revision, if any | `invalid_operation_prior` | Recovery baseline |
| `operation.bound_engine_identity` | Engine authorized to perform operation | `engine_identity_mismatch` | Recovery admission guard |
| `operation.starting_infra_head` | Infra head at marker open | `state_head_mismatch` | Recovery/checkpoint evidence |
| `operation.starting_runtime_head` | Runtime head at marker open | `state_head_mismatch` | Recovery/checkpoint evidence |

### Rollback Checkpoint Addition

| Field | Target policy | Error if invalid | Mutation rule |
|---|---|---|---|
| `from_source_ref` | Applied Source Reference at checkpoint creation, or absent only when no source revision has ever applied | `invalid_rollback_source` | Restored with checkpoint engine/config/state through the rollback commit |

### Confirmed Plan

The Confirmed Plan is retained only after its Plan Basis has been revalidated under the Operation
Lease. Its digest covers the complete canonical plan object except its own digest.

| Field | Target policy | Error if invalid | Identity / persistence impact |
|---|---|---|---|
| `schema` | Supported confirmed-plan schema | `invalid_confirmed_plan` | Included in Confirmed Plan Digest |
| `operation_id` | Matches the Apply Operation Marker | `operation_id_mismatch` | Included in Confirmed Plan Digest |
| `plan_basis` | Exact local/remote identities and Observed Versions revalidated under lease | `invalid_confirmed_plan` | Included in Confirmed Plan Digest |
| `target_source_ref` | Exact revision intended for convergence | `invalid_operation_target` | Included in Confirmed Plan Digest |
| `actions[]` | Complete ordered provider action set shown to the confirmer | `invalid_confirmed_plan` | Included in Confirmed Plan Digest |
| `confirmed_by` | Non-empty operator or automation identity | `invalid_attribution` | Included in Confirmed Plan Digest |
| `confirmed_at` | UTC confirmation instant | `invalid_attribution` | Included in Confirmed Plan Digest |

### Apply Result

The Apply Result is retained create-only after convergence and before the final envelope CAS. The
final CAS references its digest, records the same resulting identities, and closes the matching marker.

| Field | Target policy | Error if invalid | Identity / persistence impact |
|---|---|---|---|
| `schema` | Supported apply-result schema | `invalid_apply_result` | Included in Apply Result Digest |
| `operation_id` | Matches the open marker | `operation_id_mismatch` | Included in Apply Result Digest |
| `confirmed_plan_digest` | Matches the marker's original or recovery plan | `invalid_confirmed_plan` | Included in Apply Result Digest |
| `recovery_strategy` | Absent for ordinary apply; matches marker for recovery | `recovery_strategy_mismatch` | Included in Apply Result Digest when present |
| `completed_actions[]` | Complete provider action outcomes actually observed | `invalid_apply_result` | Included in Apply Result Digest |
| `resulting_source_ref` | Source revision that converged | `invalid_applied_source` | Included in Apply Result Digest and final envelope CAS |
| `resulting_config_revision` | Exact post-commit configuration revision | `stale_config_revision` | Included in Apply Result Digest and final envelope CAS |
| `final_infra_head` / `final_runtime_head` | Exact converged state heads | `state_head_mismatch` | Included in Apply Result Digest and final envelope CAS |
| `completed_by` | Non-empty operator or automation identity | `invalid_attribution` | Included in Apply Result Digest |
| `completed_at` | UTC completion instant | `invalid_attribution` | Included in Apply Result Digest |

There is no `desired_source_ref` and no independently mutable source head.

### Immutable References, Retrieval, and Backend Versions

A retained immutable object is referenced by its class-specific digest. The selected store maps its
class and digest to a logical key; no physical path, URL, bucket, or backend locator participates in
Tree Digest, Revision Digest, Release Record Digest, Confirmed Plan Digest, Apply Result Digest, or
Deployment Origin identity.

Catalog and transport records may carry an optional Retrieval Locator. Retrieval always verifies the
expected class-specific digest after reading bytes. A locator can change without changing identity,
and missing locator metadata is valid when the bytes are already supplied through the admitted
operation.

Reads of mutable logical objects return an Observed Version. S3 may use an ETag as its opaque Version
Token; local storage may use a content hash. Callers compare and return tokens exactly without
interpreting their representation.

### S3 Object Layout and Write Policy

Let `R = s3://<state-bucket>/<deployment-key-prefix>`. `If-None-Match: *` realizes create-only or an
absent Observed Version; `If-Match` carries a present opaque Version Token. Other backends provide the
same semantics without adopting S3 terminology.

| Object class | Logical key | Write condition | Crash-visible result |
|---|---|---|---|
| Source blob | `R/source/blobs/sha256/<content-digest>` | `If-None-Match: *`; verify exact bytes, size, digest on collision | May remain unreachable; never overwritten |
| Revision descriptor | `R/source/revisions/<revision-digest>.json` | `If-None-Match: *`; verify canonical bytes and digest on collision | Durable intended source, not yet applied |
| Release evidence | `R/evidence/releases/<release-record-digest>.json` | At inception, verify policy, then `If-None-Match: *`; verify envelope on collision | Durable authenticated inception evidence |
| Confirmed plan | `R/state/operations/plans/<confirmed-plan-digest>.json` | After lease-bound revalidation, `If-None-Match: *`; verify canonical bytes on collision | Durable pre-mutation operator intent |
| Apply result | `R/state/operations/results/<apply-result-digest>.json` | After convergence, `If-None-Match: *`; verify canonical bytes on collision | May remain uncommitted until final envelope CAS references it |
| Envelope snapshot | `R/state/envelope/snapshots/<timestamp>-<uuid>.json` | `If-None-Match: *` | May remain orphaned until manifest CAS |
| Envelope head | `R/state/envelope/manifest.json` | Create with `If-None-Match: *`; update with `If-Match` | Sole envelope commit point |
| Infra snapshot/head | `R/state/infra/...` | Existing immutable snapshot plus manifest CAS | Incremental convergence checkpoint |
| Runtime snapshot/head | `R/state/runtime/...` | Existing immutable snapshot plus manifest CAS | Incremental convergence checkpoint |
| Operation lease | `R/state/lock/operation/manifest.json` | Create absent or CAS expired/released; renew/release with `If-Match` | Serializes cooperating mutators |

### CLI Outcome Contract

| Outcome | Exit | stdout after command dispatch | stderr after command dispatch |
|---|---:|---|---|
| Success | `0` | One complete Markdown or JSON report | Advisories only |
| Refused | `1` | One complete typed refusal report | Advisories only; no duplicate refusal |
| Failed | `1` | One complete typed operational-failure report | Advisories only; no duplicate failure |
| Usage/parser error | `2` | Empty | Parser diagnostic and usage |
| Bootstrap failure before report dispatch | `1` | Empty | One diagnostic |

Under `--json`, stdout contains exactly one complete JSON value and no narrative text. `--detail` is
ignored for JSON. `tkr` forwards flags and propagates `tkp` status unchanged.

## Requirements

### Requirement 1: Platform Ownership and Publication

**User Story:** As a platform owner, I want the complete authored deployment input published as one
platform-owned artifact, so that ownership and release provenance are unambiguous.

#### Acceptance Criteria

1. THE platform package SHALL own every platform-authored definition, dashboard, alert rule,
   and template in its Prototypical Platform Source-Set.
2. THE provider crates SHALL expose reusable provider mechanics without owning platform-authored
   Source Members.
3. WHEN a canonical platform release is published, THE release pipeline SHALL publish one
   Prototypical Platform Source-Set for each admitted `(platform, definition format, engine version)`
   catalog selection.
4. WHEN `.tkd` and `.tkdp` projections are published for one platform composition, THE publication
   process SHALL establish that neither projection is an independently maintained platform authority.
5. THE platform release SHALL keep Source-Set bytes external to `tkp` executable bytes.
6. WHEN an operator edits a Source Member, THE deployment SHALL retain the same bound engine identity.
7. THE Source-Set SHALL exclude generated provider manifests and rendered deployment artifacts.
8. THE Source-Set SHALL represent secrets by references rather than secret values.
9. THE Source-Set SHALL exclude floating executable dependencies.
10. WHEN `tkp` plans or applies a deployment, THE bound engine SHALL interpret and validate the
    complete Source-Set selected for that deployment.

### Requirement 2: Layout-Derived Membership

**User Story:** As an operator, I want platform contents to define the source-set through one fixed
layout, so that source identity needs no separately maintained membership file.

#### Acceptance Criteria

1. THE format-specific Source-Set root SHALL contain exactly one selected definition named
   `definition.tkd` or `definition.tkdp` matching the deployment's immutable Definition Format.
2. WHERE the platform is `compose`, THE Source-Set Layout SHALL admit additional Source Members only
   beneath `alerts/`, `dashboards/`, and `templates/`.
3. THE Source Inventory SHALL include the selected definition and every regular file admitted by the
   Source-Set Layout exactly once.
4. THE Source Inventory SHALL derive each entry's canonical path, byte length, and Content Digest
   directly from the current tree without source-authored membership metadata.
5. IF the Source-Set contains both definition formats or a definition different from the selected
   format, THEN THE Source-Set validator SHALL return `definition_format_mismatch`.
6. IF a non-reserved file exists outside the platform's admitted Source-Set Layout, THEN THE
   Source-Set validator SHALL return `unsupported_source_path`.
7. WHEN canonical Dagger publication scans a Prototypical Platform Source-Set, THE publication process
   SHALL derive its Source Inventory and Tree Digest with the shared source-set scanner.
8. WHEN deployment creation admits a Prototypical Platform Source-Set, THE creator SHALL re-derive its
   Source Inventory and Tree Digest before final publication.
9. IF a Source Member claims a Reserved Path, THEN THE Source-Set validator SHALL return
   `reserved_source_path`.
10. THE Source Inventory SHALL derive file semantics from the canonical relative path without
    source-authored role declarations.
11. THE Source Inventory SHALL contain no independently asserted role or content-type fields.
12. THE Source-Set validator SHALL enforce finite versioned limits for path bytes, member count,
    individual member bytes, and total tree bytes.
13. IF a source limit is exceeded, THEN THE Source-Set validator SHALL return `source_limit_exceeded`
    with the limit name, configured bound, and observed value.
14. WHEN multiple Source-Set defects are present, THE Source-Set validator SHALL report the first
    defect in this precedence: definition selection, path form or collision, Reserved Path, file type,
    platform layout, source limits, then content or tree identity.
15. THE shared deployment layer SHALL reserve `metadata.json`, `tkp`, `tokeirad.toml`, and the complete
    `state/` subtree independently of state-backend selection.
16. WHERE the platform is `compose`, THE bound platform SHALL reserve the complete `config/` subtree
    and `docker-compose.yml` as derived publication paths.
17. WHEN Source-Set membership is derived, THE shared scanner SHALL enforce the union of shared and
    bound-platform Reserved Paths before admitting Source Members.

### Requirement 3: Portable and Exact Source Trees

**User Story:** As an operator moving a deployment between supported hosts, I want source paths and
bytes interpreted identically, so that identity and materialization are portable.

#### Acceptance Criteria

1. THE Source-Set validator SHALL accept only relative UTF-8 paths already encoded in Unicode NFC.
2. THE Source-Set validator SHALL accept `/` as the only path separator.
3. IF a source path is absolute or contains an empty, `.`, or `..` component, THEN THE Source-Set
   validator SHALL return `invalid_source_path`.
4. IF a source path contains `\`, THEN THE Source-Set validator SHALL return `invalid_source_path`.
5. IF two source paths have equal Unicode case-folded keys, THEN THE Source-Set validator SHALL return
   `source_path_collision`.
6. IF a Source Member is a symlink, THEN THE Source-Set validator SHALL return
   `unsupported_source_file_type`.
7. IF a Source Member is not a regular file, THEN THE Source-Set validator SHALL return
   `unsupported_source_file_type`.
8. THE Source-Set identity SHALL exclude host modification time, ownership, and file mode.
9. WHEN a Source-Set is materialized on Unix, THE materializer SHALL assign mode `0644` to source
   files and mode `0755` to source directories.
10. THE materializer SHALL never assign executable permission to a Source Member.
11. THE Content Digest SHALL cover each member's exact bytes without line-ending normalization.

### Requirement 4: Atomic Deployment Materialization and Immutable Format

**User Story:** As an operator creating a deployment, I want one complete source projection published
atomically, so that I never observe a mixed format or partial source tree.

#### Acceptance Criteria

1. WHEN deployment creation selects a catalog entry, THE creator SHALL select exactly one Definition
   Format for the deployment.
2. WHEN deployment creation materializes source, THE creator SHALL preserve every Source Member's
   platform-relative path directly at the deployment root.
3. THE deployment layout SHALL contain no additional `platform/` or `source/` wrapper.
4. THE deployment creator SHALL stage the Source-Set, `tkp`, metadata, and derived publication entries
   away from the final deployment path.
5. WHEN every staged artifact passes validation and admission, THE deployment creator SHALL publish
   the complete deployment directory through one atomic directory transition.
6. IF staging, validation, admission, or final publication fails, THEN THE deployment creator SHALL
   leave no visible partial deployment directory.
7. THE deployment metadata SHALL record the selected Definition Format as immutable.
8. IF apply or pull observes a Definition Format different from deployment metadata, THEN THE command
   SHALL return `definition_format_mismatch` without changing source or state.
9. WHEN an operator needs the other Definition Format, THE operator workflow SHALL require creation of
   a new deployment.
10. WHERE remote state is selected, THE deployment materializer SHALL omit provisioned state from the
    deployment directory.
11. WHERE remote state is not selected, THE deployment materializer SHALL place local state only under
    Reserved Paths excluded from source membership.
12. THE state-backend selection SHALL NOT change Source-Set identity or dirty detection.

### Requirement 5: Source and Revision Identity

**User Story:** As an operator and auditor, I want content identity separate from history identity, so
that equal trees compare equal while provenance and parentage remain inspectable.

#### Acceptance Criteria

1. THE Tree Digest SHALL be SHA-256 over a versioned length-prefixed canonical encoding tagged
   `tokeira-source-tree/v1`.
2. THE canonical tree encoding SHALL order entries by normalized relative-path bytes.
3. THE canonical tree encoding SHALL encode each entry's path, byte length, and raw 32-byte Content
   Digest.
4. THE canonical tree encoding SHALL include every layout-admitted Source Member, including the
   selected definition.
5. THE canonical tree encoding SHALL omit the derived Source Inventory itself, every Reserved Path,
   and all derived published artifacts.
6. WHEN a Source Member is added, removed, renamed, or changed byte-for-byte, THE Tree Digest SHALL
   change.
7. THE Revision Digest SHALL be SHA-256 over a versioned canonical descriptor encoding tagged
   `tokeira-source-revision/v1`.
8. THE revision encoding SHALL cover Tree Digest, parent revision, complete entry metadata, origin,
   creation time, creator, and optional message.
9. THE revision encoding SHALL exclude its own Revision Digest.
10. WHEN two revisions have equal trees but different parentage, origin, or attribution, THE Revision
    Digests SHALL differ.
11. WHEN an operator edits any Source Member, THE resulting change SHALL affect Deployment Source
    Revision identity rather than `EngineIdentity`.
12. WHEN dirty status is calculated, THE status command SHALL compare the current Tree Digest with the
    Checkout Base Tree Digest.
13. WHEN a revision is retained, THE history store SHALL retain the complete Source-Set rather than a
    definition-only subset.
14. WHEN a descriptor is loaded, THE revision store SHALL recompute every Content Digest, Tree Digest,
    and Revision Digest before trusting it.

### Requirement 6: Authenticated Platform Origin

**User Story:** As an operator, I want source origin authenticated independently of editable source
claims, so that only an authorized platform release can establish the initial binding.

#### Acceptance Criteria

1. THE Release-Authority Policy SHALL be the trust root for Canonical Publication admission.
2. THE signed canonical payload SHALL contain exactly the Inception Release Evidence fields marked
   as signed payload while signature and verification material remain outer-envelope fields.
3. WHEN canonical release evidence is admitted, THE verifier SHALL recompute the complete envelope
   digest before signature verification.
4. WHEN canonical release evidence is admitted, THE verifier SHALL verify the signature over the
   canonical payload using a policy-trusted authority and key.
5. WHEN release evidence is admitted, THE verifier SHALL compare platform, format, engine identity,
   exact `tkp` artifact, build evidence, Build Authority, and prototype Tree Digest with the selected
   catalog entry.
6. THE verifier SHALL treat Build Authority as signed provenance data rather than proof that
   authenticates itself.
7. IF any release binding field differs from the selected catalog entry, THEN THE verifier SHALL
   return `release_binding_mismatch`.
8. IF the signature scheme is unsupported, THEN THE verifier SHALL return
   `unsupported_signature_scheme`.
9. IF the authority or key is not trusted for the platform and issuance time, THEN THE verifier SHALL
   return `untrusted_release_authority`.
10. IF a signature does not verify, THEN THE verifier SHALL return `release_signature_invalid`.
11. WHEN a release key rotates, THE Release-Authority Policy SHALL retain sufficient prior key state
    to evaluate already-retained evidence.
12. WHEN a key or release is revoked, THE Release-Authority Policy SHALL distinguish cryptographic
    validity from current admissibility.
13. IF current policy marks retained evidence inadmissible, THEN THE apply or recovery command SHALL
    return `release_evidence_revoked` before provider mutation.
14. WHEN deployment inception admits Release Evidence, THE evidence store SHALL retain the envelope
    and verification material create-only before publishing deployment metadata.
15. THE deployment-inception verifier SHALL admit only origin authenticated by Release-Authority
    Policy for the exact Dagger-built `tkp` and Prototypical Platform Source-Set binding.
16. IF deployment-inception evidence is absent or unauthenticated, THEN THE verifier SHALL return
    `invalid_origin_evidence`.
17. THE release verifier SHALL require Dagger build evidence with a digest-pinned build container and
    a complete `EngineIdentity`.
18. WHEN ordinary apply creates an operator-edited descendant revision, THE revision SHALL reference
    the immutable Deployment Origin and record operator attribution without creating or modifying
    Release Evidence.

### Requirement 7: Immutable Source Persistence

**User Story:** As an operator using remote or local state, I want source history persisted with the
same integrity discipline as provisioned state, so that concurrent or failed writers cannot rewrite
history.

#### Acceptance Criteria

1. THE source-set domain layer SHALL own layout scanning, validation, canonical identity,
   materialization, and comparison independently of storage backend.
2. THE persistence layer SHALL expose immutable-object writes separately from CAS-managed mutable
   references.
3. WHERE S3 remote state is selected, THE persistence layer SHALL use the logical object keys and
   conditional-write rules in the S3 Object Layout table.
4. WHERE local state is selected, THE persistence layer SHALL provide equivalent create-only source
   object semantics under Reserved Paths.
5. WHEN a source blob, revision descriptor, or release evidence object is first written, THE
   persistence layer SHALL use create-only semantics.
6. WHEN a create-only object already exists with identical verified content, THE persistence layer
   SHALL treat the write as idempotent success.
7. IF a create-only object already exists but fails byte, size, digest, or evidence verification,
   THEN THE persistence layer SHALL return `immutable_object_collision`.
8. THE persistence layer SHALL never overwrite a retained source blob, revision descriptor, or
   Release Evidence Envelope.
9. THE Deployment Envelope SHALL carry the sole resting mutable Applied Source Reference.
10. THE persistence model SHALL contain no `desired_source_ref`.
11. THE persistence model SHALL contain no mutable `source/head.json`.
12. THE state manifests SHALL remain guarded by expected Version Token CAS.
13. THE generic state backend SHALL NOT own Source-Set domain validation merely because it stores the
    resulting bytes.
14. WHEN a mutable logical object is read, THE state backend SHALL return an Observed Version that is
    either explicit absence or a present non-empty Version Token.
15. WHEN a mutable logical object is conditionally written, THE persistence layer SHALL use the
    Observed Version obtained for that same logical object and backend.
16. THE persistence layer SHALL treat a present Version Token as opaque without interpreting it as an
    ETag, digest, sequence number, timestamp, or portable value.
17. THE durable reference to an immutable object SHALL be its class-specific digest under the selected
    store's deterministic logical-key mapping.
18. THE signed and canonical identity models SHALL exclude optional Retrieval Locators.
19. WHEN bytes are obtained through a Retrieval Locator, THE retriever SHALL verify the expected
    class-specific digest before admitting them.
20. WHERE expected bytes are supplied directly, THE retriever SHALL NOT require a Retrieval Locator.

### Requirement 8: Apply-Coupled Publication and Commit Protocol

**User Story:** As an operator applying source changes, I want source publication and provider mutation
committed through one recoverable protocol, so that remote state never claims convergence that did not
complete.

#### Acceptance Criteria

1. WHEN `tkr infra plan` evaluates local source, THE command SHALL validate and canonicalize it without
   publishing a source object.
2. WHEN plan completes, THE Plan Basis SHALL record the target Revision Digest, bound engine identity,
   Applied Source Reference, envelope Observed Version, infra-state Observed Version, runtime-state
   Observed Version, and open marker.
3. WHEN a plan is presented, THE command SHALL render source changes before requesting confirmation.
4. WHEN apply begins after confirmation, THE command SHALL acquire and renew the deployment Operation
   Lease before publishing source or mutating a provider.
5. WHEN apply holds the lease, THE command SHALL reload every Plan Basis value before mutation.
6. IF a reloaded basis value differs materially from the confirmed plan, THEN THE command SHALL render
   a replacement plan and require renewed confirmation.
7. IF Applied Source Reference exists and Checkout Base names a different applied revision, THEN THE
   apply command SHALL return `remote_source_advanced` before source publication.
8. WHEN basis revalidation succeeds, THE apply command SHALL publish every missing source blob with
   create-only semantics.
9. WHEN a new target revision is required and its source blobs are durable, THE apply command SHALL
   verify the retained Deployment Origin's current admissibility before publishing its descriptor.
10. WHEN the target descriptor and exact revalidated Confirmed Plan are durable, THE apply command
    SHALL open an Apply Operation Marker containing that plan digest and confirmation attribution by
    envelope CAS before the first provider mutation.
11. WHEN a provider mutation succeeds, THE apply command SHALL checkpoint resulting infra or runtime
    state through its existing snapshot-plus-manifest CAS.
12. WHEN all provider mutations converge and the Apply Result is durable, THE apply command SHALL
    clear the matching marker, advance `config_revision`, set Applied Source Reference, reference the
    Apply Result, and record its final state heads in one envelope CAS.
13. THE successful final envelope CAS SHALL be the only commit point that declares the target source
    applied.
14. WHEN the final envelope CAS succeeds, THE apply command SHALL report successful convergence even
    if subsequent lease release fails.
15. WHEN apply finishes or aborts while retaining its lease, THE command SHALL CAS the lease record to
    released state.
16. IF local source is invalid, THEN THE apply command SHALL return `invalid_source_set` before remote
    publication.
17. IF engine or release admission fails, THEN THE apply command SHALL return the typed admission
    reason before provider mutation.
18. IF another active lease owns the deployment, THEN THE apply command SHALL return `operation_locked`.
19. IF an Apply Operation Marker is already open, THEN THE ordinary apply command SHALL return
    `operation_recovery_required`.
20. THE apply command SHALL expose no force option that bypasses source-base checks, admission,
    Operation Lease, marker, or CAS.
21. THE CLI SHALL expose no independent desired-source publication command.
22. IF Applied Source Reference exists and the local Tree Digest equals its Tree Digest, THEN THE apply
    command SHALL use the existing Applied Source Reference as its target source revision.
23. WHERE the existing Applied Source Reference is the target source revision, THE apply command SHALL
    publish no new source blob or revision descriptor.
24. WHEN Applied Source Reference is absent, THE first apply command SHALL publish one initial
    Deployment Source Revision with absent `parent_revision` before opening its operation marker.
25. WHEN a confirmed plan is revalidated under the Operation Lease, THE apply command SHALL retain its
    exact canonical plan object create-only before opening the operation marker.
26. WHEN provider mutations converge, THE apply command SHALL retain one immutable Apply Result
    create-only before attempting the final envelope CAS.
27. THE Apply Result SHALL bind the operation and confirmed plan to completed actions, resulting source
    and configuration revision, final state heads, and completion attribution.

### Requirement 9: Crash Visibility, Lease Loss, and Recovery

**User Story:** As an operator recovering an interrupted apply, I want the exact intended and prior
sources recorded before mutation, so that recovery reconciles deliberately rather than guessing or
clearing evidence.

#### Acceptance Criteria

1. WHEN source publication fails before the marker CAS, THE remote store SHALL expose at most
   unreachable immutable objects without a recorded in-flight operation.
2. WHEN the marker CAS succeeds, THE deployment SHALL expose the exact confirmed plan, target, and
   any prior source revision as recoverable in-flight work.
3. WHEN provider mutation succeeds before its state checkpoint, THE recovery path SHALL re-describe
   provider state and reconcile idempotently.
4. IF Operation Lease renewal is lost, THEN THE mutator SHALL stop before its next provider mutation or
   state commit.
5. WHILE an Apply Operation Marker is open, THE status, plan, and checkout reports SHALL identify the
   operation without presenting its target as applied.
6. WHILE an Apply Operation Marker is open, THE pull and ordinary apply commands SHALL return
   `operation_recovery_required`.
7. THE break-glass command SHALL be
   `tkr deployment operation recover --id <operation-id> --strategy <resume|restore-applied> --yes`.
8. IF recovery omits `--yes`, THEN THE recovery command SHALL return `confirmation_required` after
   rendering the recovery plan.
9. WHEN recovery begins, THE command SHALL acquire and renew the normal deployment Operation Lease.
10. IF the supplied operation id differs from the open marker, THEN THE recovery command SHALL return
    `operation_id_mismatch` without mutation.
11. WHEN recovery holds the lease, THE command SHALL verify target revision bytes, origin evidence,
    engine identity, and current release admissibility.
12. WHEN recovery holds the lease, THE command SHALL re-describe provider state before rendering its
    recovery plan.
13. WHERE strategy is `resume`, THE recovery command SHALL converge the marker's target source
    revision.
14. WHERE strategy is `restore-applied`, THE recovery command SHALL reconcile the marker's prior
    Applied Source Reference.
15. THE recovery command SHALL NOT expose a strategy that merely clears the marker.
16. WHEN the selected recovery source converges and its Apply Result is durable, THE recovery command
    SHALL commit the result reference, resulting source and configuration revision, and final state
    heads while closing the marker through one guarded envelope CAS.
17. IF recovery is interrupted, THEN THE next invocation with the exact marker id and strategy SHALL
    resume from the recorded idempotent phase.
18. WHEN recovery closes successfully, THE command SHALL release the Operation Lease without making
    release the convergence commit point.
19. IF strategy is `restore-applied` and the marker has no prior Applied Source Reference, THEN THE
    recovery command SHALL return `recovery_strategy_unavailable` without mutation.
20. WHEN a recovery plan is confirmed, THE recovery command SHALL record its strategy, exact plan
    digest, confirmation attribution, and initial phase by marker CAS before provider mutation.
21. IF a marker already records a different recovery strategy, THEN THE recovery command SHALL return
    `recovery_strategy_mismatch` without mutation.
22. WHERE recorded strategy is `resume`, THE recovery Apply Result SHALL set resulting source to the
    marker target, advance `config_revision`, and record final state heads.
23. WHERE recorded strategy is `restore-applied`, THE recovery Apply Result SHALL set resulting source
    to the marker prior, preserve `config_revision`, and record final state heads.

### Requirement 10: Checkout, Status, and Pull

**User Story:** As an operator editing deployment source locally, I want safe synchronization with the
applied remote revision, so that local work and newer remote work are never overwritten silently.

#### Acceptance Criteria

1. WHEN `tkr deployment checkout <deployment> <directory>` runs without an open marker, THE command
   SHALL materialize Applied Source Reference when present or otherwise the prototype authenticated by
   Deployment Origin as editable files.
2. WHEN checkout materializes source, THE command SHALL preserve source-relative paths and exact bytes.
3. WHEN checkout succeeds, THE local metadata SHALL record deployment id, remote locator, base
   envelope Observed Version, Checkout Base, immutable Definition Format, bound `EngineIdentity`, and
   prototype origin.
4. IF checkout observes an open marker, THEN THE command SHALL materialize the prior applied source
   when present or otherwise the inception prototype and report the interrupted operation.
5. WHERE remote state is selected, THE checkout command SHALL keep provisioned state and transient
   planning caches outside the deployment directory.
6. WHEN `tkr deployment status` runs, THE command SHALL calculate dirty state from the canonical local
   Tree Digest and Checkout Base.
7. WHEN status runs, THE command SHALL report local clean/modified state, remote current/advanced state,
   and any open operation marker.
8. WHEN `tkr deployment pull` finds a clean checkout and a newer Applied Source Reference, THE command
   SHALL atomically replace Source Members with the remote applied revision.
9. WHEN pull succeeds, THE command SHALL update local Checkout Base metadata to the fetched revision
   and envelope Observed Version.
10. IF pull finds a Dirty Checkout, THEN THE command SHALL leave every local Source Member unchanged.
11. IF local source and remote applied source both advanced, THEN THE pull refusal SHALL report base,
    local, and remote revision identities.
12. IF pull observes a Definition Format mismatch, THEN THE command SHALL return
    `definition_format_mismatch` without changing local files.
13. THE pull command SHALL expose no force-overwrite option.
14. THE first implementation SHALL NOT provide an automatic source merge engine.
15. THE local metadata and any diagnostic cache SHALL remain read models rather than writable remote
    authority.

### Requirement 11: Retention and Collection Boundary

**User Story:** As an operator and auditor, I want every applied or recoverable source graph retained,
so that rollback, recovery, and provenance never depend on collected objects.

#### Acceptance Criteria

1. THE protected-root set SHALL include the current Applied Source Reference.
2. THE protected-root set SHALL include every open marker's target and any prior source revision.
3. THE protected-root set SHALL include every retained rollback checkpoint source revision.
4. THE protected-root set SHALL include every explicit audit-retention pin.
5. WHEN reachability is evaluated, THE traversal SHALL include each protected revision's complete
   parent-revision closure.
6. WHEN reachability is evaluated, THE traversal SHALL include every source blob and Release Evidence
   Envelope referenced by reachable revisions.
7. THE normal checkout, pull, plan, apply, recovery, rollback, and destroy commands SHALL NOT collect
   immutable source or release-evidence objects.
8. THE source persistence API SHALL distinguish object protection from physical storage lifecycle.
9. THE physical mark-and-sweep maintenance utility SHALL remain outside this feature's implementation
   tasks.
10. THE protected-object set SHALL include every Confirmed Plan and Apply Result referenced by the
    current envelope, an open marker, or a retained envelope snapshot.
11. THE protected-object set SHALL include the Deployment Origin's Release Evidence even before the
    first Deployment Source Revision exists.
12. WHEN a protected Confirmed Plan is traversed, THE traversal SHALL include its target source
    revision.
13. WHEN a protected Apply Result is traversed, THE traversal SHALL include its Confirmed Plan and
    resulting source revision.

### Requirement 12: Engine and Source Upgrade Boundaries

**User Story:** As an operator, I want source changes distinguished from executable changes, so that I
rebuild or upgrade `tkp` only when its implementation provenance changes.

#### Acceptance Criteria

1. THE `EngineIdentity` SHALL remain independent of Tree Digest and Revision Digest.
2. WHEN a definition, dashboard, alert rule, or template changes, THE deployment SHALL create a
   new Deployment Source Revision without requiring an engine upgrade.
3. WHEN Rust source closure, lock closure, toolchain, build container, feature set, or build profile
   changes, THE deployment SHALL require the existing engine-upgrade boundary.
4. WHEN a Source-Set path convention or renderer semantics require new executable behavior, THE
   release SHALL classify that change as an engine upgrade.
5. WHEN a compatible Prototypical Platform Source-Set changes without an engine-identity change, THE
   deployment SHALL adopt it through source-set reconciliation rather than an engine rebuild.
6. THE `tkp` `EngineIdentity` SHALL structurally require a digest-pinned build-container digest.
7. THE release pipeline SHALL build every deployable `tkp` only through the canonical Dagger path with
   a complete `EngineIdentity`.
8. THE deployment-inception admission decision SHALL require `EngineIdentity`, canonical Dagger
   evidence, Build Authority, exact artifact digest, and authenticated Release Evidence as
   non-substitutable facts.
9. WHEN a rollback checkpoint is created, THE checkpoint SHALL record the current Applied Source
   Reference or explicit absence when no source revision has ever applied.
10. WHEN rollback converges, THE final guarded envelope CAS SHALL restore the checkpoint's source
    reference together with its engine, configuration reference, and infra/runtime state heads.
11. IF a rollback checkpoint names a source revision whose descriptor or bytes cannot be verified,
    THEN THE rollback command SHALL refuse before provider mutation.

### Requirement 13: Operator Output, Exit Codes, and Streams

**User Story:** As a human or automation operator, I want every source-set command to report success,
refusal, or failure through one stable contract, so that I can understand and automate recovery
without parsing ad hoc prose.

#### Acceptance Criteria

1. WHEN an in-scope command reaches dispatch, THE command SHALL construct a serializable result model
   before rendering output.
2. WHEN narrative output is selected, THE command SHALL render deterministic Markdown conforming to
   `docs/platforms/operator-output-contract.md`.
3. WHEN `--json` is selected, THE command SHALL emit the complete result model as exactly one JSON value
   on stdout.
4. WHERE `--json` is selected, THE command SHALL ignore `--detail` for schema and field inclusion.
5. WHEN a command succeeds, THE process SHALL exit `0` after writing one success report to stdout.
6. WHEN a command refuses safely, THE process SHALL exit `1` after writing one typed refusal report to
   stdout.
7. WHEN an operational command fails after dispatch, THE process SHALL exit `1` after writing one typed
   failure report to stdout.
8. WHEN a refusal or failure report has been written, THE process SHALL omit duplicate error prose from
   stderr.
9. THE stderr stream SHALL carry advisories only after command dispatch.
10. IF command-line parsing fails, THEN THE process SHALL exit `2` with empty stdout and parser output on
    stderr.
11. IF process bootstrap fails before report dispatch, THEN THE process SHALL exit `1` with empty stdout
    and one diagnostic on stderr.
12. WHEN `tkr` forwards an in-scope verb to `tkp`, THE launcher SHALL forward `--json` and `--detail`
    unchanged.
13. WHEN `tkp` exits, THE launcher SHALL propagate its exit status without reinterpretation.
14. THE checkout success report SHALL use title `# Deployment Checkout` and identify deployment,
    applied-source state, Definition Format, and destination.
15. THE source-status report SHALL use title `# Deployment Source Status` and identify local state,
    remote state, and operation state.
16. THE pull success report SHALL use title `# Deployment Pull` and identify prior and resulting
    Checkout Base states.
17. THE recovery report SHALL use title `# Deployment Operation Recovery` and identify marker id,
    strategy, source target, provider observations, and planned or completed outcome.
18. WHEN a source-aware infra plan renders, THE plan report SHALL identify source change state in
    summary and source identities in detail.
19. WHEN a source-aware apply renders, THE apply report SHALL identify the source revision that
    committed convergence.
20. WHEN a command refuses, THE Markdown report SHALL contain a title ending in `Refused`, a
    `**No changes made to <deployment>**` assurance, a `## Why` section, and a `## Next` section.
21. WHEN a command fails after dispatch, THE Markdown report SHALL contain a title ending in `Failed`,
    the last durable boundary, a `## Why` section, and a `## Next` section.
22. WHEN summary depth renders, THE report SHALL omit full digests, provenance chains, and internal
    filesystem paths unless one is required for the operator's next action.
23. WHEN detail depth renders, THE report SHALL include base, local, remote, target, engine, release,
    and state identities relevant to the command.
24. THE structured outcome model SHALL distinguish `success`, `refused`, and `failed` and carry a stable
    machine reason code for every non-success outcome.
25. THE output renderer SHALL redact secret values from narrative and structured forms.

## Iteration and Feedback Notes

- Requirements are derived from the accepted pre-spec architecture in
  `docs/provisioning/platform-source-sets.md` and current implementation evidence listed above.
- The design phase must choose the initial signature scheme and canonical binary encodings while
  preserving the algorithm/version agility required here.
- The design phase must assign concrete finite values to the versioned source-path, member-count,
  per-member, and total-tree limits.
- Physical immutable-object garbage collection remains a separate future maintenance specification.
- No compiler, HIR, or projection-generator mechanism belongs in this feature; publication must only
  prove that multiple frontend projections are not independent platform authorities.
