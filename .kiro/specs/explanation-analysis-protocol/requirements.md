# Requirements Document: Explanation Analysis Protocol

## Introduction

This spec covers **Feature 5 (Analysis Protocol)** from the umbrella
[`operator-explanation`](../operator-explanation/requirements.md). It makes produced
explanations *navigable*: a read-only query surface an agent client (Feature 6), a CI
job, or an operator's script can interrogate — which change, which resource, what depends
on what, what changed between revisions, what could not be determined.

The architecture is fixed by umbrella decision **D1** (*explanation is an artifact, never
a service*) and this spec's own founding move, which makes D1 cheap to honor:

> **Retention makes analysis artifact-pure.** Each applying verb already retains its
> definition per revision. This spec widens that retention into an **analysis bundle** —
> the explanation artifact, the definition source it planned against, and the canonical
> desired snapshot with its dependency edges. Every query in the protocol then reads
> bundles. Nothing queries the engine, nothing realizes a definition at query time,
> nothing spawns `tkp`, and revision comparison degenerates to comparing two bundles
> with the syntactic diff `tokeira-tkd` already owns (Feature 4).

The serving process is `tkr` — the unprivileged operator CLI — never `tkp`. The
provisioner's surface does not grow by one listening byte.

The scope is:

1. **Analysis bundles** — per-revision retention widened (explanation + definition +
   desired snapshot with dependency edges), plus a last-plan bundle at a well-known
   location.
2. **The query surface** — the nine queries from the umbrella plus revision comparison,
   defined transport-agnostically as a typed API with typed not-found outcomes.
3. **Two transports over one library** — an MCP stdio server (`tkr analysis serve`) for
   agent clients, and a one-shot form (`tkr analysis query … --json`) so the protocol is
   exercisable by humans and CI without an agent.
4. **Trust posture** — structurally read-only, bundle-confined file access, schema-version
   gating.

### What This Spec Covers

- The bundle format, its retention by `tkp`'s applying and planning verbs, and its
  lifecycle (bundles follow the revision retention they extend).
- The `tokeira-analysis` library: bundle discovery, the query API, revision comparison.
- The `tkr analysis` noun: `serve` (MCP over stdio) and `query` (one-shot).
- Not-found, schema-version, and path-confinement behaviour.

### What This Spec Does NOT Cover

- Any narration, prose synthesis, or model integration (Feature 6 consumes this
  protocol; it does not shape it).
- Mutation of any kind — there is no apply, destroy, scale, or write query, and none may
  be added under this spec's name.
- Live-state queries: the protocol answers from what was produced and retained. "What is
  running *right now*" is `tkr infra plan`'s job, which produces a fresh bundle.
- Remote transport (network sockets, HTTP): stdio and one-shot only. A future remote
  transport is its own spec with its own threat model.
- Cross-deployment queries: one server instance serves one deployment's bundles.

## Evidence From Current Code

| Fact | Anchor | Consequence |
|---|---|---|
| Per-revision retention exists, keyed under the platform's config basename (`state/config-revisions/{n}/{basename}`), with snapshot/restore and the cross-platform refuse property | `crates/tokeira-provisioner-cli/src/config_history.rs` | The bundle is a widening of an existing, shipped mechanism — not a new store |
| The explanation artifact is self-contained, schema-versioned, secret-free, and closure-checked | Feature 1 (Requirements 7, Properties 3, 10) | Bundles inherit the artifact's integrity guarantees |
| Desired snapshots are canonical per-resource manifests, produced platform-side | Feature 3 (`desired_snapshot`) | The snapshot half of the bundle exists; retention is the only addition |
| `ResourceState.dependencies` records edges; Feature 3 computes dependants from the graph | `crates/tokeira-iac/src/document.rs`; Feature 3 | Dependency-path queries need the edges retained with the snapshot, and they are available at retention time |
| Definition edits between two sources are computable syntactically with spans (`definition_edits`), and config values compare host-free (`changed_config_values`) | Feature 4 (`tokeira-tkd::attribution`) | Revision comparison composes shipped primitives; no new diff machinery |
| `tkr` is the unprivileged CLI; `tkp` is the privileged, deployment-married provisioner; D1 forbids `tkp` listening | AGENTS.md architecture; umbrella D1 | The server's home is `tkr`, structurally |

## Target State

An operator (or their agent) runs `tkr analysis serve --deployment compose-explore` and
gets a read-only MCP server over that deployment's bundles; or asks one question with
`tkr analysis query get-change compose/grafana --json`. Every answer is traceable to a
bundle on disk; no query can touch a provider, the engine, live state, or `tkp`; and two
retained revisions can be compared — definition edits with spans, resource-level
consequences — without anything being realized at query time.

## Glossary

Terms additional to the umbrella and sibling glossaries:

- **Analysis Bundle** — the per-revision retained set: the explanation artifact, the
  definition source it corresponds to, and the desired snapshot with dependency edges.
- **Last-Plan Bundle** — the bundle produced by the most recent plan verb, at a
  well-known path, distinct from (and shaped identically to) per-revision bundles.
- **Bundle Root** — `state/analysis/` within the deployment directory: the only tree the
  analysis surface may read.
- **Query** — one named, read-only question with typed inputs and a typed answer or a
  typed not-found.
- **Transport** — how queries reach the library: MCP over stdio, or the one-shot CLI.
- **Comparison** — the revision-vs-revision report: definition edits (spans, enclosing
  constructs) plus per-resource desired differences.

## Query Surface Accounting

Every query, its input, its answer, and its not-found. No query outside this table exists
on the surface; adding one is a spec change.

| Query | Input | Answer | Not-found |
|---|---|---|---|
| `get_deployment_summary` | — | deployment, platform, operation, revisions, counts by change kind, destructive count, uncertainty count | n/a (bundle presence is the precondition) |
| `get_change_summary` | — | all changes: id, kind, cause class, impact membership | n/a |
| `get_change` | change or resource `EvidenceId` | the full `ExplainedChange` — evidence, semantics, cause, dependants, source | typed: id unknown, with the valid id shape named |
| `get_resource` | resource id | the resource's desired manifest from the snapshot + its change (if any) | typed: not in snapshot |
| `get_service` | service name | the service-kind resource's manifest + change | typed: no such service |
| `get_dependency_path` | from id, to id | the dependency path(s) between them over the retained edges | typed: no path, both endpoints echoed |
| `get_source_excerpt` | source location (file + line, from an attribution) | the surrounding definition lines from the bundle's retained definition | typed: location outside the retained definition |
| `get_uncertainties` | — | every uncertainty with subject, reason, consequence, resolution | n/a (empty set is a valid answer: full confirmation) |
| `compare_revisions` | revision N, revision M | definition edits (spans, constructs) + per-resource desired differences + summary counts | typed: revision not retained, with the retained revision list |

## Requirements

### Requirement 1: The analysis bundle

**User Story:** As the analysis surface, I want everything a query needs retained at
produce time, so that answering never requires realizing, refreshing, or invoking
anything.

#### Acceptance Criteria

1. WHEN an applying verb completes and retains its config revision THE provisioner SHALL
   also retain that revision's analysis bundle: the apply's explanation artifact, the
   definition source, and the canonical desired snapshot including each resource's
   dependency edges.
2. WHEN a plan verb completes THE provisioner SHALL retain the last-plan bundle at the
   well-known location under the bundle root, replacing the previous last-plan bundle.
3. THE bundle SHALL be readable without access to anything outside its own directory
   (self-containment inherited from the artifact, extended to the bundle).
4. THE bundle SHALL record the schema version of its explanation artifact.
5. WHERE the deployment has no definition source (a config-less local deployment) THE
   bundle SHALL retain what exists and record the absence, mirroring the existing
   retention's tolerance.
6. THE bundle lifecycle SHALL follow the revision retention it extends: a revision whose
   config snapshot is removed loses its bundle with it, and the last-plan bundle is
   disposable derived data.
7. IF bundle retention fails THEN THE verb SHALL fail with the path and reason, exactly
   as the artifact write does (Feature 1), and SHALL NOT report success with a missing
   bundle.

### Requirement 2: One query library, transport-agnostic

**User Story:** As a maintainer, I want the queries defined once as a typed API, so that
MCP and the one-shot CLI cannot drift apart.

#### Acceptance Criteria

1. THE query surface SHALL be implemented as a library whose API is exactly the Query
   Surface Accounting table.
2. THE library SHALL read only from the bundle root of the deployment it was opened for
   — everything a query needs, including the definition copies the comparison reads,
   lives in the bundles; the functional revision retention stays outside the analysis
   surface's confinement.
3. THE library SHALL NOT contact providers, SHALL NOT read or write engine state, SHALL
   NOT realize definitions, and SHALL NOT spawn processes.
4. WHEN a query names an unknown identifier THE library SHALL return the typed not-found
   from the accounting table, never an empty success.
5. WHEN the bundle's schema version is newer than the library supports THE library SHALL
   refuse with both versions named and the remedy (upgrade the reader).
6. WHERE the bundle's schema version is older but readable THE library SHALL answer, with
   absent-in-that-version fields reported as absent rather than defaulted.
7. THE library's answers SHALL be byte-faithful to the bundle: no recomputation,
   summarization, or enrichment of facts at query time, excepting the comparison (which
   composes retained inputs) and the excerpt (which slices the retained definition).

### Requirement 3: Revision comparison from bundles alone

**User Story:** As an operator, I want to compare two revisions of my deployment, so that
"what changed between 2 and 4" is answerable after the fact without re-planning.

#### Acceptance Criteria

1. WHEN two retained revisions are named THE comparison SHALL report the definition edits
   between their retained definitions — spans and enclosing constructs, via the Feature 4
   machinery — and the per-resource differences between their retained desired snapshots.
2. THE comparison SHALL classify per-resource differences as introduced, removed, or
   modified, with field-level differences for modified resources.
3. THE comparison SHALL be computed from the two bundles alone: no realization at query
   time.
4. IF a named revision is not retained THEN THE comparison SHALL refuse with the retained
   revision list.
5. WHERE the two revisions were retained under different platforms (different config
   basenames) THE comparison SHALL refuse and say so, mirroring the existing retention's
   cross-platform refusal.
6. THE comparison SHALL be deterministic and symmetric up to direction: comparing (N, M)
   SHALL report the same differences as (M, N) with before/after reversed.

### Requirement 4: The MCP transport

**User Story:** As an agent client, I want the queries as MCP tools over stdio, so that
the operator's existing agent subscription can navigate explanations with no new
credentials.

#### Acceptance Criteria

1. THE `tkr analysis serve` command SHALL serve the query surface as MCP tools over
   stdio for one named deployment.
2. THE served tool set SHALL contain exactly the queries in the accounting table: no
   tool that mutates, and no tool absent from the table.
3. THE server SHALL be `tkr`; `tkp` SHALL gain no serving verb, no socket, and no
   listening mode under this spec or its successors.
4. WHEN the deployment has no bundles THE server SHALL start, report the absence through
   its tool responses, and name the verbs that produce bundles.
5. THE server SHALL hold no lock that blocks provisioning verbs: serving analysis SHALL
   NOT prevent a concurrent plan or apply.
6. WHEN a bundle is replaced while the server runs THE server SHALL answer subsequent
   queries from the new bundle.

### Requirement 5: The one-shot transport

**User Story:** As an operator or CI job, I want single queries without a session, so
that the protocol is testable and scriptable with nothing but the CLI.

#### Acceptance Criteria

1. THE `tkr analysis query <name> [inputs]` command SHALL execute exactly one query from
   the accounting table and exit.
2. THE one-shot transport SHALL support `--json` emitting the query's answer as the
   complete typed value, and its narrative form SHALL follow the output contract.
3. THE two transports SHALL answer identically for identical queries over identical
   bundles.
4. WHEN the query name or inputs do not parse THE command SHALL refuse with the accounting
   table's query names.

### Requirement 6: Trust posture

**User Story:** As an operator, I want the analysis surface to be incapable of the things
it promises not to do, so that pointing an agent at it requires no faith.

#### Acceptance Criteria

1. THE analysis surface SHALL be structurally read-only: the library exposes no mutating
   operation for a transport to reach.
2. THE analysis surface SHALL resolve every file access within the bundle root and the
   retained revisions; a source location resolving outside them SHALL be refused as
   out-of-bundle.
3. THE analysis surface SHALL NOT transmit anything anywhere: both transports write only
   to their own stdout/stderr.
4. THE analysis surface serves definitions as authored; umbrella decision D7 is what
   makes that safe — a conforming definition carries no cleartext secret, only
   platform-secret references, which are not sensitive. (The shipped template's
   `admin_password` is a recorded defect against D7, resolved by the future
   platform-secrets work, not by this surface.)
5. THE server SHALL run with no ambient credentials: no provider configuration is read,
   loaded, or required.

### Requirement 7: Lexicon and rendering

**User Story:** As an operator, I want the analysis surface to speak the product's
language, so that a query answer reads like a report.

#### Acceptance Criteria

1. WHERE this feature introduces operator-facing vocabulary (analysis, bundle,
   comparison) THE change SHALL add those terms to `operator-language.md` in the same
   change.
2. THE one-shot narrative rendering SHALL render through `tokeira-report` under the
   existing depth rules.
3. THE MCP tool descriptions SHALL use lexicon vocabulary and SHALL state the surface's
   read-only nature.

## Notes

- **Retention is the load-bearing decision.** The naive design realizes definitions at
  query time, which drags platform crates (and their provider dependencies) into the
  analysis process and reopens every purity question this umbrella spent five specs
  closing. Retaining the snapshot at produce time makes the entire protocol a file
  reader.
- Requirement 4.3 writes the session's earlier agreement into enforceable text: the
  provisioner never listens. It is phrased to bind successors deliberately.
- Requirement 2.7 (byte-faithfulness) is what makes Feature 6's "facts must trace to
  evidence" enforceable: if the protocol re-derived facts at query time, a citation would
  name a moving target.
- The MCP dependency choice (an SDK crate vs. a minimal hand-rolled JSON-RPC loop) is an
  implementation decision flagged in the design; adding the dependency is architectural
  under the house rules and gets its explicit approval at implementation time, not here.
