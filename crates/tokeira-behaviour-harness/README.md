# Behavioural evidence harness

This unpublished crate starts with the ledger because independent decision
comparisons need a durable record before deterministic scenario execution exists.
The next implementation stage adds typed scenarios and replay, the direct service
fixture, scripted workers, scheduling and lifecycle checks. Recorded corpus
calibration follows that work. This crate currently runs none of those layers.

`src/ledger/` owns evidence types, JSON loading, JUnit import, source declaration
checks, descriptor/catalog joins and stable Markdown rendering. The normal test in
`tests/ledger.rs` checks the [ledger page](../../docs/conformance/behavioural-ledger.md).
It runs in the workspace test job. Only development dependencies may reach this
crate. It alone may have a normal dependency on the independent model; no
production dependency chain may reach either crate.

Implementation dependencies are the model, public descriptors and compatibility
metadata, plus existing serialization, hashing, protobuf, XML, TOML and Rust token
libraries. `proc-macro2` and `syn` inspect test attributes without confusing strings
or comments for tests and without expanding property-test macros. `proptest` is a
development dependency. The workspace `toml` crate reads the comparison profile.
Every dependency is already in the workspace lockfile.

## Evidence and verdicts

`decision-evidence.json` is strict versioned JSON, one record per catalog key.
Counterparts, comparison test references, reachable RPC paths, intended verdicts,
reasons and correction commits are maintained by hand. Only the nested `run`
objects are written from execution. They contain per-test outcomes, the SHA-256 of
the JUnit bytes, and the SHA-256 of the complete typed rule serialized as compact
JSON. Rule hashing includes branch order, citations and model support, but ignores
JSON layout. Changing any rule fact makes prior evidence stale.

JUnit identity is the exact nextest library suite name (Cargo package) plus the
fully qualified testcase name. Integration-test binaries are not supported yet.
Unknown report tests are ignored, duplicate identities and reports matching no
comparison are errors. A partial report replaces every record: absent comparisons
become `missing`, so old successes cannot survive a partial run. Retries are disabled;
flaky or rerun outcomes are treated as errors. A JUnit failure records execution,
not its cause: diagnose infrastructure failures before assigning an ignored
difference. There is no inference of success from test registration.

`agrees` and `differed and fixed` require every named comparison to have passed.
`differs, ignored with reason` requires matching Rust ignore reasons and an
executed failure for every named ignored test. `not yet tested` claims no agreement.
Every other outcome renders `not yet tested` with an explanation. Source-only rules
retain their catalog label and reason. All current verdicts are provisional and
none constitutes calibrated scenario coverage. Reasons record branch gaps.

Check mode is read-only and the default. It rejects missing/unknown/duplicate
records, stale or absent rule digests, missing test declarations, ignore mismatches,
unknown RPC references, catalog/descriptor ownership drift and changed generated
blocks. It also requires the `behaviour-ledger` profile's `default-filter` to match
the sorted unique comparison names exactly, ignoring whitespace. A mismatch prints
the expected filter for replacement. It needs no local Temporal checkout.

```sh
CARGO_NET_OFFLINE=true cargo nextest run --locked -p tokeira-behaviour-harness -E 'test(=behavioural_ledger_matches_evidence)'
```

## Import a comparison run

Run from the repository root with the current catalog and implementations. Import
immediately without changing either: JUnit itself does not carry a catalog revision.
Keep the JUnit report if its digest must later be audited. No machine-local paths,
wall-clock times or production revision claims enter the generated page.

```sh
CARGO_NET_OFFLINE=true cargo nextest run --locked --profile behaviour-ledger --run-ignored all -p tokeira-runtime -p tokeira-edge
TOKEIRA_LEDGER_WRITE=1 TOKEIRA_LEDGER_JUNIT="target/nextest/behaviour-ledger/junit.xml" CARGO_NET_OFFLINE=true cargo nextest run --locked -p tokeira-behaviour-harness -E 'test(=behavioural_ledger_matches_evidence)'
CARGO_NET_OFFLINE=true cargo nextest run --locked -p tokeira-behaviour-harness -E 'test(=behavioural_ledger_matches_evidence)'
```

Nextest writes this report under its workspace-relative store directory,
`target/nextest`, independently of Cargo's compilation target directory.
A comparison run containing an intentional ignored difference exits nonzero.
Inspect its failures, then run import separately; do not connect the commands with
`&&`. Import uses `TOKEIRA_LEDGER_WRITE=1` exactly and an explicit
`TOKEIRA_LEDGER_JUNIT` path. It updates only nested run facts and generated blocks;
maintained facts keep their values and reference lists are sorted. After changing
a rule or comparison, rerun this profile and import the new report. When comparison
names change, update the profile filter to match the expected filter from check mode.
Notes outside `<!-- BEGIN GENERATED decisions -->`, `rpcs`, and `areas` marker pairs remain
unchanged. Check mode never refreshes evidence.

## Refresh the generated page

When only feature claims, descriptor metadata or rendering changes, refresh the
page from the existing evidence. Set `TOKEIRA_LEDGER_WRITE=1` and leave
`TOKEIRA_LEDGER_JUNIT` unset. This does not write `decision-evidence.json` or alter
its run facts. Rule digests and the comparison profile are still checked; stale
rules require the comparison run and import above.

```sh
TOKEIRA_LEDGER_WRITE=1 CARGO_NET_OFFLINE=true cargo nextest run --locked -p tokeira-behaviour-harness -E 'test(=behavioural_ledger_matches_evidence)'
CARGO_NET_OFFLINE=true cargo nextest run --locked -p tokeira-behaviour-harness -E 'test(=behavioural_ledger_matches_evidence)'
```

RPC rows derive every public descriptor method and join `FEATURE_MATRIX` with
`NEWER_VENDORED_WIRE_RPCS`. Decision reachability is maintained separately from
observed scenario calls. Scenarios, corpus, Tier 2, modes and evidence revisions
remain `none yet`; area campaigns remain `not yet measured`. Model support counts
refer to rules, not branch citations. No aggregate coverage percentage is computed.

```sh
CARGO_NET_OFFLINE=true cargo nextest run --locked -p tokeira-behaviour-harness -p tokeira-behaviour-model
```
