# Decision note — dynamic config under the Tier-2 seam

**For:** Codex (and any implementer) triaging tests that call `OverrideDynamicConfig`.
**Status:** decided. This is the standing rule; apply it to every `OverrideDynamicConfig`-dependent
test. Supersedes any per-cluster guesswork about "the override didn't reach tokeirad."

---

## The fact this rule exists to handle

Temporal's functional tests set behaviour via `s.OverrideDynamicConfig(setting, value)`. Under the
Option-B seam that write lands in the **in-process onebox's** `MemoryClient.overrides` slice
(`onebox.go:1014` → `dynamicconfig/memory_client.go @ v1.31.0`), which is injected into Temporal's
own services via fx. Under Option-B those services are **not booted**, and the external `tokeirad`
is a separate process with no handle to that slice. So the override is written correctly but has
**no reader** — it never reaches tokeirad.

The wrong response is "build a bridge so overrides reach tokeirad." The right response is to ask
what the override actually *does*, because most overrides need no bridge at all.

---

## The triage rule: classify the setting, not the delivery

The seam's inability to deliver an override only matters **iff** the override induces a behaviour
tokeira must reproduce to be compliant. Classify every overridden setting:

### Class 1 — tuning / limit knobs (numeric thresholds, rates, sizes)

The *behaviour shape* is identical regardless of value; only the boundary moves. Examples:
`callbackURLMaxLength`, `callbackHeaderMaxLength`, `maxCallbacksPerWorkflow`, blob/history size
limits, history-count limits.

- Tokeira holds the **v1.31.0 default as a hardcoded, source-cited constant** (see
  `DECISION-callback-validation.md` and the FINDINGS implementer mandate, rule 3).
- Tokeira at the default exhibits the correct behaviour; it simply trips at the default boundary, not
  the test's lowered one.
- **Classification: harness-limited, NOT a tokeira gap.** The test lowered the limit to make the
  boundary cheap to hit; tokeira enforcing the real default is conformant.
- **Never** lower a tokeira constant below the v1.31.0 default to satisfy such a test — that would be
  non-conformant. Record the limitation; move on. **No bridge.**

### Class 2 — mode / feature switches (booleans, enums selecting a code path or semantic)

The override changes *what the server does*, not a magnitude. Split further:

- **2a — default-on.** v1.31.0 ships the behaviour on by default; the override is just explicit.
  Tokeira implements it as fixed default behaviour and is compliant in the default run. No bridge.
- **2b — default-off, test flips it on, AND tokeira claims the mode.** This is the only case the
  seam genuinely blocks. It is **not** handled by per-test override delivery. See "Features are
  independent runs" below.
- **2b' — default-off mode tokeira does NOT claim.** Out-of-claim. Classify accordingly
  (out-of-public-scope / deliberate-deviation with rationale). No run, no bridge.

---

## Features are independent runs, not per-test overrides

A Class-2b mode tokeira claims is a **distinct tokeirad configuration**, so it gets its **own
conformance run** — not an override injected into the default run:

1. Boot `tokeirad` with the feature/mode enabled (its own config/profile, set at boot).
2. Run the corpus — or just the feature's suite subset — against that tokeirad.
3. Produce a **separate** results + ledger, tagged with the mode.

This matches how tokeira represents config generally: near-zero, set-at-boot, restart-to-change
(AGENTS). The mode is fixed for the whole run, so there is **no dynamic-config bridge, no test-only
injection RPC, no per-test override delivery**. Each run's claim stays honest:

- the **default run** proves the v1.31.0 default contract;
- a **feature run** proves that feature's contract in its own configuration.

They never coexist in one process, which is exactly why no bridge is needed.

---

## What the harness needs for this (small, not a bridge)

The run-all executor (`tests/tokeira_conformance_runall`) and the distiller
(`tests/tokeira_conformance_ledger`) currently assume a **single default run** and write to fixed
output paths. To support feature runs without collision they need a **per-run profile tag**:

- a run label (e.g. `default`, `feature-worker-deployment`) threaded into the results/outcomes output
  path, and into the ledger key, so a feature run's artifacts do not overwrite the default run's.

That is the entire harness change required — a label, not a config-delivery channel. Do **not** build
a dynamic-config injection path into tokeirad unless and until a claimed Class-2b mode appears that
genuinely requires it; even then, the independent-run model above is preferred.

---

## Applying it to current FINDINGS clusters

- **C5a callbacks** — all **Class 1** (URL length, header size, max-callbacks). Harness-limited;
  default-as-constant; no bridge. (Already covered in `DECISION-callback-validation.md`.)
- **C2 worker-deployment / versioning** — **verify the setting class before classifying.**
  `FailedPrecondition: registry not configured` smells like a feature-enablement (Class 2) path, not
  a limit. If tokeira claims worker-deployment, this is a candidate **feature run** (2b), not a
  default-run gap. Confirm against v1.31.0 before writing the ledger entry.
- **C3 advanced visibility** — partly gated behaviour; **verify the setting class.** Some sub-cases
  may be Class-2b feature-run material rather than default-run real-gaps.

When in doubt about a setting's class, resolve it against v1.31.0 (`dynamicconfig/constants.go` for
the default; the consuming service source for whether it gates a code path vs a boundary) and raise
the question rather than guessing (FINDINGS mandate, rule 4).
