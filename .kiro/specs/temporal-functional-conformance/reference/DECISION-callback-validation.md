# Decision note — callback validation (C5a) and compliance config

**For:** Codex, addressing FINDINGS cluster C5a.
**Status:** decided. Implement as below. No further approval needed for the constants; the one
deferred item (allowed-addresses) is called out explicitly.

---

## TL;DR

- **Implement the callback validation logic.** It is a real-gap: v1.31.0 rejects malformed
  completion callbacks with `InvalidArgument`; tokeira accepts them.
- **Do NOT add config fields.** Use the v1.31.0 default values as named, source-cited **constants**
  at the edge. Tokeira's posture is near-zero config (AGENTS.md), and — critically — the test's
  `OverrideDynamicConfig` calls **cannot reach an external `tokeirad`** (see "Seam limitation"
  below), so configurability buys zero conformance value here.
- **Defer allowed-address rules** as the one genuine deployment-policy decision; do not invent it now.

---

## The compliance-config principle (use this for every future cluster)

> A behaviour governed by a Temporal dynamic-config value defaults, in Tokeira, to the **v1.31.0
> default as a hardcoded, source-cited constant**. It becomes real Tokeira config **only if both**:
> (a) it is a genuine deployment-environment policy (security/topology), not a behavioural constant,
> **and** (b) leaving it fixed at the v1.31.0 default would be operationally wrong.
>
> When (a)+(b) hold, do **not** add the field silently — raise it as a deliberate config decision
> (per the FINDINGS implementer mandate, rule 3).

Applying the principle to the four callback knobs:

| Temporal setting (v1.31.0) | Default | Tokeira representation | Why |
|---|---|---|---|
| `frontend.callbackURLMaxLength` | `1000` | **constant** | behavioural limit, never varies |
| `frontend.callbackHeaderMaxLength` | `8192` (8×1024) | **constant** | behavioural limit, never varies |
| `system.maxCallbacksPerWorkflow` | `32` | **constant** | behavioural limit, never varies |
| `component.callbacks.allowedAddresses` | empty rule set | **deferred** (see below) | genuine deployment security policy |

Source: `common/dynamicconfig/constants.go @ v1.31.0` (lines ~988–1001) for the three int settings;
`components/callbacks/config.go @ v1.31.0` (line ~55) for `AllowedAddresses`.

---

## What to implement

A `validate_completion_callbacks` helper invoked from the `StartWorkflowExecution` (and
`SignalWithStartWorkflowExecution`) admission path in `tokeira-edge`. **Edge only — no kernel
change** (FINDINGS mandate rule 2). Ground-truth: `WorkflowHandler.validateWorkflowCompletionCallbacks`
and `validateCallbackURL` in `service/frontend/workflow_handler.go @ v1.31.0`, plus
`AddressMatchRules.Validate` / `AddressMatchRule.Allow` in `components/callbacks/config.go @ v1.31.0`.

Rules, in order, each returning `InvalidArgument` with the v1.31.0 message:

1. **Count cap:** `len(callbacks) > MAX_CALLBACKS_PER_WORKFLOW (32)` →
   `"cannot attach more than 32 callbacks to a workflow"`.
2. **Per callback (Nexus variant):**
   a. **URL length:** `len(url) > CALLBACK_URL_MAX_LENGTH (1000)` →
      `"invalid url: url length longer than max length allowed of 1000"`.
   b. **URL parse + scheme:** parse the URL; scheme must be `http` or `https`, else
      `"invalid url: unknown scheme: <u>"`; host must be non-empty, else
      `"invalid url: missing host"`.
   c. **Header size:** `Σ(len(k)+len(v)) > CALLBACK_HEADER_MAX_SIZE (8192)` →
      `"invalid header: header size longer than max allowed size of 8192"`.
      (v1.31.0 also lowercases header keys after this check — mirror that.)
3. **Internal callback variant:** v1.31.0 skips validation (nothing to check). Match that.

Message strings must match v1.31.0 **exactly** — the test asserts on `err.Error()`, not just the
code (`callbacks_test.go:158` `s.Equal(tc.message, err.Error())`).

The numeric literals in the messages should be derived from the constants (format string), not
double-typed, so the constant is the single source of truth.

---

## The deferred item: allowed-address rules

This is the **one** value that is genuine config, and it has a sharp edge:

- v1.31.0 default for `component.callbacks.allowedAddresses` is an **empty rule set**.
- An empty rule set means **every external URL is rejected** — only the internal
  `temporal://system` URL passes (`AddressMatchRules.Validate @ v1.31.0`).
- So a faithful "default config" Tokeira would reject **all** user-supplied callback URLs. That is
  operationally restrictive and is a deployment security decision, not a behavioural constant.

**Do not implement address-pattern matching now.** Implement rules 1–2b–2c above (which already
cover the `invalid-scheme`, `url-length-too-long`, `header-size-too-large`, and `too many callbacks`
test cases). Leave the two address-policy cases (`url not configured`, `https required`) unimplemented
and **raise the allowed-addresses config as a separate decision** when a real deployment needs
external callbacks. Note in the ledger that those two sub-cases are blocked on that decision.

---

## Seam limitation (record this; it is itself a finding)

`TestWorkflowCallbacks_InvalidArgument` calls `OverrideDynamicConfig` to set the limits to small
values (URL max 50, header 6, max 2 callbacks) so it can drive the boundaries. Under the Option-B
shim, `OverrideDynamicConfig` writes to the **in-process onebox's** memory dynamic-config client
(`onebox.go:1014 overrideDynamicConfig → c.dcClient.PartialOverrideValue`). The **external
`tokeirad` never reads that client**, so those overrides do not reach Tokeira.

Consequence: even with this validation implemented against the v1.31.0 default constants, the test's
boundary sub-cases (which rely on the *overridden* small limits) will **not** pass — tokeira will be
enforcing 1000/8192/32, not 50/6/2. The validation is still correct and worth shipping; the test is
just not a clean green-light oracle for it under Option-B.

**Ledger classification for this test:** split, per sub-case —
- `invalid-scheme` → real-gap (fixed by rule 2b; should pass once implemented, no override needed).
- `too many callbacks`, `url-length-too-long`, `header-size-too-large` → **harness-limited**: the
  validation exists but the test drives overridden limits the seam cannot deliver. Classify as a
  deliberate-deviation-from-test-setup / out-of-public-scope-of-the-seam with this note as the
  evidence, NOT as a tokeira real-gap.
- `url not configured`, `https required` → real-gap, **blocked** on the deferred allowed-addresses
  config decision; link this note.

Raising the seam limitation is the honest move (FINDINGS mandate rule 4): do not try to make the
overridden-limit sub-cases pass by lowering tokeira's constants below v1.31.0 defaults — that would
be non-conformant.

---


