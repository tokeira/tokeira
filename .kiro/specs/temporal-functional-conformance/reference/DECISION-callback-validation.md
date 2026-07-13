# Decision note — callback validation (C5a) and compliance config

**For:** Tier 5.32 callback conformance.
**Status:** amended 2026-07-13 after the conformance override bridge landed. Implement as below.

---

## Decision

- Implement callback validation at workflow-start admission in `tokeira-edge`; it is public API
  validation, not a kernel concern.
- Keep the v1.31.0 defaults as named, source-cited constants:
  `frontend.callbackURLMaxLength = 1000`, `frontend.callbackHeaderMaxLength = 8192`, and
  `system.maxCallbacksPerWorkflow = 32` (`common/dynamicconfig/constants.go @ v1.31.0`).
- In `--features conformance` builds, read live overrides for those limits from the existing control
  bridge. The feature-off path continues to compile to the constants.
- Carry the structured `component.callbacks.allowedAddresses` override as JSON through the generic
  control service. The fork serializes the Go value; the edge validates its callback-rule schema and
  applies it at admission.
- Do not add production TOML/environment configuration. Production allowed-address policy remains a
  separate security/topology decision; without a conformance override, preserve Tokeira's current
  production address posture.

This supersedes the earlier claim that `OverrideDynamicConfig` could not reach external `tokeirad`.
The conformance control service and onebox bridge now provide that delivery path. The original reason
for classifying the limit-driven leaves as harness-limited therefore no longer applies.

## Ground truth

`WorkflowHandler.validateWorkflowCompletionCallbacks` and `validateCallbackURL` in
`service/frontend/workflow_handler.go @ v1.31.0` enforce, in order:

1. callback count (`system.maxCallbacksPerWorkflow`),
2. per-Nexus-callback URL length,
3. URL parse, scheme, and host,
4. allowed-address rules,
5. aggregate header size and lower-casing of header keys.

The exact default limits are declared in `common/dynamicconfig/constants.go @ v1.31.0`.
`components/callbacks/config.go @ v1.31.0` defines address-rule behavior:

- `Pattern` is matched against the URL host using a full wildcard match where `*` spans any host
  characters;
- a URL matching no rule is rejected;
- `http` is rejected unless the matching rule has `AllowInsecure = true`;
- system callback URLs are allowed independently of external-address rules.

## Admission behavior

For every workflow-start request carrying completion callbacks:

1. If the callback count exceeds the active limit, return `InvalidArgument` with
   `cannot attach more than <limit> callbacks to a workflow`.
2. For each Nexus callback:
   - if URL length exceeds the active limit, return
     `invalid url: url length longer than max length allowed of <limit>`;
   - reject a scheme other than `http`/`https` with `invalid url: unknown scheme: <url>`;
   - reject a missing host with `invalid url: missing host`;
   - when conformance address rules are present, reject a non-matching host with
     `invalid url: url does not match any configured callback address: <url>`;
   - when a matching rule disallows insecure transport, reject `http` with
     `invalid url: callback address does not allow insecure connections: <url>`;
   - if total header key/value bytes exceed the active limit, return
     `invalid header: header size longer than max allowed size of <limit>`;
   - lower-case header keys after validation.
3. Internal callback variants have no URL/header validation, matching v1.31.0.

Message text is part of the compatibility contract because `tests/callbacks_test.go @ v1.31.0`
compares the returned error string.

## Structured override transport

The generic conformance proto gains `json_value`. `OverrideValue::Json(String)` and
`ValueType::Json` preserve that text losslessly; the registry does not interpret setting-specific
schemas. The Temporal fork's bridge uses `json.Marshal` for composite values that are not one of the
existing scalar kinds. `tokeira-edge` owns deserialization of the callback rule list because it owns
the consult site and its validation errors.

This is test-only transport, compiled out without the `conformance` feature. It is not a general
production dynamic-config facility and does not weaken the config-as-constant policy.

## Production policy remains open

The v1.31.0 default allowed-address list is empty, which rejects every external callback URL. That is
a genuine deployment security decision rather than a neutral behavioral limit. Tier 5.32 wires the
corpus's explicit override and proves Temporal-compatible rule behavior, but deliberately does not
choose a production configuration model. Until that separate decision is made, feature-off Tokeira
retains its existing accept-after-shape-validation posture. This bounded deviation must remain visible
in the conformance ledger.

## Validation

- Property tests cover scalar/JSON override type fidelity and lifecycle.
- Edge tests cover wildcard full matching, `AllowInsecure`, the no-match case, and exact error text.
- The pinned corpus `TestCallbacksSuiteHSM` must pass twice consecutively.
- `TestCallbacksSuiteCHASM` is an exact top-level classified skip: CHASM framework internals are
  outside the default v1.31.0 compatibility gate (`docs/conformance/v1.31.0/excluded.md`).

