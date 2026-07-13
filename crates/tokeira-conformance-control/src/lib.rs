//! Conformance-only Connect-RPC control service for dynamic-config overrides.
//!
//! Bridges the `ConformanceControlService` RPC surface
//! (`tokeira-conformance-proto`) to the process-global override registry
//! (`tokeira-conformance`). It is mounted only in a `conformance`-feature build,
//! on a separate loopback listener — never the production gRPC router — and it
//! honours only wired, non-kernel keys. Every other override is rejected with a
//! Connect status the fork bridge treats as a signal to fall back to the skip
//! registry, never a silent no-op (spec `.kiro/specs/conformance-config-override/`).

// The service impl returns the owned response type, refining the trait's
// `impl Encodable<_>` return bound; the generated proto crate carries the same allow.
#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]

use std::time::Duration;

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use tokeira_conformance::{OverrideError, OverrideValue, overrides};
use tokeira_conformance_proto::connect::tokeira::conformance::v1 as proto;

use proto::__buffa::view::oneof::dynamic_config_value::Kind;

/// Connect-RPC handler backing the conformance control service.
///
/// Stateless: it reads and mutates the process-global override registry
/// ([`tokeira_conformance::overrides`]). Constructed only by a conformance-mode
/// `tokeirad` mount.
#[derive(Clone, Debug, Default)]
pub struct ConformanceControlHandler;

impl proto::ConformanceControlService for ConformanceControlHandler {
    async fn set_dynamic_config_override(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::SetDynamicConfigOverrideRequest>,
    ) -> ServiceResult<proto::SetDynamicConfigOverrideResponse> {
        // Borrowed (zero-copy) view; fields are read directly. The owned
        // `OverrideValue` is built before the request borrow ends, so nothing
        // borrowed escapes into the registry.
        let view = request.view();
        let key = view.key;
        let value = view.value.as_option().ok_or_else(|| {
            ConnectError::invalid_argument("SetDynamicConfigOverride requires a value")
        })?;
        let override_value = match value.kind.as_ref() {
            Some(Kind::IntValue(v)) => OverrideValue::Int(*v),
            Some(Kind::DoubleValue(v)) => OverrideValue::Double(*v),
            Some(Kind::BoolValue(v)) => OverrideValue::Bool(*v),
            Some(Kind::StringValue(v)) => OverrideValue::Text((*v).to_string()),
            // The wire carries a duration as integer nanoseconds; a negative
            // value is clamped to zero (durations are non-negative).
            Some(Kind::DurationNanos(v)) => {
                OverrideValue::Duration(Duration::from_nanos((*v).max(0) as u64))
            }
            Some(Kind::JsonValue(v)) => OverrideValue::Json((*v).to_string()),
            None => {
                return Err(ConnectError::invalid_argument(
                    "SetDynamicConfigOverride value has no kind set",
                ));
            }
        };
        overrides()
            .set(key, override_value)
            .map_err(override_error_to_connect)?;
        Ok(Response::new(
            proto::SetDynamicConfigOverrideResponse::default(),
        ))
    }

    async fn clear_dynamic_config_override(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::ClearDynamicConfigOverrideRequest>,
    ) -> ServiceResult<proto::ClearDynamicConfigOverrideResponse> {
        overrides().clear(request.view().key);
        Ok(Response::new(
            proto::ClearDynamicConfigOverrideResponse::default(),
        ))
    }

    async fn reset_dynamic_config_overrides(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, proto::ResetDynamicConfigOverridesRequest>,
    ) -> ServiceResult<proto::ResetDynamicConfigOverridesResponse> {
        overrides().reset();
        Ok(Response::new(
            proto::ResetDynamicConfigOverridesResponse::default(),
        ))
    }
}

/// Map a registry rejection to the Connect status the fork bridge keys on.
///
/// Unwired / kernel-excluded / not-enforced keys are `unimplemented` (the
/// override is not honourable); a type mismatch or empty key is
/// `invalid_argument`. Either way the fork bridge falls back to the skip
/// registry — never a silent no-op — per the honesty boundary.
fn override_error_to_connect(error: OverrideError) -> ConnectError {
    match error {
        OverrideError::UnknownKey(_)
        | OverrideError::KernelExcluded(_)
        | OverrideError::NotEnforced(_) => ConnectError::unimplemented(error.to_string()),
        OverrideError::TypeMismatch { .. } | OverrideError::MissingKey => {
            ConnectError::invalid_argument(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use connectrpc::ErrorCode;
    use tokeira_conformance::{ConformanceOverrides, OverrideValue};

    use super::override_error_to_connect;

    const REPORTED_PROBLEMS_KEY: &str =
        "system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute";

    // Feature: conformance-config-override, Property 4: honesty boundary (service edge).
    // A wired key set with a matching value kind is accepted; the handler surfaces no
    // error to map, so the RPC succeeds.
    #[test]
    fn wired_valid_override_is_accepted() {
        let overrides = ConformanceOverrides::default();
        let result = overrides
            .set(REPORTED_PROBLEMS_KEY, OverrideValue::Int(2))
            .map_err(override_error_to_connect);
        assert!(result.is_ok());
    }

    // Feature: conformance-config-override, Property 3: value-type fidelity (service edge).
    // A value whose kind does not match the key's declared type maps to invalid_argument.
    #[test]
    fn mismatched_value_kind_maps_to_invalid_argument() {
        let overrides = ConformanceOverrides::default();
        let err = overrides
            .set(REPORTED_PROBLEMS_KEY, OverrideValue::Bool(true))
            .map_err(override_error_to_connect)
            .expect_err("type mismatch must be rejected");
        assert!(matches!(err.code, ErrorCode::InvalidArgument));
    }

    // Feature: conformance-config-override, Property 4: honesty boundary (service edge).
    // Unknown, kernel-excluded, and not-enforced keys all map to unimplemented — the
    // status the fork bridge treats as "fall back to the skip registry", never a
    // silent no-op.
    #[test]
    fn non_wired_keys_map_to_unimplemented() {
        let overrides = ConformanceOverrides::default();
        for key in [
            "nope.not.a.key",                         // unknown
            "history.workflowIdReuseMinimalInterval", // kernel-excluded
            "limit.blobSize.error",                   // not-enforced
        ] {
            let err = overrides
                .set(key, OverrideValue::Int(1))
                .map_err(override_error_to_connect)
                .expect_err("non-wired key must be rejected");
            assert!(
                matches!(err.code, ErrorCode::Unimplemented),
                "{key} should map to unimplemented",
            );
        }
    }

    // Feature: conformance-config-override, Property 4: honesty boundary (service edge).
    // An empty key is invalid_argument.
    #[test]
    fn empty_key_maps_to_invalid_argument() {
        let overrides = ConformanceOverrides::default();
        let err = overrides
            .set("", OverrideValue::Int(1))
            .map_err(override_error_to_connect)
            .expect_err("empty key must be rejected");
        assert!(matches!(err.code, ErrorCode::InvalidArgument));
    }
}
