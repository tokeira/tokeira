//! Connect-rust service implementation for Tokeira compatibility metadata.
//!
//! The compatibility matrix is owned by `tokeira-compatibility`; this crate is
//! deliberately only an adapter from that in-process model to the Buffa DTOs
//! exposed by `tokeira-compatibility-proto`.

use buffa::{EnumValue, MessageField};
use connectrpc::{ConnectError, RequestContext, Response, ServiceResult};
use tokeira_build_info::BuildInfo;
use tokeira_compatibility::{
    CompatibilityEvidenceKind, CompatibilitySurfaceKind, FEATURE_MATRIX, FeatureEntry,
    FeatureState, SDK_MATRIX, SdkCompatEntry, SdkVerificationState, feature_matrix_digest,
    sdk_matrix_digest,
};
use tokeira_compatibility_proto::connect::tokeira::compatibility::v1 as proto;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompatibilityServiceConfig {
    pub process_kind: String,
    pub process_identity: String,
}

#[derive(Clone, Debug)]
pub struct CompatibilityServiceHandler {
    config: CompatibilityServiceConfig,
}

impl CompatibilityServiceHandler {
    pub fn new(config: CompatibilityServiceConfig) -> Self {
        Self { config }
    }

    pub fn compatibility_response(&self) -> proto::GetCompatibilityResponse {
        proto::GetCompatibilityResponse {
            build_info: MessageField::some(build_info_to_proto(tokeira_build_info::summary())),
            features: FEATURE_MATRIX.iter().map(feature_to_proto).collect(),
            sdk_matrix: SDK_MATRIX.iter().map(sdk_entry_to_proto).collect(),
            process_kind: self.config.process_kind.clone(),
            process_identity: self.config.process_identity.clone(),
            feature_matrix_digest: feature_matrix_digest(),
            sdk_matrix_digest: sdk_matrix_digest(),
            ..Default::default()
        }
    }

    pub fn surfaces_response(&self) -> proto::ListCompatibilitySurfacesResponse {
        proto::ListCompatibilitySurfacesResponse {
            surfaces: FEATURE_MATRIX
                .iter()
                .flat_map(|feature| {
                    feature
                        .surfaces
                        .iter()
                        .map(move |surface| surface_to_proto(feature.id, surface))
                })
                .collect(),
            ..Default::default()
        }
    }

    pub fn feature_response(&self, feature_id: &str) -> Option<proto::GetFeatureResponse> {
        FEATURE_MATRIX
            .iter()
            .find(|feature| feature.id == feature_id)
            .map(|feature| proto::GetFeatureResponse {
                feature: MessageField::some(feature_to_proto(feature)),
                ..Default::default()
            })
    }

    pub fn sdk_response(&self, language: &str) -> proto::GetSdkCompatibilityResponse {
        proto::GetSdkCompatibilityResponse {
            sdk_matrix: SDK_MATRIX
                .iter()
                .filter(|entry| {
                    language.is_empty() || entry.language.eq_ignore_ascii_case(language)
                })
                .map(sdk_entry_to_proto)
                .collect(),
            ..Default::default()
        }
    }
}

impl proto::CompatibilityService for CompatibilityServiceHandler {
    async fn get_compatibility(
        &self,
        _ctx: RequestContext,
        _request: proto::OwnedGetCompatibilityRequestView,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::GetCompatibilityResponse> + Send> {
        Ok(Response::new(self.compatibility_response()))
    }

    async fn list_compatibility_surfaces(
        &self,
        _ctx: RequestContext,
        _request: proto::OwnedListCompatibilitySurfacesRequestView,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::ListCompatibilitySurfacesResponse> + Send>
    {
        Ok(Response::new(self.surfaces_response()))
    }

    async fn get_feature(
        &self,
        _ctx: RequestContext,
        request: proto::OwnedGetFeatureRequestView,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::GetFeatureResponse> + Send> {
        let Some(response) = self.feature_response(request.feature_id) else {
            return Err(ConnectError::not_found(format!(
                "compatibility feature `{}` not found",
                request.feature_id
            )));
        };
        Ok(Response::new(response))
    }

    async fn get_sdk_compatibility(
        &self,
        _ctx: RequestContext,
        request: proto::OwnedGetSdkCompatibilityRequestView,
    ) -> ServiceResult<impl connectrpc::Encodable<proto::GetSdkCompatibilityResponse> + Send> {
        Ok(Response::new(self.sdk_response(request.language)))
    }
}

fn build_info_to_proto(info: BuildInfo) -> proto::BuildInfo {
    proto::BuildInfo {
        tokeira_version: info.tokeira_version.to_string(),
        tokeira_git_sha: info.tokeira_git_sha.to_string(),
        temporal_proto_version: info.temporal_proto_version.to_string(),
        temporal_server_compat: info.temporal_server_compat.to_string(),
        rust_toolchain: info.rust_toolchain.to_string(),
        source_tree_hash: info.source_tree_hash.to_string(),
        feature_matrix_digest: info.feature_matrix_digest.to_string(),
        sdk_matrix_digest: info.sdk_matrix_digest.to_string(),
        build_mode: info.build_mode.to_string(),
        ..Default::default()
    }
}

fn feature_to_proto(feature: &FeatureEntry) -> proto::FeatureEntry {
    proto::FeatureEntry {
        id: feature.id.to_string(),
        name: feature.name.to_string(),
        state: EnumValue::from(feature_state_to_proto(feature.state)),
        surfaces: feature
            .surfaces
            .iter()
            .map(|surface| surface_to_proto(feature.id, surface))
            .collect(),
        capability_field: feature.capability_field.unwrap_or_default().to_string(),
        dynamic_config_key: feature.dynamic_config_key.unwrap_or_default().to_string(),
        rpcs: feature.rpcs.iter().map(|rpc| (*rpc).to_string()).collect(),
        notes: feature.notes.to_string(),
        evidence: feature.evidence.iter().map(evidence_to_proto).collect(),
        ..Default::default()
    }
}

fn surface_to_proto(
    feature_id: &str,
    surface: &tokeira_compatibility::CompatibilitySurface,
) -> proto::CompatibilitySurface {
    proto::CompatibilitySurface {
        kind: EnumValue::from(surface_kind_to_proto(surface.kind)),
        identifier: surface.identifier.to_string(),
        feature_id: feature_id.to_string(),
        ..Default::default()
    }
}

fn evidence_to_proto(
    evidence: &tokeira_compatibility::CompatibilityEvidence,
) -> proto::CompatibilityEvidence {
    proto::CompatibilityEvidence {
        kind: EnumValue::from(evidence_kind_to_proto(evidence.kind)),
        reference: evidence.reference.to_string(),
        ..Default::default()
    }
}

fn sdk_entry_to_proto(entry: &SdkCompatEntry) -> proto::SdkCompatEntry {
    proto::SdkCompatEntry {
        language: entry.language.to_string(),
        minimum_supported_version: entry.minimum_supported_version.to_string(),
        maximum_tested_version: entry.maximum_tested_version.to_string(),
        known_incompatible: entry
            .known_incompatible
            .iter()
            .map(|version| proto::IncompatibleVersion {
                version: version.version.to_string(),
                reason: version.reason.to_string(),
                ..Default::default()
            })
            .collect(),
        verification_state: EnumValue::from(sdk_state_to_proto(entry.verification_state)),
        evidence: entry.evidence.iter().map(evidence_to_proto).collect(),
        ..Default::default()
    }
}

fn feature_state_to_proto(state: FeatureState) -> proto::FeatureState {
    match state {
        FeatureState::Implemented => proto::FeatureState::FEATURE_STATE_IMPLEMENTED,
        FeatureState::Partial => proto::FeatureState::FEATURE_STATE_PARTIAL,
        FeatureState::Experimental => proto::FeatureState::FEATURE_STATE_EXPERIMENTAL,
        FeatureState::Stubbed => proto::FeatureState::FEATURE_STATE_STUBBED,
        FeatureState::Unsupported => proto::FeatureState::FEATURE_STATE_UNSUPPORTED,
    }
}

fn surface_kind_to_proto(kind: CompatibilitySurfaceKind) -> proto::CompatibilitySurfaceKind {
    match kind {
        CompatibilitySurfaceKind::Rpc => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_RPC
        }
        CompatibilitySurfaceKind::RequestField => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_REQUEST_FIELD
        }
        CompatibilitySurfaceKind::ResponseField => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_RESPONSE_FIELD
        }
        CompatibilitySurfaceKind::HistoryEvent => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_HISTORY_EVENT
        }
        CompatibilitySurfaceKind::CommandAttribute => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_COMMAND_ATTRIBUTE
        }
        CompatibilitySurfaceKind::EnumVariant => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_ENUM_VARIANT
        }
        CompatibilitySurfaceKind::CapabilityFlag => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_CAPABILITY_FLAG
        }
        CompatibilitySurfaceKind::ErrorDetail => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_ERROR_DETAIL
        }
        CompatibilitySurfaceKind::BehaviouralInvariant => {
            proto::CompatibilitySurfaceKind::COMPATIBILITY_SURFACE_KIND_BEHAVIOURAL_INVARIANT
        }
    }
}

fn evidence_kind_to_proto(kind: CompatibilityEvidenceKind) -> proto::CompatibilityEvidenceKind {
    match kind {
        CompatibilityEvidenceKind::Test => {
            proto::CompatibilityEvidenceKind::COMPATIBILITY_EVIDENCE_KIND_TEST
        }
        CompatibilityEvidenceKind::ManualReview => {
            proto::CompatibilityEvidenceKind::COMPATIBILITY_EVIDENCE_KIND_MANUAL_REVIEW
        }
        CompatibilityEvidenceKind::SdkConformance => {
            proto::CompatibilityEvidenceKind::COMPATIBILITY_EVIDENCE_KIND_SDK_CONFORMANCE
        }
    }
}

fn sdk_state_to_proto(state: SdkVerificationState) -> proto::SdkVerificationState {
    match state {
        SdkVerificationState::Untested => {
            proto::SdkVerificationState::SDK_VERIFICATION_STATE_UNTESTED
        }
        SdkVerificationState::SmokeTested => {
            proto::SdkVerificationState::SDK_VERIFICATION_STATE_SMOKE_TESTED
        }
        SdkVerificationState::ConformancePartial => {
            proto::SdkVerificationState::SDK_VERIFICATION_STATE_CONFORMANCE_PARTIAL
        }
        SdkVerificationState::ConformancePassing => {
            proto::SdkVerificationState::SDK_VERIFICATION_STATE_CONFORMANCE_PASSING
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_response_contains_static_matrices() {
        let handler = CompatibilityServiceHandler::new(CompatibilityServiceConfig {
            process_kind: "tokeirad".to_string(),
            process_identity: "node-a".to_string(),
        });

        let response = handler.compatibility_response();

        assert!(response.build_info.is_set());
        assert_eq!(response.process_kind, "tokeirad");
        assert_eq!(response.process_identity, "node-a");
        assert_eq!(response.features.len(), FEATURE_MATRIX.len());
        assert_eq!(response.sdk_matrix.len(), SDK_MATRIX.len());
        assert_eq!(response.feature_matrix_digest, feature_matrix_digest());
        assert_eq!(response.sdk_matrix_digest, sdk_matrix_digest());
    }

    #[test]
    fn feature_lookup_filters_by_id() {
        let handler = CompatibilityServiceHandler::new(CompatibilityServiceConfig {
            process_kind: "tokeirad".to_string(),
            process_identity: "node-a".to_string(),
        });

        let response = handler
            .feature_response("workflow-start")
            .expect("known feature");
        let feature = response.feature.as_option().expect("feature");

        assert_eq!(feature.id, "workflow-start");
        assert!(handler.feature_response("missing").is_none());
    }

    #[test]
    fn sdk_lookup_filters_by_language_case_insensitively() {
        let handler = CompatibilityServiceHandler::new(CompatibilityServiceConfig {
            process_kind: "tokeirad".to_string(),
            process_identity: "node-a".to_string(),
        });

        let response = handler.sdk_response("go");

        assert_eq!(response.sdk_matrix.len(), 1);
        assert_eq!(response.sdk_matrix[0].language, "Go");
    }
}
