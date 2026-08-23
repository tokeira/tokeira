//! Production adaptation between the contract-shaped interface and `aws_sdk_dsql`.

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use aws_sdk_dsql::operation::{
    create_cluster::{CreateClusterError, CreateClusterInput},
    delete_cluster::{DeleteClusterError, DeleteClusterInput},
    get_cluster::{GetClusterError, GetClusterInput},
    update_cluster::{UpdateClusterError, UpdateClusterInput},
};

use crate::control::{
    ClusterObservation, ClusterStatus, CreateClusterRequest, DeleteClusterRequest,
    DsqlControlError, DsqlControlPlane, RetryableErrorKind, SetDeletionProtectionRequest,
};

/// AWS SDK implementation bound to exactly one Region.
#[derive(Clone, Debug)]
pub struct AwsDsqlControlPlane {
    region: String,
    client: aws_sdk_dsql::Client,
}

impl AwsDsqlControlPlane {
    /// Wraps an SDK client whose configuration is already bound to `region`.
    pub fn new(region: impl Into<String>, client: aws_sdk_dsql::Client) -> Self {
        Self {
            region: region.into(),
            client,
        }
    }

    /// Loads the standard AWS credential/Region chain, overriding the Region explicitly.
    pub async fn from_region(region: impl Into<String>) -> Self {
        let region = region.into();
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()))
            .load()
            .await;
        Self::new(&region, aws_sdk_dsql::Client::new(&sdk_config))
    }

    fn require_region(&self, request: &str) -> Result<(), DsqlControlError> {
        if request == self.region {
            Ok(())
        } else {
            Err(DsqlControlError::RegionMismatch {
                adapter: self.region.clone(),
                request: request.to_owned(),
            })
        }
    }

    async fn get_after_update(
        &self,
        region: &str,
        cluster_id: &str,
    ) -> Result<ClusterObservation, DsqlControlError> {
        self.get_cluster(region, cluster_id).await
    }
}

#[async_trait]
impl DsqlControlPlane for AwsDsqlControlPlane {
    async fn create_cluster(
        &self,
        request: CreateClusterRequest,
    ) -> Result<ClusterObservation, DsqlControlError> {
        self.require_region(&request.region)?;
        request.validate()?;
        let input = build_create_input(&request)?;
        let output = self
            .client
            .create_cluster()
            .set_deletion_protection_enabled(input.deletion_protection_enabled)
            .set_tags(input.tags)
            .set_client_token(input.client_token)
            .send()
            .await
            .map_err(|error| map_create_error(error.as_service_error()))?;
        observation_from_create(&self.region, &output)
    }

    async fn get_cluster(
        &self,
        region: &str,
        cluster_id: &str,
    ) -> Result<ClusterObservation, DsqlControlError> {
        self.require_region(region)?;
        let input = build_get_input(cluster_id)?;
        let output = self
            .client
            .get_cluster()
            .set_identifier(input.identifier)
            .send()
            .await
            .map_err(|error| map_get_error(error.as_service_error()))?;
        observation_from_get(&self.region, &output)
    }

    async fn set_deletion_protection(
        &self,
        request: SetDeletionProtectionRequest,
    ) -> Result<ClusterObservation, DsqlControlError> {
        self.require_region(&request.region)?;
        let input = build_update_input(&request)?;
        self.client
            .update_cluster()
            .set_identifier(input.identifier)
            .set_deletion_protection_enabled(input.deletion_protection_enabled)
            .set_client_token(input.client_token)
            .send()
            .await
            .map_err(|error| map_update_error(error.as_service_error()))?;
        // UpdateCluster omits endpoint and protection in its response, so a canonical
        // GetCluster read is required instead of manufacturing a partial observation.
        self.get_after_update(&request.region, &request.cluster_id)
            .await
    }

    async fn delete_cluster(
        &self,
        request: DeleteClusterRequest,
    ) -> Result<ClusterStatus, DsqlControlError> {
        self.require_region(&request.region)?;
        let input = build_delete_input(&request)?;
        let output = self
            .client
            .delete_cluster()
            .set_identifier(input.identifier)
            .set_client_token(input.client_token)
            .send()
            .await
            .map_err(|error| map_delete_error(error.as_service_error()))?;
        Ok(map_status(output.status()))
    }
}

fn build_create_input(
    request: &CreateClusterRequest,
) -> Result<CreateClusterInput, DsqlControlError> {
    request.validate()?;
    CreateClusterInput::builder()
        .deletion_protection_enabled(request.deletion_protection_enabled)
        .set_tags((!request.tags.is_empty()).then(|| {
            request
                .tags
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<HashMap<_, _>>()
        }))
        .client_token(request.client_token.expose())
        .build()
        .map_err(|_| DsqlControlError::Unexpected {
            code: "CreateClusterInputBuild".to_owned(),
        })
}

fn build_get_input(cluster_id: &str) -> Result<GetClusterInput, DsqlControlError> {
    GetClusterInput::builder()
        .identifier(cluster_id)
        .build()
        .map_err(|_| input_build_error("GetClusterInputBuild"))
}

fn build_update_input(
    request: &SetDeletionProtectionRequest,
) -> Result<UpdateClusterInput, DsqlControlError> {
    UpdateClusterInput::builder()
        .identifier(&request.cluster_id)
        .deletion_protection_enabled(request.enabled)
        .client_token(request.client_token.expose())
        .build()
        .map_err(|_| input_build_error("UpdateClusterInputBuild"))
}

fn build_delete_input(
    request: &DeleteClusterRequest,
) -> Result<DeleteClusterInput, DsqlControlError> {
    DeleteClusterInput::builder()
        .identifier(&request.cluster_id)
        .client_token(request.client_token.expose())
        .build()
        .map_err(|_| input_build_error("DeleteClusterInputBuild"))
}

fn input_build_error(code: &str) -> DsqlControlError {
    DsqlControlError::Unexpected {
        code: code.to_owned(),
    }
}

fn observation_from_create(
    region: &str,
    output: &aws_sdk_dsql::operation::create_cluster::CreateClusterOutput,
) -> Result<ClusterObservation, DsqlControlError> {
    Ok(ClusterObservation {
        region: region.to_owned(),
        identifier: output.identifier().to_owned(),
        arn: output.arn().to_owned(),
        endpoint: output
            .endpoint()
            .ok_or_else(|| DsqlControlError::Unexpected {
                code: "CreateClusterMissingEndpoint".to_owned(),
            })?
            .to_owned(),
        status: map_status(output.status()),
        deletion_protection_enabled: output.deletion_protection_enabled(),
        multi_region: output.multi_region_properties().is_some(),
    })
}

fn observation_from_get(
    region: &str,
    output: &aws_sdk_dsql::operation::get_cluster::GetClusterOutput,
) -> Result<ClusterObservation, DsqlControlError> {
    Ok(ClusterObservation {
        region: region.to_owned(),
        identifier: output.identifier().to_owned(),
        arn: output.arn().to_owned(),
        endpoint: output
            .endpoint()
            .ok_or_else(|| DsqlControlError::Unexpected {
                code: "GetClusterMissingEndpoint".to_owned(),
            })?
            .to_owned(),
        status: map_status(output.status()),
        deletion_protection_enabled: output.deletion_protection_enabled(),
        multi_region: output.multi_region_properties().is_some(),
    })
}

fn map_status(status: &aws_sdk_dsql::types::ClusterStatus) -> ClusterStatus {
    use aws_sdk_dsql::types::ClusterStatus as AwsStatus;
    match status {
        AwsStatus::Creating => ClusterStatus::Creating,
        AwsStatus::Active => ClusterStatus::Active,
        AwsStatus::Idle => ClusterStatus::Idle,
        AwsStatus::Inactive => ClusterStatus::Inactive,
        AwsStatus::Updating => ClusterStatus::Updating,
        AwsStatus::Deleting => ClusterStatus::Deleting,
        AwsStatus::Deleted => ClusterStatus::Deleted,
        AwsStatus::Failed => ClusterStatus::Failed,
        AwsStatus::PendingSetup => ClusterStatus::PendingSetup,
        AwsStatus::PendingDelete => ClusterStatus::PendingDelete,
        other => ClusterStatus::Unknown(other.as_str().to_owned()),
    }
}

fn retry_after(value: Option<i32>) -> Option<Duration> {
    value
        .and_then(|seconds| u64::try_from(seconds).ok())
        .map(Duration::from_secs)
}

fn map_create_error(error: Option<&CreateClusterError>) -> DsqlControlError {
    match error {
        Some(CreateClusterError::ConflictException(_)) => {
            retryable(RetryableErrorKind::Conflict, None)
        }
        Some(CreateClusterError::ServiceQuotaExceededException(inner)) => {
            DsqlControlError::QuotaExceeded {
                service_code: inner.service_code().to_owned(),
                quota_code: inner.quota_code().to_owned(),
            }
        }
        Some(CreateClusterError::ValidationException(_)) => validation_from_aws(),
        Some(CreateClusterError::AccessDeniedException(_)) => DsqlControlError::AccessDenied,
        Some(CreateClusterError::InternalServerException(inner)) => retryable(
            RetryableErrorKind::Internal,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(CreateClusterError::ThrottlingException(inner)) => retryable(
            RetryableErrorKind::Throttling,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(_) => unexpected_aws(),
        None => retryable(RetryableErrorKind::Transport, None),
    }
}

fn map_get_error(error: Option<&GetClusterError>) -> DsqlControlError {
    match error {
        Some(GetClusterError::ResourceNotFoundException(_)) => DsqlControlError::NotFound,
        Some(GetClusterError::AccessDeniedException(_)) => DsqlControlError::AccessDenied,
        Some(GetClusterError::InternalServerException(inner)) => retryable(
            RetryableErrorKind::Internal,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(GetClusterError::ThrottlingException(inner)) => retryable(
            RetryableErrorKind::Throttling,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(GetClusterError::ValidationException(_)) => validation_from_aws(),
        Some(_) => unexpected_aws(),
        None => retryable(RetryableErrorKind::Transport, None),
    }
}

fn map_update_error(error: Option<&UpdateClusterError>) -> DsqlControlError {
    match error {
        Some(UpdateClusterError::ConflictException(_)) => {
            retryable(RetryableErrorKind::Conflict, None)
        }
        Some(UpdateClusterError::ResourceNotFoundException(_)) => DsqlControlError::NotFound,
        Some(UpdateClusterError::ValidationException(_)) => validation_from_aws(),
        Some(UpdateClusterError::AccessDeniedException(_)) => DsqlControlError::AccessDenied,
        Some(UpdateClusterError::InternalServerException(inner)) => retryable(
            RetryableErrorKind::Internal,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(UpdateClusterError::ThrottlingException(inner)) => retryable(
            RetryableErrorKind::Throttling,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(_) => unexpected_aws(),
        None => retryable(RetryableErrorKind::Transport, None),
    }
}

fn map_delete_error(error: Option<&DeleteClusterError>) -> DsqlControlError {
    match error {
        Some(DeleteClusterError::ConflictException(_)) => {
            retryable(RetryableErrorKind::Conflict, None)
        }
        Some(DeleteClusterError::ResourceNotFoundException(_)) => DsqlControlError::NotFound,
        Some(DeleteClusterError::AccessDeniedException(_)) => DsqlControlError::AccessDenied,
        Some(DeleteClusterError::InternalServerException(inner)) => retryable(
            RetryableErrorKind::Internal,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(DeleteClusterError::ThrottlingException(inner)) => retryable(
            RetryableErrorKind::Throttling,
            retry_after(inner.retry_after_seconds()),
        ),
        Some(DeleteClusterError::ValidationException(_)) => validation_from_aws(),
        Some(_) => unexpected_aws(),
        None => retryable(RetryableErrorKind::Transport, None),
    }
}

fn retryable(kind: RetryableErrorKind, retry_after: Option<Duration>) -> DsqlControlError {
    DsqlControlError::Retryable { kind, retry_after }
}

fn validation_from_aws() -> DsqlControlError {
    DsqlControlError::Validation {
        field: "AWS request",
        reason: "was rejected by Aurora DSQL",
    }
}

fn unexpected_aws() -> DsqlControlError {
    DsqlControlError::Unexpected {
        code: "UnhandledAwsServiceError".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::{build_create_input, build_delete_input, build_get_input, build_update_input};
    use crate::{
        control::{CreateClusterRequest, DeleteClusterRequest, SetDeletionProtectionRequest},
        descriptor::CreationClientToken,
    };

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 4: AWS request construction is complete and identity-neutral
        #[test]
        fn aws_request_construction_is_complete_and_identity_neutral(
            token in "[A-Za-z0-9-]{1,64}",
            tag_value in "[A-Za-z0-9 _-]{0,64}",
            operation in 0_u8..4
        ) {
            let mut tags = BTreeMap::new();
            tags.insert("purpose".to_owned(), tag_value.clone());
            let request = CreateClusterRequest {
                region: "eu-west-2".to_owned(),
                client_token: CreationClientToken::new(token.clone()).expect("generated token is valid"),
                deletion_protection_enabled: true,
                tags,
            };
            match operation {
                0 => {
                    let input = build_create_input(&request).expect("generated request is valid");
                    prop_assert_eq!(input.client_token(), Some(token.as_str()));
                    prop_assert_eq!(input.deletion_protection_enabled(), Some(true));
                    prop_assert_eq!(input.tags().and_then(|values| values.get("purpose")), Some(&tag_value));
                    prop_assert!(input.multi_region_properties().is_none());
                    prop_assert!(input.kms_encryption_key().is_none());
                    prop_assert!(input.policy().is_none());
                    prop_assert_eq!(input.bypass_policy_lockout_safety_check(), None);
                }
                1 => {
                    let input = build_get_input("abcdefghijklmnopqrstuv1234")
                        .expect("generated target is valid");
                    prop_assert_eq!(input.identifier(), Some("abcdefghijklmnopqrstuv1234"));
                }
                2 => {
                    let update = SetDeletionProtectionRequest {
                        region: request.region,
                        cluster_id: "abcdefghijklmnopqrstuv1234".to_owned(),
                        enabled: false,
                        client_token: request.client_token,
                    };
                    let input = build_update_input(&update).expect("generated update is valid");
                    prop_assert_eq!(input.identifier(), Some(update.cluster_id.as_str()));
                    prop_assert_eq!(input.client_token(), Some(token.as_str()));
                    prop_assert_eq!(input.deletion_protection_enabled(), Some(false));
                    prop_assert!(input.kms_encryption_key().is_none());
                    prop_assert!(input.multi_region_properties().is_none());
                }
                _ => {
                    let delete = DeleteClusterRequest {
                        region: request.region,
                        cluster_id: "abcdefghijklmnopqrstuv1234".to_owned(),
                        client_token: request.client_token,
                    };
                    let input = build_delete_input(&delete).expect("generated delete is valid");
                    prop_assert_eq!(input.identifier(), Some(delete.cluster_id.as_str()));
                    prop_assert_eq!(input.client_token(), Some(token.as_str()));
                }
            }
        }
    }
}
