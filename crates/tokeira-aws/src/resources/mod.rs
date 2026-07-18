//! Concrete AWS resource implementations.
//!
//! Each submodule implements `tokeira_iac::Resource` for one AWS resource type.
//! Tag conversion helpers and the `poll_until` utility live here so resource
//! implementations can reference them via `super::`.

pub mod dsql_cluster;
pub mod dsql_connection_endpoint;
pub mod dynamodb_table;
pub mod ebs_volume;
pub mod ec2_instance;
pub mod ecr_repository;
pub mod ecs_cluster;
pub mod ecs_service;
pub mod eks;
pub mod elbv2;
pub mod iam_instance_profile;
pub mod iam_role;
pub mod pod_identity_association;
pub mod s3_bucket;
pub mod s3_object;
pub mod secrets_manager_secret;
pub mod security_group;
pub mod ssm_parameter;
pub mod vpc;
pub mod vpc_endpoint;

use std::{collections::HashMap, future::Future, time::Duration};

use tokeira_iac::{ProvisionContext, ResourceId, ResourceType, error::IacError};
use tokio::time::Instant;

// ── Poll helper ──────────────────────────────────────────────────

#[derive(Debug)]
pub struct PollTarget<'a> {
    pub resource_desc: &'a str,
    pub resource_id: &'a ResourceId,
    pub resource_type: ResourceType,
    pub phase: &'a str,
}

/// Poll an async condition at `interval` until it returns `Ok(true)` or `timeout` expires.
pub async fn poll_until<F, Fut>(
    interval: Duration,
    timeout: Duration,
    ctx: &ProvisionContext,
    target: PollTarget<'_>,
    mut check: F,
) -> Result<(), IacError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, IacError>>,
{
    let start = Instant::now();
    let deadline = start + timeout;
    loop {
        if check().await? {
            return Ok(());
        }
        let elapsed = Instant::now().saturating_duration_since(start);
        ctx.emit_wait_progress(
            target.resource_id,
            &target.resource_type,
            target.phase,
            elapsed,
            timeout,
        );
        if Instant::now() + interval > deadline {
            return Err(IacError::ResourceWaitTimedOut {
                resource_type: target.resource_desc.to_string(),
                resource_id: String::new(),
                details: format!("timed out after {:?}", timeout),
            });
        }
        tokio::time::sleep(interval).await;
    }
}

// ── Tag conversion helpers ───────────────────────────────────────

/// Convert a tag map to EC2 tag format.
pub fn ec2_tags(tags: &HashMap<String, String>) -> Vec<aws_sdk_ec2::types::Tag> {
    tags.iter()
        .map(|(k, v)| aws_sdk_ec2::types::Tag::builder().key(k).value(v).build())
        .collect()
}

/// Convert a tag map to IAM tag format.
pub fn iam_tags(tags: &HashMap<String, String>) -> Vec<aws_sdk_iam::types::Tag> {
    tags.iter()
        .map(|(k, v)| {
            aws_sdk_iam::types::Tag::builder()
                .key(k)
                .value(v)
                .build()
                .expect("key and value are set")
        })
        .collect()
}

/// Convert a tag map to ECR tag format.
pub fn ecr_tags(tags: &HashMap<String, String>) -> Vec<aws_sdk_ecr::types::Tag> {
    tags.iter()
        .map(|(k, v)| {
            aws_sdk_ecr::types::Tag::builder()
                .key(k)
                .value(v)
                .build()
                .expect("key and value are set")
        })
        .collect()
}

/// Convert a tag map to S3 tag format.
pub fn s3_tags(tags: &HashMap<String, String>) -> Vec<aws_sdk_s3::types::Tag> {
    tags.iter()
        .map(|(k, v)| {
            aws_sdk_s3::types::Tag::builder()
                .key(k)
                .value(v)
                .build()
                .expect("key and value are set")
        })
        .collect()
}

/// Convert a tag map to DynamoDB tag format.
pub fn dynamodb_tags(tags: &HashMap<String, String>) -> Vec<aws_sdk_dynamodb::types::Tag> {
    tags.iter()
        .map(|(k, v)| {
            aws_sdk_dynamodb::types::Tag::builder()
                .key(k)
                .value(v)
                .build()
                .expect("key and value are set")
        })
        .collect()
}

/// Convert a tag map to Elastic Load Balancing v2 tag format.
pub fn elbv2_tags(
    tags: &HashMap<String, String>,
) -> Result<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>, IacError> {
    Ok(tags
        .iter()
        .map(|(k, v)| {
            aws_sdk_elasticloadbalancingv2::types::Tag::builder()
                .key(k)
                .value(v)
                .build()
        })
        .collect())
}

/// Convert a tag map to Secrets Manager tag format.
pub fn secretsmanager_tags(
    tags: &HashMap<String, String>,
) -> Vec<aws_sdk_secretsmanager::types::Tag> {
    tags.iter()
        .map(|(k, v)| {
            aws_sdk_secretsmanager::types::Tag::builder()
                .key(k)
                .value(v)
                .build()
        })
        .collect()
}
