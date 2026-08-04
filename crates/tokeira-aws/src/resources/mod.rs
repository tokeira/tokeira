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

use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, Disruption, LifecycleOperation,
    ProvisionContext, ReplacementPolicy, ResourceId, ResourceType, Reversibility, error::IacError,
};
use tokio::time::Instant;

// ── Change-semantics helpers ─────────────────────────────────────

/// Claims for a kind whose entire content is generated from the definition
/// and upserted whole: `Put*`-style create/update, idempotent delete,
/// nothing held is data of record (it regenerates on the next apply), and
/// every state is reachable again by re-applying a revision. The shape is
/// shared; the evidence is not — `upsert`/`delete` cite the calling kind's
/// own lifecycle fns. Update/delete disruption is deliberately an
/// inference: the engine fact is only that a control-plane call is issued,
/// and how consumers pick content up is the consumer's story.
pub fn generated_content_semantics(
    kind: ChangeKind,
    upsert: Citation,
    delete: Citation,
) -> ChangeSemantics {
    let claims = |operation: LifecycleOperation, live_read_inferred: bool, citation: Citation| {
        ChangeSemantics {
            operation: Confidence::EngineFact {
                value: operation,
                citation: citation.clone(),
            },
            replacement: Confidence::EngineFact {
                value: ReplacementPolicy::NotRequired,
                citation: citation.clone(),
            },
            disruption: if live_read_inferred {
                Confidence::Inference {
                    value: Disruption::None,
                    citation: citation.clone(),
                }
            } else {
                Confidence::EngineFact {
                    value: Disruption::None,
                    citation: citation.clone(),
                }
            },
            data_effect: Confidence::EngineFact {
                value: DataEffect::NoDataHeld,
                citation: citation.clone(),
            },
            reversibility: Confidence::EngineFact {
                value: Reversibility::Reversible,
                citation,
            },
            statement: None,
            provider_assigned: Vec::new(),
        }
    };
    match kind {
        ChangeKind::Create => claims(LifecycleOperation::Created, false, upsert),
        ChangeKind::Update | ChangeKind::Replace => {
            claims(LifecycleOperation::UpdatedInPlace, true, upsert)
        }
        ChangeKind::Delete => claims(LifecycleOperation::Deleted, true, delete),
        ChangeKind::NoChange => ChangeSemantics::default(),
    }
}

/// Claims for a control-plane-only kind: every lifecycle fn issues
/// non-disruptive control-plane calls, the resource holds no data of
/// record, updates reconcile in place, and any prior state is reachable
/// again by re-applying its revision. The shape is shared; the evidence is
/// not — each citation carries the calling kind's own API story. A kind
/// whose create records provider-assigned values sets `provider_assigned`
/// on the returned value.
pub fn control_plane_semantics(
    kind: ChangeKind,
    create: Citation,
    update: Citation,
    delete: Citation,
) -> ChangeSemantics {
    let claims = |operation: LifecycleOperation, citation: Citation| ChangeSemantics {
        operation: Confidence::EngineFact {
            value: operation,
            citation: citation.clone(),
        },
        replacement: Confidence::EngineFact {
            value: ReplacementPolicy::NotRequired,
            citation: citation.clone(),
        },
        disruption: Confidence::EngineFact {
            value: Disruption::None,
            citation: citation.clone(),
        },
        data_effect: Confidence::EngineFact {
            value: DataEffect::NoDataHeld,
            citation: citation.clone(),
        },
        reversibility: Confidence::EngineFact {
            value: Reversibility::Reversible,
            citation,
        },
        statement: None,
        provider_assigned: Vec::new(),
    };
    match kind {
        ChangeKind::Create => claims(LifecycleOperation::Created, create),
        ChangeKind::Update | ChangeKind::Replace => {
            claims(LifecycleOperation::UpdatedInPlace, update)
        }
        ChangeKind::Delete => claims(LifecycleOperation::Deleted, delete),
        ChangeKind::NoChange => ChangeSemantics::default(),
    }
}

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
