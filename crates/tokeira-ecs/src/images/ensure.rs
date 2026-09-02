//! ECR repository preparation derived from declared ECS image sources.
//!
//! Repository names come from each image's desired reference so provisioning
//! and later writeback share one naming contract.

use std::collections::HashMap;

use tokeira_aws::{EcrClient, ensure_ecr_repositories};
use tokeira_deploy_engine::{Image, ImageContext};
use tokeira_iac::{IacError, ProvisionContext};

pub async fn ensure_ecr_repositories_from_images(
    ecr: &dyn EcrClient,
    ctx: &ProvisionContext,
    images: &[&dyn Image],
    image_ctx: &ImageContext,
) -> Result<(), IacError> {
    let mut repos: Vec<(String, HashMap<String, String>)> = Vec::with_capacity(images.len());
    for image in images {
        let desired = image
            .desired_ref(image_ctx)
            .map_err(|err| IacError::Other(anyhow::anyhow!(err)))?;
        let tags = ctx.resource_tags(&desired.repository);
        repos.push((desired.repository, tags));
    }
    ensure_ecr_repositories(ecr, &repos)
        .await
        .map_err(|err| IacError::Other(anyhow::anyhow!(err)))
}
