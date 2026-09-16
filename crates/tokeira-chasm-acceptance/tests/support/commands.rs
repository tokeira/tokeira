//! The embedding application's composition of pure commands and the typed handle.
#![cfg(test)]

use tokeira_chasm::{BusinessIdPolicy, ExecutionKey};
use tokeira_chasm_acceptance::{
    Resource,
    commands::{self, AcceptanceError, ResourceView},
};
use tokeira_engine::chasm::{ChasmError, ComponentRef, DeploymentVersionTarget, TypedEngine};

pub(crate) async fn create(
    handle: &TypedEngine<Resource>,
    key: ExecutionKey,
    request_id: &str,
    digest: &str,
    target: DeploymentVersionTarget,
    task_queue: &str,
) -> Result<u64, AcceptanceError> {
    let started = handle
        .start_with(
            key,
            commands::create(request_id, digest, target, task_queue),
            Some(request_id.into()),
            BusinessIdPolicy::default(),
            |_, _| Ok(()),
        )
        .await;
    match started {
        Ok(started) if started.created => Ok(1),
        Ok(started) => {
            handle
                .read(&started.reference, |resource, _| {
                    Ok(commands::repeat_create(resource.state()?, digest))
                })
                .await?
        }
        Err(ChasmError::BusinessIdAlreadyStarted { run_id, .. }) => {
            Err(AcceptanceError::AlreadyStarted { run_id })
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn update(
    handle: &TypedEngine<Resource>,
    reference: &ComponentRef,
    expected_generation: u64,
    digest: &str,
) -> Result<u64, AcceptanceError> {
    let mut rejected = None;
    let result = handle
        .update(reference, |resource, ctx| {
            match commands::update(resource, expected_generation, digest, ctx) {
                Ok(generation) => Ok(generation),
                Err(AcceptanceError::Engine(error)) => Err(error),
                Err(error) => {
                    // TypedEngine errors abort before persist. Carry the domain error
                    // out separately instead of returning Ok and committing a no-op,
                    // or parsing a ChasmError message back into structured fields.
                    rejected = Some(error);
                    Err(ChasmError::Validation(
                        "resource command precondition failed".into(),
                    ))
                }
            }
        })
        .await;
    match rejected {
        Some(error) => Err(error),
        None => result.map(|(generation, _)| generation).map_err(Into::into),
    }
}

pub(crate) async fn read(
    handle: &TypedEngine<Resource>,
    reference: &ComponentRef,
) -> Result<ResourceView, AcceptanceError> {
    Ok(handle
        .read(reference, |resource, _| commands::read(resource))
        .await?)
}
