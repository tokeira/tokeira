//! Generated commands against an independent generation model, without executors.
#![cfg(test)]

#[path = "support/commands.rs"]
mod commands;

use std::sync::Arc;

use proptest::prelude::*;
use tokeira_chasm::{ComponentRef, ExecutionKey, Registry, archetype_id_for_fqn};
use tokeira_chasm_acceptance::{AcceptanceLibrary, Resource, commands::AcceptanceError};
use tokeira_engine::chasm::{ChasmError, Component, DeploymentVersionTarget, Library, TypedEngine};
use tokeira_runtime::chasm::{ChasmEngine, CollectingDispatchSink, CollectingVisibilitySink};
use tokeira_storage::{ChasmNodeRepository, InMemoryChasmNodeStore};

#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Generation(u64),
    Conflict,
    AlreadyStarted(String),
    Mismatch(u64, u64),
    Missing,
}

fn answer(result: Result<u64, AcceptanceError>) -> Answer {
    match result {
        Ok(generation) => Answer::Generation(generation),
        Err(AcceptanceError::Conflict) => Answer::Conflict,
        Err(AcceptanceError::AlreadyStarted { run_id }) => Answer::AlreadyStarted(run_id),
        Err(AcceptanceError::GenerationMismatch { expected, actual }) => {
            Answer::Mismatch(expected, actual)
        }
        Err(AcceptanceError::Engine(ChasmError::ExecutionNotFound)) => Answer::Missing,
        Err(other) => panic!("unexpected command error: {other}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: chasm-extension-archetypes, Property 15: acceptance reference model
    // Command replies and reads follow generation/idempotency rules after every step.
    #[test]
    fn commands_match_the_generation_model(script in prop::collection::vec((0u8..3, 0u8..3, 0u8..3, -1i8..2), 1..65)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let store = Arc::new(InMemoryChasmNodeStore::new());
            let mut builder = Registry::builder();
            AcceptanceLibrary::register(&mut builder).unwrap();
            let engine = Arc::new(ChasmEngine::new(store.clone(), Arc::new(builder.build()), Arc::new(CollectingDispatchSink::default()), Arc::new(CollectingVisibilitySink::default())).with_clock(Arc::new(|| 100)));
            let handle = TypedEngine::<Resource>::new(engine);
            let mut created: Option<(String, String, ExecutionKey)> = None;
            let mut generation = 0u64;
            for (index, (action, request, digest, offset)) in script.into_iter().enumerate() {
                let proposed = ExecutionKey::new("ns", "resource", format!("candidate-{index}"));
                let existing = created.as_ref().map(|(_, _, key)| key.clone()).unwrap_or_else(|| proposed.clone());
                let reference = ComponentRef::new(existing, archetype_id_for_fqn(Resource::FQN), Default::default(), vec![], Default::default());
                let digest = format!("digest-{digest}");
                match action {
                    0 => {
                        let request = format!("request-{request}");
                        let expected = match &created {
                            Some((original, _, key)) if original != &request => Answer::AlreadyStarted(key.run_id.clone()),
                            Some((_, original_digest, _)) if original_digest != &digest => Answer::Conflict,
                            Some(_) => Answer::Generation(1),
                            None => { created = Some((request.clone(), digest.clone(), proposed.clone())); generation = 1; Answer::Generation(1) }
                        };
                        let result = commands::create(&handle, proposed, &request, &digest, DeploymentVersionTarget { deployment_name: "acceptance".into(), build_id: "v1".into() }, "queue").await;
                        prop_assert_eq!(answer(result), expected);
                    }
                    1 => {
                        let expected_generation = generation.saturating_add_signed(i64::from(offset));
                        let expected = if created.is_none() { Answer::Missing } else if expected_generation != generation { Answer::Mismatch(expected_generation, generation) } else { generation += 1; Answer::Generation(generation) };
                        prop_assert_eq!(answer(commands::update(&handle, &reference, expected_generation, &digest).await), expected);
                    }
                    _ => {
                        let result = commands::read(&handle, &reference).await.map(|view| view.desired_generation);
                        prop_assert_eq!(answer(result), if created.is_some() { Answer::Generation(generation) } else { Answer::Missing });
                    }
                }
                if let Some((_, _, key)) = &created {
                    let reference = ComponentRef::new(key.clone(), archetype_id_for_fqn(Resource::FQN), Default::default(), vec![], Default::default());
                    let view = commands::read(&handle, &reference).await.unwrap();
                    prop_assert_eq!(view.desired_generation, generation);
                    prop_assert_eq!(view.observed_generation, 1);
                    prop_assert!(view.history.is_empty());
                    prop_assert!(view.last_failure.is_empty());
                    prop_assert_eq!(view.retry_attempt, 0);
                    prop_assert_eq!(view.status, if generation == 1 { "Converged" } else { "Reconciling" });
                    prop_assert_eq!(view.active_operation.as_ref().map(|operation| operation.generation), (generation > 1).then_some(2));
                    prop_assert_eq!(store.scan_executions().await.unwrap().len(), 1);
                    let current = store.current_run("ns", archetype_id_for_fqn(Resource::FQN), "resource").await.unwrap().unwrap();
                    prop_assert_eq!(&current.run_id, &key.run_id);
                }
            }
            Ok(())
        })?;
    }
}
