//! Embedded assembly excludes internal activity offers without consulting peer files.

use std::sync::Arc;

use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_config::peer::PeerConfig;
use tokeira_storage::InMemoryStore;

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 4: no-peer assembly needs no file and exposes no activity offer route, even for malformed requests.
    #[test]
    fn activity_offer_no_peer_bootstrap(missing_file in any::<bool>(), bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let mut config = crate::TokeiraConfig::default();
            config.infrastructure.peer = missing_file.then(|| PeerConfig { path: "/missing-activity-peer-credentials".into() });
            let crate::ConstructedStack::Embedded(mut stack) = crate::build_in_memory_stack(
                crate::StackTransport::Embedded, Arc::new(config), InMemoryStore::default(), false,
                crate::ChasmExtensions::default(), None, Arc::new(FixedClock),
            ).await.unwrap() else { panic!("embedded assembly"); };
            let error = stack.service.call(tokeira_edge::InProcessGrpcRequest {
                service: "tokeira.internal.runtime.v1.ActivityTaskDispatch".into(), rpc: "OfferActivityTask".into(),
                headers: Default::default(), proto: bytes.into(),
            }).await.unwrap_err();
            prop_assert_eq!(error.code(), tonic::Code::Unimplemented);
            if let Some(recovery) = stack.recovery_task.take() { recovery.await.unwrap().unwrap(); }
            stack.background_cancel.cancel(); stack.runtime_tasks.begin_shutdown(); stack.engine_tasks.begin_shutdown();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
            stack.runtime_tasks.wait(deadline).await.unwrap(); stack.engine_tasks.wait(deadline).await.unwrap();
            Ok(())
        })?;
    }
}
