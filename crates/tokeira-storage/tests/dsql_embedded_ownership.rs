#![cfg(feature = "dsql-integration")]

//! SQL-backed ownership integration for the exclusive embedded engine claim.
//!
//! The test uses the same opt-in database URL mechanism as the other DSQL SQL
//! integrations. It advances database state explicitly instead of sleeping for a lease.

use std::time::{Duration as StdDuration, Instant};

use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use time::Duration;
use tokeira_storage::dsql::{
    ControlLeaseAcquireOutcome, ControlLeaseAcquireRequest, ControlLeaseClusterIdentity,
    ControlLeaseError, ControlLeaseRepository, OwnershipAdmissionError, OwnershipAdmissionGate,
    OwnershipAdmissionState,
};

#[tokio::test]
async fn embedded_owner_clean_and_expired_takeovers_are_fenced() -> Result<()> {
    let Some(pool) = test_pool().await? else {
        return Ok(());
    };
    let repository = ControlLeaseRepository::new(pool.clone());
    repository.bootstrap().await?;
    let claim_name = format!("embedded-owner-integration-{}", uuid::Uuid::new_v4());
    clear_claim(&pool, &claim_name).await?;
    let identity = ControlLeaseClusterIdentity {
        cluster_id: "integration-cluster".to_owned(),
        cluster_arn: "arn:aws:dsql:eu-west-2:123456789012:cluster/integration-cluster".to_owned(),
    };

    let first = repository
        .acquire(&request(&claim_name, &identity, "owner-a"))
        .await?;
    assert_eq!(first.outcome(), ControlLeaseAcquireOutcome::Clean);
    let first_gate = OwnershipAdmissionGate::for_guard(&first);
    assert!(first_gate.admit().is_ok());
    assert!(matches!(
        repository
            .acquire(&request(&claim_name, &identity, "owner-b"))
            .await,
        Err(ControlLeaseError::Busy { .. })
    ));

    repository.release(&first, &first_gate).await?;
    assert_eq!(first_gate.state(), OwnershipAdmissionState::Closing);
    let clean_takeover = repository
        .acquire(&request(&claim_name, &identity, "owner-b"))
        .await?;
    assert_eq!(clean_takeover.outcome(), ControlLeaseAcquireOutcome::Clean);
    assert!(clean_takeover.fence_token() > first.fence_token());
    let clean_gate = OwnershipAdmissionGate::for_guard(&clean_takeover);
    assert_eq!(clean_gate.state(), OwnershipAdmissionState::Open);
    repository.release(&clean_takeover, &clean_gate).await?;

    let mut crashed = repository
        .acquire(&request(&claim_name, &identity, "owner-c"))
        .await?;
    let crashed_gate = OwnershipAdmissionGate::for_guard(&crashed);
    expire_claim(&pool, &claim_name).await?;
    let expired_takeover = repository
        .acquire(&request(&claim_name, &identity, "owner-d"))
        .await?;
    assert_eq!(
        expired_takeover.outcome(),
        ControlLeaseAcquireOutcome::ExpiredTakeover
    );
    assert!(expired_takeover.fence_token() > crashed.fence_token());
    let takeover_gate = OwnershipAdmissionGate::for_guard(&expired_takeover);
    assert_eq!(takeover_gate.state(), OwnershipAdmissionState::Closing);
    assert!(matches!(
        takeover_gate.finish_quiescence(&expired_takeover, Instant::now()),
        Err(OwnershipAdmissionError::Quiescing)
    ));

    assert!(matches!(
        repository
            .renew(
                &mut crashed,
                Duration::seconds(60),
                Duration::seconds(20),
                &crashed_gate,
            )
            .await,
        Err(ControlLeaseError::Fenced)
    ));
    assert_eq!(crashed_gate.state(), OwnershipAdmissionState::Fenced);
    assert!(matches!(
        crashed_gate.admit(),
        Err(OwnershipAdmissionError::Fenced)
    ));

    let quiescence_deadline = expired_takeover
        .quiescence_deadline()
        .expect("an expired takeover has a quiescence deadline");
    takeover_gate.finish_quiescence(&expired_takeover, quiescence_deadline)?;
    assert_eq!(takeover_gate.state(), OwnershipAdmissionState::Open);
    repository
        .release(&expired_takeover, &takeover_gate)
        .await?;
    clear_claim(&pool, &claim_name).await?;
    Ok(())
}

fn request(
    claim_name: &str,
    identity: &ControlLeaseClusterIdentity,
    owner_id: &str,
) -> ControlLeaseAcquireRequest {
    ControlLeaseAcquireRequest {
        claim_name: claim_name.to_owned(),
        cluster: identity.clone(),
        owner_id: owner_id.to_owned(),
        lease_duration: Duration::seconds(60),
        admission_margin: Duration::seconds(20),
        acquire_deadline: Instant::now() + StdDuration::from_secs(10),
    }
}

async fn test_pool() -> Result<Option<PgPool>> {
    let Some(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
    else {
        return Ok(None);
    };
    Ok(Some(
        PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?,
    ))
}

async fn expire_claim(pool: &PgPool, claim_name: &str) -> Result<()> {
    sqlx::query(
        "UPDATE tokeira_control_lease SET expires_at = now() - INTERVAL '1 second' \
         WHERE claim_name = $1",
    )
    .bind(claim_name)
    .execute(pool)
    .await?;
    Ok(())
}

async fn clear_claim(pool: &PgPool, claim_name: &str) -> Result<()> {
    sqlx::query("DELETE FROM tokeira_control_lease WHERE claim_name = $1")
        .bind(claim_name)
        .execute(pool)
        .await?;
    Ok(())
}
