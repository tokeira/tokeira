//! The remote operation lock around mutating verbs (tasks 12.1/12.2, Req 11).
//!
//! Every mutating `tkp` command acquires the deployment's operation lock before
//! any provider-side work and releases it on completion, so two provisioners
//! cannot make conflicting changes. `rollback` holds **one continuous** lock
//! across its whole B-delete → re-pin → A-reconcile sequence (12.2), so no writer
//! interleaves at the handoff.

use std::{path::Path, process, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_state::{LocalBackend, OperationLock};

/// Lease duration.
const LOCK_TTL: Duration = Duration::from_secs(120);
/// How often to renew while the operation runs — comfortably inside `LOCK_TTL` so
/// a single missed/slow renew still leaves the lease valid.
const RENEW_INTERVAL: Duration = Duration::from_secs(40);

fn operation_lock(deployment_dir: &Path) -> OperationLock {
    // A dedicated lock object, distinct from the envelope and the state docs. For
    // now a local file; the cloud path uses the S3 backend via the same primitive.
    OperationLock::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/lock"))),
        "operation",
    )
}

/// Run `body` while holding the deployment's operation lock — acquired before any
/// work, **renewed on an interval** so an operation longer than the lease cannot
/// have its lock stolen mid-mutation, and released afterwards. Refuses if another
/// provisioner already holds the lock.
pub(crate) async fn with_operation_lock<F, Fut>(
    deployment_dir: &Path,
    verb: &str,
    body: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let lock = operation_lock(deployment_dir);
    let holder = format!("tkp-{verb}-pid{}", process::id());
    run_locked(&lock, &holder, LOCK_TTL, RENEW_INTERVAL, body).await
}

/// Core of [`with_operation_lock`], parameterized on the lease/renew timing so
/// the renewal path is testable without a two-minute wait.
async fn run_locked<F, Fut>(
    lock: &OperationLock,
    holder: &str,
    ttl: Duration,
    renew_interval: Duration,
    body: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut guard = lock.acquire(holder, ttl).await.with_context(|| {
        "failed to acquire the remote operation lock — another provisioner may be operating this \
         deployment"
    })?;

    // Drive the body while renewing the lease on an interval. A renew failure is
    // tolerated as long as the current lease is still valid (transient backend
    // blip); once the lease has actually lapsed we can no longer guarantee
    // exclusivity, so we abort rather than risk a second concurrent writer. (A
    // takeover can only occur *after* our lease lapses — `acquire` refuses an
    // active lease — so the lapse check also catches genuine takeovers.)
    let body_fut = body();
    tokio::pin!(body_fut);
    let result = loop {
        tokio::select! {
            outcome = &mut body_fut => break outcome,
            _ = tokio::time::sleep(renew_interval) => {
                if let Err(err) = lock.renew(&mut guard, ttl).await {
                    if Utc::now() >= guard.expires_at() {
                        break Err(anyhow::anyhow!(
                            "operation lock could not be renewed and its lease has lapsed ({err}); \
                             aborting to avoid a concurrent writer"
                        ));
                    }
                    eprintln!("warning: operation lock renew failed ({err}); retrying");
                }
            }
        }
    };

    // Release regardless of outcome; a failed release is non-fatal (the lease
    // expires) but should not mask the operation's own error.
    if let Err(release_err) = lock.release(guard).await {
        eprintln!("warning: failed to release the operation lock: {release_err}");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn runs_body_under_the_lock_and_releases() {
        let tmp = tempfile::tempdir().unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();

        with_operation_lock(tmp.path(), "test", || async move {
            flag.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
        assert!(ran.load(Ordering::SeqCst), "the body ran");

        // The lock was released, so a second operation acquires it.
        with_operation_lock(tmp.path(), "test2", || async { Ok(()) })
            .await
            .expect("lock is free after release");
    }

    #[tokio::test]
    async fn refuses_when_the_lock_is_already_held() {
        let tmp = tempfile::tempdir().unwrap();
        // Another holder keeps the lock (guard not dropped/released).
        let held = operation_lock(tmp.path());
        let _guard = held.acquire("other", LOCK_TTL).await.unwrap();

        let err = with_operation_lock(tmp.path(), "test", || async { Ok(()) })
            .await
            .expect_err("a held lock refuses");
        assert!(
            err.to_string().contains("operation lock"),
            "unexpected: {err}"
        );
    }

    // Fix for the "lease lapses mid-operation" finding: an operation that outlives
    // a single lease term keeps the lock because it is renewed on an interval.
    #[tokio::test]
    async fn renews_the_lease_so_a_long_operation_keeps_the_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = operation_lock(tmp.path());
        let ttl = Duration::from_millis(1000);
        let renew = Duration::from_millis(200);

        // A body that runs well past a single (1s) lease term.
        let run = run_locked(&lock, "holder-A", ttl, renew, || async {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok(())
        });

        // Concurrently, after 1.4s (past the initial lease) a second provisioner
        // must still be refused — holder-A kept renewing.
        let probe = async {
            tokio::time::sleep(Duration::from_millis(1400)).await;
            operation_lock(tmp.path()).acquire("holder-B", ttl).await
        };

        let (run_res, probe_res) = tokio::join!(run, probe);
        run_res.expect("the long body completes under a continuously-held lock");
        assert!(
            probe_res.is_err(),
            "a second acquirer is refused across renewals (lease was not stolen)"
        );

        // After the operation releases, the lock is free again.
        with_operation_lock(tmp.path(), "after", || async { Ok(()) })
            .await
            .expect("lock is free once the renewed operation releases");
    }
}
