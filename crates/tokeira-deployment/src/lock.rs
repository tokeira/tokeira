//! The remote operation lock around mutating verbs.
//!
//! Every mutating `tkp` command acquires the deployment's operation lock before
//! any provider-side work and releases it on completion, so two provisioners
//! cannot make conflicting changes. `rollback` holds **one continuous** lock
//! across its whole B-delete → re-pin → A-reconcile sequence, so no writer
//! interleaves at the handoff.

// Transitional (deployment-repository spec, restructuring slice): this module
// migrated from the `tkp` shell, whose stderr IS the operator interface for
// these lock warnings. Behaviour preservation keeps them byte-identical; the
// operation-lease spec reworks this module and retires the allow.
#![allow(clippy::print_stderr)]

use std::{path::Path, process, time::Duration};

use crate::{ORCHESTRATED_LOCK_HOLDER_ENV, ORCHESTRATED_LOCK_TOKEN_ENV};
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

/// Run `body` while holding the deployment's operation lock. Two modes:
///
/// - **Standalone** (the default): acquire before any work, renew on an
///   interval, release afterwards. Refuses if another provisioner holds it.
/// - **Adopted**: when the orchestrator's env names a lease
///   ([`ORCHESTRATED_LOCK_HOLDER_ENV`]/[`ORCHESTRATED_LOCK_TOKEN_ENV`]),
///   join it — renew around the body, but never acquire and never release:
///   the orchestrator owns the lease lifecycle, so the lock is held
///   continuously across the two-binary relaunch boundary (extends 12.2
///   from single-process to two-binary).
pub async fn with_operation_lock<F, Fut>(deployment_dir: &Path, verb: &str, body: F) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let lock = operation_lock(deployment_dir);
    let orchestrated = std::env::var(ORCHESTRATED_LOCK_HOLDER_ENV)
        .ok()
        .zip(std::env::var(ORCHESTRATED_LOCK_TOKEN_ENV).ok());
    match orchestrated {
        Some((holder, token)) => {
            run_adopted(&lock, &holder, &token, LOCK_TTL, RENEW_INTERVAL, body).await
        }
        None => {
            let holder = format!("tkp-{verb}-pid{}", process::id());
            run_locked(&lock, &holder, LOCK_TTL, RENEW_INTERVAL, body).await
        }
    }
}

/// Adopted-mode core: join the orchestrator's lease, drive the body under
/// renewal, and hand the (still-live) lease back by simply not releasing it.
async fn run_adopted<F, Fut>(
    lock: &OperationLock,
    holder: &str,
    token: &str,
    ttl: Duration,
    renew_interval: Duration,
    body: F,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let guard = lock.adopt(holder, token, ttl).await.with_context(|| {
        "failed to adopt the orchestrator's operation lease — it may have lapsed or been \
         taken over"
    })?;
    let (result, _guard) = drive_with_renewal(lock, guard, ttl, renew_interval, body).await;
    // Deliberately no release: the orchestrator owns the lease.
    result
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
    let guard = lock.acquire(holder, ttl).await.with_context(|| {
        "failed to acquire the remote operation lock — another provisioner may be operating this \
         deployment"
    })?;
    let (result, guard) = drive_with_renewal(lock, guard, ttl, renew_interval, body).await;

    // Release regardless of outcome; a failed release is non-fatal (the lease
    // expires) but should not mask the operation's own error.
    if let Err(release_err) = lock.release(guard).await {
        eprintln!("warning: failed to release the operation lock: {release_err}");
    }
    result
}

/// Drive `body` while renewing `guard` on an interval — the shared core of
/// both lock modes. A renew failure is tolerated as long as the current lease
/// is still valid (transient backend blip); once the lease has actually
/// lapsed we can no longer guarantee exclusivity, so we abort rather than
/// risk a second concurrent writer. (A takeover can only occur *after* the
/// lease lapses — `acquire` refuses an active lease — so the lapse check
/// also catches genuine takeovers.) Returns the guard so the caller decides
/// its fate: release (standalone) or keep alive (adopted).
async fn drive_with_renewal<F, Fut>(
    lock: &OperationLock,
    mut guard: tokeira_state::OperationLockGuard,
    ttl: Duration,
    renew_interval: Duration,
    body: F,
) -> (Result<()>, tokeira_state::OperationLockGuard)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
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
    (result, guard)
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

    // Task 19.3: the adopted mode — a child joins the orchestrator's lease,
    // works under it, and leaves it live: the lock is continuous across the
    // two-binary boundary, and only the orchestrator releases.
    #[tokio::test]
    async fn adopted_mode_works_under_the_orchestrators_lease_and_never_releases() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = operation_lock(tmp.path());
        // The orchestrator (tkr) acquires…
        let orchestrator = lock.acquire("tkr-rollback", LOCK_TTL).await.unwrap();
        let token = orchestrator.token.clone();

        // …the child adopts and runs its body…
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        run_adopted(
            &lock,
            "tkr-rollback",
            &token,
            LOCK_TTL,
            RENEW_INTERVAL,
            || async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect("adopted body runs");
        assert!(ran.load(Ordering::SeqCst));

        // …and the lease SURVIVES the child: a third party still cannot
        // acquire until the orchestrator releases.
        assert!(
            lock.acquire("intruder", LOCK_TTL).await.is_err(),
            "the lease is still the orchestrator's"
        );
        lock.release(orchestrator).await.unwrap();
        lock.acquire("next", LOCK_TTL)
            .await
            .expect("free after the orchestrator releases");
    }

    #[tokio::test]
    async fn adoption_with_a_wrong_token_refuses_before_any_work() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = operation_lock(tmp.path());
        let _held = lock.acquire("tkr-rollback", LOCK_TTL).await.unwrap();

        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let err = run_adopted(
            &lock,
            "tkr-rollback",
            "not-the-token",
            LOCK_TTL,
            RENEW_INTERVAL,
            || async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect_err("a mismatched lease refuses adoption");
        assert!(err.to_string().contains("adopt"), "unexpected: {err}");
        assert!(!ran.load(Ordering::SeqCst), "the body never ran");
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
