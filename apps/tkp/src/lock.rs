//! The remote operation lock around mutating verbs (tasks 12.1/12.2, Req 11).
//!
//! Every mutating `tkp` command acquires the deployment's operation lock before
//! any provider-side work and releases it on completion, so two provisioners
//! cannot make conflicting changes. `rollback` holds **one continuous** lock
//! across its whole B-delete → re-pin → A-reconcile sequence (12.2), so no writer
//! interleaves at the handoff.

use std::path::Path;
use std::process;
use std::time::Duration;

use anyhow::{Context, Result};
use tokeira_state::{LocalBackend, OperationLock};

/// Lease duration; renew for operations that run longer than this.
const LOCK_TTL: Duration = Duration::from_secs(120);

fn operation_lock(deployment_dir: &Path) -> OperationLock {
    // A dedicated lock object, distinct from the envelope and the state docs. For
    // now a local file; the cloud path uses the S3 backend via the same primitive.
    OperationLock::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/lock"))),
        "operation",
    )
}

/// Run `body` while holding the deployment's operation lock — acquired before any
/// work, released afterwards (best-effort; the lease also expires). Refuses if
/// another provisioner already holds the lock.
pub async fn with_operation_lock<F, Fut>(
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
    let guard = lock.acquire(&holder, LOCK_TTL).await.with_context(|| {
        "failed to acquire the remote operation lock — another provisioner may be operating this \
         deployment"
    })?;

    let result = body().await;

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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

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
        assert!(err.to_string().contains("operation lock"), "unexpected: {err}");
    }
}
