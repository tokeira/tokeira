//! Renewable remote operation lock.
//!
//! A deployment-level mutual-exclusion lease held across a whole mutating
//! command (e.g. an entire upgrade or rollback sequence) — distinct from the
//! short, per-save lease inside [`S3StateStore`](crate::S3StateStore).
//!
//! It is built over any [`StateBackend`], so one primitive serves both the cloud
//! (an S3 lock object) and local dev (a filesystem lock file): the lock record is
//! stored under a dedicated key using the backend's compare-and-swap
//! `read_manifest`/`write_manifest`, and mutual exclusion comes from that CAS plus
//! a time lease that a superseded holder cannot renew. Acquiring **refuses**
//! ([`StateError::Locked`]) while another holder's lease is active; callers that
//! prefer to wait can retry.

use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StateError, backend::StateBackend};

/// Clock-skew tolerance when judging lease expiry.
const LOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(5);

/// The persisted lock record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationLease {
    pub(crate) holder: String,
    pub(crate) token: String,
    pub(crate) acquired_at: DateTime<Utc>,
    pub(crate) renewed_at: DateTime<Utc>,
    pub(crate) expires_at: DateTime<Utc>,
    /// Set on clean release so the next acquirer takes over immediately.
    pub(crate) released: bool,
}

impl OperationLease {
    /// A lease still excludes others while it is neither released nor expired.
    fn is_active(&self, now: DateTime<Utc>) -> bool {
        !self.released && !lease_expired(self.expires_at, now)
    }
}

fn lease_expired(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let tolerance =
        ChronoDuration::from_std(LOCK_SKEW_TOLERANCE).expect("skew tolerance is a valid duration");
    expires_at + tolerance <= now
}

/// A held operation lock. Carries the backend version tag used for CAS on
/// renew/release.
#[derive(Debug, Clone)]
pub struct OperationLockGuard {
    pub holder: String,
    pub token: String,
    pub(crate) expires_at: DateTime<Utc>,
    version: String,
}

impl OperationLockGuard {
    /// When the current lease expires if not renewed.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

/// A renewable remote operation lock over a [`StateBackend`].
pub struct OperationLock {
    backend: Box<dyn StateBackend>,
    key: String,
}

// Manual impl: `backend` is a trait object without a `Debug` bound.
impl std::fmt::Debug for OperationLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationLock")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl OperationLock {
    /// Construct a lock stored under `key` in `backend`.
    pub fn new(backend: Box<dyn StateBackend>, key: impl Into<String>) -> Self {
        Self {
            backend,
            key: key.into(),
        }
    }

    async fn read_lease(&self) -> Result<(Option<OperationLease>, String), StateError> {
        match self.backend.read_manifest(&self.key).await? {
            Some((bytes, version)) => {
                let lease = serde_json::from_slice(&bytes).map_err(|e| {
                    StateError::Corrupted(format!("invalid operation lock record: {e}"))
                })?;
                Ok((Some(lease), version))
            }
            None => Ok((None, String::new())),
        }
    }

    async fn write_lease(
        &self,
        lease: &OperationLease,
        expected_version: &str,
    ) -> Result<String, StateError> {
        let bytes = serde_json::to_vec(lease).map_err(|e| {
            StateError::Corrupted(format!("failed to serialize operation lock: {e}"))
        })?;
        self.backend
            .write_manifest(&self.key, &bytes, expected_version)
            .await?;
        // Re-read to get the new version tag for the guard's later CAS.
        let (_, version) = self.read_lease().await?;
        Ok(version)
    }

    /// Acquire the lock for `holder`, valid for `ttl`.
    ///
    /// Returns [`StateError::Locked`] while another holder's lease is active; an
    /// absent, expired, or released lease is taken over. Concurrent acquirers
    /// race on the backend CAS and at most one wins.
    pub async fn acquire(
        &self,
        holder: &str,
        ttl: Duration,
    ) -> Result<OperationLockGuard, StateError> {
        let ttl = ChronoDuration::from_std(ttl)
            .map_err(|e| StateError::Other(anyhow::anyhow!("invalid lock ttl: {e}")))?;
        loop {
            let now = Utc::now();
            let (current, version) = self.read_lease().await?;
            if let Some(lease) = &current
                && lease.is_active(now)
            {
                return Err(StateError::Locked(format!(
                    "operation lock held by {} until {}",
                    lease.holder, lease.expires_at
                )));
            }

            let token = Uuid::new_v4().to_string();
            let lease = OperationLease {
                holder: holder.to_string(),
                token: token.clone(),
                acquired_at: now,
                renewed_at: now,
                expires_at: now + ttl,
                released: false,
            };
            match self.write_lease(&lease, &version).await {
                Ok(new_version) => {
                    return Ok(OperationLockGuard {
                        holder: holder.to_string(),
                        token,
                        expires_at: lease.expires_at,
                        version: new_version,
                    });
                }
                // The record changed between our read and write — another writer
                // raced us. Re-read and re-evaluate.
                Err(StateError::Conflict(_)) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Adopt an **existing** lease by holder identity + token — the
    /// orchestrated two-binary flow: a parent process acquired
    /// the lease and hands it to a child, which renews around its work but
    /// never acquires or releases (the parent owns the lease lifecycle, so
    /// the lock is held continuously across the process boundary).
    ///
    /// Verifies the lease is live and matches, then renews for `ttl` and
    /// returns the guard. [`StateError::LockLost`] when the lease is gone,
    /// released, or held under a different token.
    pub async fn adopt(
        &self,
        holder: &str,
        token: &str,
        ttl: Duration,
    ) -> Result<OperationLockGuard, StateError> {
        let (current, version) = self.read_lease().await?;
        let current =
            current.ok_or_else(|| StateError::LockLost("no operation lease to adopt".into()))?;
        if current.token != token || current.holder != holder || current.released {
            return Err(StateError::LockLost(
                "the lease to adopt is gone, released, or held by another".into(),
            ));
        }
        let mut guard = OperationLockGuard {
            holder: current.holder.clone(),
            token: current.token.clone(),
            expires_at: current.expires_at,
            version,
        };
        self.renew(&mut guard, ttl).await?;
        Ok(guard)
    }

    /// Renew a held lock for another `ttl`.
    ///
    /// Returns [`StateError::LockLost`] if the lock was taken over (token
    /// mismatch), released, or is gone.
    pub async fn renew(
        &self,
        guard: &mut OperationLockGuard,
        ttl: Duration,
    ) -> Result<(), StateError> {
        let ttl = ChronoDuration::from_std(ttl)
            .map_err(|e| StateError::Other(anyhow::anyhow!("invalid lock ttl: {e}")))?;
        let (current, version) = self.read_lease().await?;
        let current = current
            .ok_or_else(|| StateError::LockLost("operation lock no longer exists".into()))?;
        if current.token != guard.token || current.released {
            return Err(StateError::LockLost(
                "operation lock was taken over by another holder".into(),
            ));
        }

        let now = Utc::now();
        let next = OperationLease {
            renewed_at: now,
            expires_at: now + ttl,
            ..current
        };
        let new_version = self.write_lease(&next, &version).await?;
        guard.version = new_version;
        guard.expires_at = next.expires_at;
        Ok(())
    }

    /// Release the lock so the next acquirer takes it immediately. Idempotent: a
    /// lock already gone or taken over by another holder is a no-op.
    pub async fn release(&self, guard: OperationLockGuard) -> Result<(), StateError> {
        let (current, version) = self.read_lease().await?;
        let Some(current) = current else {
            return Ok(());
        };
        if current.token != guard.token {
            return Ok(());
        }
        let released = OperationLease {
            released: true,
            ..current
        };
        self.write_lease(&released, &version).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalBackend;

    const KEY: &str = "operation";

    fn lock(root: &std::path::Path) -> OperationLock {
        OperationLock::new(Box::new(LocalBackend::new(root)), KEY)
    }

    fn ttl() -> Duration {
        Duration::from_secs(60)
    }

    async fn write_raw_lease(root: &std::path::Path, lease: &OperationLease) {
        // A second backend over the same root writes the lock record directly,
        // simulating another process. Create-only first, else update.
        let backend = LocalBackend::new(root);
        let bytes = serde_json::to_vec(lease).unwrap();
        if let Some((_, version)) = backend.read_manifest(KEY).await.unwrap() {
            backend.write_manifest(KEY, &bytes, &version).await.unwrap();
        } else {
            backend.write_manifest(KEY, &bytes, "").await.unwrap();
        }
    }

    #[tokio::test]
    async fn acquire_renew_release_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = lock(tmp.path());

        let mut guard = lock.acquire("worker-a", ttl()).await.unwrap();
        assert_eq!(guard.holder, "worker-a");

        let before = guard.expires_at;
        lock.renew(&mut guard, ttl()).await.unwrap();
        assert!(guard.expires_at >= before, "renew extends the lease");

        lock.release(guard).await.unwrap();
    }

    #[tokio::test]
    async fn second_acquire_refused_while_held() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = lock(tmp.path());

        let _guard = lock.acquire("worker-a", ttl()).await.unwrap();
        let err = lock.acquire("worker-b", ttl()).await.unwrap_err();
        assert!(
            matches!(err, StateError::Locked(_)),
            "second acquirer refuses while the lease is active, got {err:?}"
        );
    }

    #[tokio::test]
    async fn release_lets_next_acquire_succeed() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = lock(tmp.path());

        let guard = lock.acquire("worker-a", ttl()).await.unwrap();
        lock.release(guard).await.unwrap();

        let guard_b = lock.acquire("worker-b", ttl()).await.unwrap();
        assert_eq!(guard_b.holder, "worker-b");
    }

    #[tokio::test]
    async fn expired_lease_is_taken_over() {
        let tmp = tempfile::tempdir().unwrap();
        let past = Utc::now() - ChronoDuration::hours(1);
        write_raw_lease(
            tmp.path(),
            &OperationLease {
                holder: "stale".into(),
                token: "old-token".into(),
                acquired_at: past,
                renewed_at: past,
                expires_at: past,
                released: false,
            },
        )
        .await;

        let lock = lock(tmp.path());
        let guard = lock.acquire("worker-a", ttl()).await.unwrap();
        assert_eq!(guard.holder, "worker-a");
        assert_ne!(
            guard.token, "old-token",
            "a fresh token is minted on takeover"
        );
    }

    #[tokio::test]
    async fn renew_after_takeover_fails_lock_lost() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = lock(tmp.path());

        let mut guard = lock.acquire("worker-a", ttl()).await.unwrap();

        // Another process takes over (writes a fresh active lease directly).
        let now = Utc::now();
        write_raw_lease(
            tmp.path(),
            &OperationLease {
                holder: "worker-b".into(),
                token: "b-token".into(),
                acquired_at: now,
                renewed_at: now,
                expires_at: now + ChronoDuration::minutes(1),
                released: false,
            },
        )
        .await;

        let err = lock.renew(&mut guard, ttl()).await.unwrap_err();
        assert!(
            matches!(err, StateError::LockLost(_)),
            "renewing a taken-over lock fails LockLost, got {err:?}"
        );
    }
}
