//! Volatile, restartable disposal passes over legacy workflow backlog copies.
//!
//! Serving does not wait for disposal. Acquisition progress wakes deferred
//! entries; a page error loses the cursor and the next pass starts at the head.

use std::sync::Arc;

use tokeira_storage::RunRepository;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub(crate) async fn run_disposal<R: RunRepository + 'static>(
    repo: Arc<R>,
    wake: Arc<Notify>,
    cancel: CancellationToken,
) {
    loop {
        let mut after = None;
        let mut deferred = 0;
        loop {
            let page = tokio::select! {
                _ = cancel.cancelled() => return,
                result = repo.dispose_workflow_backlog_page(after) => result,
            };
            match page {
                Ok(page) => {
                    deferred += page.examined - page.deleted;
                    if page.exhausted {
                        if deferred == 0 {
                            return;
                        }
                        break;
                    }
                    after = page.last_examined;
                }
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "legacy workflow disposal page failed; restarting from head"
                    );
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = wake.notified() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
        }
    }
}
