use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokeira_kernel::{Command, Kernel};
use tokeira_storage::{CommitResult, RunRepository};
use tokeira_types::RunKey;

/// A lane is a single serial command processor.
///
/// Insight: lanes are *execution locality* devices. They reduce lock pressure
/// and make it obvious which piece of code serializes commands for a run, but
/// they do not define correctness. If a run moves between lanes later, the run's
/// durable state remains the source of truth.
pub struct LaneHandle {
    tx: mpsc::Sender<LaneMessage>,
}

impl LaneHandle {
    pub async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(LaneMessage { run_key, command, reply_tx }).await?;
        reply_rx.await?
    }
}

struct LaneMessage {
    run_key: RunKey,
    command: Command,
    reply_tx: oneshot::Sender<Result<CommitResult>>,
}

pub fn spawn_lane<K, R>(kernel: K, repo: R) -> LaneHandle
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
{
    let (tx, mut rx) = mpsc::channel::<LaneMessage>(1024);
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let result = handle_message(&kernel, &repo, message.run_key, message.command).await;
            let _ = message.reply_tx.send(result);
        }
    });
    LaneHandle { tx }
}

async fn handle_message<K, R>(kernel: &K, repo: &R, run_key: RunKey, command: Command) -> Result<CommitResult>
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
{
    let loaded = repo.load_run(run_key).await?;
    let transition = kernel.apply(loaded, command)?;
    repo.commit_transition(run_key, transition).await
}
