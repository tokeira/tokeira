//! A close and its successor share one DSQL transaction and one lease read.
//! SQLSTATE 54000 alone permits the runtime's size fallback. Classification
//! surrounds the whole transaction, including statements and COMMIT.

use super::*;
use crate::{PairCommitResult, api::validate_transition_pair};

#[async_trait]
trait PairTransaction: Send + Sized {
    async fn fence(&mut self) -> Result<()>;
    async fn check_closing(&mut self) -> Result<Option<PairCommitResult>>;
    async fn check_successor(&mut self) -> Result<Option<PairCommitResult>>;
    async fn check_pointer(&mut self) -> Result<Option<PairCommitResult>>;
    async fn write_closing(&mut self) -> Result<()>;
    async fn write_successor(&mut self) -> Result<()>;
    async fn commit(self) -> Result<PairCommitResult>;
}

async fn execute_pair<T: PairTransaction>(mut transaction: T) -> Result<PairCommitResult> {
    let result = async {
        // No run-state read may precede this lease read. The same transaction
        // retains the fence until both write sets commit (bundle-lease-fence).
        transaction.fence().await?;
        if let Some(result) = transaction.check_closing().await? {
            return Ok(result);
        }
        if let Some(result) = transaction.check_successor().await? {
            return Ok(result);
        }
        if let Some(result) = transaction.check_pointer().await? {
            return Ok(result);
        }
        transaction.write_closing().await?;
        transaction.write_successor().await?;
        transaction.commit().await
    }
    .await;
    classify(result)
}

fn classify(result: Result<PairCommitResult>) -> Result<PairCommitResult> {
    match result {
        Err(error) => {
            let code = error
                .chain()
                .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
                .and_then(sqlx::Error::as_database_error)
                .and_then(|error| error.code());
            match code.as_deref() {
                Some("54000") => Ok(PairCommitResult::SizeRefused),
                Some("40001") => Ok(PairCommitResult::Conflict {
                    reason: "DSQL pair serialization conflict".into(),
                }),
                _ => Err(error),
            }
        }
        result => result,
    }
}

struct SqlPair<'a> {
    tx: sqlx::Transaction<'a, sqlx::Postgres>,
    closing: Transition,
    successor: Transition,
    closing_state: WorkflowState,
    successor_state: WorkflowState,
    home: ShardId,
    fence: WriteFence,
    partitions: u32,
    prior_history_size: i64,
    prior_state_parts: Option<i32>,
}

impl DsqlRunRepository {
    #[tracing::instrument(name = "dsql.commit_transition_pair", skip_all, fields(run_key = %run_key.0, successor_run_key = %successor_run_key.0))]
    pub(super) async fn do_commit_transition_pair(
        &self,
        run_key: RunKey,
        closing: Transition,
        successor_run_key: RunKey,
        successor: Transition,
        fence: WriteFence,
    ) -> Result<PairCommitResult> {
        validate_transition_pair(run_key, &closing, successor_run_key, &successor)?;
        let home = tokeira_types::execution_home_bundle(
            closing.next_state.namespace_id.0.as_bytes(),
            closing.next_state.workflow_id.0.as_bytes(),
            self.shard_count,
        );
        fence.ensure_home(home)?;
        convert::i64_from_u64(closing.next_state.transition_seq.0, "transition_seq")?;
        convert::i64_from_u64(successor.next_state.transition_seq.0, "transition_seq")?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let tx = permit.connection()?.begin().await?;
        execute_pair(SqlPair {
            tx,
            closing_state: closing.next_state.clone(),
            successor_state: successor.next_state.clone(),
            closing,
            successor,
            home,
            fence,
            partitions: self.projection_partition_count,
            prior_history_size: 0,
            prior_state_parts: None,
        })
        .await
    }
}

#[async_trait]
impl PairTransaction for SqlPair<'_> {
    async fn fence(&mut self) -> Result<()> {
        if let Some(lost) =
            DsqlRunRepository::check_fence(&mut self.tx, &self.fence, self.closing_state.run_key)
                .await?
        {
            return Err(lost.into());
        }
        Ok(())
    }

    async fn check_closing(&mut self) -> Result<Option<PairCommitResult>> {
        let row = sqlx::query_as::<_, (i64, Option<i64>, Option<i32>)>(
            "SELECT transition_seq, history_size_bytes, state_parts FROM workflow_hot WHERE run_key = $1 FOR UPDATE",
        ).bind(self.closing_state.run_key.0).fetch_optional(&mut *self.tx).await?;
        let Some((seq, size, parts)) = row else {
            return Ok(Some(PairCommitResult::Conflict {
                reason: "closing run is absent".into(),
            }));
        };
        if convert::u64_from_i64(seq, "workflow_hot.transition_seq")? != self.closing.expected_seq.0
        {
            return Ok(Some(PairCommitResult::Conflict {
                reason: "closing run sequence changed".into(),
            }));
        }
        self.prior_history_size = size.unwrap_or(0);
        self.prior_state_parts = parts;
        check_dedupe(&mut self.tx, &self.closing).await
    }

    async fn check_successor(&mut self) -> Result<Option<PairCommitResult>> {
        let row = sqlx::query_as::<_, (Uuid,)>(
            "SELECT run_key FROM workflow_hot WHERE run_key = $1 FOR UPDATE",
        )
        .bind(self.successor_state.run_key.0)
        .fetch_optional(&mut *self.tx)
        .await?;
        if row.is_some() {
            return Ok(Some(PairCommitResult::Conflict {
                reason: "successor run already exists".into(),
            }));
        }
        check_dedupe(&mut self.tx, &self.successor).await
    }

    async fn check_pointer(&mut self) -> Result<Option<PairCommitResult>> {
        let key = DsqlRunRepository::current_execution_key(
            self.closing_state.namespace_id,
            &self.closing_state.workflow_id,
        );
        let row = sqlx::query_as::<_, (Uuid,)>(
            "SELECT run_key FROM current_execution WHERE key = $1 FOR UPDATE",
        )
        .bind(key)
        .fetch_optional(&mut *self.tx)
        .await?;
        // The pointer must name the predecessor, including when it is closed.
        // An absent pointer is also a conflict: a successor cannot claim an
        // unrelated lineage (execution_util.go:966-1009 @ v1.31.0).
        if row.map(|(key,)| key) != Some(self.closing_state.run_key.0) {
            return Ok(Some(PairCommitResult::Conflict {
                reason: "current execution no longer names closing run".into(),
            }));
        }
        Ok(None)
    }

    async fn write_closing(&mut self) -> Result<()> {
        super::commit::write_transition(
            &mut self.tx,
            self.closing_state.run_key,
            self.home,
            self.partitions,
            &self.closing,
            &mut self.closing_state,
            self.prior_history_size,
            self.prior_state_parts,
        )
        .await
    }

    async fn write_successor(&mut self) -> Result<()> {
        super::commit::write_transition(
            &mut self.tx,
            self.successor_state.run_key,
            self.home,
            self.partitions,
            &self.successor,
            &mut self.successor_state,
            0,
            None,
        )
        .await
    }

    async fn commit(self) -> Result<PairCommitResult> {
        self.tx.commit().await?;
        Ok(PairCommitResult::Applied {
            closing_state: self.closing_state,
            successor_state: self.successor_state,
        })
    }
}

async fn check_dedupe(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transition: &Transition,
) -> Result<Option<PairCommitResult>> {
    let state = &transition.next_state;
    for op in &transition.request_dedupe_ops {
        let key = DsqlRunRepository::request_dedupe_key(
            state.namespace_id,
            &state.workflow_id,
            &op.request_id,
        );
        let row = sqlx::query_as::<_, (Uuid,)>("SELECT run_id FROM request_dedupe WHERE key = $1")
            .bind(key)
            .fetch_optional(&mut **tx)
            .await?;
        if row.is_some_and(|(run_id,)| run_id == state.run_id.0) {
            return Ok(Some(PairCommitResult::Duplicate));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::{super::tests::TestDatabaseError, *};
    use std::sync::{Arc, Mutex};

    struct RecordingPair {
        steps: Arc<Mutex<Vec<&'static str>>>,
        failure: Option<(&'static str, &'static str)>,
    }
    impl RecordingPair {
        fn step(&self, name: &'static str) -> Result<()> {
            self.steps.lock().unwrap().push(name);
            if let Some((step, code)) = self.failure
                && step == name
            {
                return Err(sqlx::Error::Database(Box::new(TestDatabaseError {
                    code: Some(code),
                }))
                .into());
            }
            Ok(())
        }
    }
    #[async_trait]
    impl PairTransaction for RecordingPair {
        async fn fence(&mut self) -> Result<()> {
            self.step("fence")
        }
        async fn check_closing(&mut self) -> Result<Option<PairCommitResult>> {
            self.step("closing checks")?;
            Ok(None)
        }
        async fn check_successor(&mut self) -> Result<Option<PairCommitResult>> {
            self.step("successor checks")?;
            Ok(None)
        }
        async fn check_pointer(&mut self) -> Result<Option<PairCommitResult>> {
            self.step("pointer")?;
            Ok(None)
        }
        async fn write_closing(&mut self) -> Result<()> {
            self.step("closing writes")
        }
        async fn write_successor(&mut self) -> Result<()> {
            self.step("successor writes")
        }
        async fn commit(self) -> Result<PairCommitResult> {
            self.step("commit")?;
            Ok(PairCommitResult::Duplicate)
        }
    }
    #[tokio::test]
    async fn pair_reads_fence_first() {
        let steps = Arc::new(Mutex::new(Vec::new()));
        execute_pair(RecordingPair {
            steps: steps.clone(),
            failure: None,
        })
        .await
        .unwrap();
        assert_eq!(
            *steps.lock().unwrap(),
            [
                "fence",
                "closing checks",
                "successor checks",
                "pointer",
                "closing writes",
                "successor writes",
                "commit"
            ]
        );
    }
    #[tokio::test]
    async fn pair_classifies_size_and_serialization_at_statements_and_commit() {
        for step in [
            "fence",
            "closing checks",
            "successor checks",
            "pointer",
            "closing writes",
            "successor writes",
            "commit",
        ] {
            for code in ["54000", "40001", "08006"] {
                let result = execute_pair(RecordingPair {
                    steps: Arc::new(Mutex::new(Vec::new())),
                    failure: Some((step, code)),
                })
                .await;
                match code {
                    "54000" => assert_eq!(result.unwrap(), PairCommitResult::SizeRefused, "{step}"),
                    "40001" => assert!(
                        matches!(result.unwrap(), PairCommitResult::Conflict { .. }),
                        "{step}"
                    ),
                    _ => assert!(
                        result.is_err(),
                        "a connection error must not permit fallback at {step}"
                    ),
                }
            }
        }
    }
}
