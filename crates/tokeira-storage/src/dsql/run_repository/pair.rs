//! A close and its successor share one DSQL transaction and one lease read.
//! Only recognized DSQL size refusals permit the runtime's fallback. Classification
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
            let database = error
                .chain()
                .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
                .and_then(sqlx::Error::as_database_error);
            if let Some(database) = database {
                if is_size_refusal(database.code().as_deref(), database.message()) {
                    return Ok(PairCommitResult::SizeRefused);
                }
                if database.code().as_deref() == Some("40001") {
                    return Ok(PairCommitResult::Conflict {
                        reason: "DSQL pair serialization conflict".into(),
                    });
                }
            }
            Err(error)
        }
        result => result,
    }
}

// SQLSTATE 54000 also covers transaction age, keys and schema-object limits.
// Only AWS's explicit data/row-size messages justify closing before a staged
// successor. Unknown messages remain errors, even under the same SQLSTATE.
// https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html
fn is_size_refusal(code: Option<&str>, message: &str) -> bool {
    code == Some("54000")
        && matches!(
            message,
            "transaction size limit 10mb exceeded"
                | "transaction size limit"
                | "transaction row limit exceeded"
                | "transaction row limit"
                | "maximum row size exceeded"
                | "maximum column size exceeded"
        )
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
    pub(super) fn do_commit_transition_pair(
        &self,
        run_key: RunKey,
        closing: Transition,
        successor_run_key: RunKey,
        successor: Transition,
        fence: WriteFence,
    ) -> impl std::future::Future<Output = Result<PairCommitResult>> + Send + '_ {
        let span = tracing::info_span!(
            "dsql.commit_transition_pair",
            run_key = %run_key.0,
            successor_run_key = %successor_run_key.0,
        );
        // This function must remain synchronous: an async function captures both
        // inline transitions before executing its first allocation. Keep both
        // the traced body and the transaction executor behind allocation
        // boundaries, so constructing/polling them does not stack their owned
        // transitions in this future or in the runtime awaiting the pair.
        Box::pin(async move {
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
            Box::pin(execute_pair(SqlPair {
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
            }))
            .await
        })
        .instrument(span)
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
        super::preparation::claim_creation(
            &mut self.tx,
            self.successor_state.run_key,
            self.successor_state.run_key.0,
        )
        .await?;
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

    #[test]
    fn size_fallback_requires_a_recognized_data_limit_and_sqlstate() {
        for message in [
            "transaction size limit 10mb exceeded",
            "transaction size limit",
            "transaction row limit exceeded",
            "transaction row limit",
            "maximum row size exceeded",
            "maximum column size exceeded",
        ] {
            assert!(is_size_refusal(Some("54000"), message));
            assert!(!is_size_refusal(Some("08006"), message));
        }
        for message in [
            "key size too large",
            "transaction age limit of 300s exceeded",
            "more than 24 indexes per table are not allowed",
            "unknown limit",
            "",
        ] {
            assert!(!is_size_refusal(Some("54000"), message));
        }
    }

    struct RecordingPair {
        steps: Arc<Mutex<Vec<&'static str>>>,
        failure: Option<(&'static str, &'static str, &'static str)>,
    }
    impl RecordingPair {
        fn step(&self, name: &'static str) -> Result<()> {
            self.steps.lock().unwrap().push(name);
            if let Some((step, code, message)) = self.failure
                && step == name
            {
                return Err(sqlx::Error::Database(Box::new(TestDatabaseError {
                    code: Some(code),
                    message,
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
            for (code, message, size) in [
                ("54000", "transaction row limit exceeded", true),
                ("54000", "transaction age limit of 300s exceeded", false),
                ("40001", "serialization failure", false),
                ("08006", "connection lost", false),
            ] {
                let result = execute_pair(RecordingPair {
                    steps: Arc::new(Mutex::new(Vec::new())),
                    failure: Some((step, code, message)),
                })
                .await;
                match code {
                    "54000" if size => {
                        assert_eq!(result.unwrap(), PairCommitResult::SizeRefused, "{step}")
                    }
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
