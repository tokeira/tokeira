//! The only visibility boundary for a staged publication. Participant writes,
//! the current pointer and the new run become visible in one fenced transaction.

use super::*;

impl DsqlRunRepository {
    pub(in crate::dsql::run_repository) async fn do_publish_preparation(
        &self,
        token: PreparationToken,
        fence: WriteFence,
    ) -> Result<PublicationResult> {
        fence.ensure_home(token.home)?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.begin().await?;
        if let Some(lost) = Self::check_fence(&mut tx, &fence, token.target_run).await? {
            return Err(lost.into());
        }
        let record = by_token(&mut tx, &token).await?;
        if record.phase >= PUBLISHED {
            let state = super::super::outbound::read_source(&mut tx, token.target_run)
                .await?
                .ok_or(RecoveryError::InvalidPreparation("published run is absent"))?;
            tx.commit().await?;
            return Ok(PublicationResult::AlreadyPublished {
                new_run: Box::new(state),
            });
        }
        ensure!(
            record.phase == READY,
            "publication is not completely staged"
        );
        let publication = token.decode_manifest(&manifest(&mut tx, &record).await?)?;
        let mut new_state = publication.new_run.next_state.clone();
        let key = Self::current_execution_key(new_state.namespace_id, &new_state.workflow_id);
        let current = sqlx::query_as::<_, (Uuid,)>(
            "SELECT run_key FROM current_execution WHERE key=$1 FOR UPDATE",
        )
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
        if current.map(|(run,)| RunKey(run)) != publication.expected_current {
            return Ok(PublicationResult::Conflict {
                reason: "current execution changed during preparation".into(),
            });
        }
        let exists = sqlx::query_as::<_, (Uuid,)>(
            "SELECT run_key FROM workflow_hot WHERE run_key=$1 FOR UPDATE",
        )
        .bind(token.target_run.0)
        .fetch_optional(&mut *tx)
        .await?;
        if exists.is_some() {
            return Ok(PublicationResult::Conflict {
                reason: "publication target already exists".into(),
            });
        }
        claim_creation(&mut tx, token.target_run, token.generation).await?;
        let mut prior = Vec::new();
        for transition in &publication.participants {
            let row = sqlx::query_as::<_, (i64,Option<i64>,Option<i32>)>("SELECT transition_seq,history_size_bytes,state_parts FROM workflow_hot WHERE run_key=$1 FOR UPDATE")
                .bind(transition.next_state.run_key.0).fetch_optional(&mut *tx).await?;
            let Some((seq, history_size, parts)) = row else {
                return Ok(PublicationResult::Conflict {
                    reason: "publication participant is absent".into(),
                });
            };
            if convert::u64_from_i64(seq, "transition_seq")? != transition.expected_seq.0 {
                return Ok(PublicationResult::Conflict {
                    reason: "publication participant sequence changed".into(),
                });
            }
            prior.push((history_size.unwrap_or(0), parts));
        }
        stage::verify(&mut tx, &token, &publication.new_run).await?;
        super::super::successor::settle_publication(&mut tx, &publication).await?;
        let mut participants = Vec::new();
        for (transition, (size, parts)) in publication.participants.iter().zip(prior) {
            let mut state = transition.next_state.clone();
            super::super::commit::write_transition(
                &mut tx,
                state.run_key,
                token.home,
                self.projection_partition_count,
                transition,
                &mut state,
                size,
                parts,
            )
            .await?;
            participants.push(state);
        }
        super::super::commit::write_prepared_transition(
            &mut tx,
            token.target_run,
            token.home,
            self.projection_partition_count,
            &publication.new_run,
            &mut new_state,
        )
        .await?;
        sqlx::query("DELETE FROM run_bulk_write WHERE run_key=$1")
            .bind(token.target_run.0)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE run_preparation SET phase=$2 WHERE preparation_id=$1")
            .bind(token.id)
            .bind(PUBLISHED)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(PublicationResult::Applied {
            participants,
            new_run: Box::new(new_state),
        })
    }
}
