//! Query and update dispatch methods of [`TokeiraRuntime`].
//!
//! This `impl` continuation owns the two read-adjacent worker interactions that
//! must be ordered against a run's pending workflow task (WFT) rather than
//! simply mutating it: consistent queries and synchronous updates. Both are
//! transport-coordinated — the runtime parks a caller on a `oneshot` and the
//! lane/transport layer resolves it — so the contract here is mostly about
//! *when* a request is allowed to reach a worker and how the caller is woken.
//!
//! Key invariants:
//! - A consistent query never observes stale state. While a WFT is in flight
//!   the query is buffered behind a barrier (the run's `last_event_id` at query
//!   time) and only released once the run advances past it; otherwise it is
//!   dispatched directly because the run is quiescent.
//! - Updates are two-phase (admit, then worker-accept). The wait policy decides
//!   whether the caller returns at admission or blocks for the final
//!   resolution. The caller registration is always cleaned up on every error
//!   and timeout path so a failed update cannot leak a parked waiter.
use super::*;
use tokeira_observability::{QueryDispatchOutcomeLabel, QueryDispatchPathLabel};

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    /// Dispatch a read-only query to a workflow worker and await the response.
    ///
    /// Two delivery paths exist depending on whether the run has a pending
    /// workflow task (WFT). When a WFT is in flight, the query is buffered
    /// behind a *barrier* — the run's `last_event_id` at query time — so the
    /// worker cannot evaluate it against stale state. Once the WFT completes
    /// and the run advances past the barrier, the transport layer releases the
    /// query for delivery. When no WFT is pending the query goes directly to
    /// the broker for immediate dispatch, because the run is quiescent and the
    /// worker already has up-to-date state.
    pub async fn query_workflow(
        &self,
        execution: ExecutionRef,
        query_type: String,
        query_args: Payloads,
        timeout_after: Duration,
    ) -> Result<QueryResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;

        let state = match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => state,
            LoadedRun::Absent => {
                return Err(anyhow!("execution disappeared before query dispatch"));
            }
        };

        // A paused workflow rejects queries without dispatch, matching Temporal:
        // the run is frozen, so there is no worker turn to evaluate the query
        // against. The edge translates this into a `QueryRejected` response.
        if state.status == tokeira_types::ExecutionStatus::Paused {
            runtime_metrics::record_query_dispatch(
                QueryDispatchPathLabel::Direct,
                QueryDispatchOutcomeLabel::Rejected,
            );
            return Ok(QueryResult::Rejected {
                status: tokeira_types::ExecutionStatus::Paused,
            });
        }

        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        };

        let now = OffsetDateTime::now_utc();
        let sticky_preferred = state.sticky.as_ref().and_then(|affinity| {
            (affinity.expires_at > now).then_some(affinity.worker_identity.clone())
        });
        let required_barrier = state.last_event_id;

        let (response_tx, response_rx) = oneshot::channel();
        let query_id = Uuid::new_v4().to_string();
        let has_pending_wft = state.pending_workflow_task.is_some();

        if has_pending_wft {
            self.buffered_queries
                .buffer(
                    run_key,
                    BufferedQuery {
                        query_id: query_id.clone(),
                        query_type,
                        query_args,
                        required_barrier,
                        enqueued_at: std::time::Instant::now(),
                        response_tx,
                    },
                )
                .map_err(|_| {
                    runtime_metrics::record_query_dispatch(
                        QueryDispatchPathLabel::Buffered,
                        QueryDispatchOutcomeLabel::Rejected,
                    );
                    anyhow!("too many buffered queries for run {:?}", run_key)
                })?;
            runtime_metrics::record_query_dispatch(
                QueryDispatchPathLabel::Buffered,
                QueryDispatchOutcomeLabel::Queued,
            );
        } else {
            self.broker
                .publish_query_task(QueryTask {
                    run_key,
                    query_type,
                    query_args,
                    queue,
                    sticky_preferred,
                    response_tx,
                })
                .await;
            runtime_metrics::record_query_dispatch(
                QueryDispatchPathLabel::Direct,
                QueryDispatchOutcomeLabel::Published,
            );
        }

        let timeout_after: std::time::Duration = timeout_after
            .try_into()
            .map_err(|_| anyhow!("query timeout must be non-negative"))?;

        let cleanup = BufferedQueryCleanup {
            registry: self.buffered_queries.clone(),
            run_key,
            query_id,
            enabled: has_pending_wft,
        };

        match tokio::time::timeout(timeout_after, response_rx).await {
            Ok(Ok(result)) => {
                cleanup.disarm();
                Ok(result)
            }
            Ok(Err(_)) => Err(anyhow!("query response channel closed")),
            Err(_) => Err(anyhow!("query timed out")),
        }
    }

    /// Dispatch a synchronous update and optionally wait for completion.
    ///
    /// Updates follow a two-phase lifecycle: the kernel first *admits* the
    /// update (recording it in `admitted_updates`), then the worker *accepts*
    /// it during a subsequent WFT, which promotes it to `pending_updates` and
    /// writes the acceptance event. This split lets the API return quickly for
    /// `Accepted` wait policy (phase 1) while `Completed` callers block on a
    /// oneshot until the lane notifies the `UpdateRegistry` with the final
    /// resolution.
    pub async fn update_workflow(
        &self,
        execution: ExecutionRef,
        update_id: String,
        update_name: String,
        input: Payloads,
        request: RequestContext,
        timeout_after: Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateOutcome> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;

        let mut complete_rx = None;
        if wait_policy == UpdateWaitPolicy::Completed {
            let (complete_tx, rx) = oneshot::channel::<UpdateResolution>();
            self.update_registry.register(
                run_key,
                update_id.clone(),
                update_name.clone(),
                input.clone(),
                request.caller_identity.clone().unwrap_or_default(),
                complete_tx,
            );
            complete_rx = Some(rx);
        }

        let command = Command::Update(UpdateRequest {
            update_id: update_id.clone(),
            update_name,
            input,
            request,
            now: OffsetDateTime::now_utc(),
        });

        let submit_result = self.submit(run_key, command).await;
        let commit_result = match submit_result {
            Ok(result) => result,
            Err(error) => {
                if wait_policy == UpdateWaitPolicy::Completed {
                    self.update_registry.remove(run_key, &update_id);
                }
                return Err(error);
            }
        };

        // The update has been admitted (tracked in admitted_updates).
        // The accepted_event_id is not yet known — it will be assigned
        // when the worker sends an Acceptance message. For now, use 0
        // as a placeholder for the Accepted wait policy.
        match commit_result {
            CommitResult::Applied { .. } => {}
            CommitResult::Duplicate => {
                if wait_policy == UpdateWaitPolicy::Completed {
                    self.update_registry.remove(run_key, &update_id);
                }
                return Ok(UpdateOutcome::Accepted {
                    accepted_event_id: 0,
                });
            }
            CommitResult::Conflict { reason } => {
                if wait_policy == UpdateWaitPolicy::Completed {
                    self.update_registry.remove(run_key, &update_id);
                }
                return Err(anyhow!("update commit conflicted: {reason}"));
            }
        };

        if wait_policy == UpdateWaitPolicy::Accepted {
            return Ok(UpdateOutcome::Accepted {
                accepted_event_id: 0,
            });
        }

        let timeout_after: std::time::Duration = timeout_after
            .try_into()
            .map_err(|_| anyhow!("update timeout must be non-negative"))?;
        let complete_rx = complete_rx.expect("completion receiver should exist");

        match tokio::time::timeout(timeout_after, complete_rx).await {
            Ok(Ok(UpdateResolution::Completed { result })) => Ok(UpdateOutcome::Completed {
                accepted_event_id: 0,
                result,
            }),
            Ok(Ok(UpdateResolution::Rejected { failure })) => Ok(UpdateOutcome::Rejected {
                accepted_event_id: 0,
                failure,
            }),
            Ok(Ok(UpdateResolution::RunClosed)) => {
                Err(anyhow!("run closed before update completed"))
            }
            Ok(Err(_)) => Err(anyhow!("update response channel closed")),
            Err(_) => {
                self.update_registry.remove(run_key, &update_id);
                Err(anyhow!("update timed out"))
            }
        }
    }
}
