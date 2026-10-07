//! The runtime's part in `run-growth-limits`: the limits it puts on each commit
//! it makes, and the termination of a run a store refused for one of them.
//!
//! A store refuses a commit over a limit with
//! [`RunLimitExceeded`](tokeira_kernel::limits::RunLimitExceeded) and writes
//! nothing of it. When the breach terminates the run, the caller terminates it
//! through the run's lane with an ordinary terminate command, so the
//! termination has every effect a termination has, and answers with the
//! breach.

use time::OffsetDateTime;
use tokeira_kernel::{
    Command, TerminateRequest,
    limits::{self, RunGrowthLimits, RunLimit, RunLimitExceeded},
};
use tokeira_proto::{
    conversions::common::{failure_to_payload, payload_to_failure},
    failure_limits::oversized_failure,
};
use tokeira_types::{Payload, Payloads, RequestContext, RequestId, RunKey};

/// A growth limit: v1.31.0's value, which only the Temporal functional
/// harness's build overrides (`run-growth-limits` criterion 2.10;
/// `conformance-config-override`).
#[cfg(not(feature = "conformance"))]
fn growth_limit(_key: &str, default: usize) -> usize {
    default
}

#[cfg(feature = "conformance")]
fn growth_limit(key: &str, default: usize) -> usize {
    crate::conformance::reads()
        .get_i64(key)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default)
}

/// The limits the runtime puts on each commit it makes.
pub(crate) fn run_growth_limits() -> RunGrowthLimits {
    RunGrowthLimits {
        history_size_error: growth_limit(
            "limit.historySize.error",
            limits::HISTORY_SIZE_LIMIT_ERROR,
        ),
        history_size_warn: growth_limit("limit.historySize.warn", limits::HISTORY_SIZE_LIMIT_WARN),
        history_count_error: growth_limit(
            "limit.historyCount.error",
            limits::HISTORY_COUNT_LIMIT_ERROR,
        ),
        history_count_warn: growth_limit(
            "limit.historyCount.warn",
            limits::HISTORY_COUNT_LIMIT_WARN,
        ),
        state_size_error: growth_limit(
            "limit.mutableStateSize.error",
            limits::MUTABLE_STATE_SIZE_LIMIT_ERROR,
        ),
        state_size_warn: growth_limit(
            "limit.mutableStateSize.warn",
            limits::MUTABLE_STATE_SIZE_LIMIT_WARN,
        ),
        transaction_size: growth_limit(
            "system.transactionSizeLimit",
            limits::TRANSACTION_SIZE_LIMIT,
        ),
    }
}

/// The encoded size above which a retried activity's failure is replaced
/// before it is stored (`run-growth-limits` criterion 2.8).
pub(crate) fn stored_activity_failure_limit() -> usize {
    growth_limit(
        "limit.mutableStateActivityFailureSize.error",
        limits::MUTABLE_STATE_ACTIVITY_FAILURE_SIZE_LIMIT_ERROR,
    )
}

/// The failure a retried activity stores: the worker's, or, when its encoded
/// size is over `limit`, a server failure `Failure exceeds size limit.` not
/// marked non-retryable, whose cause is the worker's failure cut down so that
/// the whole fits `limit` (`truncateRetryableActivityFailure`,
/// mutable_state_impl.go:6587-6608 @ v1.31.0). The payload's data is the
/// encoded `Failure`, which is what v1.31.0 measures.
pub(crate) fn stored_activity_failure(failure: Payload, limit: usize) -> Payload {
    if failure.data.len() <= limit {
        return failure;
    }
    failure_to_payload(&oversized_failure(
        &payload_to_failure(&failure),
        limit,
        false,
    ))
}

/// Whether `command` completes a workflow task, whose refused history batch
/// terminates the run (`respondworkflowtaskcompleted/api.go:645-674 @
/// v1.31.0`).
pub(crate) fn completes_workflow_task(command: &Command) -> bool {
    matches!(
        command,
        Command::WorkflowTaskCompleted(_)
            | Command::WorkflowTaskCompletedWithCron { .. }
            | Command::WorkflowTaskCompletedWithRetry { .. }
    )
}

/// The breach in a commit's error when it terminates the run: a breach of the
/// history size, count or state size limit, or of the transaction size limit
/// by a workflow task's completion (`context.go:1002-1113 @ v1.31.0`).
pub(crate) fn terminating_breach(
    error: &anyhow::Error,
    completes_workflow_task: bool,
) -> Option<RunLimitExceeded> {
    let breach = error.downcast_ref::<RunLimitExceeded>()?;
    let terminates = match breach.limit {
        RunLimit::HistorySize | RunLimit::HistoryCount | RunLimit::StateSize => true,
        RunLimit::TransactionSize => completes_workflow_task,
    };
    terminates.then(|| breach.clone())
}

/// The terminate v1.31.0 records for a breach: its reason, no details but for
/// a history batch's, which carry the error as one JSON payload, and the
/// `history-service` identity (`forceTerminateWorkflow`, context.go:1115-1155;
/// `respondworkflowtaskcompleted/api.go:655-662 @ v1.31.0`). A run is
/// terminated once, so its key makes the request id.
pub(crate) fn terminate_command(run_key: RunKey, breach: &RunLimitExceeded) -> Command {
    let now = OffsetDateTime::now_utc();
    let details =
        (breach.limit == RunLimit::TransactionSize).then(|| json_string_payloads(&breach.message));
    Command::Terminate(TerminateRequest {
        reason: breach.limit.termination_reason().to_owned(),
        details,
        identity: "history-service".to_owned(),
        links: Vec::new(),
        request: RequestContext {
            request_id: RequestId(format!("limit-terminate:{}", run_key.0)),
            caller_identity: Some("history-service".to_owned()),
            principal: None,
            received_at: now,
        },
        now,
    })
}

/// `payloads.EncodeString`: one `json/plain` payload holding `text`.
fn json_string_payloads(text: &str) -> Payloads {
    let mut payload = Payload::new(serde_json::to_vec(text).unwrap_or_default());
    payload
        .metadata
        .insert("encoding".to_owned(), "json/plain".to_owned());
    Payloads(vec![payload])
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost::Message as _;
    use tokeira_proto::{
        failure_limits::FAILURE_EXCEEDS_LIMIT, public::temporal::api::failure::v1 as failure_proto,
    };

    use super::*;

    fn arb_text() -> impl Strategy<Value = String> {
        prop_oneof![3 => "[a-zé€😀 ]{0,40}", 1 => "[a-z€]{0,2500}"]
    }

    fn arb_failure() -> impl Strategy<Value = failure_proto::Failure> {
        let info = prop_oneof![
            Just(None),
            (any::<bool>(), "[A-Za-z]{0,12}").prop_map(|(non_retryable, r#type)| Some(
                failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                    failure_proto::ApplicationFailureInfo {
                        non_retryable,
                        r#type,
                        ..Default::default()
                    }
                )
            )),
        ];
        let leaf = (arb_text(), arb_text(), arb_text(), info.clone()).prop_map(
            |(source, message, stack_trace, failure_info)| failure_proto::Failure {
                source,
                message,
                stack_trace,
                failure_info,
                ..Default::default()
            },
        );
        leaf.prop_recursive(3, 6, 1, move |inner| {
            (arb_text(), arb_text(), info.clone(), inner).prop_map(
                |(message, stack_trace, failure_info, cause)| failure_proto::Failure {
                    message,
                    stack_trace,
                    failure_info,
                    cause: Some(Box::new(cause)),
                    ..Default::default()
                },
            )
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: run-growth-limits, Property 4: A stored activity failure fits the limit
        #[test]
        fn property_4_a_stored_activity_failure_fits_the_limit(failure in arb_failure()) {
            let limit = limits::MUTABLE_STATE_ACTIVITY_FAILURE_SIZE_LIMIT_ERROR;
            let payload = failure_to_payload(&failure);
            prop_assert_eq!(payload.data.len(), failure.encoded_len());
            let stored = stored_activity_failure(payload.clone(), limit);
            if failure.encoded_len() <= limit {
                prop_assert_eq!(stored, payload);
            } else {
                // The edge's tests check how the cause is cut down; here, the
                // stored failure stands in for it and fits the limit.
                prop_assert!(stored.data.len() <= limit);
                let replaced = payload_to_failure(&stored);
                prop_assert_eq!(replaced.message.as_str(), FAILURE_EXCEEDS_LIMIT);
                prop_assert_eq!(
                    replaced.failure_info,
                    Some(failure_proto::failure::FailureInfo::ServerFailureInfo(
                        failure_proto::ServerFailureInfo { non_retryable: false }
                    ))
                );
                if let Some(cause) = replaced.cause {
                    prop_assert!(failure.message.starts_with(&cause.message));
                }
            }
        }
    }

    #[test]
    fn only_a_completion_s_batch_terminates() {
        let batch = anyhow::Error::new(RunLimitExceeded::transaction_size(5, 4));
        assert_eq!(terminating_breach(&batch, false), None);
        assert!(terminating_breach(&batch, true).is_some());
        for limit in [
            RunLimit::HistorySize,
            RunLimit::HistoryCount,
            RunLimit::StateSize,
        ] {
            let breach = anyhow::Error::new(RunLimitExceeded::of(limit));
            assert_eq!(
                terminating_breach(&breach, false).map(|breach| breach.limit),
                Some(limit)
            );
        }
        assert_eq!(terminating_breach(&anyhow::anyhow!("other"), true), None);
    }

    #[test]
    fn the_terminate_carries_v1_31_0_s_reason_details_and_identity() {
        let run_key = RunKey::new();
        let Command::Terminate(size) =
            terminate_command(run_key, &RunLimitExceeded::of(RunLimit::HistorySize))
        else {
            panic!("expected a terminate");
        };
        assert_eq!(size.reason, "Workflow history size exceeds limit.");
        assert_eq!(size.details, None);
        assert_eq!(size.identity, "history-service");

        let Command::Terminate(batch) =
            terminate_command(run_key, &RunLimitExceeded::transaction_size(5, 4))
        else {
            panic!("expected a terminate");
        };
        assert_eq!(batch.reason, "Transaction size exceeds limit.");
        let details = batch.details.expect("details");
        assert_eq!(details.0.len(), 1);
        assert_eq!(
            details.0[0].data,
            br#""transaction size of 5 bytes exceeds limit of 4 bytes""#
        );
        assert_eq!(
            details.0[0].metadata.get("encoding").map(String::as_str),
            Some("json/plain")
        );
    }
}
