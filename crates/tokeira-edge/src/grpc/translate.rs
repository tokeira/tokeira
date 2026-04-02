use std::time::Duration;

use time::OffsetDateTime;
use tokeira_kernel::{event::HistoryEvent, WorkflowCommand};
use tokeira_proto::{
    conversions::{
        ProtoConversionError,
        common::{
            memo_from_domain, memo_to_domain, payloads_from_domain, payloads_to_domain,
            search_attributes_from_domain, search_attributes_to_domain, task_queue_from_domain,
            task_queue_to_domain, workflow_execution_from_ids,
        },
    },
    enums, workflowservice,
};
use tokeira_types::{ExecutionStatus, Payload, Payloads, RunId, RunKey, WorkflowId};

use crate::translate::{
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse, DescribeWorkflowExecutionRequest,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse,
    RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
    SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse,
    StartWorkflowExecutionRequest, StartWorkflowExecutionResponse, WorkflowExecutionDescription,
    WorkflowExecutionSummary,
};

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STICKY_TTL: Duration = Duration::from_secs(30);

pub fn start_request_to_edge(
    req: workflowservice::StartWorkflowExecutionRequest,
) -> Result<StartWorkflowExecutionRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.task_queue",
        ))?;

    Ok(StartWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        workflow_type: req.workflow_type,
        task_queue: task_queue_to_domain(task_queue).0,
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        memo: req.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
        search_attributes: req
            .search_attributes
            .as_ref()
            .map(search_attributes_to_domain)
            .transpose()?
            .unwrap_or_default(),
        identity: None,
        run_key: None,
        run_id: None,
        now: None,
    })
}

pub fn signal_request_to_edge(
    req: workflowservice::SignalWorkflowExecutionRequest,
) -> Result<SignalWorkflowExecutionRequest, ProtoConversionError> {
    Ok(SignalWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        signal_name: req.signal_name,
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        identity: non_empty(req.identity),
        now: None,
    })
}

pub fn poll_request_to_edge(
    req: workflowservice::PollWorkflowTaskQueueRequest,
) -> Result<PollWorkflowTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "PollWorkflowTaskQueueRequest.task_queue",
        ))?;

    Ok(PollWorkflowTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue_to_domain(task_queue).0,
        worker_identity: req.identity,
        sticky_run: None,
        timeout: DEFAULT_POLL_TIMEOUT,
        sticky_ttl: DEFAULT_STICKY_TTL,
    })
}

pub fn respond_completed_request_to_edge(
    req: workflowservice::RespondWorkflowTaskCompletedRequest,
) -> Result<RespondWorkflowTaskCompletedRequest, ProtoConversionError> {
    Ok(RespondWorkflowTaskCompletedRequest {
        task_token: req.task_token,
        identity: req.identity,
        commands: req
            .commands
            .into_iter()
            .map(proto_command_to_workflow_command)
            .collect::<Result<Vec<_>, _>>()?,
        force_new_workflow_task: false,
    })
}

pub fn describe_request_to_edge(
    req: workflowservice::DescribeWorkflowExecutionRequest,
) -> Result<DescribeWorkflowExecutionRequest, ProtoConversionError> {
    Ok(DescribeWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
    })
}

pub fn list_request_to_edge(
    req: workflowservice::ListWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(ListWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        page_size: req.page_size.max(0) as usize,
        next_page_token: if req.next_page_token.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&req.next_page_token).into_owned())
        },
    })
}

pub fn count_request_to_edge(
    req: workflowservice::CountWorkflowExecutionsRequest,
) -> Result<CountWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(CountWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        group_by: req.group_by.into_iter().next(),
    })
}

pub fn start_response_to_proto(
    resp: StartWorkflowExecutionResponse,
) -> workflowservice::StartWorkflowExecutionResponse {
    workflowservice::StartWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
    }
}

pub fn signal_response_to_proto(
    _resp: SignalWorkflowExecutionResponse,
) -> workflowservice::SignalWorkflowExecutionResponse {
    workflowservice::SignalWorkflowExecutionResponse {}
}

pub fn poll_response_to_proto(
    resp: PollWorkflowTaskQueueResponse,
) -> workflowservice::PollWorkflowTaskQueueResponse {
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.payload.workflow_id),
        run_id_from_run_key(resp.payload.run_key),
    ));

    workflowservice::PollWorkflowTaskQueueResponse {
        task_token: resp.task_token,
        workflow_execution,
        started_event_id: resp.started_event_id,
        history_blob: history_blob(&resp.payload.history),
        sticky: false,
    }
}

pub fn completed_response_to_proto(
    _resp: RespondWorkflowTaskCompletedResponse,
) -> workflowservice::RespondWorkflowTaskCompletedResponse {
    workflowservice::RespondWorkflowTaskCompletedResponse {
        workflow_completed: false,
        new_run_id: String::new(),
    }
}

pub fn describe_response_to_proto(
    resp: WorkflowExecutionDescription,
) -> workflowservice::DescribeWorkflowExecutionResponse {
    workflowservice::DescribeWorkflowExecutionResponse {
        execution: Some(workflow_execution_info_from_description(resp)),
    }
}

pub fn list_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ListWorkflowExecutionsResponse {
    workflowservice::ListWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

pub fn count_response_to_proto(
    resp: CountWorkflowExecutionsResponse,
) -> workflowservice::CountWorkflowExecutionsResponse {
    workflowservice::CountWorkflowExecutionsResponse {
        count: resp.total_count,
        groups: resp
            .groups
            .into_iter()
            .map(|group| workflowservice::CountGroup {
                group_value: group.value,
                count: group.count,
            })
            .collect(),
    }
}

pub fn proto_command_to_workflow_command(
    cmd: workflowservice::Command,
) -> Result<WorkflowCommand, ProtoConversionError> {
    use workflowservice::command::Attributes;

    match cmd.attributes {
        Some(Attributes::ScheduleActivity(attrs)) => {
            let task_queue = attrs.task_queue.as_ref().ok_or(ProtoConversionError::MissingField(
                "ScheduleActivityCommandAttributes.task_queue",
            ))?;
            Ok(WorkflowCommand::ScheduleActivity {
                activity_id: attrs.activity_id,
                task_queue: task_queue_to_domain(task_queue),
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            })
        }
        Some(Attributes::StartTimer(attrs)) => Ok(WorkflowCommand::StartTimer {
            timer_id: attrs.timer_id,
            fire_at: OffsetDateTime::now_utc()
                + time::Duration::milliseconds(attrs.delay_millis.max(0)),
        }),
        Some(Attributes::UpsertSearchAttributes(attrs)) => Ok(WorkflowCommand::UpsertSearchAttributes(
            attrs
                .search_attributes
                .as_ref()
                .map(search_attributes_to_domain)
                .transpose()?
                .unwrap_or_default(),
        )),
        Some(Attributes::UpsertMemo(attrs)) => Ok(WorkflowCommand::UpsertMemo(
            attrs.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
        )),
        Some(Attributes::CompleteWorkflow(attrs)) => Ok(WorkflowCommand::CompleteWorkflow {
            result: attrs
                .result
                .as_ref()
                .map(payloads_to_domain)
                .unwrap_or_default(),
        }),
        Some(Attributes::FailWorkflow(attrs)) => Ok(WorkflowCommand::FailWorkflow {
            message: attrs.message,
            details: first_payload(
                attrs
                    .details
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
            ),
        }),
        None => Err(ProtoConversionError::MissingField("Command.attributes")),
    }
}

pub fn workflow_command_to_proto(
    cmd: &WorkflowCommand,
) -> Result<workflowservice::Command, ProtoConversionError> {
    use workflowservice::command::Attributes;

    let attributes = match cmd {
        WorkflowCommand::ScheduleActivity {
            activity_id,
            task_queue,
            input,
            ..
        } => Some(Attributes::ScheduleActivity(
            workflowservice::ScheduleActivityCommandAttributes {
                activity_id: activity_id.clone(),
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                input: Some(payloads_from_domain(input)),
            },
        )),
        WorkflowCommand::StartTimer { timer_id, fire_at } => {
            let now = OffsetDateTime::now_utc();
            let delay_millis = (*fire_at - now)
                .whole_milliseconds()
                .clamp(0, i64::MAX as i128) as i64;
            Some(Attributes::StartTimer(
                workflowservice::StartTimerCommandAttributes {
                    timer_id: timer_id.clone(),
                    delay_millis,
                },
            ))
        }
        WorkflowCommand::UpsertSearchAttributes(search_attributes) => Some(
            Attributes::UpsertSearchAttributes(
                workflowservice::UpsertSearchAttributesCommandAttributes {
                    search_attributes: Some(search_attributes_from_domain(search_attributes)),
                },
            ),
        ),
        WorkflowCommand::UpsertMemo(memo) => Some(Attributes::UpsertMemo(
            workflowservice::UpsertMemoCommandAttributes {
                memo: Some(memo_from_domain(memo)),
            },
        )),
        WorkflowCommand::CompleteWorkflow { result } => Some(Attributes::CompleteWorkflow(
            workflowservice::CompleteWorkflowExecutionCommandAttributes {
                result: Some(payloads_from_domain(result)),
            },
        )),
        WorkflowCommand::FailWorkflow { message, details } => Some(Attributes::FailWorkflow(
            workflowservice::FailWorkflowExecutionCommandAttributes {
                message: message.clone(),
                details: Some(payloads_from_optional_payload(details.clone())),
            },
        )),
        WorkflowCommand::CancelWorkflow
        | WorkflowCommand::RecordMarker { .. }
        | WorkflowCommand::ContinueAsNew { .. }
        | WorkflowCommand::RequestCancelActivity { .. }
        | WorkflowCommand::CancelTimer { .. }
        | WorkflowCommand::StartChildWorkflow { .. }
        | WorkflowCommand::SignalExternalWorkflowExecution { .. }
        | WorkflowCommand::RequestCancelExternalWorkflowExecution { .. }
        | WorkflowCommand::ScheduleNexusOperation { .. }
        | WorkflowCommand::CancelNexusOperation { .. }
        | WorkflowCommand::UpdateCompleted { .. }
        | WorkflowCommand::UpdateRejected { .. }
        | WorkflowCommand::ProtocolMessage { .. }
        | WorkflowCommand::RequestNewWorkflowTask
            => {
            return Err(ProtoConversionError::MissingField(
                "WorkflowCommand has no proto Command equivalent",
            ))
        }
    };

    Ok(workflowservice::Command { attributes })
}

fn workflow_execution_info_from_description(
    value: WorkflowExecutionDescription,
) -> workflowservice::WorkflowExecutionInfo {
    workflowservice::WorkflowExecutionInfo {
        namespace: value.namespace,
        workflow_id: value.workflow_id,
        run_id: value.run_id.0.to_string(),
        workflow_type: value.workflow_type,
        task_queue: Some(task_queue_from_domain(&tokeira_types::TaskQueueName(value.task_queue))),
        status: execution_status_to_proto(value.status),
        start_time_unix_nanos: value.start_time.map(to_unix_nanos).unwrap_or_default(),
        execution_time_unix_nanos: None,
        close_time_unix_nanos: value.close_time.map(to_unix_nanos),
        history_length: value.history_length,
        state_transition_count: value.state_transition_count,
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
    }
}

fn workflow_execution_info_from_summary(
    value: WorkflowExecutionSummary,
) -> workflowservice::WorkflowExecutionInfo {
    workflowservice::WorkflowExecutionInfo {
        namespace: value.namespace,
        workflow_id: value.workflow_id,
        run_id: value.run_id.0.to_string(),
        workflow_type: value.workflow_type,
        task_queue: Some(task_queue_from_domain(&tokeira_types::TaskQueueName(value.task_queue))),
        status: execution_status_to_proto(value.status),
        start_time_unix_nanos: value.start_time.map(to_unix_nanos).unwrap_or_default(),
        execution_time_unix_nanos: None,
        close_time_unix_nanos: value.close_time.map(to_unix_nanos),
        history_length: 0,
        state_transition_count: 0,
        memo: Some(Default::default()),
        search_attributes: Some(Default::default()),
    }
}

fn execution_status_to_proto(value: ExecutionStatus) -> i32 {
    use enums::WorkflowExecutionStatus as Proto;

    match value {
        ExecutionStatus::Running => Proto::Running as i32,
        ExecutionStatus::Completed => Proto::Completed as i32,
        ExecutionStatus::Failed => Proto::Failed as i32,
        ExecutionStatus::Cancelled => Proto::Canceled as i32,
        ExecutionStatus::Terminated => Proto::Terminated as i32,
        ExecutionStatus::ContinuedAsNew => Proto::ContinuedAsNew as i32,
        ExecutionStatus::TimedOut => Proto::TimedOut as i32,
    }
}

fn payloads_from_optional_payload(value: Option<Payload>) -> tokeira_proto::common::Payloads {
    match value {
        Some(payload) => Payloads(vec![payload]),
        None => Payloads::default(),
    }
    .pipe(|payloads| payloads_from_domain(&payloads))
}

fn first_payload(payloads: Payloads) -> Option<Payload> {
    payloads.0.into_iter().next()
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn to_unix_nanos(value: OffsetDateTime) -> i64 {
    let nanos = value.unix_timestamp_nanos();
    if nanos > i64::MAX as i128 {
        i64::MAX
    } else if nanos < i64::MIN as i128 {
        i64::MIN
    } else {
        nanos as i64
    }
}

fn run_id_from_run_key(run_key: RunKey) -> RunId {
    RunId(run_key.0)
}

fn history_blob(_history: &[HistoryEvent]) -> Vec<u8> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_request_applies_default_timeout_and_sticky_ttl() {
        let req = workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(tokeira_proto::common::TaskQueue {
                name: "queue".to_string(),
            }),
            identity: "worker-1".to_string(),
            deployment: String::new(),
            build_id: String::new(),
        };

        let edge = poll_request_to_edge(req).expect("poll request should convert");
        assert_eq!(edge.timeout, Duration::from_secs(60));
        assert_eq!(edge.sticky_ttl, Duration::from_secs(30));
    }

    #[test]
    fn command_without_attributes_returns_missing_field() {
        let err = proto_command_to_workflow_command(workflowservice::Command { attributes: None })
            .expect_err("missing attributes should fail");

        match err {
            ProtoConversionError::MissingField(field) => {
                assert_eq!(field, "Command.attributes");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}
