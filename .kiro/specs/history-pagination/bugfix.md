# Bugfix Requirements Document

## Introduction

A history read that wants every event passes `usize::MAX` as the limit. The DSQL repository substitutes 1,000 for it (`DEFAULT_HISTORY_PAGE_SIZE`, from `dsql-throughput-optimization` Requirement 4.5), while the in-memory repository returns everything. So on DSQL every whole-history reader stops at event 1,000 without saying so, and tests on the in-memory repository can't see it:

- GetWorkflowExecutionHistory with `maximum_page_size` 0 asks for `usize::MAX`, receives 1,000 events, and decides whether more exist by comparing the count with `usize::MAX`. It returns no `next_page_token`, so the client is told an incomplete history is complete. v1.31.0 treats a page size of 0 as 256, clamps larger sizes to 256, and keeps paging (`service/frontend/workflow_handler.go:909-927`; `common/primitives/constants.go:20-21 @ v1.31.0`).
- GetWorkflowExecutionHistoryReverse reverses the truncated prefix, so its first page starts at event 1,000 rather than the run's last event.
- Reset validation, batch reset and its target resolution, a direct query's poll response, the update lifecycle snapshot, the restore of an activity's original options, and the edge's last-event-id lookup all read only the first 1,000 events.

Each DSQL read also fetches every history batch after its cursor before decoding, though it returns at most a page. Paging through a run's history therefore reads O(n²) bytes.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN `read_history` or `read_attributed_history` is called with `usize::MAX` on the DSQL repository THEN the system returns at most 1,000 events, with no sign that more exist

1.2 WHEN GetWorkflowExecutionHistory is called with `maximum_page_size` 0 for a run with more than 1,000 events on DSQL THEN the system returns the first 1,000 events with an empty `next_page_token`

1.3 WHEN GetWorkflowExecutionHistory or GetWorkflowExecutionHistoryReverse is called with `maximum_page_size` above 256 THEN the system returns pages of the requested size, where v1.31.0 returns at most 256

1.4 WHEN GetWorkflowExecutionHistoryReverse is called without a token for a run with more than 1,000 events on DSQL THEN the first page starts at event 1,000 rather than the run's last event

1.5 WHEN GetWorkflowExecutionHistoryReverse returns the page that includes event 1 THEN the response still carries a non-empty `next_page_token`, so the client makes one more request for an empty page

1.6 WHEN reset validation, batch reset, batch reset target resolution, a direct query's poll response, the update lifecycle snapshot, or the restore of an activity's original options reads a run's history on DSQL THEN it sees at most the first 1,000 events

1.7 WHEN the edge needs a run's last event id THEN it reads the whole history to find it, and on DSQL it finds at most event 1,000

1.8 WHEN the DSQL repository reads a page of history THEN it fetches every history batch after `after_event_id`, and decodes only until the page is full

### Expected Behavior (Correct)

2.1 WHEN `read_history` or `read_attributed_history` is called THEN both repositories SHALL return the events after `after_event_id` in order, at most `limit` of them, and fewer than `limit` only when no more events exist. No repository SHALL substitute a smaller limit for the one requested.

2.2 WHEN a caller needs the rest of a run's history THEN it SHALL read it with `read_history_to_end` or `read_attributed_history_to_end`, which read pages of a bounded size until a short page. No production caller SHALL pass `usize::MAX` as a limit.

2.3 WHEN the DSQL repository reads a page THEN it SHALL fetch history batches in bounded statements, ordered by `first_event_id` with a `LIMIT`, continuing after the last batch it read until it holds `limit` events or no batches remain.

2.4 WHEN GetWorkflowExecutionHistory or GetWorkflowExecutionHistoryReverse is called with a `maximum_page_size` of 0 or less, or above 256, THEN the edge SHALL use 256, as v1.31.0 does (`service/frontend/workflow_handler.go:909-927, 976-992 @ v1.31.0`).

2.5 WHEN a GetWorkflowExecutionHistory page is full THEN the response SHALL carry a `next_page_token` for the events after it, unless the page holds the run's close event. WHEN the page is shorter, or holds the run's close event, THEN the token SHALL be empty, except where `wait_new_event` keeps a token on an open run, as it does today. Nothing follows a close event, and v1.31.0 ends a closed run's read there (`service/history/api/getworkflowexecutionhistory/api.go:327-398 @ v1.31.0`).

2.6 WHEN GetWorkflowExecutionHistoryReverse is called without a token THEN its first page SHALL end at the run's last event, taken from the run's state. Each page SHALL hold the page size's worth of events before its token's event id, newest first, and the token SHALL be empty on the page that includes event 1.

2.7 WHEN the edge needs a run's last event id THEN it SHALL take `last_event_id` from the run's state.

2.8 WHEN the runtime restores an activity's original options THEN it SHALL read each matching activity's scheduled event by its event id.

2.9 WHEN reset validation, batch reset, batch reset target resolution, a direct query's poll response, or the update lifecycle snapshot needs a run's history THEN it SHALL read it to the end (criterion 2.2).

### Unchanged Behavior (Regression Prevention)

3.1 WHEN a page has fewer events than the limit because the history ends THEN both repositories SHALL CONTINUE TO return exactly the remaining events

3.2 WHEN GetWorkflowExecutionHistory is called with `wait_new_event` or with the close-event filter THEN the edge SHALL CONTINUE TO apply today's token and filtering rules to each page it returns, except that a close-event read reads on past a full page that holds no close event, rather than returning that page or waiting on it, and the page that holds the close event ends the read (criterion 2.5)

3.3 WHEN a workflow task is delivered THEN its poll response SHALL CONTINUE TO carry the history up to the event the task started at, read with an exact limit (`translate/from_internal.rs`)

3.4 Page tokens SHALL CONTINUE TO encode the last event id returned (forward) or the event id to read before (reverse), so a token issued before the fix stays valid

### Out of Scope

- Poll responses carry the whole history in one message, both for workflow tasks and for direct queries. v1.31.0 sends a first page and a `next_page_token` instead (`service/history/api/recordworkflowtaskstarted/api.go:336-362 @ v1.31.0`). Paging them is a separate change.
