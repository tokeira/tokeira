# History Pagination — Bugfix Design

## Overview

Every history read goes through `RunRepository::read_history(run_key, after_event_id, limit)` or its attributed twin. This fix makes that contract hold on both repositories, gives callers that need every event an explicit way to read them, bounds DSQL's statements, and makes the edge page history as v1.31.0 does. Readers that need one fact read that fact instead of the history.

## Glossary

- **Page:** the events one read returns.
- **Short page:** a page with fewer events than its limit. It means the history has no more events after it.
- **Whole-history reader:** a caller that needs every event after a cursor.
- **History batch:** a `history_batch` row, holding the events one transition appended.
- **Effective page size:** the page size the edge uses for a history call: 256 when the request asks for 0 or less or for more than 256, the requested size otherwise.

## Bug Details

### Bug Condition

A read misbehaves when it asks the DSQL repository for `usize::MAX` events and the run has more than 1,000 events after the cursor (1.1, 1.2, 1.4, 1.6, 1.7), when the edge receives a page size above 256 (1.3), or when a reverse page includes event 1 (1.5). Every DSQL read pays for all the batches after its cursor (1.8).

### Examples

- A run with 1,500 events, read with GetWorkflowExecutionHistory and a page size of 0, returns events 1 to 1,000 and an empty token. The SDK replays a history without its last 500 events.
- The same run, read with GetWorkflowExecutionHistoryReverse and a page size of 0, returns events 1,000 down to 1 in one page, with a token that asks for an empty page.
- Resetting that run to an event after 1,000 can't find its reset point.

## Expected Behavior

### Preservation Requirements

- A page that ends with the history still returns exactly the remaining events (3.1).
- `wait_new_event` and the close-event filter keep today's per-page rules, except that a close-event read reads on past full pages that hold no close event, and the close event's page ends the read (3.2, 2.5).
- A workflow task's poll response still carries the history up to the event the task started at, read with an exact limit (3.3).
- Page tokens keep their encoding, so tokens issued before the fix stay valid (3.4).

## Root Cause

`dsql-throughput-optimization` Requirement 4.5 gave the DSQL repository a 1,000-event default for callers that pass no page size, to stop unbounded reads. Callers that wanted everything kept passing `usize::MAX`, and the in-memory repository kept honouring it, so the two repositories diverged silently. The edge then derived its token from the limit it asked for, not from the page it received. Separately, the DSQL read never put a `LIMIT` in its SQL, so a page fetched every remaining batch.

## Correctness Properties

Property 1: Pages concatenate to the history

_For any_ history and any limit of at least 1, reading pages from event 0, each starting after the previous page's last event, SHALL yield the whole history in order, with every page full except the last. `read_history_to_end` SHALL return the same events.

**Validates: Requirements 2.1, 2.2, 3.1**

Property 2: Forward pages follow v1.31.0's page size

_For any_ history of an open run and any requested page size, GetWorkflowExecutionHistory SHALL page through the whole history in pages of the effective page size, with a token after every full page and none after the last, short page.

**Validates: Requirements 2.4, 2.5**

Property 3: Reverse pages start at the last event

_For any_ history and any requested page size, GetWorkflowExecutionHistoryReverse SHALL return the whole history newest first, starting at the run's last event, in pages of the effective page size, with an empty token exactly on the page that includes event 1.

**Validates: Requirements 2.4, 2.6**

Property 4: Single facts come from state or by id

The edge's last event id SHALL equal the run state's `last_event_id`, and each restored activity option SHALL come from that activity's scheduled event.

**Validates: Requirements 2.7, 2.8**

## Fix Implementation

### Storage (`tokeira-storage`)

- `RunRepository::read_history_to_end(run_key, after_event_id)` and `read_attributed_history_to_end(run_key, after_event_id)` are default methods. They call `read_history` or `read_attributed_history` with pages of `HISTORY_READ_PAGE` (1,024) events, each after the previous page's last event, until a short page. Wrappers and test doubles inherit them.
- DSQL removes `effective_history_limit` and `DEFAULT_HISTORY_PAGE_SIZE`. `do_read_attributed_history` selects `WHERE run_key = $1 AND last_event_id > $2 ORDER BY first_event_id ASC LIMIT $3`, 64 batches a statement. It decodes them, keeping events after `after_event_id`, and repeats after the last batch's `last_event_id` until it holds `limit` events or a statement returns fewer than 64 batches.
- The in-memory repository already meets 2.1 and is unchanged.

### Edge (`tokeira-edge`)

- `effective_history_page_size(requested)` returns 256 (`HISTORY_MAX_PAGE_SIZE`) for 0 or less or for more than 256, and the requested size otherwise.
- GetWorkflowExecutionHistory reads with the effective page size. Its existing full-page check (`history.len() >= limit`) then decides the token, since the limit is finite.
- The page that holds the run's close event carries no token, even when it is full (criterion 2.5). Before this fix only a finite requested size could fill a page. At the default size of 256, a closed run whose close event ends a page would otherwise hand a token to a client following the history with `wait_new_event`, and that client would then wait on a closed run, 20 s at a time.
- A close-event read skips a full page that holds no close event and reads the next one, rather than returning that page or waiting on it (criterion 3.2). Waiting there would hold a long poll for its 20 s expiry once per page before it reached the close event.
- GetWorkflowExecutionHistoryReverse takes `before` from the token, or as the run's `last_event_id` plus 1 from `load_run`. It reads the events from `max(1, before - size)` to `before - 1`, which works because event ids are dense, reverses them, and sets the token to the page's smallest event id unless that id is 1.
- `read_last_event_id` returns the run state's `last_event_id`, or 0 for an absent run.
- Reset validation, batch reset, batch reset target resolution and the direct query's poll response read the history with the to-end methods.

### Runtime (`tokeira-runtime`)

- `original_activity_options` reads each matching activity's scheduled event with `read_history(run_key, schedule_event_id - 1, 1)`, and fails as today when that event isn't an `ActivityTaskScheduled`.
- `update_lifecycle_snapshot` reads the history with `read_history_to_end`. A completed update leaves no record in run state (`pending_updates` holds only accepted ones), so its outcome has to come from history.

### Other specs

- `dsql-throughput-optimization` Requirement 4.5 and its Phase 2 design point here.
- `history-delivery`'s design describes the poll response's exact-limit read and the GetWorkflowExecutionHistory page size as above.

### Out of scope

Poll responses still carry the whole history in one message, where v1.31.0 sends a first page and a token (`service/history/api/recordworkflowtaskstarted/api.go:336-362 @ v1.31.0`).

## Testing Strategy

### Exploratory Bug Condition Checking

- A DSQL unit test asserts that the batch statement carries a `LIMIT`, and the test that pinned `effective_history_limit(usize::MAX)` to 1,000 goes with the function.
- On live DSQL, when a cluster is available: a run with more than 1,000 events reads completely through both history calls.

### Property-Based Tests

- Property 1 on the in-memory repository, with histories of up to 3,000 events and limits from 1 to 2,000, including `read_history_to_end`.
- Properties 2 and 3 at the edge on the in-memory repository, with open runs of 2 to 701 events, weighted towards exactly one and two full pages, and requested page sizes weighted towards 0, 256 and 257. The expected page size comes from a reference copy of v1.31.0's rule, not from `effective_history_page_size`.
- The reverse window's arithmetic, for cursors up to 5,000 and page sizes up to 600.

### Unit Tests

- `effective_history_page_size` at 0, 1, 256, 257 and a large value.
- Property 4: the last event id on a run whose history is longer than a page, and an activity options restore that reads its scheduled event by id.
- A closed run whose close event ends a full page: that page carries no token, with either filter, with and without `wait_new_event`.
- A close-event long poll on a closed run whose close event lies past the first page returns at once, with the close event.

### Preservation Checking

- The existing history tests for `wait_new_event`, the close-event filter and poll responses stay green.
