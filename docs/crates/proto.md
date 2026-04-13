# tokeira-proto

Generated protobuf and gRPC bindings for Tokeira, covering both the public Temporal-compatible API surface and internal control-plane packages.

## Dependencies

- `tokeira-types` — domain types for conversions
- External: `prost`, `prost-types`, `tonic`, `serde`, `serde_json`, `thiserror`, `time`, `uuid`
- Build: `tonic-build`, `walkdir`

## Module Structure

| File | Contents |
|---|---|
| `public.rs` | All 24 `temporal.api.*` packages, service constants, file descriptor set, convenience re-exports |
| `internal.rs` | `tokeira.internal.runtime.v1` and `tokeira.internal.admin.v1` (currently empty shells), internal file descriptor set |
| `conversions/mod.rs` | `ProtoConversionError` enum (InvalidUuid, InvalidTaskToken, InvalidTimestamp, MissingField) |
| `conversions/common.rs` | Wire ↔ domain helpers for payloads, headers, memo, search attributes, task queues, task tokens, timestamps, durations |

## Public API Packages

Uses upstream Temporal API protos (v1.43.0) vendored via `tools/proto-sync`. The `public.rs` module exports all 24 packages under `temporal::api::*::v1`:

`activity`, `batch`, `command`, `common`, `deployment`, `enums`, `errordetails`, `export`, `failure`, `filter`, `history`, `namespace`, `nexus`, `operatorservice`, `protocol`, `query`, `replication`, `schedule`, `sdk`, `taskqueue`, `update`, `version`, `workflow`, `workflowservice`

Convenience re-exports: `common`, `enums`, `failure`, `history`, `operatorservice`, `taskqueue`, `workflow`, `workflowservice`.

Service constants: `WORKFLOW_SERVICE_NAME`, `OPERATOR_SERVICE_NAME`, `WORKFLOW_HTTP_SERVICE`, `OPERATOR_HTTP_SERVICE`, `http_proxy_path()`.

## Conversions

`conversions/common.rs` provides explicit helpers:

- `payload_from_domain` / `payload_to_domain` — `Payload` ↔ proto
- `payloads_from_domain` / `payloads_to_domain` — `Payloads` ↔ proto
- `headers_from_domain` / `headers_to_domain` — `Headers` ↔ proto `Header`
- `memo_from_domain` / `memo_to_domain` — `Memo` ↔ proto
- `search_attributes_from_domain` / `search_attributes_to_domain` — `SearchAttributes` ↔ proto
- `task_queue_from_domain` / `task_queue_to_domain` — `TaskQueueName` ↔ proto `TaskQueue`
- `encode_task_token` / `decode_task_token` — `TaskToken` ↔ opaque bytes (JSON)
- `workflow_execution_from_ids` — build proto `WorkflowExecution` from IDs
- `to_proto_timestamp` / `to_opt_proto_timestamp` — `OffsetDateTime` → `prost_types::Timestamp`
- `to_proto_duration` / `to_opt_proto_duration` — `time::Duration` → `prost_types::Duration`

## Internal Protos

`tokeira.internal.runtime.v1` and `tokeira.internal.admin.v1` are declared but currently empty. The file descriptor set is available for future reflection use.

## Status

Stable. All 24 upstream Temporal API packages are generated. Conversion helpers cover the types needed by the edge and runtime layers. Dead conversion files (workflow.rs, operator.rs) have been removed.
