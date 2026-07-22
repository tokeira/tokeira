# Temporal OpenAPI artifacts

These documents are the decompressed official OpenAPI artifacts distributed by
`go.temporal.io/api@v1.62.11`, the release pinned by `TEMPORAL_PROTO_VERSION`.
They were extracted mechanically from that module's
`temporalproto/openapi/openapi.go`; no generated schema content was authored or
modified in Tokeira.

The uncompressed files are committed because the HTTP compatibility edge serves
their exact bytes, while keeping runtime startup independent from gzip decoding.

| File | SHA-256 |
| --- | --- |
| `openapiv2.swagger.json` | `5035a2f0b56212124cc5480d90bd6e7a2770d4342288c5ecbc97b7913819e770` |
| `openapiv3.yaml` | `a9d8ebd92bf171caed8fb3ba46756215bee2a0a9b7c7e482c3740ac4ded87cc1` |
