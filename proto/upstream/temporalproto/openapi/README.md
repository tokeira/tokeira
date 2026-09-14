# Temporal OpenAPI artifacts

These documents are the official OpenAPI artifacts published with
`go.temporal.io/api@v1.63.5`, the release pinned by `TEMPORAL_PROTO_VERSION`.
They were copied byte-for-byte from the `openapi/` directory of
`temporalio/api` at tag `v1.63.5` (`openapiv2.json` is stored here as
`openapiv2.swagger.json`); that directory is the source the module embeds in
`temporalproto/openapi/`. No generated schema content was authored or modified
in Tokeira.

The uncompressed files are committed because the HTTP compatibility edge serves
their exact bytes, while keeping runtime startup independent from gzip decoding.

`tools/proto-sync` does not manage this directory: a sync wipes `proto/upstream/`
and this folder must be refreshed by hand at the new tag, then
`cargo run -p proto-sync -- generate` copies the files into the generated tree.

| File | SHA-256 |
| --- | --- |
| `openapiv2.swagger.json` | `3a64a4937a8b382f8371d0e595900a297db09a2d07b4a4b7f9e899cd5f71b55d` |
| `openapiv3.yaml` | `1447212be18353a6108a352504d607347d741e0f41cbec9b11f8e826ba3ae452` |
