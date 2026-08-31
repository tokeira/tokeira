# tokeira-config

Typed configuration contracts for `tokeirad` and the embedded engine, together
with loading, overlay, validation, documentation, and secret-reference
primitives.

## Where it sits

This cross-cutting crate defines configuration data and resolution rules used by
process and embedded hosts. It is separate from deployment definitions: the
deployment layer decides how configuration reaches a process, while this crate
decides how the document is parsed and validated.

## Surface map

| Area | Representative contracts |
|---|---|
| Server schema | `TokeiraConfig`, infrastructure, policy, capacity, emergency, authorization, observability, task-queue, Nexus, and Worker Compute settings |
| Embedded schema | `EmbeddedEngineConfig`, explicit `EmbeddedStorageConfig`, DSQL limits and migration policy |
| Sources | `ConfigSource`, `CONFIG_ENV`, file and environment-document locators |
| Loading | `load_config`, optional profile deep merge, `write_config_toml` |
| Overlays | Schema-default overlay and complete-document rendering |
| Secrets | `SecretRef`, redacting `Secret<T>`, `SecretsProvider`, `NoSecretsProvider` |
| Documentation | `CONFIG_FIELD_CATALOG` and field classification metadata |
| CLI | Shared `Cli` flags for config, resolved output, and build-version output |

## Contracts

- Config structs reject unknown fields and validate related values together.
- A named source that cannot be read is a hard error; source selection never
  silently falls through.
- File and environment content use the same parse and overlay pipeline. The
  source layer does not fetch configuration over a network.
- A partial overlay receives the schema's nested defaults, then renders as a
  complete effective document.
- Embedded storage is an explicit closed choice: in-memory, managed DSQL, or an
  existing DSQL identity. Invalid durable configuration never falls back to
  in-memory execution.
- Configuration stores secret references, not secret values. Resolved
  `Secret<T>` values redact debug output and cannot be serialized.

## It does not own

The crate does not create infrastructure, open storage connections, install
observability, fetch store-backed secrets without an injected provider, or start
the engine. Those actions belong to deployment or process composition layers.

## Pointers

- [Crate root](../../crates/tokeira-config/src/lib.rs)
- [Embedded configuration](../../crates/tokeira-config/src/embedded.rs)
- [Configuration sources](../../crates/tokeira-config/src/source.rs)
- [Engine facade](engine.md)
- [Provisioning guide](../provisioning/README.md)
