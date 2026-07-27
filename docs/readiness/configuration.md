# Tokeira configuration — readiness

> Release-readiness status for configuration policy and its public documentation.
> The authoritative operator contract is
> [`tokeira-configuration.md`](../conformance/v1.31.0/tokeira-configuration.md);
> Temporal's complete comparison denominator is
> [`temporal-configuration.md`](../conformance/v1.31.0/temporal-configuration.md).

**Last updated:** 2026-07-26

## Release posture

The configuration surface is now machine-accounted rather than estimated:

- an empty `tokeirad` TOML document is valid and selects safe documented defaults;
- every accepted strict production leaf is represented once in
  `tokeira_config::CONFIG_FIELD_CATALOG`;
- the generated [`config.example.toml`](../../config.example.toml) is parseable and
  resolves to the same Empty Configuration posture;
- the Feature Catalog is the canonical feature/default/enablement inventory;
- the verified Temporal v1.31.0 denominator contains 613 production dynamic-setting
  declarations plus 12 relevant static groups;
- production accepts typed Tokeira policy only. Raw Temporal key names remain confined
  to the conformance-only test bridge.

## Operator-critical fields

The generated reference enumerates every field. The values most likely to require
deployment-specific action are:

- DSQL endpoint, region, and runtime roles when
  `infrastructure.storage = "dsql"`;
- the public gRPC and metrics listener addresses;
- `policy.nexus_completion.system_callback_url`, which must be reachable from Nexus
  workers;
- exact JWT issuer values and grants when authorization is enabled;
- `[policy.task_queues] enable_fairness = true` when weighted User Fairness is wanted;
- emergency restrictions, which are break-glass policy and generate warnings.

## Durable live policy

Task-queue rates and fairness-weight overrides are not startup TOML. Operators author
them through `UpdateTaskQueueConfig`; records commit through a dedicated CAS repository
before success, remain isolated by task kind, survive process replacement, and hydrate
before task delivery begins.

## Remaining release work

- Keep `cargo run -p compatibility-docs -- check` in the release/CI documentation bar.
- Re-run the two-pass functional priority/fairness evidence after the final release
  candidate binary is built.
- Review the generated public prose as part of the normal release editorial pass.

The earlier field-by-field readiness inventory and configuration-policy deliberation
remain preserved in
[the owning spec record](../../.kiro/specs/configuration-policy/reference/configuration-policy-proposal.md).
