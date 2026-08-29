//! Deterministic projections of Tokeira's checked compatibility catalogs.
//!
//! This crate owns exactly three generated artifacts: the complete Temporal
//! configuration denominator, Tokeira's operator-facing feature/configuration
//! reference, and the canonical safe-default TOML example. Rendering is pure;
//! filesystem mutation remains confined to the small CLI.

use std::fmt::Write;

use anyhow::{Context, Result};
use tokeira_compatibility::{
    FEATURE_MATRIX, FeatureEntry, VerifiedConfigurationLedger, checked_configuration_ledger,
};
use tokeira_config::{CONFIG_FIELD_CATALOG, ConfigFieldDocumentation, TokeiraConfig};

/// Relative path of the generated Temporal configuration denominator.
pub const TEMPORAL_CONFIGURATION_PATH: &str = "docs/conformance/v1.31.0/temporal-configuration.md";
/// Relative path of the generated Tokeira operator configuration reference.
pub const TOKEIRA_CONFIGURATION_PATH: &str = "docs/conformance/v1.31.0/tokeira-configuration.md";
/// Relative path of the canonical annotated production configuration example.
pub const CONFIG_EXAMPLE_PATH: &str = "config.example.toml";

/// One complete deterministic render.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedDocumentation {
    /// Complete Temporal v1.31.0 configuration denominator.
    pub temporal_configuration: String,
    /// Tokeira feature/configuration reference.
    pub tokeira_configuration: String,
    /// Safe-default annotated configuration example.
    pub config_example: String,
}

/// Render all owned documents from checked production metadata.
pub fn render_all() -> Result<RenderedDocumentation> {
    let ledger = checked_configuration_ledger().context("verify configuration ledger")?;
    Ok(RenderedDocumentation {
        temporal_configuration: render_temporal_configuration(&ledger),
        tokeira_configuration: render_tokeira_configuration(FEATURE_MATRIX, CONFIG_FIELD_CATALOG),
        config_example: render_config_example(CONFIG_FIELD_CATALOG)?,
    })
}

/// Render the complete source-aware Temporal configuration denominator.
#[must_use]
pub(crate) fn render_temporal_configuration(ledger: &VerifiedConfigurationLedger) -> String {
    let mut dynamic = ledger.dynamic_settings.clone();
    dynamic.sort_by(|left, right| left.0.key.cmp(&right.0.key));
    let mut static_groups = ledger.static_groups.clone();
    static_groups.sort_by(|left, right| left.group.cmp(&right.group));

    let mut output = String::new();
    output.push_str(
        "# Temporal v1.31.0 configuration surface\n\n\
> This is the complete source-aware configuration denominator for Tokeira's \
Temporal server v1.31.0 compatibility target. It records what Temporal exposes \
and Tokeira's treatment of each item; it is not a list of raw keys accepted by \
`tokeirad`.\n\n\
## Method and authority\n\n\
The checked audit reads production, non-test Go files from the local Temporal \
source at tag `v1.31.0`, parses `New*Setting` declarations with the Go AST, and \
records constructor, scope, value kind, default expression, and source anchor. \
The immutable extraction snapshot is joined one-to-one with an owner-authored \
classification ledger. Duplicate, missing, extra, non-literal, or unresolved \
entries fail verification before this document can be generated.\n\n",
    );
    writeln!(
        output,
        "- Dynamic setting declarations: **{}**.",
        dynamic.len()
    )
    .expect("String writes are infallible");
    writeln!(
        output,
        "- Relevant static configuration groups: **{}**.",
        static_groups.len()
    )
    .expect("String writes are infallible");
    output.push_str(
        "- Source evidence: `crates/tokeira-compatibility/data/temporal-v1.31.0-settings.json`.\n\
- Product decisions: `crates/tokeira-compatibility/data/temporal-v1.31.0-classification.json`.\n\n\
## Disposition summary\n\n\
Counts below include dynamic settings and the separately audited static groups.\n\n\
| Tokeira treatment | Count |\n\
|---|---:|\n",
    );
    for (disposition, count) in &ledger.disposition_counts {
        writeln!(output, "| {} | {count} |", disposition.label())
            .expect("String writes are infallible");
    }

    output.push_str(
        "\n## Static Temporal server configuration\n\n\
| Group | Tokeira treatment | Owner | Evidence |\n\
|---|---|---|---|\n",
    );
    for group in static_groups {
        writeln!(
            output,
            "| `{}` | {} — {} | `{}` | {} |",
            markdown(&group.group),
            group.classification.label(),
            markdown(&group.tokeira_treatment),
            markdown(&group.owner),
            code_list(&group.evidence),
        )
        .expect("String writes are infallible");
    }

    output.push_str(
        "\n## Dynamic settings\n\n\
| Temporal key | Scope / type | Temporal default | Tokeira treatment | Conformance override | Source |\n\
|---|---|---|---|---|---|\n",
    );
    for (declaration, classification) in dynamic {
        writeln!(
            output,
            "| `{}` | {} / `{}` | `{}` | {} — {} | {} | `{}` |",
            markdown(&declaration.key),
            declaration.scope.label(),
            markdown(&declaration.value_kind),
            markdown(&declaration.default_expression),
            classification.classification.label(),
            markdown(&classification.tokeira_treatment),
            classification.conformance_override.label(),
            markdown(&declaration.source),
        )
        .expect("String writes are infallible");
    }
    output
}

/// Render Tokeira's canonical operator-facing feature and config reference.
#[must_use]
pub(crate) fn render_tokeira_configuration(
    features: &[FeatureEntry],
    fields: &[ConfigFieldDocumentation],
) -> String {
    let mut features = features.to_vec();
    features.sort_by(|left, right| left.id.cmp(right.id));
    let mut fields = fields.to_vec();
    fields.sort_by(|left, right| left.path.cmp(right.path));

    let mut output = String::new();
    output.push_str(
        "# Tokeira configuration and feature availability\n\n\
> This is the canonical operator-facing configuration reference for the \
Temporal v1.31.0 compatibility profile. The Feature Catalog and strict typed \
configuration schema generate it; hand-maintained test outcomes do not define \
feature availability.\n\n\
## Empty Configuration guarantee\n\n\
An empty TOML document is valid. It selects Tokeira's documented safe defaults: \
Temporal priority bands remain active, User Fairness is disabled, Standalone \
Activities are disabled, authentication/authorization is a stock-compatible \
no-op until an identity source is configured, and no emergency restriction is \
active. Production accepts typed Tokeira fields only—never raw Temporal dynamic \
configuration keys.\n\n\
## Operational warnings\n\n\
- **JWT issuer routing is exact.** Each configured `policy.authorization.jwt.issuers[].issuer` \
must exactly match the signed token's `iss` value; a friendly provider name is not a substitute.\n\
- **Nexus callbacks require routability.** `policy.nexus_completion.system_callback_url` \
must be reachable from Nexus workers. The loopback default is suitable only when workers are co-located.\n\
- **Priority and User Fairness are distinct.** Omitting `[policy.task_queues]` preserves \
five priority bands and default key 3 while leaving weighted User Fairness disabled. Enable it with \
`[policy.task_queues] enable_fairness = true`.\n\
- **Conformance overrides are not production configuration.** A conformance build may \
receive selected Temporal keys from the test bridge; stock production builds expose no such input path.\n\n\
## Feature catalog\n\n\
| Feature | State | Conformance | Temporal maturity | Temporal default | Empty Configuration | Enablement | Scope / mutability |\n\
|---|---|---|---|---|---|---|---|\n",
    );
    for feature in &features {
        let scopes = feature
            .catalog
            .scopes
            .iter()
            .map(|scope| scope.label())
            .collect::<Vec<_>>()
            .join(", ");
        let enablement = match feature.catalog.enablement.reference {
            Some(reference) => format!(
                "{}: `{}`",
                feature.catalog.enablement.kind.label(),
                markdown(reference)
            ),
            None => feature.catalog.enablement.kind.label().to_owned(),
        };
        writeln!(
            output,
            "| `{}` — {} | {} | {} | {} | {} | {} | {} | {} / {} |",
            markdown(feature.id),
            markdown(feature.name),
            feature.state.label(),
            feature.catalog.conformance.label(),
            feature.catalog.temporal_maturity.label(),
            feature.catalog.temporal_default.label(),
            feature.catalog.tokeira_default.label(),
            enablement,
            scopes,
            feature.catalog.mutability.label(),
        )
        .expect("String writes are infallible");
    }

    output.push_str("\n## Feature details\n\n");
    for feature in &features {
        writeln!(
            output,
            "### {} (`{}`)\n\n{}\n\n- Guidance: {}\n- Prerequisites: {}\n- Evidence: {}\n",
            feature.name,
            feature.id,
            feature.notes,
            feature.catalog.guidance,
            if feature.catalog.prerequisites.is_empty() {
                "none".to_owned()
            } else {
                code_list(feature.catalog.prerequisites)
            },
            feature
                .evidence
                .iter()
                .map(|evidence| format!(
                    "{} `{}`",
                    evidence.kind.label(),
                    markdown(evidence.reference)
                ))
                .collect::<Vec<_>>()
                .join("; "),
        )
        .expect("String writes are infallible");
    }

    output.push_str(
        "## Production TOML fields\n\n\
All accepted production leaves are listed below. Fields are startup-static; \
changing one requires a `tokeirad` restart. Optional live task-queue policy is \
not a TOML field and is described separately.\n\n\
| Field | Class | Default | Required in optional section | Restart | Owning feature | Guidance |\n\
|---|---|---|---|---|---|---|\n",
    );
    for field in &fields {
        writeln!(
            output,
            "| `{}` | {} | `{}` | {} | {} | {} | {} |",
            markdown(field.path),
            field.class.label(),
            markdown(field.default),
            yes_no(field.required),
            yes_no(field.restart_required),
            field
                .feature_id
                .map(|feature| format!("`{}`", markdown(feature)))
                .unwrap_or_else(|| "—".to_owned()),
            markdown(field.guidance),
        )
        .expect("String writes are infallible");
    }

    output.push_str(
        "\n## Scoped Worker authorization\n\n\
`scoped-worker-authorization` is a Tokeira-native, presence-activated attenuation for \
standard Temporal SDK Workers. Tokeira does not mint, rotate, or distribute the bearer: an \
external IdP, workload-identity system, or Worker Compute provider owns that lifecycle. When \
the verified credential carries or maps to a Worker scope, ordinary Temporal roles cannot \
widen it.\n\n\
The fixed signed JWT claim is `tokeira_worker_scope`:\n\n\
```json\n\
{\n\
  \"version\": 1,\n\
  \"namespace\": \"payments\",\n\
  \"task_queues\": [\"payments-worker\"],\n\
  \"deployment_name\": \"payments\",\n\
  \"build_id\": \"2026-07-28.1\"\n\
}\n\
```\n\n\
Alternatively, map a verified JWT subject or AWS STS caller ARN in TOML:\n\n\
```toml\n\
[[policy.authorization.jwt.issuers.worker_scopes]]\n\
match_sub = \"system:serviceaccount:workers:payments-*\"\n\
namespace = \"payments\"\n\
task_queues = [\"payments-worker\"]\n\
deployment_name = \"payments\"\n\
build_id = \"2026-07-28.1\"\n\n\
[[policy.authorization.aws_iam.worker_scopes]]\n\
match_arn = \"arn:aws:sts::123456789012:assumed-role/payments-worker-*\"\n\
namespace = \"payments\"\n\
task_queues = [\"payments-worker\"]\n\
deployment_name = \"payments\"\n\
build_id = \"2026-07-28.1\"\n\
```\n\n\
Every resource comparison is exact and case-sensitive. Polls must use VERSIONED Worker \
Deployment mode with both the configured deployment name and build ID. The fixed Worker RPC \
surface is `PollWorkflowTaskQueue`, `PollActivityTaskQueue`, `PollNexusTaskQueue`, \
`RespondWorkflowTaskCompleted`, `RespondWorkflowTaskFailed`, `RespondQueryTaskCompleted`, \
`RespondActivityTaskCompleted`, `RespondActivityTaskFailed`, `RespondActivityTaskCanceled`, \
`RecordActivityTaskHeartbeat`, `RespondNexusTaskCompleted`, `RespondNexusTaskFailed`, \
`RecordWorkerHeartbeat`, `ShutdownWorker`, and queue-scoped `DescribeTaskQueue`. Universal \
`Health/Check` and `GetSystemInfo` remain available independently of Worker scope.\n\n\
Activity By-ID aliases, standalone Activities, unversioned or deprecated Worker Versioning, \
Worker inventory, visibility/history reads, Workflow starts, and every other namespace-wide \
API are denied for a scoped identity. Sticky Workflow polls authorize the request's stable \
`normal_name`; sticky aliases are never configured as resources.\n\n\
The Go SDK can refresh an externally issued bearer through its standard credentials callback; \
return the token without a `Bearer ` prefix because the SDK adds it:\n\n\
```go\n\
c, err := client.Dial(client.Options{\n\
    HostPort:  \"tokeira.example:7233\",\n\
    Namespace: \"payments\",\n\
    Credentials: client.NewAPIKeyDynamicCredentials(\n\
        func(ctx context.Context) (string, error) {\n\
            return credentialSource.Token(ctx)\n\
        },\n\
    ),\n\
})\n\
```\n\n\
## Durable live task-queue policy\n\n\
`UpdateTaskQueueConfig` authors durable policy independently for each \
`(namespace, task queue, task kind)`. Queue rate limits, the default per-fairness-key \
rate, and fairness-weight overrides commit through compare-and-swap storage before \
the API returns success. Every server hydrates its disposable cache before admitting \
traffic and refreshes remote revisions internally. This public API policy therefore \
survives process replacement without becoming workflow history or kernel state.\n\n\
## What Tokeira does not configure\n\n\
Temporal's file-backed dynamic-config loader, separate frontend/history/matching/worker \
service topology, plugin persistence selection, multi-cluster redirection, and excluded \
feature controls are not Tokeira production configuration surfaces. See \
[`temporal-configuration.md`](./temporal-configuration.md) for the complete denominator \
and treatment of all 613 source declarations.\n",
    );
    output
}

/// Render the canonical annotated configuration without enabling optional
/// identity sources or emergency behavior.
pub(crate) fn render_config_example(fields: &[ConfigFieldDocumentation]) -> Result<String> {
    let mut fields = fields.to_vec();
    fields.sort_by(|left, right| left.path.cmp(right.path));

    let mut output = String::new();
    output.push_str(
        "# Canonical Tokeira production configuration example.\n\
# An empty document is valid; the active values below spell out the same safe defaults.\n\
# Priority remains enabled. User Fairness is disabled unless enable_fairness is true.\n\
# Worker Compute is disabled; enabling it may cause configured providers to create billable capacity.\n\
# The Nexus callback URL must be reachable from Nexus workers; loopback assumes co-location.\n\
# JWT issuer routing requires issuer to exactly equal the token's signed iss value.\n\
# Optional JWT/AWS identity sources are shown commented and are not enabled by this file.\n\n\
# Complete accepted-field inventory. These generated markers occur once per strict schema leaf.\n",
    );
    for field in &fields {
        writeln!(
            output,
            "# @field {} | {} | default={} | {}",
            field.path,
            field.class.label(),
            field.default,
            field.guidance,
        )
        .expect("String writes are infallible");
    }
    output.push('\n');
    output.push_str(
        &TokeiraConfig::default()
            .to_toml()
            .context("serialize empty-configuration defaults")?,
    );
    output.push_str(
        "\n# Optional authorization example (leave commented to preserve no-op parity):\n\
# [policy.authorization]\n\
# principal_attribution = true\n\
# expose_authorizer_errors = false\n\
#\n\
# [[policy.authorization.jwt.issuers]]\n\
# name = \"production-idp\"\n\
# issuer = \"https://issuer.example/tenant\" # exact signed iss value\n\
# jwks_uri = \"https://issuer.example/tenant/keys\"\n\
# audience = \"tokeira\"\n\
# refresh_interval = \"1m\"\n\
# permissions_claim = \"permissions\"\n\
#\n\
# [[policy.authorization.jwt.issuers.grants]]\n\
# match_sub = \"system:serviceaccount:workers:*\"\n\
# grant = [\"default:worker\"]\n\
#\n\
# # Presence activates exact scoped-Worker attenuation. The IdP may instead\n\
# # sign the fixed version-1 `tokeira_worker_scope` JWT claim with equal fields.\n\
# [[policy.authorization.jwt.issuers.worker_scopes]]\n\
# match_sub = \"system:serviceaccount:workers:payments-*\"\n\
# namespace = \"payments\"\n\
# task_queues = [\"payments-worker\"]\n\
# deployment_name = \"payments\"\n\
# build_id = \"2026-07-28.1\"\n\
#\n\
# [policy.authorization.aws_iam]\n\
# [[policy.authorization.aws_iam.grants]]\n\
# match_arn = \"arn:aws:sts::123456789012:assumed-role/tokeira-worker-*\"\n\
# grant = [\"default:worker\"]\n\
#\n\
# [[policy.authorization.aws_iam.worker_scopes]]\n\
# match_arn = \"arn:aws:sts::123456789012:assumed-role/payments-worker-*\"\n\
# namespace = \"payments\"\n\
# task_queues = [\"payments-worker\"]\n\
# deployment_name = \"payments\"\n\
# build_id = \"2026-07-28.1\"\n",
    );
    Ok(output)
}

fn markdown(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn code_list(values: &[impl AsRef<str>]) -> String {
    values
        .iter()
        .map(|value| format!("`{}`", markdown(value.as_ref())))
        .collect::<Vec<_>>()
        .join(", ")
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn checked_render_contains_complete_denominators_and_safe_example() {
        let rendered = render_all().expect("checked catalogs render");
        assert!(
            rendered
                .temporal_configuration
                .contains("Dynamic setting declarations: **613**")
        );
        assert!(
            rendered
                .tokeira_configuration
                .contains("`policy.task_queues.enable_fairness`")
        );
        assert!(
            rendered
                .tokeira_configuration
                .contains("`scoped-worker-authorization`")
        );
        assert!(
            rendered
                .tokeira_configuration
                .contains("`tokeira_worker_scope`")
        );
        assert!(
            rendered
                .tokeira_configuration
                .contains("Activity By-ID aliases, standalone Activities")
        );
        assert!(
            rendered
                .config_example
                .contains("# [[policy.authorization.jwt.issuers.worker_scopes]]")
        );
        assert!(
            rendered
                .config_example
                .contains("# [[policy.authorization.aws_iam.worker_scopes]]")
        );
        assert!(
            !rendered
                .config_example
                .contains("\n[[policy.authorization.jwt.issuers]]")
        );
        assert!(
            !rendered
                .config_example
                .contains("\n[[policy.authorization.jwt.issuers.worker_scopes]]")
        );
        let parsed: TokeiraConfig =
            toml::from_str(&rendered.config_example).expect("example parses strictly");
        parsed.validate().expect("example defaults validate");
        assert_eq!(parsed, TokeiraConfig::default());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: configuration-policy, Property 7: deterministic documentation projection
        #[test]
        fn deterministic_documentation_projection(
            feature_rotation in any::<usize>(),
            field_rotation in any::<usize>(),
        ) {
            let mut features = FEATURE_MATRIX.to_vec();
            let feature_len = features.len();
            features.rotate_left(feature_rotation % feature_len);
            let mut fields = CONFIG_FIELD_CATALOG.to_vec();
            let field_len = fields.len();
            fields.rotate_left(field_rotation % field_len);

            prop_assert_eq!(
                render_tokeira_configuration(&features, &fields),
                render_tokeira_configuration(FEATURE_MATRIX, CONFIG_FIELD_CATALOG)
            );
        }

        // Feature: configuration-policy, Property 13: annotated configuration example coverage
        #[test]
        fn annotated_configuration_example_coverage(
            cluster_name in "[a-z][a-z0-9-]{0,15}",
            starts_per_second in 1_u64..100_000,
        ) {
            let example = render_config_example(CONFIG_FIELD_CATALOG).unwrap();
            let substituted = example
                .replace(
                    "cluster_name = \"tokeira-local\"",
                    &format!("cluster_name = {cluster_name:?}"),
                )
                .replace(
                    "target_workflow_starts_per_second = 1000",
                    &format!(
                        "target_workflow_starts_per_second = {starts_per_second}"
                    ),
                );
            let parsed: TokeiraConfig = toml::from_str(&substituted).unwrap();
            parsed.validate().unwrap();

            let markers = substituted
                .lines()
                .filter_map(|line| line.strip_prefix("# @field "))
                .map(|line| line.split(" | ").next().unwrap())
                .collect::<Vec<_>>();
            let unique = markers.iter().copied().collect::<BTreeSet<_>>();
            prop_assert_eq!(markers.len(), CONFIG_FIELD_CATALOG.len());
            prop_assert_eq!(unique.len(), CONFIG_FIELD_CATALOG.len());
            prop_assert!(CONFIG_FIELD_CATALOG
                .iter()
                .all(|field| unique.contains(field.path)));
        }
    }
}
