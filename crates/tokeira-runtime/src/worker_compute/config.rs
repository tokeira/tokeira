//! Pure ComputeConfig validation, eligibility, and configuration identity.
//!
//! The Worker Deployment registry invokes this module before its CAS commit. It never
//! resolves Nexus endpoints or performs provider I/O: resources may be created in
//! either order, and delivery resolves the current endpoint record for every attempt.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use thiserror::Error;
use tokeira_storage::{
    ComputeConfig, ComputeConfigScalingGroup, ComputeProvider, ComputeScaler,
    DeploymentTaskQueueType,
};
use tokeira_types::{ConfigurationFingerprint, Payload, ScalingGroupId, WorkerComputeTaskType};

const FINGERPRINT_DOMAIN: &[u8] = b"tokeira.worker-compute.config.v1\0";
const BUILT_IN_PROVIDER_TYPES: &[&str] = &[
    "aws-lambda",
    "aws-ecs",
    "subprocess",
    "k8s",
    "gcp-cloud-run",
    "test-invoke",
    "test-worker-set",
];
const NO_SYNC_SCALER: &str = "no-sync";
const RATE_BASED_SCALER: &str = "rate-based";
const SCALE_UP_COOLOFF_MS: &str = "scale_up_cooloff_ms";
const SCALE_UP_BACKLOG_THRESHOLD: &str = "scale_up_backlog_threshold";
const MAX_WORKER_LIFETIME_MS: &str = "max_worker_lifetime_ms";
const SCALE_UP_DISPATCH_RATE_EPSILON: &str = "scale_up_dispatch_rate_epsilon";
const METRICS_POLL_INTERVAL_MS: &str = "metrics_poll_interval_ms";
const VALID_NO_SYNC_KEYS: &[&str] = &[
    SCALE_UP_COOLOFF_MS,
    SCALE_UP_BACKLOG_THRESHOLD,
    MAX_WORKER_LIFETIME_MS,
    SCALE_UP_DISPATCH_RATE_EPSILON,
    METRICS_POLL_INTERVAL_MS,
];

/// Decoded `no-sync` scaler policy at the pinned WCI defaults.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NoSyncConfig {
    /// Minimum shared interval between scale-up decisions; zero disables cooloff.
    pub scale_up_cooloff_ms: i64,
    /// Metrics scale-up threshold, compared with strict greater-than.
    pub scale_up_backlog_threshold: i64,
    /// Backlog-present worker refresh interval; zero disables refresh.
    pub max_worker_lifetime_ms: i64,
    /// Dispatch-rate delta at or below which a metrics decision is suppressed.
    pub scale_up_dispatch_rate_epsilon: f64,
    /// Period between metrics evaluations.
    pub metrics_poll_interval_ms: i64,
}

impl Default for NoSyncConfig {
    fn default() -> Self {
        Self {
            scale_up_cooloff_ms: 100,
            scale_up_backlog_threshold: 0,
            max_worker_lifetime_ms: 600_000,
            scale_up_dispatch_rate_epsilon: 0.0,
            metrics_poll_interval_ms: 60_000,
        }
    }
}

/// Remote provider data forwarded byte-for-byte through Nexus.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteNexusProvider {
    /// Implementation-specific provider discriminator.
    pub provider_type: String,
    /// Opaque provider configuration.
    pub details: Option<Payload>,
    /// Nexus endpoint name resolved at delivery time.
    pub nexus_endpoint: String,
}

/// One scaling group eligible for active controller decisions.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectiveScalingGroup {
    /// Stable caller-supplied group identity.
    pub id: ScalingGroupId,
    /// Task types after explicit assignments and catch-all resolution.
    pub task_types: BTreeSet<WorkerComputeTaskType>,
    /// Remote provider invoked through Nexus.
    pub provider: RemoteNexusProvider,
    /// Decoded active scaler configuration.
    pub scaler: NoSyncConfig,
    /// Original scaler details retained without canonicalization.
    pub scaler_details: Option<Payload>,
    /// Digest fencing activation and provider actions.
    pub fingerprint: ConfigurationFingerprint,
}

/// Why a valid stored scaling group cannot create controller actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsupportedScalingGroupReason {
    /// The provider is implemented directly rather than through Nexus.
    DirectProvider,
    /// The scaler is accepted for round-trip compatibility but is not implemented.
    RateBasedScaler,
}

/// Valid stored group retained in controller health without active decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsupportedScalingGroup {
    /// Stable caller-supplied group identity.
    pub id: ScalingGroupId,
    /// Task types after explicit assignments and catch-all resolution.
    pub task_types: BTreeSet<WorkerComputeTaskType>,
    /// Stable reason exposed through bounded controller health.
    pub reason: UnsupportedScalingGroupReason,
}

/// Eligibility result for one validated scaling group.
#[derive(Clone, Debug, PartialEq)]
pub enum ValidatedScalingGroup {
    /// Remote Nexus provider using the active `no-sync` scaler.
    Eligible(EffectiveScalingGroup),
    /// Valid configuration outside the active first slice.
    Unsupported(UnsupportedScalingGroup),
}

impl ValidatedScalingGroup {
    /// Effective task types assigned to this group.
    #[must_use]
    pub fn task_types(&self) -> &BTreeSet<WorkerComputeTaskType> {
        match self {
            Self::Eligible(group) => &group.task_types,
            Self::Unsupported(group) => &group.task_types,
        }
    }
}

/// Deterministic normalized view of one stored ComputeConfig.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ValidatedComputeConfig {
    /// Groups in stable caller-supplied identity order.
    pub groups: BTreeMap<ScalingGroupId, ValidatedScalingGroup>,
}

/// Pure ComputeConfig validation failure mapped to `INVALID_ARGUMENT` by the edge.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WorkerComputeConfigError {
    /// One named group violates the compute contract.
    #[error("entry {group}: {message}")]
    InvalidGroup {
        /// Scaling-group map key.
        group: String,
        /// Deterministic operator-facing explanation.
        message: String,
    },
}

/// Validate and normalize a stored ComputeConfig without changing its payload bytes.
pub fn validate_compute_config(
    config: &ComputeConfig,
) -> Result<ValidatedComputeConfig, WorkerComputeConfigError> {
    let mut claimed_types = BTreeSet::new();
    let mut catch_all = None;

    for (group_id, group) in &config.scaling_groups {
        if group_id.is_empty() {
            return invalid(group_id, "compute config scaling group key cannot be empty");
        }
        if group.task_queue_types.is_empty() && catch_all.replace(group_id.as_str()).is_some() {
            return invalid(
                group_id,
                "only one scaling group can have no task types defined",
            );
        }
        for task_type in &group.task_queue_types {
            let Some(task_type) = task_type_from_storage(*task_type) else {
                return invalid(group_id, "task type undefined not allowed in compute spec");
            };
            if !claimed_types.insert(task_type) {
                return invalid(
                    group_id,
                    format!(
                        "task type {} appears in more than one entry",
                        task_type_name(task_type)
                    ),
                );
            }
        }
    }

    let all_types = BTreeSet::from([
        WorkerComputeTaskType::Workflow,
        WorkerComputeTaskType::Activity,
        WorkerComputeTaskType::Nexus,
    ]);
    let catch_all_types = all_types
        .difference(&claimed_types)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut validated = ValidatedComputeConfig::default();

    for (group_id, group) in &config.scaling_groups {
        let task_types = if group.task_queue_types.is_empty() {
            catch_all_types.clone()
        } else {
            group
                .task_queue_types
                .iter()
                .filter_map(|task_type| task_type_from_storage(*task_type))
                .collect()
        };
        let classification = classify_group(group_id, group, task_types)?;
        validated
            .groups
            .insert(ScalingGroupId(group_id.clone()), classification);
    }

    Ok(validated)
}

fn classify_group(
    group_id: &str,
    group: &ComputeConfigScalingGroup,
    task_types: BTreeSet<WorkerComputeTaskType>,
) -> Result<ValidatedScalingGroup, WorkerComputeConfigError> {
    let provider = group
        .provider
        .as_ref()
        .ok_or_else(|| invalid_error(group_id, "invalid compute provider type ''"))?;
    let is_remote = !provider.nexus_endpoint.is_empty();

    if provider.provider_type.is_empty() {
        return invalid(group_id, "invalid compute provider type ''");
    }
    if !is_remote && !BUILT_IN_PROVIDER_TYPES.contains(&provider.provider_type.as_str()) {
        return invalid(
            group_id,
            format!("invalid compute provider type '{}'", provider.provider_type),
        );
    }
    if !is_remote {
        validate_built_in_provider_details(group_id, provider)?;
    }

    let scaler = match group.scaler.as_ref() {
        Some(scaler) => scaler,
        None if is_remote => {
            return invalid(group_id, "remote Nexus provider requires a compute scaler");
        }
        None => {
            return Ok(ValidatedScalingGroup::Unsupported(
                UnsupportedScalingGroup {
                    id: ScalingGroupId(group_id.to_owned()),
                    task_types,
                    reason: UnsupportedScalingGroupReason::DirectProvider,
                },
            ));
        }
    };

    match scaler.scaler_type.as_str() {
        NO_SYNC_SCALER => {
            let decoded = decode_no_sync_config(group_id, scaler.details.as_ref())?;
            if !is_remote {
                return Ok(ValidatedScalingGroup::Unsupported(
                    UnsupportedScalingGroup {
                        id: ScalingGroupId(group_id.to_owned()),
                        task_types,
                        reason: UnsupportedScalingGroupReason::DirectProvider,
                    },
                ));
            }
            let fingerprint = configuration_fingerprint(group_id, &task_types, provider, scaler);
            Ok(ValidatedScalingGroup::Eligible(EffectiveScalingGroup {
                id: ScalingGroupId(group_id.to_owned()),
                task_types,
                provider: RemoteNexusProvider {
                    provider_type: provider.provider_type.clone(),
                    details: provider.details.clone(),
                    nexus_endpoint: provider.nexus_endpoint.clone(),
                },
                scaler: decoded,
                scaler_details: scaler.details.clone(),
                fingerprint,
            }))
        }
        RATE_BASED_SCALER => Ok(ValidatedScalingGroup::Unsupported(
            UnsupportedScalingGroup {
                id: ScalingGroupId(group_id.to_owned()),
                task_types,
                reason: UnsupportedScalingGroupReason::RateBasedScaler,
            },
        )),
        other => invalid(
            group_id,
            format!("invalid scaling algorithm type '{other}'"),
        ),
    }
}

fn validate_built_in_provider_details(
    group_id: &str,
    provider: &ComputeProvider,
) -> Result<(), WorkerComputeConfigError> {
    if matches!(
        provider.provider_type.as_str(),
        "test-invoke" | "test-worker-set"
    ) && provider
        .details
        .as_ref()
        .is_some_and(payload_has_illegal_test_field)
    {
        // The pinned WCI test providers deliberately reject this key. Remote
        // providers remain opaque and therefore never take this branch.
        return invalid(group_id, "illegal_field found in config");
    }
    Ok(())
}

fn payload_has_illegal_test_field(details: &Payload) -> bool {
    serde_json::from_slice::<Value>(&details.data)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .is_some_and(|object| object.contains_key("illegal_field"))
}

fn decode_no_sync_config(
    group_id: &str,
    details: Option<&Payload>,
) -> Result<NoSyncConfig, WorkerComputeConfigError> {
    let Some(details) = details else {
        return Ok(NoSyncConfig::default());
    };
    if details.metadata.get("encoding").map(String::as_str) != Some("json/plain")
        || !details.external_payloads.is_empty()
    {
        return invalid(
            group_id,
            "no-sync scaler details must use inline json/plain encoding",
        );
    }
    let value = serde_json::from_slice::<Value>(&details.data).map_err(|error| {
        invalid_error(
            group_id,
            format!("no-sync scaler details must be a JSON object: {error}"),
        )
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid_error(group_id, "no-sync scaler details must be a JSON object"))?;

    let mut keys = object.keys().collect::<Vec<_>>();
    keys.sort();
    if let Some(key) = keys
        .into_iter()
        .find(|key| !VALID_NO_SYNC_KEYS.contains(&key.as_str()))
    {
        return invalid(
            group_id,
            format!("unknown config key {key:?} for no-sync scaler"),
        );
    }

    let defaults = NoSyncConfig::default();
    let decoded = NoSyncConfig {
        scale_up_cooloff_ms: int_field(
            group_id,
            object.get(SCALE_UP_COOLOFF_MS),
            SCALE_UP_COOLOFF_MS,
            defaults.scale_up_cooloff_ms,
            0,
        )?,
        scale_up_backlog_threshold: int_field(
            group_id,
            object.get(SCALE_UP_BACKLOG_THRESHOLD),
            SCALE_UP_BACKLOG_THRESHOLD,
            defaults.scale_up_backlog_threshold,
            0,
        )?,
        max_worker_lifetime_ms: int_field(
            group_id,
            object.get(MAX_WORKER_LIFETIME_MS),
            MAX_WORKER_LIFETIME_MS,
            defaults.max_worker_lifetime_ms,
            0,
        )?,
        scale_up_dispatch_rate_epsilon: float_field(
            group_id,
            object.get(SCALE_UP_DISPATCH_RATE_EPSILON),
            SCALE_UP_DISPATCH_RATE_EPSILON,
            defaults.scale_up_dispatch_rate_epsilon,
            0.0,
        )?,
        metrics_poll_interval_ms: int_field(
            group_id,
            object.get(METRICS_POLL_INTERVAL_MS),
            METRICS_POLL_INTERVAL_MS,
            defaults.metrics_poll_interval_ms,
            10_000,
        )?,
    };
    if decoded.scale_up_cooloff_ms > 0
        && decoded.metrics_poll_interval_ms < decoded.scale_up_cooloff_ms
    {
        return invalid(
            group_id,
            format!(
                "{METRICS_POLL_INTERVAL_MS} ({}) must be >= {SCALE_UP_COOLOFF_MS} ({})",
                decoded.metrics_poll_interval_ms, decoded.scale_up_cooloff_ms
            ),
        );
    }
    Ok(decoded)
}

fn int_field(
    group_id: &str,
    value: Option<&Value>,
    field: &str,
    default: i64,
    minimum: i64,
) -> Result<i64, WorkerComputeConfigError> {
    // `map_access.go @ auto-scaled-workers edd947d743d2` treats a map value
    // decoded as nil exactly like an absent key.
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(default);
    };
    let parsed = match value {
        Value::Number(number) => {
            let number = number.as_f64().ok_or_else(|| {
                invalid_error(
                    group_id,
                    format!("{field} must be an integer and at least {minimum}"),
                )
            })?;
            if !number.is_finite() || number < minimum as f64 || number > i64::MAX as f64 {
                return invalid(
                    group_id,
                    format!("{field} must be an integer and at least {minimum}"),
                );
            }
            number as i64
        }
        Value::String(number) => number.parse::<i64>().map_err(|_| {
            invalid_error(
                group_id,
                format!("{field} must be an integer and at least {minimum}"),
            )
        })?,
        _ => {
            return invalid(
                group_id,
                format!("{field} must be an integer and at least {minimum}"),
            );
        }
    };
    if parsed < minimum {
        return invalid(
            group_id,
            format!("{field} must be an integer and at least {minimum}"),
        );
    }
    Ok(parsed)
}

fn float_field(
    group_id: &str,
    value: Option<&Value>,
    field: &str,
    default: f64,
    minimum: f64,
) -> Result<f64, WorkerComputeConfigError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(default);
    };
    let parsed = match value {
        Value::Number(number) => number.as_f64(),
        Value::String(number) => number.parse::<f64>().ok(),
        _ => None,
    }
    .filter(|number| number.is_finite())
    .ok_or_else(|| {
        invalid_error(
            group_id,
            format!("{field} must be a number and at least {minimum}"),
        )
    })?;
    if parsed < minimum {
        return invalid(
            group_id,
            format!("{field} must be a number and at least {minimum}"),
        );
    }
    Ok(parsed)
}

fn configuration_fingerprint(
    group_id: &str,
    task_types: &BTreeSet<WorkerComputeTaskType>,
    provider: &ComputeProvider,
    scaler: &ComputeScaler,
) -> ConfigurationFingerprint {
    let mut canonical = FINGERPRINT_DOMAIN.to_vec();
    append_bytes(&mut canonical, group_id.as_bytes());
    append_u64(&mut canonical, task_types.len() as u64);
    for task_type in task_types {
        append_bytes(&mut canonical, &[task_type_discriminant(*task_type)]);
    }
    append_bytes(&mut canonical, provider.provider_type.as_bytes());
    append_payload(&mut canonical, provider.details.as_ref());
    append_bytes(&mut canonical, provider.nexus_endpoint.as_bytes());
    append_bytes(&mut canonical, scaler.scaler_type.as_bytes());
    append_payload(&mut canonical, scaler.details.as_ref());
    ConfigurationFingerprint::from_canonical_bytes(&canonical)
}

fn append_payload(target: &mut Vec<u8>, payload: Option<&Payload>) {
    match payload {
        None => target.push(0),
        Some(payload) => {
            target.push(1);
            append_u64(target, payload.metadata.len() as u64);
            for (key, value) in &payload.metadata {
                append_bytes(target, key.as_bytes());
                append_bytes(target, value.as_bytes());
            }
            append_bytes(target, &payload.data);
        }
    }
}

fn append_bytes(target: &mut Vec<u8>, value: &[u8]) {
    append_u64(target, value.len() as u64);
    target.extend_from_slice(value);
}

fn append_u64(target: &mut Vec<u8>, value: u64) {
    target.extend_from_slice(&value.to_le_bytes());
}

const fn task_type_discriminant(task_type: WorkerComputeTaskType) -> u8 {
    match task_type {
        WorkerComputeTaskType::Workflow => 1,
        WorkerComputeTaskType::Activity => 2,
        WorkerComputeTaskType::Nexus => 3,
    }
}

const fn task_type_from_storage(
    task_type: DeploymentTaskQueueType,
) -> Option<WorkerComputeTaskType> {
    match task_type {
        DeploymentTaskQueueType::Workflow => Some(WorkerComputeTaskType::Workflow),
        DeploymentTaskQueueType::Activity => Some(WorkerComputeTaskType::Activity),
        DeploymentTaskQueueType::Nexus => Some(WorkerComputeTaskType::Nexus),
        DeploymentTaskQueueType::Unspecified => None,
    }
}

const fn task_type_name(task_type: WorkerComputeTaskType) -> &'static str {
    match task_type {
        WorkerComputeTaskType::Workflow => "Workflow",
        WorkerComputeTaskType::Activity => "Activity",
        WorkerComputeTaskType::Nexus => "Nexus",
    }
}

fn invalid<T>(group: &str, message: impl Into<String>) -> Result<T, WorkerComputeConfigError> {
    Err(invalid_error(group, message))
}

fn invalid_error(group: &str, message: impl Into<String>) -> WorkerComputeConfigError {
    WorkerComputeConfigError::InvalidGroup {
        group: group.to_owned(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use tokeira_types::ExternalPayloadDetail;

    use super::*;

    fn json_payload(value: serde_json::Value) -> Payload {
        Payload {
            data: serde_json::to_vec(&value).expect("JSON value serializes"),
            metadata: BTreeMap::from([("encoding".to_owned(), "json/plain".to_owned())]),
            external_payloads: Vec::new(),
        }
    }

    fn group(
        provider_type: &str,
        endpoint: &str,
        scaler_type: Option<&str>,
        details: Option<Payload>,
        task_queue_types: Vec<DeploymentTaskQueueType>,
    ) -> ComputeConfigScalingGroup {
        ComputeConfigScalingGroup {
            task_queue_types,
            provider: Some(ComputeProvider {
                provider_type: provider_type.to_owned(),
                details: None,
                nexus_endpoint: endpoint.to_owned(),
            }),
            scaler: scaler_type.map(|scaler_type| ComputeScaler {
                scaler_type: scaler_type.to_owned(),
                details,
            }),
        }
    }

    #[test]
    fn no_sync_defaults_and_pinned_conversion_rules_are_preserved() {
        let details = json_payload(serde_json::json!({
            "scale_up_cooloff_ms": 101.9,
            "scale_up_backlog_threshold": "2",
            "max_worker_lifetime_ms": null,
            "scale_up_dispatch_rate_epsilon": "0.25",
            "metrics_poll_interval_ms": "10000"
        }));
        let config = ComputeConfig {
            scaling_groups: BTreeMap::from([(
                "remote".to_owned(),
                group(
                    "implementation-specific",
                    "provider-endpoint",
                    Some("no-sync"),
                    Some(details.clone()),
                    Vec::new(),
                ),
            )]),
        };

        let validated = validate_compute_config(&config).expect("valid no-sync config");
        let ValidatedScalingGroup::Eligible(group) = validated
            .groups
            .get(&ScalingGroupId("remote".to_owned()))
            .expect("group")
        else {
            panic!("remote no-sync group must be eligible");
        };
        assert_eq!(
            group.scaler,
            NoSyncConfig {
                scale_up_cooloff_ms: 101,
                scale_up_backlog_threshold: 2,
                max_worker_lifetime_ms: 600_000,
                scale_up_dispatch_rate_epsilon: 0.25,
                metrics_poll_interval_ms: 10_000,
            }
        );
        assert_eq!(group.scaler_details.as_ref(), Some(&details));
    }

    #[test]
    fn remote_provider_requires_scaler_but_not_a_built_in_type() {
        let config = ComputeConfig {
            scaling_groups: BTreeMap::from([(
                "remote".to_owned(),
                group("yadori", "provider-endpoint", None, None, Vec::new()),
            )]),
        };
        assert_eq!(
            validate_compute_config(&config),
            Err(WorkerComputeConfigError::InvalidGroup {
                group: "remote".to_owned(),
                message: "remote Nexus provider requires a compute scaler".to_owned(),
            })
        );
    }

    #[test]
    fn catch_all_receives_only_unclaimed_task_types() {
        let config = ComputeConfig {
            scaling_groups: BTreeMap::from([
                (
                    "explicit".to_owned(),
                    group(
                        "yadori",
                        "provider-endpoint",
                        Some("no-sync"),
                        None,
                        vec![DeploymentTaskQueueType::Workflow],
                    ),
                ),
                (
                    "remainder".to_owned(),
                    group(
                        "yadori",
                        "provider-endpoint",
                        Some("rate-based"),
                        None,
                        Vec::new(),
                    ),
                ),
            ]),
        };
        let validated = validate_compute_config(&config).expect("valid partition");
        assert_eq!(
            validated
                .groups
                .get(&ScalingGroupId("explicit".to_owned()))
                .expect("explicit")
                .task_types(),
            &BTreeSet::from([WorkerComputeTaskType::Workflow])
        );
        assert_eq!(
            validated
                .groups
                .get(&ScalingGroupId("remainder".to_owned()))
                .expect("remainder")
                .task_types(),
            &BTreeSet::from([
                WorkerComputeTaskType::Activity,
                WorkerComputeTaskType::Nexus
            ])
        );
    }

    #[test]
    fn scaler_payload_must_be_inline_json_plain() {
        let config = ComputeConfig {
            scaling_groups: BTreeMap::from([(
                "remote".to_owned(),
                group(
                    "yadori",
                    "provider-endpoint",
                    Some("no-sync"),
                    Some(Payload {
                        data: b"{}".to_vec(),
                        metadata: BTreeMap::from([(
                            "encoding".to_owned(),
                            "binary/plain".to_owned(),
                        )]),
                        external_payloads: vec![ExternalPayloadDetail { size_bytes: 2 }],
                    }),
                    Vec::new(),
                ),
            )]),
        };
        assert!(validate_compute_config(&config).is_err());
    }

    #[test]
    fn configuration_fingerprint_has_a_stable_vector() {
        let mut provider_details = Payload::new(b"provider-details".to_vec());
        provider_details
            .metadata
            .insert("encoding".to_owned(), "binary/plain".to_owned());
        let scaler_details = json_payload(serde_json::json!({
            "metrics_poll_interval_ms": 10000,
            "scale_up_cooloff_ms": 100
        }));
        let mut scaling_group = group(
            "yadori",
            "provider-endpoint",
            Some("no-sync"),
            Some(scaler_details),
            vec![
                DeploymentTaskQueueType::Workflow,
                DeploymentTaskQueueType::Nexus,
            ],
        );
        scaling_group.provider.as_mut().expect("provider").details = Some(provider_details);
        let config = ComputeConfig {
            scaling_groups: BTreeMap::from([("remote".to_owned(), scaling_group)]),
        };

        let validated = validate_compute_config(&config).expect("valid config");
        let ValidatedScalingGroup::Eligible(group) = validated
            .groups
            .get(&ScalingGroupId("remote".to_owned()))
            .expect("group")
        else {
            panic!("remote no-sync group must be eligible");
        };
        assert_eq!(
            group.fingerprint,
            ConfigurationFingerprint::from_bytes([
                0x7a, 0xb9, 0x4b, 0x6b, 0xfc, 0x92, 0xe7, 0x73, 0x59, 0x5d, 0x4e, 0xd3, 0x58, 0x9a,
                0x61, 0x4c, 0xc8, 0xd4, 0x3e, 0x0a, 0x85, 0xde, 0x41, 0x64, 0x28, 0x00, 0x9b, 0xbc,
                0x22, 0x08, 0x34, 0x18,
            ])
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 2: eligibility is deterministic and mutation-atomic
        #[test]
        fn eligibility_is_deterministic(
            explicit_types in proptest::collection::btree_set(0u8..3, 0..=3),
            remote in any::<bool>(),
            rate_based in any::<bool>(),
        ) {
            let task_queue_types = explicit_types
                .into_iter()
                .map(|value| match value {
                    0 => DeploymentTaskQueueType::Workflow,
                    1 => DeploymentTaskQueueType::Activity,
                    _ => DeploymentTaskQueueType::Nexus,
                })
                .collect();
            let config = ComputeConfig {
                scaling_groups: BTreeMap::from([(
                    "group".to_owned(),
                    group(
                        if remote { "implementation-specific" } else { "aws-lambda" },
                        if remote { "endpoint" } else { "" },
                        Some(if rate_based { "rate-based" } else { "no-sync" }),
                        None,
                        task_queue_types,
                    ),
                )]),
            };

            prop_assert_eq!(
                validate_compute_config(&config),
                validate_compute_config(&config)
            );
        }

        // Feature: worker-compute-controller, Property 3: no-sync decoding is total and preserving
        #[test]
        fn no_sync_decoding_is_total_and_preserving(
            field_index in 0u8..7,
            value_kind in 0u8..6,
            number in -100_000i64..1_000_000,
            supported_encoding in any::<bool>(),
            object_root in any::<bool>(),
        ) {
            let field = match field_index {
                0 => SCALE_UP_COOLOFF_MS,
                1 => SCALE_UP_BACKLOG_THRESHOLD,
                2 => MAX_WORKER_LIFETIME_MS,
                3 => SCALE_UP_DISPATCH_RATE_EPSILON,
                4 => METRICS_POLL_INTERVAL_MS,
                _ => "unknown_key",
            };
            let value = match value_kind {
                0 => Value::Number(number.into()),
                1 => Value::String(number.to_string()),
                2 => Value::Bool(number % 2 == 0),
                3 => Value::Null,
                4 => Value::String("not-a-number".to_owned()),
                _ => Value::Number(
                    serde_json::Number::from_f64(number as f64 + 0.75)
                        .expect("finite generated number"),
                ),
            };
            let json = if object_root {
                Value::Object(serde_json::Map::from_iter([(field.to_owned(), value)]))
            } else {
                value
            };
            let mut details = json_payload(json);
            if !supported_encoding {
                details
                    .metadata
                    .insert("encoding".to_owned(), "binary/plain".to_owned());
            }
            let config = ComputeConfig {
                scaling_groups: BTreeMap::from([(
                    "remote".to_owned(),
                    group(
                        "implementation-specific",
                        "endpoint",
                        Some("no-sync"),
                        Some(details.clone()),
                        Vec::new(),
                    ),
                )]),
            };

            let first = validate_compute_config(&config);
            let second = validate_compute_config(&config);
            prop_assert_eq!(&first, &second);
            if let Ok(validated) = first {
                let ValidatedScalingGroup::Eligible(group) = validated
                    .groups
                    .get(&ScalingGroupId("remote".to_owned()))
                    .expect("group")
                else {
                    return Err(TestCaseError::fail("remote no-sync must be eligible"));
                };
                prop_assert_eq!(group.scaler_details.as_ref(), Some(&details));
            }
        }
    }
}
