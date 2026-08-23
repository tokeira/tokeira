//! Metric manifest validation and cardinality governance.
//!
//! Manifests are a contract between code, dashboards, alerting, and the
//! autoscaler. They are not an exporter registry. Their job is to keep emitted
//! metric names stable, catch incompatible duplicate descriptors, and prevent
//! accidental high-cardinality labels from entering production telemetry.

use std::{collections::HashMap, fmt};

use thiserror::Error;
use tokeira_types::{NamingError, validate_metric_name};

pub use tokeira_types::MetricType;

/// Semantic unit declared for dashboards and validation.
///
/// Units are intentionally coarse. The manifest should describe enough
/// semantics for dashboards and validators without coupling crates to one
/// backend's unit vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricUnit {
    Count,
    Seconds,
    Bytes,
    Ratio,
    Info,
    Other(&'static str),
}

/// Cardinality contract for one metric label.
///
/// Existing production labels are classified rather than blindly rejected. A
/// label may be configuration-bounded, enum-bounded, or explicitly accepted with
/// a documented budget. Truly request-scoped identifiers remain forbidden.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LabelCardinality {
    /// Values come from a finite enum-like set.
    BoundedEnum,
    /// Values are generated from a bounded numeric domain such as lane IDs.
    BoundedNumericRange,
    /// Values are operator/application config, such as namespaces or queues.
    ConfigurationBounded,
    /// Values are high cardinality but intentionally accepted with a budget.
    UnboundedAccepted,
    /// Values must never be emitted as labels.
    UnboundedForbidden,
}

/// One allowed metric label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelDescriptor {
    /// Prometheus label key.
    pub name: &'static str,
    /// Cardinality class used by manifest validation and review.
    pub cardinality: LabelCardinality,
    /// Finite accepted values for enum labels. Empty means the validator uses
    /// the cardinality description rather than checking exact values.
    pub allowed_values: &'static [&'static str],
    /// Operational budget for non-enum labels.
    pub max_cardinality_hint: Option<usize>,
    /// Human-facing rationale for why this label is safe.
    pub description: &'static str,
}

pub type MetricLabel = LabelDescriptor;

pub const NAMESPACE_LABEL: LabelDescriptor = LabelDescriptor {
    name: "namespace",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(256),
    description: "operator-defined namespace; bounded by deployment configuration",
};

pub const TASK_QUEUE_LABEL: LabelDescriptor = LabelDescriptor {
    name: "task_queue",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(1024),
    description: "operator-defined task queue; bounded by application configuration",
};

pub const WORKER_INSTANCE_KEY_LABEL: LabelDescriptor = LabelDescriptor {
    name: "worker_instance_key",
    cardinality: LabelCardinality::UnboundedAccepted,
    allowed_values: &[],
    max_cardinality_hint: Some(1_000_000),
    description: "worker heartbeat key; accepted because the heartbeat store enforces a 1M entry cap",
};

pub const PARTITION_ID_LABEL: LabelDescriptor = LabelDescriptor {
    name: "partition_id",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(4096),
    description: "projection partition; bounded by configured projection partition count",
};

pub const SHARD_ID_LABEL: LabelDescriptor = LabelDescriptor {
    name: "shard_id",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(4096),
    description: "runtime shard; bounded by configured shard count",
};

pub const LANE_ID_LABEL: LabelDescriptor = LabelDescriptor {
    name: "lane_id",
    cardinality: LabelCardinality::ConfigurationBounded,
    allowed_values: &[],
    max_cardinality_hint: Some(1024),
    description: "runtime lane; bounded by configured lane count",
};

/// Label names that must never appear on metrics.
///
/// Stable execution identifiers belong on spans/events. Content, credentials,
/// and tokens belong nowhere in Tokeira telemetry by default.
pub const FORBIDDEN_METRIC_LABEL_NAMES: &[&str] = &[
    "workflow_id",
    "run_id",
    "request_id",
    "trace_id",
    "span_id",
    "activity_id",
    "activity_type",
    "prompt",
    "tool_input",
    "tool_output",
    "payload",
    "credential",
    "credentials",
    "authorization",
    "token",
    "auth_token",
    "client_token",
    "creation_client_token",
    "password",
    "connection_string",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "raw_sql",
    "raw_error",
    "node_endpoint",
    "task_arn",
];

/// One metric descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricDescriptor {
    /// Existing production names are authoritative. Rename only with an explicit
    /// migration that updates dashboards, alerts, and autoscaler queries.
    pub name: &'static str,
    /// Metric kind expected by the `metrics` recorder.
    pub metric_type: MetricType,
    /// Semantic unit used by dashboard validation.
    pub unit: MetricUnit,
    /// Operator-facing meaning of the time series.
    pub description: &'static str,
    /// Complete label set intentionally emitted with the metric.
    pub labels: &'static [LabelDescriptor],
}

/// Per-crate metric manifest.
#[derive(Clone, Debug)]
pub struct MetricManifest {
    /// Owning crate name used in validation diagnostics.
    pub crate_name: &'static str,
    /// Metrics owned by that crate.
    pub metrics: &'static [MetricDescriptor],
}

/// Manifest validation error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("metric `{metric}` in `{crate_name}` has invalid name: {source}")]
    InvalidMetricName {
        crate_name: &'static str,
        metric: &'static str,
        source: NamingError,
    },
    #[error("metric `{metric}` has duplicate incompatible descriptor")]
    DuplicateMetric { metric: &'static str },
    #[error("label `{label}` on metric `{metric}` must be snake_case")]
    InvalidLabelName {
        metric: &'static str,
        label: &'static str,
    },
    #[error("metric `{metric}` uses forbidden unbounded label `{label}`")]
    ForbiddenLabel {
        metric: &'static str,
        label: &'static str,
    },
    #[error("ratio metric `{metric}` must end with `_ratio`")]
    InvalidRatioSuffix { metric: &'static str },
    #[error("build info metric `{metric}` must be a gauge with info unit")]
    InvalidBuildInfoDescriptor { metric: &'static str },
    #[error("label `{label}` on metric `{metric}` is unbounded-accepted without a budget hint")]
    MissingCardinalityBudget {
        metric: &'static str,
        label: &'static str,
    },
}

/// Validate one crate's manifest in isolation.
pub fn validate_manifest(manifest: &MetricManifest) -> Result<(), ManifestError> {
    for metric in manifest.metrics {
        validate_metric_name(metric.name, metric.metric_type).map_err(|source| {
            ManifestError::InvalidMetricName {
                crate_name: manifest.crate_name,
                metric: metric.name,
                source,
            }
        })?;
        validate_metric_unit(metric)?;
        for label in metric.labels {
            validate_label(metric.name, label)?;
        }
    }
    Ok(())
}

/// Validate a set of manifests as one process surface.
///
/// Duplicate descriptors are allowed only when they are exactly identical. That
/// permits shared constants and compatibility shims without allowing two crates
/// to disagree about a production metric's label set or type.
pub fn validate_manifests(manifests: &[&MetricManifest]) -> Result<(), ManifestError> {
    let mut seen = HashMap::<&'static str, &MetricDescriptor>::new();
    for manifest in manifests {
        validate_manifest(manifest)?;
        for metric in manifest.metrics {
            if let Some(previous) = seen.insert(metric.name, metric)
                && previous != metric
            {
                return Err(ManifestError::DuplicateMetric {
                    metric: metric.name,
                });
            }
        }
    }
    Ok(())
}

/// Return metric names and types for one manifest.
pub fn manifest_metric_names(manifest: &MetricManifest) -> Vec<(&'static str, MetricType)> {
    manifest
        .metrics
        .iter()
        .map(|metric| (metric.name, metric.metric_type))
        .collect()
}

/// Return metric names and types across a process manifest set.
pub fn all_metric_names(manifests: &[&MetricManifest]) -> Vec<(&'static str, MetricType)> {
    manifests
        .iter()
        .flat_map(|manifest| manifest.metrics.iter())
        .map(|metric| (metric.name, metric.metric_type))
        .collect()
}

fn validate_metric_unit(metric: &MetricDescriptor) -> Result<(), ManifestError> {
    if metric.unit == MetricUnit::Ratio && !metric.name.ends_with("_ratio") {
        return Err(ManifestError::InvalidRatioSuffix {
            metric: metric.name,
        });
    }
    if metric.name.ends_with("_build_info")
        && !(metric.metric_type == MetricType::Gauge && metric.unit == MetricUnit::Info)
    {
        return Err(ManifestError::InvalidBuildInfoDescriptor {
            metric: metric.name,
        });
    }
    Ok(())
}

fn validate_label(metric: &'static str, label: &LabelDescriptor) -> Result<(), ManifestError> {
    if !is_snake_case(label.name) {
        return Err(ManifestError::InvalidLabelName {
            metric,
            label: label.name,
        });
    }
    if label.cardinality == LabelCardinality::UnboundedForbidden
        || is_forbidden_metric_label(label.name)
    {
        return Err(ManifestError::ForbiddenLabel {
            metric,
            label: label.name,
        });
    }
    if label.cardinality == LabelCardinality::UnboundedAccepted
        && label.max_cardinality_hint.is_none()
    {
        return Err(ManifestError::MissingCardinalityBudget {
            metric,
            label: label.name,
        });
    }
    Ok(())
}

fn is_snake_case(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

/// Return whether a metric label would violate cardinality or content policy.
pub fn is_forbidden_metric_label(value: &str) -> bool {
    FORBIDDEN_METRIC_LABEL_NAMES.contains(&value)
}

impl fmt::Display for MetricUnit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count => formatter.write_str("count"),
            Self::Seconds => formatter.write_str("seconds"),
            Self::Bytes => formatter.write_str("bytes"),
            Self::Ratio => formatter.write_str("ratio"),
            Self::Info => formatter.write_str("info"),
            Self::Other(value) => formatter.write_str(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const OUTCOME_LABEL: LabelDescriptor = LabelDescriptor {
        name: "outcome",
        cardinality: LabelCardinality::BoundedEnum,
        allowed_values: &["success", "error"],
        max_cardinality_hint: Some(2),
        description: "bounded operation outcome",
    };

    const VALID_METRIC: MetricDescriptor = MetricDescriptor {
        name: "tokeira_test_component_operation_total",
        metric_type: MetricType::Counter,
        unit: MetricUnit::Count,
        description: "test counter",
        labels: &[OUTCOME_LABEL],
    };

    const VALID_MANIFEST: MetricManifest = MetricManifest {
        crate_name: "test",
        metrics: &[VALID_METRIC],
    };

    #[test]
    fn accepts_valid_manifest() {
        validate_manifest(&VALID_MANIFEST).unwrap();
    }

    #[test]
    fn rejects_forbidden_unbounded_label_names() {
        const BAD_LABEL: LabelDescriptor = LabelDescriptor {
            name: "run_id",
            cardinality: LabelCardinality::ConfigurationBounded,
            allowed_values: &[],
            max_cardinality_hint: None,
            description: "forbidden high-cardinality label",
        };
        const BAD_METRIC: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_operation_total",
            metric_type: MetricType::Counter,
            unit: MetricUnit::Count,
            description: "test counter",
            labels: &[BAD_LABEL],
        };
        const BAD_MANIFEST: MetricManifest = MetricManifest {
            crate_name: "test",
            metrics: &[BAD_METRIC],
        };

        assert!(matches!(
            validate_manifest(&BAD_MANIFEST),
            Err(ManifestError::ForbiddenLabel {
                metric: "tokeira_test_component_operation_total",
                label: "run_id"
            })
        ));
    }

    #[test]
    fn classifies_every_forbidden_high_cardinality_or_sensitive_label() {
        for &name in FORBIDDEN_METRIC_LABEL_NAMES {
            assert!(is_forbidden_metric_label(name), "{name}");
        }
    }

    #[test]
    fn rejects_unbounded_accepted_labels_without_budget() {
        const BAD_LABEL: LabelDescriptor = LabelDescriptor {
            name: "worker_instance_key",
            cardinality: LabelCardinality::UnboundedAccepted,
            allowed_values: &[],
            max_cardinality_hint: None,
            description: "accepted only with a documented cap",
        };
        const BAD_METRIC: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_operation_total",
            metric_type: MetricType::Counter,
            unit: MetricUnit::Count,
            description: "test counter",
            labels: &[BAD_LABEL],
        };
        const BAD_MANIFEST: MetricManifest = MetricManifest {
            crate_name: "test",
            metrics: &[BAD_METRIC],
        };

        assert!(matches!(
            validate_manifest(&BAD_MANIFEST),
            Err(ManifestError::MissingCardinalityBudget {
                metric: "tokeira_test_component_operation_total",
                label: "worker_instance_key"
            })
        ));
    }

    #[test]
    fn rejects_invalid_counter_and_duration_suffixes() {
        const BAD_COUNTER: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_operation_count",
            metric_type: MetricType::Counter,
            unit: MetricUnit::Count,
            description: "bad counter",
            labels: &[],
        };
        const BAD_DURATION: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_operation_duration",
            metric_type: MetricType::DurationHistogram,
            unit: MetricUnit::Seconds,
            description: "bad duration",
            labels: &[],
        };

        assert!(matches!(
            validate_manifest(&MetricManifest {
                crate_name: "test",
                metrics: &[BAD_COUNTER],
            }),
            Err(ManifestError::InvalidMetricName { .. })
        ));
        assert!(matches!(
            validate_manifest(&MetricManifest {
                crate_name: "test",
                metrics: &[BAD_DURATION],
            }),
            Err(ManifestError::InvalidMetricName { .. })
        ));
    }

    #[test]
    fn rejects_ratio_gauges_without_ratio_suffix() {
        const BAD_RATIO: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_utilization_value",
            metric_type: MetricType::Gauge,
            unit: MetricUnit::Ratio,
            description: "bad ratio",
            labels: &[],
        };

        assert!(matches!(
            validate_manifest(&MetricManifest {
                crate_name: "test",
                metrics: &[BAD_RATIO],
            }),
            Err(ManifestError::InvalidRatioSuffix {
                metric: "tokeira_test_component_utilization_value"
            })
        ));
    }

    #[test]
    fn rejects_build_info_unless_gauge_with_info_unit() {
        const BAD_BUILD_INFO: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_build_info",
            metric_type: MetricType::Gauge,
            unit: MetricUnit::Count,
            description: "bad build info",
            labels: &[],
        };

        assert!(matches!(
            validate_manifest(&MetricManifest {
                crate_name: "test",
                metrics: &[BAD_BUILD_INFO],
            }),
            Err(ManifestError::InvalidBuildInfoDescriptor {
                metric: "tokeira_test_component_build_info"
            })
        ));
    }

    #[test]
    fn accepts_existing_metric_label_classifications() {
        const METRIC: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_existing_labels_total",
            metric_type: MetricType::Counter,
            unit: MetricUnit::Count,
            description: "test counter for existing label classifications",
            labels: &[
                NAMESPACE_LABEL,
                TASK_QUEUE_LABEL,
                WORKER_INSTANCE_KEY_LABEL,
                PARTITION_ID_LABEL,
                SHARD_ID_LABEL,
                LANE_ID_LABEL,
            ],
        };
        const MANIFEST: MetricManifest = MetricManifest {
            crate_name: "test",
            metrics: &[METRIC],
        };

        validate_manifest(&MANIFEST).unwrap();
    }

    #[test]
    fn documents_worker_instance_key_cardinality_budget() {
        assert_eq!(
            WORKER_INSTANCE_KEY_LABEL.cardinality,
            LabelCardinality::UnboundedAccepted
        );
        assert_eq!(
            WORKER_INSTANCE_KEY_LABEL.max_cardinality_hint,
            Some(1_000_000)
        );
        assert!(
            WORKER_INSTANCE_KEY_LABEL
                .description
                .contains("heartbeat store")
        );
    }

    #[test]
    fn rejects_duplicate_incompatible_descriptors() {
        const DIFFERENT_LABEL: LabelDescriptor = LabelDescriptor {
            name: "status",
            cardinality: LabelCardinality::BoundedEnum,
            allowed_values: &["ok"],
            max_cardinality_hint: Some(1),
            description: "different label",
        };
        const DIFFERENT_METRIC: MetricDescriptor = MetricDescriptor {
            name: "tokeira_test_component_operation_total",
            metric_type: MetricType::Counter,
            unit: MetricUnit::Count,
            description: "test counter",
            labels: &[DIFFERENT_LABEL],
        };
        const OTHER_MANIFEST: MetricManifest = MetricManifest {
            crate_name: "other",
            metrics: &[DIFFERENT_METRIC],
        };

        assert!(matches!(
            validate_manifests(&[&VALID_MANIFEST, &OTHER_MANIFEST]),
            Err(ManifestError::DuplicateMetric {
                metric: "tokeira_test_component_operation_total"
            })
        ));
    }

    #[test]
    fn duplicate_identical_descriptors_are_allowed() {
        validate_manifests(&[&VALID_MANIFEST, &VALID_MANIFEST]).unwrap();
    }

    #[test]
    fn exposes_all_metric_names_for_legacy_compatibility() {
        assert_eq!(
            all_metric_names(&[&VALID_MANIFEST]),
            vec![(
                "tokeira_test_component_operation_total",
                MetricType::Counter
            )]
        );
    }

    proptest! {
        #[test]
        fn property_label_names_must_be_snake_case(label in "[A-Za-z0-9_]{1,16}") {
            let descriptor = LabelDescriptor {
                name: Box::leak(label.clone().into_boxed_str()),
                cardinality: LabelCardinality::ConfigurationBounded,
                allowed_values: &[],
                max_cardinality_hint: Some(128),
                description: "generated label",
            };
            let result = validate_label("tokeira_test_component_operation_total", &descriptor);
            let expected_ok = label
                .bytes()
                .all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit());
            prop_assert_eq!(result.is_ok(), expected_ok);
        }

        #[test]
        fn property_metric_name_validation_matches_shared_validator(
            suffix in prop_oneof![Just("total"), Just("seconds"), Just("bytes")],
        ) {
            let metric_type = match suffix {
                "total" => MetricType::Counter,
                "seconds" => MetricType::DurationHistogram,
                "bytes" => MetricType::SizeHistogram,
                _ => unreachable!(),
            };
            let name = format!("tokeira_test_component_operation_{suffix}");
            let leaked_name = Box::leak(name.into_boxed_str());
            let metric = MetricDescriptor {
                name: leaked_name,
                metric_type,
                unit: MetricUnit::Other("generated"),
                description: "generated metric",
                labels: &[],
            };
            let manifest = MetricManifest {
                crate_name: "generated",
                metrics: Box::leak(vec![metric].into_boxed_slice()),
            };
            prop_assert!(validate_manifest(&manifest).is_ok());
        }
    }
}
