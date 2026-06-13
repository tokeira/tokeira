use crate::{Feature, FeatureEntry, FeatureState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    Proceed,
    Disabled {
        feature_id: &'static str,
        reason: DisabledReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisabledReason {
    /// An experimental feature whose gate is off and whose disabled contract is
    /// `FAILED_PRECONDITION` (the common case — e.g. batch operations, schedules).
    ExperimentalDisabled,
    /// An experimental feature whose gate is off and whose disabled contract is
    /// `UNIMPLEMENTED`. This mirrors a targeted-release feature that answers
    /// `UNIMPLEMENTED` while gated rather than `FAILED_PRECONDITION` — standalone
    /// activities are the canonical case (`chasm/lib/activity/frontend.go:36 @
    /// v1.31.0`: `serviceerror.NewUnimplemented("Standalone activity is disabled")`).
    ExperimentalUnimplemented,
    Stubbed,
    Unsupported,
}

pub trait DynamicConfigReader {
    fn bool_for_namespace(&self, key: &str, namespace: Option<&str>) -> bool;
}

pub trait DispatchMetrics {
    fn increment_dispatch(&self, feature_id: &str, state: FeatureState);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StaticDynamicConfig {
    experimental_enabled: bool,
}

impl StaticDynamicConfig {
    pub const fn enabled() -> Self {
        Self {
            experimental_enabled: true,
        }
    }

    pub const fn disabled() -> Self {
        Self {
            experimental_enabled: false,
        }
    }
}

impl DynamicConfigReader for StaticDynamicConfig {
    fn bool_for_namespace(&self, _key: &str, _namespace: Option<&str>) -> bool {
        self.experimental_enabled
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopDispatchMetrics;

impl DispatchMetrics for NoopDispatchMetrics {
    fn increment_dispatch(&self, _feature_id: &str, _state: FeatureState) {}
}

pub fn dispatch_rpc<F: Feature>(
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
    metrics: &dyn DispatchMetrics,
) -> DispatchOutcome {
    let entry = F::ENTRY;
    metrics.increment_dispatch(entry.id, entry.state);

    dispatch_outcome_for(entry, dynamic_config, namespace)
}

/// The single state→outcome decision behind both the compile-time-keyed
/// [`dispatch_rpc`] and the coverage [`expected_outcome`](crate::coverage::expected::expected_outcome)
/// projection.
///
/// `dispatch_rpc` is generic over [`Feature`] for compile-time keying, which makes it
/// unusable from a runtime context that only holds a `&FeatureEntry`. Factoring the actual
/// policy here — operating on a borrowed entry rather than a type parameter — lets the
/// runtime expected-outcome projection reuse the *exact same* decision instead of mirroring
/// the `match` independently. This is the single-source-of-truth guarantee the design calls
/// for: "one policy, two views" holds by construction, not by manual synchronisation.
///
/// `metrics` is deliberately not taken here: incrementing a dispatch counter is a side effect
/// of an actual dispatch, not of asking what the expected outcome would be, so it stays in
/// [`dispatch_rpc`].
pub fn dispatch_outcome_for(
    entry: &FeatureEntry,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> DispatchOutcome {
    match entry.state {
        FeatureState::Implemented | FeatureState::Partial => DispatchOutcome::Proceed,
        FeatureState::Experimental => match entry.dynamic_config_key {
            Some(key) if dynamic_config.bool_for_namespace(key, namespace) => {
                DispatchOutcome::Proceed
            }
            Some(_) | None => DispatchOutcome::Disabled {
                feature_id: entry.id,
                reason: experimental_disabled_reason(entry.id),
            },
        },
        FeatureState::Stubbed => DispatchOutcome::Disabled {
            feature_id: entry.id,
            reason: DisabledReason::Stubbed,
        },
        FeatureState::Unsupported => DispatchOutcome::Disabled {
            feature_id: entry.id,
            reason: DisabledReason::Unsupported,
        },
    }
}

/// The disabled contract an `Experimental` feature presents when its gate is off.
///
/// Temporal does not gate every experimental feature the same way: most answer
/// `FAILED_PRECONDITION` while disabled, but a few answer `UNIMPLEMENTED`. The
/// disabled contract is therefore a per-feature property, resolved here by id
/// (mirroring the localized `secondary_capability_fields` pattern) rather than
/// carried as a field on every `FeatureEntry` literal. The default is the common
/// `FAILED_PRECONDITION` contract; only features that mirror a targeted-release
/// `UNIMPLEMENTED`-while-gated behaviour are listed.
///
/// - `activity-executions`: standalone activities are gated per-namespace by
///   `activity.enableStandalone` and answer `UNIMPLEMENTED` when off
///   (`chasm/lib/activity/frontend.go:36 @ v1.31.0`).
fn experimental_disabled_reason(feature_id: &str) -> DisabledReason {
    match feature_id {
        "activity-executions" => DisabledReason::ExperimentalUnimplemented,
        _ => DisabledReason::ExperimentalDisabled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Feature, FeatureEntry, lookup_feature_const};

    #[derive(Debug, Clone, Copy)]
    struct WorkflowExecution;

    impl Feature for WorkflowExecution {
        const ID: &'static str = "workflow-start";
        const ENTRY: &'static FeatureEntry = lookup_feature_const(Self::ID);
    }

    #[test]
    fn partial_feature_dispatches() {
        let outcome = dispatch_rpc::<WorkflowExecution>(
            &StaticDynamicConfig::disabled(),
            Some("default"),
            &NoopDispatchMetrics,
        );
        assert_eq!(outcome, DispatchOutcome::Proceed);
    }
}
