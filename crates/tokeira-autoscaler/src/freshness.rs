//! Metric freshness classification and scaling permission policy.
//!
//! # The "missing metrics must never trigger scale-in" invariant
//!
//! If the metrics pipeline is down (Mimir unavailable, scrape failures, or
//! network partitions), the autoscaler has no reliable signal about current
//! load. Scaling in during this blind period could remove capacity that is
//! actively serving traffic, causing cascading failures.
//!
//! The freshness policy enforces a conservative stance:
//! - **Missing or stale metrics → block scale-in.** The fleet stays at its
//!   current size until fresh data confirms the load has actually decreased.
//! - **Mimir completely unavailable → freeze all scaling.** Neither scale-out
//!   nor scale-in proceeds because the autoscaler cannot make informed
//!   decisions.
//! - **Overload signal present → allow scale-out even with partial staleness.**
//!   If the system is actively overloaded (e.g., admission rejection), adding
//!   capacity is safe regardless of metric freshness because the overload
//!   signal itself is authoritative.
//!
//! This asymmetry (scale-out is permissive, scale-in is conservative) reflects
//! the operational reality that under-provisioning causes immediate user pain
//! while over-provisioning only costs money.

/// Classification of a metric sample's age relative to the staleness threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricFreshness {
    /// Sample is within the staleness threshold — safe to use for decisions.
    Fresh,
    /// Sample exists but is older than the threshold — may not reflect current
    /// state.
    Stale,
    /// No sample exists at all — the metric has never been reported or the
    /// query returned empty.
    Missing,
}

/// The autoscaler's permission to proceed with a scaling action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingPermission {
    /// Proceed with the scaling action.
    Allow,
    /// Scale-out is allowed but scale-in is blocked.
    BlockScaleIn,
    /// All scaling is frozen — neither direction is safe.
    Freeze,
}

/// Aggregates freshness signals from multiple metric sources and computes
/// whether the autoscaler is allowed to scale in a given direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessTracker {
    pub mimir_available: bool,
    pub service_metrics: MetricFreshness,
    pub controller_snapshot: MetricFreshness,
    pub dsql_headroom: MetricFreshness,
    /// Whether the system is actively rejecting work due to overload.
    /// This is an authoritative signal that bypasses staleness checks for
    /// scale-out because the overload itself proves capacity is insufficient.
    pub overload_signal: bool,
}

impl FreshnessTracker {
    /// Determine whether the autoscaler may proceed with a scaling action.
    ///
    /// `scale_out` indicates the direction: `true` for adding capacity,
    /// `false` for removing it.
    pub fn scaling_permission(&self, scale_out: bool) -> ScalingPermission {
        if !self.mimir_available {
            return ScalingPermission::Freeze;
        }
        if scale_out && self.overload_signal {
            return ScalingPermission::Allow;
        }
        if !scale_out
            && (self.service_metrics != MetricFreshness::Fresh
                || self.controller_snapshot != MetricFreshness::Fresh)
        {
            return ScalingPermission::BlockScaleIn;
        }
        if scale_out && self.dsql_headroom == MetricFreshness::Missing {
            return ScalingPermission::BlockScaleIn;
        }
        ScalingPermission::Allow
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn mimir_unavailable_freezes_desired_capacity() {
        let tracker = FreshnessTracker {
            mimir_available: false,
            service_metrics: MetricFreshness::Fresh,
            controller_snapshot: MetricFreshness::Fresh,
            dsql_headroom: MetricFreshness::Fresh,
            overload_signal: true,
        };

        assert_eq!(tracker.scaling_permission(true), ScalingPermission::Freeze);
    }

    #[test]
    fn stale_metrics_block_scale_in() {
        let tracker = FreshnessTracker {
            mimir_available: true,
            service_metrics: MetricFreshness::Stale,
            controller_snapshot: MetricFreshness::Fresh,
            dsql_headroom: MetricFreshness::Fresh,
            overload_signal: false,
        };

        assert_eq!(
            tracker.scaling_permission(false),
            ScalingPermission::BlockScaleIn
        );
    }

    proptest! {
        #[test]
        fn property_mimir_unavailable_never_allows_scale_in(
            service_metrics in metric_freshness(),
            controller_snapshot in metric_freshness(),
            dsql_headroom in metric_freshness(),
            overload_signal in any::<bool>(),
        ) {
            let tracker = FreshnessTracker {
                mimir_available: false,
                service_metrics,
                controller_snapshot,
                dsql_headroom,
                overload_signal,
            };

            prop_assert_ne!(tracker.scaling_permission(false), ScalingPermission::Allow);
        }
    }

    fn metric_freshness() -> impl Strategy<Value = MetricFreshness> {
        prop_oneof![
            Just(MetricFreshness::Fresh),
            Just(MetricFreshness::Stale),
            Just(MetricFreshness::Missing),
        ]
    }
}
