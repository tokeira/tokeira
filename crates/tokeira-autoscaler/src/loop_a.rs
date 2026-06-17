//! Loop A: Replica scaling with consecutive-sample hysteresis.
//!
//! # Why hysteresis?
//!
//! Without hysteresis, a single metric spike would trigger an immediate
//! scale-out, followed by a scale-in once the spike passes — classic flapping.
//! By requiring N consecutive samples in the same direction before acting, the
//! autoscaler filters out transient noise and only responds to sustained
//! pressure changes.
//!
//! The effective reaction time is `polling_interval × consecutive_samples`.
//! For example, with a 15s poll and 2 consecutive scale-out samples, the
//! autoscaler reacts within 30s of sustained pressure. Scale-in uses a higher
//! sample count (default 6 = 90s) because premature scale-in is more
//! disruptive than a brief over-provision.
//!
//! # Counter reset semantics
//!
//! When pressure direction changes (e.g., ScaleOut → ScaleIn), the opposite
//! counter is reset to zero. This prevents stale history from one direction
//! from carrying over and causing a delayed reaction in the new direction.
//! A Hold signal resets both counters — the system is in equilibrium and
//! historical pressure is no longer relevant.

use std::collections::BTreeMap;

use crate::{
    config::AutoscalerServiceConfig,
    reconciler::{DesiredState, ScalingAction},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServicePressure {
    ScaleOut,
    ScaleIn,
    Hold,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSignal {
    pub service: String,
    pub current_count: u32,
    pub pressure: ServicePressure,
}

/// Maintains per-service consecutive-sample counters for hysteresis.
///
/// Each service independently tracks how many consecutive polls have shown
/// the same pressure direction. This prevents one service's scaling decision
/// from being delayed by another service's counter state.
#[derive(Debug, Clone, Default)]
pub struct ReplicaScalingLoop {
    scale_out_samples: BTreeMap<String, u32>,
    scale_in_samples: BTreeMap<String, u32>,
}

impl ReplicaScalingLoop {
    pub fn apply_signals(
        &mut self,
        config: &AutoscalerServiceConfig,
        desired: &mut DesiredState,
        signals: &[ServiceSignal],
    ) -> Vec<ScalingAction> {
        for signal in signals {
            let Some(service_config) = config.service_configs.get(&signal.service) else {
                continue;
            };
            let current = desired
                .service_counts
                .get(&signal.service)
                .copied()
                .unwrap_or(signal.current_count);
            let next = match signal.pressure {
                ServicePressure::ScaleOut => {
                    let samples = increment_counter(&mut self.scale_out_samples, &signal.service);
                    self.scale_in_samples.remove(&signal.service);
                    if samples >= config.scale_out_consecutive_samples {
                        current
                            .saturating_add(service_config.step)
                            .min(service_config.max)
                    } else {
                        current
                    }
                }
                ServicePressure::ScaleIn => {
                    let samples = increment_counter(&mut self.scale_in_samples, &signal.service);
                    self.scale_out_samples.remove(&signal.service);
                    if samples >= config.scale_in_consecutive_samples {
                        current
                            .saturating_sub(service_config.step)
                            .max(service_config.min)
                    } else {
                        current
                    }
                }
                ServicePressure::Hold => {
                    self.scale_out_samples.remove(&signal.service);
                    self.scale_in_samples.remove(&signal.service);
                    current
                }
            };
            desired.service_counts.insert(signal.service.clone(), next);
        }
        Vec::new()
    }
}

fn increment_counter(counters: &mut BTreeMap<String, u32>, service: &str) -> u32 {
    let value = counters.entry(service.to_owned()).or_insert(0);
    *value = value.saturating_add(1);
    *value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_out_requires_configured_consecutive_samples() {
        let config = AutoscalerServiceConfig {
            scale_out_consecutive_samples: 2,
            ..Default::default()
        };
        let mut desired = DesiredState::default();
        let mut loop_a = ReplicaScalingLoop::default();
        let signal = ServiceSignal {
            service: "tokeira-edge-api".to_owned(),
            current_count: 2,
            pressure: ServicePressure::ScaleOut,
        };

        loop_a.apply_signals(&config, &mut desired, std::slice::from_ref(&signal));
        assert_eq!(desired.service_counts["tokeira-edge-api"], 2);

        loop_a.apply_signals(&config, &mut desired, &[signal]);
        assert_eq!(desired.service_counts["tokeira-edge-api"], 3);
    }

    #[test]
    fn hold_resets_hysteresis_counters() {
        let config = AutoscalerServiceConfig {
            scale_out_consecutive_samples: 2,
            ..Default::default()
        };
        let mut desired = DesiredState::default();
        let mut loop_a = ReplicaScalingLoop::default();
        let scale_out = ServiceSignal {
            service: "tokeira-edge-api".to_owned(),
            current_count: 2,
            pressure: ServicePressure::ScaleOut,
        };
        let hold = ServiceSignal {
            pressure: ServicePressure::Hold,
            ..scale_out.clone()
        };

        loop_a.apply_signals(&config, &mut desired, std::slice::from_ref(&scale_out));
        loop_a.apply_signals(&config, &mut desired, &[hold]);
        loop_a.apply_signals(&config, &mut desired, &[scale_out]);

        assert_eq!(desired.service_counts["tokeira-edge-api"], 2);
    }
}
