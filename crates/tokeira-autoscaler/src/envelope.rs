//! Scaling envelope: the hard ceiling on runtime fleet size.
//!
//! # Why does DSQL constrain the scaling envelope?
//!
//! Each runtime host reserves a fixed number of DSQL connections at startup
//! (configured via `per_runtime_reserved_connections`). Additionally, each
//! host's startup burst consumes connection-rate budget. Aurora DSQL enforces
//! cluster-wide limits on both total connections and connection creation rate.
//!
//! If the autoscaler scales beyond what the DSQL budget can support, new hosts
//! will fail to establish their connection pools, causing cascading failures
//! across the fleet. The scaling envelope computes the tightest bound across:
//! 1. The operator-configured maximum (`configured_max_runtime_hosts`)
//! 2. The connection budget divided by per-host reservation
//! 3. The connection-rate budget divided by per-host startup rate
//!
//! This ensures the autoscaler never scales into a resource wall, regardless
//! of how much CPU/memory headroom exists.

/// Defines the hard upper bound on how many runtime hosts the autoscaler is
/// allowed to provision, considering both operator config and DSQL resource
/// constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalingEnvelope {
    pub configured_max_runtime_hosts: u32,
    pub dsql_connection_budget: u32,
    pub dsql_connection_rate_budget: u32,
    pub per_runtime_reserved_connections: u32,
    pub per_runtime_startup_connection_rate: u32,
}

impl ScalingEnvelope {
    /// Compute the effective maximum, taking the minimum across all
    /// constraints. This is the true ceiling the autoscaler must respect.
    pub fn effective_max_runtime_hosts(&self) -> u32 {
        let connection_bound = divide_or_unbounded(
            self.dsql_connection_budget,
            self.per_runtime_reserved_connections,
        );
        let rate_bound = divide_or_unbounded(
            self.dsql_connection_rate_budget,
            self.per_runtime_startup_connection_rate,
        );
        self.configured_max_runtime_hosts
            .min(connection_bound)
            .min(rate_bound)
    }

    /// Check whether a target host count is within the envelope.
    pub fn allows_scale_to(&self, target_hosts: u32) -> bool {
        target_hosts <= self.effective_max_runtime_hosts()
    }
}

fn divide_or_unbounded(budget: u32, per_unit: u32) -> u32 {
    budget.checked_div(per_unit).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn effective_max_uses_the_tightest_budget() {
        let envelope = ScalingEnvelope {
            configured_max_runtime_hosts: 100,
            dsql_connection_budget: 320,
            dsql_connection_rate_budget: 30,
            per_runtime_reserved_connections: 64,
            per_runtime_startup_connection_rate: 10,
        };

        assert_eq!(envelope.effective_max_runtime_hosts(), 3);
        assert!(envelope.allows_scale_to(3));
        assert!(!envelope.allows_scale_to(4));
    }

    #[test]
    fn zero_budget_blocks_runtime_hosts_when_per_host_reservation_is_nonzero() {
        let envelope = ScalingEnvelope {
            configured_max_runtime_hosts: 10,
            dsql_connection_budget: 0,
            dsql_connection_rate_budget: 100,
            per_runtime_reserved_connections: 64,
            per_runtime_startup_connection_rate: 10,
        };

        assert_eq!(envelope.effective_max_runtime_hosts(), 0);
        assert!(!envelope.allows_scale_to(1));
    }

    proptest! {
        #[test]
        fn property_effective_max_decreases_as_per_runtime_reserved_increases(
            configured_max in 1u32..10_000,
            budget in 1u32..100_000,
            rate_budget in 1u32..100_000,
            reserved_a in 1u32..10_000,
            delta in 0u32..10_000,
            per_runtime_rate in 1u32..10_000,
        ) {
            let reserved_b = reserved_a.saturating_add(delta);
            let envelope_a = ScalingEnvelope {
                configured_max_runtime_hosts: configured_max,
                dsql_connection_budget: budget,
                dsql_connection_rate_budget: rate_budget,
                per_runtime_reserved_connections: reserved_a,
                per_runtime_startup_connection_rate: per_runtime_rate,
            };
            let envelope_b = ScalingEnvelope {
                per_runtime_reserved_connections: reserved_b,
                ..envelope_a
            };

            prop_assert!(envelope_a.effective_max_runtime_hosts() <= configured_max);
            prop_assert!(envelope_b.effective_max_runtime_hosts() <= envelope_a.effective_max_runtime_hosts());
        }

        #[test]
        fn property_allows_scale_to_matches_effective_max(
            configured_max in 0u32..10_000,
            budget in 0u32..100_000,
            rate_budget in 0u32..100_000,
            reserved in 0u32..10_000,
            per_runtime_rate in 0u32..10_000,
            target in 0u32..10_000,
        ) {
            let envelope = ScalingEnvelope {
                configured_max_runtime_hosts: configured_max,
                dsql_connection_budget: budget,
                dsql_connection_rate_budget: rate_budget,
                per_runtime_reserved_connections: reserved,
                per_runtime_startup_connection_rate: per_runtime_rate,
            };

            prop_assert_eq!(
                envelope.allows_scale_to(target),
                target <= envelope.effective_max_runtime_hosts()
            );
        }
    }
}
