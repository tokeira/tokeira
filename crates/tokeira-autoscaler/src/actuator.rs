//! Platform-agnostic actuator trait for autoscaler mutations.
//!
//! # Why a trait?
//!
//! The autoscaler's decision logic (loops A/B/C, reconciler, envelope) is
//! platform-independent — it reasons about desired counts, capacities, and
//! drain phases without knowing whether the underlying platform is ECS, EKS,
//! or a local dev environment. The `Actuator` trait is the seam that separates
//! "what to do" from "how to do it on this platform."
//!
//! This separation provides two benefits:
//! 1. **Platform agnosticism** — the same autoscaler logic works across ECS,
//!    Kubernetes, or any future platform by swapping the actuator impl.
//! 2. **Testability** — unit tests can inject a mock actuator to verify
//!    reconciliation logic without real API calls or network I/O.
//!
//! # Contract
//!
//! Implementations MUST handle transient failures (throttling, network errors)
//! internally via retry/backoff. The autoscaler treats errors returned from
//! these methods as non-retryable for the current reconciliation cycle — it
//! will re-read state and re-plan on the next iteration rather than retrying
//! the same action immediately.

use anyhow::Result;
use async_trait::async_trait;

/// Snapshot of a service's current replica state.
///
/// Platform-agnostic: represents the desired and running counts regardless
/// of whether the underlying platform is ECS, Kubernetes, or something else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceState {
    pub desired_count: u32,
    pub running_count: u32,
}

/// Snapshot of an auto-scaling group's current capacity state.
///
/// Platform-agnostic: maps to any compute fleet that has a desired capacity
/// bounded by min/max (ECS capacity providers, Kubernetes node pools, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsgState {
    pub desired_capacity: u32,
    pub min_size: u32,
    pub max_size: u32,
}

/// The actuator trait abstracts platform-specific mutations that the
/// autoscaler's reconciler needs to converge desired state with actual state.
///
/// Each method corresponds to a single atomic operation. The reconciler calls
/// these after diffing desired vs. current state — it never batches multiple
/// mutations into a single call because partial failure semantics differ per
/// platform.
#[async_trait]
pub trait Actuator: Send + Sync + std::fmt::Debug {
    /// Set the desired replica count for a service.
    ///
    /// Returns `Ok(true)` if the count was actually changed, `Ok(false)` if
    /// the service was already at the requested count (no-op). This distinction
    /// lets the caller avoid logging spurious "updated" events.
    async fn update_service_desired_count(
        &self,
        cluster: &str,
        service: &str,
        desired: u32,
    ) -> Result<bool>;

    /// Set the desired capacity for a compute fleet (ASG, node pool, etc.).
    ///
    /// Returns `Ok(true)` if capacity was changed, `Ok(false)` if already at
    /// the requested value.
    async fn set_asg_desired_capacity(&self, asg_name: &str, desired: u32) -> Result<bool>;

    /// Mark a container instance as draining so the platform stops scheduling
    /// new work onto it.
    ///
    /// On ECS this sets the container instance status to DRAINING. On
    /// Kubernetes this would cordon the node. The instance continues running
    /// existing tasks until they complete or are migrated.
    async fn drain_container_instance(
        &self,
        cluster: &str,
        container_instance_arn: &str,
    ) -> Result<()>;

    /// Remove scale-in protection from an instance so the platform's fleet
    /// manager can terminate it.
    ///
    /// Called after the instance has been fully drained and its workload
    /// migrated. Without this step, protected instances would block ASG
    /// scale-in indefinitely.
    async fn clear_instance_protection(&self, asg_name: &str, instance_id: &str) -> Result<()>;

    /// Terminate a specific instance and decrement the fleet's desired capacity
    /// atomically.
    ///
    /// This is the final step in the retirement state machine. The atomic
    /// decrement prevents the fleet from launching a replacement — the
    /// autoscaler has already decided this capacity is no longer needed.
    async fn terminate_instance_with_decrement(&self, instance_id: &str) -> Result<()>;

    /// Read the current state of a service (desired + running counts).
    ///
    /// Used by the reconciler to detect drift between the autoscaler's intent
    /// and the platform's actual state.
    async fn describe_service(&self, cluster: &str, service: &str) -> Result<ServiceState>;

    /// Read the current state of a compute fleet (desired capacity, min, max).
    ///
    /// The envelope module uses `max_size` to cap scale-out decisions. The
    /// reconciler uses `desired_capacity` to detect drift.
    async fn describe_asg(&self, asg_name: &str) -> Result<AsgState>;

    /// Resolve a compute instance ID to the platform's container-instance
    /// identifier.
    ///
    /// On ECS, EC2 instance IDs and container-instance ARNs are different
    /// identifiers for the same host. The drain operation needs the
    /// container-instance ARN, but the ASG reports EC2 instance IDs. This
    /// method bridges that gap.
    async fn resolve_container_instance_for_ec2(
        &self,
        cluster: &str,
        ec2_instance_id: &str,
    ) -> Result<String>;
}

// The platform-specific actuator implementation should handle throttle retry
// internally. See `platforms/ecs/` for the ECS implementation which uses
// exponential backoff on AWS API throttling errors.
