//! Provider-neutral identities shared by Worker Compute Controller components.
//!
//! The controller is a runtime/storage concern, but its task-type and fingerprint
//! values cross runtime, storage, diagnostics, and provider-encoding boundaries.
//! Keeping those values here avoids making any of those planes depend on another
//! plane's implementation types.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{BuildId, DeploymentId, NamespaceId, TaskQueueName};

/// One logical controller instance per Worker Deployment Version.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ControllerInstanceKey {
    /// Namespace owning the Worker Deployment.
    pub namespace_id: NamespaceId,
    /// Worker Deployment name.
    pub deployment_name: DeploymentId,
    /// Exact Version Build ID.
    pub build_id: BuildId,
}

impl PartialOrd for ControllerInstanceKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ControllerInstanceKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.namespace_id
            .0
            .as_bytes()
            .cmp(other.namespace_id.0.as_bytes())
            .then_with(|| self.deployment_name.0.cmp(&other.deployment_name.0))
            .then_with(|| self.build_id.0.cmp(&other.build_id.0))
    }
}

/// Caller-supplied identity of one ComputeConfig scaling group.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScalingGroupId(pub String);

/// Task family governed by one worker-compute scaling group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WorkerComputeTaskType {
    /// Workflow tasks.
    Workflow,
    /// Activity tasks.
    Activity,
    /// Nexus tasks.
    Nexus,
}

/// Exact-version queue identity used by periodic controller metrics.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerComputeQueueKey {
    /// Namespace containing the queue.
    pub namespace_id: NamespaceId,
    /// Exact Worker Deployment name.
    pub deployment_name: DeploymentId,
    /// Exact Worker Deployment Build ID.
    pub build_id: BuildId,
    /// Task family sampled independently.
    pub task_type: WorkerComputeTaskType,
    /// Logical task-queue family name.
    pub task_queue: TaskQueueName,
}

impl PartialOrd for WorkerComputeQueueKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkerComputeQueueKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.namespace_id
            .0
            .as_bytes()
            .cmp(other.namespace_id.0.as_bytes())
            .then_with(|| self.deployment_name.0.cmp(&other.deployment_name.0))
            .then_with(|| self.build_id.0.cmp(&other.build_id.0))
            .then_with(|| self.task_type.cmp(&other.task_type))
            .then_with(|| self.task_queue.0.cmp(&other.task_queue.0))
    }
}

/// One task-queue family advised to a remote compute provider.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerComputeTaskQueueBinding {
    /// Exact logical task-queue family name.
    pub name: TaskQueueName,
    /// Poll API family.
    pub task_type: WorkerComputeTaskType,
}

impl PartialOrd for WorkerComputeTaskQueueBinding {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkerComputeTaskQueueBinding {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.task_type
            .cmp(&other.task_type)
            .then_with(|| self.name.0.cmp(&other.name.0))
    }
}

/// Reason one durable provider action was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerComputeInvokeReason {
    /// Initial activation for a new configuration fingerprint.
    ConfigurationActivation,
    /// At least one task could not sync-match a compatible poller.
    NoSyncMatch,
    /// Periodic metrics exceeded the configured backlog threshold.
    Backlog,
    /// Persistent backlog outlived the configured worker lifetime.
    WorkerRefresh,
}

/// Durable eligibility classification for one scaling group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerComputeGroupEligibility {
    /// Remote Nexus provider with an active scaler.
    Eligible,
    /// Direct provider is stored but not executed by this controller.
    UnsupportedProvider,
    /// Scaler is stored but not implemented by this controller.
    UnsupportedScaler,
}

/// Bounded durable health reported for one scaling group or controller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerComputeHealth {
    /// Controller is eligible and evaluating demand.
    Active,
    /// Process policy leaves the controller disabled.
    Disabled,
    /// Provider shape is valid but outside the active slice.
    UnsupportedProvider,
    /// Scaler shape is valid but outside the active slice.
    UnsupportedScaler,
    /// Stored configuration could not be activated.
    InvalidConfiguration,
    /// Canonical provider request exceeded the Nexus payload limit.
    ProviderRequestTooLarge,
    /// Configured Nexus endpoint does not currently resolve.
    MisconfiguredEndpoint,
    /// Namespace exhausted its fixed controller slots.
    CapacityLimited,
    /// A transient provider failure is waiting for retry.
    DeliveryRetrying,
    /// Provider delivery failed terminally.
    DeliveryTerminalFailure,
    /// Version or group no longer participates in reconciliation.
    Inactive,
}

/// Bounded persisted provider/controller failure category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerComputeFailureCategory {
    /// Namespace name could not be resolved.
    NamespaceUnresolved,
    /// Nexus endpoint does not exist.
    EndpointNotFound,
    /// Nexus transport failed before a handler response.
    Transport,
    /// Provider returned a retryable Nexus handler error.
    RetryableHandler,
    /// Provider returned a non-retryable Nexus handler error.
    NonRetryableHandler,
    /// Nexus operation completed unsuccessfully.
    OperationUnsuccessful,
    /// Provider returned asynchronous acceptance where sync success is required.
    AsyncResponse,
    /// Canonical request exceeded the Nexus payload limit.
    RequestTooLarge,
    /// Provider response payload was missing, malformed, or ambiguous.
    InvalidResponsePayload,
    /// Provider response echoed another action ID.
    ResponseIdMismatch,
    /// Controller persistence failed.
    Storage,
}

/// Durable lifecycle of one controller instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerComputeControllerLifecycle {
    /// Holds one namespace slot and participates in evaluation.
    Active,
    /// Eligible configuration awaiting a namespace slot.
    CapacityLimited,
    /// Version no longer exists or no longer has eligible groups.
    Inactive,
}

/// Durable delivery state of one provider action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerComputeProviderActionStatus {
    /// Eligible for a future claim.
    Pending,
    /// Owned under a time-bounded claim epoch.
    Claimed,
    /// Provider returned the exact synchronous acknowledgement.
    Delivered,
    /// A non-retryable outcome ended delivery.
    TerminalFailed,
    /// Newer configuration made the pending action stale.
    Superseded,
}

/// Stable digest identifying every behavior-affecting field of one scaling group.
///
/// This value fences actions across configuration changes. It is not a credential
/// and callers must not use it as a message authentication code.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConfigurationFingerprint([u8; 32]);

impl ConfigurationFingerprint {
    /// Hash an already-canonical, domain-separated byte representation.
    #[must_use]
    pub fn from_canonical_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Reconstruct a fingerprint from its persisted bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the fixed-width persisted/provider representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ConfigurationFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ConfigurationFingerprint")
            .field(&FingerprintHex(self.0))
            .finish()
    }
}

struct FingerprintHex([u8; 32]);

impl fmt::Debug for FingerprintHex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn canonical_fingerprint_is_deterministic_and_sensitive(
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            suffix in any::<u8>(),
        ) {
            let first = ConfigurationFingerprint::from_canonical_bytes(&bytes);
            let second = ConfigurationFingerprint::from_canonical_bytes(&bytes);
            prop_assert_eq!(first, second);

            let mut changed = bytes;
            changed.push(suffix);
            prop_assert_ne!(
                first,
                ConfigurationFingerprint::from_canonical_bytes(&changed)
            );
        }
    }
}
