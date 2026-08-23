//! Bounded metric label vocabularies shared across crates.
//!
//! Free-form strings are the easiest way to create accidental time-series
//! explosions. Domain crates should prefer these enums for labels with a known
//! vocabulary and use manifest descriptors to justify labels that are bounded by
//! configuration rather than by an enum.

use std::fmt;

/// Trait implemented by stable, enum-backed metric label values.
pub trait BoundedLabel: Copy + fmt::Display {
    /// Return the canonical on-wire label value.
    fn as_str(self) -> &'static str;
}

// Keep the enum implementation pattern identical for every label family. The
// generated `Display` and `AsRef<str>` implementations make labels ergonomic at
// call sites without letting arbitrary strings leak into metric helpers.
macro_rules! label_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl BoundedLabel for $name {
            fn as_str(self) -> &'static str {
                self.as_str()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

/// Build a `metrics` label from a bounded vocabulary value.
pub fn bounded_metric_label(name: &'static str, value: impl BoundedLabel) -> metrics::Label {
    metrics::Label::new(name, value.as_str())
}

label_enum! {
    /// Common operation outcome values shared by multiple crates.
    pub enum OutcomeLabel {
        Success => "success",
        Failure => "failure",
        Exhausted => "exhausted",
        NotRetried => "not_retried",
        Skipped => "skipped",
        RolledBack => "rolled_back",
        Accepted => "accepted",
        Rejected => "rejected",
        Timeout => "timeout",
        Conflict => "conflict",
        Error => "error",
    }
}

label_enum! {
    /// DSQL class-budget labels.
    pub enum DbClassLabel {
        Control => "control",
        Commit => "commit",
        Read => "read",
        Projection => "projection",
        Maintenance => "maintenance",
    }
}

label_enum! {
    /// Explicit embedded storage modes.
    pub enum EmbeddedStorageModeLabel {
        InMemory => "in_memory",
        ManagedDsql => "managed_dsql",
        ExistingDsql => "existing_dsql",
    }
}

label_enum! {
    /// Bounded Aurora DSQL lifecycle states.
    pub enum ClusterStatusLabel {
        NotApplicable => "not_applicable",
        Creating => "creating",
        Active => "active",
        Idle => "idle",
        Inactive => "inactive",
        Updating => "updating",
        Deleting => "deleting",
        Deleted => "deleted",
        Failed => "failed",
        PendingSetup => "pending_setup",
        PendingDelete => "pending_delete",
        Unknown => "unknown",
    }
}

label_enum! {
    /// Bounded embedded schema-compatibility outcomes.
    pub enum SchemaOutcomeLabel {
        NotApplicable => "not_applicable",
        Compatible => "compatible",
        Initialized => "initialized",
        Migrated => "migrated",
        MetadataBackfilled => "metadata_backfilled",
        MigrationRequired => "migration_required",
        Incompatible => "incompatible",
        Failed => "failed",
    }
}

label_enum! {
    /// Bounded embedded-owner lifecycle outcomes.
    pub enum OwnershipOutcomeLabel {
        NotApplicable => "not_applicable",
        AcquiredClean => "acquired_clean",
        AcquiredExpired => "acquired_expired",
        Released => "released",
        Lost => "lost",
        Rejected => "rejected",
    }
}

label_enum! {
    /// Bounded embedded lifecycle operation kinds.
    pub enum EmbeddedOperationLabel {
        Startup => "startup",
        ClusterRecovery => "cluster_recovery",
        Schema => "schema",
        Ownership => "ownership",
        Shutdown => "shutdown",
        DestroyPlan => "destroy_plan",
        DestroyApply => "destroy_apply",
    }
}

label_enum! {
    /// Redacted error classes for lifecycle metrics.
    pub enum ErrorClassLabel {
        None => "none",
        Configuration => "configuration",
        Descriptor => "descriptor",
        AccessDenied => "access_denied",
        Quota => "quota",
        Retryable => "retryable",
        Identity => "identity",
        Status => "status",
        Storage => "storage",
        Schema => "schema",
        Ownership => "ownership",
        Deadline => "deadline",
        Internal => "internal",
    }
}

label_enum! {
    /// Autoscaler control-loop labels.
    pub enum AutoscalerLoopLabel {
        Replica => "replica",
        ScaleOut => "scale_out",
        Retirement => "retirement",
    }
}

label_enum! {
    /// Autoscaler scaling decision direction.
    pub enum ScalingDirectionLabel {
        Up => "up",
        Down => "down",
        Hold => "hold",
    }
}

label_enum! {
    /// Controller nomination result labels.
    pub enum NominationOutcomeLabel {
        Accepted => "accepted",
        Rejected => "rejected",
        Timeout => "timeout",
    }
}

label_enum! {
    /// Retry-loop result labels.
    pub enum RetryOutcomeLabel {
        Retry => "retry",
        Success => "success",
        Exhausted => "exhausted",
        NotRetried => "not_retried",
    }
}

label_enum! {
    /// Controller CAS write outcome labels.
    pub enum ControllerCasOutcomeLabel {
        Success => "success",
        Conflict => "conflict",
        Error => "error",
    }
}

label_enum! {
    /// Projection record processing outcome labels.
    pub enum ProjectionOutcomeLabel {
        Success => "success",
        Failure => "failure",
        Skipped => "skipped",
    }
}

label_enum! {
    /// Bounded projection sink/checkpoint error categories.
    pub enum ProjectionErrorKindLabel {
        Storage => "storage",
        Sink => "sink",
        Serialization => "serialization",
        Checkpoint => "checkpoint",
        Unknown => "unknown",
    }
}

label_enum! {
    /// Runtime query dispatch path labels.
    pub enum QueryDispatchPathLabel {
        Direct => "direct",
        Buffered => "buffered",
    }
}

label_enum! {
    /// Runtime query dispatch outcome labels.
    pub enum QueryDispatchOutcomeLabel {
        Published => "published",
        Queued => "queued",
        Rejected => "rejected",
    }
}

label_enum! {
    /// Buffered query release outcome labels.
    pub enum QueryBufferOutcomeLabel {
        Released => "released",
        Failed => "failed",
    }
}

label_enum! {
    /// Runtime ownership rejection operation labels.
    pub enum NotShardOwnerOperationLabel {
        SubmitDrain => "submit_drain",
        SubmitInactive => "submit_inactive",
        SubmitForOwnedShard => "submit_for_owned_shard",
        CurrentShardEpoch => "current_shard_epoch",
        ShardEpochForCompletion => "shard_epoch_for_completion",
        Submit => "submit",
    }
}

label_enum! {
    /// Storage operation labels used by latency and outcome metrics.
    pub enum StorageOperationLabel {
        CommitTransition => "commit_transition",
        CommitTransitionForBundle => "commit_transition_for_bundle",
        LoadRun => "load_run",
        ResolveCurrentExecution => "resolve_current_execution",
        AppendProjectionRecord => "append_projection_record",
        AccumulateRollup => "accumulate_rollup",
        ProjectionApplyTx => "projection_apply_tx",
        AcquireLease => "acquire_lease",
        RenewLease => "renew_lease",
        ListBundleLeases => "list_bundle_leases",
        ApplyMigration => "apply_migration",
    }
}

label_enum! {
    /// Process and infrastructure service labels.
    pub enum ServiceLabel {
        Tokeirad => "tokeirad",
        Controller => "tokeira-controller",
        Autoscaler => "tokeira-autoscaler",
        Alloy => "alloy",
        Mimir => "mimir",
        Loki => "loki",
        Grafana => "grafana",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_label_values<const N: usize>(actual: [(&'static str, &'static str); N]) {
        for (actual, expected) in actual {
            assert_eq!(actual, expected);
            assert!(!actual.is_empty());
            assert!(
                actual
                    .bytes()
                    .all(|byte| byte == b'_' || byte == b'-' || byte.is_ascii_lowercase())
            );
        }
    }

    #[test]
    fn outcome_labels_are_stable() {
        assert_label_values([
            (OutcomeLabel::Success.as_str(), "success"),
            (OutcomeLabel::Failure.as_str(), "failure"),
            (OutcomeLabel::Exhausted.as_str(), "exhausted"),
            (OutcomeLabel::NotRetried.as_str(), "not_retried"),
            (OutcomeLabel::Skipped.as_str(), "skipped"),
            (OutcomeLabel::RolledBack.as_str(), "rolled_back"),
            (OutcomeLabel::Accepted.as_str(), "accepted"),
            (OutcomeLabel::Rejected.as_str(), "rejected"),
            (OutcomeLabel::Timeout.as_str(), "timeout"),
            (OutcomeLabel::Conflict.as_str(), "conflict"),
            (OutcomeLabel::Error.as_str(), "error"),
        ]);
    }

    #[test]
    fn storage_and_database_labels_are_stable() {
        assert_label_values([
            (DbClassLabel::Control.as_str(), "control"),
            (DbClassLabel::Commit.as_str(), "commit"),
            (DbClassLabel::Read.as_str(), "read"),
            (DbClassLabel::Projection.as_str(), "projection"),
            (DbClassLabel::Maintenance.as_str(), "maintenance"),
            (
                StorageOperationLabel::CommitTransition.as_str(),
                "commit_transition",
            ),
            (StorageOperationLabel::LoadRun.as_str(), "load_run"),
            (
                StorageOperationLabel::ResolveCurrentExecution.as_str(),
                "resolve_current_execution",
            ),
            (
                StorageOperationLabel::AppendProjectionRecord.as_str(),
                "append_projection_record",
            ),
            (
                StorageOperationLabel::AcquireLease.as_str(),
                "acquire_lease",
            ),
            (StorageOperationLabel::RenewLease.as_str(), "renew_lease"),
            (
                StorageOperationLabel::ListBundleLeases.as_str(),
                "list_bundle_leases",
            ),
            (
                StorageOperationLabel::ApplyMigration.as_str(),
                "apply_migration",
            ),
        ]);
    }

    #[test]
    fn controller_autoscaler_and_projection_labels_are_stable() {
        assert_label_values([
            (AutoscalerLoopLabel::Replica.as_str(), "replica"),
            (AutoscalerLoopLabel::ScaleOut.as_str(), "scale_out"),
            (AutoscalerLoopLabel::Retirement.as_str(), "retirement"),
            (ScalingDirectionLabel::Up.as_str(), "up"),
            (ScalingDirectionLabel::Down.as_str(), "down"),
            (ScalingDirectionLabel::Hold.as_str(), "hold"),
            (NominationOutcomeLabel::Accepted.as_str(), "accepted"),
            (NominationOutcomeLabel::Rejected.as_str(), "rejected"),
            (NominationOutcomeLabel::Timeout.as_str(), "timeout"),
            (RetryOutcomeLabel::Success.as_str(), "success"),
            (RetryOutcomeLabel::Retry.as_str(), "retry"),
            (RetryOutcomeLabel::Exhausted.as_str(), "exhausted"),
            (RetryOutcomeLabel::NotRetried.as_str(), "not_retried"),
            (ControllerCasOutcomeLabel::Success.as_str(), "success"),
            (ControllerCasOutcomeLabel::Conflict.as_str(), "conflict"),
            (ControllerCasOutcomeLabel::Error.as_str(), "error"),
            (ProjectionOutcomeLabel::Success.as_str(), "success"),
            (ProjectionOutcomeLabel::Failure.as_str(), "failure"),
            (ProjectionOutcomeLabel::Skipped.as_str(), "skipped"),
            (ProjectionErrorKindLabel::Storage.as_str(), "storage"),
            (ProjectionErrorKindLabel::Sink.as_str(), "sink"),
            (
                ProjectionErrorKindLabel::Serialization.as_str(),
                "serialization",
            ),
            (ProjectionErrorKindLabel::Checkpoint.as_str(), "checkpoint"),
            (ProjectionErrorKindLabel::Unknown.as_str(), "unknown"),
            (QueryDispatchPathLabel::Direct.as_str(), "direct"),
            (QueryDispatchPathLabel::Buffered.as_str(), "buffered"),
            (QueryDispatchOutcomeLabel::Published.as_str(), "published"),
            (QueryDispatchOutcomeLabel::Queued.as_str(), "queued"),
            (QueryDispatchOutcomeLabel::Rejected.as_str(), "rejected"),
            (QueryBufferOutcomeLabel::Released.as_str(), "released"),
            (QueryBufferOutcomeLabel::Failed.as_str(), "failed"),
            (
                NotShardOwnerOperationLabel::SubmitDrain.as_str(),
                "submit_drain",
            ),
            (
                NotShardOwnerOperationLabel::SubmitInactive.as_str(),
                "submit_inactive",
            ),
            (
                NotShardOwnerOperationLabel::SubmitForOwnedShard.as_str(),
                "submit_for_owned_shard",
            ),
            (
                NotShardOwnerOperationLabel::CurrentShardEpoch.as_str(),
                "current_shard_epoch",
            ),
            (
                NotShardOwnerOperationLabel::ShardEpochForCompletion.as_str(),
                "shard_epoch_for_completion",
            ),
            (NotShardOwnerOperationLabel::Submit.as_str(), "submit"),
        ]);
    }

    #[test]
    fn service_labels_are_stable() {
        assert_label_values([
            (ServiceLabel::Tokeirad.as_str(), "tokeirad"),
            (ServiceLabel::Controller.as_str(), "tokeira-controller"),
            (ServiceLabel::Autoscaler.as_str(), "tokeira-autoscaler"),
            (ServiceLabel::Alloy.as_str(), "alloy"),
            (ServiceLabel::Mimir.as_str(), "mimir"),
            (ServiceLabel::Loki.as_str(), "loki"),
            (ServiceLabel::Grafana.as_str(), "grafana"),
        ]);
    }

    #[test]
    fn bounded_metric_label_uses_typed_label_values() {
        let label = bounded_metric_label("outcome", ProjectionOutcomeLabel::Success);

        assert_eq!(label.key(), "outcome");
        assert_eq!(label.value(), "success");
    }
}
