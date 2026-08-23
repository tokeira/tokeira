//! Pure Aurora DSQL schema compatibility assessment.
//!
//! Assessment accepts catalog observations as values and performs no I/O. It
//! verifies every recognized ledger identity and applicable cumulative digest
//! before considering version policy, so an apparently supported version can
//! never hide modified migration bytes.

use crate::schema_contract::{MigrationIdentity, migration_set_digest};

/// Release-pinned schema compatibility limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaCompatibilityContract {
    /// Tokeira release owning the contract.
    pub tokeira_release: String,
    /// Oldest readable schema version.
    pub minimum_supported_version: u32,
    /// Schema version automatic migration reaches.
    pub target_version: u32,
    /// Newest readable schema version.
    pub maximum_readable_version: u32,
    /// Canonical digest through `maximum_readable_version`.
    pub migration_set_digest: String,
}

/// Storage-local migration authority selected by the engine boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaMigrationPolicy {
    /// Apply verified forward migrations to the contract target.
    Automatic,
    /// Perform no writes and require the operator to migrate an older schema.
    ValidateOnly,
}

/// One applied row from the `schema_version` ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMigration {
    /// Applied numeric version.
    pub version: u32,
    /// Applied filename-derived identity.
    pub name: String,
    /// Applied SQL checksum.
    pub checksum: String,
}

/// Persisted cumulative compatibility identity for one schema version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaCompatibilityRecord {
    /// Schema version represented by this row.
    pub schema_version: u32,
    /// Tokeira release that recorded the row.
    pub tokeira_release: String,
    /// Canonical migration prefix digest at `schema_version`.
    pub migration_set_digest: String,
}

/// Read-only catalog observation supplied to pure assessment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SchemaObservation {
    /// Ordered rows read from `schema_version`, or empty when the table is absent.
    pub applied_migrations: Vec<AppliedMigration>,
    /// Latest applicable `schema_compatibility` row, if the table/row exists.
    pub compatibility: Option<SchemaCompatibilityRecord>,
}

/// Approved compatibility outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaDecision {
    /// Install the complete schema into an empty automatic-policy database.
    Initialize {
        /// Target schema version.
        target: u32,
    },
    /// Apply verified migrations from the current version to target.
    Migrate {
        /// Current verified version.
        from: u32,
        /// Target schema version.
        to: u32,
    },
    /// Continue without migrating or downgrading.
    Compatible {
        /// Current verified readable version.
        current: u32,
        /// Whether automatic policy may backfill missing legacy digest metadata.
        legacy_backfill: bool,
    },
    /// The schema is valid but policy forbids the required write.
    MigrationRequired {
        /// Current version, with zero representing an uninitialized database.
        current: u32,
        /// Required target version.
        target: u32,
    },
    /// The observed schema is unsafe for this release.
    Reject(SchemaIncompatibility),
}

/// Structured rejection including the full supported interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaIncompatibility {
    /// Highest observed ledger version, if any.
    pub observed_version: Option<u32>,
    /// Contract minimum.
    pub minimum_supported_version: u32,
    /// Contract migration target.
    pub target_version: u32,
    /// Contract readable maximum.
    pub maximum_readable_version: u32,
    /// Specific mismatch category.
    pub category: SchemaIncompatibilityCategory,
}

/// Reason an observed schema cannot be read or migrated safely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaIncompatibilityCategory {
    /// Ledger versions are missing, duplicated, or reordered.
    LedgerOrdering {
        /// Version expected at the failing position.
        expected: u32,
        /// Version actually observed.
        observed: u32,
    },
    /// An applied version is not recognized by this readable set.
    UnknownMigration {
        /// Unknown applied version.
        version: u32,
    },
    /// A recognized version carries a different filename identity.
    MigrationNameMismatch {
        /// Affected version.
        version: u32,
        /// Expected canonical name.
        expected: String,
        /// Observed ledger name.
        observed: String,
    },
    /// A recognized version carries a different SQL checksum.
    ChecksumMismatch {
        /// Affected version.
        version: u32,
        /// Expected SQL checksum.
        expected: String,
        /// Observed ledger checksum.
        observed: String,
    },
    /// Persisted cumulative digest differs from the canonical prefix.
    DigestMismatch {
        /// Version whose prefix digest differs.
        version: u32,
        /// Expected canonical digest.
        expected: String,
        /// Observed persisted digest.
        observed: String,
    },
    /// Compatibility metadata describes a different version from the ledger head.
    MetadataVersionMismatch {
        /// Ledger head.
        current: u32,
        /// Persisted compatibility version.
        metadata: u32,
    },
    /// Current version predates the readable interval.
    BelowMinimum,
    /// Current version exceeds the readable interval.
    FutureSchema,
}

/// Apply the approved decision table to a read-only schema observation.
pub fn assess_schema_compatibility(
    contract: &SchemaCompatibilityContract,
    recognized: &[MigrationIdentity],
    observation: &SchemaObservation,
    policy: SchemaMigrationPolicy,
) -> SchemaDecision {
    let observed_version = observation
        .applied_migrations
        .last()
        .map(|migration| migration.version);
    let reject = |category| {
        SchemaDecision::Reject(SchemaIncompatibility {
            observed_version,
            minimum_supported_version: contract.minimum_supported_version,
            target_version: contract.target_version,
            maximum_readable_version: contract.maximum_readable_version,
            category,
        })
    };

    match migration_set_digest(recognized, contract.maximum_readable_version) {
        Ok(expected) if expected == contract.migration_set_digest => {}
        Ok(expected) => {
            return reject(SchemaIncompatibilityCategory::DigestMismatch {
                version: contract.maximum_readable_version,
                expected,
                observed: contract.migration_set_digest.clone(),
            });
        }
        Err(_) => {
            return reject(SchemaIncompatibilityCategory::UnknownMigration {
                version: contract.maximum_readable_version,
            });
        }
    }

    for (index, applied) in observation.applied_migrations.iter().enumerate() {
        let expected_version = match u32::try_from(index + 1) {
            Ok(version) => version,
            Err(_) => {
                return reject(SchemaIncompatibilityCategory::FutureSchema);
            }
        };
        if applied.version != expected_version {
            return reject(SchemaIncompatibilityCategory::LedgerOrdering {
                expected: expected_version,
                observed: applied.version,
            });
        }
        if applied.version > contract.maximum_readable_version {
            return reject(SchemaIncompatibilityCategory::FutureSchema);
        }
        let Some(expected) = recognized.get(index) else {
            return reject(SchemaIncompatibilityCategory::UnknownMigration {
                version: applied.version,
            });
        };
        if applied.name != expected.name {
            return reject(SchemaIncompatibilityCategory::MigrationNameMismatch {
                version: applied.version,
                expected: expected.name.clone(),
                observed: applied.name.clone(),
            });
        }
        if applied.checksum != expected.checksum {
            return reject(SchemaIncompatibilityCategory::ChecksumMismatch {
                version: applied.version,
                expected: expected.checksum.clone(),
                observed: applied.checksum.clone(),
            });
        }
    }

    let Some(current) = observed_version else {
        if let Some(metadata) = &observation.compatibility {
            return reject(SchemaIncompatibilityCategory::MetadataVersionMismatch {
                current: 0,
                metadata: metadata.schema_version,
            });
        }
        return match policy {
            SchemaMigrationPolicy::Automatic => SchemaDecision::Initialize {
                target: contract.target_version,
            },
            SchemaMigrationPolicy::ValidateOnly => SchemaDecision::MigrationRequired {
                current: 0,
                target: contract.target_version,
            },
        };
    };

    if let Some(metadata) = &observation.compatibility {
        if metadata.schema_version != current {
            return reject(SchemaIncompatibilityCategory::MetadataVersionMismatch {
                current,
                metadata: metadata.schema_version,
            });
        }
        let expected = match migration_set_digest(recognized, current) {
            Ok(digest) => digest,
            Err(_) => {
                return reject(SchemaIncompatibilityCategory::UnknownMigration {
                    version: current,
                });
            }
        };
        if metadata.migration_set_digest != expected {
            return reject(SchemaIncompatibilityCategory::DigestMismatch {
                version: current,
                expected,
                observed: metadata.migration_set_digest.clone(),
            });
        }
    }

    if current < contract.minimum_supported_version {
        return reject(SchemaIncompatibilityCategory::BelowMinimum);
    }
    if current > contract.maximum_readable_version {
        return reject(SchemaIncompatibilityCategory::FutureSchema);
    }
    if current < contract.target_version {
        return match policy {
            SchemaMigrationPolicy::Automatic => SchemaDecision::Migrate {
                from: current,
                to: contract.target_version,
            },
            SchemaMigrationPolicy::ValidateOnly => SchemaDecision::MigrationRequired {
                current,
                target: contract.target_version,
            },
        };
    }
    SchemaDecision::Compatible {
        current,
        legacy_backfill: observation.compatibility.is_none()
            && policy == SchemaMigrationPolicy::Automatic,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::schema_contract::sha256_hex;

    fn recognized(count: u32) -> Vec<MigrationIdentity> {
        (1..=count)
            .map(|version| MigrationIdentity {
                version,
                name: format!("migration_{version}"),
                checksum: sha256_hex(format!("sql-{version}").as_bytes()),
            })
            .collect()
    }

    fn observation(
        migrations: &[MigrationIdentity],
        current: u32,
        with_metadata: bool,
    ) -> SchemaObservation {
        let applied_migrations = migrations
            .iter()
            .take(usize::try_from(current).expect("test version fits usize"))
            .map(|migration| AppliedMigration {
                version: migration.version,
                name: migration.name.clone(),
                checksum: migration.checksum.clone(),
            })
            .collect();
        let compatibility = (with_metadata && current > 0).then(|| SchemaCompatibilityRecord {
            schema_version: current,
            tokeira_release: "previous".to_owned(),
            migration_set_digest: migration_set_digest(migrations, current)
                .expect("fixture prefix exists"),
        });
        SchemaObservation {
            applied_migrations,
            compatibility,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 7: schema compatibility matches the decision table.
        #[test]
        fn compatibility_matches_decision_table(
            maximum in 2_u32..40,
            minimum_seed in 1_u32..40,
            target_seed in 1_u32..40,
            current_seed in 0_u32..45,
            automatic in any::<bool>(),
            metadata in any::<bool>(),
            corrupt_checksum in any::<bool>(),
        ) {
            let minimum = 1 + (minimum_seed - 1) % maximum;
            let target = minimum + (target_seed - 1) % (maximum - minimum + 1);
            let current = current_seed % (maximum + 2);
            let migrations = recognized(maximum);
            let contract = SchemaCompatibilityContract {
                tokeira_release: "test".to_owned(),
                minimum_supported_version: minimum,
                target_version: target,
                maximum_readable_version: maximum,
                migration_set_digest: migration_set_digest(&migrations, maximum)
                    .expect("fixture full digest"),
            };
            let mut observed = observation(&migrations, current.min(maximum), metadata);
            if current > maximum {
                observed.applied_migrations.push(AppliedMigration {
                    version: maximum + 1,
                    name: "future".to_owned(),
                    checksum: sha256_hex(b"future"),
                });
            }
            if corrupt_checksum && !observed.applied_migrations.is_empty() {
                observed.applied_migrations[0].checksum = sha256_hex(b"corrupt");
            }
            let before = observed.clone();
            let policy = if automatic {
                SchemaMigrationPolicy::Automatic
            } else {
                SchemaMigrationPolicy::ValidateOnly
            };
            let decision = assess_schema_compatibility(&contract, &migrations, &observed, policy);
            prop_assert_eq!(&observed, &before);

            if corrupt_checksum && current > 0 {
                prop_assert!(matches!(
                    decision,
                    SchemaDecision::Reject(SchemaIncompatibility {
                        category: SchemaIncompatibilityCategory::ChecksumMismatch { .. },
                        ..
                    })
                ), "checksum drift must reject before policy");
            } else if current == 0 {
                prop_assert_eq!(
                    decision,
                    if automatic {
                        SchemaDecision::Initialize { target }
                    } else {
                        SchemaDecision::MigrationRequired { current: 0, target }
                    }
                );
            } else if current > maximum {
                prop_assert!(matches!(decision, SchemaDecision::Reject(_)));
            } else if current < minimum {
                prop_assert!(matches!(
                    decision,
                    SchemaDecision::Reject(SchemaIncompatibility {
                        category: SchemaIncompatibilityCategory::BelowMinimum,
                        ..
                    })
                ), "below-minimum schemas must reject");
            } else if current < target {
                prop_assert_eq!(
                    decision,
                    if automatic {
                        SchemaDecision::Migrate { from: current, to: target }
                    } else {
                        SchemaDecision::MigrationRequired { current, target }
                    }
                );
            } else {
                prop_assert_eq!(
                    decision,
                    SchemaDecision::Compatible {
                        current,
                        legacy_backfill: !metadata && automatic,
                    }
                );
            }
        }
    }

    #[test]
    fn checksum_and_digest_mismatches_precede_version_policy() {
        let migrations = recognized(3);
        let contract = SchemaCompatibilityContract {
            tokeira_release: "test".to_owned(),
            minimum_supported_version: 3,
            target_version: 3,
            maximum_readable_version: 3,
            migration_set_digest: migration_set_digest(&migrations, 3).expect("fixture digest"),
        };
        let mut observed = observation(&migrations, 1, true);
        observed.applied_migrations[0].checksum = sha256_hex(b"changed");
        assert!(matches!(
            assess_schema_compatibility(
                &contract,
                &migrations,
                &observed,
                SchemaMigrationPolicy::ValidateOnly,
            ),
            SchemaDecision::Reject(SchemaIncompatibility {
                category: SchemaIncompatibilityCategory::ChecksumMismatch { .. },
                ..
            })
        ));

        let mut observed = observation(&migrations, 3, true);
        observed
            .compatibility
            .as_mut()
            .expect("metadata fixture")
            .migration_set_digest = "sha256:bad".to_owned();
        assert!(matches!(
            assess_schema_compatibility(
                &contract,
                &migrations,
                &observed,
                SchemaMigrationPolicy::Automatic,
            ),
            SchemaDecision::Reject(SchemaIncompatibility {
                category: SchemaIncompatibilityCategory::DigestMismatch { .. },
                ..
            })
        ));
    }

    #[test]
    fn every_compatibility_decision_row_is_explicit_and_read_only() {
        let migrations = recognized(5);
        let contract = SchemaCompatibilityContract {
            tokeira_release: "test".to_owned(),
            minimum_supported_version: 2,
            target_version: 3,
            maximum_readable_version: 4,
            migration_set_digest: migration_set_digest(&migrations, 4).expect("fixture digest"),
        };
        let cases = [
            (
                observation(&migrations, 0, false),
                SchemaMigrationPolicy::Automatic,
                SchemaDecision::Initialize { target: 3 },
            ),
            (
                observation(&migrations, 0, false),
                SchemaMigrationPolicy::ValidateOnly,
                SchemaDecision::MigrationRequired {
                    current: 0,
                    target: 3,
                },
            ),
            (
                observation(&migrations, 2, true),
                SchemaMigrationPolicy::Automatic,
                SchemaDecision::Migrate { from: 2, to: 3 },
            ),
            (
                observation(&migrations, 2, true),
                SchemaMigrationPolicy::ValidateOnly,
                SchemaDecision::MigrationRequired {
                    current: 2,
                    target: 3,
                },
            ),
            (
                observation(&migrations, 3, true),
                SchemaMigrationPolicy::Automatic,
                SchemaDecision::Compatible {
                    current: 3,
                    legacy_backfill: false,
                },
            ),
            (
                observation(&migrations, 4, true),
                SchemaMigrationPolicy::ValidateOnly,
                SchemaDecision::Compatible {
                    current: 4,
                    legacy_backfill: false,
                },
            ),
            (
                observation(&migrations, 4, false),
                SchemaMigrationPolicy::Automatic,
                SchemaDecision::Compatible {
                    current: 4,
                    legacy_backfill: true,
                },
            ),
        ];

        for (observed, policy, expected) in cases {
            let before = observed.clone();
            assert_eq!(
                assess_schema_compatibility(&contract, &migrations, &observed, policy),
                expected
            );
            assert_eq!(observed, before, "assessment must never write metadata");
        }

        assert!(matches!(
            assess_schema_compatibility(
                &contract,
                &migrations,
                &observation(&migrations, 1, true),
                SchemaMigrationPolicy::Automatic,
            ),
            SchemaDecision::Reject(SchemaIncompatibility {
                category: SchemaIncompatibilityCategory::BelowMinimum,
                ..
            })
        ));
        assert!(matches!(
            assess_schema_compatibility(
                &contract,
                &migrations,
                &observation(&migrations, 5, true),
                SchemaMigrationPolicy::Automatic,
            ),
            SchemaDecision::Reject(SchemaIncompatibility {
                category: SchemaIncompatibilityCategory::FutureSchema,
                ..
            })
        ));
    }

    #[test]
    fn released_name_and_metadata_version_changes_are_rejected() {
        let migrations = recognized(3);
        let contract = SchemaCompatibilityContract {
            tokeira_release: "test".to_owned(),
            minimum_supported_version: 1,
            target_version: 3,
            maximum_readable_version: 3,
            migration_set_digest: migration_set_digest(&migrations, 3).expect("fixture digest"),
        };
        let mut changed_name = observation(&migrations, 3, true);
        changed_name.applied_migrations[1].name = "renamed_release".to_owned();
        assert!(matches!(
            assess_schema_compatibility(
                &contract,
                &migrations,
                &changed_name,
                SchemaMigrationPolicy::Automatic,
            ),
            SchemaDecision::Reject(SchemaIncompatibility {
                category: SchemaIncompatibilityCategory::MigrationNameMismatch { .. },
                ..
            })
        ));

        let mut changed_version = observation(&migrations, 3, true);
        changed_version
            .compatibility
            .as_mut()
            .expect("metadata fixture")
            .schema_version = 2;
        assert!(matches!(
            assess_schema_compatibility(
                &contract,
                &migrations,
                &changed_version,
                SchemaMigrationPolicy::ValidateOnly,
            ),
            SchemaDecision::Reject(SchemaIncompatibility {
                category: SchemaIncompatibilityCategory::MetadataVersionMismatch { .. },
                ..
            })
        ));
    }
}
