//! Model-based properties for managed embedded DSQL schema bootstrap ordering.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::{
    CONTROL_LEASE_BOOTSTRAP_SQL, MigrationRunner, SCHEMA_COMPATIBILITY_BOOTSTRAP_SQL,
    SCHEMA_VERSION_BOOTSTRAP_SQL, bootstrap_statements_for_decision,
    post_claim_bootstrap_statements,
};
use crate::{
    dsql::{
        AppliedMigration, DdlValidator, SchemaCompatibilityRecord, SchemaDecision,
        SchemaMigrationPolicy, SchemaObservation, assess_schema_compatibility,
    },
    schema_contract::{MigrationIdentity, migration_set_digest},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelOperation {
    AcquireFence,
    CheckFence,
    Bootstrap { migration_version: u32 },
    Execute { migration_version: u32 },
    Record { migration_version: u32 },
    PersistCompatibility { schema_version: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ApplicationModelState {
    applied_prefix: u32,
    physically_completed: BTreeSet<u32>,
    compatibility_table_available: bool,
    compatibility_version: Option<u32>,
    compatibility_digest: Option<String>,
    fence_owned: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelFailure {
    Fenced,
    InvalidOrdering,
    MissingCompatibilityTable,
}

fn bootstrap_version(statement: &str) -> u32 {
    if statement == SCHEMA_VERSION_BOOTSTRAP_SQL {
        1
    } else if statement == SCHEMA_COMPATIBILITY_BOOTSTRAP_SQL {
        66
    } else if statement == CONTROL_LEASE_BOOTSTRAP_SQL {
        67
    } else {
        panic!("post-claim bootstrap must use a recognized embedded migration")
    }
}

fn automatic_application_plan(applied_prefix: u32, target: u32) -> Vec<ModelOperation> {
    let mut operations = vec![ModelOperation::AcquireFence];
    for statement in post_claim_bootstrap_statements() {
        operations.push(ModelOperation::CheckFence);
        operations.push(ModelOperation::Bootstrap {
            migration_version: bootstrap_version(statement),
        });
    }
    operations.push(ModelOperation::CheckFence);
    for migration_version in (applied_prefix + 1)..=target {
        operations.push(ModelOperation::CheckFence);
        operations.push(ModelOperation::Execute { migration_version });
        operations.push(ModelOperation::CheckFence);
        operations.push(ModelOperation::Record { migration_version });
        operations.push(ModelOperation::CheckFence);
        operations.push(ModelOperation::PersistCompatibility {
            schema_version: migration_version,
        });
    }
    operations
}

fn legacy_backfill_plan(schema_version: u32) -> Vec<ModelOperation> {
    vec![
        ModelOperation::AcquireFence,
        ModelOperation::CheckFence,
        ModelOperation::Bootstrap {
            migration_version: 66,
        },
        ModelOperation::CheckFence,
        ModelOperation::PersistCompatibility { schema_version },
    ]
}

fn prefix_digest(version: u32) -> String {
    MigrationRunner::embedded_prefix_digests()
        .iter()
        .find(|prefix| prefix.version == version)
        .map(|prefix| prefix.digest.to_owned())
        .expect("generated version is a recognized migration prefix")
}

fn starting_state(applied_prefix: u32, metadata_seed: u32) -> ApplicationModelState {
    let compatibility_version = if applied_prefix == 0 {
        None
    } else {
        let selected = metadata_seed % (applied_prefix + 1);
        (selected > 0).then_some(selected)
    };
    let compatibility_table_available = applied_prefix >= 66 || compatibility_version.is_some();
    let mut physically_completed = (1..=applied_prefix).collect::<BTreeSet<_>>();
    if compatibility_table_available {
        physically_completed.insert(66);
    }
    ApplicationModelState {
        applied_prefix,
        physically_completed,
        compatibility_table_available,
        compatibility_version,
        compatibility_digest: compatibility_version.map(prefix_digest),
        fence_owned: false,
    }
}

fn apply_operation(
    state: &mut ApplicationModelState,
    operation: ModelOperation,
) -> Result<(), ModelFailure> {
    match operation {
        ModelOperation::AcquireFence => state.fence_owned = true,
        ModelOperation::CheckFence => {
            if !state.fence_owned {
                return Err(ModelFailure::Fenced);
            }
        }
        ModelOperation::Bootstrap { migration_version } => {
            if !state.fence_owned {
                return Err(ModelFailure::Fenced);
            }
            state.physically_completed.insert(migration_version);
            if migration_version == 66 {
                state.compatibility_table_available = true;
            }
        }
        ModelOperation::Execute { migration_version } => {
            if !state.fence_owned {
                return Err(ModelFailure::Fenced);
            }
            state.physically_completed.insert(migration_version);
            if migration_version == 66 {
                state.compatibility_table_available = true;
            }
        }
        ModelOperation::Record { migration_version } => {
            if !state.fence_owned {
                return Err(ModelFailure::Fenced);
            }
            if migration_version != state.applied_prefix + 1
                || !state.physically_completed.contains(&migration_version)
            {
                return Err(ModelFailure::InvalidOrdering);
            }
            state.applied_prefix = migration_version;
        }
        ModelOperation::PersistCompatibility { schema_version } => {
            if !state.fence_owned {
                return Err(ModelFailure::Fenced);
            }
            if !state.compatibility_table_available {
                return Err(ModelFailure::MissingCompatibilityTable);
            }
            if schema_version > state.applied_prefix {
                return Err(ModelFailure::InvalidOrdering);
            }
            state.compatibility_version = Some(schema_version);
            state.compatibility_digest = Some(prefix_digest(schema_version));
        }
    }
    Ok(())
}

fn apply_operations(
    state: &mut ApplicationModelState,
    operations: &[ModelOperation],
) -> Result<(), ModelFailure> {
    for operation in operations {
        apply_operation(state, *operation)?;
    }
    Ok(())
}

fn recovery_plan(state: &ApplicationModelState, target: u32) -> Vec<ModelOperation> {
    if state.applied_prefix < target {
        automatic_application_plan(state.applied_prefix, target)
    } else if state.compatibility_version != Some(target) {
        legacy_backfill_plan(target)
    } else {
        Vec::new()
    }
}

fn recognized_migrations() -> Vec<MigrationIdentity> {
    MigrationRunner::embedded()
        .dry_run()
        .expect("embedded migrations are valid")
        .into_iter()
        .map(|migration| MigrationIdentity {
            version: migration.version,
            name: migration.name,
            checksum: migration.checksum,
        })
        .collect()
}

fn observation(
    recognized: &[MigrationIdentity],
    current: u32,
    metadata_version: Option<u32>,
) -> SchemaObservation {
    SchemaObservation {
        applied_migrations: recognized
            .iter()
            .take(usize::try_from(current).expect("test version fits usize"))
            .map(|migration| AppliedMigration {
                version: migration.version,
                name: migration.name.clone(),
                checksum: migration.checksum.clone(),
            })
            .collect(),
        compatibility: metadata_version.map(|schema_version| SchemaCompatibilityRecord {
            schema_version,
            tokeira_release: "previous".to_owned(),
            migration_set_digest: migration_set_digest(recognized, schema_version)
                .expect("metadata version is a recognized prefix"),
        }),
    }
}

fn selected_metadata_version(current: u32, seed: u32) -> Option<u32> {
    if current == 0 {
        None
    } else {
        let selected = seed % (current + 1);
        (selected > 0).then_some(selected)
    }
}

// Feature: managed-embedded-dsql-schema-bootstrap, Property 1: bug condition is eliminated
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_bug_condition_is_eliminated(
        applied_prefix in 0_u32..MigrationRunner::embedded_schema_contract().target_version,
    ) {
        let target = MigrationRunner::embedded_schema_contract().target_version;
        let operations = automatic_application_plan(applied_prefix, target);
        let first_persistence = operations
            .iter()
            .position(|operation| {
                matches!(operation, ModelOperation::PersistCompatibility { .. })
            })
            .expect("every prefix below target has a compatibility persistence");
        let compatibility_bootstrap = operations
            .iter()
            .position(|operation| {
                matches!(
                    operation,
                    ModelOperation::Bootstrap {
                        migration_version: 66
                    }
                )
            });

        prop_assert!(
            compatibility_bootstrap.is_some_and(|position| position < first_persistence),
            "automatic prefix V{} persisted compatibility before the exact V066 bootstrap: {:?}",
            applied_prefix,
            operations,
        );
        let compatibility_bootstrap = compatibility_bootstrap.expect("asserted present");
        prop_assert_eq!(
            operations.get(compatibility_bootstrap - 1),
            Some(&ModelOperation::CheckFence)
        );
        prop_assert_eq!(
            operations.get(compatibility_bootstrap + 1),
            Some(&ModelOperation::CheckFence)
        );
    }
}

// Feature: managed-embedded-dsql-schema-bootstrap, Property 2: every valid prefix converges
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_every_valid_prefix_converges(
        applied_prefix in 0_u32..MigrationRunner::embedded_schema_contract().target_version,
        metadata_seed in any::<u32>(),
    ) {
        let contract = MigrationRunner::embedded_schema_contract();
        let mut state = starting_state(applied_prefix, metadata_seed);
        let operations = automatic_application_plan(applied_prefix, contract.target_version);

        prop_assert_eq!(apply_operations(&mut state, &operations), Ok(()));
        prop_assert_eq!(state.applied_prefix, contract.target_version);
        prop_assert_eq!(
            state.physically_completed,
            (1..=contract.target_version).collect::<BTreeSet<_>>()
        );
        prop_assert_eq!(state.compatibility_version, Some(contract.target_version));
        prop_assert_eq!(
            state.compatibility_digest.as_deref(),
            Some(contract.migration_set_digest.as_str())
        );
    }
}

// Feature: managed-embedded-dsql-schema-bootstrap, Property 3: every crash boundary recovers
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_every_crash_boundary_recovers(
        applied_prefix in 0_u32..MigrationRunner::embedded_schema_contract().target_version,
        metadata_seed in any::<u32>(),
        crash_seed in any::<usize>(),
    ) {
        let contract = MigrationRunner::embedded_schema_contract();
        let mut state = starting_state(applied_prefix, metadata_seed);
        let operations = automatic_application_plan(applied_prefix, contract.target_version);
        let crash_boundary = crash_seed % (operations.len() + 1);

        prop_assert_eq!(
            apply_operations(&mut state, &operations[..crash_boundary]),
            Ok(())
        );
        let recovery = recovery_plan(&state, contract.target_version);
        prop_assert_eq!(apply_operations(&mut state, &recovery), Ok(()));
        prop_assert_eq!(state.applied_prefix, contract.target_version);
        prop_assert_eq!(state.compatibility_version, Some(contract.target_version));
        prop_assert_eq!(
            state.compatibility_digest.as_deref(),
            Some(contract.migration_set_digest.as_str())
        );
        let all_versions_physical = (1..=contract.target_version)
            .all(|version| state.physically_completed.contains(&version));
        prop_assert!(all_versions_physical);
    }
}

// Feature: managed-embedded-dsql-schema-bootstrap, Property 4: fence loss stops mutation
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_fence_loss_stops_mutation(
        applied_prefix in 0_u32..MigrationRunner::embedded_schema_contract().target_version,
        metadata_seed in any::<u32>(),
        boundary_seed in any::<usize>(),
    ) {
        let target = MigrationRunner::embedded_schema_contract().target_version;
        let operations = automatic_application_plan(applied_prefix, target);
        let check_positions = operations
            .iter()
            .enumerate()
            .filter_map(|(position, operation)| {
                (*operation == ModelOperation::CheckFence).then_some(position)
            })
            .collect::<Vec<_>>();
        let selected = check_positions[boundary_seed % check_positions.len()];
        let mut state = starting_state(applied_prefix, metadata_seed);

        prop_assert_eq!(apply_operations(&mut state, &operations[..selected]), Ok(()));
        state.fence_owned = false;
        let fenced_state = state.clone();
        prop_assert_eq!(
            apply_operations(&mut state, &operations[selected..]),
            Err(ModelFailure::Fenced)
        );
        prop_assert_eq!(state, fenced_state);
    }
}

// Feature: managed-embedded-dsql-schema-bootstrap, Property 5: decisions outside the bug condition are preserved
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_decisions_outside_bug_condition_are_preserved(
        current_seed in any::<u32>(),
        metadata_seed in any::<u32>(),
        automatic in any::<bool>(),
    ) {
        let contract = MigrationRunner::compatibility_contract();
        let current = current_seed % (contract.target_version + 1);
        let metadata_version = selected_metadata_version(current, metadata_seed);
        let recognized = recognized_migrations();
        let observed = observation(&recognized, current, metadata_version);
        let policy = if automatic {
            SchemaMigrationPolicy::Automatic
        } else {
            SchemaMigrationPolicy::ValidateOnly
        };
        let decision = assess_schema_compatibility(&contract, &recognized, &observed, policy);

        match decision {
            SchemaDecision::Initialize { .. } | SchemaDecision::Migrate { .. } => {
                prop_assert!(automatic);
                prop_assert_eq!(
                    bootstrap_statements_for_decision(&decision)
                        .expect("automatic decisions bootstrap coordination"),
                    &[SCHEMA_VERSION_BOOTSTRAP_SQL, CONTROL_LEASE_BOOTSTRAP_SQL]
                );
            }
            SchemaDecision::MigrationRequired { .. } => {
                prop_assert!(!automatic);
                prop_assert!(bootstrap_statements_for_decision(&decision).is_err());
            }
            SchemaDecision::Compatible {
                current,
                legacy_backfill,
            } => {
                prop_assert!(
                    bootstrap_statements_for_decision(&decision)
                        .expect("compatible decisions have no pre-claim bootstrap")
                        .is_empty()
                );
                let expected_backfill = automatic && metadata_version != Some(current);
                prop_assert_eq!(legacy_backfill, expected_backfill);
                if legacy_backfill {
                    let bootstraps_compatibility =
                        legacy_backfill_plan(current).iter().any(|operation| {
                            matches!(
                            operation,
                            ModelOperation::Bootstrap {
                                migration_version: 66
                            }
                        )
                        });
                    prop_assert!(bootstraps_compatibility);
                }
            }
            SchemaDecision::Reject(incompatibility) => {
                prop_assert!(false, "valid generated observation rejected: {incompatibility:?}");
            }
        }
    }
}

// Feature: managed-embedded-dsql-schema-bootstrap, Property 6: bootstrap sources and schema contract are preserved
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_bootstrap_sources_and_schema_contract_are_preserved(
        pre_claim in any::<bool>(),
        statement_seed in any::<usize>(),
    ) {
        let initialize = SchemaDecision::Initialize { target: 67 };
        let statements = if pre_claim {
            bootstrap_statements_for_decision(&initialize)
                .expect("initialize has a coordination bootstrap")
        } else {
            post_claim_bootstrap_statements()
        };
        let expected: &[&str] = if pre_claim {
            &[SCHEMA_VERSION_BOOTSTRAP_SQL, CONTROL_LEASE_BOOTSTRAP_SQL]
        } else {
            &[
                SCHEMA_VERSION_BOOTSTRAP_SQL,
                CONTROL_LEASE_BOOTSTRAP_SQL,
                SCHEMA_COMPATIBILITY_BOOTSTRAP_SQL,
            ]
        };
        let selected = statement_seed % statements.len();

        prop_assert_eq!(statements, expected);
        prop_assert_eq!(statements[selected], expected[selected]);
        prop_assert_eq!(statements[selected].matches(';').count(), 1);
        prop_assert!(DdlValidator::validate(statements[selected], "bootstrap").is_empty());

        let contract = MigrationRunner::embedded_schema_contract();
        prop_assert_eq!(contract.target_version, 67);
        prop_assert_eq!(contract.maximum_readable_version, 67);
        prop_assert_eq!(contract.immutable_through_version, 67);
        prop_assert_eq!(
            contract.migration_set_digest.as_str(),
            "sha256:f9acbc0b4f472b90446109fa3553bde4ce71fa95b1f6dbd4efaa67a358783400"
        );
        prop_assert_eq!(
            prefix_digest(contract.maximum_readable_version),
            contract.migration_set_digest.as_str()
        );
    }
}
