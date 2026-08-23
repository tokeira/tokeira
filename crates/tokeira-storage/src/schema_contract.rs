//! Versioned compatibility contract for Tokeira's Aurora DSQL schema.
//!
//! This module is shared by the storage build script and runtime migration
//! code. Keeping canonical digest construction in one place prevents build
//! validation, compatibility assessment, and migration execution from
//! disagreeing about the identity of a migration prefix.

use sha2::{Digest, Sha256};

/// Canonical digest domain for the first schema-contract format.
pub const MIGRATION_SET_DIGEST_DOMAIN: &str = "tokeira-dsql-migration-set-v1\n";

/// One discovered migration's stable identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationIdentity {
    /// Contiguous one-based migration version.
    pub version: u32,
    /// Filename-derived migration name without the version prefix.
    pub name: String,
    /// Lowercase SHA-256 digest of the migration SQL bytes.
    pub checksum: String,
}

/// Release-pinned schema compatibility limits embedded in each binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaContract {
    /// On-disk contract format version.
    pub format_version: u32,
    /// Tokeira package release that owns this contract.
    pub tokeira_release: String,
    /// Oldest schema version this release can read.
    pub minimum_supported_version: u32,
    /// Schema version this release migrates to.
    pub target_version: u32,
    /// Newest schema version this release can safely read.
    pub maximum_readable_version: u32,
    /// Canonical digest through `maximum_readable_version`.
    pub migration_set_digest: String,
    /// Highest published migration whose identity is immutable.
    pub immutable_through_version: u32,
}

/// Checked-in immutable identity for one published migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedMigration {
    /// Published migration version.
    pub version: u32,
    /// Published migration name.
    pub name: String,
    /// Published lowercase SHA-256 digest of its SQL bytes.
    pub checksum: String,
}

/// Baseline lock protecting published migrations from edits or reordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBaseline {
    /// On-disk baseline format version.
    pub format_version: u32,
    /// Highest migration protected by this baseline.
    pub immutable_through_version: u32,
    /// Ordered immutable migration identities.
    pub migrations: Vec<LockedMigration>,
}

/// Return the lowercase SHA-256 digest for arbitrary bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Validate migration ordering and stable identity syntax.
pub fn validate_migrations(migrations: &[MigrationIdentity]) -> Result<(), String> {
    if migrations.is_empty() {
        return Err("migration set must not be empty".to_owned());
    }

    for (index, migration) in migrations.iter().enumerate() {
        let expected = u32::try_from(index)
            .map_err(|_| "migration set contains too many entries".to_owned())?
            .checked_add(1)
            .ok_or_else(|| "migration version overflow".to_owned())?;
        if migration.version != expected {
            return Err(format!(
                "migration version {} is out of order; expected {expected}",
                migration.version
            ));
        }
        if migration.name.is_empty()
            || !migration
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(format!(
                "migration V{} has invalid canonical name {:?}",
                migration.version, migration.name
            ));
        }
        if !is_lowercase_sha256(&migration.checksum) {
            return Err(format!(
                "migration V{} checksum must be 64 lowercase hexadecimal characters",
                migration.version
            ));
        }
    }
    Ok(())
}

/// Compute the canonical digest for migrations through `maximum_version`.
pub fn canonical_migration_set_bytes(
    migrations: &[MigrationIdentity],
    maximum_version: u32,
) -> Result<Vec<u8>, String> {
    validate_migrations(migrations)?;
    if maximum_version == 0 {
        return Err("maximum digest version must be non-zero".to_owned());
    }
    let maximum_index = usize::try_from(maximum_version)
        .map_err(|_| "maximum digest version does not fit usize".to_owned())?;
    if maximum_index > migrations.len() {
        return Err(format!(
            "migration set ends at V{}, below requested digest V{maximum_version}",
            migrations.len()
        ));
    }

    let mut canonical = Vec::from(MIGRATION_SET_DIGEST_DOMAIN.as_bytes());
    for migration in &migrations[..maximum_index] {
        canonical.extend_from_slice(migration.version.to_string().as_bytes());
        canonical.push(0);
        canonical.extend_from_slice(migration.name.as_bytes());
        canonical.push(0);
        canonical.extend_from_slice(migration.checksum.as_bytes());
        canonical.push(b'\n');
    }
    Ok(canonical)
}

/// Compute the canonical digest for migrations through `maximum_version`.
pub fn migration_set_digest(
    migrations: &[MigrationIdentity],
    maximum_version: u32,
) -> Result<String, String> {
    let canonical = canonical_migration_set_bytes(migrations, maximum_version)?;
    Ok(format!("sha256:{}", sha256_hex(&canonical)))
}

/// Compute the canonical digest at every migration prefix.
pub fn cumulative_prefix_digests(
    migrations: &[MigrationIdentity],
) -> Result<Vec<(u32, String)>, String> {
    validate_migrations(migrations)?;
    migrations
        .iter()
        .map(|migration| {
            migration_set_digest(migrations, migration.version)
                .map(|digest| (migration.version, digest))
        })
        .collect()
}

/// Validate the release contract and immutable baseline against discovered SQL.
pub fn validate_schema_contract(
    contract: &SchemaContract,
    baseline: &SchemaBaseline,
    migrations: &[MigrationIdentity],
    package_version: &str,
) -> Result<(), String> {
    validate_migrations(migrations)?;
    if contract.format_version != 1 {
        return Err(format!(
            "unsupported schema contract format {}",
            contract.format_version
        ));
    }
    if baseline.format_version != 1 {
        return Err(format!(
            "unsupported schema baseline format {}",
            baseline.format_version
        ));
    }
    if contract.tokeira_release != package_version {
        return Err(format!(
            "schema contract release {} does not match package version {package_version}",
            contract.tokeira_release
        ));
    }
    if contract.minimum_supported_version == 0
        || contract.minimum_supported_version > contract.target_version
        || contract.target_version > contract.maximum_readable_version
    {
        return Err(format!(
            "schema versions must satisfy 0 < minimum <= target <= maximum; got {} <= {} <= {}",
            contract.minimum_supported_version,
            contract.target_version,
            contract.maximum_readable_version
        ));
    }
    if contract.immutable_through_version < contract.maximum_readable_version {
        return Err(format!(
            "immutable ceiling V{} is below maximum readable V{}",
            contract.immutable_through_version, contract.maximum_readable_version
        ));
    }
    if baseline.immutable_through_version != contract.immutable_through_version {
        return Err(format!(
            "baseline ceiling V{} does not match contract ceiling V{}",
            baseline.immutable_through_version, contract.immutable_through_version
        ));
    }
    let migration_head = migrations
        .last()
        .map(|migration| migration.version)
        .ok_or_else(|| "migration set must not be empty".to_owned())?;
    if contract.maximum_readable_version > migration_head
        || contract.immutable_through_version > migration_head
    {
        return Err(format!(
            "schema contract references V{} but migration head is V{migration_head}",
            contract
                .maximum_readable_version
                .max(contract.immutable_through_version)
        ));
    }
    let expected_digest = migration_set_digest(migrations, contract.maximum_readable_version)?;
    if contract.migration_set_digest != expected_digest {
        return Err(format!(
            "schema migration-set digest mismatch: contract {}, computed {expected_digest}",
            contract.migration_set_digest
        ));
    }

    let immutable_count = usize::try_from(contract.immutable_through_version)
        .map_err(|_| "immutable ceiling does not fit usize".to_owned())?;
    if baseline.migrations.len() != immutable_count {
        return Err(format!(
            "baseline contains {} entries but immutable ceiling requires {immutable_count}",
            baseline.migrations.len()
        ));
    }
    for (locked, actual) in baseline
        .migrations
        .iter()
        .zip(migrations.iter().take(immutable_count))
    {
        if locked.version != actual.version
            || locked.name != actual.name
            || locked.checksum != actual.checksum
        {
            return Err(format!(
                "published migration V{} changed: locked {:?}/{}, actual {:?}/{}",
                actual.version, locked.name, locked.checksum, actual.name, actual.checksum
            ));
        }
    }
    Ok(())
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn identities(names: &[String]) -> Vec<MigrationIdentity> {
        names
            .iter()
            .enumerate()
            .map(|(index, name)| MigrationIdentity {
                version: u32::try_from(index + 1).expect("test migration count fits u32"),
                name: name.clone(),
                checksum: sha256_hex(format!("CREATE TABLE {name} (id TEXT);\n").as_bytes()),
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 6: the release schema contract is deterministic and immutable
        #[test]
        fn migration_digests_are_deterministic_and_prefix_stable(
            names in prop::collection::vec("[a-z][a-z0-9_]{0,11}", 1..40),
            appended in prop::collection::vec("[a-z][a-z0-9_]{0,11}", 0..20),
        ) {
            let original = identities(&names);
            let first = cumulative_prefix_digests(&original).expect("generated set is valid");
            let second = cumulative_prefix_digests(&original).expect("same set stays valid");
            prop_assert_eq!(&first, &second);

            let mut extended_names = names;
            extended_names.extend(appended);
            let extended = identities(&extended_names);
            let extended_prefixes = cumulative_prefix_digests(&extended)
                .expect("appended set is valid");
            prop_assert_eq!(first.as_slice(), &extended_prefixes[..first.len()]);
        }

        // Feature: managed-embedded-dsql, Property 6: the release schema contract is deterministic and immutable
        #[test]
        fn generated_contract_mutations_match_the_reference_model(
            names in prop::collection::vec("[a-z][a-z0-9_]{0,11}", 1..40),
            fault in 0u8..8,
            position in any::<usize>(),
        ) {
            let mut migrations = identities(&names);
            let head = u32::try_from(migrations.len()).expect("property migration count fits u32");
            let mut contract = SchemaContract {
                format_version: 1,
                tokeira_release: "0.1.0".to_owned(),
                minimum_supported_version: 1,
                target_version: head,
                maximum_readable_version: head,
                migration_set_digest: migration_set_digest(&migrations, head)
                    .expect("generated digest"),
                immutable_through_version: head,
            };
            let mut baseline = SchemaBaseline {
                format_version: 1,
                immutable_through_version: head,
                migrations: migrations
                    .iter()
                    .map(|migration| LockedMigration {
                        version: migration.version,
                        name: migration.name.clone(),
                        checksum: migration.checksum.clone(),
                    })
                    .collect(),
            };
            let selected = position % migrations.len();
            match fault {
                0 => {}
                1 => migrations[selected].version = migrations[selected].version.saturating_add(1),
                2 => migrations[selected].name = "NOT_CANONICAL".to_owned(),
                3 => migrations[selected].checksum = "g".repeat(64),
                4 => baseline.migrations[selected].checksum = "f".repeat(64),
                5 => contract.tokeira_release = "0.2.0".to_owned(),
                6 => contract.migration_set_digest = format!("sha256:{}", "0".repeat(64)),
                7 => contract.minimum_supported_version = 0,
                _ => unreachable!("fault strategy is bounded"),
            }

            let accepted = validate_schema_contract(
                &contract,
                &baseline,
                &migrations,
                "0.1.0",
            )
            .is_ok();
            prop_assert_eq!(accepted, fault == 0);
        }
    }

    #[test]
    fn migration_validation_rejects_gap_duplicate_and_ordering() {
        let valid = identities(&["one".to_owned(), "two".to_owned()]);
        let mut gap = valid.clone();
        gap[1].version = 3;
        assert!(validate_migrations(&gap).is_err());

        let mut duplicate = valid.clone();
        duplicate[1].version = 1;
        assert!(validate_migrations(&duplicate).is_err());

        let mut reordered = valid;
        reordered.swap(0, 1);
        assert!(validate_migrations(&reordered).is_err());
    }

    #[test]
    fn canonical_bytes_and_fixed_digest_are_stable() {
        let migrations = vec![
            MigrationIdentity {
                version: 1,
                name: "one".to_owned(),
                checksum: "0".repeat(64),
            },
            MigrationIdentity {
                version: 2,
                name: "two".to_owned(),
                checksum: "1".repeat(64),
            },
        ];
        let expected = format!(
            "{MIGRATION_SET_DIGEST_DOMAIN}1\0one\0{}\n2\0two\0{}\n",
            "0".repeat(64),
            "1".repeat(64)
        )
        .into_bytes();

        assert_eq!(
            canonical_migration_set_bytes(&migrations, 2).expect("canonical fixture"),
            expected
        );
        assert_eq!(
            migration_set_digest(&migrations, 2).expect("fixed fixture digest"),
            "sha256:2a7ae66405a33a3c0dc8c750e8fdc10ad9ffdd0babd87178f9aa9ff1314a0987"
        );
    }

    #[test]
    fn contract_validation_rejects_release_digest_and_locked_changes() {
        let migrations = identities(&["one".to_owned(), "two".to_owned()]);
        let digest = migration_set_digest(&migrations, 2).expect("fixture digest");
        let contract = SchemaContract {
            format_version: 1,
            tokeira_release: "0.1.0".to_owned(),
            minimum_supported_version: 1,
            target_version: 2,
            maximum_readable_version: 2,
            migration_set_digest: digest,
            immutable_through_version: 2,
        };
        let baseline = SchemaBaseline {
            format_version: 1,
            immutable_through_version: 2,
            migrations: migrations
                .iter()
                .map(|migration| LockedMigration {
                    version: migration.version,
                    name: migration.name.clone(),
                    checksum: migration.checksum.clone(),
                })
                .collect(),
        };
        assert!(validate_schema_contract(&contract, &baseline, &migrations, "0.1.0").is_ok());

        let mut wrong_release = contract.clone();
        wrong_release.tokeira_release = "0.2.0".to_owned();
        assert!(validate_schema_contract(&wrong_release, &baseline, &migrations, "0.1.0").is_err());

        let mut wrong_digest = contract.clone();
        wrong_digest.migration_set_digest = format!("sha256:{}", "0".repeat(64));
        assert!(validate_schema_contract(&wrong_digest, &baseline, &migrations, "0.1.0").is_err());

        let mut changed_baseline = baseline;
        changed_baseline.migrations[0].name = "changed".to_owned();
        assert!(
            validate_schema_contract(&contract, &changed_baseline, &migrations, "0.1.0").is_err()
        );
    }

    #[test]
    fn contract_validation_rejects_every_invalid_version_inequality() {
        let migrations = identities(&["one".to_owned(), "two".to_owned(), "three".to_owned()]);
        let baseline = SchemaBaseline {
            format_version: 1,
            immutable_through_version: 3,
            migrations: migrations
                .iter()
                .map(|migration| LockedMigration {
                    version: migration.version,
                    name: migration.name.clone(),
                    checksum: migration.checksum.clone(),
                })
                .collect(),
        };
        let valid = SchemaContract {
            format_version: 1,
            tokeira_release: "0.1.0".to_owned(),
            minimum_supported_version: 1,
            target_version: 2,
            maximum_readable_version: 3,
            migration_set_digest: migration_set_digest(&migrations, 3).expect("fixture digest"),
            immutable_through_version: 3,
        };

        for (minimum, target, maximum, immutable) in
            [(0, 2, 3, 3), (3, 2, 3, 3), (1, 3, 2, 3), (1, 2, 3, 2)]
        {
            let invalid = SchemaContract {
                minimum_supported_version: minimum,
                target_version: target,
                maximum_readable_version: maximum,
                immutable_through_version: immutable,
                ..valid.clone()
            };
            assert!(validate_schema_contract(&invalid, &baseline, &migrations, "0.1.0").is_err());
        }
    }

    #[test]
    fn every_baseline_position_is_immutable() {
        let names = (1..=24).map(|index| format!("migration_{index}"));
        let migrations = identities(&names.collect::<Vec<_>>());
        let contract = SchemaContract {
            format_version: 1,
            tokeira_release: "0.1.0".to_owned(),
            minimum_supported_version: 1,
            target_version: 24,
            maximum_readable_version: 24,
            migration_set_digest: migration_set_digest(&migrations, 24).expect("fixture digest"),
            immutable_through_version: 24,
        };
        let baseline = SchemaBaseline {
            format_version: 1,
            immutable_through_version: 24,
            migrations: migrations
                .iter()
                .map(|migration| LockedMigration {
                    version: migration.version,
                    name: migration.name.clone(),
                    checksum: migration.checksum.clone(),
                })
                .collect(),
        };

        for position in 0..baseline.migrations.len() {
            let mut changed = baseline.clone();
            changed.migrations[position].checksum = "f".repeat(64);
            assert!(
                validate_schema_contract(&contract, &changed, &migrations, "0.1.0").is_err(),
                "baseline mutation at position {position} was accepted"
            );
        }
    }
}
