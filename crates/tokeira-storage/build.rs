use std::{
    env, fs,
    path::{Path, PathBuf},
};

use schema_contract::{
    LockedMigration, MigrationIdentity, SchemaBaseline, SchemaContract, cumulative_prefix_digests,
    sha256_hex, validate_schema_contract,
};
use toml::Value;

// The shared module's public API is externally reachable from the library;
// build-script inclusion necessarily makes those same items locally private.
#[allow(unreachable_pub)]
#[path = "src/schema_contract.rs"]
mod schema_contract;

#[derive(Debug)]
struct Migration {
    version: u32,
    name: String,
    path: PathBuf,
    sql: String,
    checksum: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let migrations_dir = manifest_dir.join("migrations");
    let contract_path = manifest_dir.join("schema-contract.toml");
    let baseline_path = manifest_dir.join("schema-baseline.lock");
    println!("cargo:rerun-if-changed={}", migrations_dir.display());
    println!("cargo:rerun-if-changed={}", contract_path.display());
    println!("cargo:rerun-if-changed={}", baseline_path.display());

    let migrations = discover_migrations(&migrations_dir)?;
    let identities = migrations
        .iter()
        .map(|migration| MigrationIdentity {
            version: migration.version,
            name: migration.name.clone(),
            checksum: migration.checksum.clone(),
        })
        .collect::<Vec<_>>();
    let contract = parse_contract(&contract_path)?;
    let baseline = parse_baseline(&baseline_path)?;
    validate_schema_contract(
        &contract,
        &baseline,
        &identities,
        &env::var("CARGO_PKG_VERSION")?,
    )
    .map_err(invalid_data)?;
    let prefix_digests = cumulative_prefix_digests(&identities).map_err(invalid_data)?;
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    fs::write(
        out_dir.join("migrations_embedded.rs"),
        render(&migrations, &contract, &prefix_digests),
    )?;
    Ok(())
}

fn discover_migrations(dir: &Path) -> Result<Vec<Migration>, Box<dyn std::error::Error>> {
    let mut migrations = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        println!("cargo:rerun-if-changed={}", path.display());
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                invalid_data(format!("invalid migration filename {}", path.display()))
            })?;
        let (version, name) = parse_migration_filename(filename)?;
        let sql = fs::read_to_string(&path)?;
        let checksum = checksum(&sql);
        let embedded_path = path
            .strip_prefix(dir)
            .map(|relative| PathBuf::from("migrations").join(relative))
            .unwrap_or_else(|_| path.clone());
        migrations.push(Migration {
            version,
            name,
            path: embedded_path,
            sql,
            checksum,
        });
    }
    migrations.sort_by_key(|migration| migration.version);
    for pair in migrations.windows(2) {
        if pair[0].version == pair[1].version {
            return Err(format!("duplicate migration version {}", pair[0].version).into());
        }
        if pair[0].version + 1 != pair[1].version {
            return Err(format!(
                "migration version gap between {} and {}",
                pair[0].version, pair[1].version
            )
            .into());
        }
    }
    Ok(migrations)
}

fn parse_migration_filename(filename: &str) -> Result<(u32, String), Box<dyn std::error::Error>> {
    let rest = filename
        .strip_prefix('V')
        .ok_or_else(|| invalid_data("migration filename must start with V"))?;
    let (version, description) = rest
        .split_once("__")
        .ok_or_else(|| invalid_data("migration filename must contain __ separator"))?;
    let description = description
        .strip_suffix(".sql")
        .ok_or_else(|| invalid_data("migration filename must end with .sql"))?;
    Ok((version.parse()?, description.to_owned()))
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn checksum(sql: &str) -> String {
    sha256_hex(sql.as_bytes())
}

fn parse_contract(path: &Path) -> Result<SchemaContract, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let value = content.parse::<Value>()?;
    Ok(SchemaContract {
        format_version: contract_u32(&value, "format_version")?,
        tokeira_release: contract_string(&value, "tokeira_release")?,
        minimum_supported_version: contract_u32(&value, "minimum_supported_version")?,
        target_version: contract_u32(&value, "target_version")?,
        maximum_readable_version: contract_u32(&value, "maximum_readable_version")?,
        migration_set_digest: contract_string(&value, "migration_set_digest")?,
        immutable_through_version: contract_u32(&value, "immutable_through_version")?,
    })
}

fn contract_u32(value: &Value, key: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let integer = value
        .get(key)
        .and_then(Value::as_integer)
        .ok_or_else(|| invalid_data(format!("schema contract missing integer {key}")))?;
    u32::try_from(integer)
        .map_err(|_| invalid_data(format!("schema contract {key} is out of range")).into())
}

fn contract_string(value: &Value, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_data(format!("schema contract missing string {key}")).into())
}

fn parse_baseline(path: &Path) -> Result<SchemaBaseline, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let mut lines = content.lines();
    if lines.next() != Some("# tokeira-dsql-schema-baseline-v1") {
        return Err(invalid_data("schema baseline has an unsupported header").into());
    }
    let ceiling_line = lines
        .next()
        .ok_or_else(|| invalid_data("schema baseline is missing its immutable ceiling"))?;
    let (ceiling_key, ceiling_value) = ceiling_line
        .split_once(' ')
        .ok_or_else(|| invalid_data("schema baseline ceiling is malformed"))?;
    if ceiling_key != "immutable_through_version" {
        return Err(invalid_data("schema baseline ceiling key is malformed").into());
    }

    let migrations = lines
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| parse_locked_migration(index + 3, line))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SchemaBaseline {
        format_version: 1,
        immutable_through_version: ceiling_value.parse()?,
        migrations,
    })
}

fn parse_locked_migration(
    line_number: usize,
    line: &str,
) -> Result<LockedMigration, Box<dyn std::error::Error>> {
    let mut fields = line.split_ascii_whitespace();
    let version = fields
        .next()
        .ok_or_else(|| invalid_data(format!("baseline line {line_number} is missing version")))?
        .parse()?;
    let name = fields
        .next()
        .ok_or_else(|| invalid_data(format!("baseline line {line_number} is missing name")))?
        .to_owned();
    let checksum = fields
        .next()
        .ok_or_else(|| invalid_data(format!("baseline line {line_number} is missing checksum")))?
        .to_owned();
    if fields.next().is_some() {
        return Err(invalid_data(format!(
            "baseline line {line_number} has unexpected trailing fields"
        ))
        .into());
    }
    Ok(LockedMigration {
        version,
        name,
        checksum,
    })
}

fn render(
    migrations: &[Migration],
    contract: &SchemaContract,
    prefix_digests: &[(u32, String)],
) -> String {
    let mut output = String::from("static EMBEDDED_MIGRATIONS: &[EmbeddedMigration] = &[\n");
    for migration in migrations {
        output.push_str("    EmbeddedMigration {\n");
        output.push_str(&format!("        version: {},\n", migration.version));
        output.push_str(&format!("        name: {:?},\n", migration.name));
        output.push_str(&format!(
            "        path: {:?},\n",
            migration.path.display().to_string()
        ));
        output.push_str(&format!("        checksum: {:?},\n", migration.checksum));
        output.push_str(&format!("        sql: {:?},\n", migration.sql));
        output.push_str("    },\n");
    }
    output.push_str("];\n\n");
    output.push_str(
        "static EMBEDDED_SCHEMA_CONTRACT: EmbeddedSchemaContract = EmbeddedSchemaContract {\n",
    );
    output.push_str(&format!(
        "    format_version: {},\n",
        contract.format_version
    ));
    output.push_str(&format!(
        "    tokeira_release: {:?},\n",
        contract.tokeira_release
    ));
    output.push_str(&format!(
        "    minimum_supported_version: {},\n",
        contract.minimum_supported_version
    ));
    output.push_str(&format!(
        "    target_version: {},\n",
        contract.target_version
    ));
    output.push_str(&format!(
        "    maximum_readable_version: {},\n",
        contract.maximum_readable_version
    ));
    output.push_str(&format!(
        "    migration_set_digest: {:?},\n",
        contract.migration_set_digest
    ));
    output.push_str(&format!(
        "    immutable_through_version: {},\n",
        contract.immutable_through_version
    ));
    output.push_str("};\n\n");
    output.push_str(
        "static EMBEDDED_MIGRATION_PREFIX_DIGESTS: &[EmbeddedMigrationPrefixDigest] = &[\n",
    );
    for (version, digest) in prefix_digests {
        output.push_str("    EmbeddedMigrationPrefixDigest {\n");
        output.push_str(&format!("        version: {version},\n"));
        output.push_str(&format!("        digest: {digest:?},\n"));
        output.push_str("    },\n");
    }
    output.push_str("];\n");
    output
}
