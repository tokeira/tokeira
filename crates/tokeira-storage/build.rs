use std::{
    env, fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

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
    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    let migrations = discover_migrations(&migrations_dir)?;
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    fs::write(out_dir.join("migrations_embedded.rs"), render(&migrations))?;
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
            .ok_or_else(|| invalid_data(format!("invalid migration filename {}", path.display())))?;
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
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn render(migrations: &[Migration]) -> String {
    let mut output = String::from(
        "static EMBEDDED_MIGRATIONS: &[EmbeddedMigration] = &[\n",
    );
    for migration in migrations {
        output.push_str("    EmbeddedMigration {\n");
        output.push_str(&format!("        version: {},\n", migration.version));
        output.push_str(&format!("        name: {:?},\n", migration.name));
        output.push_str(&format!("        path: {:?},\n", migration.path.display().to_string()));
        output.push_str(&format!("        checksum: {:?},\n", migration.checksum));
        output.push_str(&format!("        sql: {:?},\n", migration.sql));
        output.push_str("    },\n");
    }
    output.push_str("];\n");
    output
}
