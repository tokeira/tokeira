/// Static DSQL DDL validator.
#[derive(Clone, Debug, Default)]
pub struct DdlValidator;

/// One validation problem found in a migration file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationIssue {
    pub file: String,
    pub line: usize,
    pub kind: ValidationKind,
    pub message: String,
}

/// DDL constructs rejected by the schema rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationKind {
    BigSerial,
    CheckConstraint,
    TempTable,
    PlPgsql,
    ForeignKey,
    MissingAsyncKeyword,
    MonotonicPrimaryKey,
}

impl DdlValidator {
    /// Validate SQL against the DSQL-compatible subset used by Tokeira.
    pub fn validate(sql: &str, filename: &str) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();
        let lower = sql.to_ascii_lowercase();

        for (idx, line) in sql.lines().enumerate() {
            let line_no = idx + 1;
            let normalized = line.to_ascii_lowercase();
            if normalized.trim_start().starts_with("--") {
                continue;
            }
            if normalized.contains("bigserial")
                || normalized.contains(" serial ")
                || normalized.contains(" serial,")
                || normalized.contains(" serial)")
            {
                issues.push(issue(
                    filename,
                    line_no,
                    ValidationKind::BigSerial,
                    "DSQL migrations must not use SERIAL or BIGSERIAL",
                ));
            }
            if normalized.contains(" check ") || normalized.contains("check(") {
                issues.push(issue(
                    filename,
                    line_no,
                    ValidationKind::CheckConstraint,
                    "CHECK constraints are intentionally not used",
                ));
            }
            if normalized.contains("create temp table")
                || normalized.contains("create temporary table")
            {
                issues.push(issue(
                    filename,
                    line_no,
                    ValidationKind::TempTable,
                    "temporary tables are not supported in migrations",
                ));
            }
            if normalized.contains("plpgsql") || normalized.contains("create trigger") {
                issues.push(issue(
                    filename,
                    line_no,
                    ValidationKind::PlPgsql,
                    "functions and triggers must stay in application code",
                ));
            }
            if normalized.contains("foreign key") || normalized.contains(" references ") {
                issues.push(issue(
                    filename,
                    line_no,
                    ValidationKind::ForeignKey,
                    "foreign keys are out of scope for the MVP schema",
                ));
            }
            if normalized.contains("create index") && !normalized.contains("create index async") {
                issues.push(issue(
                    filename,
                    line_no,
                    ValidationKind::MissingAsyncKeyword,
                    "secondary indexes must use CREATE INDEX ASYNC",
                ));
            }
        }

        if lower.contains("primary key (id)")
            || lower.contains("primary key(id)")
            || lower.contains("primary key (insertion_seq)")
        {
            issues.push(issue(
                filename,
                1,
                ValidationKind::MonotonicPrimaryKey,
                "hot-write primary keys must not be led by monotonic columns",
            ));
        }

        issues
    }
}

fn issue(
    filename: &str,
    line: usize,
    kind: ValidationKind,
    message: impl Into<String>,
) -> ValidationIssue {
    ValidationIssue {
        file: filename.to_owned(),
        line,
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use proptest::prelude::*;

    use super::{DdlValidator, ValidationKind};

    #[test]
    fn catches_index_without_async() {
        let issues = DdlValidator::validate("CREATE INDEX idx ON t (a);", "V001.sql");
        assert!(
            issues
                .iter()
                .any(|issue| issue.kind == ValidationKind::MissingAsyncKeyword)
        );
    }

    #[test]
    fn allows_async_index() {
        let issues = DdlValidator::validate("CREATE INDEX ASYNC idx ON t (a);", "V001.sql");
        assert!(issues.is_empty());
    }

    #[test]
    fn catches_each_disallowed_construct() {
        let cases = [
            (
                "CREATE TABLE t (id BIGSERIAL PRIMARY KEY);",
                ValidationKind::BigSerial,
            ),
            (
                "CREATE TABLE t (a INT CHECK (a > 0));",
                ValidationKind::CheckConstraint,
            ),
            ("CREATE TEMP TABLE t (a INT);", ValidationKind::TempTable),
            (
                "CREATE TRIGGER trg BEFORE INSERT ON t EXECUTE FUNCTION f();",
                ValidationKind::PlPgsql,
            ),
            (
                "CREATE TABLE t (a INT REFERENCES other(id));",
                ValidationKind::ForeignKey,
            ),
            (
                "CREATE TABLE t (id UUID, PRIMARY KEY (id));",
                ValidationKind::MonotonicPrimaryKey,
            ),
        ];

        for (sql, kind) in cases {
            let issues = DdlValidator::validate(sql, "case.sql");
            assert!(issues.iter().any(|issue| issue.kind == kind));
        }
    }

    #[test]
    fn all_migration_files_pass_validator() {
        let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        for entry in std::fs::read_dir(migrations_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
                continue;
            }
            let sql = std::fs::read_to_string(&path).unwrap();
            let issues = DdlValidator::validate(&sql, &path.display().to_string());
            assert!(issues.is_empty(), "{}: {issues:?}", path.display());
        }
    }

    proptest! {
        #[test]
        fn prohibited_constructs_are_detected(keyword in prop::sample::select(vec![
            "BIGSERIAL",
            "CHECK (a > 0)",
            "CREATE TEMP TABLE",
            "FOREIGN KEY",
            "REFERENCES other(id)",
            "CREATE INDEX idx",
            "plpgsql",
        ])) {
            let sql = format!("CREATE TABLE t (a INT); {keyword}");
            let issues = DdlValidator::validate(&sql, "property.sql");
            prop_assert!(!issues.is_empty());
        }
    }
}
