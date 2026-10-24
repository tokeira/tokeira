//! The bounded visibility apply against an operator-owned ephemeral Aurora DSQL cluster
//! (`bounded-visibility-apply`, Live Tests). No connection occurs unless the dedicated
//! test URL is provided. Each test applies into a namespace of its own and deletes what
//! it wrote, a few hundred rows a statement.

use std::path::Path;

use anyhow::{Result, anyhow};
use sqlx::PgPool;
use tokeira_storage::{
    ProjectionRecord,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig},
};
use tokeira_types::{ExecutionStatus, NamespaceId, RunKey, SearchAttrValue, SearchAttributes};
use uuid::Uuid;

use super::tests::live_image;
use crate::{
    ApplyOutcome, AttrDescriptor, CompareOp, CompiledFilter, DsqlVisibilityStore, FieldRef,
    FilterExpr, FilterValue, PageBounds, ProjectionSink, SearchAttrType, SortOrder,
    VisibilityStore,
};

/// How a test deletes its namespace's rows from one table, in statements under DSQL's
/// row limit: run by run, and attribute by attribute where a run holds many rows.
struct NamespaceTable {
    /// The namespace's runs in the table, when its rows go run by run.
    runs: Option<&'static str>,
    /// A run's attributes in the table, when its rows go attribute by attribute.
    attrs: Option<&'static str>,
    /// The delete, bound to the namespace, then to the run and attribute as above.
    delete: &'static str,
}

/// The tables an image's attributes and its row live in, cleared after each test.
const NAMESPACE_TABLES: [NamespaceTable; 6] = [
    NamespaceTable {
        runs: Some(
            "SELECT DISTINCT run_key FROM execution_visibility_attr_index WHERE namespace_id = $1",
        ),
        attrs: Some(
            "SELECT DISTINCT attr_id FROM execution_visibility_attr_index \
             WHERE namespace_id = $1 AND run_key = $2",
        ),
        delete: "DELETE FROM execution_visibility_attr_index \
                 WHERE namespace_id = $1 AND run_key = $2 AND attr_id = $3",
    },
    NamespaceTable {
        runs: Some(
            "SELECT DISTINCT run_key FROM execution_visibility_attr_cell WHERE namespace_id = $1",
        ),
        attrs: Some(
            "SELECT DISTINCT attr_id FROM execution_visibility_attr_cell \
             WHERE namespace_id = $1 AND run_key = $2",
        ),
        delete: "DELETE FROM execution_visibility_attr_cell \
                 WHERE namespace_id = $1 AND run_key = $2 AND attr_id = $3",
    },
    NamespaceTable {
        runs: Some(
            "SELECT DISTINCT run_key FROM execution_visibility_attr_value WHERE namespace_id = $1",
        ),
        attrs: None,
        delete: "DELETE FROM execution_visibility_attr_value WHERE namespace_id = $1 AND run_key = $2",
    },
    NamespaceTable {
        runs: Some(
            "SELECT DISTINCT run_key FROM execution_visibility_current WHERE namespace_id = $1",
        ),
        attrs: None,
        delete: "DELETE FROM execution_visibility_current WHERE namespace_id = $1 AND run_key = $2",
    },
    NamespaceTable {
        runs: None,
        attrs: None,
        delete: "DELETE FROM execution_visibility_rollup WHERE namespace_id = $1",
    },
    NamespaceTable {
        runs: None,
        attrs: None,
        delete: "DELETE FROM sa_registry WHERE namespace_id = $1",
    },
];

/// A registered attribute: its name and descriptor.
pub(super) type Attribute = (String, AttrDescriptor);

/// A run's attributes with their values.
pub(super) type Attributes = Vec<(Attribute, SearchAttrValue)>;

pub(super) struct Live {
    pub(super) pool: PgPool,
    store: DsqlStore,
    pub(super) visibility: DsqlVisibilityStore,
    pub(super) namespace_id: NamespaceId,
}

impl Live {
    pub(super) async fn connect() -> Result<Option<Self>> {
        let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        let pool = PgPool::connect(&url).await?;
        let config = DsqlPoolConfig {
            // nextest starts this test in its crate directory, not the workspace root.
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            ..DsqlPoolConfig::default()
        };
        let store = DsqlStore::from_database_url_for_tests(url, config).await?;
        store.migration_runner().apply(&pool).await?;
        let visibility = DsqlVisibilityStore::new(store.connection_director_arc());
        Ok(Some(Self {
            pool,
            store,
            visibility,
            namespace_id: NamespaceId(Uuid::new_v4()),
        }))
    }

    /// Register `count` `Text` attributes named `{prefix}{n}` in the test namespace.
    pub(super) async fn register_text(&self, prefix: &str, count: usize) -> Result<Vec<Attribute>> {
        let mut attrs = Vec::new();
        for n in 1..=count {
            let name = format!("{prefix}{n}");
            let attr_id = self
                .visibility
                .register_attr(self.namespace_id, name.clone(), SearchAttrType::Text)
                .await?;
            attrs.push((
                name,
                AttrDescriptor {
                    attr_id,
                    attr_type: SearchAttrType::Text,
                },
            ));
        }
        Ok(attrs)
    }

    /// A running workflow's image at `seq`, carrying `attributes`.
    pub(super) fn image(
        &self,
        run_key: RunKey,
        seq: u64,
        attributes: Vec<(String, SearchAttrValue)>,
    ) -> ProjectionRecord {
        let mut record = live_image(run_key, self.namespace_id, seq, ExecutionStatus::Running);
        record.context.search_attributes = SearchAttributes(attributes.into_iter().collect());
        record
    }

    /// The runs a filter `name = value` on a `Text` attribute matches.
    pub(super) async fn matching(&self, attr: &Attribute, value: &str) -> Result<Vec<RunKey>> {
        let filter = CompiledFilter {
            archetype: None,
            expr: Some(FilterExpr::Compare {
                field: FieldRef::Custom {
                    name: attr.0.clone(),
                    attr_id: attr.1.attr_id,
                    attr_type: attr.1.attr_type,
                },
                op: CompareOp::Eq,
                value: FilterValue::String(value.to_owned()),
            }),
        };
        Ok(self
            .visibility
            .list_executions(
                self.namespace_id,
                &filter,
                SortOrder::Default,
                &PageBounds {
                    limit: 100,
                    after: None,
                },
            )
            .await?
            .rows
            .into_iter()
            .map(|row| row.run_key)
            .collect())
    }

    /// Delete everything the test namespace wrote, run by run and attribute by attribute,
    /// so no statement passes DSQL's row limit.
    pub(super) async fn finish(self) -> Result<()> {
        for table in &NAMESPACE_TABLES {
            let Some(runs) = table.runs else {
                sqlx::query(table.delete)
                    .bind(self.namespace_id.0)
                    .execute(&self.pool)
                    .await?;
                continue;
            };
            let run_keys: Vec<Uuid> = sqlx::query_scalar(runs)
                .bind(self.namespace_id.0)
                .fetch_all(&self.pool)
                .await?;
            for run_key in run_keys {
                let Some(attrs) = table.attrs else {
                    sqlx::query(table.delete)
                        .bind(self.namespace_id.0)
                        .bind(run_key)
                        .execute(&self.pool)
                        .await?;
                    continue;
                };
                let attr_ids: Vec<i64> = sqlx::query_scalar(attrs)
                    .bind(self.namespace_id.0)
                    .bind(run_key)
                    .fetch_all(&self.pool)
                    .await?;
                for attr_id in attr_ids {
                    sqlx::query(table.delete)
                        .bind(self.namespace_id.0)
                        .bind(run_key)
                        .bind(attr_id)
                        .execute(&self.pool)
                        .await?;
                }
            }
        }
        self.pool.close().await;
        self.store.shutdown().await
    }
}

/// `count` distinct tokens of one and then two lowercase letters or digits, the
/// shortest `text_search_tokens` keeps.
pub(super) fn short_tokens(count: usize) -> Vec<String> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let singles = ALPHABET.iter().map(|&c| char::from(c).to_string());
    let pairs = ALPHABET.iter().flat_map(|&a| {
        ALPHABET
            .iter()
            .map(move |&b| format!("{}{}", char::from(a), char::from(b)))
    });
    singles.chain(pairs).take(count).collect()
}

/// `count` distinct three-letter tokens, none of them in [`short_tokens`].
pub(super) fn three_letter_tokens(count: usize) -> Vec<String> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    ALPHABET
        .iter()
        .flat_map(|&a| {
            ALPHABET.iter().flat_map(move |&b| {
                ALPHABET
                    .iter()
                    .map(move |&c| format!("{}{}{}", char::from(a), char::from(b), char::from(c)))
            })
        })
        .take(count)
        .collect()
}

/// A `Text` value of `tokens`, checked against the 2 KiB value limit: its JSON payload
/// is the text with two quotes.
pub(super) fn text_value(tokens: &[String]) -> Result<SearchAttrValue> {
    let text = tokens.join(" ");
    anyhow::ensure!(
        text.len() + 2 <= 2 * 1024,
        "a {}-byte Text value passes the 2 KiB value limit",
        text.len()
    );
    Ok(SearchAttrValue::Text(text))
}

// Feature: bounded-visibility-apply, exploration 1: a first apply past 3,000 rows.
// Five `Text` attributes, each a 2 KiB value of 694 distinct tokens, within the
// search-attribute limits: 3,470 index rows. The run appears in List.
#[tokio::test]
async fn dsql_an_image_past_3000_index_rows_applies() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let attrs = live.register_text("BoundedFirstText", 5).await?;
    let tokens = short_tokens(694);
    let value = text_value(&tokens)?;
    let run_key = RunKey(Uuid::new_v4());
    let image = live.image(
        run_key,
        1,
        attrs
            .iter()
            .map(|(name, _)| (name.clone(), value.clone()))
            .collect(),
    );
    let applied = live.visibility.apply(&image, 0).await;
    let first = match &applied {
        Ok(_) => live.matching(&attrs[0], "a").await?,
        Err(_) => Vec::new(),
    };
    let last = match &applied {
        Ok(_) => live.matching(&attrs[4], &tokens[693]).await?,
        Err(_) => Vec::new(),
    };
    live.finish().await?;
    let outcome = applied.map_err(|error| anyhow!("the apply failed: {error:#}"))?;
    assert_eq!(outcome, ApplyOutcome::Advanced);
    assert_eq!(
        first,
        vec![run_key],
        "a token of the first attribute matches"
    );
    assert_eq!(
        last,
        vec![run_key],
        "the last token of the last attribute matches"
    );
    Ok(())
}

// Feature: bounded-visibility-apply, exploration 2: an update past 3,000 rows.
// Three attributes of 694 two-character tokens apply. The next image replaces them
// with 511 three-letter tokens each: today, 2,082 deletes and 1,533 inserts.
#[tokio::test]
async fn dsql_an_update_past_3000_rows_applies() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let attrs = live.register_text("BoundedUpdateText", 3).await?;
    let first_tokens = short_tokens(694);
    let second_tokens = three_letter_tokens(511);
    let first_value = text_value(&first_tokens)?;
    let second_value = text_value(&second_tokens)?;
    let run_key = RunKey(Uuid::new_v4());
    let with = |value: &SearchAttrValue| {
        attrs
            .iter()
            .map(|(name, _)| (name.clone(), value.clone()))
            .collect::<Vec<_>>()
    };
    let first = live
        .visibility
        .apply(&live.image(run_key, 1, with(&first_value)), 0)
        .await;
    let second = match &first {
        Ok(_) => Some(
            live.visibility
                .apply(&live.image(run_key, 2, with(&second_value)), 0)
                .await,
        ),
        Err(_) => None,
    };
    let new_token = live.matching(&attrs[1], &second_tokens[0]).await?;
    let old_token = live.matching(&attrs[1], &first_tokens[100]).await?;
    live.finish().await?;
    let first = first.map_err(|error| anyhow!("the first image failed: {error:#}"))?;
    assert_eq!(first, ApplyOutcome::Advanced);
    let second = second
        .ok_or_else(|| anyhow!("the second image wasn't applied"))?
        .map_err(|error| anyhow!("the second image failed: {error:#}"))?;
    assert_eq!(second, ApplyOutcome::Advanced);
    assert_eq!(
        new_token,
        vec![run_key],
        "a token of the second image matches"
    );
    assert!(
        old_token.is_empty(),
        "a token only the first image held matches nothing"
    );
    Ok(())
}

#[test]
fn the_exploration_values_are_within_the_limits() -> Result<()> {
    let short = short_tokens(694);
    let three = three_letter_tokens(511);
    assert_eq!(short.len(), 694);
    assert_eq!(three.len(), 511);
    assert!(three.iter().all(|token| !short.contains(token)));
    let SearchAttrValue::Text(short_text) = text_value(&short)? else {
        unreachable!("a Text value")
    };
    let SearchAttrValue::Text(three_text) = text_value(&three)? else {
        unreachable!("a Text value")
    };
    assert_eq!(crate::types::text_search_tokens(&short_text).len(), 694);
    assert_eq!(crate::types::text_search_tokens(&three_text).len(), 511);
    // Five such attributes and their keys fit in the 40 KiB total.
    assert!(5 * (short_text.len() + 64) < 40 * 1024);
    Ok(())
}

/// An image ready for the apply's steps, as `ProjectionSink::apply` prepares it.
pub(super) struct Prepared {
    pub(super) record: ProjectionRecord,
    pub(super) row: crate::ExecutionRow,
    pub(super) memo: Vec<u8>,
    pub(super) image: super::ImageAttributes,
    pub(super) version: super::Version,
}

impl Live {
    /// `attributes` normalized and built into cells and values, with the run's row.
    pub(super) fn prepared(
        &self,
        run_key: RunKey,
        seq: u64,
        attributes: &[(Attribute, SearchAttrValue)],
    ) -> Result<Prepared> {
        let record = self.image(
            run_key,
            seq,
            attributes
                .iter()
                .map(|((name, _), value)| (name.clone(), value.clone()))
                .collect(),
        );
        let mut row = crate::ExecutionRow::from_projection_record(&record);
        let mut resolved = Vec::new();
        for ((name, attr), value) in attributes {
            let normalized = crate::normalize_search_attr_value(attr.attr_type, value)?;
            row.search_attributes
                .0
                .insert(name.clone(), normalized.clone());
            resolved.push((attr.clone(), normalized));
        }
        Ok(Prepared {
            memo: tokeira_storage::dsql::codec::encode(&row.memo)?,
            image: super::ImageAttributes::build(&resolved, false)?,
            version: super::Version::of_row(&row),
            record,
            row,
        })
    }

    /// The run's cells and values below `version`.
    pub(super) async fn below(
        &self,
        run_key: RunKey,
        version: super::Version,
    ) -> Result<(i64, i64)> {
        let mut connection = self.pool.acquire().await?;
        let counts =
            super::count_below(&mut connection, self.namespace_id, run_key, version).await?;
        Ok((i64::try_from(counts.cells)?, i64::try_from(counts.values)?))
    }

    /// The run's cells at `version`.
    pub(super) async fn cells_at(&self, run_key: RunKey, version: super::Version) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM execution_visibility_attr_cell WHERE namespace_id = $1 \
             AND run_key = $2 AND authority_epoch = $3 AND source_transition_seq = $4",
        )
        .bind(self.namespace_id.0)
        .bind(run_key.0)
        .bind(version.epoch)
        .bind(version.seq)
        .fetch_one(&self.pool)
        .await?)
    }

    /// Which of `probes` match the run: each probe is an attribute and a token.
    pub(super) async fn matches(&self, probes: &[(&Attribute, &str)]) -> Result<Vec<bool>> {
        let mut matched = Vec::new();
        for (attr, value) in probes {
            matched.push(!self.matching(attr, value).await?.is_empty());
        }
        Ok(matched)
    }

    /// The run's row at `seq` with no attributes, then its old index rows as V052's
    /// apply wrote them: a row per cell, each carrying the attribute's whole value.
    pub(super) async fn seed_legacy_run(
        &self,
        run_key: RunKey,
        seq: u64,
        attributes: &[(Attribute, SearchAttrValue)],
    ) -> Result<()> {
        self.visibility
            .apply(&self.image(run_key, seq, Vec::new()), 0)
            .await?;
        for ((_, attr), value) in attributes {
            let attr_id = i64::try_from(attr.attr_id.0)?;
            let value_data = tokeira_storage::dsql::codec::encode(value)?;
            let cells = super::stored_attr_cells(attr.attr_type, value, false)?;
            for chunk in cells.chunks(100) {
                let mut insert = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                    "INSERT INTO execution_visibility_attr_index (namespace_id, run_key, attr_id, \
                     attr_type, value_discriminator, keyword_value, int_value, bool_value, \
                     double_value, datetime_value, text_token, value_data) ",
                );
                insert.push_values(chunk, |mut row, cell| {
                    row.push_bind(self.namespace_id.0)
                        .push_bind(run_key.0)
                        .push_bind(attr_id)
                        .push_bind(attr.attr_type.to_db_smallint())
                        .push_bind(cell.discriminator.clone())
                        .push_bind(cell.keyword_value.clone())
                        .push_bind(cell.int_value)
                        .push_bind(cell.bool_value)
                        .push_bind(cell.double_value)
                        .push_bind(cell.datetime_value)
                        .push_bind(cell.text_token.clone())
                        .push_bind(value_data.clone());
                });
                insert.push(" ON CONFLICT DO NOTHING");
                insert.build().execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    /// The run's values at its row's version, decoded, by attribute id.
    pub(super) async fn values_at_row(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<(i64, SearchAttrValue)>> {
        let rows: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT value.attr_id, value.value_data FROM execution_visibility_current v \
             JOIN execution_visibility_attr_value value ON value.namespace_id = v.namespace_id \
                 AND value.run_key = v.run_key AND value.authority_epoch = v.authority_epoch \
                 AND value.source_transition_seq = v.source_transition_seq \
             WHERE v.namespace_id = $1 AND v.archetype_id = 0 AND v.run_key = $2 \
             ORDER BY value.attr_id",
        )
        .bind(self.namespace_id.0)
        .bind(run_key.0)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(attr_id, data)| Ok((attr_id, tokeira_storage::dsql::codec::decode(&data)?)))
            .collect()
    }

    /// The copy's stored position and whether it finished, for `name`.
    pub(super) async fn copy_progress(
        &self,
        name: &str,
    ) -> Result<(Option<super::CopyPosition>, bool)> {
        let (position, completed_at): (Option<Vec<u8>>, Option<time::OffsetDateTime>) =
            sqlx::query_as(
                "SELECT position_data, completed_at FROM execution_visibility_attr_copy \
                 WHERE name = $1",
            )
            .bind(name)
            .fetch_one(&self.pool)
            .await?;
        Ok((
            position
                .as_deref()
                .map(tokeira_storage::dsql::codec::decode)
                .transpose()?,
            completed_at.is_some(),
        ))
    }

    /// A copy scope of this test's own: its namespace, under a fresh name.
    pub(super) fn copy_scope(&self) -> super::attributes::CopyScope {
        super::attributes::CopyScope {
            name: format!("test-copy-{}", Uuid::new_v4()),
            namespace: Some(self.namespace_id),
        }
    }

    /// Delete this test's copy progress row.
    pub(super) async fn forget_copy(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM execution_visibility_attr_copy WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// `attrs` each holding `value`.
fn each(attrs: &[Attribute], value: &SearchAttrValue) -> Attributes {
    attrs
        .iter()
        .map(|attr| (attr.clone(), value.clone()))
        .collect()
}

// Feature: bounded-visibility-apply, live test 2: two images of one run, applied in
// interleaved steps as two projectors of one partition would, each past one
// transaction. At every step a filter matches the image at the run's version only.
#[tokio::test]
async fn dsql_interleaved_applies_match_one_image_at_every_step() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let attrs = live.register_text("BoundedInterleaveText", 5).await?;
    let first_tokens = short_tokens(694);
    let second_tokens = three_letter_tokens(511);
    let run_key = RunKey(Uuid::new_v4());
    let first = live.prepared(run_key, 1, &each(&attrs, &text_value(&first_tokens)?))?;
    let second = live.prepared(run_key, 2, &each(&attrs, &text_value(&second_tokens)?))?;
    let probes = [
        (&attrs[2], first_tokens[400].as_str()),
        (&attrs[2], second_tokens[300].as_str()),
    ];
    let mut seen = Vec::new();

    let batches = super::cell_batches(second.image.cells.len());
    assert!(
        batches.len() > 1,
        "the second image needs more than one cell batch"
    );
    live.visibility
        .write_cells(
            &second.row,
            second.version,
            &second.image.cells[batches[0].clone()],
        )
        .await?;
    seen.push(("second's first batch", live.matches(&probes).await?));
    let applied = live.visibility.apply(&first.record, 0).await?;
    seen.push(("first applied", live.matches(&probes).await?));
    for batch in &batches[1..] {
        live.visibility
            .write_cells(
                &second.row,
                second.version,
                &second.image.cells[batch.clone()],
            )
            .await?;
    }
    seen.push(("second's cells written", live.matches(&probes).await?));
    let flipped = live
        .visibility
        .flip(
            &second.record,
            &second.row,
            &second.memo,
            &second.image,
            second.version,
        )
        .await?;
    seen.push(("second flipped", live.matches(&probes).await?));
    let deleted = live
        .visibility
        .cleanup_below(live.namespace_id, run_key, second.version)
        .await?;
    seen.push(("second cleaned up", live.matches(&probes).await?));
    let below = live.below(run_key, second.version).await?;
    let current = live.cells_at(run_key, second.version).await?;
    live.finish().await?;

    assert_eq!(applied, ApplyOutcome::Advanced);
    assert_eq!(flipped, ApplyOutcome::Advanced);
    assert_eq!(
        seen,
        vec![
            ("second's first batch", vec![false, false]),
            ("first applied", vec![true, false]),
            ("second's cells written", vec![true, false]),
            ("second flipped", vec![false, true]),
            ("second cleaned up", vec![false, true]),
        ],
        "a filter matches the image at the run's version, never the other"
    );
    assert_eq!(
        deleted,
        u64::try_from(first.image.cells.len())?,
        "cleanup deleted the first image's cells; the flip deleted its values"
    );
    assert_eq!(below, (0, 0), "nothing is left below the version");
    assert_eq!(current, i64::try_from(second.image.cells.len())?);
    Ok(())
}

// Feature: bounded-visibility-apply, live test 3: orphans of an image that never flips
// match nothing, and cleanup leaves no row below the run's version.
#[tokio::test]
async fn dsql_cleanup_leaves_nothing_below_and_orphans_match_nothing() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let attrs = live.register_text("BoundedOrphanText", 5).await?;
    let run_key = RunKey(Uuid::new_v4());
    let one = |token: &str| text_value(&[token.to_owned()]);
    let first = live.prepared(run_key, 1, &[(attrs[0].clone(), one("first")?)])?;
    let orphan_below = live.prepared(run_key, 2, &[(attrs[0].clone(), one("lower")?)])?;
    let third = live.prepared(run_key, 3, &[(attrs[0].clone(), one("third")?)])?;
    let orphan_above = live.prepared(run_key, 5, &[(attrs[0].clone(), one("upper")?)])?;
    let last_tokens = short_tokens(694);
    let last = live.prepared(run_key, 6, &each(&attrs, &text_value(&last_tokens)?))?;
    let probes = [
        (&attrs[0], "first"),
        (&attrs[0], "lower"),
        (&attrs[0], "third"),
        (&attrs[0], "upper"),
        (&attrs[0], last_tokens[10].as_str()),
    ];
    let mut seen = Vec::new();

    live.visibility.apply(&first.record, 0).await?;
    // Two interrupted applies: one below the image that follows, one above it.
    for orphan in [&orphan_below, &orphan_above] {
        live.visibility
            .write_cells(&orphan.row, orphan.version, &orphan.image.cells)
            .await?;
    }
    seen.push(("orphans written", live.matches(&probes).await?));
    live.visibility.apply(&third.record, 0).await?;
    seen.push(("third applied", live.matches(&probes).await?));
    let below_third = live.below(run_key, third.version).await?;
    let orphan_left = live.cells_at(run_key, orphan_above.version).await?;
    live.visibility.apply(&last.record, 0).await?;
    seen.push(("last applied", live.matches(&probes).await?));
    let below_last = live.below(run_key, last.version).await?;
    live.finish().await?;

    assert_eq!(
        seen,
        vec![
            ("orphans written", vec![true, false, false, false, false]),
            ("third applied", vec![false, false, true, false, false]),
            ("last applied", vec![false, false, false, false, true]),
        ]
    );
    assert_eq!(
        below_third,
        (0, 0),
        "the third image's apply left nothing below it"
    );
    assert_eq!(
        orphan_left, 1,
        "an orphan above the version waits for a newer image"
    );
    assert_eq!(below_last, (0, 0), "the newer image's cleanup removed it");
    Ok(())
}

// Feature: bounded-visibility-apply, live test 2: a staged apply interrupted before its
// flip leaves the previous image matching, and its retry applies the image whole.
#[tokio::test]
async fn dsql_an_interrupted_staged_apply_keeps_the_previous_image() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let attrs = live.register_text("BoundedInterruptText", 5).await?;
    let wide = short_tokens(694);
    let run_key = RunKey(Uuid::new_v4());
    let small = live.prepared(
        run_key,
        1,
        &[(attrs[0].clone(), text_value(&["before".to_owned()])?)],
    )?;
    let large = live.prepared(run_key, 2, &each(&attrs, &text_value(&wide)?))?;
    let probes = [(&attrs[0], "before"), (&attrs[0], wide[200].as_str())];
    live.visibility.apply(&small.record, 0).await?;
    super::apply_faults::arm(super::apply_faults::Fault::CellBatch(run_key));
    let interrupted = live.visibility.apply(&large.record, 0).await.is_err();
    let after_interruption = live.matches(&probes).await?;
    let retried = live.visibility.apply(&large.record, 0).await?;
    let after_retry = live.matches(&probes).await?;
    let below = live.below(run_key, large.version).await?;
    live.finish().await?;
    assert!(interrupted, "the injected batch failure fails the apply");
    assert_eq!(
        after_interruption,
        vec![true, false],
        "the run still matches its previous image, and none of the interrupted one"
    );
    assert_eq!(retried, ApplyOutcome::Advanced);
    assert_eq!(after_retry, vec![false, true]);
    assert_eq!(below, (0, 0));
    Ok(())
}

/// The old index of a few runs in the test namespace, one of them past the test's
/// copy budget, and what each attribute's token should match after the copy.
async fn seed_copy_corpus(live: &Live) -> Result<(Vec<Attribute>, Vec<(RunKey, Attributes)>)> {
    let attrs = live.register_text("BoundedCopyText", 3).await?;
    let text =
        |tokens: &[&str]| text_value(&tokens.iter().map(|t| (*t).to_owned()).collect::<Vec<_>>());
    let wide: Vec<String> = short_tokens(30);
    let mut runs = Vec::new();
    for (seq, attributes) in [
        (3, vec![(attrs[0].clone(), text(&["alpha", "beta"])?)]),
        (4, vec![]),
        (
            7,
            vec![
                (attrs[0].clone(), text_value(&wide)?),
                (attrs[1].clone(), text_value(&wide[..12])?),
                (attrs[2].clone(), text(&["omega"])?),
            ],
        ),
        (2, vec![(attrs[1].clone(), text(&["gamma"])?)]),
        (9, vec![(attrs[2].clone(), text(&["delta", "epsilon"])?)]),
    ] {
        let run_key = RunKey(Uuid::new_v4());
        live.seed_legacy_run(run_key, seq, &attributes).await?;
        runs.push((run_key, attributes));
    }
    Ok((attrs, runs))
}

/// Check every seeded token matches its run after the copy, and each run's values.
async fn check_copied(live: &Live, runs: &[(RunKey, Attributes)]) -> Result<Vec<String>> {
    let mut problems = Vec::new();
    for (run_key, attributes) in runs {
        for (attr, value) in attributes {
            let SearchAttrValue::Text(text) = value else {
                unreachable!("the corpus holds Text values")
            };
            for token in crate::types::text_search_tokens(text) {
                if !live.matching(attr, &token).await?.contains(run_key) {
                    problems.push(format!("{} {token} doesn't match {}", attr.0, run_key.0));
                }
            }
        }
        let mut expected: Vec<(i64, SearchAttrValue)> = attributes
            .iter()
            .map(|((_, attr), value)| Ok((i64::try_from(attr.attr_id.0)?, value.clone())))
            .collect::<Result<_>>()?;
        expected.sort_by_key(|(attr_id, _)| *attr_id);
        let values = live.values_at_row(*run_key).await?;
        if values != expected {
            problems.push(format!("run {} has values {values:?}", run_key.0));
        }
    }
    Ok(problems)
}

/// Copy limits that split the corpus's wide run across transactions.
fn small_copy_limits(max_transactions: Option<usize>) -> super::attributes::CopyLimits {
    super::attributes::CopyLimits {
        budget: 12,
        run_page: 2,
        max_transactions,
    }
}

// Feature: bounded-visibility-apply, live test 4: a copy stopped part-way resumes from
// its stored position, not the head, and every answer then matches the old index.
#[tokio::test]
async fn dsql_the_copy_resumes_from_its_stored_position() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let (_attrs, runs) = seed_copy_corpus(&live).await?;
    let scope = live.copy_scope();
    let first = live
        .visibility
        .copy_legacy_attributes_with(&scope, small_copy_limits(Some(3)))
        .await?;
    let (stored, finished_early) = live.copy_progress(&scope.name).await?;
    let second = live
        .visibility
        .copy_legacy_attributes_with(&scope, small_copy_limits(None))
        .await?;
    let problems = check_copied(&live, &runs).await?;
    live.forget_copy(&scope.name).await?;
    live.finish().await?;

    assert!(
        !first.completed && !finished_early,
        "the first call stopped part-way: {first:?}"
    );
    assert!(stored.is_some(), "the first call stored its position");
    assert_eq!(
        second.started_from, stored,
        "the second call resumed from the stored position"
    );
    assert!(second.completed, "{second:?}");
    assert!(problems.is_empty(), "{problems:#?}");
    Ok(())
}

// Feature: bounded-visibility-apply, live test 4: two copies at once take turns on the
// progress row; both finish without an error, and the copy is whole.
#[tokio::test]
async fn dsql_two_copies_at_once_both_finish() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let (_attrs, runs) = seed_copy_corpus(&live).await?;
    let scope = live.copy_scope();
    let (left, right) = tokio::join!(
        live.visibility
            .copy_legacy_attributes_with(&scope, small_copy_limits(None)),
        live.visibility
            .copy_legacy_attributes_with(&scope, small_copy_limits(None)),
    );
    let problems = check_copied(&live, &runs).await?;
    live.forget_copy(&scope.name).await?;
    live.finish().await?;
    let (left, right) = (left?, right?);
    assert!(left.completed && right.completed, "{left:?} {right:?}");
    assert!(problems.is_empty(), "{problems:#?}");
    Ok(())
}

// Feature: bounded-visibility-apply, live test 4: a copy that has finished writes
// nothing: its first transaction reads the completion and stops.
#[tokio::test]
async fn dsql_a_finished_copy_writes_nothing() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let (attrs, _runs) = seed_copy_corpus(&live).await?;
    let scope = live.copy_scope();
    let done = live
        .visibility
        .copy_legacy_attributes_with(&scope, small_copy_limits(None))
        .await?;
    // A run seeded after completion stays uncopied: nothing reads the old table again.
    let late = RunKey(Uuid::new_v4());
    live.seed_legacy_run(
        late,
        1,
        &[(attrs[0].clone(), text_value(&["late".to_owned()])?)],
    )
    .await?;
    let again = live
        .visibility
        .copy_legacy_attributes_with(&scope, small_copy_limits(None))
        .await?;
    let late_matches = live.matching(&attrs[0], "late").await?;
    live.forget_copy(&scope.name).await?;
    live.finish().await?;
    assert!(done.completed);
    // A finished copy answers from a plain read of its completion: it commits no
    // transaction, so nodes that start together take no lock on it.
    assert_eq!(
        (
            again.transactions,
            again.cells,
            again.values,
            again.completed
        ),
        (0, 0, 0, true),
        "{again:?}"
    );
    assert!(late_matches.is_empty());
    Ok(())
}

/// `EXPLAIN` lines for `query`.
async fn plan(
    pool: &PgPool,
    query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<Vec<String>> {
    Ok(query
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| sqlx::Row::try_get::<String, _>(&row, 0))
        .collect::<std::result::Result<_, _>>()?)
}

// Feature: bounded-visibility-apply, the live `EXPLAIN` checks: keyword equality uses
// the lookup index, the version-correlated probe and the cleanup's delete use the cell
// table's primary key, the copy's per-run read uses the old table's, and the copy's
// pages of runs seek the current table's.
#[tokio::test]
async fn dsql_cell_statements_use_their_keys() -> Result<()> {
    let Some(live) = Live::connect().await? else {
        return Ok(());
    };
    let namespace = live.namespace_id.0;
    let run_key = Uuid::new_v4();
    let field = |attr_type| FieldRef::Custom {
        name: "attribute".to_owned(),
        attr_id: crate::AttrId(7),
        attr_type,
    };
    let filter_plan = |expr: FilterExpr| {
        let filter = CompiledFilter {
            archetype: None,
            expr: Some(expr),
        };
        let (filter_sql, values, _) =
            super::compile_filter(&filter, 2).expect("the filter compiles");
        let sql = format!(
            "EXPLAIN SELECT run_key FROM execution_visibility_current \
             WHERE namespace_id = $1 AND lifecycle_state <> 2 {filter_sql}"
        );
        (sql, values)
    };
    let mut plans = Vec::new();
    for (label, expr) in [
        (
            "keyword equality",
            FilterExpr::Compare {
                field: field(SearchAttrType::Keyword),
                op: CompareOp::Eq,
                value: FilterValue::String("a".to_owned()),
            },
        ),
        (
            "keyword list inequality",
            FilterExpr::Compare {
                field: field(SearchAttrType::KeywordList),
                op: CompareOp::Ne,
                value: FilterValue::String("a".to_owned()),
            },
        ),
    ] {
        // The compiler's output, with its values bound in order after the namespace.
        let (sql, values) = filter_plan(expr);
        let mut explain = sqlx::QueryBuilder::<sqlx::Postgres>::new(sql);
        let query = super::bind_sql_values(explain.build().bind(namespace), &values);
        plans.push((label, plan(&live.pool, query).await?));
    }
    let boundary = super::attributes::ChunkBoundary {
        authority_epoch: 1,
        source_transition_seq: 2,
        attr_id: 7,
        attr_type: 1,
        value_discriminator: Some("a".to_owned()),
    };
    let mut cleanup = super::attributes::cleanup_delete(
        "EXPLAIN ",
        super::ATTR_CELL_TABLE,
        live.namespace_id,
        RunKey(run_key),
        super::Version { epoch: 3, seq: 4 },
        Some(&boundary),
    );
    plans.push(("cleanup delete", plan(&live.pool, cleanup.build()).await?));
    plans.push((
        "copy per-run read",
        plan(
            &live.pool,
            sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN ")
                .push(super::attributes::LEGACY_CELL_COUNTS)
                .build()
                .bind(namespace)
                .bind(vec![run_key]),
        )
        .await?,
    ));
    plans.push((
        "copy page within an archetype",
        plan(
            &live.pool,
            sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN ")
                .push(super::attributes::RUNS_AFTER_IN_ARCHETYPE)
                .build()
                .bind(namespace)
                .bind(0i64)
                .bind(run_key)
                .bind(256i64),
        )
        .await?,
    ));
    plans.push((
        "copy page after a namespace",
        plan(
            &live.pool,
            sqlx::QueryBuilder::<sqlx::Postgres>::new("EXPLAIN ")
                .push(super::attributes::RUNS_AFTER_NAMESPACE)
                .build()
                .bind(namespace)
                .bind(256i64),
        )
        .await?,
    ));
    live.finish().await?;
    let uses = |label: &str, index: &str| {
        let (_, lines) = plans
            .iter()
            .find(|(name, _)| *name == label)
            .expect("planned");
        assert!(
            lines
                .iter()
                .any(|line| line.contains(&format!("using {index}"))),
            "{label} uses {index}: {lines:#?}"
        );
        lines.clone()
    };
    uses(
        "keyword equality",
        "idx_execution_visibility_attr_cell_lookup",
    );
    // The planner picks which of the cell table's indexes serves the version-correlated
    // anti join; either way it seeks with an index condition and never scans the table.
    let (_, probe) = plans
        .iter()
        .find(|(name, _)| *name == "keyword list inequality")
        .expect("planned");
    assert!(
        probe
            .iter()
            .any(|line| line.contains("on execution_visibility_attr_cell")
                && (line.contains("using idx_execution_visibility_attr_cell_lookup")
                    || line.contains("using execution_visibility_attr_cell_pkey"))),
        "the probe reads the cell table through an index: {probe:#?}"
    );
    assert!(
        !probe
            .iter()
            .any(|line| line.contains("Full Scan") || line.contains("Seq Scan")),
        "the probe never scans a table: {probe:#?}"
    );
    uses("cleanup delete", "execution_visibility_attr_cell_pkey");
    uses("copy per-run read", "execution_visibility_attr_index_pkey");
    let within = uses(
        "copy page within an archetype",
        "execution_visibility_current_pkey",
    );
    assert!(
        within
            .iter()
            .any(|line| line.contains("Index Cond:") && line.contains("run_key >")),
        "the page seeks past the position: {within:#?}"
    );
    let after = uses(
        "copy page after a namespace",
        "execution_visibility_current_pkey",
    );
    assert!(
        after
            .iter()
            .any(|line| line.contains("Index Cond:") && line.contains("namespace_id >")),
        "the page seeks past the namespace: {after:#?}"
    );
    Ok(())
}
