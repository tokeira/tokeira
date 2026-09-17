//! In-process, archetype-scoped discovery over the derived visibility projection.
//!
//! A handle fixes namespace and archetype independently of query text or cursors.
//! It is not an authorization credential: embedding applications authorize access
//! before obtaining it. Commands and authoritative reads belong to the runtime.

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{
    ArchetypeId, Memo, NamespaceId, RunId, SearchAttributes, TransitionSeq,
    VisibilityLifecycleState,
};

use crate::{
    CompiledFilter, ExecutionRow, MAX_PAGE_SIZE, PageBounds, PageToken, SortOrder, VisibilityStore,
    compile_filter,
};

/// Read-only discovery for one namespace and archetype.
///
/// Results may lag committed component state. Pages and counts are independent
/// reads, not a frozen snapshot; concurrent transitions can move rows between pages.
#[derive(Clone)]
pub struct ComponentVisibility {
    store: Arc<dyn VisibilityStore>,
    namespace_id: NamespaceId,
    archetype_id: ArchetypeId,
}

impl std::fmt::Debug for ComponentVisibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentVisibility")
            .field("namespace_id", &self.namespace_id)
            .field("archetype_id", &self.archetype_id)
            .finish_non_exhaustive()
    }
}

/// Filter and continuation for component discovery.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ComponentQuery {
    /// Existing visibility predicate syntax, resolved against this namespace's
    /// registered attributes. `None` or blank means every execution in scope.
    pub query: Option<String>,
    /// Maximum rows returned, from 1 through 1000; defaults to 100.
    pub page_size: usize,
    /// Opaque continuation from a previous page with the same scope and query.
    pub next_page_token: Option<String>,
}

impl Default for ComponentQuery {
    fn default() -> Self {
        Self {
            query: None,
            page_size: 100,
            next_page_token: None,
        }
    }
}

/// One projected execution, preserving component status independently of lifecycle.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComponentSummary {
    /// Namespace that owns the execution.
    pub namespace_id: NamespaceId,
    /// Registered root archetype.
    pub archetype_id: ArchetypeId,
    /// Component's business identifier.
    pub business_id: String,
    /// Concrete execution instance.
    pub run_id: RunId,
    /// Archetype-defined status; never converted to a workflow status enum.
    pub status_keyword: String,
    /// Whether the execution is open or closed, independently of operational status.
    pub lifecycle_state: VisibilityLifecycleState,
    /// Projected start time; components without one project the Unix epoch.
    pub start_time: OffsetDateTime,
    /// Projected close time, if supplied by the component.
    pub close_time: Option<OffsetDateTime>,
    /// Authority epoch of this projected image.
    pub authority_epoch: i64,
    /// Committed transition from which this image was derived.
    pub source_transition_seq: TransitionSeq,
    /// Complete typed search attributes for this image.
    pub search_attributes: SearchAttributes,
    /// Projected component memo.
    pub memo: Memo,
}

impl From<ExecutionRow> for ComponentSummary {
    fn from(row: ExecutionRow) -> Self {
        Self {
            namespace_id: row.namespace_id,
            archetype_id: row.archetype_id,
            business_id: row.business_id,
            run_id: row.run_id,
            status_keyword: row.status_keyword,
            lifecycle_state: row.lifecycle_state,
            start_time: row.start_time,
            close_time: row.close_time,
            authority_epoch: row.authority_epoch,
            source_transition_seq: row.source_transition_seq,
            search_attributes: row.search_attributes,
            memo: row.memo,
        }
    }
}

/// One bounded page in the shared index's default keyset order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComponentPage {
    /// Projected component executions.
    pub executions: Vec<ComponentSummary>,
    /// Continuation position, or `None` when traversal is complete.
    pub next_page_token: Option<String>,
}

/// Separates caller input errors from projection availability/corruption failures.
#[derive(Debug, Error)]
pub enum ComponentQueryError {
    /// A predicate could not be parsed or typed against registered attributes.
    #[error("invalid component visibility query: {0}")]
    InvalidQuery(String),
    /// A page size is outside the supported bound.
    #[error("component visibility page size {0} is outside 1..=1000")]
    InvalidPageSize(usize),
    /// A continuation is malformed or belongs to a different query/scope.
    #[error("invalid component visibility page token: {0}")]
    InvalidPageToken(String),
    /// A projection read or token serialization failed.
    #[error("component visibility store failed: {0}")]
    Store(#[source] anyhow::Error),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    namespace_id: NamespaceId,
    archetype_id: ArchetypeId,
    query: String,
    position: String,
}

impl ComponentVisibility {
    /// Bind a store to a fixed query scope. Application code normally obtains
    /// this through the engine's registered-root accessor. This does not grant
    /// authorization; callers must already be entitled to read the namespace.
    pub fn new(
        store: Arc<dyn VisibilityStore>,
        namespace_id: NamespaceId,
        archetype_id: ArchetypeId,
    ) -> Self {
        Self {
            store,
            namespace_id,
            archetype_id,
        }
    }

    async fn filter(&self, query: Option<&str>) -> Result<CompiledFilter, ComponentQueryError> {
        let mut filter = compile_filter(query, self.namespace_id, self.store.as_ref())
            .await
            .map_err(|error| {
                if error.is::<crate::filter::AttributeLookupError>() {
                    ComponentQueryError::Store(error)
                } else {
                    ComponentQueryError::InvalidQuery(error.to_string())
                }
            })?;
        // Scope is never accepted from a predicate or cursor. Even a forged
        // continuation only changes position within this handle's fixed scope.
        filter.archetype = Some(self.archetype_id);
        Ok(filter)
    }

    /// List projected executions. Unknown attributes and ill-typed predicates
    /// fail before the row read. Deleted rows are excluded by the store.
    pub async fn list(
        &self,
        request: ComponentQuery,
    ) -> Result<ComponentPage, ComponentQueryError> {
        if !(1..=MAX_PAGE_SIZE).contains(&request.page_size) {
            return Err(ComponentQueryError::InvalidPageSize(request.page_size));
        }
        let query = request.query.as_deref().unwrap_or_default().trim();
        let after = request
            .next_page_token
            .as_deref()
            .map(|token| self.decode_cursor(token, query))
            .transpose()?;
        let filter = self.filter(Some(query)).await?;
        let result = self
            .store
            .list_component_executions(
                self.namespace_id,
                &filter,
                SortOrder::Default,
                &PageBounds {
                    limit: request.page_size,
                    after,
                },
            )
            .await
            .map_err(ComponentQueryError::Store)?;
        let next_page_token = result
            .next_page_token
            .map(|position| {
                let cursor = Cursor {
                    version: 1,
                    namespace_id: self.namespace_id,
                    archetype_id: self.archetype_id,
                    query: query.to_owned(),
                    position: position.encode()?,
                };
                Ok(STANDARD.encode(serde_json::to_vec(&cursor)?))
            })
            .transpose()
            .map_err(ComponentQueryError::Store)?;
        Ok(ComponentPage {
            executions: result.rows.into_iter().map(Into::into).collect(),
            next_page_token,
        })
    }

    /// Count matching projected executions in this handle's scope. This is a
    /// separate read from any list call, so concurrent transitions may change it.
    pub async fn count(&self, query: Option<&str>) -> Result<i64, ComponentQueryError> {
        let filter = self.filter(query).await?;
        self.store
            .count_executions(self.namespace_id, &filter, None)
            .await
            .map(|result| result.total_count)
            .map_err(ComponentQueryError::Store)
    }

    fn decode_cursor(&self, token: &str, query: &str) -> Result<PageToken, ComponentQueryError> {
        let decode = || -> anyhow::Result<PageToken> {
            let cursor: Cursor = serde_json::from_slice(&STANDARD.decode(token)?)?;
            anyhow::ensure!(cursor.version == 1, "unsupported version");
            anyhow::ensure!(
                cursor.namespace_id == self.namespace_id
                    && cursor.archetype_id == self.archetype_id
                    && cursor.query == query,
                "scope or query does not match"
            );
            let position = PageToken::decode(&cursor.position)?;
            anyhow::ensure!(
                position.sort_order == SortOrder::Default,
                "unsupported sort order"
            );
            Ok(position)
        };
        decode().map_err(|error| ComponentQueryError::InvalidPageToken(error.to_string()))
    }
}

#[cfg(test)]
mod tests;
