use anyhow::Result;
use async_trait::async_trait;
use tokeira_types::{NamespaceId, ProjectionCursor, RunKey, SearchAttrValue};

use crate::types::{
    AttrDescriptor, AttrId, CompiledFilter, CountResult, ExecutionRow, GroupByField, ListResult,
    PageBounds, RollupDelta, RollupDimension, SearchAttrType, SortOrder,
};

#[async_trait]
pub trait VisibilityStore: Send + Sync {
    async fn upsert_execution(&self, row: &ExecutionRow) -> Result<()>;
    async fn delete_execution(&self, run_key: RunKey) -> Result<()>;
    async fn upsert_search_attr_index(
        &self,
        run_key: RunKey,
        namespace_id: NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
        value: &SearchAttrValue,
    ) -> Result<()>;
    async fn remove_search_attr_index(
        &self,
        run_key: RunKey,
        namespace_id: NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
    ) -> Result<()>;
    async fn accumulate_rollup(&self, entries: &[RollupDelta]) -> Result<()>;

    async fn list_executions(
        &self,
        namespace_id: NamespaceId,
        filter: &CompiledFilter,
        sort: SortOrder,
        page: &PageBounds,
    ) -> Result<ListResult>;
    async fn count_executions(
        &self,
        namespace_id: NamespaceId,
        filter: &CompiledFilter,
        group_by: Option<GroupByField>,
    ) -> Result<CountResult>;
    async fn count_from_rollup(
        &self,
        namespace_id: NamespaceId,
        dimension: RollupDimension,
    ) -> Result<CountResult>;

    async fn load_checkpoint(&self, sink_id: &str) -> Result<Option<ProjectionCursor>>;
    async fn save_checkpoint(&self, sink_id: &str, cursor: &ProjectionCursor) -> Result<()>;

    async fn resolve_attr(
        &self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<AttrDescriptor>>;
    async fn register_attr(
        &self,
        namespace_id: NamespaceId,
        name: String,
        attr_type: SearchAttrType,
    ) -> Result<AttrId>;

    async fn get_row(&self, run_key: RunKey) -> Option<ExecutionRow>;
}
