use anyhow::Result;
use async_trait::async_trait;
use tokeira_types::{ArchetypeId, NamespaceId, ProjectionCursor, RunKey, SearchAttrValue};

use crate::types::{
    AttrDescriptor, AttrId, CompiledFilter, CountResult, ExecutionRow, GroupByField, ListResult,
    PageBounds, RollupDelta, RollupDimension, SearchAttrType, SortOrder, VisibilitySnapshot,
};

#[async_trait]
pub trait VisibilityStore: Send + Sync {
    /// Apply a complete versioned visibility image.
    ///
    /// Stores that understand the generalized CHASM visibility schema should
    /// persist the snapshot directly and enforce `(authority_epoch,
    /// source_transition_seq)` monotonicity. The default mirrors workflow
    /// snapshots into the legacy workflow row shape so older stores remain
    /// usable while task 23 migrates them.
    async fn upsert_snapshot(&self, snapshot: &VisibilitySnapshot) -> Result<()> {
        if let Some(row) = ExecutionRow::from_workflow_snapshot(snapshot) {
            self.upsert_execution(&row).await?;
        }
        Ok(())
    }
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
        archetype_id: ArchetypeId,
        dimension: RollupDimension,
    ) -> Result<CountResult>;

    /// Load the resume cursor for `partition_id`, or `None` if this partition
    /// has never checkpointed. Keyed by `partition_id` alone (V055
    /// `projection_checkpoint`); the persisted `fanout` rides inside the cursor
    /// so a re-partitioning can be detected by the worker on resume.
    async fn load_checkpoint(&self, partition_id: u32) -> Result<Option<ProjectionCursor>>;
    /// Persist `cursor` as the progress of `cursor.partition_id`.
    async fn save_checkpoint(&self, cursor: &ProjectionCursor) -> Result<()>;

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
