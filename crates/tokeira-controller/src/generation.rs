//! CAS-backed routing generation management.

use std::sync::Arc;

use anyhow::Result;
use tokeira_storage::{BudgetAllocationResult, ControlRepository, GenerationAdvanceResult};
use tokeira_types::{GenerationCounter, IncarnationId};

/// Thin controller wrapper around the storage-level CAS surface.
#[derive(Clone)]
pub struct GenerationManager {
    control_repo: Arc<dyn ControlRepository>,
}

impl std::fmt::Debug for GenerationManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationManager").finish_non_exhaustive()
    }
}

impl GenerationManager {
    pub(crate) fn new(control_repo: Arc<dyn ControlRepository>) -> Self {
        Self { control_repo }
    }

    pub(crate) async fn advance_generation(
        &self,
        expected: GenerationCounter,
    ) -> Result<GenerationAdvanceResult> {
        self.control_repo.advance_generation(expected).await
    }

    pub async fn current_generation(&self) -> Result<GenerationCounter> {
        self.control_repo.current_generation().await
    }

    pub(crate) async fn current_budget_version(&self) -> Result<u64> {
        self.control_repo.current_budget_version().await
    }

    pub(crate) async fn allocate_budget(
        &self,
        expected_version: u64,
        allocator_id: IncarnationId,
        rate_budget: f64,
        capacity_budget: u64,
    ) -> Result<BudgetAllocationResult> {
        self.control_repo
            .allocate_budget(
                expected_version,
                allocator_id.0,
                rate_budget,
                capacity_budget,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokeira_storage::InMemoryStore;

    use super::*;

    #[tokio::test]
    async fn generation_advance_uses_storage_cas() {
        let manager = GenerationManager::new(Arc::new(InMemoryStore::default()));

        assert_eq!(
            manager
                .advance_generation(GenerationCounter::ZERO)
                .await
                .unwrap(),
            GenerationAdvanceResult::Advanced(GenerationCounter(1))
        );
        assert_eq!(
            manager
                .advance_generation(GenerationCounter::ZERO)
                .await
                .unwrap(),
            GenerationAdvanceResult::Conflict(GenerationCounter(1))
        );
    }
}
