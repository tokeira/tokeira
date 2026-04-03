use async_trait::async_trait;

use crate::errors::{EdgeError, EdgeResult};

/// Transport-level routing decision made at the edge.
///
/// This is intentionally *not* a correctness decision. If a request is remote,
/// it means "forward the same request to another edge/runtime node", not "change
/// how the workflow behaves".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTarget {
    Local,
    Remote { target: String },
}

#[async_trait]
pub trait EdgeRouter: Send + Sync + 'static {
    async fn route_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> EdgeResult<RouteTarget>;

    async fn route_task_queue(
        &self,
        namespace: &str,
        task_queue: &str,
    ) -> EdgeResult<RouteTarget>;
}

#[derive(Debug, Default)]
pub struct LocalOnlyRouter;

#[async_trait]
impl EdgeRouter for LocalOnlyRouter {
    async fn route_workflow(
        &self,
        _namespace: &str,
        _workflow_id: &str,
    ) -> EdgeResult<RouteTarget> {
        Ok(RouteTarget::Local)
    }

    async fn route_task_queue(
        &self,
        _namespace: &str,
        _task_queue: &str,
    ) -> EdgeResult<RouteTarget> {
        Ok(RouteTarget::Local)
    }
}

pub fn ensure_local(route: RouteTarget) -> EdgeResult<()> {
    match route {
        RouteTarget::Local => Ok(()),
        RouteTarget::Remote { target } => {
            Err(EdgeError::RemoteRouteUnsupported { target })
        }
    }
}
