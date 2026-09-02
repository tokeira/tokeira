//! First-party `tokeirad` image declaration for ECS.
//!
//! One build feeds every Tokeira service. The generic image contract retains
//! the owned configuration fields, while definition-bound image commands use
//! only the desired reference and perform no writeback.

use tokeira_deploy_engine::{
    DesiredImageRef, Image, ImageContext, ImageSourceType, RuntimeError, WritebackTarget,
};

use crate::images::{EcsImageConfig, missing_config_error};

#[derive(Debug)]
pub struct TokeiradImage;

impl Image for TokeiradImage {
    fn name(&self) -> &str {
        "tokeirad"
    }

    fn source_type(&self) -> ImageSourceType {
        ImageSourceType::Build
    }

    fn desired_ref(&self, ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError> {
        let cfg = ctx
            .extension::<EcsImageConfig>()
            .ok_or_else(missing_config_error::<EcsImageConfig>)?;
        Ok(DesiredImageRef {
            repository: format!("{}/tokeirad", cfg.project_name),
            tag: "latest".to_owned(),
            upstream_ref: None,
        })
    }

    fn writeback_targets(&self, _ctx: &ImageContext) -> Vec<WritebackTarget> {
        vec![
            WritebackTarget {
                field: "services.edge_api.image",
            },
            WritebackTarget {
                field: "services.edge_poll.image",
            },
            WritebackTarget {
                field: "services.runtime.image",
            },
            WritebackTarget {
                field: "services.projection.image",
            },
            WritebackTarget {
                field: "services.controller.image",
            },
            WritebackTarget {
                field: "services.autoscaler.image",
            },
            WritebackTarget {
                field: "services.admin.image",
            },
        ]
    }
}

pub(crate) fn all() -> Vec<Box<dyn Image>> {
    vec![Box::new(TokeiradImage)]
}
