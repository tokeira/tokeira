use tokeira_deploy_engine::{
    DesiredImageRef, Image, ImageContext, ImageSourceType, RuntimeError, WritebackTarget,
};

use crate::{config::ComposeConfig, images::missing_config_error};

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
        ctx.extension::<ComposeConfig>()
            .ok_or_else(missing_config_error::<ComposeConfig>)?;
        Ok(DesiredImageRef {
            repository: "tokeira/tokeirad".to_owned(),
            tag: "latest".to_owned(),
            upstream_ref: None,
        })
    }

    fn writeback_targets(&self, _ctx: &ImageContext) -> Vec<WritebackTarget> {
        vec![WritebackTarget {
            field: "tokeirad.image",
        }]
    }
}

pub fn all() -> Vec<Box<dyn Image>> {
    vec![Box::new(TokeiradImage)]
}
