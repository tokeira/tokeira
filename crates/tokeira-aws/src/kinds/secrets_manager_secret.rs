//! Typed author input for a Secrets Manager secret.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::secrets_manager_secret::{
    SecretValue, SecretsManagerSecret as Resource, SecretsManagerSecretConfig,
};

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "SecretsManagerSecret";

/// Authored secret material source, mirroring the resource's
/// [`SecretValue`]. Generated material is produced once at first apply and
/// never recomputed.
/// Generated secret material: the username recorded beside the generated
/// password, and the password length.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedPassword {
    /// Username recorded beside the generated password.
    pub username: String,
    /// Generated password length.
    pub password_length: i32,
}

/// Tuple variants, not struct variants: the definition frontend does not
/// admit struct enum variants across its boundary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum SecretSource {
    /// A static value authored in the definition.
    Static(String),
    /// A generated username/password JSON document.
    GeneratedPasswordJson(GeneratedPassword),
}

/// Reusable author input for one managed secret.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretsManagerSecret {
    /// AWS region.
    pub region: String,
    /// Full secret name (resource id `secret-<name>`).
    pub name: String,
    /// Secret material source.
    pub source: SecretSource,
    /// Recovery window in days; `None` uses the provider default.
    #[serde(default)]
    pub recovery_window_days: Option<i64>,
}

impl Kind<Resource> for SecretsManagerSecret {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let rctx = super::resource_context(&self.region, placement);
        Ok(Resource::new(
            self.name.clone(),
            SecretsManagerSecretConfig {
                value: match &self.source {
                    SecretSource::Static(value) => SecretValue::Static(value.clone()),
                    SecretSource::GeneratedPasswordJson(generated) => {
                        SecretValue::GeneratedPasswordJson {
                            username: generated.username.clone(),
                            password_length: generated.password_length,
                        }
                    }
                },
                recovery_window_days: self.recovery_window_days,
                module: placement.module.clone(),
            },
            &rctx,
        ))
    }
}
