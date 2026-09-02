//! ECS definition realization and provider execution.
//!
//! This crate owns the ECS kinds, workload derivation, image sources, and the
//! deploy platform used by generated provisioners. Operator lifecycle is
//! definition-bound; there is no in-process deployment adapter.

// Service construction mirrors the broad ECS/ELB API surface, which takes many
// parameters.
#![allow(clippy::too_many_arguments)]

pub mod config;
pub mod execution;
pub mod gates;
pub mod images;
pub mod kinds;
pub mod modules;
pub mod operations;
mod roles;
pub mod services;

pub use config::EcsConfig;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AlbListenerProtocol, DsqlClusterMode, required_vpc_endpoints};

    #[test]
    fn default_config_validates_and_round_trips() {
        let config = EcsConfig::default();
        config.validate().expect("default config is valid");

        let toml = tokeira_config::write_config_toml(&config).expect("serialize config");
        let decoded: EcsConfig = toml::from_str(&toml).expect("deserialize config");

        assert_eq!(decoded, config);
    }

    #[test]
    fn unknown_config_fields_are_rejected() {
        let mut toml = tokeira_config::write_config_toml(&EcsConfig::default()).expect("toml");
        toml.push_str("\nunknown_field = true\n");

        let err = toml::from_str::<EcsConfig>(&toml).expect_err("unknown root field rejected");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn validation_enforces_https_certificate_and_preexisting_dsql() {
        let mut config = EcsConfig::default();
        config.alb.listener_protocol = AlbListenerProtocol::Https;

        assert!(matches!(
            config.validate(),
            Err(crate::config::EcsConfigError::MissingCertificateArn)
        ));

        let mut config = EcsConfig::default();
        config.dsql.mode = DsqlClusterMode::Preexisting;

        assert!(matches!(
            config.validate(),
            Err(crate::config::EcsConfigError::MissingPreexistingDsqlField(
                "dsql.endpoint"
            ))
        ));
    }

    #[test]
    fn required_endpoints_include_ssm_but_not_cloudwatch_logs() {
        let endpoints = required_vpc_endpoints("eu-west-2");

        assert!(endpoints.contains(&"com.amazonaws.eu-west-2.ssm".to_owned()));
        assert!(endpoints.contains(&"com.amazonaws.eu-west-2.ssmmessages".to_owned()));
        assert!(endpoints.contains(&"com.amazonaws.eu-west-2.ec2messages".to_owned()));
        assert!(!endpoints.contains(&"com.amazonaws.eu-west-2.logs".to_owned()));
    }
}
