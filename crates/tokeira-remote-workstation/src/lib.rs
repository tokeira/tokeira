//! Remote workstation lifecycle management.
//!
//! This crate owns the workstation-specific concerns:
//! - Module composition: assembles generic AWS resources (from `tokeira-aws`)
//!   into the workstation topology via `tokeira-iac::Module`
//! - Bootstrap rendering: cloud-init script generation
//! - Operational methods: stop, start, ssh, remote-exec, idle-defer
//! - Profile and cost model
//!
//! Generic AWS resource implementations (`IamRole`, `SecurityGroup`,
//! `EbsVolume`, `Ec2Instance`, `IamInstanceProfile`) live in `tokeira-aws`.
//! This crate composes them with workstation-specific configuration.

pub mod bootstrap;
pub mod deployment;
pub mod engine;
pub mod instance;
pub mod module;
