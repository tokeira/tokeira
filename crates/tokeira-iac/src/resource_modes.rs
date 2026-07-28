//! Convention for resources with resource-specific lifecycle modes.
//!
//! The IaC engine intentionally has no generic `ResourceMode` trait or enum.
//! Some provider resources still need a local lifecycle distinction such as
//! `Managed` versus `Preexisting`. Those resources should define their own mode
//! enum, persist the chosen mode under `ResourceState.properties["mode"]`, and
//! centralize delete eligibility in a pure helper such as:
//!
//! ```ignore
//! fn effective_managed(config_mode: DsqlClusterMode, state_mode: &str) -> bool {
//!     config_mode == DsqlClusterMode::Managed || state_mode == "managed"
//! }
//! ```
//!
//! This convention prevents orphaning resources that were originally created
//! as managed and later reconfigured as preexisting before destroy. The engine
//! still calls `Resource::delete` unconditionally; the resource implementation
//! decides whether that should perform a provider delete or return without
//! side effects.
