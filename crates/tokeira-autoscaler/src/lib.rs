//! Platform-agnostic autoscaler control loops for Tokeira deployments.
//!
//! # Architecture
//!
//! The autoscaler is decomposed into three independent control loops that run
//! concurrently under a single leader lease:
//!
//! - **Loop A (Replica Scaling):** Adjusts the desired task count for each ECS
//!   service (or equivalent platform unit) based on per-service pressure signals.
//!   Uses consecutive-sample hysteresis to avoid flapping on transient spikes.
//!
//! - **Loop B (Runtime Scale-Out):** Adds runtime hosts (ASG capacity or
//!   equivalent) when broad saturation is detected across the cluster. Only
//!   fires when DSQL connection headroom permits the additional hosts — this
//!   prevents scaling into a connection budget wall.
//!
//! - **Loop C (Runtime Retirement):** Drains and terminates individual runtime
//!   hosts through a multi-phase state machine. Separating retirement from
//!   scale-in avoids partial-drain races where a host is terminated before its
//!   workload has fully migrated.
//!
//! The loops are separated because they operate on different time scales and
//! failure domains: replica scaling reacts in seconds, scale-out in tens of
//! seconds, and retirement over minutes. Coupling them would force the slowest
//! loop's cadence onto the fastest.
//!
//! # Platform Agnosticism
//!
//! This crate defines the scaling logic and decision-making but does NOT
//! contain any platform-specific API clients. The [`actuator::Actuator`] trait
//! abstracts the mutations that the reconciler needs (update service count,
//! set ASG capacity, drain instances, etc.). Platform crates (e.g.,
//! `platforms/ecs/`) provide concrete implementations.

pub mod actuator;
pub mod config;
pub mod controller_client;
pub mod envelope;
pub mod freshness;
pub mod leader;
pub mod loop_a;
pub mod loop_b;
pub mod loop_c;
pub mod mimir;
pub mod reconciler;
pub mod signals;
