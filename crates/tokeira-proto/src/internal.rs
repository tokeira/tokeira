//! Tokeira-internal protobuf packages.
//!
//! The public compatibility surface should remain narrow and stable.
//! The internal surface is where we can model the actual runtime architecture:
//!
//! - command envelopes for shard/lane/run mutation
//! - dispatch records and reservations
//! - projection mutations and checkpoints
//! - operator/admin control plane APIs

// Generated code: the codegen's unwraps are its own.
#![allow(clippy::unwrap_used)]
pub use crate::public::temporal;

pub mod tokeira {
    pub mod internal {
        pub mod runtime {
            pub mod v1 {}
        }

        pub mod controller {
            pub mod v1 {
                include!("generated/tokeira/tokeira.internal.controller.v1.rs");
            }
        }

        pub mod admin {
            pub mod v1 {}
        }
    }
}

/// File descriptor set for the internal API surface.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("generated/tokeira/tokeira_internal_descriptor.bin");

pub use tokeira::internal::{
    admin::v1 as admin, controller::v1 as controller, runtime::v1 as runtime,
};

pub const ADMIN_SERVICE_NAME: &str = "tokeira.internal.admin.v1.AdminService";
pub const PLACEMENT_CONTROLLER_SERVICE_NAME: &str =
    "tokeira.internal.controller.v1.PlacementController";
