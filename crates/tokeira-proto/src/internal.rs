//! Tokeira-internal protobuf packages.
//!
//! The public compatibility surface should remain narrow and stable.
//! The internal surface is where we can model the actual runtime architecture:
//!
//! - command envelopes for shard/lane/run mutation
//! - dispatch records and reservations
//! - projection mutations and checkpoints
//! - operator/admin control plane APIs

pub use crate::public::temporal;

pub mod tokeira {
    pub mod internal {
        pub mod runtime {
            pub mod v1 {}
        }

        pub mod admin {
            pub mod v1 {}
        }
    }
}

/// File descriptor set for the internal API surface.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("tokeira_internal_descriptor");

pub use tokeira::internal::admin::v1 as admin;
pub use tokeira::internal::runtime::v1 as runtime;

pub const ADMIN_SERVICE_NAME: &str = "tokeira.internal.admin.v1.AdminService";
