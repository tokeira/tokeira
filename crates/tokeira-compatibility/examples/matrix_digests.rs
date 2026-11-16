//! Print the digests of the compatibility matrices compiled into this build,
//! as one JSON object.
//!
//! The tokeirad image build runs this in its builder, against the workspace it
//! builds, and records both digests in the versioned build manifest.

// The JSON on stdout is the example's output.
#![allow(clippy::print_stdout)]

use tokeira_compatibility::{feature_matrix_digest, sdk_matrix_digest};

fn main() {
    let digests = serde_json::json!({
        "feature_matrix_digest": feature_matrix_digest(),
        "sdk_matrix_digest": sdk_matrix_digest(),
    });
    println!("{digests}");
}
