use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    // Public, Temporal-compatible surface.
    //
    // In the mature repository, these files should come from a workspace-level vendored proto
    // tree synced from upstream Temporal. We keep them inside this starter crate so the artifact
    // is self-contained and easy to inspect.
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .btree_map(["."])
        .file_descriptor_set_path(out_dir.join("tokeira_public_descriptor.bin"))
        .compile(
            &[
                "proto/upstream/temporal/api/common/v1/message.proto",
                "proto/upstream/temporal/api/enums/v1/workflow.proto",
                "proto/upstream/temporal/api/workflowservice/v1/service.proto",
                "proto/upstream/temporal/api/operatorservice/v1/service.proto",
            ],
            &["proto/upstream"],
        )?;

    // Internal Tokeira-only control surface.
    //
    // These APIs are intentionally narrow. They should model the runtime's durable-execution
    // mechanics rather than mirror the external Temporal API.
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .btree_map(["."])
        .file_descriptor_set_path(out_dir.join("tokeira_internal_descriptor.bin"))
        .compile(
            &[
                "proto/tokeira/internal/runtime/v1/command.proto",
                "proto/tokeira/internal/runtime/v1/dispatch.proto",
                "proto/tokeira/internal/runtime/v1/projection.proto",
                "proto/tokeira/internal/admin/v1/service.proto",
            ],
            &["proto", "proto/upstream"],
        )?;

    Ok(())
}
