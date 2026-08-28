//! Thin CLI entry point. All bootstrap logic lives in [`tokeirad`] (the
//! library target defined in the same crate). Tests and in-process harnesses
//! should use [`tokeirad::TokeiradHandle::start_in_memory`] rather than
//! forking this binary.

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = tokeirad::__cli_parse();
    #[cfg(feature = "conformance")]
    {
        tokeirad::conformance::run(cli).await
    }
    #[cfg(not(feature = "conformance"))]
    {
        tokeirad::run_from_cli(cli).await
    }
}
