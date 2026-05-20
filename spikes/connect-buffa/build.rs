fn main() {
    // connectrpc-build generates both buffa message types AND service stubs
    // in a single pass — no separate buffa-build step needed.
    connectrpc_build::Config::new()
        .files(&["../../proto/tokeira/internal/controller/v1/controller.proto"])
        .includes(&["../../proto"])
        .include_file("_connectrpc.rs")
        .compile()
        .unwrap();
}
