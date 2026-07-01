fn main() {
    // Capture the compile target triple so the running binary can record its own
    // per-target integrity descriptor at stamp time (task 4.1).
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TKP_TARGET={target}");
}
