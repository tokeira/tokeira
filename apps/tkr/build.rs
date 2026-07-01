fn main() {
    // Capture the compile target triple so the launcher can verify the installed
    // `tkp` against the integrity manifest's descriptor for *this host's* target
    // (task 9.1) — tkr and the tkp it launches run on the same host.
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TKR_TARGET={target}");
}
