//! `tkr version` — print the CLI's `CARGO_PKG_VERSION` from its
//! `Cargo.toml` (workspace-inherited). Trivial by design; kept as a
//! separate module so it slots into the dispatch pattern without special
//! cases.

pub fn run() {
    println!("{}", env!("CARGO_PKG_VERSION"));
}
