//! `tkr version` — print compile-time build provenance embedded in the CLI.

pub(crate) fn run(verbose: bool, json: bool) {
    let info = tokeira_build_info::summary();
    let rendered = if json {
        crate::output::build_info::format_version_json(&info)
    } else if verbose {
        crate::output::build_info::format_version_verbose(&info)
    } else {
        crate::output::build_info::format_version_short(&info)
    };
    println!("{rendered}");
}
