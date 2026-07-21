//! Hook entry points for Claude Code and Kiro CLI.
//!
//! The committed hook configuration (`.claude/settings.json`,
//! `.kiro/hooks/rust-quality.json`) stays a bare `tkw hook <verb>` command;
//! all logic lives here, in Rust, testable. Both harnesses hand hook context
//! as JSON on stdin and interpret exit codes:
//!
//! - Claude Code: a `Stop` hook exiting 2 blocks the session from finishing
//!   and feeds stderr back to the model.
//! - Kiro CLI v3: exit 2 blocks only `PreToolUse`/`UserPromptSubmit`; on
//!   `Stop` a non-zero exit surfaces as a warning. The gate is therefore
//!   enforced under Claude and advisory under Kiro — stated in
//!   docs/agents/concurrent-agents.md rather than papered over here.
//!
//! `post_edit` always exits 0: formatting is a convenience and must never
//! block an edit, and hooks also fire for non-Rust writes we simply ignore.

use std::{io::Read, process::Command};

use serde_json::Value;

/// Read hook context JSON from stdin. Absent or malformed input is not an
/// error — hooks must degrade to a no-op, not fail the session.
fn stdin_context() -> Option<Value> {
    let mut buffer = String::new();
    std::io::stdin().read_to_string(&mut buffer).ok()?;
    serde_json::from_str(buffer.trim()).ok()
}

/// Extract the edited file's path from whichever key the harness uses.
/// Claude Code nests it as `tool_input.file_path`; the flat fallbacks cover
/// Kiro's file triggers, whose context shape is not pinned by its docs.
fn edited_file(context: &Value) -> Option<String> {
    for candidate in [
        context.pointer("/tool_input/file_path"),
        context.get("file_path"),
        context.get("path"),
        context.get("file"),
    ] {
        if let Some(path) = candidate.and_then(Value::as_str) {
            return Some(path.to_string());
        }
    }
    None
}

/// Format the edited Rust file with the project's nightly rustfmt.
/// Single-file rustfmt (not `cargo fmt --all`) keeps the per-edit cost flat
/// regardless of workspace size; rustfmt discovers `rustfmt.toml` on its own.
pub(crate) fn post_edit() -> i32 {
    let Some(context) = stdin_context() else {
        return 0;
    };
    let Some(file) = edited_file(&context) else {
        return 0;
    };
    if !file.ends_with(".rs") || !std::path::Path::new(&file).is_file() {
        return 0;
    }
    let result = Command::new("rustfmt")
        .args(["+nightly", "--edition", "2024", &file])
        .output();
    if let Ok(output) = result
        && !output.status.success()
    {
        // Surface the problem without blocking: a syntactically broken file
        // mid-edit is normal, and the Stop gate still catches real breakage.
        eprintln!(
            "tkw hook post-edit: rustfmt failed on {file}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    0
}

/// Finish-green gate: refuse to let a session end with a broken workspace.
pub(crate) fn stop() -> i32 {
    // `stop_hook_active` is Claude Code's re-entry flag: it is set when the
    // session is already continuing because of a previous blocking Stop hook.
    // Honoring it prevents an unfixable failure from looping forever.
    if let Some(context) = stdin_context()
        && context
            .get("stop_hook_active")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return 0;
    }
    let output = match Command::new("cargo")
        .args(["check", "--workspace", "--quiet"])
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            eprintln!("tkw hook stop: failed to run cargo check: {error}");
            // Can't prove the tree is green, but blocking on a missing cargo
            // would trap the session; report and let it end.
            return 0;
        }
    };
    if output.status.success() {
        return 0;
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    // The tail carries the errors; full rustc output can be thousands of
    // lines and the harness truncates from the front.
    let tail: Vec<&str> = stderr.lines().collect();
    let start = tail.len().saturating_sub(80);
    eprintln!("cargo check --workspace failed — fix the errors before finishing:");
    for line in &tail[start..] {
        eprintln!("{line}");
    }
    2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edited_file_reads_claude_shape() {
        let context: Value = serde_json::from_str(
            r#"{"tool_name":"Edit","tool_input":{"file_path":"src/lib.rs","old_string":"a"}}"#,
        )
        .expect("valid json");
        assert_eq!(edited_file(&context), Some("src/lib.rs".to_string()));
    }

    #[test]
    fn edited_file_reads_flat_shapes() {
        for key in ["file_path", "path", "file"] {
            let context: Value =
                serde_json::from_str(&format!(r#"{{"{key}":"src/main.rs"}}"#)).expect("valid json");
            assert_eq!(
                edited_file(&context),
                Some("src/main.rs".to_string()),
                "key {key}"
            );
        }
    }

    #[test]
    fn edited_file_absent_when_no_path_key() {
        let context: Value = serde_json::from_str(r#"{"tool_name":"Bash"}"#).expect("valid json");
        assert_eq!(edited_file(&context), None);
    }
}
