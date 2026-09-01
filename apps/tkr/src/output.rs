//! Simple helper for human vs JSON rendering of a single value or table.
//!
//! Used by `commands::deployment` for the `deployment list` table. Other
//! commands emit bespoke output (image commands use a different
//! json-or-human helper colocated with their row builders) so this is
//! deliberately small rather than a workspace-wide formatter.

use anyhow::Result;
use serde::Serialize;
use std::fmt::Display;

pub(crate) mod build_info;

/// Render a refusal through the operator Markdown convention — skinned via
/// termimad on a terminal, raw deterministic Markdown to a pipe — mirroring
/// `tkp`'s report emission. Refusals are authored in Markdown from here on;
/// existing plain-text messages render unchanged. Always stderr: stdout
/// stays parseable for `--json` consumers.
pub(crate) fn render_refusal(error: &anyhow::Error, json: bool) {
    use std::io::IsTerminal;
    if json {
        let rendered = error
            .downcast_ref::<tokeira_build::ReleaseError>()
            .map_or_else(
                || {
                    serde_json::json!({
                        "code": "refused",
                        "summary": error.to_string(),
                        "details": serde_json::Value::Null,
                    })
                },
                |release| {
                    serde_json::json!({
                        "code": release.code(),
                        "summary": release.to_string(),
                        "details": serde_json::Value::Null,
                    })
                },
            );
        eprintln!(
            "{}",
            serde_json::to_string(&rendered).expect("refusal report is serializable")
        );
        return;
    }
    let mut text = format!("**refused:** {error}\n");
    let mut causes = error.chain().skip(1).peekable();
    if causes.peek().is_some() {
        text.push_str("\nbecause:\n");
        for cause in causes {
            text.push_str(&format!("- {cause}\n"));
        }
    }
    if std::io::stderr().is_terminal() {
        eprintln!("{}", termimad::term_text(&text));
    } else {
        eprint!("{text}");
    }
}

/// Render a report through the same operator Markdown convention as
/// [`render_refusal`], on stdout: reports are the verb's answer, not a
/// complaint about the request.
pub(crate) fn render_markdown(text: &str) {
    use std::io::IsTerminal;
    if std::io::stdout().is_terminal() {
        println!("{}", termimad::term_text(text));
    } else {
        print!("{text}");
    }
}

pub(crate) struct OutputFormatter {
    json: bool,
}

impl OutputFormatter {
    pub(crate) fn new(json: bool) -> Self {
        Self { json }
    }

    #[allow(dead_code)]
    pub(crate) fn print<T>(&self, value: &T) -> Result<()>
    where
        T: Serialize + Display,
    {
        if self.json {
            println!("{}", serde_json::to_string_pretty(value)?);
        } else {
            println!("{value}");
        }
        Ok(())
    }

    pub(crate) fn print_json<T: Serialize>(&self, value: &T) -> Result<()> {
        if self.json {
            println!("{}", serde_json::to_string_pretty(value)?);
        } else {
            println!("{}", serde_json::to_string(value)?);
        }
        Ok(())
    }

    pub(crate) fn print_table(&self, rows: &[Vec<String>]) {
        if self.json {
            println!(
                "{}",
                serde_json::to_string_pretty(rows).expect("table rows serialize")
            );
        } else {
            for row in rows {
                println!("{}", row.join("\t"));
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_error(&self, error: &anyhow::Error) {
        eprintln!("error: {error:#}");
    }
}
