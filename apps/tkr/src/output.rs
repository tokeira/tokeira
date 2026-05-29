//! Simple helper for human vs JSON rendering of a single value or table.
//!
//! Used by `commands::deployment` for the `deployment list` table. Other
//! commands emit bespoke output (image commands use a different
//! json-or-human helper colocated with their row builders) so this is
//! deliberately small rather than a workspace-wide formatter.

use anyhow::Result;
use serde::Serialize;
use std::fmt::Display;

pub mod build_info;

pub struct OutputFormatter {
    json: bool,
}

impl OutputFormatter {
    pub fn new(json: bool) -> Self {
        Self { json }
    }

    #[allow(dead_code)]
    pub fn print<T>(&self, value: &T) -> Result<()>
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

    pub fn print_json<T: Serialize>(&self, value: &T) -> Result<()> {
        if self.json {
            println!("{}", serde_json::to_string_pretty(value)?);
        } else {
            println!("{}", serde_json::to_string(value)?);
        }
        Ok(())
    }

    pub fn print_table(&self, rows: &[Vec<String>]) {
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
    pub fn print_error(&self, error: &anyhow::Error) {
        eprintln!("error: {error:#}");
    }
}
