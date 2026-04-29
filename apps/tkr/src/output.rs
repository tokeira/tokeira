use anyhow::Result;
use serde::Serialize;
use std::fmt::Display;

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
