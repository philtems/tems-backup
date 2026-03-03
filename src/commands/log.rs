//! Log command - show file history

use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::storage::database::Database;

#[derive(Args)]
pub struct LogArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// File path in archive
    #[arg(required = true)]
    pub path: String,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: LogArgs, _config: &crate::utils::config::Config) -> Result<()> {
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }

    let db = Database::open(&db_path)?;
    let versions = db.get_file_history(&args.path)?;

    if versions.is_empty() {
        println!("No versions found for: {}", args.path);
        return Ok(());
    }

    if args.json {
        let json = serde_json::to_string_pretty(&versions)?;
        println!("{}", json);
    } else {
        println!("History for '{}':", args.path);
        println!("{:<6}  {:<20}  {:<10}  {}", "Version", "Date", "Size", "Action");
        println!("{}", "-".repeat(60));

        for (_id, version, modified) in versions {
            let date = chrono::DateTime::from_timestamp(modified, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".to_string());

            // Action would be determined from database
            let action = if version == 1 { "created" } else { "modified" };

            println!("v{:<5}  {}  {:<10}  {}", version, date, "-", action);
        }
    }

    Ok(())
}

