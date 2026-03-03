//! List command implementation

use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::utils::config::Config;
use crate::storage::database::Database;
use crate::core::archive::Archive;
use crate::commands::create::format_size;

#[derive(Args)]
pub struct ListArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Patterns to list (glob)
    pub patterns: Vec<String>,

    /// Long format (with details)
    #[arg(short, long)]
    pub long: bool,

    /// Show all versions (not just current)
    #[arg(long)]
    pub all_versions: bool,

    /// Show deleted files
    #[arg(long)]
    pub deleted: bool,

    /// Sort by size
    #[arg(long)]
    pub sort_size: bool,

    /// Sort by date
    #[arg(long)]
    pub sort_date: bool,

    /// Reverse sort
    #[arg(short, long)]
    pub reverse: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub fn execute(args: ListArgs, _config: &Config) -> Result<()> {
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }

    let db = Database::open(&db_path)?;
    let archive = Archive::open(args.archive.clone(), db)?;

    if args.json {
        list_json(&archive, &args)
    } else {
        list_text(&archive, &args)
    }
}

fn list_text(archive: &Archive, args: &ListArgs) -> Result<()> {
    let files = archive.list_files(
        args.patterns.clone(),
        args.all_versions,
        args.deleted,
    )?;

    if files.is_empty() {
        println!("No files found");
        return Ok(());
    }

    // Sort files
    let mut files = files;
    if args.sort_size {
        files.sort_by_key(|f| f.size);
    } else if args.sort_date {
        files.sort_by_key(|f| f.modified);
    } else {
        files.sort_by_key(|f| f.path.clone());
    }

    if args.reverse {
        files.reverse();
    }

    // Display
    if args.long {
        println!("{:>10}  {:19}  {}", "Size", "Modified", "Path");
        println!("{}", "-".repeat(50));
        for f in &files {
            let size = format_size(f.size);
            let modified = chrono::DateTime::from_timestamp(f.modified as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".to_string());
            
            let version = if args.all_versions {
                format!(" v{}", f.version)
            } else {
                String::new()
            };

            println!("{:>10}  {}  {}{}", size, modified, f.path.display(), version);
        }
    } else {
        for f in &files {
            if args.all_versions {
                println!("{} [v{}]", f.path.display(), f.version);
            } else {
                println!("{}", f.path.display());
            }
        }
    }

    println!("\nTotal: {} files", files.len());
    Ok(())
}

fn list_json(archive: &Archive, args: &ListArgs) -> Result<()> {
    let files = archive.list_files(
        args.patterns.clone(),
        args.all_versions,
        args.deleted,
    )?;

    let json = serde_json::to_string_pretty(&files)?;
    println!("{}", json);
    Ok(())
}

