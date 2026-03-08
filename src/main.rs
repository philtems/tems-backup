//! tems-backup - Advanced backup tool with deduplication and versioning
//! Copyright (c) 2026 Philippe TEMESI <philippe@tems.be>
//! Licensed under MIT OR Apache-2.0

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;

mod commands;
mod core;
mod storage;
mod utils;
mod error;
mod remote;  // ← Nouveau module

use commands::*;
use utils::config::Config;

#[derive(Parser)]
#[command(name = "tems-backup")]
#[command(author = "Philippe TEMESI <philippe@tems.be>")]
#[command(version = "0.2.0")]  // ← Nouvelle version
#[command(about = "Advanced backup tool with deduplication and versioning", long_about = None)]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Configuration file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Authentication file for remote (login:password or ssh key path)
    #[arg(short = 'A', long, value_name = "FILE")]
    auth_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new backup archive (local or remote)
    Create(create::CreateArgs),

    /// Add files to an existing archive (local or remote)
    Add(add::AddArgs),

    /// Restore files from an archive (local or remote)
    Restore(restore::RestoreArgs),

    /// List files in archive
    List(list::ListArgs),

    /// Show file history
    Log(log::LogArgs),

    /// Compare file versions
    Diff(diff::DiffArgs),

    /// Garbage collection (remove orphaned chunks)
    Gc(gc::GcArgs),

    /// Check archive integrity
    Check(check::CheckArgs),

    /// Manage volumes
    Volume(volume::VolumeArgs),
}

pub use tems_backup::{VERSION, YEAR, AUTHOR, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, VOLUME_NUMBER_DIGITS};

fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    utils::logging::init(cli.verbose)?;

    // Load configuration
    let config = Config::load(cli.config)?;

    // Execute command
    match cli.command {
        Commands::Create(args) => create::execute(args, &config, cli.auth_file),
        Commands::Add(args) => add::execute(args, &config, cli.auth_file),
        Commands::Restore(args) => restore::execute(args, &config, cli.auth_file),
        Commands::List(args) => list::execute(args, &config),
        Commands::Log(args) => log::execute(args, &config),
        Commands::Diff(args) => diff::execute(args, &config),
        Commands::Gc(args) => gc::execute(args, &config),
        Commands::Check(args) => check::execute(args, &config),
        Commands::Volume(args) => volume::execute(args, &config),
    }
}

