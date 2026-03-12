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
mod remote;

use commands::*;
use utils::config::Config;

#[derive(Parser)]
#[command(name = "tems-backup")]
#[command(author = "Philippe TEMESI <philippe@tems.be>")]
#[command(version = "0.2.0")]
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

    // Nettoyer les dossiers temporaires orphelins au démarrage
    cleanup_orphaned_temp_dirs()?;

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

fn cleanup_orphaned_temp_dirs() -> Result<()> {
    let temp_base = std::env::temp_dir();
    let pattern = "tems-backup-";
    
    if !temp_base.exists() {
        return Ok(());
    }
    
    for entry in std::fs::read_dir(temp_base)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(pattern) {
                    // Vérifier si le processus est toujours en vie
                    if let Some(pid_str) = name.strip_prefix(pattern) {
                        if let Ok(pid) = pid_str.parse::<u32>() {
                            if !is_process_running(pid) {
                                println!("🧹 Nettoyage du dossier temporaire orphelin: {}", path.display());
                                let _ = std::fs::remove_dir_all(&path);
                            }
                        }
                    }
                }
            }
        }
    }
    
    Ok(())
}

#[cfg(unix)]
fn is_process_running(pid: u32) -> bool {
    use sysinfo::{System, Pid};
    let system = System::new();
    system.process(Pid::from_u32(pid)).is_some()
}


#[cfg(windows)]
fn is_process_running(pid: u32) -> bool {
    use winapi::um::processthreadsapi::OpenProcess;
    use winapi::um::handleapi::CloseHandle;
    use winapi::um::winnt::PROCESS_QUERY_INFORMATION;
    
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid);
        if handle.is_null() {
            false
        } else {
            CloseHandle(handle);
            true
        }
    }
}

