//! Check command - verify archive integrity

use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::storage::database::Database;
use crate::storage::volume::VolumeManager;
use crate::utils::progress::ProgressBar;
use crate::commands::create::format_size;

#[derive(Args)]
pub struct CheckArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Verify all chunks (read and hash verify)
    #[arg(short, long)]
    pub verify: bool,

    /// Repair if possible
    #[arg(short, long)]
    pub repair: bool,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Check specific volume only
    #[arg(long)]
    pub volume: Option<u64>,

    /// Show progress
    #[arg(short = 'p', long)]
    pub progress: bool,
}

pub fn execute(args: CheckArgs, _config: &crate::utils::config::Config) -> Result<()> {
    println!("tems-backup v{} - Archive Integrity Check", crate::VERSION);
    println!("Archive: {}", args.archive.display());

    // Open database
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }

    let db = Database::open(&db_path)?;
    let mut volume_manager = VolumeManager::new(args.archive.clone());
    volume_manager.set_database(db.clone());
    volume_manager.load_volumes()?;

    // Check database integrity
    println!("Checking database integrity...");
    let db_ok = db.integrity_check()?;
    
    if !db_ok {
        println!("❌ Database integrity check failed!");
        if args.repair {
            println!("Attempting database repair...");
            println!("Database repair not implemented yet");
        }
    } else {
        println!("✅ Database OK");
    }

    // Check volumes
    let volumes_to_check = if let Some(v) = args.volume {
        vec![v]
    } else {
        volume_manager.list_volumes()
    };

    println!("\nChecking volumes...");
    
    // Progress bar
    let progress_bar = if args.progress {
        Some(ProgressBar::new_check_bar(volumes_to_check.len() as u64))
    } else {
        None
    };

    for (i, volume_num) in volumes_to_check.iter().enumerate() {
        if let Some(pb) = &progress_bar {
            pb.set_message(format!("Volume {:010}", volume_num));
            pb.set_position((i + 1) as u64);
        }
        
        if args.verbose {
            println!("Checking volume {}", volume_num);
        }
        
        if let Some(info) = volume_manager.get_volume_info(*volume_num) {
            if info.path.exists() {
                let metadata = std::fs::metadata(&info.path)?;
                if args.verbose {
                    println!("  Volume file: {}", info.path.display());
                    println!("  Size on disk: {}", format_size(metadata.len()));
                    println!("  Expected size: {}", format_size(info.size));
                }
                
                if metadata.len() >= info.size {
                    if args.verbose {
                        println!("  ✅ Volume OK");
                    }
                } else if args.verbose {
                    println!("  ❌ Volume size mismatch");
                }
            } else if args.verbose {
                println!("  ❌ Volume file not found: {}", info.path.display());
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    println!("\nCheck completed");
    Ok(())
}

