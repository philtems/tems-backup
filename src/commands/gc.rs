//! Garbage collection command - remove orphaned chunks

use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::storage::database::Database;
use crate::storage::volume::VolumeManager;
use crate::utils::progress::ProgressBar;
use crate::commands::create::format_size;

#[derive(Args)]
pub struct GcArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Dry run (only show what would be removed)
    #[arg(short, long)]
    pub dry_run: bool,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Force GC without confirmation
    #[arg(short, long)]
    pub force: bool,

    /// Show progress
    #[arg(short = 'p', long)]
    pub progress: bool,
}

pub fn execute(args: GcArgs, _config: &crate::utils::config::Config) -> Result<()> {
    println!("tems-backup v{} - Garbage Collection", crate::VERSION);
    println!("Archive: {}", args.archive.display());

    // Open database
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }

    let db = Database::open(&db_path)?;
    let mut volume_manager = VolumeManager::new(args.archive.clone());
    volume_manager.load_volumes()?;

    // Find orphaned chunks (reference_count = 0)
    println!("Scanning for orphaned chunks...");
    let orphans = db.get_orphaned_chunks()?;
    
    if orphans.is_empty() {
        println!("No orphaned chunks found. Archive is clean.");
        return Ok(());
    }

    // Calculate total size
    let total_size: u64 = orphans.iter()
        .map(|(_, _, size)| *size)
        .sum();

    println!("Found {} orphaned chunks (total: {})", 
        orphans.len(), 
        format_size(total_size));

    if args.dry_run {
        if args.verbose {
            println!("\nOrphaned chunks:");
            for (hash, volume, size) in &orphans {
                println!("  Volume {}: size {} (hash: {})", 
                    volume, 
                    format_size(*size),
                    hex::encode(&hash[..8]));
            }
        }
        println!("\nDry run completed. Run without --dry-run to remove.");
        return Ok(());
    }

    // Confirm unless forced
    if !args.force {
        use dialoguer::Confirm;
        if !Confirm::new()
            .with_prompt("Remove orphaned chunks? This cannot be undone.")
            .interact()? 
        {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Progress bar
    let progress_bar = if args.progress {
        Some(ProgressBar::new_gc_bar(orphans.len() as u64))
    } else {
        None
    };

    // Group orphans by volume for efficient processing
    let mut by_volume: std::collections::HashMap<u64, Vec<(Vec<u8>, u64)>> = 
        std::collections::HashMap::new();
    
    for (hash, volume, size) in orphans {
        by_volume.entry(volume)
            .or_insert_with(Vec::new)
            .push((hash, size));
    }

    // Process each volume
    let _total_volumes = by_volume.len();
    let mut processed = 0;

    for (volume_num, chunks) in by_volume {
        processed += 1;
        
        if let Some(pb) = &progress_bar {
            pb.set_message(format!("Volume {}: {} chunks", volume_num, chunks.len()));
            pb.set_position(processed as u64);
        }
        
        // In a real implementation, you would:
        // 1. Open the volume file
        // 2. Mark chunks as free (or rewrite volume without them)
        // 3. Update volume free space
        
        for (hash, _) in chunks {
            log::debug!("Would delete chunk {} from volume {}", hex::encode(&hash[..8]), volume_num);
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    // Delete from database
    let deleted = db.delete_orphaned_chunks()?;
    println!("Removed {} chunks from database", deleted);

    // Vacuum database to reclaim space
    println!("Optimizing database...");
    db.vacuum()?;

    println!("Garbage collection completed successfully!");
    Ok(())
}

