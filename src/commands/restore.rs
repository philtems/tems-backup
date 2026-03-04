//! Restore command implementation

use clap::Args;
use std::path::{Path, PathBuf};
use anyhow::Result;
use crate::utils::config::Config;
use crate::core::archive::Archive;
use crate::storage::database::Database;
use crate::utils::progress::ProgressBar;
use crate::commands::create::format_size;
use crate::utils::retry::RetryConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Args)]
pub struct RestoreArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Files/directories to restore (restore all if not specified)
    pub paths: Vec<PathBuf>,

    /// Target directory
    #[arg(short = 'C', long)]
    pub target: Option<PathBuf>,

    /// Restore specific version
    #[arg(short, long)]
    pub version: Option<u64>,

    /// Restore state as of this date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
    #[arg(long)]
    pub as_of: Option<String>,

    /// Restore all versions (with .v1, .v2 suffixes)
    #[arg(long)]
    pub all_versions: bool,

    /// Snapshot ID to restore
    #[arg(short, long)]
    pub snapshot: Option<String>,

    /// Overwrite existing files
    #[arg(long)]
    pub overwrite: bool,

    /// Skip existing files
    #[arg(long)]
    pub skip_existing: bool,

    /// Backup existing files with .bak suffix
    #[arg(long)]
    pub backup_existing: bool,

    /// Interactive mode (ask for each conflict)
    #[arg(short, long)]
    pub interactive: bool,

    /// Preserve permissions
    #[arg(long)]
    pub preserve_permissions: bool,

    /// Preserve ownership (requires root)
    #[arg(long)]
    pub preserve_ownership: bool,

    /// Preserve timestamps
    #[arg(long)]
    pub preserve_times: bool,

    /// Strip N components from paths
    #[arg(long)]
    pub strip_components: Option<usize>,

    /// Flatten directory structure
    #[arg(long)]
    pub flatten: bool,

    /// Show progress
    #[arg(short = 'p', long)]
    pub progress: bool,

    /// Dry run (show what would be done)
    #[arg(short, long)]
    pub dry_run: bool,

    /// Number of retry attempts for failed files (-1 = infinite, 0 = no retry)
    #[arg(long, default_value_t = 0)]
    pub retry: i32,

    /// Delay between retries in seconds
    #[arg(long, default_value_t = 5)]
    pub retry_delay: u64,
}

pub fn execute(args: RestoreArgs, _config: &Config) -> Result<()> {
    println!("tems-backup v{} - Restoring from archive", crate::VERSION);
    println!("Archive: {}", args.archive.display());

    // Open database
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }
    let db = Database::open(&db_path)?;

    // Open archive
    let archive = Archive::open(args.archive.clone(), db)?;

    // Determine target directory
    let target = args.target.unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&target)?;

    // Build restore options
    let options = RestoreOptions {
        paths: args.paths,
        version: args.version,
        as_of: args.as_of,
        all_versions: args.all_versions,
        snapshot: args.snapshot,
        overwrite: args.overwrite,
        skip_existing: args.skip_existing,
        backup_existing: args.backup_existing,
        interactive: args.interactive,
        preserve_permissions: args.preserve_permissions,
        preserve_ownership: args.preserve_ownership,
        preserve_times: args.preserve_times,
        strip_components: args.strip_components,
        flatten: args.flatten,
        dry_run: args.dry_run,
    };

    // Create retry configuration
    let retry_config = RetryConfig {
        max_retries: args.retry,
        delay_seconds: args.retry_delay,
    };

    // Get files to restore
    let files = archive.get_files_for_restore(&options)?;
    
    if files.is_empty() {
        println!("No files found to restore");
        return Ok(());
    }

    // Calculate total size
    let total_size: u64 = files.iter().map(|f| f.size).sum();
    println!("Found {} files to restore (total: {})", files.len(), format_size(total_size));
    
    if args.retry != 0 {
        println!("Retry: {} attempts, {}s delay", 
            if args.retry == -1 { "infinite".to_string() } else { args.retry.to_string() },
            args.retry_delay);
    }

    // Progress bar
    let progress_bar = if args.progress {
        Some(ProgressBar::new_dual_restore_bar(files.len() as u64, total_size))
    } else {
        None
    };

    let restored_files = Arc::new(AtomicU64::new(0));
    let restored_bytes = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Instant::now());
    let progress_bar_ref = progress_bar.as_ref();

    // Restore files
    for file in files {
        if let Some(pb) = &progress_bar_ref {
            let current_files = restored_files.fetch_add(1, Ordering::Relaxed) + 1;
            
            // Calculate files per second
            let elapsed = start_time.elapsed().as_secs_f64();
            let files_per_sec = if elapsed > 0.0 {
                current_files as f64 / elapsed
            } else {
                0.0
            };
            
            pb.set_files_message(format!("Restoring: {}", file.path.display()));
            pb.set_files_speed(files_per_sec);
            pb.set_position(current_files);
        }

        match archive.restore_file_with_retry(&file, &target, &options, &retry_config) {
            Ok(true) => {
                restored_bytes.fetch_add(file.size, Ordering::Relaxed);
                if let Some(pb) = progress_bar_ref {
                    pb.set_data_position(restored_bytes.load(Ordering::Relaxed));
                }
            }
            Ok(false) => {
                // File was skipped (already exists, etc.)
                if let Some(pb) = progress_bar_ref {
                    pb.println(&format!("Skipped: {}", file.path.display()));
                }
            }
            Err(e) => {
                eprintln!("Failed to restore {}: {}", file.path.display(), e);
                failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    let failed = failed_files.load(Ordering::Relaxed);
    if failed > 0 {
        println!("\n⚠️  {} files failed to restore", failed);
    } else {
        println!("\n✅ Restore completed successfully!");
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub paths: Vec<PathBuf>,
    pub version: Option<u64>,
    pub as_of: Option<String>,
    pub all_versions: bool,
    pub snapshot: Option<String>,
    pub overwrite: bool,
    pub skip_existing: bool,
    pub backup_existing: bool,
    pub interactive: bool,
    pub preserve_permissions: bool,
    pub preserve_ownership: bool,
    pub preserve_times: bool,
    pub strip_components: Option<usize>,
    pub flatten: bool,
    pub dry_run: bool,
}

impl RestoreOptions {
    pub fn should_overwrite(&self, path: &Path) -> Result<bool> {
        if !path.exists() {
            return Ok(true);
        }

        if self.overwrite {
            Ok(true)
        } else if self.skip_existing {
            Ok(false)
        } else if self.backup_existing {
            Ok(true) // Will rename existing
        } else if self.interactive {
            use dialoguer::Confirm;
            Ok(Confirm::new()
                .with_prompt(format!("File {} exists. Overwrite?", path.display()))
                .interact()?)
        } else {
            // Default: don't overwrite
            Ok(false)
        }
    }
}

