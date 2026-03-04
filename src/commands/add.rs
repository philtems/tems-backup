//! Add command - add files to existing archive

use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::utils::config::Config;
use crate::core::archive::Archive;
use crate::storage::database::Database;
use crate::core::file_scanner::FileScanner;
use crate::core::chunk::Chunker;
use crate::commands::create::format_size;
use crate::core::compression::CompressionAlgorithm;
use crate::core::hash::HashAlgorithm;
use crate::utils::progress::ProgressBar;
use crate::utils::parse_duration;
use crate::utils::retry::RetryConfig;
use crate::core::archive::ProcessResult;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;
use std::time::Instant;

#[derive(Args)]
pub struct AddArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Files/directories to add
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    /// Compression algorithm (override archive default)
    #[arg(short, long)]
    pub compression: Option<String>,

    /// Compression level
    #[arg(short = 'l', long)]
    pub compress_level: Option<i32>,

    /// Disable deduplication for this addition
    #[arg(long)]
    pub no_dedup: bool,

    /// Volume size for new volumes (e.g., 1G, 500M) - optional
    #[arg(short = 'V', long)]
    pub volume_size: Option<String>,

    /// Exclude patterns
    #[arg(short, long)]
    pub exclude: Vec<String>,

    /// Include patterns
    #[arg(short, long)]
    pub include: Vec<String>,

    /// Exclude caches
    #[arg(long)]
    pub exclude_caches: bool,

    /// Dry run
    #[arg(short, long)]
    pub dry_run: bool,

    /// Show progress
    #[arg(short = 'p', long)]
    pub progress: bool,

    /// Only include files modified within this duration (e.g., 1d, 12h, 30m)
    #[arg(long)]
    pub max_age: Option<String>,

    /// Only add files that are newer than the version in the archive
    #[arg(long)]
    pub newer_only: bool,

    /// Number of retry attempts for failed files (-1 = infinite, 0 = no retry)
    #[arg(long, default_value_t = 0)]
    pub retry: i32,

    /// Delay between retries in seconds
    #[arg(long, default_value_t = 5)]
    pub retry_delay: u64,
}

pub fn execute(args: AddArgs, config: &Config) -> Result<()> {
    println!("tems-backup v{} - Adding to archive", crate::VERSION);
    println!("Archive: {}", args.archive.display());

    // Open existing archive
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }

    let db = Database::open(&db_path)?;
    
    // Load archive configuration from database
    let archive_config = load_archive_config(&db)?;
    
    // Create chunker with archive defaults (overridden by args)
    let chunker = Chunker::new(
        archive_config.chunk_size,
        archive_config.hash_algo,
        if let Some(ref c) = args.compression {
            CompressionAlgorithm::from_str(c)?
        } else {
            archive_config.compression
        },
        args.compress_level.unwrap_or(archive_config.compression_level),
    );

    // Parse volume size if specified (optional, for info only)
    let _volume_size = if let Some(size) = args.volume_size {
        Some(crate::commands::create::parse_size(&size)? as u64)
    } else {
        None
    };
    
    // Parse max age if specified
    let max_age_seconds = if let Some(age) = &args.max_age {
        Some(parse_duration(age)?)
    } else {
        None
    };

    // Create retry configuration
    let retry_config = RetryConfig {
        max_retries: args.retry,
        delay_seconds: args.retry_delay,
    };

    // Scan files
    let mut scanner = FileScanner::new(
        if args.exclude.is_empty() { config.exclude.patterns.clone() } else { args.exclude },
        args.include,
        args.exclude_caches || config.exclude.caches,
    );
    scanner.set_hidden(true);

    println!("Scanning files...");
    let all_files = scanner.scan_paths(&args.paths)?;
    
    // Filter by max age if specified
    let files = if let Some(max_age_seconds) = max_age_seconds {
        let cutoff = SystemTime::now() - std::time::Duration::from_secs(max_age_seconds);
        all_files.into_iter()
            .filter(|f| f.modified >= cutoff)
            .collect()
    } else {
        all_files
    };
    
    let total_size: u64 = files.iter().map(|f| f.size).sum();
    println!("Found {} files to add (total: {})", files.len(), format_size(total_size));
    
    if files.is_empty() {
        println!("No files to add (none match the criteria)");
        return Ok(());
    }

    // Show summary
    if args.newer_only {
        println!("Mode: newer-only (skipping unchanged files)");
    }
    if args.retry != 0 {
        println!("Retry: {} attempts, {}s delay", 
            if args.retry == -1 { "infinite".to_string() } else { args.retry.to_string() },
            args.retry_delay);
    }

    // Open archive WITH existing volumes loaded
    let mut archive = Archive::open_with_config(
        args.archive.clone(),
        db,
        chunker,
        args.no_dedup,
        args.dry_run,
    )?;

    // Progress bar
    let progress_bar = if args.progress {
        Some(ProgressBar::new_dual_backup_bar(files.len() as u64, total_size))
    } else {
        None
    };

    let processed_files = Arc::new(AtomicU64::new(0));
    let processed_bytes = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicUsize::new(0));
    let skipped_files = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Instant::now());
    let progress_bar_ref = progress_bar.as_ref();

    // Add files with progress
    for file in &files {
        if let Some(pb) = progress_bar_ref {
            let current_files = processed_files.fetch_add(1, Ordering::Relaxed) + 1;
            
            // Calculate files per second
            let elapsed = start_time.elapsed().as_secs_f64();
            let files_per_sec = if elapsed > 0.0 {
                current_files as f64 / elapsed
            } else {
                0.0
            };
            
            pb.set_files_message(format!("Adding: {}", file.path.display()));
            pb.set_files_speed(files_per_sec);
            pb.set_position(current_files);
        }
        
        match archive.process_file(file, args.newer_only, &retry_config) {
            Ok(ProcessResult::Processed) => {
                // File was processed
                processed_bytes.fetch_add(file.size, Ordering::Relaxed);
                if let Some(pb) = progress_bar_ref {
                    pb.set_data_position(processed_bytes.load(Ordering::Relaxed));
                }
            }
            Ok(ProcessResult::Skipped) => {
                // File was skipped (newer_only)
                skipped_files.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                // File failed after retries
                eprintln!("Failed to add {}: {}", file.path.display(), e);
                failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    // Show stats
    let stats = archive.get_stats()?;
    let failed = failed_files.load(Ordering::Relaxed);
    let skipped = skipped_files.load(Ordering::Relaxed);
    
    println!("\nAddition completed successfully!");
    if skipped > 0 {
        println!("⏭️  {} files skipped (unchanged)", skipped);
    }
    if failed > 0 {
        println!("⚠️  {} files failed and were not added", failed);
    }
    println!("Files added: {}", files.len() - skipped - failed);
    println!("Total files in archive: {}", stats.get("files").unwrap_or(&"0".to_string()));
    println!("Total size: {}", format_size(stats.get("total_size").unwrap_or(&"0".to_string()).parse().unwrap_or(0)));

    Ok(())
}

fn load_archive_config(_db: &Database) -> Result<ArchiveConfig> {
    // In a real implementation, this would load from database
    // For now, use default values
    Ok(ArchiveConfig {
        chunk_size: crate::DEFAULT_CHUNK_SIZE,
        hash_algo: HashAlgorithm::Blake3,
        compression: CompressionAlgorithm::Zstd,
        compression_level: 3,
    })
}

struct ArchiveConfig {
    chunk_size: usize,
    hash_algo: HashAlgorithm,
    compression: CompressionAlgorithm,
    compression_level: i32,
}

