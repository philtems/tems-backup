//! Create command implementation

use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::utils::config::Config;
use crate::core::archive::Archive;
use crate::storage::database::Database;
use crate::core::file_scanner::FileScanner;
use crate::core::chunk::Chunker;
use crate::core::compression::CompressionAlgorithm;
use crate::core::hash::HashAlgorithm;
use crate::utils::progress::ProgressBar;
use crate::utils::parse_duration;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

#[derive(Args)]
pub struct CreateArgs {
    /// Archive path
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Files/directories to backup
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    /// Compression algorithm: zstd, xz, none
    #[arg(short, long, default_value = "zstd")]
    pub compression: String,

    /// Compression level (1-19 for zstd, 1-9 for xz)
    #[arg(short = 'l', long, default_value_t = 3)]
    pub compress_level: i32,

    /// Disable deduplication
    #[arg(long)]
    pub no_dedup: bool,

    /// Chunk size in bytes (with suffix: K, M, G)
    #[arg(short, long, default_value = "1M")]
    pub chunk_size: String,

    /// Hash algorithm: xxhash3, blake3, sha256
    #[arg(long, default_value = "blake3")]
    pub hash: String,

    /// Volume size (with suffix: K, M, G). If not set, single volume
    #[arg(short = 'V', long)]
    pub volume_size: Option<String>,

    /// Exclude patterns
    #[arg(short, long)]
    pub exclude: Vec<String>,

    /// Include patterns
    #[arg(short, long)]
    pub include: Vec<String>,

    /// Exclude caches (CACHEDIR.TAG)
    #[arg(long)]
    pub exclude_caches: bool,

    /// Dry run (don't write)
    #[arg(short, long)]
    pub dry_run: bool,

    /// Show progress
    #[arg(short = 'p', long)]
    pub progress: bool,

    /// Number of threads
    #[arg(short = 'j', long)]
    pub threads: Option<usize>,

    /// Only include files modified within this duration (e.g., 1d, 12h, 30m)
    #[arg(long)]
    pub max_age: Option<String>,
}

pub fn execute(args: CreateArgs, config: &Config) -> Result<()> {
    println!("tems-backup v{} - Copyright (c) {} {}", 
        crate::VERSION, crate::YEAR, crate::AUTHOR);
    println!("Creating archive: {}", args.archive.display());

    // Parse options
    let chunk_size = parse_size(&args.chunk_size)?;
    let compression = CompressionAlgorithm::from_str(&args.compression)?;
    let hash_algo = HashAlgorithm::from_str(&args.hash)?;
    let threads = args.threads.unwrap_or(config.threads);
    
    // Parse max age if specified
    let max_age_seconds = if let Some(age) = &args.max_age {
        Some(parse_duration(age)?)
    } else {
        None
    };

    // Initialize components
    let db_path = args.archive.with_extension("db");
    let db = Database::open(&db_path)?;
    
    let chunker = Chunker::new(
        chunk_size,
        hash_algo,
        compression,
        args.compress_level,
    );

    let mut scanner = FileScanner::new(
        if args.exclude.is_empty() { config.exclude.patterns.clone() } else { args.exclude },
        args.include,
        args.exclude_caches || config.exclude.caches,
    );
    scanner.set_hidden(true);

    let mut archive = Archive::new(
        args.archive.clone(),
        db,
        chunker,
        args.no_dedup,
        args.dry_run,
    );

    // Parse volume size if specified
    let volume_size = if let Some(size) = args.volume_size {
        Some(parse_size(&size)? as u64)
    } else {
        None
    };

    // Show summary
    println!("Configuration:");
    println!("  Chunk size: {}", format_size(chunk_size as u64));
    println!("  Compression: {} (level {})", compression, args.compress_level);
    println!("  Hash: {}", hash_algo);
    println!("  Deduplication: {}", if args.no_dedup { "off" } else { "on" });
    if let Some(vs) = volume_size {
        println!("  Volume size: {}", format_size(vs));
    }
    println!("  Threads: {}", threads);
    if let Some(age) = max_age_seconds {
        println!("  Max age: {} seconds", age);
    }
    println!();

    // Scan files
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
    println!("Found {} files to backup (total: {})", files.len(), format_size(total_size));
    
    if files.is_empty() {
        println!("No files to backup (none match the criteria)");
        return Ok(());
    }

    // Create progress bar if requested
    let progress_bar = if args.progress {
        Some(ProgressBar::new_backup_bar(files.len() as u64, total_size))
    } else {
        None
    };

    let processed_files = Arc::new(AtomicU64::new(0));
    let progress_bar_ref = progress_bar.as_ref();

    // Use create_with_progress instead of create
    archive.create_with_progress(
        &files,
        volume_size,
        |file_name: &str, _file_size: u64| {
            if let Some(pb) = progress_bar_ref {
                let current = processed_files.fetch_add(1, Ordering::Relaxed) + 1;
                pb.set_message(format!("File: {}", file_name));
                pb.set_position(current);
            }
        }
    )?;

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    // Show stats
    let stats = archive.get_stats()?;
    println!("\nBackup completed successfully!");
    println!("Summary:");
    println!("  Files backed up: {}", stats.get("files").unwrap_or(&"0".to_string()));
    println!("  Unique chunks: {}", stats.get("chunks").unwrap_or(&"0".to_string()));
    println!("  Total size: {}", stats.get("total_size").unwrap_or(&"0".to_string()));
    println!("  Stored size: {}", stats.get("stored_size").unwrap_or(&"0".to_string()));

    if !args.no_dedup {
        let total: f64 = stats.get("total_size").unwrap_or(&"0".to_string()).parse().unwrap_or(0.0);
        let unique: f64 = stats.get("unique_size").unwrap_or(&"1".to_string()).parse().unwrap_or(1.0);
        if unique > 0.0 {
            let dedup_ratio = total / unique;
            println!("  Deduplication ratio: {:.2}x", dedup_ratio);
        }
    }

    Ok(())
}

/// Parse size string (e.g., "1M", "2G", "500K")
pub fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim().to_uppercase();
    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: f64 = num_str.parse()?;
    
    match unit {
        "K" => Ok((num * 1024.0) as usize),
        "M" => Ok((num * 1024.0 * 1024.0) as usize),
        "G" => Ok((num * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => Ok(s.parse()?),
    }
}

/// Format size in human-readable form
pub fn format_size(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = size as f64;
    let mut unit = 0;
    
    while size >= 1024.0 && unit < 4 {
        size /= 1024.0;
        unit += 1;
    }
    
    format!("{:.2} {}", size, UNITS[unit])
}

impl FromStr for CompressionAlgorithm {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "zstd" => Ok(CompressionAlgorithm::Zstd),
            "xz" => Ok(CompressionAlgorithm::Xz),
            "none" => Ok(CompressionAlgorithm::None),
            _ => Err(anyhow::anyhow!("Unknown compression: {}", s)),
        }
    }
}

impl FromStr for HashAlgorithm {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "xxhash3" | "xxh3" => Ok(HashAlgorithm::XxHash3),
            "blake3" => Ok(HashAlgorithm::Blake3),
            "sha256" => Ok(HashAlgorithm::Sha256),
            _ => Err(anyhow::anyhow!("Unknown hash: {}", s)),
        }
    }
}

