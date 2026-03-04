//! Create command implementation

use clap::Args;
use std::path::{Path, PathBuf};
use anyhow::{Result, anyhow};
use crate::utils::config::Config;
use crate::core::archive::Archive;
use crate::storage::database::Database;
use crate::core::file_scanner::FileScanner;
use crate::core::chunk::Chunker;
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
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::{Arc as SyncArc, Mutex};
use std::process::Command;

// VSS implementation
#[cfg(windows)]
struct VolumeSnapshot {
    volume: String,
    shadow_id: String,
    device_path: String,
}

#[cfg(windows)]
struct VssManager {
    snapshots: SyncArc<Mutex<HashMap<String, VolumeSnapshot>>>,
    initialized: bool,
}

#[cfg(windows)]
impl VssManager {
    fn new() -> Self {
        Self {
            snapshots: SyncArc::new(Mutex::new(HashMap::new())),
            initialized: false,
        }
    }

    fn check_admin() -> bool {
        match Command::new("vssadmin").arg("list").output() {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    fn initialize(&mut self) -> Result<()> {
        if !Self::check_admin() {
            return Err(anyhow!("VSS requires administrator privileges"));
        }
        self.initialized = true;
        info!("VSS Manager initialized");
        Ok(())
    }

    fn get_volume_root(path: &Path) -> Result<String> {
        let path_str = path.to_string_lossy();
        if path_str.len() >= 2 && path_str.as_bytes()[1] == b':' {
            Ok(format!("{}:", &path_str[0..2]))
        } else {
            Err(anyhow!("Could not determine volume from path: {}", path_str))
        }
    }

    fn collect_volumes(&self, paths: &[&Path]) -> Vec<String> {
        let mut volumes = Vec::new();
        for path in paths {
            if let Ok(volume) = Self::get_volume_root(path) {
                if !volumes.contains(&volume) {
                    volumes.push(volume);
                }
            }
        }
        volumes
    }

    fn create_snapshot_for_volume(&self, volume: &str) -> Result<VolumeSnapshot> {
        info!("Creating VSS snapshot for volume {}", volume);
        
        let output = Command::new("vssadmin")
            .args(&["create", "shadow", "/for", volume])
            .output()
            .map_err(|e| anyhow!("Failed to execute vssadmin: {}", e))?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("vssadmin failed: {}", stderr));
        }
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stdout_str = stdout.to_string();
        
        let shadow_id = stdout_str
            .lines()
            .find(|line| line.contains("Shadow Copy ID:"))
            .and_then(|line| {
                line.split(':').nth(1).map(|s| s.trim().to_string())
            })
            .ok_or_else(|| anyhow!("Could not find Shadow Copy ID"))?;
        
        let device_path = stdout_str
            .lines()
            .find(|line| line.contains("Shadow Copy Volume:"))
            .and_then(|line| {
                line.split(':').nth(1).map(|s| s.trim().to_string())
            })
            .ok_or_else(|| anyhow!("Could not find Shadow Copy Volume"))?;
        
        Ok(VolumeSnapshot {
            volume: volume.to_string(),
            shadow_id,
            device_path,
        })
    }

    fn create_snapshots(&mut self, paths: &[&Path]) -> Result<()> {
        if !self.initialized {
            return Err(anyhow!("VSS Manager not initialized"));
        }

        let volumes = self.collect_volumes(paths);
        for volume in volumes {
            let mut snapshots = self.snapshots.lock().unwrap();
            if !snapshots.contains_key(&volume) {
                match self.create_snapshot_for_volume(&volume) {
                    Ok(snapshot) => {
                        snapshots.insert(volume.clone(), snapshot);
                        info!("✅ Created snapshot for volume {}", volume);
                    }
                    Err(e) => {
                        eprintln!("⚠️  Failed to create snapshot for volume {}: {}", volume, e);
                    }
                }
            }
        }
        Ok(())
    }

    fn translate_path(&self, original_path: &Path) -> Result<PathBuf> {
        let volume = Self::get_volume_root(original_path)?;
        let snapshots = self.snapshots.lock().unwrap();
        let snapshot = snapshots.get(&volume)
            .ok_or_else(|| anyhow!("No snapshot found for volume {}", volume))?;
        
        let original_str = original_path.to_string_lossy();
        let relative_path = &original_str[volume.len()..];
        let relative_path = relative_path.trim_start_matches('\\');
        
        Ok(Path::new(&snapshot.device_path).join(relative_path))
    }

    fn has_snapshot(&self, path: &Path) -> bool {
        if let Ok(volume) = Self::get_volume_root(path) {
            let snapshots = self.snapshots.lock().unwrap();
            snapshots.contains_key(&volume)
        } else {
            false
        }
    }

    fn snapshot_count(&self) -> usize {
        let snapshots = self.snapshots.lock().unwrap();
        snapshots.len()
    }
}

#[cfg(windows)]
impl Drop for VssManager {
    fn drop(&mut self) {
        let snapshots = self.snapshots.lock().unwrap();
        for (_, snapshot) in snapshots.iter() {
            let _ = Command::new("vssadmin")
                .args(&["delete", "shadows", "/shadow", &snapshot.shadow_id, "/quiet"])
                .output();
        }
        info!("VSS Manager cleaned up");
    }
}

// Stub pour non-Windows
#[cfg(not(windows))]
struct VssManager;

#[cfg(not(windows))]
impl VssManager {
    fn new() -> Self { Self }
    fn check_admin() -> bool { true }
    fn initialize(&mut self) -> Result<()> { Ok(()) }
    fn create_snapshots(&mut self, _paths: &[&Path]) -> Result<()> { Ok(()) }
    fn translate_path(&self, path: &Path) -> Result<PathBuf> { Ok(path.to_path_buf()) }
    fn has_snapshot(&self, _path: &Path) -> bool { false }
    fn snapshot_count(&self) -> usize { 0 }
}

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

    /// Number of retry attempts for failed files (-1 = infinite, 0 = no retry)
    #[arg(long, default_value_t = 0)]
    pub retry: i32,

    /// Delay between retries in seconds
    #[arg(long, default_value_t = 5)]
    pub retry_delay: u64,

    /// Use VSS (Volume Shadow Copy) on Windows to backup open files
    #[arg(long)]
    pub use_vss: bool,
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

    // Create retry configuration
    let retry_config = RetryConfig {
        max_retries: args.retry,
        delay_seconds: args.retry_delay,
    };

    // Initialize VSS if requested
    let mut vss_manager = if args.use_vss {
        if !VssManager::check_admin() {
            return Err(anyhow::anyhow!(
                "VSS requires administrator privileges. Please run as administrator."
            ));
        }
        let mut manager = VssManager::new();
        match manager.initialize() {
            Ok(()) => {
                println!("✅ VSS Manager initialized");
                Some(manager)
            }
            Err(e) => {
                return Err(anyhow::anyhow!("VSS initialization failed: {}", e));
            }
        }
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
    if args.retry != 0 {
        println!("  Retry: {} attempts, {}s delay", 
            if args.retry == -1 { "infinite".to_string() } else { args.retry.to_string() },
            args.retry_delay);
    }
    if args.use_vss {
        println!("  VSS: enabled");
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

    // Create VSS snapshots for all volumes needed
    if let Some(ref mut vss) = vss_manager {
        let path_refs: Vec<&Path> = files.iter().map(|f| f.path.as_path()).collect();
        match vss.create_snapshots(&path_refs) {
            Ok(()) => {
                println!("✅ Created {} VSS snapshots", vss.snapshot_count());
            }
            Err(e) => {
                eprintln!("⚠️  Warning: Failed to create some VSS snapshots: {}", e);
                println!("Continuing without VSS for affected volumes");
            }
        }
    }

    // Create progress bar if requested
    let progress_bar = if args.progress {
        Some(ProgressBar::new_dual_backup_bar(files.len() as u64, total_size))
    } else {
        None
    };

    let processed_files = Arc::new(AtomicU64::new(0));
    let processed_bytes = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Instant::now());
    let progress_bar_ref = progress_bar.as_ref();

    // Process files
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
            
            pb.set_files_message(format!("File: {}", file.path.display()));
            pb.set_files_speed(files_per_sec);
            pb.set_position(current_files);
        }

        // Determine which path to use (original or VSS snapshot)
        let file_path = if let Some(ref vss) = vss_manager {
            if vss.has_snapshot(&file.path) {
                match vss.translate_path(&file.path) {
                    Ok(snapshot_path) => {
                        debug!("Using VSS snapshot path: {}", snapshot_path.display());
                        snapshot_path
                    }
                    Err(_) => file.path.clone()
                }
            } else {
                file.path.clone()
            }
        } else {
            file.path.clone()
        };

        // Create a temporary FileInfo with the correct path
        let mut file_info = file.clone();
        file_info.path = file_path;

        match archive.process_file(&file_info, false, &retry_config) {
            Ok(ProcessResult::Processed) => {
                processed_bytes.fetch_add(file.size, Ordering::Relaxed);
                if let Some(pb) = progress_bar_ref {
                    pb.set_data_position(processed_bytes.load(Ordering::Relaxed));
                }
            }
            Ok(ProcessResult::Skipped) => {
                // Should not happen with newer_only=false
            }
            Err(e) => {
                eprintln!("Failed to process {}: {}", file.path.display(), e);
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
    
    println!("\nBackup completed successfully!");
    if failed > 0 {
        println!("⚠️  {} files failed and were skipped", failed);
    }
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

