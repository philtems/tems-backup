//! Add command - add files to existing archive

use clap::Args;
use std::path::{Path, PathBuf};
use anyhow::{Result, anyhow};
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

    /// Use VSS (Volume Shadow Copy) on Windows to backup open files
    #[arg(long)]
    pub use_vss: bool,
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
    if args.use_vss {
        println!("  VSS: enabled");
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

        match archive.process_file(&file_info, args.newer_only, &retry_config) {
            Ok(ProcessResult::Processed) => {
                processed_bytes.fetch_add(file.size, Ordering::Relaxed);
                if let Some(pb) = progress_bar_ref {
                    pb.set_data_position(processed_bytes.load(Ordering::Relaxed));
                }
            }
            Ok(ProcessResult::Skipped) => {
                skipped_files.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
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

