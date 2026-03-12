//! Create command implementation

use clap::Args;
use std::path::{Path, PathBuf};
use anyhow::Result;
use crate::utils::config::Config;
use crate::core::archive::Archive;
use crate::storage::database::Database;
use crate::storage::database::ArchiveConfig;
use crate::core::file_scanner::FileScanner;
use crate::core::chunk::Chunker;
use crate::core::compression::CompressionAlgorithm;
use crate::core::hash::HashAlgorithm;
use crate::utils::progress::ProgressBar;
use crate::utils::parse_duration;
use crate::utils::retry::RetryConfig;
use crate::core::archive::ProcessResult;
use crate::remote::{self, RemoteLocation, AuthInfo};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Instant;

#[derive(Args)]
pub struct CreateArgs {
    /// Local archive path (required if not using --sftp or --webdav)
    #[arg(required_unless_present = "sftp", required_unless_present = "webdav")]
    pub archive: Option<PathBuf>,

    /// SFTP remote destination (format: user@host:port/path)
    #[arg(long)]
    pub sftp: Option<String>,

    /// WebDAV remote destination (URL: http://host/path or https://host/path)
    #[arg(long)]
    pub webdav: Option<String>,

    /// Files/directories to backup
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    /// Compression algorithm: zstd, xz, none
    #[arg(short = 'C', long, default_value = "zstd")]
    pub compression: String,

    /// Compression level (1-19 for zstd, 1-9 for xz)
    #[arg(short = 'l', long, default_value_t = 3)]
    pub compress_level: i32,

    /// Disable deduplication
    #[arg(long)]
    pub no_dedup: bool,

    /// Chunk size in bytes (with suffix: K, M, G)
    #[arg(short = 'c', long, default_value = "1M")]
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

    /// Only include files modified within this duration (e.g., 1d, 12h, 30m)
    #[arg(long)]
    pub max_age: Option<String>,

    /// Number of retry attempts for failed files (-1 = infinite, 0 = no retry)
    #[arg(long, default_value_t = 0)]
    pub retry: i32,

    /// Delay between retries in seconds
    #[arg(long, default_value_t = 5)]
    pub retry_delay: u64,

    /// Keep volume files locally after upload (remote mode only)
    #[arg(long)]
    pub keep_volumes: bool,
}

pub fn execute(args: CreateArgs, config: &Config, auth_file: Option<PathBuf>) -> Result<()> {
    println!("tems-backup v{} - Copyright (c) {} {}", 
        crate::VERSION, crate::YEAR, crate::AUTHOR);

    let exclude = args.exclude.clone();
    let include = args.include.clone();
    let exclude_caches = args.exclude_caches;
    let no_dedup = args.no_dedup;
    let dry_run = args.dry_run;
    let progress = args.progress;
    let retry = args.retry;
    let retry_delay = args.retry_delay;
    let compress_level = args.compress_level;
    let keep_volumes = args.keep_volumes;
    let paths = args.paths.clone();
    let compression_str = args.compression.clone();
    let chunk_size_str = args.chunk_size.clone();
    let hash_str = args.hash.clone();
    let max_age = args.max_age.clone();
    let volume_size_str = args.volume_size.clone();

    let mode = if let Some(sftp_dest) = &args.sftp {
        println!("Mode: SFTP remote backup");
        RemoteMode::Sftp(sftp_dest.clone())
    } else if let Some(webdav_dest) = &args.webdav {
        println!("Mode: WebDAV remote backup");
        RemoteMode::Webdav(webdav_dest.clone())
    } else if let Some(local_path) = &args.archive {
        println!("Mode: Local backup");
        println!("Creating archive: {}", local_path.display());
        RemoteMode::Local(local_path.clone())
    } else {
        unreachable!("clap ensures one of these is present");
    };

    let chunk_size = parse_size(&chunk_size_str)?;
    let compression = CompressionAlgorithm::from_str(&compression_str)?;
    let hash_algo = HashAlgorithm::from_str(&hash_str)?;
    
    let max_age_seconds = if let Some(age) = &max_age {
        Some(parse_duration(age)?)
    } else {
        None
    };

    let volume_size = if let Some(size) = volume_size_str {
        Some(parse_size(&size)? as u64)
    } else {
        None
    };

    print_config(
        &compression_str,
        compress_level,
        no_dedup,
        chunk_size,
        volume_size,
        max_age_seconds,
        retry,
        retry_delay,
    );

    match mode {
        RemoteMode::Local(local_path) => {
            execute_local(
                local_path,
                paths,
                exclude,
                include,
                exclude_caches,
                no_dedup,
                dry_run,
                progress,
                retry,
                retry_delay,
                chunk_size,
                compression,
                compress_level,
                hash_algo,
                volume_size,
                max_age_seconds,
                config,
            )
        }
        RemoteMode::Sftp(remote_dest) => {
            execute_remote(
                remote_dest,
                paths,
                exclude,
                include,
                exclude_caches,
                no_dedup,
                dry_run,
                progress,
                retry,
                retry_delay,
                keep_volumes,
                chunk_size,
                compression,
                compress_level,
                hash_algo,
                volume_size,
                max_age_seconds,
                config,
                auth_file,
                "sftp",
            )
        }
        RemoteMode::Webdav(remote_dest) => {
            execute_remote(
                remote_dest,
                paths,
                exclude,
                include,
                exclude_caches,
                no_dedup,
                dry_run,
                progress,
                retry,
                retry_delay,
                keep_volumes,
                chunk_size,
                compression,
                compress_level,
                hash_algo,
                volume_size,
                max_age_seconds,
                config,
                auth_file,
                "webdav",
            )
        }
    }
}

enum RemoteMode {
    Local(PathBuf),
    Sftp(String),
    Webdav(String),
}

#[allow(clippy::too_many_arguments)]
fn execute_local(
    archive_path: PathBuf,
    paths: Vec<PathBuf>,
    exclude: Vec<String>,
    include: Vec<String>,
    exclude_caches: bool,
    no_dedup: bool,
    dry_run: bool,
    progress: bool,
    retry: i32,
    retry_delay: u64,
    chunk_size: usize,
    compression: CompressionAlgorithm,
    compress_level: i32,
    hash_algo: HashAlgorithm,
    volume_size: Option<u64>,
    max_age_seconds: Option<u64>,
    config: &Config,
) -> Result<()> {
    let db_path = archive_path.with_extension("db");
    let db = Database::open(&db_path)?;
    
    let archive_config = ArchiveConfig {
        chunk_size,
        compression,
        compression_level: compress_level,
        hash_algorithm: hash_algo,
        created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64,
        version: 1,
    };
    db.save_config(&archive_config)?;
    
    let chunker = Chunker::new(
        chunk_size,
        hash_algo,
        compression,
        compress_level,
    );

    let mut scanner = FileScanner::new(
        if exclude.is_empty() { config.exclude.patterns.clone() } else { exclude },
        include,
        exclude_caches || config.exclude.caches,
    );
    scanner.set_hidden(true);

    let mut archive = Archive::new(
        archive_path,
        db,
        chunker,
        no_dedup,
        dry_run,
    );

    let retry_config = RetryConfig {
        max_retries: retry,
        delay_seconds: retry_delay,
    };

    let files = scan_files(paths, &mut scanner, max_age_seconds)?;
    if files.is_empty() {
        println!("No files to backup (none match the criteria)");
        return Ok(());
    }

    archive.init_volumes(volume_size)?;

    process_files(&mut archive, files, progress, retry, &retry_config)?;

    let stats = archive.get_stats()?;
    print_final_stats(stats, no_dedup);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_remote(
    remote_dest: String,
    paths: Vec<PathBuf>,
    exclude: Vec<String>,
    include: Vec<String>,
    exclude_caches: bool,
    no_dedup: bool,
    dry_run: bool,
    progress: bool,
    retry: i32,
    retry_delay: u64,
    keep_volumes: bool,
    chunk_size: usize,
    compression: CompressionAlgorithm,
    compress_level: i32,
    hash_algo: HashAlgorithm,
    volume_size: Option<u64>,
    max_age_seconds: Option<u64>,
    config: &Config,
    auth_file: Option<PathBuf>,
    protocol: &str,
) -> Result<()> {
    println!("\n⚠️  REMOTE MODE: Files are uploaded only when volumes are full.");
    
    let location = if protocol == "sftp" {
        RemoteLocation::from_sftp_str(&remote_dest)?
    } else {
        RemoteLocation::from_webdav_str(&remote_dest)?
    };

    let auth = if let Some(auth_path) = auth_file {
        AuthInfo::from_file(&auth_path, Some(&location.user))?
    } else {
        if location.user.is_empty() {
            return Err(anyhow::anyhow!("Username required for remote access. Provide via --auth-file or in the destination string."));
        }
        let password = rpassword::prompt_password("Password: ")?;
        AuthInfo {
            username: location.user.clone(),
            password: Some(password),
            key_file: None,
            passphrase: None,
        }
    };

    let storage = remote::create_remote_storage(location, auth, config.remote.temp_dir.clone().unwrap_or_else(|| std::env::temp_dir()))?;

    storage.create_dir(Path::new(""))?;
    storage.create_dir(Path::new("volumes"))?;
    println!("📁 Remote directories ready");

    let temp_dir = config.remote.temp_dir.clone()
        .unwrap_or_else(|| std::env::temp_dir())
        .join(format!("tems-backup-{}", std::process::id()));
    
    // Nettoyer si le dossier existe déjà (processus précédent crashé)
    if temp_dir.exists() {
        println!("🧹 Nettoyage du dossier temporaire existant...");
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
    
    std::fs::create_dir_all(&temp_dir)?;

    println!("📁 Local temporary directory: {}", temp_dir.display());
    println!("   Do NOT interrupt the process during uploads!\n");

    let db_path = temp_dir.join("archive.db");
    let db_exists = storage.exists(Path::new("archive.db"))?;

    if db_exists {
        println!("Found existing remote database, downloading...");
        let max_retries = 3;
        for attempt in 1..=max_retries {
            match storage.download_file(Path::new("archive.db"), &db_path) {
                Ok(_) => break,
                Err(e) => {
                    if attempt == max_retries {
                        return Err(anyhow::anyhow!("Failed to download database after {} attempts: {}", max_retries, e));
                    }
                    println!("⚠️  Download failed (attempt {}/{}), retrying in 5s...", attempt, max_retries);
                    std::thread::sleep(std::time::Duration::from_secs(5));
                }
            }
        }
    } else {
        println!("No existing remote database found, creating new one");
    }

    let db = Database::open(&db_path)?;

    // Sauvegarder la configuration seulement si c'est une nouvelle archive
    if !db_exists {
        let archive_config = ArchiveConfig {
            chunk_size,
            compression,
            compression_level: compress_level,
            hash_algorithm: hash_algo,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64,
            version: 1,
        };
        db.save_config(&archive_config)?;
    } else {
        // Vérifier que la configuration existante est compatible
        let existing_config = db.load_config()?
            .ok_or_else(|| anyhow::anyhow!("Remote archive has no configuration"))?;
        
        if existing_config.chunk_size != chunk_size {
            println!("⚠️  Warning: Using existing chunk size {} instead of requested {}", 
                existing_config.chunk_size, chunk_size);
        }
        if existing_config.compression != compression {
            println!("⚠️  Warning: Using existing compression {} instead of requested {}", 
                existing_config.compression, compression);
        }
    }

    let chunker = Chunker::new(
        chunk_size,
        hash_algo,
        compression,
        compress_level,
    );

    let mut scanner = FileScanner::new(
        if exclude.is_empty() { config.exclude.patterns.clone() } else { exclude },
        include,
        exclude_caches || config.exclude.caches,
    );
    scanner.set_hidden(true);

    let mut storage_opt = Some(storage);
    
    let mut archive = Archive::new_with_remote(
        temp_dir.join("archive.tms"),
        db,
        chunker,
        no_dedup,
        dry_run,
        &storage_opt,
        keep_volumes,
    );
    
    let storage = storage_opt.take().unwrap();

    let retry_config = RetryConfig {
        max_retries: retry,
        delay_seconds: retry_delay,
    };

    let files = scan_files(paths, &mut scanner, max_age_seconds)?;
    if files.is_empty() {
        println!("No files to backup (none match the criteria)");
        return Ok(());
    }

    archive.init_volumes(volume_size)?;

    process_files_remote(&mut archive, files, progress, retry, &retry_config)?;

    println!("\n📤 Uploading final volume...");
    archive.upload_final_volume()?;

    println!("📤 Uploading updated database...");
    
    // Upload avec retry
    let max_retries = 3;
    for attempt in 1..=max_retries {
        match storage.upload_file(&db_path, Path::new("archive.db")) {
            Ok(_) => break,
            Err(e) => {
                if attempt == max_retries {
                    return Err(anyhow::anyhow!("Failed to upload database after {} attempts: {}", max_retries, e));
                }
                println!("⚠️  Upload failed (attempt {}/{}), retrying in 5s...", attempt, max_retries);
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    }
    
    println!("✅ Database uploaded successfully");

    // Vérifier que la DB est bien présente
    if storage.exists(Path::new("archive.db"))? {
        println!("✅ Database verified on remote");
    } else {
        println!("⚠️  Warning: Database not found on remote after upload");
    }

    println!("\n🔍 Verifying remote files...");
    match storage.list_files(Path::new("volumes")) {
        Ok(files) => println!("   Remote volumes: {} files", files.len()),
        Err(e) => eprintln!("   Could not list remote volumes: {}", e),
    }

    // Nettoyage amélioré
    if !keep_volumes {
        println!("🧹 Cleaning temporary directory...");
        
        // Essayer plusieurs fois de supprimer (parfois les fichiers sont encore utilisés)
        for attempt in 1..=3 {
            match std::fs::remove_dir_all(&temp_dir) {
                Ok(_) => {
                    println!("🧹 Local temporary directory cleaned");
                    break;
                }
                Err(e) => {
                    if attempt == 3 {
                        println!("⚠️  Could not clean temp dir (will be cleaned on next run): {}", e);
                    } else {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                }
            }
        }
    } else {
        println!("📁 Keeping temporary files in: {}", temp_dir.display());
    }

    let stats = archive.get_stats()?;
    print_final_stats(stats, no_dedup);

    Ok(())
}

fn print_config(
    compression_arg: &str,
    compress_level: i32,
    no_dedup: bool,
    chunk_size: usize,
    volume_size: Option<u64>,
    max_age_seconds: Option<u64>,
    retry: i32,
    retry_delay: u64,
) {
    println!("\nConfiguration:");
    println!("  Compression: {} (level {})", compression_arg, compress_level);
    println!("  Chunk size: {}", format_size(chunk_size as u64));
    if let Some(vs) = volume_size {
        println!("  Volume size: {}", format_size(vs));
    }
    if let Some(age) = max_age_seconds {
        println!("  Max age: {} seconds", age);
    }
    println!("  Deduplication: {}", if no_dedup { "off" } else { "on" });
    if retry != 0 {
        println!("  Retry: {} attempts, {}s delay", 
            if retry == -1 { "infinite".to_string() } else { retry.to_string() },
            retry_delay);
    }
    println!();
}

fn scan_files(
    paths: Vec<PathBuf>,
    scanner: &mut FileScanner,
    max_age_seconds: Option<u64>,
) -> Result<Vec<crate::core::file_scanner::FileInfo>> {
    println!("Scanning files...");
    let all_files = scanner.scan_paths(&paths)?;
    
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
    
    Ok(files)
}

fn process_files(
    archive: &mut Archive,
    files: Vec<crate::core::file_scanner::FileInfo>,
    show_progress: bool,
    _retry_count: i32,
    retry_config: &RetryConfig,
) -> Result<()> {
    let progress_bar = if show_progress {
        Some(ProgressBar::new_backup_bar(files.len() as u64))
    } else {
        None
    };

    let _progress_bar_ref = progress_bar.as_ref();

    let processed_files = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Instant::now());

    for file in &files {
        if let Some(pb) = &progress_bar {
            let current = processed_files.fetch_add(1, Ordering::Relaxed) + 1;
            
            let elapsed = start_time.elapsed().as_secs_f64();
            let _files_per_sec = if elapsed > 0.0 {
                current as f64 / elapsed
            } else {
                0.0
            };
            
            pb.set_message(format!("Processing: {}", file.path.display()));
            pb.set_position(current);
        }
        
        match archive.process_file(file, false, retry_config) {
            Ok(ProcessResult::Processed) => {}
            Ok(ProcessResult::Skipped) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("⏭️  Skipped: {}", file.path.display()));
                }
            }
            Err(e) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("❌ Failed: {} - {}", file.path.display(), e));
                }
                failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    let failed = failed_files.load(Ordering::Relaxed);
    if failed > 0 {
        println!("⚠️  {} files failed and were skipped", failed);
    }

    Ok(())
}

fn process_files_remote(
    archive: &mut Archive,
    files: Vec<crate::core::file_scanner::FileInfo>,
    show_progress: bool,
    _retry_count: i32,
    retry_config: &RetryConfig,
) -> Result<()> {
    let progress_bar = if show_progress {
        Some(ProgressBar::new_backup_bar(files.len() as u64))
    } else {
        None
    };

    if let Some(pb) = &progress_bar {
        pb.set_prefix("📤 Remote: waiting for full volumes...".to_string());
    }

    let processed_files = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Instant::now());

    for file in &files {
        if let Some(pb) = &progress_bar {
            let current = processed_files.fetch_add(1, Ordering::Relaxed) + 1;
            
            let elapsed = start_time.elapsed().as_secs_f64();
            let _files_per_sec = if elapsed > 0.0 {
                current as f64 / elapsed
            } else {
                0.0
            };
            
            pb.set_message(format!("Processing: {}", file.path.display()));
            pb.set_position(current);
        }
        
        match archive.process_file(file, false, retry_config) {
            Ok(ProcessResult::Processed) => {}
            Ok(ProcessResult::Skipped) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("⏭️  Skipped: {}", file.path.display()));
                }
            }
            Err(e) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("❌ Failed: {} - {}", file.path.display(), e));
                }
                failed_files.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish();
    }

    let failed = failed_files.load(Ordering::Relaxed);
    if failed > 0 {
        println!("⚠️  {} files failed and were skipped", failed);
    }

    Ok(())
}

fn print_final_stats(stats: std::collections::HashMap<String, String>, no_dedup: bool) {
    println!("\n✅ Backup completed successfully!");
    println!("Summary:");
    println!("  Files backed up: {}", stats.get("files").unwrap_or(&"0".to_string()));
    println!("  Unique chunks: {}", stats.get("chunks").unwrap_or(&"0".to_string()));
    println!("  Total size: {}", stats.get("total_size").unwrap_or(&"0".to_string()));
    println!("  Stored size: {}", stats.get("stored_size").unwrap_or(&"0".to_string()));

    if !no_dedup {
        let total: f64 = stats.get("total_size").unwrap_or(&"0".to_string()).parse().unwrap_or(0.0);
        let unique: f64 = stats.get("unique_size").unwrap_or(&"1".to_string()).parse().unwrap_or(1.0);
        if unique > 0.0 {
            let dedup_ratio = total / unique;
            println!("  Deduplication ratio: {:.2}x", dedup_ratio);
        }
    }
}

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

pub fn format_size(size: u64) -> String {
    crate::utils::format_size(size)
}

