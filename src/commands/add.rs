//! Add command - add files to existing archive

use clap::Args;
use std::path::{Path, PathBuf};
use anyhow::Result;
use crate::utils::config::Config;
use crate::core::archive::Archive;
use crate::storage::database::Database;
use crate::core::file_scanner::FileScanner;
use crate::core::chunk::Chunker;
use crate::commands::create::{parse_size, format_size};
use crate::utils::progress::ProgressBar;
use crate::utils::parse_duration;
use crate::utils::retry::RetryConfig;
use crate::core::archive::ProcessResult;
use crate::remote::{self, RemoteLocation, AuthInfo};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;
use std::time::Instant;

#[derive(Args)]
pub struct AddArgs {
    /// Local archive path (required if not using --sftp or --webdav)
    #[arg(required_unless_present = "sftp", required_unless_present = "webdav")]
    pub archive: Option<PathBuf>,

    /// SFTP remote destination (format: user@host:port/path)
    #[arg(long)]
    pub sftp: Option<String>,

    /// WebDAV remote destination (URL: http://host/path or https://host/path)
    #[arg(long)]
    pub webdav: Option<String>,

    /// Files/directories to add
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,

    /// Compression algorithm (override archive default) - warning only
    #[arg(short = 'C', long)]
    pub compression: Option<String>,

    /// Compression level (override archive default) - warning only
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

    /// Keep volume files locally after upload (remote mode only)
    #[arg(long)]
    pub keep_volumes: bool,
}

pub fn execute(args: AddArgs, config: &Config, auth_file: Option<PathBuf>) -> Result<()> {
    println!("tems-backup v{} - Adding to archive", crate::VERSION);

    let exclude = args.exclude.clone();
    let include = args.include.clone();
    let exclude_caches = args.exclude_caches;
    let no_dedup = args.no_dedup;
    let dry_run = args.dry_run;
    let progress = args.progress;
    let retry = args.retry;
    let retry_delay = args.retry_delay;
    let newer_only = args.newer_only;
    let keep_volumes = args.keep_volumes;
    let paths = args.paths.clone();
    let volume_size_str = args.volume_size.clone();
    let max_age = args.max_age.clone();
    let compression_opt = args.compression.clone();
    let compress_level_opt = args.compress_level;

    let mode = if let Some(sftp_dest) = &args.sftp {
        println!("Mode: SFTP remote add");
        RemoteMode::Sftp(sftp_dest.clone())
    } else if let Some(webdav_dest) = &args.webdav {
        println!("Mode: WebDAV remote add");
        RemoteMode::Webdav(webdav_dest.clone())
    } else if let Some(local_path) = &args.archive {
        println!("Mode: Local add");
        println!("Archive: {}", local_path.display());
        RemoteMode::Local(local_path.clone())
    } else {
        unreachable!("clap ensures one of these is present");
    };

    let max_age_seconds = if let Some(age) = &max_age {
        Some(parse_duration(age)?)
    } else {
        None
    };

    match mode {
        RemoteMode::Local(local_path) => {
            execute_local_add(
                local_path,
                paths,
                exclude,
                include,
                exclude_caches,
                no_dedup,
                dry_run,
                progress,
                newer_only,
                retry,
                retry_delay,
                volume_size_str,
                max_age_seconds,
                compression_opt,
                compress_level_opt,
                config,
            )
        }
        RemoteMode::Sftp(remote_dest) => {
            execute_remote_add(
                remote_dest,
                paths,
                exclude,
                include,
                exclude_caches,
                no_dedup,
                dry_run,
                progress,
                newer_only,
                retry,
                retry_delay,
                keep_volumes,
                volume_size_str,
                max_age_seconds,
                compression_opt,
                compress_level_opt,
                config,
                auth_file,
                "sftp",
            )
        }
        RemoteMode::Webdav(remote_dest) => {
            execute_remote_add(
                remote_dest,
                paths,
                exclude,
                include,
                exclude_caches,
                no_dedup,
                dry_run,
                progress,
                newer_only,
                retry,
                retry_delay,
                keep_volumes,
                volume_size_str,
                max_age_seconds,
                compression_opt,
                compress_level_opt,
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
fn execute_local_add(
    archive_path: PathBuf,
    paths: Vec<PathBuf>,
    exclude: Vec<String>,
    include: Vec<String>,
    exclude_caches: bool,
    no_dedup: bool,
    dry_run: bool,
    progress: bool,
    newer_only: bool,
    retry: i32,
    retry_delay: u64,
    volume_size_str: Option<String>,
    max_age_seconds: Option<u64>,
    _compression_opt: Option<String>,
    _compress_level_opt: Option<i32>,
    config: &Config,
) -> Result<()> {
    let db_path = archive_path.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", archive_path.display()));
    }

    let db = Database::open(&db_path)?;
    
    let archive_config = match db.load_config()? {
        Some(config) => config,
        None => return Err(anyhow::anyhow!("Archive has no configuration")),
    };
    
    println!("Archive configuration loaded:");
    println!("  Chunk size: {}", format_size(archive_config.chunk_size as u64));
    println!("  Compression: {} (level {})", archive_config.compression, archive_config.compression_level);
    println!("  Hash: {}", archive_config.hash_algorithm);
    
    let chunker = Chunker::new(
        archive_config.chunk_size,
        archive_config.hash_algorithm,
        archive_config.compression,
        archive_config.compression_level,
    );

    let volume_size = if let Some(size) = volume_size_str {
        Some(parse_size(&size)? as u64)
    } else {
        None
    };

    let retry_config = RetryConfig {
        max_retries: retry,
        delay_seconds: retry_delay,
    };

    let mut scanner = FileScanner::new(
        if exclude.is_empty() { config.exclude.patterns.clone() } else { exclude },
        include,
        exclude_caches || config.exclude.caches,
    );
    scanner.set_hidden(true);

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
    println!("Found {} files to add (total: {})", files.len(), format_size(total_size));
    
    if files.is_empty() {
        println!("No files to add (none match the criteria)");
        return Ok(());
    }

    if newer_only {
        println!("Mode: newer-only (skipping unchanged files)");
    }

    let mut archive = Archive::open_with_config(
        archive_path,
        db,
        chunker,
        no_dedup,
        dry_run,
    )?;

    archive.init_volumes(volume_size)?;

    process_add_files(&mut archive, files, progress, newer_only, &retry_config)?;

    let stats = archive.get_stats()?;
    println!("\n✅ Addition completed successfully!");
    println!("Total files in archive: {}", stats.get("files").unwrap_or(&"0".to_string()));

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_remote_add(
    remote_dest: String,
    paths: Vec<PathBuf>,
    exclude: Vec<String>,
    include: Vec<String>,
    exclude_caches: bool,
    no_dedup: bool,
    dry_run: bool,
    progress: bool,
    newer_only: bool,
    retry: i32,
    retry_delay: u64,
    keep_volumes: bool,
    volume_size_str: Option<String>,
    max_age_seconds: Option<u64>,
    _compression_opt: Option<String>,
    _compress_level_opt: Option<i32>,
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
            return Err(anyhow::anyhow!("Username required"));
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
    
    if temp_dir.exists() {
        println!("🧹 Nettoyage du dossier temporaire existant...");
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
    
    std::fs::create_dir_all(&temp_dir)?;

    println!("📁 Local temporary directory: {}", temp_dir.display());
    println!("   Do NOT interrupt the process during uploads!\n");

    let db_path = temp_dir.join("archive.db");
    
    // Vérifier si la DB distante existe (elle DOIT exister pour un add)
    if !storage.exists(Path::new("archive.db"))? {
        return Err(anyhow::anyhow!("Remote archive not found. Use 'create' command first."));
    }
    
    println!("Downloading database...");
    
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

    let db = Database::open(&db_path)?;
    
    let archive_config = match db.load_config()? {
        Some(config) => config,
        None => return Err(anyhow::anyhow!("Archive has no configuration")),
    };
    
    println!("Archive configuration loaded:");
    println!("  Chunk size: {}", format_size(archive_config.chunk_size as u64));
    println!("  Compression: {} (level {})", archive_config.compression, archive_config.compression_level);
    println!("  Hash: {}", archive_config.hash_algorithm);
    
    let chunker = Chunker::new(
        archive_config.chunk_size,
        archive_config.hash_algorithm,
        archive_config.compression,
        archive_config.compression_level,
    );

    let volume_size = if let Some(size) = volume_size_str {
        Some(parse_size(&size)? as u64)
    } else {
        None
    };

    let retry_config = RetryConfig {
        max_retries: retry,
        delay_seconds: retry_delay,
    };

    let mut scanner = FileScanner::new(
        if exclude.is_empty() { config.exclude.patterns.clone() } else { exclude },
        include,
        exclude_caches || config.exclude.caches,
    );
    scanner.set_hidden(true);

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
    println!("Found {} files to add (total: {})", files.len(), format_size(total_size));
    
    if files.is_empty() {
        println!("No files to add (none match the criteria)");
        return Ok(());
    }

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

    archive.init_volumes(volume_size)?;

    process_add_files_remote(&mut archive, files, progress, newer_only, &retry_config)?;

    println!("\n📤 Uploading final volume...");
    archive.upload_final_volume()?;

    println!("📤 Uploading updated database...");
    
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

    println!("\n🔍 Verifying remote files...");
    match storage.list_files(Path::new("volumes")) {
        Ok(files) => println!("   Remote volumes: {} files", files.len()),
        Err(e) => eprintln!("   Could not list remote volumes: {}", e),
    }

    if !keep_volumes {
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
    }

    println!("\n✅ Remote addition completed successfully!");

    Ok(())
}

fn process_add_files(
    archive: &mut Archive,
    files: Vec<crate::core::file_scanner::FileInfo>,
    show_progress: bool,
    newer_only: bool,
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
    let skipped_files = Arc::new(AtomicUsize::new(0));
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
            
            pb.set_message(format!("Adding: {}", file.path.display()));
            pb.set_position(current);
        }
        
        match archive.process_file(file, newer_only, retry_config) {
            Ok(ProcessResult::Processed) => {}
            Ok(ProcessResult::Skipped) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("⏭️  Skipped: {}", file.path.display()));
                }
                skipped_files.fetch_add(1, Ordering::Relaxed);
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
    let skipped = skipped_files.load(Ordering::Relaxed);
    
    if skipped > 0 {
        println!("⏭️  {} files skipped (unchanged)", skipped);
    }
    if failed > 0 {
        println!("⚠️  {} files failed and were not added", failed);
    }

    Ok(())
}

fn process_add_files_remote(
    archive: &mut Archive,
    files: Vec<crate::core::file_scanner::FileInfo>,
    show_progress: bool,
    newer_only: bool,
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
    let skipped_files = Arc::new(AtomicUsize::new(0));
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
            
            pb.set_message(format!("Adding: {}", file.path.display()));
            pb.set_position(current);
        }
        
        match archive.process_file(file, newer_only, retry_config) {
            Ok(ProcessResult::Processed) => {}
            Ok(ProcessResult::Skipped) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("⏭️  Skipped: {}", file.path.display()));
                }
                skipped_files.fetch_add(1, Ordering::Relaxed);
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
    let skipped = skipped_files.load(Ordering::Relaxed);
    
    if skipped > 0 {
        println!("⏭️  {} files skipped (unchanged)", skipped);
    }
    if failed > 0 {
        println!("⚠️  {} files failed and were not added", failed);
    }

    Ok(())
}

