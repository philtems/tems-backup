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
use crate::remote::{self, RemoteLocation, AuthInfo};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Args)]
pub struct RestoreArgs {
    /// Local archive path (required if not using --sftp or --webdav)
    #[arg(required_unless_present = "sftp", required_unless_present = "webdav")]
    pub archive: Option<PathBuf>,

    /// SFTP remote source (format: user@host:port/path)
    #[arg(long)]
    pub sftp: Option<String>,

    /// WebDAV remote source (URL: http://host/path or https://host/path)
    #[arg(long)]
    pub webdav: Option<String>,

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

    /// Keep volume files locally after use (remote mode only)
    #[arg(long)]
    pub keep_volumes: bool,
}

pub fn execute(args: RestoreArgs, config: &Config, auth_file: Option<PathBuf>) -> Result<()> {
    println!("tems-backup v{} - Restoring from archive", crate::VERSION);

    // Déterminer le mode
    let mode = if let Some(sftp_src) = &args.sftp {
        println!("Mode: SFTP remote restore");
        RemoteMode::Sftp(sftp_src.clone())
    } else if let Some(webdav_src) = &args.webdav {
        println!("Mode: WebDAV remote restore");
        RemoteMode::Webdav(webdav_src.clone())
    } else if let Some(local_path) = &args.archive {
        println!("Mode: Local restore");
        println!("Archive: {}", local_path.display());
        RemoteMode::Local(local_path.clone())
    } else {
        unreachable!("clap ensures one of these is present");
    };

    match mode {
        RemoteMode::Local(local_path) => {
            execute_local_restore(args, config, local_path)
        }
        RemoteMode::Sftp(remote_src) => {
            execute_remote_restore(args, config, auth_file, remote_src, "sftp")
        }
        RemoteMode::Webdav(remote_src) => {
            execute_remote_restore(args, config, auth_file, remote_src, "webdav")
        }
    }
}

enum RemoteMode {
    Local(PathBuf),
    Sftp(String),
    Webdav(String),
}

fn execute_local_restore(
    args: RestoreArgs,
    _config: &Config,
    archive_path: PathBuf,
) -> Result<()> {
    // Ouvrir DB locale
    let db_path = archive_path.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", archive_path.display()));
    }
    let db = Database::open(&db_path)?;

    // Ouvrir archive
    let archive = Archive::open(archive_path, db)?;

    // Options de restauration
    let target = args.target.unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&target)?;

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

    let retry_config = RetryConfig {
        max_retries: args.retry,
        delay_seconds: args.retry_delay,
    };

    // Obtenir fichiers à restaurer
    let files = archive.get_files_for_restore(&options)?;
    
    if files.is_empty() {
        println!("No files found to restore");
        return Ok(());
    }

    let total_size: u64 = files.iter().map(|f| f.size).sum();
    println!("Found {} files to restore (total: {})", files.len(), format_size(total_size));

    // Restaurer
    restore_files(&archive, files, &target, &options, &retry_config, args.progress)?;

    Ok(())
}

fn execute_remote_restore(
    args: RestoreArgs,
    config: &Config,
    auth_file: Option<PathBuf>,
    remote_src: String,
    protocol: &str,
) -> Result<()> {
    // Parser destination
    let location = if protocol == "sftp" {
        RemoteLocation::from_sftp_str(&remote_src)?
    } else {
        RemoteLocation::from_webdav_str(&remote_src)?
    };

    // Charger auth
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

    // Créer storage
    let storage = remote::create_remote_storage(location, auth, config.remote.temp_dir.clone().unwrap_or_else(|| std::env::temp_dir()))?;

    // Répertoire de travail local
    let temp_dir = config.remote.temp_dir.clone().unwrap_or_else(|| std::env::temp_dir()).join(format!("tems-backup-{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir)?;

    println!("Working directory: {}", temp_dir.display());

    // Télécharger DB
    let db_path = temp_dir.join("archive.db");
    if !storage.exists(Path::new("archive.db"))? {
        return Err(anyhow::anyhow!("Remote archive not found"));
    }
    println!("Downloading database...");
    storage.download_file(Path::new("archive.db"), &db_path)?;

    // Ouvrir DB
    let db = Database::open(&db_path)?;

    // Options de restauration
    let target = args.target.unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&target)?;

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

    // Ouvrir archive avec support remote
    let archive = Archive::open_with_remote(
        temp_dir.join("archive.tms"),
        db,
        storage,
        args.keep_volumes,
    )?;

    // Obtenir fichiers à restaurer
    let files = archive.get_files_for_restore(&options)?;
    
    if files.is_empty() {
        println!("No files found to restore");
        return Ok(());
    }

    let total_size: u64 = files.iter().map(|f| f.size).sum();
    println!("Found {} files to restore (total: {})", files.len(), format_size(total_size));

    let retry_config = RetryConfig {
        max_retries: args.retry,
        delay_seconds: args.retry_delay,
    };

    // Restaurer
    restore_files(&archive, files, &target, &options, &retry_config, args.progress)?;

    // Nettoyer
    if !args.keep_volumes {
        std::fs::remove_dir_all(temp_dir)?;
    }

    Ok(())
}

fn restore_files(
    archive: &Archive,
    files: Vec<crate::core::archive::FileEntry>,
    target: &Path,
    options: &RestoreOptions,
    retry_config: &RetryConfig,
    show_progress: bool,
) -> Result<()> {
    let total_size: u64 = files.iter().map(|f| f.size).sum();
    
    let progress_bar = if show_progress {
        Some(ProgressBar::new_dual_restore_bar(files.len() as u64, total_size))
    } else {
        None
    };

    let restored_files = Arc::new(AtomicU64::new(0));
    let restored_bytes = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicUsize::new(0));
    let start_time = Arc::new(Instant::now());

    for file in files {
        if let Some(pb) = &progress_bar {
            let current_files = restored_files.fetch_add(1, Ordering::Relaxed) + 1;
            
            let elapsed = start_time.elapsed().as_secs_f64();
            let files_per_sec = if elapsed > 0.0 {
                current_files as f64 / elapsed
            } else {
                0.0
            };
            
            pb.set_files_message(format!("Restoring: {}", file.path.display()));
            pb.set_position(current_files);
            
            if let ProgressBar::Multi(_, files_bar, _) = pb {
                files_bar.set_prefix(format!("{:.1} files/s", files_per_sec));
            }
        }

        match archive.restore_file_with_retry(&file, target, options, retry_config) {
            Ok(true) => {
                restored_bytes.fetch_add(file.size, Ordering::Relaxed);
                if let Some(pb) = &progress_bar {
                    pb.set_data_position(restored_bytes.load(Ordering::Relaxed));
                    pb.println(&format!("✅ Restored: {}", file.path.display()));
                }
            }
            Ok(false) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("⏭️  Skipped: {}", file.path.display()));
                }
            }
            Err(e) => {
                if let Some(pb) = &progress_bar {
                    pb.println(&format!("❌ Failed to restore {}: {}", file.path.display(), e));
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
            Ok(true)
        } else if self.interactive {
            use dialoguer::Confirm;
            Ok(Confirm::new()
                .with_prompt(format!("File {} exists. Overwrite?", path.display()))
                .interact()?)
        } else {
            Ok(false)
        }
    }
}

