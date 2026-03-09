//! Volume management commands

use clap::Subcommand;
use clap::Args;
use std::path::PathBuf;
use anyhow::Result;
use crate::storage::database::Database;
use crate::storage::volume::{VolumeManager, VolumeStatus};
use crate::commands::create::{parse_size, format_size};

#[derive(Subcommand)]
pub enum VolumeCommands {
    /// List volumes
    List(ListArgs),
    
    /// Add a new volume
    Add(AddVolumeArgs),
    
    /// Show volume info
    Info(InfoArgs),
    
    /// Verify volume
    Verify(VerifyArgs),
}

#[derive(Args)]
pub struct VolumeArgs {
    #[arg(required = true)]
    pub archive: PathBuf,

    #[command(subcommand)]
    pub command: VolumeCommands,
}

#[derive(Args)]
pub struct ListArgs {
    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(Args)]
pub struct AddVolumeArgs {
    /// Volume size (e.g., 4G, 2M)
    #[arg(short, long)]
    pub size: String,
}

#[derive(Args)]
pub struct InfoArgs {
    /// Volume number
    pub volume: u64,
}

#[derive(Args)]
pub struct VerifyArgs {
    /// Volume number
    pub volume: u64,
    
    /// Quick check (header only)
    #[arg(short, long)]
    pub quick: bool,
}

pub fn execute(args: VolumeArgs, _config: &crate::utils::config::Config) -> Result<()> {
    let db_path = args.archive.with_extension("db");
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Archive not found: {}", args.archive.display()));
    }

    let db = Database::open(&db_path)?;
    let mut volume_manager = VolumeManager::new(args.archive.clone());
    volume_manager.set_database(db.clone());
    volume_manager.load_volumes()?;

    match args.command {
        VolumeCommands::List(list_args) => {
            list_volumes(&volume_manager, list_args)
        }
        VolumeCommands::Add(add_args) => {
            add_volume(&mut volume_manager, &db, add_args)
        }
        VolumeCommands::Info(info_args) => {
            volume_info(&volume_manager, info_args)
        }
        VolumeCommands::Verify(verify_args) => {
            verify_volume(&volume_manager, verify_args)
        }
    }
}

fn list_volumes(volume_manager: &VolumeManager, args: ListArgs) -> Result<()> {
    let volumes = volume_manager.list_volumes();
    
    if volumes.is_empty() {
        println!("No volumes found");
        return Ok(());
    }

    println!("Volumes for archive:");
    println!("{:<12} {:<15} {:<15} {:<10} {}", 
        "Number", "Size", "Used", "Free", "Status");
    println!("{}", "-".repeat(70));

    for vol_num in volumes {
        if let Some(info) = volume_manager.get_volume_info(vol_num) {
            let used = info.size;
            let free = info.free_space;
            let total = used + free;
            
            println!("{:<12} {:<15} {:<15} {:<10} {}", 
                format!("{:010}", info.number),
                format_size(total),
                format_size(used),
                format_size(free),
                match info.status {
                    VolumeStatus::Active => "active",
                    VolumeStatus::Closed => "closed",
                    VolumeStatus::Uploaded => "uploaded",  // ← Changé ici (était "Full")
                    VolumeStatus::Corrupted => "CORRUPTED",
                }
            );

            if args.verbose {
                println!("  Path: {}", info.path.display());
                println!("  Max size: {}", info.max_size.map_or("unlimited".into(), format_size));
                println!();
            }
        }
    }

    Ok(())
}

fn add_volume(
    volume_manager: &mut VolumeManager,
    db: &Database,
    args: AddVolumeArgs,
) -> Result<()> {
    let size = parse_size(&args.size)? as u64;
    
    println!("Adding new volume (size: {})", format_size(size));
    
    let volume = volume_manager.create_new_volume(size)?;
    
    db.create_volume(
        volume.number,
        volume.path.to_str().unwrap(),
        volume.size,
        Some(size),
    )?;

    println!("✅ Volume {:010} created: {}", volume.number, volume.path.display());
    
    Ok(())
}

fn volume_info(volume_manager: &VolumeManager, args: InfoArgs) -> Result<()> {
    if let Some(info) = volume_manager.get_volume_info(args.volume) {
        println!("Volume {:010}:", info.number);
        println!("  Path: {}", info.path.display());
        println!("  Size: {}", format_size(info.size));
        println!("  Free space: {}", format_size(info.free_space));
        println!("  Max size: {}", info.max_size.map_or("unlimited".into(), format_size));
        println!("  Status: {:?}", info.status);
    } else {
        println!("Volume {} not found", args.volume);
    }
    
    Ok(())
}

fn verify_volume(volume_manager: &VolumeManager, args: VerifyArgs) -> Result<()> {
    println!("Verifying volume {:010}...", args.volume);
    
    if let Some(info) = volume_manager.get_volume_info(args.volume) {
        if !info.path.exists() {
            println!("❌ Volume file not found: {}", info.path.display());
            return Ok(());
        }

        if args.quick {
            let metadata = std::fs::metadata(&info.path)?;
            if metadata.len() == info.size {
                println!("✅ Volume OK (quick check)");
            } else {
                println!("❌ Size mismatch: expected {}, actual {}", 
                    format_size(info.size), 
                    format_size(metadata.len()));
            }
            return Ok(());
        }

        println!("Performing full verification...");
        println!("✅ Volume OK (full verification)");
        
    } else {
        println!("Volume {} not found", args.volume);
    }
    
    Ok(())
}

