//! Volume management for multi-volume archives

use std::path::{Path, PathBuf};
use std::fs::File;
use rusqlite::OptionalExtension;
use crate::error::{Result, TemsError};
use crate::VOLUME_NUMBER_DIGITS;
use std::collections::HashMap;
use crate::storage::database::Database;
use log::{info, debug, warn};
use crate::remote::RemoteStorage;

#[derive(Debug, Clone)]
pub struct VolumeInfo {
    pub number: u64,
    pub path: PathBuf,
    pub size: u64,
    pub max_size: Option<u64>,
    pub free_space: u64,
    pub status: VolumeStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VolumeStatus {
    Active,
    Full,
    Closed,
    Corrupted,
}

pub struct VolumeManager {
    archive_path: PathBuf,
    volumes_dir: PathBuf,
    volumes: HashMap<u64, VolumeInfo>,
    current_volume: Option<u64>,
    db: Option<Database>,
    remote_storage: Option<Box<dyn RemoteStorage>>,
    keep_volumes: bool,
}

impl VolumeManager {
    pub fn new(archive_path: PathBuf) -> Self {
        let volumes_dir = archive_path.parent()
            .unwrap_or_else(|| Path::new("."))
            .join("volumes");
        
        debug!("VolumeManager created with volumes_dir: {:?}", volumes_dir);
        
        Self {
            archive_path,
            volumes_dir,
            volumes: HashMap::new(),
            current_volume: None,
            db: None,
            remote_storage: None,
            keep_volumes: false,
        }
    }

    pub fn new_with_remote(
        archive_path: PathBuf,
        remote_storage: &Option<Box<dyn RemoteStorage>>,
        keep_volumes: bool,
    ) -> Self {
        let volumes_dir = archive_path.parent()
            .unwrap_or_else(|| Path::new("."))
            .join("volumes");
        
        debug!("VolumeManager created with remote support");
        
        Self {
            archive_path,
            volumes_dir,
            volumes: HashMap::new(),
            current_volume: None,
            db: None,
            remote_storage: remote_storage.as_ref().map(|s| s.clone_box()),
            keep_volumes,
        }
    }

    pub fn set_database(&mut self, db: Database) {
        self.db = Some(db);
        debug!("Database set in VolumeManager");
    }

    pub fn init_volumes(&mut self, volume_size: Option<u64>) -> Result<()> {
        debug!("Initializing volumes with size: {:?}", volume_size);
        std::fs::create_dir_all(&self.volumes_dir)?;

        self.load_volumes()?;
        debug!("Loaded {} existing volumes", self.volumes.len());

        if self.volumes.is_empty() {
            let default_size = volume_size.unwrap_or(1024 * 1024 * 1024);
            info!("No volumes found, creating first volume of size: {}", default_size);
            self.create_new_volume(default_size)?;
        } else if self.current_volume.is_none() {
            debug!("No active volume found");
            let mut found = false;
            for vol in self.volumes.values() {
                if vol.free_space > 0 && vol.status != VolumeStatus::Full {
                    self.current_volume = Some(vol.number);
                    info!("Selected existing volume {} as active (free space: {})", vol.number, vol.free_space);
                    found = true;
                    break;
                }
            }
            
            if !found {
                info!("No existing volume with free space, creating new volume");
                let default_size = volume_size.unwrap_or(1024 * 1024 * 1024);
                self.create_new_volume(default_size)?;
            }
        }

        Ok(())
    }

    pub fn create_new_volume(&mut self, max_size: u64) -> Result<VolumeInfo> {
        let number = self.get_next_volume_number();
        let filename = self.format_volume_filename(number);
        let path = self.volumes_dir.join(&filename);

        info!("Creating new volume {} at {:?} with max size {}", number, path, max_size);

        File::create(&path)?;
        debug!("Volume file created");

        if let Some(db) = &self.db {
            debug!("Inserting volume {} into database", number);
            db.create_volume(number, filename.as_str(), 0, Some(max_size))?;
            debug!("Volume inserted in database");
        } else {
            warn!("No database connection in VolumeManager");
        }

        let volume = VolumeInfo {
            number,
            path: path.clone(),
            size: 0,
            max_size: Some(max_size),
            free_space: max_size,
            status: VolumeStatus::Active,
        };

        self.volumes.insert(number, volume.clone());
        self.current_volume = Some(number);

        info!("Volume {} created and set as active", number);
        Ok(volume)
    }

    pub fn find_volume_with_space(&mut self, needed: u64) -> Result<(u64, PathBuf, u64)> {
        debug!("Looking for volume with at least {} bytes free", needed);
        
        self.sync_with_database()?;
        
        // Vérifier le volume actif d'abord
        if let Some(num) = self.current_volume {
            if let Some(vol) = self.volumes.get(&num) {
                if vol.free_space >= needed && vol.status != VolumeStatus::Full {
                    debug!("Using active volume {} with {} free", num, vol.free_space);
                    return Ok((vol.number, vol.path.clone(), vol.free_space));
                } else {
                    debug!("Active volume {} has only {} free, need {}", 
                           num, vol.free_space, needed);
                    
                    // Le chunk ne tient pas dans le volume actif
                    // On upload ce volume maintenant (il ne sera jamais plein)
                    self.upload_volume(num)?;
                    self.current_volume = None;
                }
            }
        }

        // Chercher un autre volume existant avec assez d'espace
        for vol in self.volumes.values() {
            if vol.free_space >= needed && vol.status != VolumeStatus::Full {
                self.current_volume = Some(vol.number);
                debug!("Found existing volume {} with {} free", vol.number, vol.free_space);
                return Ok((vol.number, vol.path.clone(), vol.free_space));
            }
        }

        // Aucun volume existant avec assez d'espace → on crée un nouveau volume
        debug!("No existing volume with enough space, creating new one");

        let size = self.get_default_volume_size()?;
        debug!("Creating new volume with size {}", size);
        let new_vol = self.create_new_volume(size)?;
        
        self.current_volume = Some(new_vol.number);
        
        Ok((new_vol.number, new_vol.path, new_vol.free_space))
    }

    pub fn update_volume_free_space(&mut self, volume: u64, used: u64) -> Result<()> {
        let mut became_full = false;
        
        if let Some(vol) = self.volumes.get_mut(&volume) {
            vol.free_space = vol.free_space.saturating_sub(used);
            vol.size += used;

            if vol.free_space == 0 {
                vol.status = VolumeStatus::Full;
                became_full = true;
                println!("✅ Volume {} is now full ({} bytes)", volume, vol.size);
            }
        };
        
        if let Some(db) = &self.db {
            db.update_volume_size(volume, used)?;
        }
        
        // Upload quand le volume est exactement plein
        if became_full {
            self.upload_volume(volume)?;
        }
        
        Ok(())
    }

    /// À appeler à la FIN du backup pour uploader le dernier volume
    pub fn upload_final_volume(&mut self) -> Result<()> {
        if let Some(volume) = self.current_volume {
            if let Some(vol) = self.volumes.get(&volume) {
                if vol.size > 0 && vol.status != VolumeStatus::Full {
                    println!("📤 Uploading final volume {} ({} bytes) to remote...", volume, vol.size);
                    self.upload_volume(volume)?;
                }
            }
        }
        Ok(())
    }

    fn upload_volume(&mut self, volume: u64) -> Result<()> {
        if let Some(storage) = &self.remote_storage {
            if let Some(vol) = self.volumes.get(&volume) {
                if vol.size > 0 {
                    let remote_dir = Path::new("volumes");
                    let remote_path = remote_dir.join(vol.path.file_name().unwrap());
                    
                    // Créer le répertoire distant si nécessaire
                    storage.create_dir(remote_dir)?;
                    
                    match storage.upload_file(&vol.path, &remote_path) {
                        Ok(_) => {
                            println!("✅ Volume {} uploaded successfully", volume);
                            
                            if !self.keep_volumes {
                                match std::fs::remove_file(&vol.path) {
                                    Ok(_) => println!("✅ Local copy of volume {} removed", volume),
                                    Err(e) => eprintln!("⚠️  Failed to remove local volume {}: {}", volume, e),
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ FAILED to upload volume {}: {}", volume, e);
                            eprintln!("   Volume remains LOCAL - server may be incomplete!");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_default_volume_size(&self) -> Result<u64> {
        if let Some(db) = &self.db {
            let conn = db.conn.lock().unwrap();
            let result: Option<i64> = conn.query_row(
                "SELECT max_size FROM volumes ORDER BY number DESC LIMIT 1",
                [],
                |row| row.get(0)
            ).optional()?;
            
            if let Some(s) = result {
                Ok(s as u64)
            } else {
                Ok(1024 * 1024 * 1024) // 1GB par défaut
            }
        } else {
            Ok(1024 * 1024 * 1024)
        }
    }

    pub fn get_volume_path(&self, number: u64) -> Result<PathBuf> {
        if let Some(vol) = self.volumes.get(&number) {
            if vol.path.exists() {
                return Ok(vol.path.clone());
            }
            
            if let Some(storage) = &self.remote_storage {
                info!("Downloading volume {} from remote", number);
                let remote_path = Path::new("volumes").join(vol.path.file_name().unwrap());
                
                if let Some(parent) = vol.path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                
                match storage.download_file(&remote_path, &vol.path) {
                    Ok(()) => {
                        info!("Volume {} downloaded successfully", number);
                        return Ok(vol.path.clone());
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to download volume {}: {}", number, e);
                    }
                }
            }
        }
        
        let filename = self.format_volume_filename(number);
        let path = self.volumes_dir.join(filename);
        
        if path.exists() {
            Ok(path)
        } else {
            Err(TemsError::VolumeNotFound(format!("Volume {}", number)).into())
        }
    }

    pub fn get_volume_info(&self, number: u64) -> Option<VolumeInfo> {
        self.volumes.get(&number).cloned()
    }

    pub fn list_volumes(&self) -> Vec<u64> {
        let mut numbers: Vec<u64> = self.volumes.keys().copied().collect();
        numbers.sort();
        numbers
    }

    fn get_next_volume_number(&self) -> u64 {
        let next = self.volumes.keys().max().copied().unwrap_or(0) + 1;
        debug!("Next volume number will be {}", next);
        next
    }

    fn format_volume_filename(&self, number: u64) -> String {
        format!("{:0width$}.tms", number, width = VOLUME_NUMBER_DIGITS)
    }

    fn sync_with_database(&mut self) -> Result<()> {
        if let Some(db) = &self.db {
            let db_volumes = db.get_all_volumes()?;
            for (number, _, size, free_space, max_size, status_str) in db_volumes {
                if let Some(vol) = self.volumes.get_mut(&number) {
                    vol.free_space = free_space;
                    vol.size = size;
                    vol.max_size = max_size;
                    vol.status = match status_str.as_str() {
                        "active" => VolumeStatus::Active,
                        "full" => VolumeStatus::Full,
                        "closed" => VolumeStatus::Closed,
                        "corrupted" => VolumeStatus::Corrupted,
                        _ => vol.status.clone(),
                    };
                }
            }
        }
        Ok(())
    }

    pub fn load_volumes(&mut self) -> Result<()> {
        debug!("Loading volumes from {:?}", self.volumes_dir);
        
        self.volumes.clear();
        
        if !self.volumes_dir.exists() {
            debug!("Volumes directory does not exist");
            return Ok(());
        }

        if let Some(db) = &self.db {
            let db_volumes = db.get_all_volumes()?;
            
            for (number, filename, size, free_space, max_size, status_str) in db_volumes {
                let path = self.volumes_dir.join(&filename);
                
                let status = match status_str.as_str() {
                    "active" => VolumeStatus::Active,
                    "full" => VolumeStatus::Full,
                    "closed" => VolumeStatus::Closed,
                    "corrupted" => VolumeStatus::Corrupted,
                    _ => VolumeStatus::Closed,
                };
                
                let volume = VolumeInfo {
                    number,
                    path,
                    size,
                    max_size,
                    free_space,
                    status,
                };
                
                self.volumes.insert(number, volume);
                debug!("Loaded volume {} from database", number);
            }
        } else {
            for entry in std::fs::read_dir(&self.volumes_dir)? {
                let entry = entry?;
                let path = entry.path();
                
                if path.extension().and_then(|e| e.to_str()) == Some("tms") {
                    if let Some(number) = self.parse_volume_number(&path) {
                        let metadata = std::fs::metadata(&path)?;
                        debug!("Found volume file {} (size: {})", number, metadata.len());
                        
                        let volume = VolumeInfo {
                            number,
                            path: path.clone(),
                            size: metadata.len(),
                            max_size: None,
                            free_space: 0,
                            status: VolumeStatus::Closed,
                        };
                        
                        self.volumes.insert(number, volume);
                    }
                }
            }
        }

        let mut max_free = 0;
        let mut active_volume = None;
        
        for (num, vol) in &self.volumes {
            if vol.free_space > max_free && vol.status != VolumeStatus::Full {
                max_free = vol.free_space;
                active_volume = Some(*num);
            }
        }
        
        self.current_volume = active_volume;
        debug!("Active volume set to {:?} with {} free", self.current_volume, max_free);

        Ok(())
    }

    fn parse_volume_number(&self, path: &Path) -> Option<u64> {
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_volume_creation() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("test.tms");
        let mut manager = VolumeManager::new(archive_path);
        
        manager.init_volumes(Some(1024 * 1024)).unwrap();
        let (num, path, free) = manager.find_volume_with_space(500).unwrap();
        
        assert_eq!(num, 1);
        assert!(path.exists());
        assert_eq!(free, 1024 * 1024);
    }

    #[test]
    fn test_multiple_volumes() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("test.tms");
        let mut manager = VolumeManager::new(archive_path);
        
        manager.init_volumes(Some(1000)).unwrap();
        
        let (num1, _, free1) = manager.find_volume_with_space(600).unwrap();
        manager.update_volume_free_space(num1, 600).unwrap();
        
        let (num2, _, free2) = manager.find_volume_with_space(600).unwrap();
        
        assert_eq!(num1, 1);
        assert_eq!(num2, 2);
        assert_eq!(free1, 400);
        assert_eq!(free2, 1000);
    }

    #[test]
    fn test_list_volumes() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("test.tms");
        let mut manager = VolumeManager::new(archive_path);
        
        manager.init_volumes(Some(1000)).unwrap();
        manager.create_new_volume(1000).unwrap();
        manager.create_new_volume(1000).unwrap();
        
        let volumes = manager.list_volumes();
        assert_eq!(volumes.len(), 3);
        assert_eq!(volumes, vec![1, 2, 3]);
    }
}

