//! Volume management for multi-volume archives

use std::path::{Path, PathBuf};
use std::fs::File;
use rusqlite::OptionalExtension;
use crate::error::{Result, TemsError};
use crate::VOLUME_NUMBER_DIGITS;
use std::collections::HashMap;
use crate::storage::database::Database;
use log::{info, debug, warn};

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
    db: Option<Database>,  // Changé : on garde la Database, pas une référence
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
        }
    }

    pub fn set_database(&mut self, db: &Database) {
        // On ne peut pas stocker une référence, donc on clone ?
        // En fait, mieux vaut que Archive possède Database et VolumeManager
        // Mais pour l'instant, on garde cette approche
        self.db = Some(db.clone());
        debug!("Database set in VolumeManager");
    }

    pub fn init_volumes(&mut self, volume_size: Option<u64>) -> Result<()> {
        debug!("Initializing volumes with size: {:?}", volume_size);
        std::fs::create_dir_all(&self.volumes_dir)?;

        // Charger d'abord les volumes existants
        self.load_volumes()?;
        debug!("Loaded {} existing volumes", self.volumes.len());

        // Si aucun volume n'existe, en créer un par défaut
        if self.volumes.is_empty() {
            let default_size = volume_size.unwrap_or(1024 * 1024 * 1024); // 1GB par défaut
            info!("No volumes found, creating first volume of size: {}", default_size);
            self.create_new_volume(default_size)?;
        }
        // Si on a des volumes existants mais qu'aucun n'est actif, en créer un nouveau
        else if self.current_volume.is_none() {
            debug!("No active volume found among {} existing volumes", self.volumes.len());
            // Chercher un volume avec de l'espace libre
            let mut found = false;
            for vol in self.volumes.values() {
                if vol.free_space > 0 && vol.status == VolumeStatus::Closed {
                    self.current_volume = Some(vol.number);
                    info!("Selected existing volume {} as active (free space: {})", vol.number, vol.free_space);
                    found = true;
                    break;
                }
            }
            
            // Si pas de volume avec espace, en créer un nouveau
            if !found {
                info!("No existing volume with free space, creating new volume");
                let default_size = volume_size.unwrap_or(1024 * 1024 * 1024);
                self.create_new_volume(default_size)?;
            }
        } else {
            debug!("Active volume is {:?}", self.current_volume);
        }

        Ok(())
    }

    pub fn create_new_volume(&mut self, max_size: u64) -> Result<VolumeInfo> {
        let number = self.get_next_volume_number();
        let filename = self.format_volume_filename(number);
        let path = self.volumes_dir.join(&filename);

        info!("Creating new volume {} at {:?} with max size {}", number, path, max_size);

        // Créer le fichier physique
        File::create(&path)?;
        debug!("Volume file created");

        if let Some(db) = &self.db {
            // Vérifier si le volume existe déjà en base
            let exists: bool = {
                let conn = db.conn.lock().unwrap();
                conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM volumes WHERE number = ?)",
                    [number as i64],
                    |row| row.get(0)
                ).unwrap_or(false)
            };
            
            if !exists {
                debug!("Inserting volume {} into database", number);
                db.create_volume(number, filename.as_str(), 0, Some(max_size))?;
                debug!("Volume inserted in database");
            } else {
                warn!("Volume {} already exists in database", number);
            }
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
        
        // Vérifier d'abord si on a un volume actif avec assez d'espace
        if let Some(num) = self.current_volume {
            if let Some(vol) = self.volumes.get(&num) {
                if vol.free_space >= needed && vol.status == VolumeStatus::Active {
                    debug!("Using active volume {} with {} free", num, vol.free_space);
                    return Ok((vol.number, vol.path.clone(), vol.free_space));
                } else {
                    debug!("Active volume {} has {} free, need {}", num, vol.free_space, needed);
                }
            }
        }

        // Chercher dans tous les volumes
        for vol in self.volumes.values() {
            if vol.free_space >= needed && vol.status == VolumeStatus::Active {
                self.current_volume = Some(vol.number);
                debug!("Found volume {} with {} free", vol.number, vol.free_space);
                return Ok((vol.number, vol.path.clone(), vol.free_space));
            }
        }

        debug!("No existing volume with enough space, creating new one");

        // Si aucun volume n'a assez d'espace, en créer un nouveau
        // Déterminer la taille du nouveau volume
        let max_size = if let Some(db) = &self.db {
            let conn = db.conn.lock().unwrap();
            let size: Option<i64> = conn.query_row(
                "SELECT max_size FROM volumes ORDER BY number DESC LIMIT 1",
                [],
                |row| row.get(0)
            ).optional()?;
            size.map(|s| s as u64)
        } else {
            None
        };
        
        if let Some(size) = max_size {
            if size > 0 {
                debug!("Creating new volume with size {} from database", size);
                let new_vol = self.create_new_volume(size)?;
                return Ok((new_vol.number, new_vol.path, new_vol.free_space));
            }
        }

        // Si on ne peut pas déterminer la taille, utiliser 1GB par défaut
        let default_size = 1024 * 1024 * 1024; // 1GB
        debug!("Creating new volume with default size 1GB");
        let new_vol = self.create_new_volume(default_size)?;
        Ok((new_vol.number, new_vol.path, new_vol.free_space))
    }

    pub fn update_volume_free_space(&mut self, volume: u64, used: u64) -> Result<()> {
        if let Some(vol) = self.volumes.get_mut(&volume) {
            vol.free_space = vol.free_space.saturating_sub(used);
            vol.size += used;

            if vol.free_space == 0 {
                vol.status = VolumeStatus::Full;
                info!("Volume {} is now full", volume);
            }
            
            debug!("Volume {}: used {} bytes, {} free", volume, used, vol.free_space);
        }
        Ok(())
    }

    pub fn get_volume_path(&self, number: u64) -> Result<PathBuf> {
        if let Some(vol) = self.volumes.get(&number) {
            Ok(vol.path.clone())
        } else {
            let filename = self.format_volume_filename(number);
            let path = self.volumes_dir.join(filename);
            
            if path.exists() {
                Ok(path)
            } else {
                Err(TemsError::VolumeNotFound(format!("Volume {}", number)).into())
            }
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

    pub fn load_volumes(&mut self) -> Result<()> {
        debug!("Loading volumes from {:?}", self.volumes_dir);
        
        if !self.volumes_dir.exists() {
            debug!("Volumes directory does not exist");
            return Ok(());
        }

        let mut loaded = 0;
        for entry in std::fs::read_dir(&self.volumes_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|e| e.to_str()) == Some("tms") {
                if let Some(number) = self.parse_volume_number(&path) {
                    let metadata = std::fs::metadata(&path)?;
                    debug!("Found volume file {} (size: {})", number, metadata.len());
                    
                    let max_size = if let Some(db) = &self.db {
                        let conn = db.conn.lock().unwrap();
                        let size: Option<i64> = conn.query_row(
                            "SELECT max_size FROM volumes WHERE number = ?",
                            [number as i64],
                            |row| row.get(0)
                        ).optional()?;
                        let size = size.map(|s| s as u64);
                        debug!("Volume {} max_size from DB: {:?}", number, size);
                        size
                    } else {
                        None
                    };
                    
                    let volume = VolumeInfo {
                        number,
                        path: path.clone(),
                        size: metadata.len(),
                        max_size,
                        free_space: max_size.map_or(0, |m| m.saturating_sub(metadata.len())),
                        status: VolumeStatus::Closed,
                    };
                    
                    self.volumes.insert(number, volume);
                    loaded += 1;
                }
            }
        }

        debug!("Loaded {} volumes", loaded);

        // Déterminer le volume actif (celui avec le plus d'espace libre)
        let mut max_free = 0;
        let mut active_volume = None;
        
        for (num, vol) in &self.volumes {
            if vol.free_space > max_free {
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

