//! Volume management for multi-volume archives

use std::path::{Path, PathBuf};
use std::fs::File;
use rusqlite::OptionalExtension;
use crate::error::{Result, TemsError};
use crate::VOLUME_NUMBER_DIGITS;
use std::collections::HashMap;
use crate::storage::database::Database;

pub struct VolumeManager {
    archive_path: PathBuf,
    volumes_dir: PathBuf,
    volumes: HashMap<u64, VolumeInfo>,
    current_volume: Option<u64>,
    db: Option<Database>,
}

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

impl VolumeManager {
    pub fn new(archive_path: PathBuf) -> Self {
        let volumes_dir = archive_path.parent()
            .unwrap_or_else(|| Path::new("."))
            .join("volumes");
        
        Self {
            archive_path,
            volumes_dir,
            volumes: HashMap::new(),
            current_volume: None,
            db: None,
        }
    }

    pub fn set_database(&mut self, db: Database) {
        self.db = Some(db);
    }

    pub fn init_volumes(&mut self, volume_size: Option<u64>) -> Result<()> {
        std::fs::create_dir_all(&self.volumes_dir)?;

        if let Some(size) = volume_size {
            self.create_new_volume(size)?;
        }

        Ok(())
    }

    pub fn create_new_volume(&mut self, max_size: u64) -> Result<VolumeInfo> {
        let number = self.get_next_volume_number();
        let filename = self.format_volume_filename(number);
        let path = self.volumes_dir.join(&filename);

        File::create(&path)?;

        if let Some(db) = &self.db {
            let _ = db.create_volume(number, filename.as_str(), 0, Some(max_size))?;
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

        Ok(volume)
    }

    pub fn find_volume_with_space(&mut self, needed: u64) -> Result<(u64, PathBuf, u64)> {
        if let Some(num) = self.current_volume {
            if let Some(vol) = self.volumes.get(&num) {
                if vol.free_space >= needed && vol.status == VolumeStatus::Active {
                    return Ok((vol.number, vol.path.clone(), vol.free_space));
                }
            }
        }

        for vol in self.volumes.values() {
            if vol.free_space >= needed && vol.status == VolumeStatus::Active {
                self.current_volume = Some(vol.number);
                return Ok((vol.number, vol.path.clone(), vol.free_space));
            }
        }

        if self.volumes.is_empty() {
            return Err(TemsError::InvalidVolumeSize("No volumes exist in archive".into()).into());
        }

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
                let new_vol = self.create_new_volume(size)?;
                return Ok((new_vol.number, new_vol.path, new_vol.free_space));
            }
        }

        let max_size = self.volumes.values()
            .filter_map(|v| v.max_size)
            .max()
            .ok_or_else(|| TemsError::InvalidVolumeSize("Cannot determine volume size for new volume".into()))?;

        let new_vol = self.create_new_volume(max_size)?;
        Ok((new_vol.number, new_vol.path, new_vol.free_space))
    }

    pub fn update_volume_free_space(&mut self, volume: u64, used: u64) -> Result<()> {
        if let Some(vol) = self.volumes.get_mut(&volume) {
            vol.free_space = vol.free_space.saturating_sub(used);
            vol.size += used;

            if vol.free_space == 0 {
                vol.status = VolumeStatus::Full;
            }
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
        self.volumes.keys().max().copied().unwrap_or(0) + 1
    }

    fn format_volume_filename(&self, number: u64) -> String {
        format!("{:0width$}.tms", number, width = VOLUME_NUMBER_DIGITS)
    }

    pub fn load_volumes(&mut self) -> Result<()> {
        if !self.volumes_dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(&self.volumes_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|e| e.to_str()) == Some("tms") {
                if let Some(number) = self.parse_volume_number(&path) {
                    let metadata = std::fs::metadata(&path)?;
                    
                    let max_size = if let Some(db) = &self.db {
                        let conn = db.conn.lock().unwrap();
                        let size: Option<i64> = conn.query_row(
                            "SELECT max_size FROM volumes WHERE number = ?",
                            [number as i64],
                            |row| row.get(0)
                        ).optional()?;
                        size.map(|s| s as u64)
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
                }
            }
        }

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

