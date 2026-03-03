//! Blob storage for chunks

use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use crate::error::{Result, TemsError};
use crate::core::chunk::ChunkInfo;

pub struct BlobStore {
    base_path: PathBuf,
}

impl BlobStore {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Write blob to volume
    pub fn write_blob(
        &self,
        volume_path: &Path,
        data: &[u8],
        chunk_info: &mut ChunkInfo,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(volume_path)?;

        let offset = file.seek(SeekFrom::End(0))?;
        
        // Write blob size
        file.write_all(&(data.len() as u64).to_le_bytes())?;
        
        // Write blob data
        file.write_all(data)?;
        
        // Write blob hash for verification
        file.write_all(&chunk_info.hash)?;

        chunk_info.offset = offset;
        
        Ok(())
    }

    /// Read blob from volume
    pub fn read_blob(&self, chunk_info: &ChunkInfo) -> Result<Vec<u8>> {
        let volume_path = self.get_volume_path(chunk_info.volume)?;
        let mut file = File::open(volume_path)?;
        
        file.seek(SeekFrom::Start(chunk_info.offset))?;
        
        // Read size
        let mut size_buf = [0u8; 8];
        file.read_exact(&mut size_buf)?;
        let size = u64::from_le_bytes(size_buf) as usize;
        
        // Read data
        let mut data = vec![0u8; size];
        file.read_exact(&mut data)?;
        
        // Read and verify hash
        let mut hash = vec![0u8; chunk_info.hash.len()];
        file.read_exact(&mut hash)?;
        
        if hash != chunk_info.hash {
            return Err(TemsError::Corrupted("Hash mismatch".into()).into());
        }
        
        Ok(data)
    }

    /// Get volume path by number
    fn get_volume_path(&self, volume: u64) -> Result<PathBuf> {
        let filename = format!("{:010}.tms", volume);
        let path = self.base_path.join("volumes").join(filename);
        
        if path.exists() {
            Ok(path)
        } else {
            Err(TemsError::VolumeNotFound(format!("Volume {}", volume)).into())
        }
    }

    /// Delete blob (mark as free space)
    pub fn delete_blob(&self, chunk_info: &ChunkInfo) -> Result<()> {
        // In a real implementation, you would:
        // 1. Mark this space as free in a free space map
        // 2. Or rewrite the volume without this blob
        
        // For now, just log
        log::debug!("Deleting blob at volume {} offset {}", 
            chunk_info.volume, chunk_info.offset);
        
        Ok(())
    }
}

