//! Archive management

use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use anyhow::Result;
use rusqlite::{params, OptionalExtension};
use crate::storage::database::Database;
use crate::core::chunk::{Chunker, Chunk, ChunkInfo};
use crate::core::file_scanner::FileInfo;
use crate::storage::volume::{VolumeManager};
use crate::commands::restore::RestoreOptions;
use crate::utils::parse_date;
use std::collections::HashMap;
use filetime::FileTime;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug, serde::Serialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified: u64,
    pub version: u64,
    pub deleted: bool,
}

pub struct Archive {
    path: PathBuf,
    db: Database,
    chunker: Chunker,
    no_dedup: bool,
    dry_run: bool,
    volume_manager: VolumeManager,
}

impl Archive {
    /// Create new archive
    pub fn new<P: AsRef<Path>>(
        path: P,
        db: Database,
        chunker: Chunker,
        no_dedup: bool,
        dry_run: bool,
    ) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut volume_manager = VolumeManager::new(path.clone());
        volume_manager.set_database(db.clone());
        
        Self {
            path: path.clone(),
            db,
            chunker,
            no_dedup,
            dry_run,
            volume_manager,
        }
    }

    /// Open existing archive
    pub fn open<P: AsRef<Path>>(path: P, db: Database) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut volume_manager = VolumeManager::new(path.clone());
        volume_manager.set_database(db.clone());
        volume_manager.load_volumes()?;
        
        Ok(Self {
            path,
            db,
            chunker: Chunker::default(),
            no_dedup: false,
            dry_run: false,
            volume_manager,
        })
    }

    /// Open existing archive with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        db: Database,
        chunker: Chunker,
        no_dedup: bool,
        dry_run: bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut volume_manager = VolumeManager::new(path.clone());
        volume_manager.set_database(db.clone());
        volume_manager.load_volumes()?;
        
        Ok(Self {
            path,
            db,
            chunker,
            no_dedup,
            dry_run,
            volume_manager,
        })
    }

    /// Initialize volumes
    pub fn init_volumes(&mut self, volume_size: Option<u64>) -> Result<()> {
        self.volume_manager.init_volumes(volume_size)?;
        Ok(())
    }

    /// Create new backup
    pub fn create(
        &mut self,
        files: &[FileInfo],
        volume_size: Option<u64>,
        _show_progress: bool,
    ) -> Result<()> {
        self.volume_manager.init_volumes(volume_size)?;
        for file in files {
            self.process_file(file)?;
        }
        Ok(())
    }

    /// Create new backup with progress callback
    pub fn create_with_progress<F>(
        &mut self,
        files: &[FileInfo],
        volume_size: Option<u64>,
        mut progress_callback: F,
    ) -> Result<()> 
    where
        F: FnMut(&str, u64),
    {
        self.volume_manager.init_volumes(volume_size)?;
        for file in files {
            progress_callback(
                file.path.file_name().unwrap_or_default().to_str().unwrap_or("?"),
                file.size
            );
            self.process_file(file)?;
        }
        Ok(())
    }

    /// Process single file
    pub fn process_file(&mut self, file: &FileInfo) -> Result<()> {
        let chunks = self.chunker.chunk_file(&file.path)?;
        let chunk_ids = self.process_chunks(chunks)?;
        let file_id = self.db.insert_file(file, self.get_next_version(&file.path)?)?;
        
        let chunk_refs: Vec<(i64, usize)> = chunk_ids.into_iter()
            .enumerate()
            .map(|(i, id)| (id, i * self.chunker.chunk_size()))
            .collect();
        self.db.link_chunks(file_id, &chunk_refs)?;

        Ok(())
    }

    /// Process chunks (deduplication) and return chunk IDs
    fn process_chunks(&mut self, chunks: Vec<Chunk>) -> Result<Vec<i64>> {
        let mut chunk_ids = Vec::new();

        for chunk in chunks {
            if !self.no_dedup {
                if let Some(_existing) = self.db.find_chunk(&chunk.hash)? {
                    self.db.increment_refcount(&chunk.hash)?;
                    if let Some(id) = self.db.get_chunk_id_by_hash(&chunk.hash)? {
                        chunk_ids.push(id);
                        continue;
                    }
                }
            }

            let compressed = self.compress_chunk(&chunk)?;
            let (volume_num, volume_path, _) = self.volume_manager
                .find_volume_with_space(compressed.len() as u64)?;

            let offset = self.write_chunk_to_volume(&volume_path, &compressed)?;
            self.volume_manager.update_volume_free_space(volume_num, compressed.len() as u64)?;

            let chunk_info = ChunkInfo {
                hash: chunk.hash,
                fast_hash: chunk.fast_hash,
                size: chunk.size,
                compressed_size: compressed.len(),
                compression: chunk.compression,
                volume: volume_num,
                offset,
            };

            let id = self.db.insert_chunk(&chunk_info)?;
            chunk_ids.push(id);
        }

        Ok(chunk_ids)
    }

    /// Compress chunk
    fn compress_chunk(&self, chunk: &Chunk) -> Result<Vec<u8>> {
        use crate::core::compression::get_compressor;
        let mut compressor = get_compressor(chunk.compression, 3);
        Ok(compressor.compress(&chunk.data)?)
    }

    /// Write chunk to volume
    fn write_chunk_to_volume(&self, volume_path: &Path, data: &[u8]) -> Result<u64> {
        if self.dry_run {
            return Ok(0);
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(volume_path)?;

        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(data)?;
        file.write_all(&(data.len() as u64).to_le_bytes())?;

        Ok(offset)
    }

    /// Get next version number for file
    fn get_next_version(&self, path: &Path) -> Result<u64> {
        let versions = self.db.get_file_history(path.to_str().unwrap())?;
        Ok(versions.first().map(|(_, v, _)| *v + 1).unwrap_or(1))
    }

    /// Read chunk from volume
    fn read_chunk(&self, chunk: &ChunkInfo) -> Result<Vec<u8>> {
        let volume_path = self.volume_manager.get_volume_path(chunk.volume)?;
        let mut file = File::open(volume_path)?;
        file.seek(SeekFrom::Start(chunk.offset))?;
        
        let mut data = vec![0u8; chunk.compressed_size];
        file.read_exact(&mut data)?;
        
        use crate::core::compression::get_compressor;
        let mut decompressor = get_compressor(chunk.compression, 3);
        Ok(decompressor.decompress(&data)?)
    }

    /// List files in archive
    pub fn list_files(
        &self,
        patterns: Vec<String>,
        all_versions: bool,
        include_deleted: bool,
    ) -> Result<Vec<FileEntry>> {
        let conn = self.db.conn.lock().unwrap();
        
        let query = if all_versions {
            "SELECT path, size, modified_time, version, deleted FROM files"
        } else if include_deleted {
            "SELECT path, size, modified_time, version, deleted FROM files WHERE deleted = 1"
        } else {
            "SELECT path, size, modified_time, version, deleted FROM current_files"
        };
        
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            Ok(FileEntry {
                path: PathBuf::from(row.get::<_, String>(0)?),
                size: row.get::<_, i64>(1)? as u64,
                modified: row.get::<_, i64>(2)? as u64,
                version: row.get::<_, i64>(3)? as u64,
                deleted: row.get(4)?,
            })
        })?;
        
        let mut files = Vec::new();
        for row in rows {
            files.push(row?);
        }
        
        if !patterns.is_empty() {
            files.retain(|f| {
                patterns.iter().any(|p| {
                    f.path.to_string_lossy().contains(p)
                })
            });
        }
        
        Ok(files)
    }

    /// Get archive statistics
    pub fn get_stats(&self) -> Result<HashMap<String, String>> {
        Ok(self.db.get_stats()?)
    }

    /// Get chunker reference
    pub fn chunker(&self) -> &Chunker {
        &self.chunker
    }

    // ========== RESTORATION METHODS ==========

    /// Get files to restore based on options
    pub fn get_files_for_restore(&self, options: &RestoreOptions) -> Result<Vec<FileEntry>> {
        let conn = self.db.conn.lock().unwrap();
        
        let query = if let Some(version) = options.version {
            format!(
                "SELECT path, size, modified_time, version, deleted FROM files 
                 WHERE version = {}",
                version
            )
        } else if let Some(snapshot) = &options.snapshot {
            format!(
                "SELECT f.path, f.size, f.modified_time, f.version, f.deleted 
                 FROM files f
                 JOIN snapshots s ON f.id = s.file_id
                 WHERE s.snapshot_id = '{}'",
                snapshot
            )
        } else if options.all_versions {
            "SELECT path, size, modified_time, version, deleted FROM files ORDER BY path, version".to_string()
        } else {
            "SELECT path, size, modified_time, version, deleted FROM current_files".to_string()
        };
        
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map([], |row| {
            Ok(FileEntry {
                path: PathBuf::from(row.get::<_, String>(0)?),
                size: row.get::<_, i64>(1)? as u64,
                modified: row.get::<_, i64>(2)? as u64,
                version: row.get::<_, i64>(3)? as u64,
                deleted: row.get(4)?,
            })
        })?;
        
        let mut files = Vec::new();
        for row in rows {
            files.push(row?);
        }
        
        if !options.paths.is_empty() {
            files.retain(|f| {
                options.paths.iter().any(|p| {
                    f.path.to_string_lossy().contains(p.to_str().unwrap_or(""))
                })
            });
        }
        
        if let Some(as_of) = &options.as_of {
            if let Ok(timestamp) = parse_date(as_of) {
                files.retain(|f| f.modified <= timestamp);
            }
        }
        
        Ok(files)
    }

    /// Restore a single file (public version)
    pub fn restore_file_public(&self, file: &FileEntry, target_dir: &Path, options: &RestoreOptions) -> Result<()> {
        let target_path = if options.flatten {
            target_dir.join(file.path.file_name().unwrap_or_default())
        } else if let Some(strip) = options.strip_components {
            let components: Vec<_> = file.path.components().skip(strip).collect();
            if components.is_empty() {
                target_dir.join(file.path.file_name().unwrap_or_default())
            } else {
                target_dir.join(components.iter().collect::<PathBuf>())
            }
        } else {
            let mut full_path = target_dir.to_path_buf();
            
            for component in file.path.components() {
                match component {
                    std::path::Component::RootDir => continue,
                    std::path::Component::CurDir => continue,
                    std::path::Component::ParentDir => {
                        full_path.pop();
                    }
                    std::path::Component::Normal(part) => {
                        full_path.push(part);
                    }
                    _ => {
                        full_path.push(component.as_os_str());
                    }
                }
            }
            full_path
        };

        if options.dry_run {
            println!("Would restore: {} -> {}", file.path.display(), target_path.display());
            return Ok(());
        }

        if !options.should_overwrite(&target_path)? {
            println!("Skipping existing file: {}", target_path.display());
            return Ok(());
        }

        if options.backup_existing && target_path.exists() {
            let backup = target_path.with_extension("bak");
            std::fs::rename(&target_path, &backup)?;
            println!("Backed up existing file to: {}", backup.display());
        }

        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let chunks = self.get_file_chunks(file)?;

        let mut output = File::create(&target_path)?;
        
        for chunk_info in chunks {
            let data = self.read_chunk(&chunk_info)?;
            output.write_all(&data)?;
        }

        if options.preserve_times {
            let atime = FileTime::now();
            let mtime = FileTime::from_unix_time(file.modified as i64, 0);
            filetime::set_file_times(&target_path, atime, mtime)?;
        }

        #[cfg(unix)]
        if options.preserve_permissions {
            if let Some(perms) = self.get_file_permissions(file)? {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(perms))?;
            }
        }

        #[cfg(unix)]
        if options.preserve_ownership {
            if let (Some(uid), Some(gid)) = (self.get_file_uid(file)?, self.get_file_gid(file)?) {
                let _ = nix::unistd::chown(&target_path, Some(uid.into()), Some(gid.into()))?;
            }
        }

        Ok(())
    }

    /// Get chunks for a specific file
    fn get_file_chunks(&self, file: &FileEntry) -> Result<Vec<ChunkInfo>> {
        let conn = self.db.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT c.hash, c.fast_hash, c.size, c.compressed_size, c.compression,
                    c.volume_number, c.offset
             FROM chunks c
             JOIN file_chunks fc ON c.id = fc.chunk_id
             JOIN files f ON fc.file_id = f.id
             WHERE f.path = ? AND f.version = ?
             ORDER BY fc.sequence"
        )?;
        
        let rows = stmt.query_map(
            params![file.path.to_str().unwrap(), file.version as i64],
            |row| {
                Ok(ChunkInfo {
                    hash: row.get(0)?,
                    fast_hash: row.get(1)?,
                    size: row.get::<_, i64>(2)? as usize,
                    compressed_size: row.get::<_, i64>(3)? as usize,
                    compression: row.get(4)?,
                    volume: row.get::<_, i64>(5)? as u64,
                    offset: row.get::<_, i64>(6)? as u64,
                })
            }
        )?;
        
        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row?);
        }
        
        Ok(chunks)
    }

    /// Get file permissions from database
    fn get_file_permissions(&self, file: &FileEntry) -> Result<Option<u32>> {
        let conn = self.db.conn.lock().unwrap();
        
        let result = conn.query_row(
            "SELECT permissions FROM files WHERE path = ? AND version = ?",
            params![file.path.to_str().unwrap(), file.version as i64],
            |row| row.get(0)
        ).optional()?;
        
        Ok(result)
    }

    /// Get file uid from database
    fn get_file_uid(&self, file: &FileEntry) -> Result<Option<u32>> {
        let conn = self.db.conn.lock().unwrap();
        
        let result = conn.query_row(
            "SELECT uid FROM files WHERE path = ? AND version = ?",
            params![file.path.to_str().unwrap(), file.version as i64],
            |row| row.get(0)
        ).optional()?;
        
        Ok(result)
    }

    /// Get file gid from database
    fn get_file_gid(&self, file: &FileEntry) -> Result<Option<u32>> {
        let conn = self.db.conn.lock().unwrap();
        
        let result = conn.query_row(
            "SELECT gid FROM files WHERE path = ? AND version = ?",
            params![file.path.to_str().unwrap(), file.version as i64],
            |row| row.get(0)
        ).optional()?;
        
        Ok(result)
    }
}

