//! Archive management

use std::path::{Path, PathBuf};
use std::io::{Write, Seek, SeekFrom, Read};
use anyhow::Result;
use rusqlite::{params, OptionalExtension};
use std::str::FromStr;
use crate::storage::database::Database;
use crate::core::chunk::{Chunker, Chunk, ChunkInfo};
use crate::core::file_scanner::FileInfo;
use crate::storage::volume::{VolumeManager};
use crate::utils::retry::{with_retry, RetryConfig};
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use crate::core::compression::CompressionAlgorithm;
use crate::commands::restore::RestoreOptions;
use filetime::FileTime;
use crate::remote::RemoteStorage;

#[derive(Debug, serde::Serialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified: u64,
    pub version: u64,
    pub deleted: bool,
}

#[derive(Debug, PartialEq)]
pub enum ProcessResult {
    Processed,
    Skipped,
}

#[derive(Debug)]
pub struct FileProcessStats {
    pub total_chunks: usize,
    pub new_chunks: usize,
    pub existing_chunks: usize,
    pub compressed_size: usize,
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

    pub fn new_with_remote<P: AsRef<Path>>(
        path: P,
        db: Database,
        chunker: Chunker,
        no_dedup: bool,
        dry_run: bool,
        remote_storage: &Option<Box<dyn RemoteStorage>>,
        keep_volumes: bool,
    ) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut volume_manager = VolumeManager::new_with_remote(
            path.clone(),
            remote_storage,
            keep_volumes,
        );
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

    pub fn open_with_remote<P: AsRef<Path>>(
        path: P,
        db: Database,
        remote_storage: Box<dyn RemoteStorage>,
        keep_volumes: bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut volume_manager = VolumeManager::new_with_remote(
            path.clone(),
            &Some(remote_storage),
            keep_volumes,
        );
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

    pub fn init_volumes(&mut self, volume_size: Option<u64>) -> Result<()> {
        self.volume_manager.init_volumes(volume_size)?;
        Ok(())
    }

    pub fn upload_final_volume(&mut self) -> Result<()> {
        Ok(self.volume_manager.upload_final_volume()?)
    }

    pub fn process_file(
        &mut self,
        file: &FileInfo,
        newer_only: bool,
        retry_config: &RetryConfig,
    ) -> Result<ProcessResult> {
        let path = file.path.clone();
        
        if newer_only {
            if let Some(last_modified) = self.get_last_modified(&path)? {
                let file_modified = file.modified.duration_since(UNIX_EPOCH)
                    .unwrap_or_default().as_secs() as i64;
                
                if file_modified <= last_modified {
                    log::debug!("Skipping unchanged file: {}", path.display());
                    return Ok(ProcessResult::Skipped);
                }
            }
        }

        let result = with_retry(
            || self.process_file_internal(file),
            retry_config.max_retries,
            retry_config.delay_seconds,
            &format!("Processing file: {}", path.display()),
        );

        match result {
            Ok(stats) => {
                log::debug!("{}", self.format_file_stats(file, stats));
                Ok(ProcessResult::Processed)
            }
            Err(e) => {
                log::error!("Failed to process {}: {}", path.display(), e);
                Err(e)
            }
        }
    }

    fn format_file_stats(&self, file: &FileInfo, stats: FileProcessStats) -> String {
        let original = crate::utils::format_size(file.size);
        let compressed = crate::utils::format_size(stats.compressed_size as u64);
        let ratio = if file.size > 0 {
            stats.compressed_size as f64 / file.size as f64 * 100.0
        } else {
            0.0
        };
        
        format!(
            "File: {}\n  Original: {} | Compressed: {} | Ratio: {:.1}%\n  Chunks: {} ({} new, {} existing)",
            file.path.display(),
            original,
            compressed,
            100.0 - ratio,
            stats.total_chunks,
            stats.new_chunks,
            stats.existing_chunks
        )
    }

    fn process_file_internal(&mut self, file: &FileInfo) -> Result<FileProcessStats> {
        let chunks = self.chunker.chunk_file(&file.path)?;
        let (chunk_ids, stats) = self.process_chunks(chunks)?;
        let file_id = self.db.insert_file(file, self.get_next_version(&file.path)?)?;
        
        let chunk_refs: Vec<(i64, usize)> = chunk_ids.into_iter()
            .enumerate()
            .map(|(i, id)| (id, i * self.chunker.chunk_size()))
            .collect();
        self.db.link_chunks(file_id, &chunk_refs)?;

        Ok(stats)
    }

    fn get_last_modified(&self, path: &Path) -> Result<Option<i64>> {
        let versions = self.db.get_file_history(path.to_str().unwrap())?;
        Ok(versions.first().map(|(_, _, modified)| *modified))
    }

    fn process_chunks(&mut self, chunks: Vec<Chunk>) -> Result<(Vec<i64>, FileProcessStats)> {
        let mut chunk_ids = Vec::new();
        let mut new_chunks = 0;
        let mut existing_chunks = 0;
        let mut compressed_size = 0;
        let total_chunks = chunks.len();

        for chunk in chunks {
            if let Some(existing_id) = self.db.find_chunk_id(&chunk.hash, chunk.fast_hash)? {
                self.db.increment_refcount(&chunk.hash)?;
                chunk_ids.push(existing_id);
                
                if let Some(info) = self.db.find_chunk(&chunk.hash)? {
                    compressed_size += info.compressed_size;
                }
                
                existing_chunks += 1;
                continue;
            }

            let compressed = self.compress_chunk(&chunk)?;
            compressed_size += compressed.len();

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
            new_chunks += 1;
        }

        Ok((chunk_ids, FileProcessStats {
            total_chunks,
            new_chunks,
            existing_chunks,
            compressed_size,
        }))
    }

    fn compress_chunk(&self, chunk: &Chunk) -> Result<Vec<u8>> {
        use crate::core::compression::get_compressor;
        let mut compressor = get_compressor(chunk.compression, 3);
        Ok(compressor.compress(&chunk.data)?)
    }

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

    fn get_next_version(&self, path: &Path) -> Result<u64> {
        let versions = self.db.get_file_history(path.to_str().unwrap())?;
        Ok(versions.first().map(|(_, v, _)| *v + 1).unwrap_or(1))
    }

    pub fn get_stats(&self) -> Result<HashMap<String, String>> {
        Ok(self.db.get_stats()?)
    }

    pub fn chunker(&self) -> &Chunker {
        &self.chunker
    }

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
            if let Ok(timestamp) = crate::utils::parse_date(as_of) {
                files.retain(|f| f.modified <= timestamp);
            }
        }
        
        Ok(files)
    }

    pub fn restore_file_with_retry(
        &self,
        file: &FileEntry,
        target_dir: &Path,
        options: &RestoreOptions,
        retry_config: &RetryConfig,
    ) -> Result<bool> {
        if options.dry_run {
            println!("Would restore: {}", file.path.display());
            return Ok(true);
        }

        let result = with_retry(
            || self.restore_file_internal(file, target_dir, options),
            retry_config.max_retries,
            retry_config.delay_seconds,
            &format!("Restoring file: {}", file.path.display()),
        );

        match result {
            Ok(restored) => Ok(restored),
            Err(e) => {
                log::error!("Failed to restore {} after retries: {}", file.path.display(), e);
                Err(e)
            }
        }
    }

    fn restore_file_internal(
        &self,
        file: &FileEntry,
        target_dir: &Path,
        options: &RestoreOptions,
    ) -> Result<bool> {
        let target_path = self.build_target_path(file, target_dir, options)?;

        if !options.should_overwrite(&target_path)? {
            println!("Skipping existing file: {}", target_path.display());
            return Ok(false);
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

        let mut output = std::fs::File::create(&target_path)?;
        
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
        {
            if options.preserve_permissions {
                if let Some(perms) = self.get_file_permissions(file)? {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(perms))?;
                }
            }
        }

        Ok(true)
    }

    fn build_target_path(
        &self,
        file: &FileEntry,
        target_dir: &Path,
        options: &RestoreOptions,
    ) -> Result<PathBuf> {
        if options.flatten {
            return Ok(target_dir.join(file.path.file_name().unwrap_or_default()));
        }

        if let Some(strip) = options.strip_components {
            let components: Vec<_> = file.path.components().skip(strip).collect();
            if components.is_empty() {
                return Ok(target_dir.join(file.path.file_name().unwrap_or_default()));
            }
            return Ok(target_dir.join(components.iter().collect::<PathBuf>()));
        }

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
        Ok(full_path)
    }

    fn get_file_chunks(&self, file: &FileEntry) -> Result<Vec<ChunkInfo>> {
        let conn = self.db.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT c.hash, c.fast_hash, c.size, c.compressed_size, ca.name,
                    c.volume_number, c.offset
             FROM chunks c
             JOIN compression_algorithms ca ON c.compression_id = ca.id
             JOIN file_chunks fc ON c.id = fc.chunk_id
             JOIN files f ON fc.file_id = f.id
             WHERE f.path = ? AND f.version = ?
             ORDER BY fc.sequence"
        )?;
        
        let rows = stmt.query_map(
            params![file.path.to_str().unwrap(), file.version as i64],
            |row| {
                let compression_name: String = row.get(4)?;
                let compression = CompressionAlgorithm::from_str(&compression_name)
                    .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
                
                Ok(ChunkInfo {
                    hash: row.get(0)?,
                    fast_hash: row.get::<_, i64>(1)? as u64,
                    size: row.get::<_, i64>(2)? as usize,
                    compressed_size: row.get::<_, i64>(3)? as usize,
                    compression,
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

    fn read_chunk(&self, chunk: &ChunkInfo) -> Result<Vec<u8>> {
        let volume_path = self.volume_manager.get_volume_path(chunk.volume)?;
        let mut file = std::fs::File::open(volume_path)?;
        file.seek(SeekFrom::Start(chunk.offset))?;
        
        let mut data = vec![0u8; chunk.compressed_size];
        file.read_exact(&mut data)?;
        
        use crate::core::compression::get_compressor;
        let mut decompressor = get_compressor(chunk.compression, 3);
        Ok(decompressor.decompress(&data)?)
    }

    fn get_file_permissions(&self, file: &FileEntry) -> Result<Option<u32>> {
        let conn = self.db.conn.lock().unwrap();
        
        let result = conn.query_row(
            "SELECT permissions FROM files WHERE path = ? AND version = ?",
            params![file.path.to_str().unwrap(), file.version as i64],
            |row| row.get(0)
        ).optional()?;
        
        Ok(result)
    }
}

