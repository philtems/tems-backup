//! SQLite database management for index

use rusqlite::{Connection, params};
use rusqlite::OptionalExtension;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use std::str::FromStr;
use crate::error::{Result, TemsError};
use crate::core::chunk::ChunkInfo;
use crate::core::file_scanner::FileInfo;
use crate::core::compression::CompressionAlgorithm;
use crate::core::hash::HashAlgorithm;

#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub chunk_size: usize,
    pub compression: CompressionAlgorithm,
    pub compression_level: i32,
    pub hash_algorithm: HashAlgorithm,
    pub created_at: i64,
    pub version: u32,
}

#[derive(Debug, Clone)]
pub struct Database {
    pub conn: Arc<Mutex<Connection>>,
    path: PathBuf,
    config: Arc<Mutex<Option<ArchiveConfig>>>,
}

impl Database {
    /// Open or create database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&path)?;
        
        // Configure database
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "cache_size", -64000)?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path,
            config: Arc::new(Mutex::new(None)),
        };

        db.init_schema()?;

        Ok(db)
    }

    /// Initialize database schema
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // Compression algorithms
        conn.execute(
            "CREATE TABLE IF NOT EXISTS compression_algorithms (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "INSERT OR IGNORE INTO compression_algorithms (name) VALUES 
             ('zstd'), ('xz'), ('none')",
            [],
        )?;

        // Hash algorithms
        conn.execute(
            "CREATE TABLE IF NOT EXISTS hash_algorithms (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "INSERT OR IGNORE INTO hash_algorithms (name) VALUES 
             ('xxhash3'), ('blake3'), ('sha256')",
            [],
        )?;

        // Archive configuration
        conn.execute(
            "CREATE TABLE IF NOT EXISTS archive_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                chunk_size INTEGER NOT NULL,
                compression_id INTEGER NOT NULL,
                compression_level INTEGER NOT NULL,
                hash_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                version INTEGER DEFAULT 1,
                FOREIGN KEY (compression_id) REFERENCES compression_algorithms(id),
                FOREIGN KEY (hash_id) REFERENCES hash_algorithms(id)
            )",
            [],
        )?;

        // Chunks table with hash as TEXT (hex)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL UNIQUE,
                fast_hash INTEGER NOT NULL,
                size INTEGER NOT NULL,
                compressed_size INTEGER NOT NULL,
                compression_id INTEGER NOT NULL,
                volume_number INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                reference_count INTEGER DEFAULT 1,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                last_accessed INTEGER,
                FOREIGN KEY (compression_id) REFERENCES compression_algorithms(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_fast_hash ON chunks(fast_hash)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_volume ON chunks(volume_number)",
            [],
        )?;

        // Files table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                modified_time INTEGER NOT NULL,
                created_time INTEGER,
                permissions INTEGER,
                uid INTEGER,
                gid INTEGER,
                hash TEXT,
                version INTEGER DEFAULT 1,
                deleted BOOLEAN DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)",
            [],
        )?;

        // File chunks association
        conn.execute(
            "CREATE TABLE IF NOT EXISTS file_chunks (
                file_id INTEGER NOT NULL,
                chunk_id INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                offset_in_file INTEGER,
                PRIMARY KEY (file_id, sequence),
                FOREIGN KEY (file_id) REFERENCES files(id),
                FOREIGN KEY (chunk_id) REFERENCES chunks(id)
            )",
            [],
        )?;

        // Volumes table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS volumes (
                number INTEGER PRIMARY KEY,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                max_size INTEGER,
                free_space INTEGER NOT NULL,
                status TEXT DEFAULT 'active',
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        // Current files view
        conn.execute(
            "CREATE VIEW IF NOT EXISTS current_files AS 
             SELECT f1.* FROM files f1
             WHERE f1.deleted = 0 
             AND f1.version = (
                 SELECT MAX(version) 
                 FROM files f2 
                 WHERE f2.path = f1.path
             )",
            [],
        )?;

        Ok(())
    }

    /// Save archive configuration
    pub fn save_config(&self, config: &ArchiveConfig) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        let compression_id: i64 = conn.query_row(
            "SELECT id FROM compression_algorithms WHERE name = ?",
            [config.compression.to_string()],
            |row| row.get(0)
        )?;
        
        let hash_id: i64 = conn.query_row(
            "SELECT id FROM hash_algorithms WHERE name = ?",
            [config.hash_algorithm.to_string()],
            |row| row.get(0)
        )?;
        
        conn.execute("DELETE FROM archive_config WHERE id = 1", [])?;
        
        conn.execute(
            "INSERT INTO archive_config (id, chunk_size, compression_id, compression_level, hash_id, created_at, version)
             VALUES (1, ?, ?, ?, ?, ?, ?)",
            params![
                config.chunk_size as i64,
                compression_id,
                config.compression_level,
                hash_id,
                config.created_at,
                config.version,
            ],
        )?;
        
        Ok(())
    }

    /// Load archive configuration
    pub fn load_config(&self) -> Result<Option<ArchiveConfig>> {
        let conn = self.conn.lock().unwrap();
        
        let result: Option<(i64, i64, i32, i64, i64, i64)> = conn.query_row(
            "SELECT chunk_size, compression_id, compression_level, hash_id, created_at, version
             FROM archive_config WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            }
        ).optional()?;
        
        if let Some((chunk_size, compression_id, compression_level, hash_id, created_at, version)) = result {
            let compression_name: String = conn.query_row(
                "SELECT name FROM compression_algorithms WHERE id = ?",
                [compression_id],
                |row| row.get(0)
            )?;
            
            let hash_name: String = conn.query_row(
                "SELECT name FROM hash_algorithms WHERE id = ?",
                [hash_id],
                |row| row.get(0)
            )?;
            
            let config = ArchiveConfig {
                chunk_size: chunk_size as usize,
                compression: CompressionAlgorithm::from_str(&compression_name)
                    .map_err(|e| TemsError::Compression(e.to_string()))?,
                compression_level,
                hash_algorithm: HashAlgorithm::from_str(&hash_name)
                    .map_err(|e| TemsError::Hash(e.to_string()))?,
                created_at,
                version: version as u32,
            };
            
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }

    /// Find chunk by hash
    pub fn find_chunk(&self, hash: &str) -> Result<Option<ChunkInfo>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT c.hash, c.fast_hash, c.size, c.compressed_size, ca.name,
                    c.volume_number, c.offset
             FROM chunks c
             JOIN compression_algorithms ca ON c.compression_id = ca.id
             WHERE c.hash = ?"
        )?;

        let mut rows = stmt.query([hash])?;
        
        if let Some(row) = rows.next()? {
            let fast_hash: i64 = row.get(1)?;
            let size: i64 = row.get(2)?;
            let compressed_size: i64 = row.get(3)?;
            let volume: i64 = row.get(5)?;
            let offset: i64 = row.get(6)?;
            let compression_name: String = row.get(4)?;
            
            Ok(Some(ChunkInfo {
                hash: row.get(0)?,
                fast_hash: fast_hash as u64,
                size: size as usize,
                compressed_size: compressed_size as usize,
                compression: CompressionAlgorithm::from_str(&compression_name)
                    .map_err(|e| TemsError::Compression(e.to_string()))?,
                volume: volume as u64,
                offset: offset as u64,
            }))
        } else {
            Ok(None)
        }
    }

    /// Find chunk ID by fast_hash and hash
    pub fn find_chunk_id(&self, hash: &str, fast_hash: u64) -> Result<Option<i64>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT id FROM chunks WHERE fast_hash = ? AND hash = ?"
        )?;
        
        let mut rows = stmt.query(params![fast_hash as i64, hash])?;
        
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    /// Insert new chunk
    pub fn insert_chunk(&self, chunk: &ChunkInfo) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        
        let compression_id: i64 = conn.query_row(
            "SELECT id FROM compression_algorithms WHERE name = ?",
            [chunk.compression.to_string()],
            |row| row.get(0)
        )?;
        
        conn.execute(
            "INSERT INTO chunks 
             (hash, fast_hash, size, compressed_size, compression_id, 
              volume_number, offset, reference_count)
             VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
            params![
                &chunk.hash,
                chunk.fast_hash as i64,
                chunk.size as i64,
                chunk.compressed_size as i64,
                compression_id,
                chunk.volume as i64,
                chunk.offset as i64,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Increment chunk reference count
    pub fn increment_refcount(&self, hash: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        conn.execute(
            "UPDATE chunks SET reference_count = reference_count + 1 WHERE hash = ?",
            [hash],
        )?;
        
        Ok(())
    }

    /// Insert file record
    pub fn insert_file(&self, file: &FileInfo, version: u64) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        
        let modified = file.modified.duration_since(UNIX_EPOCH)
            .unwrap_or_default().as_secs() as i64;
        let created = file.created.map(|t| 
            t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
        );

        let size = if file.size > i64::MAX as u64 {
            i64::MAX
        } else {
            file.size as i64
        };
        
        let version_i64 = version as i64;

        conn.execute(
            "INSERT INTO files 
             (path, size, modified_time, created_time, permissions, uid, gid, version)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                file.path.to_str().unwrap(),
                size,
                modified,
                created,
                file.permissions.map(|p| p as i64),
                file.uid.map(|u| u as i64),
                file.gid.map(|g| g as i64),
                version_i64,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Link chunks to file
pub fn link_chunks(&self, file_id: i64, chunk_ids: &[(i64, usize)]) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();  // ← ajout de 'mut'
        
        let tx = conn.transaction()?;
        
        for (seq, (chunk_id, offset)) in chunk_ids.iter().enumerate() {
            tx.execute(
                "INSERT INTO file_chunks (file_id, chunk_id, sequence, offset_in_file)
                 VALUES (?, ?, ?, ?)",
                params![file_id, chunk_id, seq as i64, *offset as i64],
            )?;
        }
        
        tx.commit()?;
        Ok(())
    }
    /// Get file history
    pub fn get_file_history(&self, path: &str) -> Result<Vec<(i64, u64, i64)>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT id, version, modified_time FROM files 
             WHERE path = ? ORDER BY version DESC"
        )?;

        let rows = stmt.query_map([path], |row| {
            let version: i64 = row.get(1)?;
            Ok((row.get(0)?, version as u64, row.get(2)?))
        })?;

        let mut versions = Vec::new();
        for row in rows {
            versions.push(row?);
        }
        
        Ok(versions)
    }

    /// Create new volume
    pub fn create_volume(&self, number: u64, filename: &str, size: u64, max_size: Option<u64>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        let number_i64 = number as i64;
        let size_i64 = if size > i64::MAX as u64 { i64::MAX } else { size as i64 };
        let max_size_i64 = max_size.map(|s| if s > i64::MAX as u64 { i64::MAX } else { s as i64 });
        
        let free_space = max_size.map_or(size_i64, |m| {
            let m_i64 = if m > i64::MAX as u64 { i64::MAX } else { m as i64 };
            m_i64 - size_i64
        });
        
        conn.execute(
            "INSERT INTO volumes (number, filename, size, max_size, free_space, status)
             VALUES (?, ?, ?, ?, ?, 'active')",
            params![
                number_i64,
                filename,
                size_i64,
                max_size_i64,
                free_space,
            ],
        )?;
        
        Ok(())
    }

    /// Update volume size
    pub fn update_volume_size(&self, volume: u64, written: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        let volume_i64 = volume as i64;
        let written_i64 = if written > i64::MAX as u64 { i64::MAX } else { written as i64 };
        
        conn.execute(
            "UPDATE volumes 
             SET size = size + ?, 
                 free_space = free_space - ?,
                 status = CASE 
                     WHEN free_space - ? <= 0 THEN 'full'
                     ELSE status
                 END
             WHERE number = ?",
            params![written_i64, written_i64, written_i64, volume_i64],
        )?;
        
        Ok(())
    }

    /// Get all volumes
    pub fn get_all_volumes(&self) -> Result<Vec<(u64, String, u64, u64, Option<u64>, String)>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT number, filename, size, free_space, max_size, status 
             FROM volumes ORDER BY number"
        )?;
        
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64,
                row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                row.get::<_, String>(5)?,
            ))
        })?;
        
        let mut volumes = Vec::new();
        for row in rows {
            volumes.push(row?);
        }
        
        Ok(volumes)
    }

    /// Get database stats
    pub fn get_stats(&self) -> Result<std::collections::HashMap<String, String>> {
        let conn = self.conn.lock().unwrap();
        let mut stats = std::collections::HashMap::new();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM current_files",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("files".to_string(), count.to_string());

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM chunks",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("chunks".to_string(), count.to_string());

        let size: i64 = conn.query_row(
            "SELECT SUM(size) FROM chunks",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("unique_size".to_string(), size.to_string());

        let size: i64 = conn.query_row(
            "SELECT SUM(compressed_size) FROM chunks",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("stored_size".to_string(), size.to_string());

        let total_size: i64 = conn.query_row(
            "SELECT SUM(f.size) FROM current_files f",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("total_size".to_string(), total_size.to_string());

        Ok(stats)
    }

    /// Integrity check
    pub fn integrity_check(&self) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let result: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        Ok(result == "ok")
    }

    /// Vacuum database
    pub fn vacuum(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("VACUUM", [])?;
        Ok(())
    }

    /// Get orphaned chunks
    pub fn get_orphaned_chunks(&self) -> Result<Vec<(String, u64, u64)>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT hash, volume_number, compressed_size FROM chunks 
             WHERE reference_count = 0"
        )?;

        let rows = stmt.query_map([], |row| {
            let volume: i64 = row.get(1)?;
            let size: i64 = row.get(2)?;
            Ok((row.get(0)?, volume as u64, size as u64))
        })?;

        let mut orphans = Vec::new();
        for row in rows {
            orphans.push(row?);
        }
        
        Ok(orphans)
    }

    /// Delete orphaned chunks
    pub fn delete_orphaned_chunks(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        
        let deleted = conn.execute(
            "DELETE FROM chunks WHERE reference_count = 0",
            [],
        )?;
        
        Ok(deleted)
    }
}

