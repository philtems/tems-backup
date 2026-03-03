//! SQLite database management for index

use rusqlite::{Connection, params};
use rusqlite::types::{ToSql, FromSql, ValueRef};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use crate::error::{Result};
use crate::core::chunk::ChunkInfo;
use crate::core::file_scanner::FileInfo;
use crate::core::compression::CompressionAlgorithm;
use std::time::{UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Database {
    pub conn: Arc<Mutex<Connection>>,
    path: PathBuf,
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
        conn.pragma_update(None, "cache_size", -64000)?; // 64MB cache
        conn.pragma_update(None, "temp_store", "MEMORY")?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path,
        };

        // Initialize schema
        db.init_schema()?;

        Ok(db)
    }

    /// Initialize database schema
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // Create chunks table - use INTEGER for large values
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY,
                hash BLOB NOT NULL UNIQUE,
                fast_hash INTEGER NOT NULL,
                size INTEGER NOT NULL,
                compressed_size INTEGER NOT NULL,
                compression TEXT NOT NULL,
                volume_number INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                reference_count INTEGER DEFAULT 1,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                last_accessed INTEGER
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

        // Create files table
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
                hash BLOB,
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

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_modified ON files(modified_time)",
            [],
        )?;

        // Create file_chunks association
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

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk ON file_chunks(chunk_id)",
            [],
        )?;

        // Create volumes table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS volumes (
                number INTEGER PRIMARY KEY,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL,
                max_size INTEGER,
                free_space INTEGER NOT NULL,
                status TEXT DEFAULT 'active',
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        // Create stats table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        // Create view for current files
        conn.execute(
            "CREATE VIEW IF NOT EXISTS current_files AS 
             SELECT * FROM files 
             WHERE deleted = 0 
             AND version = (
                 SELECT MAX(version) 
                 FROM files f2 
                 WHERE f2.path = files.path
             )",
            [],
        )?;

        // Set user version
        conn.execute("PRAGMA user_version = 1", [])?;

        Ok(())
    }

    /// Get connection (for internal use)
    pub fn get_connection(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }

    /// Find chunk by hash
    pub fn find_chunk(&self, hash: &[u8]) -> Result<Option<ChunkInfo>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT hash, fast_hash, size, compressed_size, compression, 
                    volume_number, offset, reference_count 
             FROM chunks WHERE hash = ?"
        )?;

        let mut rows = stmt.query([hash])?;
        
        if let Some(row) = rows.next()? {
            // Get i64 values and safely convert to u64
            let fast_hash: i64 = row.get(1)?;
            let size: i64 = row.get(2)?;
            let compressed_size: i64 = row.get(3)?;
            let volume: i64 = row.get(5)?;
            let offset: i64 = row.get(6)?;
            
            Ok(Some(ChunkInfo {
                hash: row.get(0)?,
                fast_hash: fast_hash as u64,
                size: size as usize,
                compressed_size: compressed_size as usize,
                compression: row.get(4)?,
                volume: volume as u64,
                offset: offset as u64,
            }))
        } else {
            Ok(None)
        }
    }

    /// Get chunk ID by hash
    pub fn get_chunk_id_by_hash(&self, hash: &[u8]) -> Result<Option<i64>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id FROM chunks WHERE hash = ?")?;
        let mut rows = stmt.query([hash])?;
        
        if let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    /// Insert new chunk
    pub fn insert_chunk(&self, chunk: &ChunkInfo) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        
        // For fast_hash, use safe conversion
        let fast_hash = if chunk.fast_hash > i64::MAX as u64 {
            (chunk.fast_hash & i64::MAX as u64) as i64
        } else {
            chunk.fast_hash as i64
        };
        
        // Other values are normally within limits
        let size = chunk.size as i64;
        let compressed_size = chunk.compressed_size as i64;
        let volume = chunk.volume as i64;
        let offset = chunk.offset as i64;
        
        conn.execute(
            "INSERT INTO chunks 
             (hash, fast_hash, size, compressed_size, compression, 
              volume_number, offset, reference_count)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                &chunk.hash,
                fast_hash,
                size,
                compressed_size,
                chunk.compression.to_string(),
                volume,
                offset,
                1
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Increment chunk reference count
    pub fn increment_refcount(&self, hash: &[u8]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        conn.execute(
            "UPDATE chunks SET reference_count = reference_count + 1 WHERE hash = ?",
            [hash],
        )?;
        
        Ok(())
    }

    /// Decrement chunk reference count (for GC)
    pub fn decrement_refcount(&self, hash: &[u8]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        conn.execute(
            "UPDATE chunks SET reference_count = reference_count - 1 WHERE hash = ?",
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

        // Check that size doesn't exceed i64::MAX
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
        let conn = self.conn.lock().unwrap();
        
        let mut conn = conn;
        let tx = conn.transaction()?;
        
        for (seq, (chunk_id, offset)) in chunk_ids.iter().enumerate() {
            let offset_i64 = *offset as i64;
            tx.execute(
                "INSERT INTO file_chunks (file_id, chunk_id, sequence, offset_in_file)
                 VALUES (?, ?, ?, ?)",
                params![file_id, chunk_id, seq as i64, offset_i64],
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

    /// Find volume with free space
    pub fn find_volume_with_space(&self, needed: u64) -> Result<Option<(u64, String, u64)>> {
        let conn = self.conn.lock().unwrap();
        
        // Check that needed doesn't exceed i64::MAX
        if needed > i64::MAX as u64 {
            return Ok(None);
        }
        
        let needed_i64 = needed as i64;
        
        let mut stmt = conn.prepare(
            "SELECT number, filename, free_space FROM volumes 
             WHERE status = 'active' AND free_space >= ? 
             ORDER BY free_space DESC LIMIT 1"
        )?;

        let mut rows = stmt.query([needed_i64])?;
        
        if let Some(row) = rows.next()? {
            let number: i64 = row.get(0)?;
            let free_space: i64 = row.get(2)?;
            Ok(Some((number as u64, row.get(1)?, free_space as u64)))
        } else {
            Ok(None)
        }
    }

    /// Create new volume
    pub fn create_volume(&self, number: u64, filename: &str, size: u64, max_size: Option<u64>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        let number_i64 = number as i64;
        let size_i64 = if size > i64::MAX as u64 { i64::MAX } else { size as i64 };
        let max_size_i64 = max_size.map(|s| if s > i64::MAX as u64 { i64::MAX } else { s as i64 });
        
        conn.execute(
            "INSERT INTO volumes (number, filename, size, max_size, free_space, status)
             VALUES (?, ?, ?, ?, ?, 'active')",
            params![
                number_i64,
                filename,
                size_i64,
                max_size_i64,
                size_i64,
            ],
        )?;
        
        Ok(())
    }

    /// Update volume free space
    pub fn update_volume_free_space(&self, volume: u64, used: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        let volume_i64 = volume as i64;
        let used_i64 = if used > i64::MAX as u64 { i64::MAX } else { used as i64 };
        
        conn.execute(
            "UPDATE volumes SET free_space = free_space - ? WHERE number = ?",
            params![used_i64, volume_i64],
        )?;
        
        Ok(())
    }

    /// Get orphaned chunks (reference_count = 0)
    pub fn get_orphaned_chunks(&self) -> Result<Vec<(Vec<u8>, u64, u64)>> {
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

    /// Vacuum database (optimize)
    pub fn vacuum(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("VACUUM", [])?;
        Ok(())
    }

    /// Integrity check
    pub fn integrity_check(&self) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let result: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        Ok(result == "ok")
    }

    /// Get database stats
    pub fn get_stats(&self) -> Result<std::collections::HashMap<String, String>> {
        let conn = self.conn.lock().unwrap();
        let mut stats = std::collections::HashMap::new();

        // Total files
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM current_files",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("files".to_string(), count.to_string());

        // Total chunks
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM chunks",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("chunks".to_string(), count.to_string());

        // Total unique data size
        let size: i64 = conn.query_row(
            "SELECT SUM(size) FROM chunks",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("unique_size".to_string(), size.to_string());

        // Total stored size (compressed)
        let size: i64 = conn.query_row(
            "SELECT SUM(compressed_size) FROM chunks",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("stored_size".to_string(), size.to_string());

        // Total size of all files
        let total_size: i64 = conn.query_row(
            "SELECT SUM(f.size) FROM current_files f",
            [],
            |r| r.get(0)
        ).unwrap_or(0);
        stats.insert("total_size".to_string(), total_size.to_string());

        Ok(stats)
    }
}

// Implement conversion for compression algorithm
impl ToSql for CompressionAlgorithm {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

impl FromSql for CompressionAlgorithm {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        value.as_str().map(|s| match s {
            "zstd" => CompressionAlgorithm::Zstd,
            "xz" => CompressionAlgorithm::Xz,
            "none" => CompressionAlgorithm::None,
            _ => CompressionAlgorithm::None,
        })
    }
}

