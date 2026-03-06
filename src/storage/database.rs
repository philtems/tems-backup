//! SQLite database management for index with optimizations

use rusqlite::{Connection, params};
use rusqlite::types::{ToSql, FromSql, ValueRef};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::str::FromStr;
use crate::error::{Result, TemsError};
use crate::core::chunk::ChunkInfo;
use crate::core::file_scanner::FileInfo;
use crate::core::compression::CompressionAlgorithm;
use std::time::{UNIX_EPOCH};

// Taille du cache LRU (nombre d'entrées)
const CACHE_SIZE: usize = 10000;

// Statistiques de la base de données
#[derive(Debug, Default, Clone)]
pub struct DbStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub query_times_ms: Vec<f64>,
    pub total_queries: u64,
}

#[derive(Debug, Clone)]
pub struct Database {
    pub conn: Arc<Mutex<Connection>>,
    path: PathBuf,
    // Cache LRU pour les hash (fast_hash -> hash complet)
    hash_cache: Arc<Mutex<LruCache<u64, Vec<u8>>>>,
    // Statistiques
    stats: Arc<Mutex<DbStats>>,
}

impl Database {
    /// Open or create database with optimizations
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&path)?;
        
        // === OPTIMISATIONS SQLITE ===
        // Journal mode WAL pour meilleures performances en écriture
        conn.pragma_update(None, "journal_mode", "WAL")?;
        
        // Synchronous NORMAL pour équilibre sécurité/performance
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        
        // Cache size plus grand (2GB) - valeur négative = kilo-octets
        conn.pragma_update(None, "cache_size", -2000000)?;
        
        // Temp store en mémoire
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        
        // Mmap size (en octets) - utiliser une valeur qui tient dans i32
        // 1GB = 1073741824, c'est dans les limites de i32
        conn.pragma_update(None, "mmap_size", 1073741824)?; // 1GB au lieu de 30GB
        
        // Page size plus grand pour meilleure compression
        conn.pragma_update(None, "page_size", 16384)?;
        
        // Auto vacuum pour garder la base compacte
        conn.pragma_update(None, "auto_vacuum", "FULL")?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            path: path.clone(),
            hash_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(CACHE_SIZE).unwrap()
            ))),
            stats: Arc::new(Mutex::new(DbStats::default())),
        };

        // Initialize schema
        db.init_schema()?;
        
        // Optimize database
        db.optimize()?;

        Ok(db)
    }

    /// Initialize database schema with optimized indexes
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        // Vérifier si la table volumes existe
        let table_exists: i32 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='volumes'",
            [],
            |row| row.get(0)
        ).unwrap_or(0);
        log::debug!("Table 'volumes' exists: {}", table_exists > 0);

        // Table des algorithmes de compression (normalisation)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS compression_algorithms (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )",
            [],
        )?;

        // Insérer les algorithmes par défaut - utiliser execute pour INSERT
        conn.execute(
            "INSERT OR IGNORE INTO compression_algorithms (name) VALUES 
             ('zstd'), ('xz'), ('none')",
            [],
        )?;

        // Create chunks table avec index optimisés
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY,
                hash BLOB NOT NULL UNIQUE,
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

        // Index sur fast_hash pour recherches rapides
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_fast_hash ON chunks(fast_hash)",
            [],
        )?;

        // Index sur volume_number
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_volume ON chunks(volume_number)",
            [],
        )?;

        // Index sur reference_count pour GC
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_refcount ON chunks(reference_count)",
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

        // Lire la version actuelle (pour consommer le résultat)
        let _: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        // Mettre à jour la version
        conn.execute("PRAGMA user_version = 2", [])?;

        Ok(())
    }

    /// Optimize database (vacuum, reindex, analyze)
    pub fn optimize(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        log::info!("Optimizing database...");
        
        // Reconstruire les index - utiliser execute_batch
        conn.execute_batch("REINDEX;")?;
        
        // Analyser les statistiques
        conn.execute_batch("ANALYZE;")?;
        
        // Vider le WAL - PRAGMA retourne un résultat, donc on utilise query_row
        let _: i32 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
        
        // VACUUM pour compresser
        conn.execute_batch("VACUUM;")?;
        
        Ok(())
    }

    /// Get database statistics
    pub fn get_db_stats(&self) -> DbStats {
        self.stats.lock().unwrap().clone()
    }

    /// Find chunk by hash with cache optimization
    pub fn find_chunk(&self, hash: &[u8], fast_hash: u64) -> Result<Option<ChunkInfo>> {
        let start = Instant::now();
        let mut stats = self.stats.lock().unwrap();
        stats.total_queries += 1;

        // 1. Cache LRU
        {
            let mut cache = self.hash_cache.lock().unwrap();
            if let Some(cached_hash) = cache.get(&fast_hash) {
                if cached_hash == hash {
                    stats.cache_hits += 1;
                    // Cache hit, mais il faut quand même chercher les infos
                    drop(cache); // Libérer le cache avant la requête SQL
                    let result = self.get_chunk_info_by_hash(hash)?;
                    stats.query_times_ms.push(start.elapsed().as_micros() as f64 / 1000.0);
                    if stats.query_times_ms.len() > 1000 {
                        stats.query_times_ms.remove(0);
                    }
                    return Ok(result);
                }
            }
        }

        stats.cache_misses += 1;

        // 2. Recherche en base (optimisée par index)
        let result = self.find_chunk_in_db(hash, fast_hash)?;

        // 3. Mettre à jour le cache si trouvé
        if let Some(ref chunk) = result {
            let mut cache = self.hash_cache.lock().unwrap();
            cache.put(fast_hash, chunk.hash.clone());
        }

        stats.query_times_ms.push(start.elapsed().as_micros() as f64 / 1000.0);
        if stats.query_times_ms.len() > 1000 {
            stats.query_times_ms.remove(0);
        }

        Ok(result)
    }

    /// Find chunk in database (internal method)
    fn find_chunk_in_db(&self, hash: &[u8], fast_hash: u64) -> Result<Option<ChunkInfo>> {
        let conn = self.conn.lock().unwrap();
        
        // Recherche d'abord par fast_hash (index compact)
        let mut stmt = conn.prepare(
            "SELECT c.hash, c.size, c.compressed_size, ca.name,
                    c.volume_number, c.offset, c.reference_count
             FROM chunks c
             JOIN compression_algorithms ca ON c.compression_id = ca.id
             WHERE c.fast_hash = ?"
        )?;

        let mut rows = stmt.query([fast_hash as i64])?;
        
        // Parcourir les collisions potentielles (très rares)
        while let Some(row) = rows.next()? {
            let stored_hash: Vec<u8> = row.get(0)?;
            if stored_hash == hash {
                // Trouvé !
                let size: i64 = row.get(1)?;
                let compressed_size: i64 = row.get(2)?;
                let volume: i64 = row.get(4)?;
                let offset: i64 = row.get(5)?;
                
                // Convertir le nom de compression en enum
                let compression_name: String = row.get(3)?;
                let compression = CompressionAlgorithm::from_str(&compression_name)
                    .map_err(|e| TemsError::Compression(e.to_string()))?;
                
                return Ok(Some(ChunkInfo {
                    hash: stored_hash,
                    fast_hash,
                    size: size as usize,
                    compressed_size: compressed_size as usize,
                    compression,
                    volume: volume as u64,
                    offset: offset as u64,
                }));
            }
        }
        
        Ok(None)
    }

    /// Get chunk info by hash (assumes hash exists)
    fn get_chunk_info_by_hash(&self, hash: &[u8]) -> Result<Option<ChunkInfo>> {
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT c.hash, c.fast_hash, c.size, c.compressed_size, ca.name,
                    c.volume_number, c.offset, c.reference_count
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
            
            // Convertir le nom de compression en enum
            let compression_name: String = row.get(4)?;
            let compression = CompressionAlgorithm::from_str(&compression_name)
                .map_err(|e| TemsError::Compression(e.to_string()))?;
            
            Ok(Some(ChunkInfo {
                hash: row.get(0)?,
                fast_hash: fast_hash as u64,
                size: size as usize,
                compressed_size: compressed_size as usize,
                compression,
                volume: volume as u64,
                offset: offset as u64,
            }))
        } else {
            Ok(None)
        }
    }

    /// Find chunk ID by hash (fast version, doesn't keep lock)
    pub fn find_chunk_id(&self, hash: &[u8], fast_hash: u64) -> Result<Option<i64>> {
        // Prendre le lock uniquement pour cette requête
        let conn = self.conn.lock().unwrap();
        
        let mut stmt = conn.prepare(
            "SELECT id FROM chunks WHERE fast_hash = ? AND hash = ?"
        )?;
        
        let mut rows = stmt.query(params![fast_hash as i64, hash])?;
        
        if let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    /// Insert chunk or get existing ID (handles duplicates gracefully)
    pub fn insert_chunk_or_get_id(&self, chunk: &ChunkInfo) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        
        // Récupérer l'ID de l'algorithme de compression
        let compression_id: i64 = conn.query_row(
            "SELECT id FROM compression_algorithms WHERE name = ?",
            [chunk.compression.to_string()],
            |row| row.get(0)
        )?;
        
        // Pour fast_hash, utiliser conversion sécurisée
        let fast_hash = if chunk.fast_hash > i64::MAX as u64 {
            (chunk.fast_hash & i64::MAX as u64) as i64
        } else {
            chunk.fast_hash as i64
        };
        
        let size = chunk.size as i64;
        let compressed_size = chunk.compressed_size as i64;
        let volume = chunk.volume as i64;
        let offset = chunk.offset as i64;
        
        // Essayer d'insérer, ignorer si déjà existant
        conn.execute(
            "INSERT OR IGNORE INTO chunks 
             (hash, fast_hash, size, compressed_size, compression_id, 
              volume_number, offset, reference_count)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                &chunk.hash,
                fast_hash,
                size,
                compressed_size,
                compression_id,
                volume,
                offset,
                1
            ],
        )?;
        
        // Récupérer l'ID (existant ou nouvellement inséré)
        let id: i64 = conn.query_row(
            "SELECT id FROM chunks WHERE hash = ?",
            [&chunk.hash],
            |row| row.get(0)
        )?;
        
        // Mettre à jour le cache
        {
            let mut cache = self.hash_cache.lock().unwrap();
            cache.put(chunk.fast_hash, chunk.hash.clone());
        }
        
        Ok(id)
    }

    /// Get chunk ID by hash (uses cache if available)
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

    /// Create new volume with improved lock handling
    pub fn create_volume(&self, number: u64, filename: &str, size: u64, max_size: Option<u64>) -> Result<()> {
        log::debug!("Attempting to lock connection for volume {} creation", number);
        
        // Essayer d'abord avec try_lock pour éviter les blocages
        let conn = match self.conn.try_lock() {
            Ok(guard) => {
                log::debug!("Connection locked successfully for volume {}", number);
                guard
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                log::warn!("Connection is busy for volume {}, waiting...", number);
                // Si le lock est pris, on attend
                self.conn.lock().unwrap()
            }
            Err(e) => {
                log::error!("Failed to lock connection for volume {}: {:?}", number, e);
                return Err(TemsError::Database(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(5),  // SQLITE_BUSY
                    Some(format!("Database is locked: {:?}", e))
                )).into());
            }
        };
        
        log::debug!("Creating volume {} in database", number);
        
        let number_i64 = number as i64;
        let size_i64 = if size > i64::MAX as u64 { i64::MAX } else { size as i64 };
        let max_size_i64 = max_size.map(|s| if s > i64::MAX as u64 { i64::MAX } else { s as i64 });
        
        // Vérifier si le volume existe déjà
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM volumes WHERE number = ?)",
            [number_i64],
            |row| row.get(0)
        ).unwrap_or(false);
        
        if exists {
            log::warn!("Volume {} already exists in database, skipping insertion", number);
            return Ok(());
        }
        
        match conn.execute(
            "INSERT INTO volumes (number, filename, size, max_size, free_space, status)
             VALUES (?, ?, ?, ?, ?, 'active')",
            params![
                number_i64,
                filename,
                size_i64,
                max_size_i64,
                size_i64,
            ],
        ) {
            Ok(rows) => {
                log::debug!("Volume {} created in database ({} rows affected)", number, rows);
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to insert volume {}: {}", number, e);
                Err(TemsError::Database(e))
            }
        }
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
        // VACUUM retourne un résultat, donc on utilise query_row
        let _: i32 = conn.query_row("VACUUM", [], |row| row.get(0))?;
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

        // Cache stats
        let db_stats = self.stats.lock().unwrap();
        stats.insert("cache_hits".to_string(), db_stats.cache_hits.to_string());
        stats.insert("cache_misses".to_string(), db_stats.cache_misses.to_string());
        stats.insert("cache_hit_ratio".to_string(), 
            format!("{:.2}%", 
                if db_stats.total_queries > 0 {
                    (db_stats.cache_hits as f64 / db_stats.total_queries as f64) * 100.0
                } else {
                    0.0
                }
            )
        );
        
        if !db_stats.query_times_ms.is_empty() {
            let avg_time: f64 = db_stats.query_times_ms.iter().sum::<f64>() / db_stats.query_times_ms.len() as f64;
            stats.insert("avg_query_time_ms".to_string(), format!("{:.3}", avg_time));
        }

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

