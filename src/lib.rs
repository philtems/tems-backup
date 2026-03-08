//! # tems-backup
//!
//! Advanced backup tool with deduplication and versioning.
//!
//! ## Features
//!
//! - **Deduplication**: Split files into fixed-size chunks (default 1MB) and store duplicates only once
//! - **Versioning**: Keep history of all file changes
//! - **Compression**: Zstandard (zstd) or XZ compression
//! - **Multi-volume**: Split archives into multiple files
//! - **Remote storage**: SFTP and WebDAV support
//! - **Cross-platform**: Works on Linux, BSD, macOS, and Windows
//! - **SQLite index**: Fast and reliable metadata storage
//! - **Integrity checking**: Verify archive integrity
//! - **Garbage collection**: Remove orphaned chunks
//!
//! ## Example
//!
//! ```rust,no_run
//! use tems_backup::core::archive::Archive;
//! use tems_backup::storage::database::Database;
//! use tems_backup::core::chunk::Chunker;
//!
//! # fn main() -> anyhow::Result<()> {
//! // Open database
//! let db = Database::open("backup.db")?;
//!
//! // Create chunker with default settings
//! let chunker = Chunker::default();
//!
//! // Create archive
//! let mut archive = Archive::new(
//!     "backup.tms".into(),
//!     db,
//!     chunker,
//!     false, // dedup enabled
//!     false, // not dry run
//! );
//! # Ok(())
//! # }
//! ```
//!
//! Copyright (c) 2026 Philippe TEMESI <philippe@tems.be>

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rust_2018_idioms)]
#![warn(clippy::all)]
#![allow(clippy::module_inception)]

pub mod commands;
pub mod core;
pub mod storage;
pub mod utils;
pub mod error;
pub mod remote;  // ← AJOUTER CETTE LIGNE

/// Current version of tems-backup
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Author information
pub const AUTHOR: &str = "Philippe TEMESI <philippe@tems.be>";

/// Project website
pub const WEBSITE: &str = "https://www.tems.be";

/// Copyright year
pub const YEAR: &str = "2026";

/// Default chunk size: 1MB
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Maximum chunk size: 16MB
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Number of digits for volume numbering (10 digits = up to 9,999,999,999 volumes)
pub const VOLUME_NUMBER_DIGITS: usize = 10;

/// Volume file extension
pub const VOLUME_EXTENSION: &str = "tms";

/// Database file extension
pub const DATABASE_EXTENSION: &str = "db";

/// Magic bytes for archive identification
pub const MAGIC_BYTES: &[u8; 8] = b"TEMSBKUP";

/// Current archive format version
pub const ARCHIVE_VERSION: u32 = 1;

/// Re-export commonly used types
pub use error::{TemsError, Result};

/// Initialize the library with default settings
///
/// This function should be called before using any other library functions
/// to set up logging and other global configuration.
pub fn init() -> Result<()> {
    // Initialize logging if not already done
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    log::debug!("tems-backup v{} initialized", VERSION);
    log::debug!("Running on: {}", std::env::consts::OS);
    log::debug!("Architecture: {}", std::env::consts::ARCH);
    
    Ok(())
}

/// Get build information
pub fn build_info() -> BuildInfo {
    BuildInfo {
        version: VERSION,
        rust_version: rustc_version_runtime::version().to_string(),
        target_triple: std::env::consts::OS.to_string(),
        features: std::env::var("CARGO_CFG_FEATURES").unwrap_or_default(),
        build_time: chrono::Utc::now().to_rfc3339(),
    }
}

/// Build information structure
#[derive(Debug, Clone, serde::Serialize)]
pub struct BuildInfo {
    /// tems-backup version
    pub version: &'static str,
    /// Rust compiler version
    pub rust_version: String,
    /// Target triple
    pub target_triple: String,
    /// Enabled features
    pub features: String,
    /// Build timestamp
    pub build_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(DEFAULT_CHUNK_SIZE >= 1024);
        assert!(MAX_CHUNK_SIZE > DEFAULT_CHUNK_SIZE);
        assert_eq!(MAGIC_BYTES.len(), 8);
        assert_eq!(ARCHIVE_VERSION, 1);
    }

    #[test]
    fn test_build_info() {
        let info = build_info();
        assert_eq!(info.version, VERSION);
        assert!(!info.rust_version.is_empty());
        assert!(!info.target_triple.is_empty());
    }
}

/// Platform-specific extensions
pub mod platform {
    //! Platform-specific utilities and extensions

    #[cfg(unix)]
    pub mod unix {
        //! Unix-specific extensions
        use std::os::unix::fs::PermissionsExt;
        
        /// Get file mode (permissions) as u32
        pub fn get_mode(metadata: &std::fs::Metadata) -> u32 {
            metadata.permissions().mode()
        }
        
        /// Check if file is a symlink
        pub fn is_symlink(metadata: &std::fs::Metadata) -> bool {
            metadata.file_type().is_symlink()
        }
    }

    #[cfg(windows)]
    pub mod windows {
        //! Windows-specific extensions
        use std::os::windows::fs::MetadataExt;
        
        /// Get file attributes
        pub fn get_attributes(metadata: &std::fs::Metadata) -> u32 {
            metadata.file_attributes()
        }
        
        /// Check if file is a reparse point
        pub fn is_reparse_point(metadata: &std::fs::Metadata) -> bool {
            metadata.file_attributes() & 0x400 != 0
        }
    }
}

/// Prelude module for easy importing of common types
pub mod prelude {
    //! Prelude for easy importing of common types
    pub use crate::core::archive::Archive;
    pub use crate::core::chunk::{Chunk, Chunker, ChunkInfo};
    pub use crate::core::file_scanner::{FileInfo, FileScanner};
    pub use crate::core::hash::{HashAlgorithm, Hasher};
    pub use crate::core::compression::{CompressionAlgorithm, Compressor};
    pub use crate::storage::database::Database;
    pub use crate::storage::volume::{VolumeManager, VolumeInfo, VolumeStatus};
    pub use crate::error::{TemsError, Result};
    pub use crate::{VERSION, AUTHOR, WEBSITE, YEAR};
}

/// Version requirements for interoperability
pub mod compat {
    //! Compatibility and version requirements
    
    /// Minimum compatible database schema version
    pub const MIN_DB_VERSION: u32 = 1;
    
    /// Maximum compatible database schema version
    pub const MAX_DB_VERSION: u32 = 1;
    
    /// Minimum compatible archive format version
    pub const MIN_ARCHIVE_VERSION: u32 = 1;
    
    /// Maximum compatible archive format version
    pub const MAX_ARCHIVE_VERSION: u32 = 1;
    
    /// Check if database version is compatible
    pub fn is_db_version_compatible(version: u32) -> bool {
        version >= MIN_DB_VERSION && version <= MAX_DB_VERSION
    }
    
    /// Check if archive version is compatible
    pub fn is_archive_version_compatible(version: u32) -> bool {
        version >= MIN_ARCHIVE_VERSION && version <= MAX_ARCHIVE_VERSION
    }
}

/// Memory and performance tuning constants
pub mod tuning {
    //! Performance tuning constants
    
    /// Default SQLite cache size in KB
    pub const DEFAULT_SQLITE_CACHE_KB: i32 = -64000; // 64MB
    
    /// Maximum number of chunks to process in parallel
    pub const MAX_PARALLEL_CHUNKS: usize = 1000;
    
    /// Default WAL autocheckpoint size in pages
    pub const DEFAULT_WAL_AUTOCHECKPOINT: i32 = 1000;
    
    /// Suggested memory limit for index (in bytes)
    pub const SUGGESTED_INDEX_MEMORY: usize = 512 * 1024 * 1024; // 512MB
    
    /// Optimal I/O buffer size
    pub const IO_BUFFER_SIZE: usize = 64 * 1024; // 64KB
}

#[doc(hidden)]
/// Internal macros
pub mod macros {
    /// Macro for creating a boxed error
    #[macro_export]
    macro_rules! box_error {
        ($e:expr) => {
            Box::new($e) as Box<dyn std::error::Error + Send + Sync>
        };
    }
    
    /// Macro for checking if a feature is enabled
    #[macro_export]
    macro_rules! feature_enabled {
        ($feature:tt) => {
            #[cfg(feature = $feature)]
            const ENABLED: bool = true;
            #[cfg(not(feature = $feature))]
            const ENABLED: bool = false;
            ENABLED
        };
    }
}

