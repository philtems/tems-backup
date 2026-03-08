//! Configuration management

use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use crate::error::{Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub default_compression: String,
    pub default_level: i32,
    pub default_chunk_size: String,
    pub default_hash: String,
    pub temp_dir: PathBuf,

    #[serde(default)]
    pub volumes: VolumeConfig,

    #[serde(default)]
    pub exclude: ExcludeConfig,

    #[serde(default)]
    pub advanced: AdvancedConfig,

    #[serde(default)]
    pub remote: RemoteConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub default_size: Option<String>,
    pub min_free: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcludeConfig {
    pub patterns: Vec<String>,
    pub caches: bool,
    pub larger_than: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedConfig {
    pub wal_autocheckpoint: i32,
    pub cache_size_mb: u32,
    pub verify_writes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]  // ← Default enlevé d'ici
pub struct RemoteConfig {
    pub timeout_seconds: u64,
    pub retry_attempts: u32,
    pub temp_dir: Option<PathBuf>,
    pub keep_volumes: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            default_compression: "zstd".to_string(),
            default_level: 3,
            default_chunk_size: "1M".to_string(),
            default_hash: "blake3".to_string(),
            temp_dir: std::env::temp_dir().join("tems-backup"),
            volumes: VolumeConfig::default(),
            exclude: ExcludeConfig::default(),
            advanced: AdvancedConfig::default(),
            remote: RemoteConfig::default(),
        }
    }
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            default_size: None,
            min_free: "100M".to_string(),
            prefix: "vol".to_string(),
        }
    }
}

impl Default for ExcludeConfig {
    fn default() -> Self {
        Self {
            patterns: vec![
                "*.tmp".to_string(),
                "*.log".to_string(),
                ".git/".to_string(),
                "node_modules/".to_string(),
            ],
            caches: true,
            larger_than: None,
        }
    }
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        Self {
            wal_autocheckpoint: 1000,
            cache_size_mb: 256,
            verify_writes: true,
        }
    }
}

impl Default for RemoteConfig {  // ← L'implémentation manuelle reste
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            retry_attempts: 3,
            temp_dir: None,
            keep_volumes: false,
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn load(path: Option<PathBuf>) -> Result<Self> {
        let config_path = if let Some(path) = path {
            path
        } else {
            Self::find_config_file()?
        };

        if config_path.exists() {
            let content = std::fs::read_to_string(config_path)?;
            Ok(toml::from_str(&content)?)
        } else {
            Ok(Config::default())
        }
    }

    /// Find configuration file in standard locations
    fn find_config_file() -> Result<PathBuf> {
        // Check current directory
        if let Ok(cwd) = std::env::current_dir() {
            let local = cwd.join("tems-backup.toml");
            if local.exists() {
                return Ok(local);
            }
        }

        // Check user config directory
        if let Some(config_dir) = dirs::config_dir() {
            let user_config = config_dir.join("tems-backup").join("config.toml");
            if user_config.exists() {
                return Ok(user_config);
            }
        }

        // Check home directory
        if let Some(home) = dirs::home_dir() {
            let home_config = home.join(".tems-backup.toml");
            if home_config.exists() {
                return Ok(home_config);
            }
        }

        // Return default path even if doesn't exist
        Ok(PathBuf::from("tems-backup.toml"))
    }

    /// Save configuration to file
    pub fn save(&self, path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

