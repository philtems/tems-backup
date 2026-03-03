use thiserror::Error;
use std::path::PathBuf;
use std::io;

#[derive(Error, Debug)]
pub enum TemsError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Hash error: {0}")]
    Hash(String),

    #[error("Archive not found: {0}")]
    ArchiveNotFound(PathBuf),

    #[error("Volume {0} not found")]
    VolumeNotFound(String),

    #[error("Invalid volume size: {0}")]
    InvalidVolumeSize(String),

    #[error("Path not in archive: {0}")]
    PathNotFound(String),

    #[error("Version {0} not found for {1}")]
    VersionNotFound(u64, String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Cross-platform error: {0}")]
    CrossPlatform(String),

    #[error("User cancelled")]
    UserCancelled,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Archive corrupted: {0}")]
    Corrupted(String),

    #[error("TOML serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("TOML deserialization error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("Parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("Dialog error: {0}")]
    Dialog(#[from] dialoguer::Error),

    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),

    #[error("Walkdir error: {0}")]
    Walkdir(#[from] walkdir::Error),

    #[error("Ignore error: {0}")]
    Ignore(#[from] ignore::Error),
}

pub type Result<T> = std::result::Result<T, TemsError>;

