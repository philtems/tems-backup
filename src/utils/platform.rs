//! Cross-platform utilities

use std::path::{Path, PathBuf};
use crate::error::Result;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

/// Get file metadata in cross-platform way
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub size: u64,
    pub modified: i64,
    pub created: Option<i64>,
    pub permissions: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    #[cfg(windows)]
    pub attributes: Option<u32>,
}

impl FileMetadata {
    pub fn from_path(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        
        Ok(Self {
            size: metadata.len(),
            modified: metadata.modified()?
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            created: metadata.created()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64),
            permissions: Self::get_permissions(&metadata),
            uid: Self::get_uid(&metadata),
            gid: Self::get_gid(&metadata),
            #[cfg(windows)]
            attributes: Self::get_attributes(&metadata),
        })
    }

    #[cfg(unix)]
    fn get_permissions(metadata: &std::fs::Metadata) -> Option<u32> {
        Some(metadata.permissions().mode())
    }

    #[cfg(not(unix))]
    fn get_permissions(_metadata: &std::fs::Metadata) -> Option<u32> {
        None
    }

    #[cfg(unix)]
    fn get_uid(metadata: &std::fs::Metadata) -> Option<u32> {
        use std::os::unix::fs::MetadataExt;
        Some(metadata.uid())
    }

    #[cfg(not(unix))]
    fn get_uid(_metadata: &std::fs::Metadata) -> Option<u32> {
        None
    }

    #[cfg(unix)]
    fn get_gid(metadata: &std::fs::Metadata) -> Option<u32> {
        use std::os::unix::fs::MetadataExt;
        Some(metadata.gid())
    }

    #[cfg(not(unix))]
    fn get_gid(_metadata: &std::fs::Metadata) -> Option<u32> {
        None
    }

    #[cfg(windows)]
    fn get_attributes(metadata: &std::fs::Metadata) -> Option<u32> {
        Some(metadata.file_attributes())
    }
}

/// Get current user information
pub struct UserInfo {
    pub username: String,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub home_dir: Option<PathBuf>,
}

impl UserInfo {
    pub fn current() -> Self {
        Self {
            username: whoami::username(),
            uid: Self::get_uid(),
            gid: Self::get_gid(),
            home_dir: dirs::home_dir(),
        }
    }

    #[cfg(unix)]
    fn get_uid() -> Option<u32> {
        Some(users::get_current_uid())
    }

    #[cfg(not(unix))]
    fn get_uid() -> Option<u32> {
        None
    }

    #[cfg(unix)]
    fn get_gid() -> Option<u32> {
        Some(users::get_current_gid())
    }

    #[cfg(not(unix))]
    fn get_gid() -> Option<u32> {
        None
    }
}

/// Path utilities for cross-platform compatibility
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                components.pop();
            }
            std::path::Component::CurDir => {
                // Skip
            }
            _ => components.push(component),
        }
    }
    
    components.iter().collect()
}

/// Check if path is absolute (cross-platform)
pub fn is_absolute(path: &Path) -> bool {
    path.is_absolute()
}

/// Get file name with platform-specific handling
pub fn get_file_name(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_metadata() {
        let file = NamedTempFile::new().unwrap();
        let metadata = FileMetadata::from_path(file.path()).unwrap();
        
        assert!(metadata.size >= 0);
        assert!(metadata.modified > 0);
    }

    #[test]
    fn test_normalize_path() {
        let path = Path::new("a/./b/../c");
        let normalized = normalize_path(path);
        assert_eq!(normalized, Path::new("a/c"));
    }
}

