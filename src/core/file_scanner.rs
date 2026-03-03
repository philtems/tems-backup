//! File system scanner with filtering

use std::path::{Path, PathBuf};
use std::time::SystemTime;
use ignore::{WalkBuilder};
use anyhow::Result;
use rayon::prelude::*;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub size: u64,
    pub modified: SystemTime,
    pub created: Option<SystemTime>,
    pub permissions: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

pub struct FileScanner {
    exclude_patterns: Vec<String>,
    include_patterns: Vec<String>,
    exclude_caches: bool,
    follow_symlinks: bool,
    max_depth: Option<usize>,
    hidden: bool,  // Control inclusion of hidden files
}

impl FileScanner {
    pub fn new(
        exclude_patterns: Vec<String>,
        include_patterns: Vec<String>,
        exclude_caches: bool,
    ) -> Self {
        Self {
            exclude_patterns,
            include_patterns,
            exclude_caches,
            follow_symlinks: false,
            max_depth: None,
            hidden: true,  // By default, include hidden files
        }
    }

    /// Set whether to include hidden files
    pub fn set_hidden(&mut self, hidden: bool) {
        self.hidden = hidden;
    }

    /// Scan multiple paths
    pub fn scan_paths(&self, paths: &[PathBuf]) -> Result<Vec<FileInfo>> {
        let results: Vec<Result<Vec<FileInfo>>> = paths.par_iter()
            .map(|path| self.scan_single_path(path))
            .collect();

        let mut all_files = Vec::new();
        for result in results {
            all_files.extend(result?);
        }

        Ok(all_files)
    }

    /// Scan single path
    fn scan_single_path(&self, path: &Path) -> Result<Vec<FileInfo>> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Path not found: {}", path.display()));
        }

        if path.is_file() {
            // Single file
            Ok(vec![self.scan_file(path)?])
        } else {
            // Directory tree
            self.scan_directory(path)
        }
    }

    /// Scan directory recursively
    fn scan_directory(&self, path: &Path) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();
        let mut walker = WalkBuilder::new(path);
        walker
            .follow_links(self.follow_symlinks)
            .max_depth(self.max_depth)
            .hidden(!self.hidden)  // IMPORTANT: Don't ignore hidden files if hidden = true
            .ignore(false)          // Don't use .gitignore
            .git_ignore(false)      // Don't ignore git files
            .git_global(false)      // Don't use global git rules
            .git_exclude(false)     // Don't use .git/info/exclude
            .parents(false);        // Don't look for ignore files in parents

        if self.exclude_caches {
            walker.filter_entry(|entry| {
                !entry.file_name().to_string_lossy().contains("CACHEDIR.TAG")
            });
        }

        for result in walker.build() {
            match result {
                Ok(entry) => {
                    if let Some(file_type) = entry.file_type() {
                        if file_type.is_file() {
                            if self.should_include(entry.path()) {
                                match self.scan_file(entry.path()) {
                                    Ok(file) => files.push(file),
                                    Err(e) => eprintln!("Warning: could not scan {}: {}", entry.path().display(), e),
                                }
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Warning: error walking directory: {}", e),
            }
        }

        Ok(files)
    }

    /// Scan single file
    fn scan_file(&self, path: &Path) -> Result<FileInfo> {
        let metadata = std::fs::symlink_metadata(path)?;
        
        Ok(FileInfo {
            path: path.to_path_buf(),
            size: metadata.len(),
            modified: metadata.modified()?,
            created: metadata.created().ok(),
            permissions: self.get_permissions(&metadata),
            uid: self.get_uid(&metadata),
            gid: self.get_gid(&metadata),
        })
    }

    /// Check if file should be included based on patterns
    fn should_include(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();

        // First check excludes
        for pattern in &self.exclude_patterns {
            if self.matches_pattern(&path_str, pattern) {
                return false;
            }
        }

        // If include patterns specified, at least one must match
        if !self.include_patterns.is_empty() {
            for pattern in &self.include_patterns {
                if self.matches_pattern(&path_str, pattern) {
                    return true;
                }
            }
            return false;
        }

        // No include patterns means include all
        true
    }

    /// Simple glob pattern matching
    fn matches_pattern(&self, path: &str, pattern: &str) -> bool {
        // Convert glob pattern to regex (simplified)
        let regex = pattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".");
        
        if let Ok(re) = regex::Regex::new(&format!("^{}$", regex)) {
            re.is_match(path)
        } else {
            false
        }
    }

    /// Get file permissions (cross-platform)
    fn get_permissions(&self, metadata: &std::fs::Metadata) -> Option<u32> {
        #[cfg(unix)]
        {
            Some(metadata.permissions().mode())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    /// Get file owner (Unix only)
    fn get_uid(&self, metadata: &std::fs::Metadata) -> Option<u32> {
        #[cfg(unix)]
        {
            Some(metadata.uid())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    /// Get file group (Unix only)
    fn get_gid(&self, metadata: &std::fs::Metadata) -> Option<u32> {
        #[cfg(unix)]
        {
            Some(metadata.gid())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, NamedTempFile};
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_scan_single_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let scanner = FileScanner::new(vec![], vec![], false);
        let files = scanner.scan_paths(&[temp_file.path().to_path_buf()]).unwrap();
        
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, temp_file.path());
    }

    #[test]
    fn test_scan_directory() {
        let dir = tempdir().unwrap();
        
        // Create some files
        File::create(dir.path().join("file1.txt")).unwrap();
        File::create(dir.path().join("file2.txt")).unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();
        File::create(dir.path().join("subdir/file3.txt")).unwrap();

        let scanner = FileScanner::new(vec![], vec![], false);
        let files = scanner.scan_paths(&[dir.path().to_path_buf()]).unwrap();
        
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_hidden_files() {
        let dir = tempdir().unwrap();
        
        // Create hidden file
        File::create(dir.path().join(".hidden")).unwrap();
        File::create(dir.path().join("normal.txt")).unwrap();

        let scanner = FileScanner::new(vec![], vec![], false);
        let files = scanner.scan_paths(&[dir.path().to_path_buf()]).unwrap();
        
        // Should include hidden file
        assert_eq!(files.len(), 2);
        
        let hidden_exists = files.iter().any(|f| f.path.to_string_lossy().ends_with(".hidden"));
        assert!(hidden_exists, "Hidden file should be included");
    }

    #[test]
    fn test_exclude_pattern() {
        let dir = tempdir().unwrap();
        
        File::create(dir.path().join("file.txt")).unwrap();
        File::create(dir.path().join("file.log")).unwrap();
        File::create(dir.path().join("file.tmp")).unwrap();

        let scanner = FileScanner::new(
            vec!["*.tmp".to_string(), "*.log".to_string()],
            vec![],
            false,
        );
        let files = scanner.scan_paths(&[dir.path().to_path_buf()]).unwrap();
        
        assert_eq!(files.len(), 1);
        assert!(files[0].path.to_string_lossy().ends_with("file.txt"));
    }

    #[test]
    fn test_include_pattern() {
        let dir = tempdir().unwrap();
        
        File::create(dir.path().join("file.txt")).unwrap();
        File::create(dir.path().join("data.txt")).unwrap();
        File::create(dir.path().join("file.log")).unwrap();

        let scanner = FileScanner::new(
            vec![],
            vec!["file.*".to_string()],
            false,
        );
        let files = scanner.scan_paths(&[dir.path().to_path_buf()]).unwrap();
        
        assert_eq!(files.len(), 2);
        for file in files {
            assert!(file.path.to_string_lossy().contains("file."));
        }
    }

    #[test]
    fn test_exclude_caches() {
        let dir = tempdir().unwrap();
        
        File::create(dir.path().join("file1.txt")).unwrap();
        File::create(dir.path().join("CACHEDIR.TAG")).unwrap();
        File::create(dir.path().join("file2.txt")).unwrap();

        let scanner = FileScanner::new(vec![], vec![], true);
        let files = scanner.scan_paths(&[dir.path().to_path_buf()]).unwrap();
        
        assert_eq!(files.len(), 2);
        assert!(!files.iter().any(|f| f.path.to_string_lossy().ends_with("CACHEDIR.TAG")));
    }
}

