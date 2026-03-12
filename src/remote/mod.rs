//! Remote storage support (SFTP, WebDAV)

use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::fmt;
use std::thread;
use std::time::Duration;

// Importer les modules
mod sftp;
mod webdav;

pub use sftp::SftpStorage;
pub use webdav::WebdavStorage;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RemoteProtocol {
    Sftp,
    Webdav,
    Webdavs,
}

impl fmt::Display for RemoteProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteProtocol::Sftp => write!(f, "sftp"),
            RemoteProtocol::Webdav => write!(f, "webdav"),
            RemoteProtocol::Webdavs => write!(f, "webdavs"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteLocation {
    pub protocol: RemoteProtocol,
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AuthInfo {
    pub username: String,
    pub password: Option<String>,
    pub key_file: Option<PathBuf>,
    pub passphrase: Option<String>,
}

impl RemoteLocation {
    pub fn from_url(url: &str) -> Result<Self> {
        let url = url.trim();
        
        let (protocol, rest) = if url.starts_with("sftp://") {
            (RemoteProtocol::Sftp, &url[7..])
        } else if url.starts_with("webdav://") {
            (RemoteProtocol::Webdav, &url[9..])
        } else if url.starts_with("webdavs://") {
            (RemoteProtocol::Webdavs, &url[10..])
        } else {
            return Err(anyhow!("Invalid protocol. Use sftp://, webdav://, or webdavs://"));
        };

        let (user_host, path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, ""),
        };

        let (user, host_port) = match user_host.find('@') {
            Some(idx) => (&user_host[..idx], &user_host[idx+1..]),
            None => ("", user_host),
        };

        let (host, port) = match host_port.find(':') {
            Some(idx) => {
                let host = &host_port[..idx];
                let port_str = &host_port[idx+1..];
                let port = port_str.parse::<u16>().map_err(|_| anyhow!("Invalid port"))?;
                (host, Some(port))
            }
            None => (host_port, None),
        };

        Ok(RemoteLocation {
            protocol,
            host: host.to_string(),
            port,
            user: user.to_string(),
            path: PathBuf::from(path),
        })
    }

    pub fn from_sftp_str(s: &str) -> Result<Self> {
        let url = format!("sftp://{}", s);
        Self::from_url(&url)
    }

    pub fn from_webdav_str(s: &str) -> Result<Self> {
        if s.starts_with("http://") || s.starts_with("https://") {
            Self::from_url(s)
        } else {
            let url = format!("webdav://{}", s);
            Self::from_url(&url)
        }
    }
}

impl AuthInfo {
    pub fn from_file(path: &Path, expected_user: Option<&str>) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let line = content.lines().next().ok_or_else(|| anyhow!("Empty auth file"))?;
        
        let parts: Vec<&str> = line.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid auth format. Expected username:password"));
        }
        
        let username = parts[0].to_string();
        
        if let Some(expected) = expected_user {
            if username != expected {
                return Err(anyhow!("Username mismatch: expected {}, got {}", expected, username));
            }
        }
        
        let rest = parts[1];
        
        if rest.starts_with('@') {
            let key_path = PathBuf::from(&rest[1..]);
            if !key_path.exists() {
                return Err(anyhow!("SSH key file not found: {}", key_path.display()));
            }
            Ok(AuthInfo {
                username,
                password: None,
                key_file: Some(key_path),
                passphrase: None,
            })
        } else if rest.contains('@') {
            let at_pos = rest.find('@').unwrap();
            let passphrase = rest[..at_pos].to_string();
            let key_path = PathBuf::from(&rest[at_pos+1..]);
            if !key_path.exists() {
                return Err(anyhow!("SSH key file not found: {}", key_path.display()));
            }
            Ok(AuthInfo {
                username,
                password: None,
                key_file: Some(key_path),
                passphrase: Some(passphrase),
            })
        } else {
            Ok(AuthInfo {
                username,
                password: Some(rest.to_string()),
                key_file: None,
                passphrase: None,
            })
        }
    }
}

pub trait RemoteStorage: Send + Sync {
    fn upload_file(&self, local_path: &Path, remote_path: &Path) -> Result<()>;
    fn download_file(&self, remote_path: &Path, local_path: &Path) -> Result<()>;
    fn exists(&self, remote_path: &Path) -> Result<bool>;
    fn create_dir(&self, remote_path: &Path) -> Result<()>;
    fn list_files(&self, remote_path: &Path) -> Result<Vec<String>>;
    fn delete_file(&self, remote_path: &Path) -> Result<()>;
    fn get_size(&self, remote_path: &Path) -> Result<u64>;
    fn clone_box(&self) -> Box<dyn RemoteStorage>;
}

impl Clone for Box<dyn RemoteStorage> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Upload a file with retry and verification
pub fn upload_with_retry(
    storage: &dyn RemoteStorage,
    local_path: &Path,
    remote_path: &Path,
    max_retries: usize,
    delay_seconds: u64,
) -> Result<()> {
    let local_size = std::fs::metadata(local_path)?.len();
    
    for attempt in 1..=max_retries {
        println!("📤 Upload attempt {}/{} for {}", attempt, max_retries, remote_path.display());
        
        match storage.upload_file(local_path, remote_path) {
            Ok(()) => {
                // Vérifier que le fichier existe bien sur le remote
                match storage.exists(remote_path) {
                    Ok(true) => {
                        println!("✅ Upload successful ({} bytes)", local_size);
                        return Ok(());
                    }
                    Ok(false) => {
                        eprintln!("❌ File not found on remote after upload");
                    }
                    Err(e) => {
                        eprintln!("⚠️  Could not verify upload: {}", e);
                        // On considère que l'upload a réussi quand même
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Upload attempt {} failed: {}", attempt, e);
            }
        }
        
        if attempt < max_retries {
            println!("⏳ Waiting {} seconds before retry...", delay_seconds);
            thread::sleep(Duration::from_secs(delay_seconds));
        }
    }
    
    Err(anyhow!("Failed to upload after {} attempts", max_retries))
}

pub fn create_remote_storage(
    location: RemoteLocation,
    auth: AuthInfo,
    temp_dir: PathBuf,
) -> Result<Box<dyn RemoteStorage>> {
    match location.protocol {
        RemoteProtocol::Sftp => {
            Ok(Box::new(SftpStorage::new(location, auth, temp_dir)?))
        }
        RemoteProtocol::Webdav | RemoteProtocol::Webdavs => {
            Ok(Box::new(WebdavStorage::new(location, auth)?))
        }
    }
}
