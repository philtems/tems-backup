//! SFTP remote storage implementation using ssh2

use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use ssh2::Session;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::remote::{RemoteStorage, RemoteLocation, AuthInfo};

pub struct SftpStorage {
    session: Arc<Mutex<Session>>,
    base_path: PathBuf,
    location: RemoteLocation,
    auth: AuthInfo,
}

impl SftpStorage {
    pub fn new(location: RemoteLocation, auth: AuthInfo, _temp_dir: PathBuf) -> Result<Self> {
        let port = location.port.unwrap_or(22);
        let addr = format!("{}:{}", location.host, port);
        
        let tcp = TcpStream::connect(addr)?;
        let mut session = Session::new()?;
        session.set_tcp_stream(tcp);
        session.handshake()?;
        
        if let Some(key_file) = &auth.key_file {
            session.userauth_pubkey_file(
                &auth.username,
                None,
                key_file,
                auth.passphrase.as_deref(),
            )?;
        } else if let Some(password) = &auth.password {
            session.userauth_password(&auth.username, password)?;
        } else {
            return Err(anyhow!("No authentication method provided"));
        }
        
        if !session.authenticated() {
            return Err(anyhow!("Authentication failed"));
        }
        
        Ok(Self {
            session: Arc::new(Mutex::new(session)),
            base_path: location.path.clone(),
            location,
            auth,
        })
    }
    
    fn sftp(&self) -> Result<ssh2::Sftp> {
        let session = self.session.lock().unwrap();
        Ok(session.sftp()?)
    }
    
    fn ensure_parent_dir(&self, sftp: &ssh2::Sftp, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = sftp.mkdir(parent, 0o755);
            }
        }
        Ok(())
    }
}

impl RemoteStorage for SftpStorage {
    fn upload_file(&self, local_path: &Path, remote_path: &Path) -> Result<()> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        
        self.ensure_parent_dir(&sftp, &full_path)?;
        
        let mut remote_file = sftp.create(&full_path)?;
        let mut local_file = std::fs::File::open(local_path)?;
        std::io::copy(&mut local_file, &mut remote_file)?;
        
        Ok(())
    }
    
    fn download_file(&self, remote_path: &Path, local_path: &Path) -> Result<()> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        let mut remote_file = sftp.open(&full_path)?;
        let mut local_file = std::fs::File::create(local_path)?;
        std::io::copy(&mut remote_file, &mut local_file)?;
        
        Ok(())
    }
    
    fn exists(&self, remote_path: &Path) -> Result<bool> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        Ok(sftp.stat(&full_path).is_ok())
    }
    
    fn get_size(&self, remote_path: &Path) -> Result<u64> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        
        match sftp.stat(&full_path) {
            Ok(stat) => Ok(stat.size.unwrap_or(0)),
            Err(e) => Err(anyhow!("Failed to get file size: {}", e)),
        }
    }
    
    fn create_dir(&self, remote_path: &Path) -> Result<()> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        
        let mut current = PathBuf::new();
        for component in full_path.components() {
            current.push(component);
            let _ = sftp.mkdir(&current, 0o755);
        }
        
        Ok(())
    }
    
    fn list_files(&self, remote_path: &Path) -> Result<Vec<String>> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        
        let entries = sftp.readdir(&full_path)?;
        let files = entries
            .into_iter()
            .filter_map(|(path, _)| {
                path.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.to_string())
            })
            .collect();
        
        Ok(files)
    }
    
    fn delete_file(&self, remote_path: &Path) -> Result<()> {
        let sftp = self.sftp()?;
        let full_path = self.base_path.join(remote_path);
        sftp.unlink(&full_path)?;
        Ok(())
    }
    
    fn clone_box(&self) -> Box<dyn RemoteStorage> {
        match SftpStorage::new(
            self.location.clone(),
            self.auth.clone(),
            std::env::temp_dir(),
        ) {
            Ok(storage) => Box::new(storage),
            Err(e) => {
                panic!("Failed to clone SftpStorage: {}", e);
            }
        }
    }
}

