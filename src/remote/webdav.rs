//! WebDAV remote storage implementation

use anyhow::{Result, anyhow};
use std::path::{Path};
use std::fs::File;
use std::io::Read;
use base64::prelude::*;

use crate::remote::{RemoteStorage, RemoteLocation, AuthInfo};

pub struct WebdavStorage {
    location: RemoteLocation,
    auth: AuthInfo,
    base_url: String,
}

impl WebdavStorage {
    pub fn new(location: RemoteLocation, auth: AuthInfo) -> Result<Self> {
        let protocol = match location.protocol {
            crate::remote::RemoteProtocol::Webdav => "http",
            crate::remote::RemoteProtocol::Webdavs => "https",
            _ => return Err(anyhow!("Invalid protocol for WebDAV")),
        };
        
        let port = location.port.map_or(String::new(), |p| format!(":{}", p));
        let base_url = format!("{}://{}{}{}", 
            protocol, 
            location.host, 
            port,
            location.path.display()
        );
        
        Ok(Self {
            location,
            auth,
            base_url,
        })
    }
    
    fn build_url(&self, path: &Path) -> String {
        let path_str = path.display().to_string();
        if self.base_url.ends_with('/') {
            format!("{}{}", self.base_url, path_str.trim_start_matches('/'))
        } else {
            format!("{}/{}", self.base_url, path_str.trim_start_matches('/'))
        }
    }
    
    fn add_auth_header(&self, request: ureq::Request) -> ureq::Request {
        if let Some(password) = &self.auth.password {
            let auth = BASE64_STANDARD.encode(format!("{}:{}", self.auth.username, password));
            request.set("Authorization", &format!("Basic {}", auth))
        } else {
            request
        }
    }
}

impl RemoteStorage for WebdavStorage {
    fn upload_file(&self, local_path: &Path, remote_path: &Path) -> Result<()> {
        let url = self.build_url(remote_path);
        
        let mut file = File::open(local_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        
        let agent = ureq::agent();
        let request = agent.put(&url);
        let request = self.add_auth_header(request);
        
        match request.send_bytes(&data) {
            Ok(response) => {
                if response.status() == 201 || response.status() == 204 {
                    Ok(())
                } else {
                    Err(anyhow!("Upload failed with status {}", response.status()))
                }
            }
            Err(ureq::Error::Status(code, _)) => {
                Err(anyhow!("Upload failed with HTTP status {}", code))
            }
            Err(e) => Err(anyhow!("Upload failed: {}", e)),
        }
    }

    fn download_file(&self, remote_path: &Path, local_path: &Path) -> Result<()> {
        let url = self.build_url(remote_path);
        
        let agent = ureq::agent();
        let request = agent.get(&url);
        let request = self.add_auth_header(request);
        
        let response = request.call()?;
        
        let mut file = File::create(local_path)?;
        let mut reader = response.into_reader();
        std::io::copy(&mut reader, &mut file)?;
        
        Ok(())
    }

    fn exists(&self, remote_path: &Path) -> Result<bool> {
        let url = self.build_url(remote_path);
        
        let agent = ureq::agent();
        let request = agent.head(&url);
        let request = self.add_auth_header(request);
        
        match request.call() {
            Ok(response) => Ok(response.status() == 200),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(anyhow!("Failed to check existence: {}", e)),
        }
    }

    fn create_dir(&self, remote_path: &Path) -> Result<()> {
        let url = self.build_url(remote_path);
        
        let agent = ureq::agent();
        let request = agent.request("MKCOL", &url);
        let request = self.add_auth_header(request);
        
        match request.call() {
            Ok(response) => {
                if response.status() == 201 || response.status() == 405 {
                    Ok(())
                } else {
                    Err(anyhow!("Failed to create directory: status {}", response.status()))
                }
            }
            Err(ureq::Error::Status(code, _)) => {
                if code == 405 || code == 409 {
                    Ok(())
                } else {
                    Err(anyhow!("Failed to create directory: HTTP {}", code))
                }
            }
            Err(e) => Err(anyhow!("Failed to create directory: {}", e)),
        }
    }

    fn list_files(&self, remote_path: &Path) -> Result<Vec<String>> {
        let url = self.build_url(remote_path);
        
        let agent = ureq::agent();
        let request = agent.request("PROPFIND", &url);
        let request = self.add_auth_header(request);
        
        let response = request.call()?;
        
        let body = response.into_string()?;
        let mut files = Vec::new();
        
        for line in body.lines() {
            if line.contains("<d:href>") || line.contains("<href>") {
                if let Some(start) = line.find('>') {
                    if let Some(end) = line.find('<') {
                        if end > start + 1 {
                            let href = &line[start+1..end];
                            if let Some(name) = href.rsplit('/').next() {
                                if !name.is_empty() && name != "." && name != ".." {
                                    files.push(name.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(files)
    }

    fn delete_file(&self, remote_path: &Path) -> Result<()> {
        let url = self.build_url(remote_path);
        
        let agent = ureq::agent();
        let request = agent.delete(&url);
        let request = self.add_auth_header(request);
        
        match request.call() {
            Ok(response) => {
                if response.status() == 204 {
                    Ok(())
                } else {
                    Err(anyhow!("Delete failed with status {}", response.status()))
                }
            }
            Err(ureq::Error::Status(code, _)) => {
                if code == 404 {
                    Ok(())
                } else {
                    Err(anyhow!("Delete failed with HTTP status {}", code))
                }
            }
            Err(e) => Err(anyhow!("Delete failed: {}", e)),
        }
    }

    fn clone_box(&self) -> Box<dyn RemoteStorage> {
        Box::new(Self {
            location: self.location.clone(),
            auth: self.auth.clone(),
            base_url: self.base_url.clone(),
        })
    }
}

