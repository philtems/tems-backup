//! Chunk handling with fixed-size blocks

use crate::error::Result;
use std::path::Path;
use std::fs::File;
use std::io::{Read, BufReader};
use crate::core::hash::{HashAlgorithm};
use crate::core::compression::{CompressionAlgorithm};
use crate::{DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE};

#[cfg(feature = "blake3")]
use blake3;

#[cfg(feature = "xxhash")]
use xxhash_rust;

#[cfg(feature = "sha256")]
use sha2;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub data: Vec<u8>,
    pub hash: String,        // ← Changé en String (hex)
    pub fast_hash: u64,
    pub size: usize,
    pub compressed_size: Option<usize>,
    pub compression: CompressionAlgorithm,
}

#[derive(Debug)]
pub struct ChunkInfo {
    pub hash: String,         // ← Changé en String (hex)
    pub fast_hash: u64,
    pub size: usize,
    pub compressed_size: usize,
    pub compression: CompressionAlgorithm,
    pub volume: u64,
    pub offset: u64,
}

pub struct Chunker {
    chunk_size: usize,
    hash_algo: HashAlgorithm,
    compression: CompressionAlgorithm,
    compression_level: i32,
}

impl Chunker {
    pub fn new(
        chunk_size: usize,
        hash_algo: HashAlgorithm,
        compression: CompressionAlgorithm,
        compression_level: i32,
    ) -> Self {
        Self {
            chunk_size: chunk_size.min(MAX_CHUNK_SIZE),
            hash_algo,
            compression,
            compression_level,
        }
    }

    /// Split file into chunks
    pub fn chunk_file<P: AsRef<Path>>(&self, path: P) -> Result<Vec<Chunk>> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        
        // Special case: empty file
        if file_size == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(file);
        let mut chunks = Vec::new();
        let mut total_read = 0;

        loop {
            let mut buffer = vec![0u8; self.chunk_size];
            
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    total_read += n;
                    buffer.truncate(n);
                    
                    let chunk = self.create_chunk(buffer)?;
                    chunks.push(chunk);
                    
                    if total_read >= file_size as usize {
                        break;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(chunks)
    }

    /// Create a chunk from data
    fn create_chunk(&self, data: Vec<u8>) -> Result<Chunk> {
        let size = data.len();
        
        // Calculate fast hash for deduplication
        let fast_hash = self.calculate_fast_hash(&data);
        
        // Calculate cryptographic hash (returns hex string)
        let hash = self.calculate_hash(&data);

        Ok(Chunk {
            data,
            hash,
            fast_hash,
            size,
            compressed_size: None,
            compression: self.compression,
        })
    }

    /// Calculate fast hash (64-bit)
    fn calculate_fast_hash(&self, data: &[u8]) -> u64 {
        match self.hash_algo {
            HashAlgorithm::XxHash3 => {
                #[cfg(feature = "xxhash")]
                {
                    xxhash_rust::xxh3::xxh3_64(data)
                }
                #[cfg(not(feature = "xxhash"))]
                {
                    panic!("xxhash3 support not enabled");
                }
            }
            HashAlgorithm::Blake3 => {
                #[cfg(feature = "blake3")]
                {
                    let hash = blake3::hash(data);
                    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap_or([0; 8]))
                }
                #[cfg(not(feature = "blake3"))]
                {
                    panic!("blake3 support not enabled");
                }
            }
            HashAlgorithm::Sha256 => {
                #[cfg(feature = "sha256")]
                {
                    use sha2::Digest;
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(data);
                    let hash = hasher.finalize();
                    u64::from_le_bytes(hash[..8].try_into().unwrap_or([0; 8]))
                }
                #[cfg(not(feature = "sha256"))]
                {
                    panic!("sha256 support not enabled");
                }
            }
        }
    }

    /// Calculate cryptographic hash (returns hex string)
    fn calculate_hash(&self, data: &[u8]) -> String {
        match self.hash_algo {
            HashAlgorithm::XxHash3 => {
                #[cfg(feature = "xxhash")]
                {
                    let hash = self.calculate_fast_hash(data).to_le_bytes();
                    hex::encode(hash)
                }
                #[cfg(not(feature = "xxhash"))]
                {
                    panic!("xxhash3 support not enabled");
                }
            }
            HashAlgorithm::Blake3 => {
                #[cfg(feature = "blake3")]
                {
                    let hash = blake3::hash(data);
                    hex::encode(hash.as_bytes())
                }
                #[cfg(not(feature = "blake3"))]
                {
                    panic!("blake3 support not enabled");
                }
            }
            HashAlgorithm::Sha256 => {
                #[cfg(feature = "sha256")]
                {
                    use sha2::Digest;
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(data);
                    let hash = hasher.finalize();
                    hex::encode(hash)
                }
                #[cfg(not(feature = "sha256"))]
                {
                    panic!("sha256 support not enabled");
                }
            }
        }
    }

    /// Get chunk size
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl Default for Chunker {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            hash_algo: HashAlgorithm::Blake3,
            compression: CompressionAlgorithm::Zstd,
            compression_level: 3,
        }
    }
}

