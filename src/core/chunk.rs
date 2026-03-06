//! Chunk handling with fixed-size blocks

use crate::error::Result;
use std::path::Path;
use std::fs::File;
use std::io::{Read, BufReader};
use rayon::prelude::*;
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
    pub hash: Vec<u8>,
    pub fast_hash: u64,
    pub size: usize,
    pub compressed_size: Option<usize>,
    pub compression: CompressionAlgorithm,
}

#[derive(Debug)]
pub struct ChunkInfo {
    pub hash: Vec<u8>,
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

    /// Split file into chunks (handles empty files correctly)
    pub fn chunk_file<P: AsRef<Path>>(&self, path: P) -> Result<Vec<Chunk>> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        
        // Special case: empty file
        if metadata.len() == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(file);
        let mut chunks = Vec::new();
        let mut buffer = vec![0u8; self.chunk_size];

        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let chunk_data = buffer[..n].to_vec();
                    let chunk = self.create_chunk(chunk_data)?;
                    chunks.push(chunk);
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
        
        // Calculate cryptographic hash
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
                    0
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
                    0
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
                    0
                }
            }
        }
    }

    /// Calculate cryptographic hash
    fn calculate_hash(&self, data: &[u8]) -> Vec<u8> {
        match self.hash_algo {
            HashAlgorithm::XxHash3 => {
                #[cfg(feature = "xxhash")]
                {
                    self.calculate_fast_hash(data).to_le_bytes().to_vec()
                }
                #[cfg(not(feature = "xxhash"))]
                {
                    Vec::new()
                }
            }
            HashAlgorithm::Blake3 => {
                #[cfg(feature = "blake3")]
                {
                    blake3::hash(data).as_bytes().to_vec()
                }
                #[cfg(not(feature = "blake3"))]
                {
                    Vec::new()
                }
            }
            HashAlgorithm::Sha256 => {
                #[cfg(feature = "sha256")]
                {
                    use sha2::Digest;
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(data);
                    hasher.finalize().to_vec()
                }
                #[cfg(not(feature = "sha256"))]
                {
                    Vec::new()
                }
            }
        }
    }

    /// Process multiple files in parallel
    pub fn chunk_files_parallel<P: AsRef<Path> + Send + Sync>(
        &self,
        paths: Vec<P>,
    ) -> Result<Vec<(P, Vec<Chunk>)>> {
        paths
            .into_par_iter()
            .map(|path| {
                let chunks = self.chunk_file(&path)?;
                Ok((path, chunks))
            })
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_chunk_creation() {
        let chunker = Chunker::new(
            1024,
            HashAlgorithm::Blake3,
            CompressionAlgorithm::Zstd,
            3,
        );
        
        let data = vec![0u8; 500];
        let chunk = chunker.create_chunk(data).unwrap();
        
        assert_eq!(chunk.size, 500);
    }

    #[test]
    fn test_chunk_file() {
        let chunker = Chunker::new(
            1024,
            HashAlgorithm::Blake3,
            CompressionAlgorithm::Zstd,
            3,
        );
        
        let mut file = NamedTempFile::new().unwrap();
        let data = vec![42u8; 2500]; // 2.5 chunks
        file.write_all(&data).unwrap();
        file.flush().unwrap();
        
        let chunks = chunker.chunk_file(file.path()).unwrap();
        assert_eq!(chunks.len(), 3); // 1024 + 1024 + 452
        assert_eq!(chunks[0].size, 1024);
        assert_eq!(chunks[1].size, 1024);
        assert_eq!(chunks[2].size, 452);
    }

    #[test]
    fn test_empty_file() {
        let chunker = Chunker::new(
            1024,
            HashAlgorithm::Blake3,
            CompressionAlgorithm::Zstd,
            3,
        );
        
        let file = NamedTempFile::new().unwrap();
        // File is empty
        
        let chunks = chunker.chunk_file(file.path()).unwrap();
        assert_eq!(chunks.len(), 0); // Empty file should return empty chunk list
    }
}

