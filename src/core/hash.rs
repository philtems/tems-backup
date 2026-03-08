//! Hash algorithms for deduplication and integrity

use std::fmt;
use std::str::FromStr;
use anyhow::{Result, anyhow};

#[cfg(feature = "blake3")]
use blake3;

#[cfg(feature = "xxhash")]
use xxhash_rust;

#[cfg(feature = "sha256")]
use sha2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HashAlgorithm {
    XxHash3,
    Blake3,
    Sha256,
}

impl fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashAlgorithm::XxHash3 => write!(f, "xxhash3"),
            HashAlgorithm::Blake3 => write!(f, "blake3"),
            HashAlgorithm::Sha256 => write!(f, "sha256"),
        }
    }
}

impl FromStr for HashAlgorithm {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "xxhash3" | "xxh3" => Ok(HashAlgorithm::XxHash3),
            "blake3" => Ok(HashAlgorithm::Blake3),
            "sha256" => Ok(HashAlgorithm::Sha256),
            _ => Err(anyhow!("Unknown hash algorithm: {}", s)),
        }
    }
}

pub trait Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8>;
    fn hash_fast(&self, data: &[u8]) -> u64;
    fn name(&self) -> &'static str;
}

pub struct XxHash3Hasher;

impl Hasher for XxHash3Hasher {
    fn hash(&self, _data: &[u8]) -> Vec<u8> {
        #[cfg(feature = "xxhash")]
        {
            xxhash_rust::xxh3::xxh3_64(_data).to_le_bytes().to_vec()
        }
        #[cfg(not(feature = "xxhash"))]
        {
            Vec::new()
        }
    }

    fn hash_fast(&self, _data: &[u8]) -> u64 {
        #[cfg(feature = "xxhash")]
        {
            xxhash_rust::xxh3::xxh3_64(_data)
        }
        #[cfg(not(feature = "xxhash"))]
        {
            0
        }
    }

    fn name(&self) -> &'static str {
        "xxhash3"
    }
}

pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        #[cfg(feature = "blake3")]
        {
            blake3::hash(data).as_bytes().to_vec()
        }
        #[cfg(not(feature = "blake3"))]
        {
            Vec::new()
        }
    }

    fn hash_fast(&self, data: &[u8]) -> u64 {
        #[cfg(feature = "blake3")]
        {
            let hash = blake3::hash(data);
            u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
        }
        #[cfg(not(feature = "blake3"))]
        {
            0
        }
    }

    fn name(&self) -> &'static str {
        "blake3"
    }
}

pub struct Sha256Hasher;

impl Hasher for Sha256Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
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

    fn hash_fast(&self, data: &[u8]) -> u64 {
        #[cfg(feature = "sha256")]
        {
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            hasher.update(data);
            let hash = hasher.finalize();
            u64::from_le_bytes(hash[..8].try_into().unwrap())
        }
        #[cfg(not(feature = "sha256"))]
        {
            0
        }
    }

    fn name(&self) -> &'static str {
        "sha256"
    }
}

pub fn get_hasher(algorithm: HashAlgorithm) -> Box<dyn Hasher> {
    match algorithm {
        HashAlgorithm::XxHash3 => Box::new(XxHash3Hasher),
        HashAlgorithm::Blake3 => Box::new(Blake3Hasher),
        HashAlgorithm::Sha256 => Box::new(Sha256Hasher),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(HashAlgorithm::from_str("xxhash3").unwrap(), HashAlgorithm::XxHash3);
        assert_eq!(HashAlgorithm::from_str("xxh3").unwrap(), HashAlgorithm::XxHash3);
        assert_eq!(HashAlgorithm::from_str("blake3").unwrap(), HashAlgorithm::Blake3);
        assert_eq!(HashAlgorithm::from_str("sha256").unwrap(), HashAlgorithm::Sha256);
        assert!(HashAlgorithm::from_str("unknown").is_err());
    }

    #[test]
    fn test_xxhash3() {
        #[cfg(feature = "xxhash")]
        {
            let hasher = XxHash3Hasher;
            let data = b"test data";
            let hash = hasher.hash(data);
            assert_eq!(hash.len(), 8);
        }
    }

    #[test]
    fn test_blake3() {
        #[cfg(feature = "blake3")]
        {
            let hasher = Blake3Hasher;
            let data = b"test data";
            let hash = hasher.hash(data);
            assert_eq!(hash.len(), 32);
        }
    }

    #[test]
    fn test_sha256() {
        #[cfg(feature = "sha256")]
        {
            let hasher = Sha256Hasher;
            let data = b"test data";
            let hash = hasher.hash(data);
            assert_eq!(hash.len(), 32);
        }
    }
}

