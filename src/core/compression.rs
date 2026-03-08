//! Compression algorithms support

use std::fmt;
use std::str::FromStr;
use anyhow::{Result, anyhow};

#[cfg(feature = "xz")]
use xz2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionAlgorithm {
    Zstd,
    Xz,
    None,
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
            CompressionAlgorithm::Xz => write!(f, "xz"),
            CompressionAlgorithm::None => write!(f, "none"),
        }
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "zstd" => Ok(CompressionAlgorithm::Zstd),
            "xz" => Ok(CompressionAlgorithm::Xz),
            "none" => Ok(CompressionAlgorithm::None),
            _ => Err(anyhow!("Unknown compression: {}", s)),
        }
    }
}

pub trait Compressor: Send + Sync {
    fn compress(&mut self, data: &[u8]) -> Result<Vec<u8>>;
    fn decompress(&mut self, data: &[u8]) -> Result<Vec<u8>>;
    fn algorithm(&self) -> CompressionAlgorithm;
}

pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "zstd")]
        {
            zstd::bulk::compress(data, self.level)
                .map_err(|e| anyhow!("Zstd compression error: {}", e))
        }
        #[cfg(not(feature = "zstd"))]
        {
            Err(anyhow!("zstd support not enabled"))
        }
    }

    fn decompress(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "zstd")]
        {
            zstd::bulk::decompress(data, 1024 * 1024)
                .map_err(|e| anyhow!("Zstd decompression error: {}", e))
        }
        #[cfg(not(feature = "zstd"))]
        {
            Err(anyhow!("zstd support not enabled"))
        }
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }
}

pub struct XzCompressor {
    level: u32,
}

impl XzCompressor {
    pub fn new(level: u32) -> Self {
        Self { level }
    }
}

impl Compressor for XzCompressor {
    fn compress(&mut self, _data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "xz")]
        {
            use std::io::Write;
            let mut encoder = xz2::write::XzEncoder::new(Vec::new(), self.level);
            encoder.write_all(_data)?;
            encoder.finish()
                .map_err(|e| anyhow!("XZ compression error: {}", e))
        }
        #[cfg(not(feature = "xz"))]
        {
            Err(anyhow!("xz support not enabled"))
        }
    }

    fn decompress(&mut self, _data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "xz")]
        {
            use std::io::Read;
            let mut decoder = xz2::read::XzDecoder::new(_data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        }
        #[cfg(not(feature = "xz"))]
        {
            Err(anyhow!("xz support not enabled"))
        }
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Xz
    }
}

pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }
}

pub fn get_compressor(algorithm: CompressionAlgorithm, level: i32) -> Box<dyn Compressor> {
    match algorithm {
        CompressionAlgorithm::Zstd => Box::new(ZstdCompressor::new(level)),
        CompressionAlgorithm::Xz => Box::new(XzCompressor::new(level as u32)),
        CompressionAlgorithm::None => Box::new(NoCompressor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_compress_decompress() {
        #[cfg(feature = "zstd")]
        {
            let mut compressor = ZstdCompressor::new(3);
            let data = b"test data to compress";
            
            let compressed = compressor.compress(data).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            
            assert_eq!(data, decompressed.as_slice());
        }
    }

    #[test]
    fn test_xz_compress_decompress() {
        #[cfg(feature = "xz")]
        {
            let mut compressor = XzCompressor::new(3);
            let data = b"test data to compress";
            
            let compressed = compressor.compress(data).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();
            
            assert_eq!(data, decompressed.as_slice());
        }
    }
}

