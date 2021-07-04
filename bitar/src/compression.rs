use bytes::Bytes;
use std::fmt;

use crate::chunk_dictionary as dict;

#[derive(Debug)]
pub enum CompressionError {
    Io(std::io::Error),
    #[cfg(feature = "lzma-compression")]
    LZMA(lzma::LzmaError),
}
impl std::error::Error for CompressionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CompressionError::Io(err) => Some(err),
            #[cfg(feature = "lzma-compression")]
            CompressionError::LZMA(err) => Some(err),
        }
    }
}
impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(_) => write!(f, "i/o error"),
            #[cfg(feature = "lzma-compression")]
            Self::LZMA(_) => write!(f, "LZMA error"),
        }
    }
}
impl From<std::io::Error> for CompressionError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
#[cfg(feature = "lzma-compression")]
impl From<lzma::LzmaError> for CompressionError {
    fn from(e: lzma::LzmaError) -> Self {
        Self::LZMA(e)
    }
}

#[derive(Debug)]
pub struct CompressionLevelOutOfRangeError(CompressionAlgorithm);
impl std::error::Error for CompressionLevelOutOfRangeError {}
impl fmt::Display for CompressionLevelOutOfRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} compression level out of range (valid range is 1-{})",
            self.0,
            self.0.max_level()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionAlgorithm {
    #[cfg(feature = "lzma-compression")]
    Lzma,
    #[cfg(feature = "zstd-compression")]
    Zstd,
    Brotli,
}

impl CompressionAlgorithm {
    /// Get the compression algorithm's max level.
    pub fn max_level(self) -> u32 {
        match self {
            #[cfg(feature = "lzma-compression")]
            CompressionAlgorithm::Lzma => 9,
            #[cfg(feature = "zstd-compression")]
            CompressionAlgorithm::Zstd => 22,
            CompressionAlgorithm::Brotli => 11,
        }
    }
    /// Decompress a block of data using the set compression.
    pub(crate) fn decompress(
        self,
        compressed: Bytes,
        size_hint: usize,
    ) -> Result<Bytes, CompressionError> {
        let mut output = Vec::with_capacity(size_hint);
        match self {
            #[cfg(feature = "lzma-compression")]
            CompressionAlgorithm::Lzma => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut f = LzmaWriter::new_decompressor(&mut output)?;
                f.write_all(&compressed)?;
                f.finish()?;
            }
            #[cfg(feature = "zstd-compression")]
            CompressionAlgorithm::Zstd => {
                zstd::stream::copy_decode(&compressed[..], &mut output)?;
            }
            CompressionAlgorithm::Brotli => {
                let mut input_slice = &compressed[..];
                brotli_decompressor::BrotliDecompress(&mut input_slice, &mut output)?;
            }
        }
        Ok(Bytes::from(output))
    }
}

impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let algorithm_name = match self {
            #[cfg(feature = "lzma-compression")]
            CompressionAlgorithm::Lzma => "LZMA",
            #[cfg(feature = "zstd-compression")]
            CompressionAlgorithm::Zstd => "zstd",
            CompressionAlgorithm::Brotli => "Brotli",
        };
        write!(f, "{}", algorithm_name)
    }
}

/// Compression.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Compression {
    pub(crate) algorithm: CompressionAlgorithm,
    pub(crate) level: u32,
}

impl Compression {
    /// Create a new compression of given algorithm and level.
    pub fn try_new(
        algorithm: CompressionAlgorithm,
        level: u32,
    ) -> Result<Compression, CompressionLevelOutOfRangeError> {
        if level < 1 || level > algorithm.max_level() {
            return Err(CompressionLevelOutOfRangeError(algorithm));
        }
        Ok(Compression { algorithm, level })
    }
    /// Create a new brotli compression of given level.
    pub fn brotli(level: u32) -> Result<Compression, CompressionLevelOutOfRangeError> {
        Self::try_new(CompressionAlgorithm::Brotli, level)
    }
    #[cfg(feature = "lzma-compression")]
    /// Create a new lzma compression of given level.
    pub fn lzma(level: u32) -> Result<Compression, CompressionLevelOutOfRangeError> {
        Self::try_new(CompressionAlgorithm::Lzma, level)
    }
    #[cfg(feature = "zstd-compression")]
    /// Create a new zstd compression of given level.
    pub fn zstd(level: u32) -> Result<Compression, CompressionLevelOutOfRangeError> {
        Self::try_new(CompressionAlgorithm::Zstd, level)
    }
    /// Compress a block of data with set compression.
    #[cfg(feature = "compress")]
    pub(crate) fn compress(self, chunk: Bytes) -> Result<Bytes, CompressionError> {
        use brotli::enc::backward_references::BrotliEncoderParams;
        use std::io::Write;
        let mut output = Vec::with_capacity(chunk.len());
        match self.algorithm {
            #[cfg(feature = "lzma-compression")]
            CompressionAlgorithm::Lzma => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut f = LzmaWriter::new_compressor(&mut output, self.level)?;
                f.write_all(&chunk)?;
                f.finish()?;
            }
            #[cfg(feature = "zstd-compression")]
            CompressionAlgorithm::Zstd => {
                zstd::stream::copy_encode(&chunk[..], &mut output, self.level as i32)?;
            }
            CompressionAlgorithm::Brotli => {
                let params = BrotliEncoderParams {
                    quality: self.level as i32,
                    magic_number: false,
                    ..Default::default()
                };
                let mut writer =
                    brotli::CompressorWriter::with_params(&mut output, 1024 * 1024, &params);
                writer.write_all(&chunk)?;
            }
        }
        Ok(Bytes::from(output))
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (level {})", self.algorithm, self.level)
    }
}

impl From<Option<Compression>> for dict::ChunkCompression {
    fn from(c: Option<Compression>) -> Self {
        let (compression, compression_level) = match c {
            #[cfg(feature = "lzma-compression")]
            Some(Compression {
                algorithm: CompressionAlgorithm::Lzma,
                level,
            }) => (dict::chunk_compression::CompressionType::Lzma, level),
            #[cfg(feature = "zstd-compression")]
            Some(Compression {
                algorithm: CompressionAlgorithm::Zstd,
                level,
            }) => (dict::chunk_compression::CompressionType::Zstd, level),
            Some(Compression {
                algorithm: CompressionAlgorithm::Brotli,
                level,
            }) => (dict::chunk_compression::CompressionType::Brotli, level),
            None => (dict::chunk_compression::CompressionType::None, 0),
        };
        Self {
            compression: compression as i32,
            compression_level,
        }
    }
}
