use blake2::{Blake2b, Digest};
use futures_util::{stream::Stream, StreamExt};
use std::{convert::TryInto, fmt};

use crate::{
    chunk_dictionary as dict, chunker, compression::CompressionAlgorithm, header, reader::ReadAt,
    ChunkIndex, CompressedArchiveChunk, CompressedChunk, Compression, HashSum, Reader,
};

#[derive(Debug)]
pub enum ArchiveError<R> {
    InvalidArchive(Box<dyn std::error::Error + Send + Sync>),
    ReaderError(R),
}
impl<R> ArchiveError<R> {
    fn invalid_archive<T: Into<Box<dyn std::error::Error + Send + Sync>>>(err: T) -> Self {
        Self::InvalidArchive(err.into())
    }
}
impl<R> std::error::Error for ArchiveError<R>
where
    R: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ArchiveError::InvalidArchive(err) => Some(err.as_ref()),
            ArchiveError::ReaderError(err) => Some(err),
        }
    }
}
impl<R> fmt::Display for ArchiveError<R>
where
    R: std::error::Error,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArchive(_) => write!(f, "invalid archive"),
            Self::ReaderError(_) => write!(f, "reader error"),
        }
    }
}
impl<R> From<prost::DecodeError> for ArchiveError<R> {
    fn from(err: prost::DecodeError) -> Self {
        ArchiveError::InvalidArchive(Box::new(err))
    }
}

/// Description of a chunk within an archive.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkDescriptor {
    /// Chunk checksum.
    pub checksum: HashSum,
    /// Actual size of chunk data in the archive (may be compressed).
    pub archive_size: usize,
    /// Absolute byte offset of chunk in the archive.
    pub archive_offset: u64,
    /// Size of the chunk data in source (uncompressed).
    pub source_size: u32,
}

impl ChunkDescriptor {
    pub fn archive_end_offset(&self) -> u64 {
        self.archive_offset + self.archive_size as u64
    }
}

/// A readable archive.
pub struct Archive<R> {
    reader: R,
    // Array with descriptor of all chunks in archive.
    archive_chunks: Vec<ChunkDescriptor>,
    // Array of indexes pointing into the archive_chunks.
    // Represents the order of chunks in source.
    source_order: Vec<usize>,
    total_chunks: usize,
    header_size: usize,
    header_checksum: HashSum,
    chunk_compression: Option<Compression>,
    created_by_app_version: String,
    chunk_data_offset: u64,
    source_total_size: u64,
    source_checksum: HashSum,
    chunker_config: chunker::Config,
    chunk_hash_length: usize,
}

impl<R> Archive<R> {
    fn verify_pre_header<E>(pre_header: &[u8]) -> Result<(), ArchiveError<E>> {
        if pre_header.len() < header::ARCHIVE_MAGIC.len() {
            return Err(ArchiveError::invalid_archive("not an archive"));
        }
        // Allow both legacy type file magic (prefixed with \0 but no null
        // termination) and 'BITA\0'.
        if &pre_header[0..header::ARCHIVE_MAGIC.len()] != header::ARCHIVE_MAGIC
            && &pre_header[0..header::ARCHIVE_MAGIC.len()] != b"\0BITA1"
        {
            return Err(ArchiveError::invalid_archive("not an archive"));
        }
        Ok(())
    }
    /// Try to initialize an archive from a reader.
    pub async fn try_init(mut reader: R) -> Result<Self, ArchiveError<R::Error>>
    where
        R: Reader,
    {
        // Read the pre-header (file magic and size)
        let mut header: Vec<u8> = reader
            .read_at(0, header::PRE_HEADER_SIZE)
            .await
            .map_err(ArchiveError::ReaderError)?
            .to_vec();
        Self::verify_pre_header(&header)?;

        let dictionary_size = u64::from_le_bytes(
            header[header::ARCHIVE_MAGIC.len()..header::PRE_HEADER_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        // Read the dictionary, chunk data offset and header hash
        header.extend_from_slice(
            &reader
                .read_at(header::PRE_HEADER_SIZE as u64, dictionary_size + 8 + 64)
                .await
                .map_err(ArchiveError::ReaderError)?,
        );

        // Verify the header against the header checksum
        let header_checksum = {
            let mut hasher = Blake2b::new();
            let offs = header::PRE_HEADER_SIZE + dictionary_size + 8;
            hasher.update(&header[..offs]);
            let header_checksum = HashSum::from(&header[offs..(offs + 64)]);
            if header_checksum != &hasher.finalize()[..] {
                return Err(ArchiveError::invalid_archive("invalid header checksum"));
            }
            header_checksum
        };

        // Deserialize the chunk dictionary
        let dictionary: dict::ChunkDictionary = {
            let offs = header::PRE_HEADER_SIZE;
            prost::Message::decode(&header[offs..(offs + dictionary_size)])?
        };

        // Get chunk data offset
        let chunk_data_offset = {
            let offs = header::PRE_HEADER_SIZE + dictionary_size;
            u64::from_le_bytes(header[offs..(offs + 8)].try_into().unwrap())
        };
        let archive_chunks = dictionary
            .chunk_descriptors
            .into_iter()
            .map(|dict| ChunkDescriptor {
                checksum: dict.checksum.into(),
                archive_size: dict.archive_size as usize,
                archive_offset: chunk_data_offset + dict.archive_offset,
                source_size: dict.source_size,
            })
            .collect();
        let chunker_params = dictionary
            .chunker_params
            .ok_or_else(|| ArchiveError::invalid_archive("invalid chunker parameters"))?;
        let chunk_hash_length = chunker_params.chunk_hash_length as usize;
        let source_order: Vec<usize> = dictionary
            .rebuild_order
            .into_iter()
            .map(|v| v as usize)
            .collect();
        Ok(Self {
            reader,
            archive_chunks,
            header_checksum,
            header_size: header.len(),
            source_total_size: dictionary.source_total_size,
            source_checksum: dictionary.source_checksum.into(),
            created_by_app_version: dictionary.application_version.clone(),
            chunk_compression: compression_from_dictionary(
                dictionary
                    .chunk_compression
                    .ok_or_else(|| ArchiveError::invalid_archive("invalid compression"))?,
            )?,
            total_chunks: source_order.len(),
            source_order,
            chunk_data_offset,
            chunk_hash_length,
            chunker_config: chunker_config_from_params(chunker_params)?,
        })
    }
    /// Total number of chunks in archive (including duplicates).
    pub fn total_chunks(&self) -> usize {
        self.total_chunks
    }
    /// Total number of unique chunks in archive (no duplicates).
    pub fn unique_chunks(&self) -> usize {
        self.archive_chunks.len()
    }
    /// Total size of chunks in archive when compressed.
    pub fn compressed_size(&self) -> u64 {
        self.archive_chunks
            .iter()
            .map(|c| c.archive_size as u64)
            .sum()
    }
    /// On which offset in the archive the chunk data starts at.
    pub fn chunk_data_offset(&self) -> u64 {
        self.chunk_data_offset
    }
    /// Get archive chunk descriptors.
    pub fn chunk_descriptors(&self) -> &[ChunkDescriptor] {
        &self.archive_chunks
    }
    /// Total size of the original source file.
    pub fn total_source_size(&self) -> u64 {
        self.source_total_size
    }
    /// Checksum of the original source file (Blake2).
    pub fn source_checksum(&self) -> &HashSum {
        &self.source_checksum
    }
    /// Get the chunker configuration used when building the archive.
    pub fn chunker_config(&self) -> &chunker::Config {
        &self.chunker_config
    }
    /// Get the checksum of the archive header.
    pub fn header_checksum(&self) -> &HashSum {
        &self.header_checksum
    }
    /// Get the size of the archive header.
    pub fn header_size(&self) -> usize {
        self.header_size
    }
    /// Get the hash length used for identifying chunks when building the archive.
    pub fn chunk_hash_length(&self) -> usize {
        self.chunk_hash_length
    }
    /// Get the compression used for chunks in the archive.
    pub fn chunk_compression(&self) -> Option<Compression> {
        self.chunk_compression
    }
    /// Get the version of crate used when building the archive.
    pub fn built_with_version(&self) -> &str {
        &self.created_by_app_version
    }
    /// Iterate chunks as ordered in source.
    pub fn iter_source_chunks(&self) -> impl Iterator<Item = (u64, &ChunkDescriptor)> {
        let mut chunk_offset = 0;
        self.source_order.iter().copied().map(move |index| {
            let offset = chunk_offset;
            let cd = &self.archive_chunks[index as usize];
            chunk_offset += cd.source_size as u64;
            (offset, cd)
        })
    }
    /// Build a ChunkIndex representing the source file.
    pub fn build_source_index(&self) -> ChunkIndex {
        let mut ci = ChunkIndex::new_empty();
        self.iter_source_chunks().for_each(|(offset, cd)| {
            ci.add_chunk(cd.checksum.clone(), cd.source_size as usize, &[offset]);
        });
        ci
    }
    /// Get a stream of chunks from the archive.
    pub fn chunk_stream<'a>(
        &'a mut self,
        chunks: &ChunkIndex,
    ) -> impl Stream<Item = Result<CompressedArchiveChunk, R::Error>> + Unpin + Sized + 'a
    where
        R: Reader + 'a,
    {
        let descriptors: Vec<&ChunkDescriptor> = self
            .archive_chunks
            .iter()
            .filter(|cd| chunks.contains(&cd.checksum))
            .collect();
        let read_at: Vec<ReadAt> = descriptors
            .iter()
            .map(|cd| ReadAt::new(cd.archive_offset, cd.archive_size))
            .collect();
        let archive_chunk_compression = self.chunk_compression().map(|c| c.algorithm);
        self.reader
            .read_chunks(read_at)
            .enumerate()
            .map(move |(index, result)| {
                let source_size = descriptors[index].source_size as usize;
                match result {
                    Ok(chunk) => Ok(CompressedArchiveChunk {
                        chunk: CompressedChunk {
                            compression: if source_size == chunk.len() {
                                // When chunk size matches the source chunk size chunk has not been compressed
                                // since compressing it probably made it bigger.
                                None
                            } else {
                                archive_chunk_compression
                            },
                            data: chunk,
                            source_size,
                        },
                        expected_hash: descriptors[index].checksum.clone(),
                    }),
                    Err(err) => Err(err),
                }
            })
    }
}

fn chunker_config_from_params<R>(
    p: dict::ChunkerParameters,
) -> Result<chunker::Config, ArchiveError<R>> {
    use dict::chunker_parameters::ChunkingAlgorithm;
    match ChunkingAlgorithm::from_i32(p.chunking_algorithm) {
        Some(ChunkingAlgorithm::Buzhash) => Ok(chunker::Config::BuzHash(chunker::FilterConfig {
            filter_bits: chunker::FilterBits::from_bits(p.chunk_filter_bits),
            min_chunk_size: p.min_chunk_size as usize,
            max_chunk_size: p.max_chunk_size as usize,
            window_size: p.rolling_hash_window_size as usize,
        })),
        Some(ChunkingAlgorithm::Rollsum) => Ok(chunker::Config::RollSum(chunker::FilterConfig {
            filter_bits: chunker::FilterBits::from_bits(p.chunk_filter_bits),
            min_chunk_size: p.min_chunk_size as usize,
            max_chunk_size: p.max_chunk_size as usize,
            window_size: p.rolling_hash_window_size as usize,
        })),
        Some(ChunkingAlgorithm::FixedSize) => {
            Ok(chunker::Config::FixedSize(p.max_chunk_size as usize))
        }
        _ => Err(ArchiveError::invalid_archive("unknown chunking algorithm")),
    }
}

fn compression_from_dictionary<R>(
    c: dict::ChunkCompression,
) -> Result<Option<Compression>, ArchiveError<R>> {
    use dict::chunk_compression::CompressionType;
    match CompressionType::from_i32(c.compression) {
        #[cfg(feature = "lzma-compression")]
        Some(dict::chunk_compression::CompressionType::Lzma) => Ok(Some(Compression {
            algorithm: CompressionAlgorithm::Lzma,
            level: c.compression_level,
        })),
        #[cfg(not(feature = "lzma-compression"))]
        Some(CompressionType::Lzma) => Err(ArchiveError::invalid_archive(
            "LZMA compression not enabled",
        )),
        #[cfg(feature = "zstd-compression")]
        Some(CompressionType::Zstd) => Ok(Some(Compression {
            algorithm: CompressionAlgorithm::Zstd,
            level: c.compression_level,
        })),
        #[cfg(not(feature = "zstd-compression"))]
        Some(CompressionType::Zstd) => Err(ArchiveError::invalid_archive(
            "ZSTD compression not enabled",
        )),
        Some(CompressionType::Brotli) => Ok(Some(Compression {
            algorithm: CompressionAlgorithm::Brotli,
            level: c.compression_level,
        })),
        Some(CompressionType::None) => Ok(None),
        None => Err(ArchiveError::invalid_archive("unknown compression")),
    }
}
