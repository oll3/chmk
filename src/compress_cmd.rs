use futures_util::future;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::prelude::*;

use crate::info_cmd;
use crate::string_utils::*;
use bitar::build_header;
use bitar::chunk_dictionary as dict;
use bitar::{Chunker, ChunkerConfig, Compression, Error, HashSum, HasherBuilder};

pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

async fn chunk_input<T>(
    mut input: T,
    chunker_config: &ChunkerConfig,
    source_hasher: HasherBuilder,
    compression: Compression,
    temp_file_path: &std::path::Path,
    chunk_hasher: HasherBuilder,
    num_chunk_buffers: usize,
) -> Result<
    (
        HashSum,
        Vec<bitar::chunk_dictionary::ChunkDescriptor>,
        u64,
        Vec<usize>,
    ),
    Error,
>
where
    T: AsyncRead + Unpin,
{
    let mut unique_chunks = HashMap::new();
    let mut source_size: u64 = 0;
    let mut chunk_order = Vec::new();
    let mut archive_offset: u64 = 0;
    let mut unique_chunk_index: usize = 0;
    let mut archive_chunks = Vec::new();
    let mut source_hasher = source_hasher.build();

    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temp_file_path)
        .await
        .expect("failed to open temp file");
    {
        let chunker = Chunker::new(chunker_config, &mut input);
        let mut chunk_stream = chunker
            .map(|result| {
                let (offset, chunk) = result.expect("error while chunking");
                // Build hash of full source
                source_hasher.input(&chunk);
                source_size += chunk.len() as u64;
                let hasher = chunk_hasher.build();
                tokio::task::spawn(async move { (hasher.hash_sum(&chunk), offset, chunk) })
            })
            .buffered(num_chunk_buffers)
            .filter_map(|result| {
                // Filter unique chunks to be compressed
                let (hash, offset, chunk) = result.expect("error while hashing chunk");
                let (unique, chunk_index) = if unique_chunks.contains_key(&hash) {
                    (false, *unique_chunks.get(&hash).unwrap())
                } else {
                    let chunk_index = unique_chunk_index;
                    unique_chunks.insert(hash.clone(), chunk_index);
                    unique_chunk_index += 1;
                    (true, chunk_index)
                };
                // Store a pointer (as index) to unique chunk index for each chunk
                chunk_order.push(chunk_index);

                future::ready(if unique {
                    Some((chunk_index, hash, offset, chunk))
                } else {
                    None
                })
            })
            .map(|(chunk_index, hash, offset, chunk)| {
                tokio::task::spawn(async move {
                    // Compress each chunk
                    let compressed_chunk = compression
                        .compress(&chunk)
                        .expect("failed to compress chunk");
                    (chunk_index, hash, offset, chunk, compressed_chunk)
                })
            })
            .buffered(num_chunk_buffers);

        while let Some(result) = chunk_stream.next().await {
            let (index, hash, offset, chunk, compressed_chunk) =
                result.expect("error while compressing chunk");
            let chunk_len = chunk.len();
            let use_uncompressed_chunk = compressed_chunk.len() >= chunk_len;
            debug!(
                "Chunk {}, '{}', offset: {}, size: {}, {}",
                index,
                hash,
                offset,
                size_to_str(chunk_len),
                if use_uncompressed_chunk {
                    "left uncompressed".to_owned()
                } else {
                    format!("compressed to: {}", size_to_str(compressed_chunk.len()))
                },
            );
            let use_data = if use_uncompressed_chunk {
                chunk
            } else {
                compressed_chunk
            };

            // Store a chunk descriptor which refres to the compressed data
            archive_chunks.push(dict::ChunkDescriptor {
                checksum: hash.to_vec(),
                source_size: chunk_len as u32,
                archive_offset,
                archive_size: use_data.len() as u32,
            });
            archive_offset += use_data.len() as u64;

            // Write the compressed chunk to temp file
            temp_file
                .write_all(&use_data)
                .await
                .expect("Failed to write to temp file");
        }
    }
    Ok((
        source_hasher.finilize(),
        archive_chunks,
        source_size,
        chunk_order,
    ))
}

#[derive(Debug, Clone)]
pub struct Command {
    pub force_create: bool,

    // Use stdin if input not given
    pub input: Option<PathBuf>,
    pub output: PathBuf,
    pub temp_file: PathBuf,
    pub hash_length: usize,
    pub chunker_config: ChunkerConfig,
    pub compression_level: u32,
    pub compression: Compression,
    pub num_chunk_buffers: usize,
}
impl Command {
    pub async fn run(self) -> Result<(), Error> {
        let chunker_config = self.chunker_config.clone();
        let compression = self.compression;
        let mut output_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(self.force_create)
            .truncate(self.force_create)
            .create_new(!self.force_create)
            .open(self.output.clone())
            .expect("failed to open output file");

        // TODO: make configurable
        let chunk_hasher = HasherBuilder {
            function: bitar::HashFunction::Blake3,
            hash_length: self.hash_length,
        };
        let source_hasher = HasherBuilder {
            function: bitar::HashFunction::Blake3,
            hash_length: bitar::SOURCE_HASH_LENGTH,
        };

        let (source_hash, archive_chunks, source_size, chunk_order) =
            if let Some(input_path) = self.input {
                chunk_input(
                    File::open(input_path)
                        .await
                        .expect("failed to open input file"),
                    &chunker_config,
                    source_hasher,
                    compression,
                    &self.temp_file,
                    chunk_hasher,
                    self.num_chunk_buffers,
                )
                .await?
            } else if !atty::is(atty::Stream::Stdin) {
                // Read source from stdin
                chunk_input(
                    tokio::io::stdin(),
                    &chunker_config,
                    source_hasher,
                    compression,
                    &self.temp_file,
                    chunk_hasher,
                    self.num_chunk_buffers,
                )
                .await?
            } else {
                panic!("Missing input");
            };

        let chunker_params = match self.chunker_config {
            ChunkerConfig::BuzHash(hash_config) => dict::ChunkerParameters {
                chunk_filter_bits: hash_config.filter_bits.bits(),
                min_chunk_size: hash_config.min_chunk_size as u32,
                max_chunk_size: hash_config.max_chunk_size as u32,
                rolling_hash_window_size: hash_config.window_size as u32,
                chunk_hash_length: self.hash_length as u32,
                hash_function: dict::HashFunction::from(chunk_hasher.function) as i32,
                chunking_algorithm: dict::chunker_parameters::ChunkingAlgorithm::Buzhash as i32,
            },
            ChunkerConfig::RollSum(hash_config) => dict::ChunkerParameters {
                chunk_filter_bits: hash_config.filter_bits.bits(),
                min_chunk_size: hash_config.min_chunk_size as u32,
                max_chunk_size: hash_config.max_chunk_size as u32,
                rolling_hash_window_size: hash_config.window_size as u32,
                chunk_hash_length: self.hash_length as u32,
                hash_function: dict::HashFunction::from(chunk_hasher.function) as i32,
                chunking_algorithm: dict::chunker_parameters::ChunkingAlgorithm::Rollsum as i32,
            },
            ChunkerConfig::FixedSize(chunk_size) => dict::ChunkerParameters {
                min_chunk_size: 0,
                chunk_filter_bits: 0,
                rolling_hash_window_size: 0,
                max_chunk_size: chunk_size as u32,
                chunk_hash_length: self.hash_length as u32,
                hash_function: dict::HashFunction::from(chunk_hasher.function) as i32,
                chunking_algorithm: dict::chunker_parameters::ChunkingAlgorithm::FixedSize as i32,
            },
        };

        // Build the final archive
        let file_header = dict::ChunkDictionary {
            rebuild_order: chunk_order.iter().map(|&index| index as u32).collect(),
            application_version: PKG_VERSION.to_string(),
            chunk_descriptors: archive_chunks,
            source_checksum: source_hash.to_vec(),
            chunk_compression: Some(self.compression.into()),
            source_total_size: source_size,
            source_hash_function: dict::HashFunction::from(source_hasher.function) as i32,
            chunker_params: Some(chunker_params),
        };
        let header_buf = build_header(&file_header, None)?;
        output_file
            .write_all(&header_buf)
            .expect("failed to write header");
        {
            let mut temp_file =
                std::fs::File::open(&self.temp_file).expect("failed to open temporary file");
            std::io::copy(&mut temp_file, &mut output_file)
                .expect("failed to write chunk data to output file");
        }
        std::fs::remove_file(&self.temp_file).expect("unable to remove temporary file");
        drop(output_file);
        {
            // Print archive info
            let mut reader = File::open(self.output).await?;
            info_cmd::print_archive_reader(&mut reader).await?;
        }
        Ok(())
    }
}
