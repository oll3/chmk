#![recursion_limit = "256"]

mod archive;
mod chunk_index;
mod chunk_location_map;
mod chunker;
mod clone;
mod compression;
mod error;
mod hasher;
mod hashsum;
mod header;
mod http_range_request;
mod reader;
mod rolling_hash;

pub use archive::{Archive, SOURCE_HASH_LENGTH};
pub use chunk_index::{ChunkIndex, ChunkSizeAndOffset, ReorderOp};
pub use chunk_location_map::{ChunkLocation, ChunkLocationMap};
pub use chunker::{Chunker, ChunkerConfig, ChunkerFilterBits, ChunkerFilterConfig};
pub use clone::{
    clone_from_archive, clone_from_readable, clone_in_place, CloneOptions, CloneOutput,
};
pub use compression::Compression;
pub use error::Error;
pub use hasher::{HashFunction, Hasher, HasherBuilder};
pub use hashsum::HashSum;
pub use header::{build_header, ARCHIVE_MAGIC, PRE_HEADER_SIZE};
pub use reader::{Reader, ReaderRemote};
pub use rolling_hash::{BuzHash, RollSum, RollingHash};

pub mod chunk_dictionary {
    include!(concat!(env!("OUT_DIR"), "/chunk_dictionary.rs"));
}
