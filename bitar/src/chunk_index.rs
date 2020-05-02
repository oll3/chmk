use futures_util::stream::StreamExt;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use tokio::io::AsyncRead;

use crate::{
    chunk_dictionary::ChunkDictionary,
    chunk_location_map::{ChunkLocation, ChunkLocationMap},
    chunker::{Chunker, ChunkerConfig},
    Error, HashSum, HasherBuilder,
};

#[derive(Debug, Clone, PartialEq)]
pub enum ReorderOp<'a> {
    Copy {
        hash: &'a HashSum,
        source: ChunkLocation,
        dest: Vec<u64>,
    },
    StoreInMem {
        hash: &'a HashSum,
        source: ChunkLocation,
    },
}

#[derive(Eq, PartialEq)]
struct MoveChunk<'a> {
    hash: &'a HashSum,
    source_offset: u64,
    size: usize,
}

impl<'a> Ord for MoveChunk<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source_offset.cmp(&other.source_offset)
    }
}

impl<'a> PartialOrd for MoveChunk<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct ChunkSizeAndOffset {
    pub size: usize,
    pub offsets: BinaryHeap<u64>,
}

impl From<(usize, &[u64])> for ChunkSizeAndOffset {
    fn from((size, offsets): (usize, &[u64])) -> Self {
        Self {
            size,
            offsets: offsets.iter().copied().collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChunkIndex(HashMap<HashSum, ChunkSizeAndOffset>);

impl ChunkIndex {
    pub async fn from_readable<T>(
        chunker_config: &ChunkerConfig,
        hasher: HasherBuilder,
        max_buffered_chunks: usize,
        readable: &mut T,
    ) -> Result<Self, Error>
    where
        T: AsyncRead + Unpin,
    {
        let chunker = Chunker::new(chunker_config, readable);
        let mut chunk_stream = chunker
            .map(|result| {
                let hasher = hasher.build();
                tokio::task::spawn(async move {
                    result.map(|(offset, chunk)| (hasher.hash_sum(&chunk), offset, chunk.len()))
                })
            })
            .buffered(max_buffered_chunks);

        let mut chunk_lookup: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
        while let Some(result) = chunk_stream.next().await {
            let (chunk_hash, chunk_offset, chunk_size) = result.unwrap()?;
            if let Some(cd) = chunk_lookup.get_mut(&chunk_hash) {
                cd.offsets.push(chunk_offset);
            } else {
                let mut offsets = BinaryHeap::new();
                offsets.push(chunk_offset);
                chunk_lookup.insert(
                    chunk_hash.clone(),
                    ChunkSizeAndOffset {
                        size: chunk_size,
                        offsets,
                    },
                );
            }
        }
        Ok(Self(chunk_lookup))
    }

    pub fn from_dictionary(dict: &ChunkDictionary) -> Self {
        let mut chunk_lookup: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
        let mut chunk_offset = 0;
        dict.rebuild_order.iter().for_each(|&chunk_index| {
            let chunk_index = chunk_index as usize;
            let chunk_hash = HashSum::from_slice(&dict.chunk_descriptors[chunk_index].checksum[..]);
            let chunk_size = dict.chunk_descriptors[chunk_index].source_size as usize;
            if let Some(cd) = chunk_lookup.get_mut(&chunk_hash) {
                cd.offsets.push(chunk_offset);
            } else {
                let mut offsets = BinaryHeap::new();
                offsets.push(chunk_offset);
                chunk_lookup.insert(
                    chunk_hash.clone(),
                    ChunkSizeAndOffset {
                        size: chunk_size,
                        offsets,
                    },
                );
            }
            chunk_offset += chunk_size as u64;
        });
        Self(chunk_lookup)
    }

    pub fn remove(&mut self, hash: &HashSum) -> bool {
        self.0.remove(hash).is_some()
    }

    pub fn contains(&self, hash: &HashSum) -> bool {
        self.0.contains_key(hash)
    }

    pub fn get(&self, hash: &HashSum) -> Option<&ChunkSizeAndOffset> {
        self.0.get(hash)
    }
    pub fn get_first_offset(&self, hash: &HashSum) -> Option<(usize, u64)> {
        self.get(&hash)
            .map(|ChunkSizeAndOffset { size, offsets }| (*size, *offsets.peek().unwrap()))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate source offsets of a chunk
    pub fn offsets<'a>(&'a self, hash: &HashSum) -> Option<impl Iterator<Item = u64> + 'a> {
        self.0.get(hash).map(|d| d.offsets.iter().copied())
    }

    /// Iterate index
    pub fn iter(&self) -> impl Iterator<Item = (&HashSum, &ChunkSizeAndOffset)> {
        self.0.iter()
    }

    /// Iterate chunk hashes in index
    pub fn keys(&self) -> impl Iterator<Item = &HashSum> {
        self.0.keys()
    }

    /// Filter the given chunk index for chunks which are already in place in self
    ///
    /// Returns the number of chunks filtered and total size of them.
    pub fn strip_chunks_already_in_place(&self, chunk_set: &mut ChunkIndex) -> (usize, u64) {
        let mut num_alread_in_place = 0;
        let mut total_size = 0;
        let new_set: HashMap<HashSum, ChunkSizeAndOffset> = chunk_set
            .0
            .iter()
            .filter_map(|(hash, cd)| {
                if let Some(ChunkSizeAndOffset { offsets, size }) = self.get(hash) {
                    // Test if the chunk offsets are equal in the given set and in our set
                    if cd.offsets.iter().zip(offsets.iter()).all(|(a, b)| a == b) {
                        num_alread_in_place += 1;
                        total_size += *size as u64;
                        return None;
                    }
                }
                Some((hash.clone(), cd.clone()))
            })
            .collect();
        chunk_set.0 = new_set;
        (num_alread_in_place, total_size)
    }

    // Each chunk to reorder will potentially overwrite other chunks.
    // Hence we do a DFS for each chunk and the chunks it will overlap and build the
    // reordering operations in reversed order, down from the leaf nodes up to the root.
    //
    // The tree-like graph may have circular dependencies, where chunk 1 wants to
    // overwrite chunk 2 and chunk 2 wants to overwrite chunk 1. Here one chunk must
    // be kept in memory while the other one(s) is copied into place, and finally write
    // the in memory chunk to its new place.
    fn build_reorder_ops<'a>(
        &'a self,
        root_chunk: MoveChunk<'a>,
        new_order: &ChunkIndex,
        source_layout: &ChunkLocationMap<&'a HashSum>,
        visited: &mut HashSet<&'a HashSum>,
        ops: &mut Vec<ReorderOp<'a>>,
    ) {
        let mut stack: Vec<(MoveChunk, Option<ReorderOp>)> = Vec::new();
        stack.push((root_chunk, None));
        while let Some((chunk, op)) = stack.last_mut() {
            if !visited.contains(chunk.hash) {
                visited.insert(chunk.hash);

                let mut child_stack = Vec::new();
                let mut destinations = Vec::new();

                if let Some(ChunkSizeAndOffset {
                    size,
                    offsets: target_offsets,
                }) = new_order.get(&chunk.hash)
                {
                    target_offsets.iter().for_each(|&target_offset| {
                        let dest_location = ChunkLocation::new(target_offset, *size);
                        source_layout
                            .iter_overlapping(&dest_location)
                            // filter overlapping chunks with same hash
                            .filter(|(_, &overlapped_hash)| chunk.hash != overlapped_hash)
                            .for_each(|(_location, &overlapped_hash)| {
                                let (chunk_size, chunk_offset) =
                                    self.get_first_offset(overlapped_hash).unwrap();
                                if visited.contains(overlapped_hash) {
                                    ops.push(ReorderOp::StoreInMem {
                                        hash: overlapped_hash,
                                        source: ChunkLocation::new(chunk_offset, chunk_size),
                                    });
                                } else {
                                    child_stack.push((
                                        MoveChunk {
                                            hash: overlapped_hash,
                                            source_offset: chunk_offset,
                                            size: chunk_size,
                                        },
                                        None,
                                    ));
                                }
                            });
                        destinations.push(dest_location.offset);
                    });
                }
                *op = Some(ReorderOp::Copy {
                    hash: chunk.hash,
                    source: ChunkLocation::new(chunk.source_offset, chunk.size),
                    dest: destinations,
                });
                stack.append(&mut child_stack);
            } else if let Some((_chunk, Some(op))) = stack.pop() {
                ops.push(op);
            }
        }
    }

    pub fn reorder_ops(&self, new_order: &Self) -> Vec<ReorderOp> {
        // Create intersection between the two chunk sets to find which chunks that should be moved.
        // Also generate a layout of the source where we can go from offset+size to which chunks are
        // located within that range.
        let mut source_layout: ChunkLocationMap<&HashSum> = ChunkLocationMap::new();
        let chunks_to_move: Vec<MoveChunk> = {
            let mut chunks = self
                .0
                .iter()
                .filter_map(|cl| {
                    if new_order.contains(cl.0) {
                        let (size, offset) = self.get_first_offset(cl.0).unwrap();
                        source_layout.insert(ChunkLocation::new(offset, size), cl.0);
                        Some(MoveChunk {
                            hash: cl.0,
                            source_offset: offset,
                            size,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<MoveChunk>>();

            // Sort chunks on source offset to make the reordering deterministic.
            chunks.sort();
            chunks
        };

        let mut ops: Vec<ReorderOp> = Vec::new();
        let mut chunks_processed: HashSet<&HashSum> = HashSet::new();
        chunks_to_move.into_iter().for_each(|chunk| {
            if !chunks_processed.contains(&chunk.hash) {
                let mut chunks_in_tree = HashSet::new();
                self.build_reorder_ops(
                    chunk,
                    new_order,
                    &source_layout,
                    &mut chunks_in_tree,
                    &mut ops,
                );

                chunks_in_tree.into_iter().for_each(|hash| {
                    let ChunkSizeAndOffset { size, offsets } = &self.get(&hash).unwrap();
                    offsets.iter().for_each(|offset| {
                        source_layout.remove(&ChunkLocation::new(*offset, *size));
                    });
                    chunks_processed.insert(hash);
                });
            }
        });
        ops
    }
}

impl From<HashMap<HashSum, ChunkSizeAndOffset>> for ChunkIndex {
    fn from(v: HashMap<HashSum, ChunkSizeAndOffset>) -> Self {
        Self(v)
    }
}

impl std::iter::FromIterator<(HashSum, ChunkSizeAndOffset)> for ChunkIndex {
    fn from_iter<I: IntoIterator<Item = (HashSum, ChunkSizeAndOffset)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reorder_with_overlap_and_loop() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[10][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[30, 50][..]).into());
            ChunkIndex(chunks)
        };
        let target_index = {
            let mut chunks: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[60][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[50][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[10, 30][..]).into());
            chunks.insert(HashSum::from_slice(&[4]), (5, &[0, 5][..]).into());
            ChunkIndex(chunks)
        };
        let ops = current_index.reorder_ops(&target_index);
        assert_eq!(
            ops,
            vec![
                ReorderOp::StoreInMem {
                    hash: &HashSum::from_slice(&[3]),
                    source: ChunkLocation {
                        offset: 50,
                        size: 20,
                    },
                },
                ReorderOp::Copy {
                    hash: &HashSum::from_slice(&[2]),
                    source: ChunkLocation {
                        offset: 10,
                        size: 20,
                    },
                    dest: vec![50],
                },
                ReorderOp::Copy {
                    hash: &HashSum::from_slice(&[3]),
                    source: ChunkLocation {
                        offset: 50,
                        size: 20,
                    },
                    dest: vec![30, 10],
                },
                ReorderOp::Copy {
                    hash: &HashSum::from_slice(&[1]),
                    source: ChunkLocation {
                        offset: 0,
                        size: 10,
                    },
                    dest: vec![60],
                }
            ]
        )
    }

    #[test]
    fn chunks_in_place() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[20][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[30, 50][..]).into());
            ChunkIndex(chunks)
        };
        let mut target_index = {
            let mut chunks: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[10][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[20][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[30, 50][..]).into());
            ChunkIndex(chunks)
        };
        current_index.strip_chunks_already_in_place(&mut target_index);
        assert_eq!(target_index.len(), 1);
        let ChunkSizeAndOffset { size, offsets } =
            target_index.get(&HashSum::from_slice(&[1])).unwrap();
        assert_eq!(*size, 10);
        assert_eq!(offsets.clone().into_sorted_vec(), vec![10]);
    }
}
