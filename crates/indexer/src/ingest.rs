//! Restores parallel parser output to chain order and builds bounded LMDB write batches.

use std::collections::BTreeMap;

use crate::{
    Digest,
    codec::{CodecError, CompactBlockRecord, TreeSizes},
    index::{IndexState, RANGE_SIZE},
    parser::ParsedCompactBlock,
};

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("prepared height {height} is stale or duplicated")]
    DuplicateHeight { height: u32 },
    #[error("block {height} does not connect to the previous canonical hash")]
    Parent { height: u32 },
    #[error("block {height} commitment-tree size overflow")]
    TreeSize { height: u32 },
    #[error("ordered pending bytes exceed the {limit}-byte budget")]
    PendingBytes { limit: usize },
    #[error("write batch bytes exceed the {limit}-byte budget")]
    BatchBytes { limit: usize },
    #[error("height, generation, or byte count overflow")]
    Overflow,
}

/// Single-threaded coordinator between parallel parsers that push
/// out of order [`ParsedCompactBlock`]s and the [`crate::index::Index`].
///
/// Produces [`WriteBatch`]es of ordered blocks.
pub struct OrderedBuilder {
    /// Lowest height that will be included in the next batch.
    next_height: u32,
    /// Expected parent hash for `next_height`.
    previous_hash: Option<Digest>,
    /// Cumulative [`TreeSizes`] immediately before `next_height`.
    tree_sizes: TreeSizes,
    /// The next [`WriteBatch::base_generation`].
    generation: u64,

    /// Ordered map of parsed blocks pending batch inclusion.
    pending: BTreeMap<u32, (ParsedCompactBlock, usize)>,
    /// Total encoded size of all blocks in `pending`.
    pending_bytes: usize,

    /// Exclusive end of the contiguous pending prefix starting at `next_height`.
    ready_end: u64,
    /// Total encoded size of all blocks in `[next_heicht, ready_end)`.
    ready_bytes: usize,

    /// Hard memory budget for pending.
    max_pending_bytes: usize,
}

/// An ordered batch produced by [`OrderedBuilder::build_batch`] for appending
/// to the [`crate::index::Index`].
pub struct WriteBatch {
    /// Index generation the batch was built against.
    pub(crate) base_generation: u64,
    /// Blocks below and equaling this height are sealed,
    /// the rest stored in `mutable_blocks`
    pub(crate) seal_through: Option<u32>,
    pub(crate) records: Vec<CompactBlockRecord>,
}

impl OrderedBuilder {
    pub fn new(state: IndexState, max_pending_bytes: usize) -> Result<Self, IngestError> {
        let next_height = match state.durable_tip {
            Some(tip) => tip.height.checked_add(1).ok_or(IngestError::Overflow)?,
            None => 0,
        };
        Ok(Self {
            next_height,
            previous_hash: state.durable_tip.map(|tip| tip.hash),
            tree_sizes: state.tree_sizes,
            generation: state.generation,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            ready_end: u64::from(next_height),
            ready_bytes: 0,
            max_pending_bytes,
        })
    }

    /// Hands a block over to queue inclusion in a [`WriteBatch`]
    pub fn push(&mut self, block: ParsedCompactBlock) -> Result<(), IngestError> {
        if block.height < self.next_height || self.pending.contains_key(&block.height) {
            return Err(IngestError::DuplicateHeight {
                height: block.height,
            });
        }
        let bytes = CompactBlockRecord::encoded_size_bound(&block.transactions)?;
        let pending_bytes = self
            .pending_bytes
            .checked_add(bytes)
            .ok_or(IngestError::Overflow)?;
        if pending_bytes > self.max_pending_bytes {
            return Err(IngestError::PendingBytes {
                limit: self.max_pending_bytes,
            });
        }
        self.pending_bytes = pending_bytes;
        let height = block.height;
        self.pending.insert(height, (block, bytes));
        if u64::from(height) == self.ready_end {
            while let Ok(height) = self.ready_end.try_into()
                && let Some((_, bytes)) = self.pending.get(&height)
            {
                self.ready_bytes = self
                    .ready_bytes
                    .checked_add(*bytes)
                    .ok_or(IngestError::Overflow)?;
                self.ready_end += 1;
            }
        }
        Ok(())
    }

    pub(crate) fn ready_bytes(&self) -> usize {
        self.ready_bytes
    }

    /// Builds a [`WriteBatch`], leaving gaps and depth-0..9 blocks pending.
    pub fn build_batch(
        &mut self,
        durable_through: Option<u32>,
        seal_through: Option<u32>,
        max_batch_bytes: usize,
    ) -> Result<Option<WriteBatch>, IngestError> {
        let range_end = self
            .next_height
            .checked_add(RANGE_SIZE - 1 - self.next_height % RANGE_SIZE)
            .ok_or(IngestError::Overflow)?;
        if seal_through.is_some_and(|height| range_end <= height)
            && self.ready_end <= u64::from(range_end)
            && self.pending_bytes <= self.max_pending_bytes.saturating_sub(max_batch_bytes)
        {
            return Ok(None);
        }
        let base_generation = self.generation;
        let mut batch_bytes = 0usize;
        let mut records = Vec::new();

        while let Some((block, bytes)) = self.pending.get(&self.next_height) {
            if durable_through.is_none_or(|height| block.height > height) {
                break;
            }
            if batch_bytes
                .checked_add(*bytes)
                .ok_or(IngestError::Overflow)?
                > max_batch_bytes
            {
                if records.is_empty() {
                    return Err(IngestError::BatchBytes {
                        limit: max_batch_bytes,
                    });
                }
                let range_end = block
                    .height
                    .checked_add(RANGE_SIZE - 1 - block.height % RANGE_SIZE)
                    .ok_or(IngestError::Overflow)?;
                if block.height.is_multiple_of(RANGE_SIZE)
                    || seal_through.is_none_or(|height| range_end > height)
                {
                    break;
                }
            }
            if self
                .previous_hash
                .is_some_and(|previous| block.previous_hash != previous)
            {
                return Err(IngestError::Parent {
                    height: block.height,
                });
            }
            let end_tree_sizes = TreeSizes {
                sapling: self
                    .tree_sizes
                    .sapling
                    .checked_add(block.sapling_additions)
                    .ok_or(IngestError::TreeSize {
                        height: block.height,
                    })?,
                orchard: self
                    .tree_sizes
                    .orchard
                    .checked_add(block.orchard_additions)
                    .ok_or(IngestError::TreeSize {
                        height: block.height,
                    })?,
                ironwood: self
                    .tree_sizes
                    .ironwood
                    .checked_add(block.ironwood_additions)
                    .ok_or(IngestError::TreeSize {
                        height: block.height,
                    })?,
            };
            let next_height = self
                .next_height
                .checked_add(1)
                .ok_or(IngestError::Overflow)?;
            let (block, bytes) = self
                .pending
                .remove(&self.next_height)
                .expect("the pending block was just checked");

            batch_bytes += bytes;
            self.pending_bytes -= bytes;
            self.ready_bytes -= bytes;
            self.next_height = next_height;
            self.previous_hash = Some(block.hash);
            self.tree_sizes = end_tree_sizes;
            records.push(CompactBlockRecord {
                height: block.height,
                hash: block.hash,
                previous_hash: block.previous_hash,
                time: block.time,
                transactions: block.transactions,
                end_tree_sizes,
            });
        }

        if records.is_empty() {
            return Ok(None);
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(IngestError::Overflow)?;
        Ok(Some(WriteBatch {
            base_generation,
            seal_through,
            records,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{CompactSaplingOutput, CompactTransaction};

    #[test]
    fn reassembles_out_of_order_blocks_and_assigns_tree_sizes() {
        let mut first = prepared(0);
        first.transactions.push(CompactTransaction {
            index: 1,
            txid: [7; 32],
            sapling_spends: Vec::new(),
            sapling_outputs: vec![CompactSaplingOutput {
                cmu: [1; 32],
                ephemeral_key: [2; 32],
                ciphertext: [3; 52],
            }],
            orchard_actions: Vec::new(),
            ironwood_actions: Vec::new(),
        });
        first.sapling_additions = 1;
        let mut builder = OrderedBuilder::new(IndexState::default(), 1_000_000).unwrap();

        builder.push(prepared(1)).unwrap();
        assert!(
            builder
                .build_batch(Some(1), None, 1_000_000)
                .unwrap()
                .is_none()
        );
        builder.push(first).unwrap();
        let batch = builder
            .build_batch(Some(1), None, 1_000_000)
            .unwrap()
            .unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].height, 0);
        assert_eq!(batch.records[1].height, 1);
        assert_eq!(batch.records[1].end_tree_sizes.sapling, 1);
    }

    #[test]
    fn ready_bytes_exclude_blocks_beyond_a_gap() {
        let bytes = CompactBlockRecord::encoded_size_bound(&[]).unwrap();
        let mut builder = OrderedBuilder::new(IndexState::default(), 1_000_000).unwrap();

        builder.push(prepared(1)).unwrap();
        builder.push(prepared(2)).unwrap();
        assert_eq!(builder.ready_bytes(), 0);

        builder.push(prepared(0)).unwrap();
        assert_eq!(builder.ready_bytes(), 3 * bytes);
        let batch = builder
            .build_batch(Some(2), None, 2 * bytes)
            .unwrap()
            .unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(builder.ready_bytes(), bytes);
    }

    #[test]
    fn sealable_ranges_stay_in_one_batch() {
        let bytes = CompactBlockRecord::encoded_size_bound(&[]).unwrap();
        let mut builder = OrderedBuilder::new(IndexState::default(), 1_000_000).unwrap();
        for height in 0..RANGE_SIZE / 2 {
            builder.push(prepared(height)).unwrap();
        }
        assert!(
            builder
                .build_batch(Some(RANGE_SIZE - 1), Some(RANGE_SIZE - 1), 2 * bytes)
                .unwrap()
                .is_none()
        );
        for height in RANGE_SIZE / 2..RANGE_SIZE {
            builder.push(prepared(height)).unwrap();
        }

        let batch = builder
            .build_batch(Some(RANGE_SIZE - 1), Some(RANGE_SIZE - 1), 2 * bytes)
            .unwrap()
            .unwrap();
        assert_eq!(batch.records.len(), RANGE_SIZE as usize);
    }

    fn prepared(height: u32) -> ParsedCompactBlock {
        ParsedCompactBlock {
            height,
            hash: hash(height),
            previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
            time: height,
            transactions: Vec::new(),
            sapling_additions: 0,
            orchard_additions: 0,
            ironwood_additions: 0,
        }
    }

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
