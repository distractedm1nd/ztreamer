//! Compact index record projection, shielded-pool filtering, and protobuf response construction.

use tonic::Status;
use ztreamer_protocol::proto;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoolSelection {
    sapling: bool,
    orchard: bool,
    ironwood: bool,
}

impl PoolSelection {
    pub(crate) fn is_all(self) -> bool {
        self.sapling && self.orchard && self.ironwood
    }

    /// Validates a CompactTxStreamer pool request. Empty means every shielded pool.
    pub(crate) fn from_request(pool_types: &[i32]) -> Result<Self, Status> {
        if pool_types.is_empty() {
            return Ok(Self {
                sapling: true,
                orchard: true,
                ironwood: true,
            });
        }

        let mut selection = Self {
            sapling: false,
            orchard: false,
            ironwood: false,
        };
        for &pool_type in pool_types {
            match proto::PoolType::try_from(pool_type) {
                Ok(proto::PoolType::Sapling) => selection.sapling = true,
                Ok(proto::PoolType::Orchard) => selection.orchard = true,
                Ok(proto::PoolType::Ironwood) => selection.ironwood = true,
                Ok(proto::PoolType::Transparent) => {
                    return Err(Status::invalid_argument(
                        "transparent compact data is not supported",
                    ));
                }
                Ok(proto::PoolType::Invalid) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "invalid pool type {pool_type}"
                    )));
                }
            }
        }
        Ok(selection)
    }
}

/// Project in place: discarded pools need no second record/object graph.
pub(crate) fn project_block(
    mut block: proto::CompactBlock,
    pools: PoolSelection,
    nullifiers: bool,
) -> proto::CompactBlock {
    for tx in &mut block.vtx {
        if !pools.sapling {
            tx.spends.clear();
        }
        if !pools.sapling || nullifiers {
            tx.outputs.clear();
        }
        if !pools.orchard {
            tx.actions.clear();
        }
        if !pools.ironwood {
            tx.ironwood_actions.clear();
        }
        if nullifiers {
            for action in tx.actions.iter_mut().chain(&mut tx.ironwood_actions) {
                action.cmx.clear();
                action.ephemeral_key.clear();
                action.ciphertext.clear();
            }
        }
    }
    if nullifiers {
        block.chain_metadata = Some(proto::ChainMetadata::default());
    } else {
        block.vtx.retain(|tx| {
            !tx.spends.is_empty()
                || !tx.outputs.is_empty()
                || !tx.actions.is_empty()
                || !tx.ironwood_actions.is_empty()
        });
    }
    block
}

#[cfg(test)]
mod tests {
    use super::*;
    use ztreamer_indexer::{
        codec::{CompactBlockRecord, TreeSizes},
        parser::{CompactSaplingOutput, CompactShieldedAction, CompactTransaction},
    };

    #[test]
    fn validates_pools_and_projects_full_and_nullifier_blocks() {
        let record = CompactBlockRecord {
            height: 7,
            hash: [1; 32],
            previous_hash: [2; 32],
            time: 3,
            transactions: vec![CompactTransaction {
                index: 5,
                txid: [6; 32],
                sapling_spends: vec![[7; 32]],
                sapling_outputs: vec![CompactSaplingOutput {
                    cmu: [8; 32],
                    ephemeral_key: [9; 32],
                    ciphertext: [10; 52],
                }],
                orchard_actions: vec![action(11)],
                ironwood_actions: vec![action(12)],
            }],
            end_tree_sizes: TreeSizes {
                sapling: 13,
                orchard: 14,
                ironwood: 15,
            },
        };

        let orchard = project_block(
            record.to_proto(),
            PoolSelection::from_request(&[proto::PoolType::Orchard as i32]).unwrap(),
            false,
        );
        assert!(orchard.header.is_empty());
        assert!(orchard.vtx[0].spends.is_empty());
        assert_eq!(orchard.vtx[0].actions[0].cmx, vec![11; 32]);
        assert!(orchard.vtx[0].ironwood_actions.is_empty());
        assert_eq!(
            orchard
                .chain_metadata
                .unwrap()
                .ironwood_commitment_tree_size,
            15
        );

        let nullifiers = project_block(
            record.to_proto(),
            PoolSelection::from_request(&[]).unwrap(),
            true,
        );
        assert_eq!(nullifiers.vtx[0].spends[0].nf, vec![7; 32]);
        assert!(nullifiers.vtx[0].outputs.is_empty());
        assert!(nullifiers.vtx[0].actions[0].cmx.is_empty());
        assert!(nullifiers.vtx[0].ironwood_actions[0].ciphertext.is_empty());
        assert_eq!(
            nullifiers.chain_metadata.unwrap(),
            proto::ChainMetadata::default()
        );

        assert_eq!(
            PoolSelection::from_request(&[proto::PoolType::Transparent as i32])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            PoolSelection::from_request(&[99]).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    fn action(byte: u8) -> CompactShieldedAction {
        CompactShieldedAction {
            nullifier: [byte; 32],
            commitment: [byte; 32],
            ephemeral_key: [byte; 32],
            ciphertext: [byte; 52],
        }
    }
}
