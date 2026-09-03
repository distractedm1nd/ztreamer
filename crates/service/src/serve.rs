//! Compact index record projection, shielded-pool filtering, and protobuf response construction.

use tonic::Status;
use ztreamer_indexer::{
    codec::CompactBlockRecord,
    parser::{CompactShieldedAction, CompactTransaction},
};
use ztreamer_protocol::proto;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoolSelection {
    sapling: bool,
    orchard: bool,
    ironwood: bool,
}

impl PoolSelection {
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

pub(crate) fn compact_block(
    record: &CompactBlockRecord,
    pools: PoolSelection,
) -> proto::CompactBlock {
    convert_block(record, pools, false)
}

pub(crate) fn compact_block_nullifiers(
    record: &CompactBlockRecord,
    pools: PoolSelection,
) -> proto::CompactBlock {
    convert_block(record, pools, true)
}

fn convert_block(
    record: &CompactBlockRecord,
    pools: PoolSelection,
    nullifiers_only: bool,
) -> proto::CompactBlock {
    let mut vtx = Vec::with_capacity(record.transactions.len());
    for transaction in &record.transactions {
        if let Some(transaction) = convert_transaction(transaction, pools, nullifiers_only) {
            vtx.push(transaction);
        }
    }

    proto::CompactBlock {
        height: u64::from(record.height),
        hash: record.hash.to_vec(),
        prev_hash: record.previous_hash.to_vec(),
        time: record.time,
        // CompactTxStreamer has historically left this field unset.
        header: Vec::new(),
        vtx,
        chain_metadata: Some(if nullifiers_only {
            proto::ChainMetadata::default()
        } else {
            proto::ChainMetadata {
                sapling_commitment_tree_size: record.end_tree_sizes.sapling,
                orchard_commitment_tree_size: record.end_tree_sizes.orchard,
                ironwood_commitment_tree_size: record.end_tree_sizes.ironwood,
            }
        }),
    }
}

fn convert_transaction(
    transaction: &CompactTransaction,
    pools: PoolSelection,
    nullifiers_only: bool,
) -> Option<proto::CompactTx> {
    let spends = if pools.sapling {
        transaction
            .sapling_spends
            .iter()
            .map(|nullifier| proto::CompactSaplingSpend {
                nf: nullifier.to_vec(),
            })
            .collect()
    } else {
        Vec::new()
    };
    let outputs = if pools.sapling && !nullifiers_only {
        transaction
            .sapling_outputs
            .iter()
            .map(|output| proto::CompactSaplingOutput {
                cmu: output.cmu.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect()
    } else {
        Vec::new()
    };
    let actions = if pools.orchard {
        convert_actions(&transaction.orchard_actions, nullifiers_only)
    } else {
        Vec::new()
    };
    let ironwood_actions = if pools.ironwood {
        convert_actions(&transaction.ironwood_actions, nullifiers_only)
    } else {
        Vec::new()
    };

    (!spends.is_empty()
        || !outputs.is_empty()
        || !actions.is_empty()
        || !ironwood_actions.is_empty()
        || nullifiers_only)
        .then(|| proto::CompactTx {
            index: transaction.index,
            txid: transaction.txid.to_vec(),
            fee: 0,
            spends,
            outputs,
            actions,
            ironwood_actions,
            vin: Vec::new(),
            vout: Vec::new(),
        })
}

fn convert_actions(
    actions: &[CompactShieldedAction],
    nullifiers_only: bool,
) -> Vec<proto::CompactOrchardAction> {
    actions
        .iter()
        .map(|action| proto::CompactOrchardAction {
            nullifier: action.nullifier.to_vec(),
            cmx: if nullifiers_only {
                Vec::new()
            } else {
                action.commitment.to_vec()
            },
            ephemeral_key: if nullifiers_only {
                Vec::new()
            } else {
                action.ephemeral_key.to_vec()
            },
            ciphertext: if nullifiers_only {
                Vec::new()
            } else {
                action.ciphertext.to_vec()
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ztreamer_indexer::{
        codec::TreeSizes,
        parser::{CompactSaplingOutput, CompactShieldedAction},
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

        let orchard = compact_block(
            &record,
            PoolSelection::from_request(&[proto::PoolType::Orchard as i32]).unwrap(),
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

        let nullifiers =
            compact_block_nullifiers(&record, PoolSelection::from_request(&[]).unwrap());
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
