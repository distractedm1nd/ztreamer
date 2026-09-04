//! Compact index record projection, shielded-pool filtering, and protobuf response construction.

use std::collections::HashSet;

use tonic::Status;
use zakura_chain::transaction;
use ztreamer_indexer::{
    Digest,
    codec::CompactBlockRecord,
    parser::{CompactShieldedAction, CompactTransaction, parse_transaction},
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

pub(crate) fn compact_mempool_txs(
    transactions: &[(Vec<u8>, transaction::Hash)],
    pools: PoolSelection,
    exclude_txid_suffixes: &[Vec<u8>],
) -> Result<Vec<proto::CompactTx>, Status> {
    let excluded = excluded_txids(transactions, exclude_txid_suffixes);
    let mut compact = Vec::with_capacity(transactions.len().saturating_sub(excluded.len()));
    for (bytes, txid) in transactions {
        if excluded.contains(&txid.0) {
            continue;
        }
        let transaction = parse_transaction(bytes, *txid, 0).map_err(|error| {
            Status::internal(format!("mempool transaction {txid} is unparsable: {error}"))
        })?;
        if let Some(transaction) = convert_transaction(&transaction, pools, false) {
            compact.push(transaction);
        }
    }
    Ok(compact)
}

fn excluded_txids(
    transactions: &[(Vec<u8>, transaction::Hash)],
    suffixes: &[Vec<u8>],
) -> HashSet<Digest> {
    let mut excluded = HashSet::new();
    for suffix in suffixes {
        let mut matches = transactions
            .iter()
            .filter(|(_, txid)| txid.0.ends_with(suffix));
        if let (Some((_, txid)), None) = (matches.next(), matches.next()) {
            excluded.insert(txid.0);
        }
    }
    excluded
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
    use zakura_chain::{
        block::Block,
        serialization::{ZcashDeserialize as _, ZcashSerialize as _},
        transaction::Transaction,
    };
    use zakura_test::vectors::{BLOCK_MAINNET_949496_BYTES, BLOCK_TESTNET_1842421_BYTES};
    use ztreamer_indexer::{
        codec::TreeSizes,
        parser::{CompactSaplingOutput, CompactShieldedAction, RawIndexBlock, parse_block},
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

    #[test]
    fn mempool_projection_matches_the_block_projection_and_flattens_the_index() {
        let mut saw_mined_index = false;
        for encoded in [&*BLOCK_MAINNET_949496_BYTES, &*BLOCK_TESTNET_1842421_BYTES] {
            let block = Block::zcash_deserialize(encoded.as_slice()).unwrap();
            let parsed = parse_block(&RawIndexBlock {
                height: block.coinbase_height().unwrap(),
                hash: block.hash(),
                bytes: encoded.to_vec(),
                txids: block
                    .transactions
                    .iter()
                    .map(|transaction| transaction.hash())
                    .collect(),
            })
            .unwrap();
            let record = CompactBlockRecord {
                height: parsed.height,
                hash: parsed.hash,
                previous_hash: parsed.previous_hash,
                time: parsed.time,
                transactions: parsed.transactions,
                end_tree_sizes: TreeSizes::default(),
            };
            let mined = compact_block(&record, all_pools());

            let shielded: Vec<&Transaction> = block
                .transactions
                .iter()
                .filter(|transaction| {
                    mined
                        .vtx
                        .iter()
                        .any(|compact| compact.txid == transaction.hash().0.to_vec())
                })
                .map(|transaction| transaction.as_ref())
                .collect();
            let entries: Vec<(Vec<u8>, transaction::Hash)> = shielded
                .iter()
                .map(|transaction| entry(transaction))
                .collect();
            let compact = compact_mempool_txs(&entries, all_pools(), &[]).unwrap();

            assert_eq!(compact.len(), mined.vtx.len());
            assert!(!compact.is_empty());
            for (mempool, mined) in compact.iter().zip(&mined.vtx) {
                saw_mined_index |= mined.index != 0;
                assert_eq!(mempool.index, 0);
                assert_eq!(
                    mempool,
                    &proto::CompactTx {
                        index: 0,
                        ..mined.clone()
                    }
                );
            }
            for (mempool, transaction) in compact.iter().zip(&shielded) {
                assert_shielded_fields(mempool, transaction);
            }
        }
        assert!(saw_mined_index);
    }

    fn assert_shielded_fields(compact: &proto::CompactTx, transaction: &Transaction) {
        let outputs: Vec<_> = transaction.sapling_outputs().collect();
        assert_eq!(compact.outputs.len(), outputs.len());
        for (compact, output) in compact.outputs.iter().zip(outputs) {
            let encrypted: [u8; 580] = output.enc_ciphertext.into();
            assert_eq!(compact.cmu, output.cm_u.to_bytes().to_vec());
            let ephemeral_key: [u8; 32] = (&output.ephemeral_key).into();
            assert_eq!(compact.ephemeral_key, ephemeral_key.to_vec());
            assert_eq!(compact.ciphertext, encrypted[..52].to_vec());
        }

        let actions: Vec<_> = transaction.orchard_actions().collect();
        assert_eq!(compact.actions.len(), actions.len());
        for (compact, action) in compact.actions.iter().zip(actions) {
            let nullifier: [u8; 32] = action.nullifier.into();
            let commitment: [u8; 32] = action.cm_x.into();
            let encrypted: [u8; 580] = action.enc_ciphertext.into();
            assert_eq!(compact.nullifier, nullifier.to_vec());
            assert_eq!(compact.cmx, commitment.to_vec());
            assert_eq!(compact.ciphertext, encrypted[..52].to_vec());
        }
    }

    #[test]
    fn mempool_projection_prunes_the_pools_the_client_did_not_request() {
        let sapling = shielded_transaction(&BLOCK_MAINNET_949496_BYTES, |transaction| {
            transaction.sapling_outputs().next().is_some()
        });
        let orchard = shielded_transaction(&BLOCK_TESTNET_1842421_BYTES, |transaction| {
            transaction.orchard_actions().next().is_some()
        });
        let transactions = vec![entry(&sapling), entry(&orchard)];

        let pools = PoolSelection::from_request(&[proto::PoolType::Orchard as i32]).unwrap();
        let compact = compact_mempool_txs(&transactions, pools, &[]).unwrap();

        assert_eq!(compact.len(), 1);
        assert_eq!(compact[0].txid, orchard.hash().0.to_vec());
        assert!(compact[0].outputs.is_empty());
        assert!(compact[0].spends.is_empty());
        assert!(!compact[0].actions.is_empty());

        let pools = PoolSelection::from_request(&[proto::PoolType::Ironwood as i32]).unwrap();
        assert!(
            compact_mempool_txs(&transactions, pools, &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn mempool_exclusions_drop_unambiguous_suffixes_only() {
        let orchard = shielded_transaction(&BLOCK_TESTNET_1842421_BYTES, |transaction| {
            transaction.orchard_actions().next().is_some()
        });
        let bytes = orchard.zcash_serialize_to_vec().unwrap();
        let mut shared = [3; 32];
        shared[31] = 2;
        let transactions = vec![
            (bytes.clone(), transaction::Hash([1; 32])),
            (bytes.clone(), transaction::Hash([2; 32])),
            (bytes, transaction::Hash(shared)),
        ];
        let txids = |compact: Vec<proto::CompactTx>| {
            compact
                .into_iter()
                .map(|transaction| transaction.txid)
                .collect::<Vec<_>>()
        };

        assert_eq!(
            txids(compact_mempool_txs(&transactions, all_pools(), &[vec![1]]).unwrap()),
            [vec![2; 32], shared.to_vec()]
        );
        assert_eq!(
            txids(compact_mempool_txs(&transactions, all_pools(), &[vec![1; 32]]).unwrap()),
            [vec![2; 32], shared.to_vec()]
        );

        let every = [vec![1; 32], vec![2; 32], shared.to_vec()];
        assert_eq!(
            txids(compact_mempool_txs(&transactions, all_pools(), &[vec![2]]).unwrap()),
            every
        );
        assert_eq!(
            txids(compact_mempool_txs(&transactions, all_pools(), &[Vec::new(), vec![9]]).unwrap()),
            every
        );
        assert_eq!(
            txids(
                compact_mempool_txs(&transactions, all_pools(), &[vec![1], vec![3, 3, 2]]).unwrap()
            ),
            [vec![2; 32]]
        );
    }

    #[test]
    fn unparsable_mempool_transactions_fail_the_request_unless_excluded() {
        let orchard = shielded_transaction(&BLOCK_TESTNET_1842421_BYTES, |transaction| {
            transaction.orchard_actions().next().is_some()
        });
        let unparsable = (vec![1, 0, 0, 0, 0xff], transaction::Hash([4; 32]));
        let transactions = vec![entry(&orchard), unparsable];

        let status = compact_mempool_txs(&transactions, all_pools(), &[]).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("unparsable"));

        let compact = compact_mempool_txs(&transactions, all_pools(), &[vec![4; 32]]).unwrap();
        assert_eq!(
            compact
                .into_iter()
                .map(|transaction| transaction.txid)
                .collect::<Vec<_>>(),
            [orchard.hash().0.to_vec()]
        );
    }

    #[test]
    fn an_empty_mempool_projects_to_nothing() {
        assert!(
            compact_mempool_txs(&[], all_pools(), &[vec![1]])
                .unwrap()
                .is_empty()
        );
    }

    fn all_pools() -> PoolSelection {
        PoolSelection::from_request(&[]).unwrap()
    }

    fn entry(transaction: &Transaction) -> (Vec<u8>, transaction::Hash) {
        (
            transaction.zcash_serialize_to_vec().unwrap(),
            transaction.hash(),
        )
    }

    fn shielded_transaction(block: &[u8], shielded: impl Fn(&Transaction) -> bool) -> Transaction {
        Block::zcash_deserialize(block)
            .unwrap()
            .transactions
            .iter()
            .find(|transaction| shielded(transaction))
            .map(|transaction| (**transaction).clone())
            .expect("the block vector holds a shielded transaction")
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
