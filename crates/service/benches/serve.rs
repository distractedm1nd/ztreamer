// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio_stream::StreamExt as _;
use tonic::Request;
use zakura_chain::{block, parameters::Network, serialization::ZcashDeserialize as _};
use zakura_state::Config;
use zakura_test::vectors::BLOCK_MAINNET_1687121_BYTES;
use ztreamer_indexer::{
    Digest,
    index::{Index, IndexState},
    ingest::OrderedBuilder,
    parser::{CompactTransaction, ParsedCompactBlock, RawIndexBlock, parse_block},
};
use ztreamer_protocol::proto::{self, compact_tx_streamer_server::CompactTxStreamer};
use ztreamer_service::CompactService;

const TIP: u32 = 2_005;

fn hash(height: u32) -> Digest {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash
}

fn shielded_transactions() -> Vec<CompactTransaction> {
    let bytes = BLOCK_MAINNET_1687121_BYTES.to_vec();
    let block = block::Block::zcash_deserialize(bytes.as_slice()).unwrap();
    parse_block(&RawIndexBlock {
        height: block.coinbase_height().unwrap(),
        hash: block.hash(),
        bytes,
        txids: block
            .transactions
            .iter()
            .map(|transaction| transaction.hash())
            .collect(),
    })
    .unwrap()
    .transactions
}

fn index_through(index: &Index, tip: u32) -> IndexState {
    let transactions = shielded_transactions();
    let mut builder = OrderedBuilder::new(IndexState::default(), 64 * 1024 * 1024).unwrap();
    for height in 0..=tip {
        builder
            .push(ParsedCompactBlock {
                height,
                hash: hash(height),
                previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
                time: height,
                transactions: transactions.clone(),
                sapling_additions: 0,
                orchard_additions: 0,
                ironwood_additions: 0,
            })
            .unwrap();
    }
    let mut state = IndexState::default();
    while let Some(batch) = builder
        .build_batch(Some(tip), Some(tip), 64 * 1024 * 1024)
        .unwrap()
    {
        state = index.write(batch).unwrap();
    }
    state
}

fn block_id(height: u32) -> Option<proto::BlockId> {
    Some(proto::BlockId {
        height: u64::from(height),
        hash: Vec::new(),
    })
}

fn get_block_range(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", [9; 32]).unwrap());
    let state = index_through(&index, TIP);
    let (_state_service, read_service, _tip, _change) = runtime.block_on(async {
        zakura_state::init(
            Config::ephemeral(),
            &Network::Mainnet,
            block::Height::MAX,
            0,
        )
        .await
        .expect("ephemeral state initializes")
    });
    let service = CompactService::new(index, state, "main", read_service);

    let mut group = c.benchmark_group("get_block_range");
    for blocks in [1, 100, 1_000] {
        group.throughput(Throughput::Elements(u64::from(blocks)));
        for (direction, start, end) in [
            ("ascending", TIP - blocks, TIP - 1),
            ("descending", TIP - 1, TIP - blocks),
        ] {
            let range = proto::BlockRange {
                start: block_id(start),
                end: block_id(end),
                pool_types: Vec::new(),
            };
            group.bench_with_input(BenchmarkId::new(direction, blocks), &range, |b, range| {
                b.to_async(&runtime).iter(|| async {
                    let mut stream = service
                        .get_block_range(Request::new(range.clone()))
                        .await
                        .unwrap()
                        .into_inner();
                    while let Some(block) = stream.next().await {
                        block.unwrap();
                    }
                })
            });
        }
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().noise_threshold(0.05).sample_size(50);
    targets = get_block_range
);
criterion_main!(benches);
