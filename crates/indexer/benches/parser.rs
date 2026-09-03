// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use zakura_chain::{block::Block, serialization::ZcashDeserialize as _};
use zakura_test::vectors::{
    BLOCK_MAINNET_396_BYTES, BLOCK_MAINNET_347500_BYTES, BLOCK_MAINNET_419200_BYTES,
    BLOCK_MAINNET_949496_BYTES, BLOCK_MAINNET_1687106_BYTES, BLOCK_MAINNET_1687121_BYTES,
    BLOCK_TESTNET_1842421_BYTES,
};
use ztreamer_indexer::parser::{RawIndexBlock, parse_block};

fn corpus() -> Vec<(&'static str, RawIndexBlock)> {
    [
        ("sprout-joinsplit", &*BLOCK_MAINNET_396_BYTES),
        ("overwinter", &*BLOCK_MAINNET_347500_BYTES),
        ("sapling", &*BLOCK_MAINNET_419200_BYTES),
        ("shielded-coinbase", &*BLOCK_MAINNET_949496_BYTES),
        ("nu5-v5", &*BLOCK_MAINNET_1687106_BYTES),
        ("orchard", &*BLOCK_MAINNET_1687121_BYTES),
        ("v6-ironwood", &*BLOCK_TESTNET_1842421_BYTES),
    ]
    .into_iter()
    .map(|(name, encoded_block)| {
        let bytes = encoded_block.to_vec();
        let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        let raw = RawIndexBlock {
            height: block.coinbase_height().unwrap(),
            hash: block.hash(),
            bytes,
            txids: block
                .transactions
                .iter()
                .map(|transaction| transaction.hash())
                .collect(),
        };
        (name, raw)
    })
    .collect()
}

fn parse_block_bytes(c: &mut Criterion) {
    let blocks = corpus();
    let mut group = c.benchmark_group("parse_block");
    for (name, raw) in &blocks {
        group.throughput(Throughput::Bytes(raw.bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), raw, |b, raw| {
            b.iter(|| parse_block(raw).unwrap())
        });
    }
    group.finish();
}

fn parse_block_rate(c: &mut Criterion) {
    let blocks = corpus();
    let mut group = c.benchmark_group("parse_block_rate");
    group.throughput(Throughput::Elements(1));
    for (name, raw) in &blocks {
        group.bench_with_input(BenchmarkId::from_parameter(name), raw, |b, raw| {
            b.iter(|| parse_block(raw).unwrap())
        });
    }
    group.finish();
}

fn criterion_config() -> Criterion {
    let mut criterion = Criterion::default().noise_threshold(0.05).sample_size(50);
    if std::env::var_os("CI").is_some() {
        criterion = criterion
            .warm_up_time(Duration::from_millis(300))
            .measurement_time(Duration::from_secs(1))
            .sample_size(10);
    }
    criterion
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = parse_block_bytes, parse_block_rate
}
criterion_main!(benches);
