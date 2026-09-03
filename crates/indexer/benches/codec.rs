// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use zakura_chain::{block::Block, serialization::ZcashDeserialize as _};
use zakura_test::vectors::BLOCK_MAINNET_1687121_BYTES;
use ztreamer_indexer::{
    Digest,
    codec::{CompactBlockRecord, TreeSizes, decode_range_record, encode_range},
    index::RANGE_SIZE,
    parser::{CompactTransaction, RawIndexBlock, parse_block},
};

fn shielded_transactions() -> Vec<CompactTransaction> {
    let bytes = BLOCK_MAINNET_1687121_BYTES.to_vec();
    let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
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

fn hash(height: u32) -> Digest {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash
}

fn record(height: u32, transactions: Vec<CompactTransaction>) -> CompactBlockRecord {
    CompactBlockRecord {
        height,
        hash: hash(height),
        previous_hash: hash(height.wrapping_sub(1)),
        time: height,
        transactions,
        end_tree_sizes: TreeSizes::default(),
    }
}

fn block_codec(c: &mut Criterion) {
    let records = [
        ("empty", record(0, Vec::new())),
        ("shielded", record(0, shielded_transactions())),
    ];

    let mut group = c.benchmark_group("encode");
    for (name, record) in &records {
        group.throughput(Throughput::Bytes(record.encode().unwrap().len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), record, |b, record| {
            b.iter(|| record.encode().unwrap())
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode");
    for (name, record) in &records {
        let encoded = record.encode().unwrap();
        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &encoded, |b, encoded| {
            b.iter(|| CompactBlockRecord::decode(encoded).unwrap())
        });
    }
    group.finish();
}

fn range_codec(c: &mut Criterion) {
    let transactions = shielded_transactions();
    let records = (0..RANGE_SIZE)
        .map(|height| record(height, transactions.clone()))
        .collect::<Vec<_>>();
    let encoded = encode_range(&records).unwrap();

    let mut group = c.benchmark_group("encode_range");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function(format!("{RANGE_SIZE}-blocks"), |b| {
        b.iter(|| encode_range(&records).unwrap())
    });
    group.finish();

    c.bench_function("decode_range_record", |b| {
        b.iter(|| decode_range_record(&encoded, RANGE_SIZE as usize / 2).unwrap())
    });
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
    targets = block_codec, range_codec
}
criterion_main!(benches);
