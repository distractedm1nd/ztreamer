# Serving optimization: stop rebuilding responses

Ordinary durable block requests now reuse a stored protobuf response. The final load tests deliver about **2.3× the fast-client throughput** of baseline; removing the lifetime-wide stream gate also improves slow-client latency substantially.

## The diff

```diff
 read a chunk from LMDB
-  deserialize Bincode into owned records
+  copy stored protobuf payloads into owned buffers
 close the LMDB transaction
 check chain continuity
-allocate protobuf transactions and byte fields
-walk fields to calculate sizes and encode
+copy the prepared payload through Prost/Tonic
 send
-hold a global reader permit during network waits
+let each stream progress under transport backpressure
```

Generated Tonic server bindings accept a pre-encoded Prost message wrapper; Prost handles protobuf encoding and decoding. Filtered/nullifier requests decode once and project the protobuf object in place. Index consumers that require the original record type decode through Prost `Bytes` fields sharing one buffer. The original protobuf API remains available; production gRPC and P2P use the encoded response path.

Each stream retains at most one prepared 64-record chunk, plus transport buffers. LMDB transactions finish before network waits. Readiness, ascending/descending order, generation handling, and rejection at an unchainable reorg retain the shared traversal and checks.

## Measurements

Baseline: local commit `541999709feceb1c9bfd2c0a49acc881fb031828`, named Criterion baseline `serving-5419997`. Synthetic mixed-v1 data, AMD Ryzen 9 9950X3D, warm small indexes; loopback client and server share four Tokio workers. Temporary LMDB fixtures use `/tmp` (tmpfs). These are not mainnet or NVMe throughput claims.

Criterion, 2,000 blocks per client; elapsed time is per complete wave:

| Clients | Baseline blocks/s | Updated blocks/s | Speedup |
|---:|---:|---:|---:|
| 1 | 55,116 | 117,796 | 2.14× |
| 8 | 183,928 | 455,766 | 2.48× |
| 16 | 206,103 | 495,601 | 2.40× |
| 32 | 211,061 | 497,499 | 2.36× |

Separate unprofiled 10-second loads on the final binary, compared with control runs of the **original, SHA-256-verified baseline binary**. All runs completed with zero errors. Slow consumers pause 10 ms after each 64 blocks.

| Workload | Blocks/s, before → after | First-block p95, ms | Complete-range p95, ms | Peak RSS, MiB |
|---|---:|---:|---:|---:|
| 32 fast clients | 218,682 → 504,432 | 172.4 → 37.5 | 325.4 → 155.5 | 137 → 159 |
| 32 slow clients | 115,722 → 175,353 | 196.0 → 3.7 | 560.6 → 370.7 | 183 → 193 |
| 64 fast clients | 217,890 → 499,408 | 469.5 → 78.7 | 623.6 → 329.5 | 150 → 211 |
| 64 slow clients | 114,250 → 347,264 | 754.2 → 5.2 | 1123.3 → 374.8 | 197 → 313 |

RSS covers the **combined client/server process**, including fixture setup and warm-up. More streams now progress concurrently, so memory scales with active streams. Relative to the wire-ready candidate that still had the gate, removing it left 32-fast-client throughput approximately unchanged (507k → 504k), improved slow first-block p95 (182 → 3.7 ms), and raised slow-workload RSS (176 → 193 MiB). Fast completion p95 rose from 144 to 156 ms in these samples.

Other comparisons, using elapsed time rather than format-dependent byte rates:

| Case | Baseline | Updated | Time change |
|---|---:|---:|---:|
| Heavy record decode | 46.86 µs | 31.50 µs | -32.8% |
| 100-block decoded service range | 830.94 µs | 539.18 µs | -35.1% |
| 100-block nullifier range | 658.44 µs | 537.61 µs | -18.4% |
| Write 100 mutable blocks | 641.11 µs | 471.99 µs | -26.4% |
| Write 1,000 sealed blocks | 7.52 ms | 5.55 ms | -26.1% |
| Replace 1,200 blocks in a deep reorg | 56.71 ms | 46.51 ms | -18.0% |
| Reopen 2,200-block index | 1260.15 µs | 906.98 µs | -28.0% |

The 2,210-record corpus occupies 10,070,970 encoded bytes before and 9,761,007 after (**3.08% smaller**). Empty records grow (93 → 143–147 bytes for the first five); the heavy sample shrinks (39,773 → 38,038). This measures encoded values, not LMDB file size or a mainnet distribution.

## Compatibility and operation

New v2 records contain a fixed height/hash/previous-hash envelope and one protobuf response, replacing the Bincode payload. V1 remains readable. Caller-created transactions without shielded payload retain v1 to preserve the differing full/nullifier RPC behavior. Normal parser records use v2.

Existing history benefits after conversion. Stop the daemon, retain a backup if you need older binaries, and run:

```sh
ztreamerd --zakura-config zakura.toml --index-dir ztreamer-index --upgrade-index
```

This commits one range/row at a time, can resume, and preserves chain state and generation. Allow free space for LMDB copy-on-write pages. Older binaries cannot read v2. No existing user index or Zakura snapshot was converted during this work.

## Verification and artifacts

- **50 workspace tests passed**, including real gRPC equivalence, all shielded pool filters, nullifiers, unary hash/height lookup, boundary directions, reorg interruption/continuation, mixed-format upgrade/reopen/idempotence, record bounds/truncation, and cancellation. Clippy with warnings denied and formatting checks passed.
- An independent old/new audit produced identical bytes for 258 responses across 24 full/filtered/nullifier ranges: SHA-256 `8dd18b74e0a226742e5002aa17ee1d12a731bd2aae3e4981516f3ece9d0f8287`.
- **49 Criterion comparisons** retain normal sampling. The 19 codec/index cases were rerun after shared-buffer decoding; all 30 serving cases were rerun after removing the stream gate. Original baseline measurements were preserved; seven index cases were added from a detached checkout of the baseline commit.

Baseline artifacts: `benchmark-runs/serving-5419997/`. Updated results, raw samples, unprofiled loads, resource usage, CPU profiles, exact commands, source archives, and intermediate candidates: `benchmark-runs/serving-wire-v2/`. The full [Markdown diff](../benchmark-runs/serving-wire-v2/diff.md) is saved there.

```sh
cargo bench --locked --bench codec --bench index --bench serve --bench grpc -- --baseline serving-5419997 --noplot
cargo run --locked --release -p ztreamer-service --example grpc-load -- --clients 32 --seconds 10 --delay-ms 10
```

The final 16-client profile contains 7,974 cycle samples with zero lost samples and no named Bincode deserialization samples (baseline: 26.91% self-cycle share). Allocator and buffer-handling symbols remain prominent. The saved profiles combine client and server work; their relative sample shares are not isolated server CPU costs. Cold/large-index reads and concurrent historical ingestion remain unmeasured. Range reads are still synchronous inside stream polling; use the available pruned Zakura snapshot to establish retained coverage and test page-fault behavior before adding blocking tasks or prefetch.
