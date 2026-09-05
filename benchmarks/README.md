# Benchmarks

Use the existing Criterion, Tonic, Tokio, and HdrHistogram integrations. All data created by the microbenchmarks is temporary. Historical runs require your own writable archive cache. Artifact scripts require Python 3.11+.

## Commands

```sh
just bench                                      # parser, codec, in-process service
just bench-grpc                                 # real loopback HTTP/2, 1/8/16/32 clients
just bench-index                                # builder, durable storage, reorg, restart
cargo bench --locked --bench parser --bench codec --bench serve --bench grpc --bench index -- --test

just load --clients 16 --seconds 30
just load --clients 32 --seconds 30 --delay-ms 1
just load --endpoint http://127.0.0.1:9067 --start 1000000 --end 1001999 --clients 16 --seconds 30
```

`just load` archives `results.json`, the exact command, logs, exit status, Cargo.lock, source revision/diff, dependency revisions, toolchain, and machine information under `benchmark-runs/grpc/`. Set `RUN_ROOT` to change that directory. For JSON on stdout without the archive wrapper:

```sh
cargo run --locked --release -p ztreamer-service --example grpc-load -- --clients 16 --seconds 30
```

Loopback benchmarks need permission to bind a local TCP port. They use an ephemeral port and shut down their server after completion.

## What each suite measures

| Target | Workload and timing boundary |
|---|---|
| `parser` | Seven historical/version fixtures plus synthetic inputs with 64/1,024 additional copies of a real transaction. Input preparation and transaction hashing are excluded; returned allocations are dropped in the timed loop. |
| `codec` | Empty, original shielded, and larger mixed records; uniform and mixed 1,000-block envelopes; first/middle/last random access. |
| `serve` | Existing uniform range workload in both directions; new mixed sealed, mutable, volatile, boundary-crossing, filtered, nullifier, and single-block reads. No network transport. |
| `grpc` | Full 2,000-block requests over plaintext loopback HTTP/2, including protobuf encoding/decoding, at 1/8/16/32 independent connections. Fast consumers and consumers pausing 1 ms per 64 blocks. |
| `index` | Ordered and reverse-arrival builders; mutable writes and sealed-range writes with normal LMDB durability; ordinary/deep suffix replacement; reopening a warm 2,200-block index with continuity verification. |

The shared `synthetic-mixed-v1` corpus combines shielded transaction fixtures from three eras. Every ten blocks contain five empty blocks, four small blocks, and one block with 32 copies of the combined transaction set. Heights, hashes, transaction indices/IDs, and cumulative tree sizes are deterministic. These are **synthetic stress inputs**, not consensus-valid chains or a measured mainnet distribution. The parser stress fixtures also deliberately do not represent consensus-valid blocks.

The original fixture/benchmark IDs remain for baseline continuity. New workloads have versioned IDs; change the version when changing their data or measurement boundaries. Parser blocks/s is `1e9 / time_ns`; its former duplicate `parse_block_rate` measurements were removed. Criterion already reports the byte rate.

The legacy range benchmark validates count/order once before timing and consumes every returned block through `black_box`. New range workloads and network clients validate count/order during each request, so truncated streams fail the run. Index write/reorg fixtures use Criterion's `iter_batched_ref` with per-iteration setup to exclude database creation, input construction, and directory cleanup. The write transaction and commit remain timed. Storage speed depends on the filesystem backing the temporary directory; set `TMPDIR` to the device you intend to measure.

## Reading the load report

Each wallet has a separate preconnected Tonic channel and one request outstanding at a time. Connections and data are warmed before the measured phase. All clients start together, run for the requested duration, and finish their final request. Throughput uses the actual elapsed duration, including that final drain. The local runtime has four worker threads; local client and server compete for those threads and CPUs.

HdrHistogram records request-to-first-decoded-block and request-to-end-of-stream, with p50/p95/p99/max in microseconds. Completion time includes configured consumer pauses. The report also contains successful request count, errors, blocks/s, and **protobuf payload** bytes/s (excluding transport headers). Requests have a configurable timeout; failures make the process exit unsuccessfully. Percentiles describe successful requests only—always inspect the error count and sample count alongside them.

This is a **closed-loop** load model. It measures a fixed number of active wallets; it does not estimate latency under a fixed external arrival rate or correct coordinated omission. Criterion's concurrent benchmark reports time per wave of requests, not per-request percentiles. Use the load report for those percentiles, and longer runs for enough p99 samples.

For steady-state indexing interference, target an existing node with `--endpoint` while it follows the chain. A reorg may intentionally terminate a stream: the load runner reports that as an error rather than silently retrying within the same sample. Standalone reorg cost is covered by `bench-index`. Network P2P negotiation/encryption, TLS, fixed-arrival-rate overload, cold large-database reads, full-chain restart, and pipeline worker sweeps are not simulated by the small local fixture.

## Comparisons and provenance

```sh
just bench -- --save-baseline base
# Change the implementation, preserving the workload and toolchain.
just bench -- --baseline base
```

Use the same flags for `bench-grpc` or `bench-index`. Run comparisons on an otherwise idle machine with the same toolchain, CPU settings, filesystem, and cache procedure. Repeat runs and alternate revision order when investigating a small change. Keep profiling runs separate from timing runs.

CI compares base and PR on the same hosted runner, reports new cases without baselines, and uploads Criterion data and provenance for 30 days. Results remain advisory. Storage benchmarks receive a smoke run in CI; collect their performance numbers on a known filesystem. Local provenance can also be captured with:

```sh
python3 scripts/benchmark-metadata.py . target/benchmark-provenance
```

Use clean commits for published results. A dirty run records its status and tracked-file patch; untracked source contents are not archived. Cargo.lock records the actual Git dependency commits, independent of sibling checkouts.

## Historical indexing

```sh
CACHE_STATE=warm SNAPSHOT_ID=mainnet-height-H-hash-HASH \
  FETCH_WORKERS=8 just snapshot /path/to/writable/cache /path/to/zakura.toml
```

`CACHE_STATE` and `SNAPSHOT_ID` are operator labels, not cache controls or assertions. The script updates the source cache in place. For comparable runs, use independent writable copies of the same archive snapshot, control network synchronization in the supplied configuration, and verify the actual target heights/hashes recorded in each `historical pipeline stage totals` log entry. Do not compare runs ending at different source tips.

The snapshot script saves the supplied configuration, locked dependencies, machine/toolchain metadata, process and disk metrics, stage timings, and optional `PERF=1` profiles. Its wall time includes embedded-node startup, synchronization waits, indexing, and shutdown; it excludes the build. The application's `historical compact index complete` log provides the indexing-only duration. `--index-only` exits before gRPC startup, so neither timing proves serving readiness.

The Zaino RPC script now also starts its wall timer before backend startup and saves a separate indexing-to-tip interval. Both scripts default to 99 Hz when profiling. This aligns broad boundaries, but a fair comparison still requires matching source state, hardware/cache conditions, revisions, indexing scope, and durability settings; the Zaino script profiles its indexer process while ztreamer embeds its backend.

To measure worker/batch sensitivity, repeat fresh-index runs with `FETCH_WORKERS`, `SOURCE_SEGMENT_BLOCKS`, `MAX_PENDING_BYTES`, and `MAX_BATCH_BYTES` varied one at a time. Run at least three repetitions per setting and retain every artifact directory. Define a reproducible warm-up procedure or explicit OS cache-reset procedure before labeling runs warm/cold; the scripts do not evict OS caches.
