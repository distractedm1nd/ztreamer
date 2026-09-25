# September 2026 serving and indexing benchmarks

This dataset powers `site/benchmarks.html`. It is independent of the older
M3 Ultra backfill measurements on the homepage.

## Serving builds

- **ztreamer v0.1.0** is the display name for commit `86bf4b0` plus the
  16-active-range-stream fix. These measurements preceded the release tag;
  they were not rerun against that tag. The original CSV label is
  `86bf4b0-limit16`.
- **ztreamer v0.0.1**, commit `ec1bce059c74eae416979a8a623b521d0be44f82`:
  the first completed release repeat.
- **zaino**, commit `d27ec9303858d0216367f5469e37364a4f34ee2c`:
  direct backend, persistent index, `no_tls_with_prometheus` build.

All serving runs used the same native client and mainnet interval
419,200–3,478,843, with seed 20260829. Node versions ran sequentially.
Ztreamer used Zakura at tip 3,478,843. Zaino used Zebra at tip 3,479,843,
with its persistent index through 3,478,842. The benchmark upper-block hash
matches; tip-dependent RPCs observe different tips.

One repeat per version. RPCs use 100 requests; ranges use 1,000 requests for
100/1,000/10,000 blocks and 100 requests for 100,000 blocks. Concurrency uses
1/8/32/128 clients and 10 seconds issuing requests plus in-flight completion.
Wallet download covers 306 chunks and excludes scanning and trial decryption.
Warmups are retained in raw samples and excluded from summary statistics.

Hardware: Ryzen 9 9950X3D, 16 cores / 32 threads, 60.4 GiB OS-reported RAM,
WD Blue SN580 1TB NVMe SSD, ext4. Hardware was collected after the runs.

## Historical indexing builds (September 13)

Fresh empty-index runs use the actual release tags: v0.0.1
`ec1bce059c74eae416979a8a623b521d0be44f82`, followed by v0.1.0
`c40f3c31d8fb3f850c3fa90d9490b4df50aa88c4`. Both use the same preserved
Zakura snapshot, eight fetch workers, 256-block source segments, 256 MiB
pending budget, 16 MiB write batches and a 64 GiB map ceiling. Peers are
disabled. One run per version; OS page cache is not reset between runs.

Height and resource samples are collected every second. Time starts at process
launch and includes embedded Zakura startup. The exact pipeline duration comes
from the historical-completion log. Readiness is the first successful gRPC TCP
connection after that log and has up to approximately one second of polling
uncertainty. Process CPU, RSS and kernel I/O counters include embedded Zakura;
Zaino's separate Zebra process is excluded from its recorded resources and
startup time. The backing-node implementations and final indexed heights differ.

The height plots use committed heights. Legends give startup-to-readiness time,
including zaino's accumulator rebuild. CPU plots average ten-second windows.
Raw cumulative CPU seconds support alternative aggregation. Expected unavailable
height metrics during startup remain missing; raw scrape errors are retained.

## Files

- `summary.csv`: all 81 scenario summaries; missing values remain absent.
- `samples.csv`: request-level observations, including warmups and statuses.
- `height-time.csv`: zaino's clean historical-indexing attempt, sampled every
  second. `height` is the throttled built-height gauge; `db_tip_height` is
  committed progress. The finalization/accumulator period is included.
- `ztreamer-height-time.csv`: both tagged ztreamer runs, identified by `label`.
- `indexing-summary.csv`: phase times, final heights and sampled resource totals.
  Zaino's unavailable pipeline-only phase times remain blank.
- `indexing-metadata.json`: binary hashes, arguments, source identities and timing
  definitions for the new ztreamer runs.
- `metadata.json`: workload, measured source identities, and sync completion.
- `hardware.json`: hardware and collection provenance.
- SVG: generated charts used directly by the site. `-ztreamer` files exclude zaino and have
  independent scales. Other serving plots show all three builds.

Paths in public metadata use portable run labels rather than host-specific
source directories. The CSV measurement values are unchanged. Full node logs
and raw Prometheus scrapes are retained in the original local run archive;
they are not required to render this site.

## Regenerate

From the repository root, install `scripts/benchmark-report-requirements.txt`
in a Python virtual environment, then run:

```sh
python scripts/render-benchmarks.py
```

The script reads only this dataset and the site header. It regenerates the
plots and `site/benchmarks.html`; it does not start a node or run benchmarks.
The site itself needs no Python, JavaScript package install, or build step.
Serve or deploy the whole `site/` directory, including this asset directory.
