# ztreamer

`ztreamer` is a heavily optimized zcash indexer and `lightwallet-protocol` implementation backed by an embedded [zakura](https://github.com/zakura-core/zakura) node.

`ztreamer` additionally adds a `CompactTxStreamer` server over v2 p2p, which enables p2p light wallets.

## Install

Install [Protocol Buffers](https://protobuf.dev/installation/), then:

```console
cargo install --git https://github.com/distractedm1nd/ztreamer ztreamerd
```

## Run

```console
ztreamerd --zakura-config zakura.toml
```

The gRPC listener is plaintext by default. To serve it over TLS, provide a PEM certificate chain and matching PEM private key together:

```console
ztreamerd --zakura-config zakura.toml \
  --grpc-listen 0.0.0.0:9067 \
  --tls-cert /etc/letsencrypt/live/ztreamer.example/fullchain.pem \
  --tls-key /etc/letsencrypt/live/ztreamer.example/privkey.pem
```

Certificate changes require a restart. The Prometheus listener is not covered by these options and should remain private or be secured by a reverse proxy.

## Protocol compatibility

 All `lightwallet-protocol` methods are implemented except `GetMempoolTx`. We intentionally deviate `lightwallet-protocol` for two other requests:

- `GetBlock` excludes transparent data, which current wallets do not request.
- `GetBlockRange` rejects transparent filters to avoid their bandwidth cost.

Ztreamer supports direct, in-process Zakura mode. It serves 24 of the 27 JSON-RPC requests provided by Zaino direct mode.

`getblockdeltas`, `getspentinfo`, and `gettxoutsetinfo` are not yet implemented.

## Historical indexing benchmark

| Metric | Result |
|---|---:|
| Genesis to serving | 92 s |
| Index rate | 37,848 blocks/s |
| Index size | 16 GiB |
| Peak Physical Footprint | 3.66 GiB |
| Total CPU seconds | 493 |


The benchmark indexed mainnet from genesis (to height 3,459,912) on an M3 Ultra with 512 GiB RAM and a warm cache. Serving throughput benchmarks will follow shortly.
