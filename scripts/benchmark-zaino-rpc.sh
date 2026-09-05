#!/usr/bin/env bash
set -Eeuo pipefail

[[ $# -ge 1 && $# -le 2 ]] || {
    echo "usage: PERF=1 $0 WRITABLE_ZAKURA_CACHE [RUN_ROOT]" >&2
    exit 2
}

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
zaino_repo=$(realpath "$repo/../zaino")
zakura_repo=$(realpath "$repo/../zakura")
snapshot=$(realpath "$1")
run_root=${2:-"$repo/benchmark-runs/zaino-rpc"}
mkdir -p "$run_root"
run_root=$(realpath "$run_root")
run="$run_root/$(date -u +%Y%m%dT%H%M%SZ)-$$"
cookie_dir="$run/zakura-cookie"
zaino_index="$run/zaino-index"
zakura_rpc=${ZAKURA_RPC:-127.0.0.1:8232}
zaino_metrics=${ZAINO_METRICS:-127.0.0.1:19998}

[[ -d "$snapshot" ]] || { echo "Zakura cache is not a directory: $snapshot" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
python3 -c 'import tomllib'
session_cmd=()
if command -v setsid >/dev/null; then
    session_cmd=(setsid)
elif command -v python3 >/dev/null; then
    session_cmd=(python3 -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])')
else
    echo "setsid or python3 is required to run benchmark process in its own session" >&2
    exit 1
fi
if [[ ${PERF:-0} == 1 ]]; then
    command -v perf >/dev/null || { echo "perf is required when PERF=1" >&2; exit 1; }
fi

mkdir -p "$run" "$cookie_dir"
cat > "$run/zakura.toml" <<EOF
[network]
network = "Mainnet"

[rpc]
listen_addr = "$zakura_rpc"
cookie_dir = "$cookie_dir"
enable_cookie_auth = true

[state]
cache_dir = "$snapshot"
storage_mode = "archive"

[zcashd_compat]
enabled = false
EOF

cat > "$run/zainod.toml" <<EOF
backend = "rpc"
network = "Mainnet"
ephemeral_finalised_state = false
metrics_endpoint = "$zaino_metrics"

[grpc_settings]
listen_address = "127.0.0.1:8137"

[validator_settings]
validator_jsonrpc_listen_address = "$zakura_rpc"
validator_cookie_path = "$cookie_dir/.cookie"

[service]
timeout = 30
channel_size = 32

[storage.cache]
capacity = 10000
shard_power = 4

[storage.database]
path = "$zaino_index"
size = 128
sync_write_batch_size = 8
accumulator_rebuild_memory_size = 8
sync_checkpoint_interval = 120
EOF

echo "Building zakurad and zainod"
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build \
    --locked --manifest-path "$zakura_repo/Cargo.toml" --release -p zakura --bin zakurad \
    --no-default-features --features prometheus,indexer
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build \
    --locked --manifest-path "$zaino_repo/Cargo.toml" --release -p zainod --features prometheus
CARGO_PROFILE_RELEASE_DEBUG=1 python3 "$repo/scripts/benchmark-metadata.py" "$zakura_repo" "$run/zakura-provenance"
CARGO_PROFILE_RELEASE_DEBUG=1 python3 "$repo/scripts/benchmark-metadata.py" "$zaino_repo" "$run/zaino-provenance"

zakurad="$zakura_repo/target/release/zakurad"
zainod="$zaino_repo/target/release/zainod"

started_seconds=$SECONDS
{
    echo "started_utc=$(date -u --iso-8601=seconds)"
    echo "cache_state=${CACHE_STATE:-unspecified}"
    echo "snapshot_id=${SNAPSHOT_ID:-unspecified}"
    echo "timing=backend startup through indexing and shutdown; excludes builds"
} > "$run/metadata.txt"
"${session_cmd[@]}" "$zakurad" -c "$run/zakura.toml" start > "$run/zakurad.log" 2>&1 &
zakura_pid=$!
zaino_group=
zaino_pid=

cleanup() {
    [[ -n ${zaino_pid:-} ]] && kill -INT "$zaino_pid" 2>/dev/null || true
    if [[ -n ${zaino_group:-} ]]; then
        for _ in {1..30}; do
            kill -0 "$zaino_group" 2>/dev/null || break
            sleep 1
        done
        kill -TERM -- "-$zaino_group" 2>/dev/null || true
    fi
    kill -INT "$zakura_pid" 2>/dev/null || true
    wait "$zakura_pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo "Waiting for Zakura JSON-RPC"
rpc_ready=0
for _ in {1..1800}; do
    kill -0 "$zakura_pid" 2>/dev/null || { echo "zakurad exited; see $run/zakurad.log" >&2; exit 1; }
    if [[ -s "$cookie_dir/.cookie" ]] && curl -fsS --max-time 2 \
        --user "$(<"$cookie_dir/.cookie")" \
        --data-binary '{"jsonrpc":"2.0","id":1,"method":"getinfo","params":[]}' \
        -H 'content-type: application/json' "http://$zakura_rpc" >/dev/null; then
        rpc_ready=1
        break
    fi
    sleep 1
done
(( rpc_ready == 1 )) || { echo "Zakura RPC did not become ready" >&2; exit 1; }

command=("$zainod" start --config "$run/zainod.toml")
if [[ ${PERF:-0} == 1 ]]; then
    command=(perf record -F "${PERF_FREQ:-99}" -g --call-graph dwarf -o "$run/perf.data" -- "${command[@]}")
fi

{
    echo "index_started_utc=$(date -u --iso-8601=seconds)"
    echo "snapshot=$snapshot"
    echo "zaino_commit=$(git -C "$zaino_repo" rev-parse HEAD)"
    echo "zaino_dirty_files=$(git -C "$zaino_repo" status --porcelain | wc -l)"
    echo "zakura_commit=$(git -C "$zakura_repo" rev-parse HEAD)"
    echo "zakura_dirty_files=$(git -C "$zakura_repo" status --porcelain | wc -l)"
    printf 'command='; printf '%q ' "${command[@]}"; echo
} >> "$run/metadata.txt"

echo "Running Zaino RPC benchmark; artifacts: $run"
index_started_seconds=$SECONDS
"${session_cmd[@]}" "${command[@]}" > >(tee "$run/zainod.log") 2>&1 &
zaino_group=$!

for _ in {1..30}; do
    zaino_pid=$(pgrep -P "$zaino_group" -x zainod || true)
    [[ -n "$zaino_pid" ]] && break
    if [[ ${PERF:-0} != 1 ]]; then zaino_pid=$zaino_group; break; fi
    sleep 1
done
[[ -n "$zaino_pid" ]] || { echo "could not find zainod process" >&2; exit 1; }

echo "unix_time,metric,value" > "$run/metrics.csv"
reached_tip=0
while kill -0 "$zaino_group" 2>/dev/null; do
    now=$(date +%s.%N)
    scrape=$(curl -fsS --max-time 2 "http://$zaino_metrics/metrics" 2>/dev/null || true)
    awk -v now="$now" '/^zaino_/ && $1 !~ /^#/ { print now "," $1 "," $2 }' \
        <<< "$scrape" >> "$run/metrics.csv"
    if awk '
        $1 == "zaino_db_tip_height" { db = $2 }
        $1 == "zaino_sync_target_height" { target = $2 }
        END { exit !(target > 0 && db >= target) }
    ' <<< "$scrape"; then
        reached_tip=1
        echo "index_to_tip_seconds=$((SECONDS - index_started_seconds))" >> "$run/metadata.txt"
        awk '$1 == "zaino_db_tip_height" || $1 == "zaino_sync_target_height" { print $1 "=" $2 }' <<< "$scrape" >> "$run/metadata.txt"
        echo "reached_tip_utc=$(date -u --iso-8601=seconds)" >> "$run/metadata.txt"
        kill -INT "$zaino_pid"
        break
    fi
    if (( $(df --output=avail -B1 "$run" | tail -1) < 32 * 1024 * 1024 * 1024 )); then
        echo "stopping before free disk falls below 32 GiB" >&2
        kill -INT "$zaino_pid"
        break
    fi
    sleep 1
done

set +e
wait "$zaino_group"
status=$?
set -e
zaino_group=
zaino_pid=
kill -INT "$zakura_pid" 2>/dev/null || true
wait "$zakura_pid" 2>/dev/null || true
trap - INT TERM EXIT

echo "finished_utc=$(date -u --iso-8601=seconds)" >> "$run/metadata.txt"
echo "exit_status=$status" >> "$run/metadata.txt"
echo "wall_seconds=$((SECONDS - started_seconds))" > "$run/time.txt"

(( reached_tip == 1 )) || { echo "Zaino did not reach tip; see $run/zainod.log" >&2; exit 1; }
(( status == 0 )) || { echo "Zaino failed with status $status; see $run/zainod.log" >&2; exit "$status"; }
echo "Benchmark complete: $run"
