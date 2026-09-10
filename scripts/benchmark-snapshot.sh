#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    echo "usage: $0 WRITABLE_ZAKURA_CACHE ZAKURA_CONFIG [RUN_ROOT]" >&2
    echo "       PERF=1 FETCH_WORKERS=8 INDEX_MAP_SIZE=68719476736 $0 WRITABLE_ZAKURA_CACHE ZAKURA_CONFIG [RUN_ROOT]" >&2
    exit 2
}

[[ $# -ge 2 && $# -le 3 ]] || usage

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
snapshot=$(realpath "$1")
config=$(realpath "$2")
run_root=${3:-"$repo/benchmark-runs"}
mkdir -p "$run_root"
run_root=$(realpath "$run_root")
run="$run_root/$(date -u +%Y%m%dT%H%M%SZ)-$$"
index="$run/ztreamer-index"
metrics=${METRICS_LISTEN:-127.0.0.1:9999}
fetch_workers=${FETCH_WORKERS:-8}
source_segment_blocks=${SOURCE_SEGMENT_BLOCKS:-256}
index_map_size=${INDEX_MAP_SIZE:-68719476736}
max_pending_bytes=${MAX_PENDING_BYTES:-268435456}
max_batch_bytes=${MAX_BATCH_BYTES:-16777216}
platform=$(uname -s)

utc_now() {
    date -u +%Y-%m-%dT%H:%M:%SZ
}

unix_now() {
    local now
    now=$(date +%s.%N 2>/dev/null || true)
    if [[ -z $now || $now == *N* ]]; then
        python3 -c 'import time; print(f"{time.time():.9f}")'
    else
        printf '%s\n' "$now"
    fi
}

logical_cpus() {
    if command -v nproc >/dev/null; then
        nproc
    elif [[ $platform == Darwin ]]; then
        sysctl -n hw.logicalcpu
    else
        getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown
    fi
}

kernel_info() {
    uname -srmo 2>/dev/null || uname -a
}

configure_macos_cxx_headers() {
    [[ $platform == Darwin ]] || return 0
    local sdk cxx_headers
    sdk=$(xcrun --show-sdk-path 2>/dev/null || true)
    cxx_headers="$sdk/usr/include/c++/v1"
    if [[ -d $cxx_headers ]]; then
        export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH:+$CPLUS_INCLUDE_PATH:}$cxx_headers"
    fi
}

[[ -d "$snapshot" ]] || { echo "snapshot cache is not a directory: $snapshot" >&2; exit 1; }
[[ -f "$config" ]] || { echo "Zakura config does not exist: $config" >&2; exit 1; }
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

mkdir -p "$run"
echo "Using Zakura state in place at $snapshot (Zakura will update it)"

echo "Building ztreamerd"
configure_macos_cxx_headers
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --locked --manifest-path "$repo/Cargo.toml" --release -p ztreamerd
cp "$config" "$run/zakura-input.toml"
CARGO_PROFILE_RELEASE_DEBUG=1 python3 "$repo/scripts/benchmark-metadata.py" "$repo" "$run/provenance"

command=(
    "$repo/target/release/ztreamerd"
    --zakura-config "$config"
    --index-dir "$index"
    --index-map-size "$index_map_size"
    --fetch-workers "$fetch_workers"
    --source-segment-blocks "$source_segment_blocks"
    --max-pending-bytes "$max_pending_bytes"
    --max-batch-bytes "$max_batch_bytes"
    --metrics-listen "$metrics"
    --index-only
)
if [[ ${PERF:-0} == 1 ]]; then
    command=(perf record -F "${PERF_FREQ:-99}" -g --call-graph dwarf -o "$run/perf.data" -- "${command[@]}")
fi
timer=()
if [[ -x /usr/bin/time ]] && /usr/bin/time -v true >/dev/null 2>&1; then
    timer=(/usr/bin/time -v -o "$run/time.txt")
elif [[ -x /usr/bin/time ]] && /usr/bin/time -lp true >/dev/null 2>&1; then
    timer=(/usr/bin/time -lp)
fi

{
    echo "started_utc=$(utc_now)"
    echo "snapshot=$snapshot"
    echo "config=$config"
    echo "ztreamer_commit=$(git -C "$repo" rev-parse HEAD)"
    echo "ztreamer_dirty_files=$(git -C "$repo" status --porcelain | wc -l)"
    echo "cache_state=${CACHE_STATE:-unspecified}"
    echo "snapshot_id=${SNAPSHOT_ID:-unspecified}"
    echo "timing=process startup through historical indexing and shutdown; excludes build; does not start gRPC"
    echo "logical_cpus=$(logical_cpus)"
    echo "kernel=$(kernel_info)"
    printf 'command='; printf '%q ' "${command[@]}"; echo
} > "$run/metadata.txt"

echo "Running benchmark; artifacts: $run"
started_seconds=$SECONDS
set +e
ZAKURA_STATE__CACHE_DIR="$snapshot" \
ZAKURA_STATE__EPHEMERAL=false \
ZAKURA_STATE__STORAGE_MODE=archive \
"${session_cmd[@]}" "${timer[@]}" "${command[@]}" > >(tee "$run/ztreamerd.log") 2>&1 &
pid=$!
if [[ $platform == Darwin ]]; then
    (
        echo "unix_time vm_stat"
        while kill -0 "$pid" 2>/dev/null; do
            echo "# $(unix_now)"
            vm_stat 2>/dev/null || true
            sleep 1
        done
    ) > "$run/vmstat.txt" &
    vmstat_pid=$!
    (
        echo "macOS iostat samples; unix_time lines precede each sample"
        while kill -0 "$pid" 2>/dev/null; do
            echo "# $(unix_now)"
            iostat -d -w 1 -c 2 2>/dev/null || true
        done
    ) > "$run/diskstats.txt" &
    diskstats_pid=$!
    (
        echo "unix_time,pid,vmsize_kb,vmrss_kb,utime,time,state"
        while kill -0 "$pid" 2>/dev/null; do
            process=$(pgrep -g "$pid" -x ztreamerd || true)
            if [[ -n $process ]]; then
                ps -p "$process" -o pid=,vsz=,rss=,utime=,time=,state= 2>/dev/null |
                    awk -v now="$(unix_now)" '{$1=$1; gsub(/[[:space:]]+/, ","); print now "," $0; fflush();}'
            fi
            sleep 1
        done
    ) > "$run/processstats.csv" &
    processstats_pid=$!
else
    vmstat -n 1 > "$run/vmstat.txt" &
    vmstat_pid=$!
    device=$(findmnt -no SOURCE --target "$snapshot")
    parent_device=$(lsblk -no PKNAME "$device" 2>/dev/null | head -1)
    device=${parent_device:-${device##*/}}
    (
        echo "unix_time stats"
        while kill -0 "$pid" 2>/dev/null; do
            printf '%s ' "$(unix_now)"
            cat "/sys/class/block/$device/stat" 2>/dev/null || true
            sleep 1
        done
    ) > "$run/diskstats.txt" &
    diskstats_pid=$!
    (
        echo "unix_time,pid,rchar,wchar,read_bytes,write_bytes,minflt,majflt,utime,stime,vmsize_kb,vmrss_kb,vmswap_kb,threads"
        while kill -0 "$pid" 2>/dev/null; do
            process=$(pgrep -g "$pid" -x ztreamerd || true)
            if [[ -n $process && -r /proc/$process/io ]]; then
                read -r rchar wchar read_bytes write_bytes < <(
                    awk '/^(rchar|wchar|read_bytes|write_bytes):/ { printf "%s ", $2 }' "/proc/$process/io"
                )
                read -r minflt majflt utime stime < <(
                    awk '{ print $10, $12, $14, $15 }' "/proc/$process/stat"
                )
                read -r vmsize vmrss vmswap threads < <(
                    awk '/^(VmSize|VmRSS|VmSwap|Threads):/ { printf "%s ", $2 }' "/proc/$process/status"
                )
                printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
                    "$(unix_now)" "$process" "$rchar" "$wchar" "$read_bytes" "$write_bytes" \
                    "$minflt" "$majflt" "$utime" "$stime" "$vmsize" "$vmrss" "$vmswap" "$threads"
            fi
            sleep 1
        done
    ) > "$run/processstats.csv" &
    processstats_pid=$!
fi
set -e

cleanup() {
    kill "$vmstat_pid" "$diskstats_pid" "$processstats_pid" 2>/dev/null || true
    kill -INT -- "-$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo "unix_time,metric,value" > "$run/metrics.csv"
while kill -0 "$pid" 2>/dev/null; do
    now=$(unix_now)
    curl -fsS --max-time 2 "http://$metrics/metrics" 2>/dev/null |
        awk -v now="$now" '
            /^(ztreamer_index_|state_finalized_block_height|sync_estimated_)/ && $1 !~ /^#/ {
                print now "," $1 "," $2
            }
        ' >> "$run/metrics.csv" || true
    sleep 1
done

set +e
wait "$pid"
status=$?
set -e
kill "$vmstat_pid" "$diskstats_pid" "$processstats_pid" 2>/dev/null || true
wait "$vmstat_pid" "$diskstats_pid" "$processstats_pid" 2>/dev/null || true
trap - INT TERM EXIT
echo "finished_utc=$(utc_now)" >> "$run/metadata.txt"
echo "exit_status=$status" >> "$run/metadata.txt"
echo "wall_seconds=$((SECONDS - started_seconds))" >> "$run/time.txt"

if (( status != 0 )); then
    echo "Benchmark failed with status $status; see $run/ztreamerd.log" >&2
    exit "$status"
fi

echo "Benchmark complete: $run"
