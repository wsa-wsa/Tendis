#!/bin/bash
# ============================================================================
# CaaS-LSM Experiment Common Functions
# 论文: "支持远程后台任务的分布式存储系统设计与实现"
# ============================================================================

set -euo pipefail

# ============================================================================
# Global Configuration
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"

# Binary paths
TENDISPLUS_BIN="${BUILD_DIR}/bin/tendisplus"
CONTROL_PLANE_BIN="${BUILD_DIR}/bin/control_plane_server"
CSA_SERVER_BIN="${BUILD_DIR}/bin/csa_server"
OBSERVATORY_BIN="${BUILD_DIR}/bin/observatory_server"
REDIS_CLI="${PROJECT_ROOT}/bin/redis-cli"
MEMTIER="${PROJECT_ROOT}/bin/memtier_benchmark"
REDIS_BENCHMARK="${PROJECT_ROOT}/bin/redis-benchmark"

# Default experiment parameters
EXPERIMENT_DIR="${SCRIPT_DIR}/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR=""
DATA_DIR=""
RESULT_DIR=""

# Process tracking
declare -a PIDS=()

# Default password for TendisPlus instances
DEFAULT_PASSWORD="test_password"

# ============================================================================
# Color Output
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info()    { echo -e "${GREEN}[INFO]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_error()   { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_section() { echo -e "\n${BLUE}========== $* ==========${NC}\n" >&2; }
log_metric()  { echo -e "${CYAN}[METRIC]${NC} $*" >&2; }

# ============================================================================
# Environment Setup
# ============================================================================
init_experiment() {
    local experiment_name="$1"
    RESULT_DIR="${EXPERIMENT_DIR}/${experiment_name}_${TIMESTAMP}"
    LOG_DIR="${RESULT_DIR}/logs"
    DATA_DIR="${RESULT_DIR}/data"

    mkdir -p "${RESULT_DIR}" "${LOG_DIR}" "${DATA_DIR}"

    log_section "Experiment: ${experiment_name}"
    log_info "Result directory: ${RESULT_DIR}"
    log_info "Timestamp: ${TIMESTAMP}"

    # Record experiment metadata
    cat > "${RESULT_DIR}/metadata.json" <<EOF
{
    "experiment": "${experiment_name}",
    "timestamp": "${TIMESTAMP}",
    "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "hostname": "$(hostname)",
    "os": "$(uname -s) $(uname -r)",
    "cpu_cores": $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 0),
    "memory_gb": $(( $(sysctl -n hw.memsize 2>/dev/null || free -b 2>/dev/null | awk '/Mem:/{print $2}' || echo 0) / 1073741824 ))
}
EOF
}

check_binaries() {
    local missing=0
    for bin in "$@"; do
        if [[ ! -x "$bin" ]]; then
            log_error "Binary not found: $bin"
            missing=1
        fi
    done
    if [[ $missing -eq 1 ]]; then
        log_error "Please build the project first: cd ${BUILD_DIR} && make -j\$(nproc)"
        exit 1
    fi
}

# ============================================================================
# TendisPlus Configuration Generator
# ============================================================================
generate_tendisplus_conf() {
    local conf_file="$1"
    local port="$2"
    local db_dir="$3"
    local log_file="$4"
    local password="${5:-test_password}"
    local control_plane_addr="${6:-}"
    local shared_fs_uri="${7:-}"

    cat > "${conf_file}" <<EOF
bind 0.0.0.0
port ${port}
loglevel notice
logdir ${log_file}
dir ${db_dir}
requirepass ${password}
masterauth ${password}

# Storage settings
rocks.blockcacheMB 256
rocks.write_buffer_size 67108864
rocks.max_write_buffer_number 4
rocks.level0_file_num_compaction_trigger 4
rocks.level0_slowdown_writes_trigger 20
rocks.level0_stop_writes_trigger 36
rocks.target_file_size_base 67108864
rocks.max_bytes_for_level_base 268435456

# Compaction settings
rocks.compactOnExpiredKeysEnabled false
EOF

    # CaaS-LSM mode
    if [[ -n "${control_plane_addr}" ]]; then
        cat >> "${conf_file}" <<EOF

# CaaS-LSM Remote Compaction
control_plane_address ${control_plane_addr}
remote_compaction.shared_fs_uri ${shared_fs_uri}
EOF
    fi
}

# ============================================================================
# Process Management
# ============================================================================
start_control_plane() {
    local listen_addr="${1:-0.0.0.0:50051}"
    local log_file="${LOG_DIR}/control_plane.log"

    log_info "Starting Control Plane on ${listen_addr}..."
    ${CONTROL_PLANE_BIN} -l "${listen_addr}" > "${log_file}" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    sleep 2

    if kill -0 $pid 2>/dev/null; then
        log_info "Control Plane started (PID: $pid)"
    else
        log_error "Control Plane failed to start. Check ${log_file}"
        return 1
    fi
    echo $pid
}

start_csa_worker() {
    local worker_id="$1"
    local listen_port="$2"
    local control_plane_addr="$3"
    local shared_fs_uri="${4:-}"
    local log_file="${LOG_DIR}/csa_worker_${worker_id}.log"

    log_info "Starting CSA Worker ${worker_id} on port ${listen_port}..."
    ${CSA_SERVER_BIN} \
        -l "0.0.0.0:${listen_port}" \
        -c "${control_plane_addr}" \
        -i "worker-${worker_id}" \
        ${shared_fs_uri:+-s "${shared_fs_uri}"} \
        > "${log_file}" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    sleep 1

    if kill -0 $pid 2>/dev/null; then
        log_info "CSA Worker ${worker_id} started (PID: $pid)"
    else
        log_error "CSA Worker ${worker_id} failed to start. Check ${log_file}"
        return 1
    fi
    echo $pid
}

start_tendisplus() {
    local instance_name="$1"
    local conf_file="$2"
    local log_file="${LOG_DIR}/tendisplus_${instance_name}.log"

    log_info "Starting TendisPlus instance ${instance_name}..."
    ${TENDISPLUS_BIN} "${conf_file}" > "${log_file}" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    sleep 3

    if kill -0 $pid 2>/dev/null; then
        log_info "TendisPlus ${instance_name} started (PID: $pid)"
    else
        log_error "TendisPlus ${instance_name} failed to start. Check ${log_file}"
        return 1
    fi
    echo $pid
}

start_observatory() {
    local control_plane_addr="${1:-localhost:50051}"
    local http_port="${2:-8080}"
    local log_file="${LOG_DIR}/observatory.log"

    log_info "Starting Observatory on port ${http_port}..."
    ${OBSERVATORY_BIN} -c "${control_plane_addr}" -p "${http_port}" > "${log_file}" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    sleep 1

    if kill -0 $pid 2>/dev/null; then
        log_info "Observatory started (PID: $pid, HTTP: ${http_port})"
    else
        log_error "Observatory failed to start. Check ${log_file}"
        return 1
    fi
    echo $pid
}

# ============================================================================
# Workload Generation
# ============================================================================
run_memtier_benchmark() {
    local name="$1"
    local host="$2"
    local port="$3"
    local password="$4"
    local duration="$5"   # seconds
    local clients="$6"
    local threads="$7"
    local pipeline="${8:-1}"
    local ratio="${9:-1:1}"   # set:get ratio
    local data_size="${10:-128}"
    local output_file="${DATA_DIR}/memtier_${name}.txt"

    log_info "Running memtier benchmark: ${name} (${duration}s, ${clients}c, ${threads}t, ratio=${ratio})"
    ${MEMTIER} \
        -h "${host}" -p "${port}" -a "${password}" \
        --test-time="${duration}" \
        --clients="${clients}" \
        --threads="${threads}" \
        --pipeline="${pipeline}" \
        --ratio="${ratio}" \
        --data-size="${data_size}" \
        --key-minimum=1 --key-maximum=100000000 \
        --distinct-client-seed --randomize \
        --hide-histogram \
        --json-out-file="${DATA_DIR}/memtier_${name}.json" \
        2>&1 | tee "${output_file}"
}

run_redis_benchmark() {
    local name="$1"
    local host="$2"
    local port="$3"
    local password="$4"
    local requests="$5"
    local clients="$6"
    local data_size="${7:-128}"
    local tests="${8:-set,get}"
    local output_file="${DATA_DIR}/redis_bench_${name}.txt"

    log_info "Running redis-benchmark: ${name} (${requests} reqs, ${clients}c)"
    ${REDIS_BENCHMARK} \
        -h "${host}" -p "${port}" -a "${password}" \
        -n "${requests}" -c "${clients}" \
        -d "${data_size}" \
        -t "${tests}" \
        --csv \
        2>&1 | tee "${output_file}"
}

# Pre-populate data for experiments
populate_data() {
    local host="$1"
    local port="$2"
    local password="$3"
    local num_keys="$4"
    local data_size="${5:-128}"

    log_info "Populating ${num_keys} keys (${data_size}B each)..."
    ${MEMTIER} \
        -h "${host}" -p "${port}" -a "${password}" \
        --key-minimum=1 --key-maximum="${num_keys}" \
        --data-size="${data_size}" \
        --ratio=1:0 \
        --clients=50 --threads=4 --pipeline=20 \
        --requests="${num_keys}" \
        --key-pattern=P:P \
        --hide-histogram \
        2>&1 | tail -5
    log_info "Data population complete."
}

# ============================================================================
# Metrics Collection
# ============================================================================
collect_system_metrics() {
    local output_file="$1"
    local duration="$2"
    local interval="${3:-1}"

    log_info "Collecting system metrics for ${duration}s (interval: ${interval}s)..."
    (
        echo "timestamp,cpu_user,cpu_sys,cpu_idle,mem_used_mb,mem_free_mb,disk_read_kb,disk_write_kb"
        local end_time=$(($(date +%s) + duration))
        while [[ $(date +%s) -lt $end_time ]]; do
            local ts=$(date +%s)
            # CPU usage (macOS compatible)
            local cpu_info
            if [[ "$(uname)" == "Darwin" ]]; then
                cpu_info=$(top -l 1 -n 0 2>/dev/null | grep "CPU usage" | head -1 || echo "0 0 0")
                local cpu_user=$(echo "$cpu_info" | awk '{print $3}' | tr -d '%')
                local cpu_sys=$(echo "$cpu_info" | awk '{print $5}' | tr -d '%')
                local cpu_idle=$(echo "$cpu_info" | awk '{print $7}' | tr -d '%')
                local mem_info=$(vm_stat 2>/dev/null | head -10)
                local page_size=$(sysctl -n hw.pagesize 2>/dev/null || echo 4096)
                local mem_free=$(echo "$mem_info" | awk '/Pages free/{gsub(/[^0-9]/,""); print}')
                local mem_free_mb=$(( (${mem_free:-0} * page_size) / 1048576 ))
                local total_mem=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1048576 ))
                local mem_used_mb=$(( total_mem - mem_free_mb ))
            else
                cpu_info=$(mpstat 1 1 2>/dev/null | tail -1 || echo "0 0 0 0 0 0 0 0 0 0 0 100")
                local cpu_user=$(echo "$cpu_info" | awk '{print $3}')
                local cpu_sys=$(echo "$cpu_info" | awk '{print $5}')
                local cpu_idle=$(echo "$cpu_info" | awk '{print $12}')
                local mem_info=$(free -m 2>/dev/null | grep Mem)
                local mem_used_mb=$(echo "$mem_info" | awk '{print $3}')
                local mem_free_mb=$(echo "$mem_info" | awk '{print $4}')
            fi
            echo "${ts},${cpu_user:-0},${cpu_sys:-0},${cpu_idle:-100},${mem_used_mb:-0},${mem_free_mb:-0},0,0"
            sleep "${interval}"
        done
    ) > "${output_file}" &
    echo $!
}

collect_observatory_metrics() {
    local observatory_url="$1"
    local output_file="$2"
    local duration="$3"
    local interval="${4:-2}"

    log_info "Collecting Observatory metrics for ${duration}s..."
    (
        local end_time=$(($(date +%s) + duration))
        echo "timestamp,workers_online,tasks_pending,tasks_running,tasks_completed,tasks_failed,p50_ms,p95_ms,p99_ms"
        while [[ $(date +%s) -lt $end_time ]]; do
            local ts=$(date +%s)
            local metrics
            metrics=$(curl -s "${observatory_url}/api/metrics" 2>/dev/null || echo "{}")
            local workers=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('workers_online',0))" 2>/dev/null || echo 0)
            local pending=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('tasks_pending',0))" 2>/dev/null || echo 0)
            local running=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('tasks_running',0))" 2>/dev/null || echo 0)
            local completed=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('tasks_completed',0))" 2>/dev/null || echo 0)
            local failed=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('tasks_failed',0))" 2>/dev/null || echo 0)
            local p50=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('p50_latency_ms',0))" 2>/dev/null || echo 0)
            local p95=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('p95_latency_ms',0))" 2>/dev/null || echo 0)
            local p99=$(echo "$metrics" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('p99_latency_ms',0))" 2>/dev/null || echo 0)
            echo "${ts},${workers},${pending},${running},${completed},${failed},${p50},${p95},${p99}"
            sleep "${interval}"
        done
    ) > "${output_file}" &
    echo $!
}

collect_prometheus_metrics() {
    local observatory_url="$1"
    local output_file="$2"

    log_info "Collecting Prometheus metrics snapshot..."
    curl -s "${observatory_url}/api/prometheus/metrics" > "${output_file}" 2>/dev/null || true
}

# ============================================================================
# Result Analysis
# ============================================================================
extract_memtier_summary() {
    local json_file="$1"
    local output_file="$2"

    if [[ ! -f "$json_file" ]]; then
        log_warn "Memtier result not found: $json_file"
        return
    fi

    python3 -c "
import json, sys
with open('${json_file}') as f:
    data = json.load(f)
totals = data.get('ALL STATS', {}).get('Totals', {})
ops = totals.get('Ops/sec', 0)
avg_lat = totals.get('Latency', 0)
p50 = totals.get('p50 Latency', 0)
p99 = totals.get('p99 Latency', 0)
hits = totals.get('Hits/sec', 0)
misses = totals.get('Misses/sec', 0)
kb = totals.get('KB/sec', 0)
print(json.dumps({
    'ops_per_sec': round(ops, 2),
    'avg_latency_ms': round(avg_lat, 3),
    'p50_latency_ms': round(p50, 3),
    'p99_latency_ms': round(p99, 3),
    'hits_per_sec': round(hits, 2),
    'misses_per_sec': round(misses, 2),
    'throughput_kb_sec': round(kb, 2)
}, indent=2))
" > "${output_file}" 2>/dev/null || log_warn "Failed to extract memtier summary"
}

generate_comparison_report() {
    local result_dir="$1"
    local report_file="${result_dir}/report.txt"

    log_section "Generating Comparison Report"

    {
        echo "============================================================"
        echo "CaaS-LSM Experiment Report"
        echo "Generated: $(date)"
        echo "Result Dir: ${result_dir}"
        echo "============================================================"
        echo ""

        # List all collected data files
        echo "--- Collected Data Files ---"
        find "${result_dir}" -name "*.json" -o -name "*.csv" -o -name "*.txt" | sort

        echo ""
        echo "--- Summary ---"

        # Extract key metrics from memtier results
        for json_file in "${result_dir}"/data/memtier_*.json; do
            if [[ -f "$json_file" ]]; then
                local name=$(basename "$json_file" .json | sed 's/memtier_//')
                echo ""
                echo "Benchmark: ${name}"
                python3 -c "
import json
with open('${json_file}') as f:
    data = json.load(f)
totals = data.get('ALL STATS', {}).get('Totals', {})
print(f\"  Ops/sec: {totals.get('Ops/sec', 'N/A')}\")
print(f\"  Avg Latency: {totals.get('Latency', 'N/A')} ms\")
print(f\"  P99 Latency: {totals.get('p99 Latency', 'N/A')} ms\")
print(f\"  Throughput: {totals.get('KB/sec', 'N/A')} KB/sec\")
" 2>/dev/null || echo "  (parse error)"
            fi
        done
    } | tee "${report_file}"

    log_info "Report saved to: ${report_file}"
}

# ============================================================================
# Cleanup
# ============================================================================
cleanup_all() {
    log_section "Cleaning Up"

    # Gracefully stop TendisPlus instances
    for port in "$@"; do
        ${REDIS_CLI} -h 127.0.0.1 -p "${port}" -a "${DEFAULT_PASSWORD}" shutdown 2>/dev/null || true
    done
    sleep 2

    # Kill remaining tracked processes
    if [[ ${#PIDS[@]} -gt 0 ]]; then
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                log_info "Stopping process $pid..."
                kill "$pid" 2>/dev/null || true
            fi
        done
        sleep 1

        # Force kill if needed
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                log_warn "Force killing process $pid"
                kill -9 "$pid" 2>/dev/null || true
            fi
        done
    fi

    PIDS=()
    log_info "Cleanup complete."
}

# Trap for cleanup on exit
trap_cleanup() {
    local ports=("$@")
    trap "cleanup_all ${ports[@]}" EXIT INT TERM
}

# ============================================================================
# Utilities
# ============================================================================
wait_for_port() {
    local host="$1"
    local port="$2"
    local timeout="${3:-30}"
    local elapsed=0

    while ! nc -z "$host" "$port" 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $timeout ]]; then
            log_error "Timeout waiting for ${host}:${port}"
            return 1
        fi
    done
    log_info "Port ${host}:${port} is ready (${elapsed}s)"
}

get_db_size_mb() {
    local db_dir="$1"
    du -sm "$db_dir" 2>/dev/null | awk '{print $1}' || echo 0
}

print_separator() {
    echo "------------------------------------------------------------"
}

seconds_to_human() {
    local s=$1
    printf '%02dh:%02dm:%02ds' $((s/3600)) $(( (s%3600)/60 )) $((s%60))
}
