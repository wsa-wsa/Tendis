#!/bin/bash
# ============================================================================
# CaaS-LSM Metrics Collector
# 独立的指标收集工具，可在实验期间并行运行
# 论文: "支持远程后台任务的分布式存储系统设计与实现"
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ============================================================================
# Usage
# ============================================================================
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
    -o, --observatory-url URL    Observatory URL (default: http://localhost:8080)
    -d, --duration SECONDS       Collection duration (default: 300)
    -i, --interval SECONDS       Collection interval (default: 2)
    -r, --result-dir DIR         Output directory (default: ./metrics_TIMESTAMP)
    -s, --system-metrics         Also collect system metrics (CPU/Memory)
    -p, --prometheus             Collect Prometheus snapshot at start and end
    -h, --help                   Show this help

Examples:
    $0 -o http://localhost:8080 -d 600 -i 5 -s
    $0 -d 300 -r /tmp/my_metrics -p
EOF
}

# ============================================================================
# Parse Arguments
# ============================================================================
OBSERVATORY_URL="http://localhost:8080"
DURATION=300
INTERVAL=2
RESULT_DIR_OVERRIDE=""
COLLECT_SYSTEM=false
COLLECT_PROMETHEUS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -o|--observatory-url) OBSERVATORY_URL="$2"; shift 2 ;;
        -d|--duration)        DURATION="$2"; shift 2 ;;
        -i|--interval)        INTERVAL="$2"; shift 2 ;;
        -r|--result-dir)      RESULT_DIR_OVERRIDE="$2"; shift 2 ;;
        -s|--system-metrics)  COLLECT_SYSTEM=true; shift ;;
        -p|--prometheus)      COLLECT_PROMETHEUS=true; shift ;;
        -h|--help)            usage; exit 0 ;;
        *)                    log_error "Unknown option: $1"; usage; exit 1 ;;
    esac
done

# ============================================================================
# Main
# ============================================================================
main() {
    local result_dir="${RESULT_DIR_OVERRIDE:-${SCRIPT_DIR}/metrics_${TIMESTAMP}}"
    mkdir -p "${result_dir}"
    LOG_DIR="${result_dir}"
    DATA_DIR="${result_dir}"

    log_section "CaaS-LSM Metrics Collection"
    log_info "Observatory URL: ${OBSERVATORY_URL}"
    log_info "Duration: ${DURATION}s"
    log_info "Interval: ${INTERVAL}s"
    log_info "Output: ${result_dir}"

    local collector_pids=()

    # 1. Collect Observatory metrics
    log_info "Starting Observatory metrics collection..."
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "${OBSERVATORY_URL}" \
        "${result_dir}/observatory_metrics.csv" \
        "${DURATION}" \
        "${INTERVAL}")
    collector_pids+=($obs_pid)

    # 2. Optional: system metrics
    if [[ "${COLLECT_SYSTEM}" == "true" ]]; then
        log_info "Starting system metrics collection..."
        local sys_pid
        sys_pid=$(collect_system_metrics \
            "${result_dir}/system_metrics.csv" \
            "${DURATION}" \
            "${INTERVAL}")
        collector_pids+=($sys_pid)
    fi

    # 3. Optional: Prometheus snapshot (start)
    if [[ "${COLLECT_PROMETHEUS}" == "true" ]]; then
        collect_prometheus_metrics "${OBSERVATORY_URL}" "${result_dir}/prometheus_start.txt"
    fi

    # Wait for collection to complete
    log_info "Collecting metrics... (${DURATION}s)"
    local start_time=$(date +%s)
    while true; do
        local elapsed=$(( $(date +%s) - start_time ))
        if [[ $elapsed -ge $DURATION ]]; then
            break
        fi
        local remaining=$(( DURATION - elapsed ))
        printf "\r  Progress: %ds / %ds (%d%%)" "$elapsed" "$DURATION" "$(( elapsed * 100 / DURATION ))"
        sleep 5
    done
    echo ""

    # Wait for collector processes
    for pid in "${collector_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # 4. Optional: Prometheus snapshot (end)
    if [[ "${COLLECT_PROMETHEUS}" == "true" ]]; then
        collect_prometheus_metrics "${OBSERVATORY_URL}" "${result_dir}/prometheus_end.txt"
    fi

    # 5. Generate summary
    generate_metrics_summary "${result_dir}"

    log_info "Metrics collection complete!"
    log_info "Results in: ${result_dir}"
}

# ============================================================================
# Summary Generator
# ============================================================================
generate_metrics_summary() {
    local result_dir="$1"
    local summary_file="${result_dir}/metrics_summary.txt"

    {
        echo "============================================================"
        echo "CaaS-LSM Metrics Summary"
        echo "Generated: $(date)"
        echo "============================================================"
        echo ""

        # Observatory metrics summary
        if [[ -f "${result_dir}/observatory_metrics.csv" ]]; then
            echo "--- Observatory Metrics ---"
            local lines
            lines=$(wc -l < "${result_dir}/observatory_metrics.csv")
            echo "  Data points: $((lines - 1))"

            python3 -c "
import csv, sys
with open('${result_dir}/observatory_metrics.csv') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
if not rows:
    print('  No data collected')
    sys.exit(0)

# Get final snapshot
last = rows[-1]
print(f\"  Final snapshot:\")
print(f\"    Workers online: {last.get('workers_online', 'N/A')}\")
print(f\"    Tasks pending: {last.get('tasks_pending', 'N/A')}\")
print(f\"    Tasks running: {last.get('tasks_running', 'N/A')}\")
print(f\"    Tasks completed: {last.get('tasks_completed', 'N/A')}\")
print(f\"    Tasks failed: {last.get('tasks_failed', 'N/A')}\")
print(f\"    P50 latency: {last.get('p50_ms', 'N/A')} ms\")
print(f\"    P95 latency: {last.get('p95_ms', 'N/A')} ms\")
print(f\"    P99 latency: {last.get('p99_ms', 'N/A')} ms\")

# Compute throughput delta
first = rows[0]
completed_delta = int(last.get('tasks_completed', 0)) - int(first.get('tasks_completed', 0))
duration = int(last.get('timestamp', 0)) - int(first.get('timestamp', 0))
if duration > 0:
    rate = completed_delta / duration * 60
    print(f\"  Task completion rate: {rate:.1f} tasks/min\")
" 2>/dev/null || echo "  (analysis error)"
            echo ""
        fi

        # System metrics summary
        if [[ -f "${result_dir}/system_metrics.csv" ]]; then
            echo "--- System Metrics ---"
            python3 -c "
import csv
with open('${result_dir}/system_metrics.csv') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
if not rows:
    print('  No data collected')
else:
    cpus = [float(r.get('cpu_user','0')) + float(r.get('cpu_sys','0')) for r in rows if r.get('cpu_user')]
    mems = [float(r.get('mem_used_mb','0')) for r in rows if r.get('mem_used_mb')]
    if cpus:
        print(f\"  Avg CPU usage: {sum(cpus)/len(cpus):.1f}%\")
        print(f\"  Max CPU usage: {max(cpus):.1f}%\")
    if mems:
        print(f\"  Avg Memory: {sum(mems)/len(mems):.0f} MB\")
        print(f\"  Max Memory: {max(mems):.0f} MB\")
" 2>/dev/null || echo "  (analysis error)"
            echo ""
        fi

        # Files summary
        echo "--- Collected Files ---"
        ls -lh "${result_dir}"/*.csv "${result_dir}"/*.txt 2>/dev/null || echo "  No files found"

    } | tee "${summary_file}"
}

main "$@"
