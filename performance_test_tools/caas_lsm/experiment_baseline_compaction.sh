#!/bin/bash
# ============================================================================
# 实验1: Baseline Compaction 基准测试
# 论文: "支持远程后台任务的分布式存储系统设计与实现"
# 开题报告场景: 远程 Compaction 基准性能评测
#
# 目标: 对比三种 Compaction 模式的性能差异
#   A) 本地 Compaction (RocksDB 默认)
#   B) 远程 Compaction - Legacy 模式 (直连 CSA)
#   C) 远程 Compaction - CaaS-LSM 模式 (Control Plane 调度)
#
# 评估维度:
#   - Compaction 吞吐量 (MB/s, tasks/min)
#   - Compaction 延迟 (P50/P95/P99)
#   - 写放大 (Write Amplification)
#   - 前台读写延迟影响 (P50/P99)
#   - CPU/内存资源使用率
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ============================================================================
# Experiment Parameters
# ============================================================================
TENDISPLUS_PORT=30001
CP_ADDR="localhost:50051"
CSA_PORT=8010
OBSERVATORY_PORT=8081
SHARED_FS_URI="nfs://localhost/tmp/caas_lsm_shared"

# Workload parameters
NUM_KEYS=5000000        # 5M keys for initial data
DATA_SIZE=256           # 256 bytes per value
BENCHMARK_DURATION=300  # 5 min per phase
WRITE_RATIO="1:0"      # Pure write to trigger compaction
MIXED_RATIO="1:3"      # Mixed for front-end impact
CLIENTS=20
THREADS=4

# ============================================================================
# Phase A: Local Compaction (Baseline)
# ============================================================================
run_phase_local() {
    log_section "Phase A: Local Compaction (Baseline)"

    local phase_dir="${RESULT_DIR}/phase_a_local"
    mkdir -p "${phase_dir}"
    DATA_DIR="${phase_dir}"

    # Generate config (no control_plane_address → local compaction)
    local conf="${phase_dir}/tendisplus.conf"
    local db_dir="${phase_dir}/db"
    local log_file="${phase_dir}/tendisplus.log"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" "${log_file}"

    # Start TendisPlus
    start_tendisplus "local" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30

    # Pre-populate data
    log_info "Phase A: Populating data..."
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Collect system metrics in background
    local sys_pid
    sys_pid=$(collect_system_metrics "${phase_dir}/system_metrics.csv" "${BENCHMARK_DURATION}" 2)

    # Run write workload to trigger compaction
    log_info "Phase A: Running write workload..."
    run_memtier_benchmark "local_write" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" 10 "${WRITE_RATIO}" "${DATA_SIZE}"

    sleep 5

    # Run mixed workload for front-end impact
    log_info "Phase A: Running mixed workload..."
    run_memtier_benchmark "local_mixed" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" 5 "${MIXED_RATIO}" "${DATA_SIZE}"

    wait $sys_pid 2>/dev/null || true

    # Collect DB size
    echo "db_size_mb=$(get_db_size_mb "${db_dir}")" > "${phase_dir}/db_stats.txt"

    # Stop TendisPlus
    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3

    log_info "Phase A complete."
}

# ============================================================================
# Phase B: Remote Compaction - Legacy Direct CSA
# ============================================================================
run_phase_legacy() {
    log_section "Phase B: Remote Compaction - Legacy Direct CSA"

    local phase_dir="${RESULT_DIR}/phase_b_legacy"
    mkdir -p "${phase_dir}"
    DATA_DIR="${phase_dir}"

    local shared_dir="/tmp/caas_lsm_shared"
    mkdir -p "${shared_dir}"

    # Start CSA Worker
    local csa_log="${phase_dir}/csa_worker.log"
    ${CSA_SERVER_BIN} -l "0.0.0.0:${CSA_PORT}" > "${csa_log}" 2>&1 &
    local csa_pid=$!
    PIDS+=($csa_pid)
    sleep 2

    # Generate config (legacy mode: csa_address directly)
    local conf="${phase_dir}/tendisplus.conf"
    local db_dir="${phase_dir}/db"
    local log_file="${phase_dir}/tendisplus.log"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" "${log_file}"
    # Add legacy CSA config
    cat >> "${conf}" <<EOF

# Legacy Remote Compaction (Direct CSA)
csa_address localhost:${CSA_PORT}
remote_compaction.shared_fs_uri ${SHARED_FS_URI}
EOF

    # Start TendisPlus
    start_tendisplus "legacy" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30

    # Pre-populate data
    log_info "Phase B: Populating data..."
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Collect system metrics
    local sys_pid
    sys_pid=$(collect_system_metrics "${phase_dir}/system_metrics.csv" "${BENCHMARK_DURATION}" 2)

    # Run write workload
    log_info "Phase B: Running write workload..."
    run_memtier_benchmark "legacy_write" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" 10 "${WRITE_RATIO}" "${DATA_SIZE}"

    sleep 5

    # Run mixed workload
    log_info "Phase B: Running mixed workload..."
    run_memtier_benchmark "legacy_mixed" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" 5 "${MIXED_RATIO}" "${DATA_SIZE}"

    wait $sys_pid 2>/dev/null || true

    echo "db_size_mb=$(get_db_size_mb "${db_dir}")" > "${phase_dir}/db_stats.txt"

    # Stop
    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3
    kill $csa_pid 2>/dev/null || true
    sleep 1

    log_info "Phase B complete."
}

# ============================================================================
# Phase C: Remote Compaction - CaaS-LSM with Control Plane
# ============================================================================
run_phase_caas_lsm() {
    log_section "Phase C: Remote Compaction - CaaS-LSM Mode"

    local phase_dir="${RESULT_DIR}/phase_c_caas_lsm"
    mkdir -p "${phase_dir}"
    DATA_DIR="${phase_dir}"

    local shared_dir="/tmp/caas_lsm_shared"
    mkdir -p "${shared_dir}"

    # Start Control Plane
    start_control_plane "${CP_ADDR}"
    sleep 2

    # Start CSA Workers (2 workers for load balancing)
    start_csa_worker "1" "8010" "${CP_ADDR}" "${SHARED_FS_URI}"
    start_csa_worker "2" "8011" "${CP_ADDR}" "${SHARED_FS_URI}"
    sleep 2

    # Start Observatory for metrics collection
    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    # Generate config (CaaS-LSM mode)
    local conf="${phase_dir}/tendisplus.conf"
    local db_dir="${phase_dir}/db"
    local log_file="${phase_dir}/tendisplus.log"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" "${log_file}" \
        "test_password" "${CP_ADDR}" "${SHARED_FS_URI}"

    # Start TendisPlus
    start_tendisplus "caas_lsm" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30

    # Pre-populate data
    log_info "Phase C: Populating data..."
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Collect Observatory metrics in background
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${phase_dir}/observatory_metrics.csv" \
        "$((BENCHMARK_DURATION * 2 + 30))" 2)

    # Collect system metrics
    local sys_pid
    sys_pid=$(collect_system_metrics \
        "${phase_dir}/system_metrics.csv" \
        "$((BENCHMARK_DURATION * 2 + 30))" 2)

    # Run write workload
    log_info "Phase C: Running write workload..."
    run_memtier_benchmark "caas_write" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" 10 "${WRITE_RATIO}" "${DATA_SIZE}"

    sleep 5

    # Run mixed workload
    log_info "Phase C: Running mixed workload..."
    run_memtier_benchmark "caas_mixed" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" 5 "${MIXED_RATIO}" "${DATA_SIZE}"

    # Collect Prometheus metrics
    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${phase_dir}/prometheus_final.txt"

    wait $obs_pid 2>/dev/null || true
    wait $sys_pid 2>/dev/null || true

    echo "db_size_mb=$(get_db_size_mb "${db_dir}")" > "${phase_dir}/db_stats.txt"

    # Stop
    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3

    log_info "Phase C complete."
}

# ============================================================================
# Report Generation
# ============================================================================
generate_baseline_report() {
    log_section "Generating Baseline Comparison Report"

    local report_file="${RESULT_DIR}/baseline_comparison_report.txt"

    {
        echo "============================================================"
        echo "实验1: Baseline Compaction 基准测试报告"
        echo "============================================================"
        echo "日期: $(date)"
        echo "数据量: ${NUM_KEYS} keys × ${DATA_SIZE}B = $(( NUM_KEYS * DATA_SIZE / 1048576 )) MB"
        echo "测试时长: ${BENCHMARK_DURATION}s per phase"
        echo ""

        for phase in "phase_a_local" "phase_b_legacy" "phase_c_caas_lsm"; do
            local phase_dir="${RESULT_DIR}/${phase}"
            local phase_name
            case "$phase" in
                phase_a_local)    phase_name="A) 本地 Compaction" ;;
                phase_b_legacy)   phase_name="B) Legacy 远程 Compaction" ;;
                phase_c_caas_lsm) phase_name="C) CaaS-LSM 远程 Compaction" ;;
            esac

            echo "--- ${phase_name} ---"
            print_separator

            # Write workload results
            for pattern in write mixed; do
                local json_file
                json_file=$(ls "${phase_dir}"/memtier_*_${pattern}.json 2>/dev/null | head -1)
                if [[ -f "${json_file}" ]]; then
                    echo "  [${pattern} workload]"
                    python3 -c "
import json
with open('${json_file}') as f:
    data = json.load(f)
t = data.get('ALL STATS', {}).get('Totals', {})
sets = data.get('ALL STATS', {}).get('Sets', {})
gets = data.get('ALL STATS', {}).get('Gets', {})
print(f\"    Throughput: {t.get('Ops/sec', 'N/A'):.0f} ops/sec\")
print(f\"    Avg Latency: {t.get('Latency', 'N/A'):.3f} ms\")
print(f\"    P50 Latency: {t.get('p50 Latency', 'N/A'):.3f} ms\")
print(f\"    P99 Latency: {t.get('p99 Latency', 'N/A'):.3f} ms\")
print(f\"    Throughput: {t.get('KB/sec', 'N/A'):.1f} KB/sec\")
if sets: print(f\"    SET ops/sec: {sets.get('Ops/sec', 'N/A')}\")
if gets: print(f\"    GET ops/sec: {gets.get('Ops/sec', 'N/A')}\")
" 2>/dev/null || echo "    (parse error)"
                fi
            done

            # DB size
            if [[ -f "${phase_dir}/db_stats.txt" ]]; then
                cat "${phase_dir}/db_stats.txt" | sed 's/^/    /'
            fi

            echo ""
        done

        echo "============================================================"
        echo "预期结论:"
        echo "  1. CaaS-LSM 模式下前台写入吞吐量应高于本地 Compaction"
        echo "  2. CaaS-LSM 模式下前台 P99 延迟应低于本地 Compaction"
        echo "  3. CaaS-LSM 模式 CPU 使用率应低于本地 Compaction"
        echo "  4. Legacy 模式和 CaaS-LSM 模式写放大率应相近"
        echo "============================================================"

    } | tee "${report_file}"

    log_info "Report: ${report_file}"
}

# ============================================================================
# Main
# ============================================================================
main() {
    init_experiment "baseline_compaction"
    trap_cleanup "${TENDISPLUS_PORT}"

    check_binaries "${TENDISPLUS_BIN}" "${MEMTIER}" "${REDIS_CLI}"

    local start_time=$(date +%s)

    # Run all three phases
    run_phase_local
    cleanup_all "${TENDISPLUS_PORT}"
    sleep 5

    run_phase_legacy
    cleanup_all "${TENDISPLUS_PORT}"
    sleep 5

    run_phase_caas_lsm
    cleanup_all "${TENDISPLUS_PORT}"

    local elapsed=$(( $(date +%s) - start_time ))

    # Generate report
    generate_baseline_report

    log_section "Experiment Complete"
    log_info "Total time: $(seconds_to_human $elapsed)"
    log_info "Results: ${RESULT_DIR}"
}

main "$@"
