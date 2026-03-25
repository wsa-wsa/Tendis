#!/bin/bash
# ============================================================================
# 实验2: 读写混合负载性能测试
# 论文: "支持远程后台任务的分布式存储系统设计与实现"
# 开题报告场景: 不同读写比例下远程 Compaction 对前台业务的影响
#
# 目标: 评估远程 Compaction 在不同读写比例下对前台业务延迟和吞吐量的影响
#   - 4种读写比例: 纯写(1:0), 写多读少(3:1), 均衡(1:1), 读多写少(1:9)
#   - 对比: 本地 Compaction vs CaaS-LSM 远程 Compaction
#
# 评估维度:
#   - 不同读写比例下的吞吐量变化
#   - 不同读写比例下的 P50/P95/P99 延迟
#   - Compaction 对读延迟和写延迟的差异影响
#   - Compaction 期间 vs 无 Compaction 期间的业务影响
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ============================================================================
# Experiment Parameters
# ============================================================================
TENDISPLUS_PORT=30002
CP_ADDR="localhost:50052"
OBSERVATORY_PORT=8082

NUM_KEYS=5000000        # 5M keys
DATA_SIZE=256
BENCHMARK_DURATION=180  # 3 min per ratio
CLIENTS=20
THREADS=4
PIPELINE=5

# Read/Write ratios to test: set:get
declare -a RATIOS=("1:0" "3:1" "1:1" "1:9")
declare -a RATIO_NAMES=("pure_write" "write_heavy" "balanced" "read_heavy")

# ============================================================================
# Run Single Ratio Test
# ============================================================================
run_ratio_test() {
    local mode="$1"      # "local" or "caas_lsm"
    local ratio="$2"
    local ratio_name="$3"
    local phase_dir="$4"

    log_info "  Testing ratio: ${ratio} (${ratio_name}) in ${mode} mode..."
    local benchmark_name="${mode}_${ratio_name}"

    # Collect system metrics
    local sys_pid
    sys_pid=$(collect_system_metrics \
        "${phase_dir}/system_${ratio_name}.csv" \
        "${BENCHMARK_DURATION}" 2)

    # Run benchmark
    run_memtier_benchmark "${benchmark_name}" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        "${BENCHMARK_DURATION}" "${CLIENTS}" "${THREADS}" "${PIPELINE}" "${ratio}" "${DATA_SIZE}"

    wait $sys_pid 2>/dev/null || true
    sleep 3
}

# ============================================================================
# Phase A: Local Compaction with Multiple Ratios
# ============================================================================
run_local_phase() {
    log_section "Phase A: Local Compaction - Mixed Workload"

    local phase_dir="${RESULT_DIR}/phase_a_local"
    mkdir -p "${phase_dir}"
    DATA_DIR="${phase_dir}"

    # Generate config
    local conf="${phase_dir}/tendisplus.conf"
    local db_dir="${phase_dir}/db"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" "${phase_dir}/tendisplus.log"

    # Start TendisPlus
    start_tendisplus "local" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30

    # Pre-populate
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Run each ratio
    for i in "${!RATIOS[@]}"; do
        run_ratio_test "local" "${RATIOS[$i]}" "${RATIO_NAMES[$i]}" "${phase_dir}"
    done

    # Stop
    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3

    log_info "Phase A complete."
}

# ============================================================================
# Phase B: CaaS-LSM with Multiple Ratios
# ============================================================================
run_caas_lsm_phase() {
    log_section "Phase B: CaaS-LSM Remote Compaction - Mixed Workload"

    local phase_dir="${RESULT_DIR}/phase_b_caas_lsm"
    mkdir -p "${phase_dir}"
    DATA_DIR="${phase_dir}"

    local shared_dir="/tmp/caas_lsm_mixed_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start Control Plane + Workers
    start_control_plane "${CP_ADDR}"
    sleep 2
    start_csa_worker "1" "8020" "${CP_ADDR}" "${shared_fs_uri}"
    start_csa_worker "2" "8021" "${CP_ADDR}" "${shared_fs_uri}"
    sleep 2
    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    # Generate config
    local conf="${phase_dir}/tendisplus.conf"
    local db_dir="${phase_dir}/db"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" "${phase_dir}/tendisplus.log" \
        "test_password" "${CP_ADDR}" "${shared_fs_uri}"

    # Start TendisPlus
    start_tendisplus "caas_lsm" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30

    # Pre-populate
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Start Observatory metrics collection for entire phase
    local total_duration=$(( BENCHMARK_DURATION * ${#RATIOS[@]} + 30 * ${#RATIOS[@]} ))
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${phase_dir}/observatory_metrics.csv" \
        "${total_duration}" 2)

    # Run each ratio
    for i in "${!RATIOS[@]}"; do
        run_ratio_test "caas_lsm" "${RATIOS[$i]}" "${RATIO_NAMES[$i]}" "${phase_dir}"
    done

    # Collect Prometheus snapshot
    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${phase_dir}/prometheus_final.txt"

    wait $obs_pid 2>/dev/null || true

    # Stop
    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3

    log_info "Phase B complete."
}

# ============================================================================
# Report
# ============================================================================
generate_mixed_workload_report() {
    log_section "Generating Mixed Workload Report"

    local report_file="${RESULT_DIR}/mixed_workload_report.txt"

    {
        echo "============================================================"
        echo "实验2: 读写混合负载性能测试报告"
        echo "============================================================"
        echo "日期: $(date)"
        echo "数据量: ${NUM_KEYS} keys × ${DATA_SIZE}B"
        echo "测试时长: ${BENCHMARK_DURATION}s per ratio"
        echo "读写比例: ${RATIOS[*]}"
        echo ""

        for mode in "local" "caas_lsm"; do
            local phase_dir
            if [[ "$mode" == "local" ]]; then
                phase_dir="${RESULT_DIR}/phase_a_local"
                echo "=== 本地 Compaction ==="
            else
                phase_dir="${RESULT_DIR}/phase_b_caas_lsm"
                echo "=== CaaS-LSM 远程 Compaction ==="
            fi
            print_separator

            for ratio_name in "${RATIO_NAMES[@]}"; do
                local json_file="${phase_dir}/memtier_${mode}_${ratio_name}.json"
                if [[ -f "${json_file}" ]]; then
                    echo "  [${ratio_name}]"
                    python3 -c "
import json
with open('${json_file}') as f:
    data = json.load(f)
t = data.get('ALL STATS', {}).get('Totals', {})
s = data.get('ALL STATS', {}).get('Sets', {})
g = data.get('ALL STATS', {}).get('Gets', {})
print(f\"    Total ops/sec: {t.get('Ops/sec', 0):.0f}\")
print(f\"    Avg latency:   {t.get('Latency', 0):.3f} ms\")
print(f\"    P50 latency:   {t.get('p50 Latency', 0):.3f} ms\")
print(f\"    P99 latency:   {t.get('p99 Latency', 0):.3f} ms\")
if s: print(f\"    SET ops/sec:   {s.get('Ops/sec', 0):.0f}  |  SET P99: {s.get('p99 Latency', 0):.3f} ms\")
if g: print(f\"    GET ops/sec:   {g.get('Ops/sec', 0):.0f}  |  GET P99: {g.get('p99 Latency', 0):.3f} ms\")
" 2>/dev/null || echo "    (parse error)"
                fi
            done
            echo ""
        done

        echo "============================================================"
        echo "预期结论:"
        echo "  1. CaaS-LSM 在纯写场景收益最大（前台 P99 延迟降低 30-50%）"
        echo "  2. 读多写少场景下收益递减，但仍有正向改善"
        echo "  3. CaaS-LSM 模式读延迟基本不受 Compaction 影响"
        echo "  4. 本地 Compaction 在 L0→L1 期间前台写入抖动明显"
        echo "============================================================"

    } | tee "${report_file}"

    log_info "Report: ${report_file}"
}

# ============================================================================
# Main
# ============================================================================
main() {
    init_experiment "mixed_workload"
    trap_cleanup "${TENDISPLUS_PORT}"

    check_binaries "${TENDISPLUS_BIN}" "${MEMTIER}" "${REDIS_CLI}"

    local start_time=$(date +%s)

    run_local_phase
    cleanup_all "${TENDISPLUS_PORT}"
    sleep 5

    run_caas_lsm_phase
    cleanup_all "${TENDISPLUS_PORT}"

    local elapsed=$(( $(date +%s) - start_time ))

    generate_mixed_workload_report

    log_section "Experiment Complete"
    log_info "Total time: $(seconds_to_human $elapsed)"
    log_info "Results: ${RESULT_DIR}"
}

main "$@"
