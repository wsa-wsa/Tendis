#!/bin/bash
# ============================================================================
# 实验4: 多任务并发调度测试
# 论文: "支持远程后台任务的分布式存储系统设计与实现"
# 开题报告场景: Compaction 与 Bulk Load 混合调度性能
#
# 目标: 评估 CaaS-LSM 调度器在多任务并发场景下的表现
#   A) 多 TendisPlus 节点并发提交 Compaction 任务
#   B) Compaction + Bulk Load 混合调度
#   C) 4 种调度策略对比 (FIFO/Priority/FairShare/LeastLoaded)
#
# 评估维度:
#   - 调度吞吐量 (tasks/min)
#   - 任务排队时间 (队列延迟 P50/P95/P99)
#   - 任务执行时间 (执行延迟分布)
#   - 调度公平性 (各节点分配均匀度)
#   - Worker 负载均衡度
#   - 不同调度策略效果对比
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ============================================================================
# Experiment Parameters
# ============================================================================
CP_ADDR="localhost:50054"
OBSERVATORY_PORT=8084

# Multi-node setup
declare -a TENDISPLUS_PORTS=(30010 30011 30012 30013)
NUM_NODES=${#TENDISPLUS_PORTS[@]}

# Worker counts for load balancing test
NUM_WORKERS=4
DATA_SIZE=256
NUM_KEYS_PER_NODE=2000000  # 2M per node
BENCHMARK_DURATION=180     # 3 min

# Scheduling policies to test
declare -a POLICIES=("FIFO" "Priority" "FairShare" "LeastLoaded")

# ============================================================================
# Test 1: Multi-Node Concurrent Compaction
# ============================================================================
run_multi_node_test() {
    log_section "Test 1: Multi-Node Concurrent Compaction"

    local test_dir="${RESULT_DIR}/multi_node"
    mkdir -p "${test_dir}"
    DATA_DIR="${test_dir}"

    local shared_dir="/tmp/caas_lsm_multi_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start Control Plane
    start_control_plane "${CP_ADDR}"
    sleep 2

    # Start Workers
    for w in $(seq 1 ${NUM_WORKERS}); do
        start_csa_worker "$w" "$((8050 + w))" "${CP_ADDR}" "${shared_fs_uri}"
    done
    sleep 2

    # Start Observatory
    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    # Start multiple TendisPlus nodes
    for i in "${!TENDISPLUS_PORTS[@]}"; do
        local port="${TENDISPLUS_PORTS[$i]}"
        local node_dir="${test_dir}/node_${i}"
        local db_dir="${node_dir}/db"
        local conf="${node_dir}/tendisplus.conf"
        mkdir -p "${db_dir}"

        generate_tendisplus_conf "${conf}" "${port}" "${db_dir}" \
            "${node_dir}/tendisplus.log" "test_password" "${CP_ADDR}" "${shared_fs_uri}"

        start_tendisplus "node_${i}" "${conf}"
    done

    # Wait for all nodes
    for port in "${TENDISPLUS_PORTS[@]}"; do
        wait_for_port "127.0.0.1" "${port}" 30
    done

    # Pre-populate all nodes
    for port in "${TENDISPLUS_PORTS[@]}"; do
        populate_data "127.0.0.1" "${port}" "test_password" "${NUM_KEYS_PER_NODE}" "${DATA_SIZE}" &
    done
    wait

    # Start metrics collection
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${test_dir}/observatory_metrics.csv" \
        "$((BENCHMARK_DURATION + 60))" 2)

    local sys_pid
    sys_pid=$(collect_system_metrics \
        "${test_dir}/system_metrics.csv" \
        "$((BENCHMARK_DURATION + 60))" 2)

    # Run concurrent write workloads on all nodes simultaneously
    log_info "Running concurrent write workloads on ${NUM_NODES} nodes..."
    for i in "${!TENDISPLUS_PORTS[@]}"; do
        local port="${TENDISPLUS_PORTS[$i]}"
        run_memtier_benchmark "node_${i}" "127.0.0.1" "${port}" "test_password" \
            "${BENCHMARK_DURATION}" 10 2 10 "1:0" "${DATA_SIZE}" &
    done
    wait

    # Collect final metrics
    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${test_dir}/prometheus_final.txt"

    wait $obs_pid 2>/dev/null || true
    wait $sys_pid 2>/dev/null || true

    # Record cluster status
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/cluster" > "${test_dir}/cluster_status.json" 2>/dev/null || true
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/workers" > "${test_dir}/workers_status.json" 2>/dev/null || true

    # Cleanup
    for port in "${TENDISPLUS_PORTS[@]}"; do
        ${REDIS_CLI} -h 127.0.0.1 -p "${port}" -a test_password shutdown 2>/dev/null || true
    done
    sleep 3
    cleanup_all "${TENDISPLUS_PORTS[@]}"
    sleep 5
}

# ============================================================================
# Test 2: Mixed Compaction + Bulk Load Scheduling
# ============================================================================
run_mixed_task_test() {
    log_section "Test 2: Mixed Compaction + Bulk Load Scheduling"

    local test_dir="${RESULT_DIR}/mixed_tasks"
    mkdir -p "${test_dir}"
    DATA_DIR="${test_dir}"

    local shared_dir="/tmp/caas_lsm_mixed_task_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start infrastructure
    start_control_plane "${CP_ADDR}"
    sleep 2

    for w in $(seq 1 ${NUM_WORKERS}); do
        start_csa_worker "$w" "$((8060 + w))" "${CP_ADDR}" "${shared_fs_uri}"
    done
    sleep 2

    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    # Start 2 TendisPlus nodes: one does Compaction, one does Bulk Load
    local compaction_port=30020
    local bulkload_port=30021

    for port_name in "compaction:${compaction_port}" "bulkload:${bulkload_port}"; do
        IFS=':' read -r name port <<< "${port_name}"
        local node_dir="${test_dir}/node_${name}"
        local db_dir="${node_dir}/db"
        mkdir -p "${db_dir}"

        generate_tendisplus_conf "${node_dir}/tendisplus.conf" "${port}" "${db_dir}" \
            "${node_dir}/tendisplus.log" "test_password" "${CP_ADDR}" "${shared_fs_uri}"

        start_tendisplus "${name}" "${node_dir}/tendisplus.conf"
    done

    wait_for_port "127.0.0.1" "${compaction_port}" 30
    wait_for_port "127.0.0.1" "${bulkload_port}" 30

    # Pre-populate compaction node
    populate_data "127.0.0.1" "${compaction_port}" "test_password" 3000000 "${DATA_SIZE}"

    # Collect metrics
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${test_dir}/observatory_metrics.csv" \
        "$((BENCHMARK_DURATION + 60))" 2)

    # Concurrent: Compaction (heavy writes) + Bulk Load
    log_info "Running concurrent Compaction + Bulk Load..."

    # Prepare Bulk Load data file
    prepare_bulk_load_data "${test_dir}" 5000000 "${DATA_SIZE}"

    # Compaction workload
    run_memtier_benchmark "compaction_during_mixed" "127.0.0.1" "${compaction_port}" "test_password" \
        "${BENCHMARK_DURATION}" 15 4 10 "1:0" "${DATA_SIZE}" &

    # Bulk Load task
    ${REDIS_CLI} -h 127.0.0.1 -p "${bulkload_port}" -a test_password \
        BULKLOAD SUBMIT "${test_dir}/bulk_load_source.kv" 2>/dev/null &

    wait

    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${test_dir}/prometheus_final.txt"
    wait $obs_pid 2>/dev/null || true

    # Cleanup
    for port in "${compaction_port}" "${bulkload_port}"; do
        ${REDIS_CLI} -h 127.0.0.1 -p "${port}" -a test_password shutdown 2>/dev/null || true
    done
    sleep 3
    cleanup_all "${compaction_port}" "${bulkload_port}"
    sleep 5
}

# ============================================================================
# Test 3: Scheduling Policy Comparison
# ============================================================================
run_policy_comparison_test() {
    log_section "Test 3: Scheduling Policy Comparison"

    local test_dir="${RESULT_DIR}/policy_comparison"
    mkdir -p "${test_dir}"

    local shared_dir="/tmp/caas_lsm_policy_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    for policy in "${POLICIES[@]}"; do
        local policy_dir="${test_dir}/${policy}"
        mkdir -p "${policy_dir}"
        DATA_DIR="${policy_dir}"

        log_info "Testing scheduling policy: ${policy}"

        # Start Control Plane with specific policy
        # Note: Policy is configured via gRPC API or config parameter
        local cp_log="${policy_dir}/control_plane.log"
        ${CONTROL_PLANE_BIN} -l "0.0.0.0:50054" -s "${policy}" > "${cp_log}" 2>&1 &
        local cp_pid=$!
        PIDS+=($cp_pid)
        sleep 2

        # Start Workers (3 workers with different capacities)
        start_csa_worker "fast" "8070" "${CP_ADDR}" "${shared_fs_uri}"   # High capacity
        start_csa_worker "medium" "8071" "${CP_ADDR}" "${shared_fs_uri}" # Medium
        start_csa_worker "slow" "8072" "${CP_ADDR}" "${shared_fs_uri}"   # Low capacity
        sleep 2

        start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
        sleep 2

        # Start 3 TendisPlus nodes
        local ports=(30030 30031 30032)
        for i in "${!ports[@]}"; do
            local port="${ports[$i]}"
            local node_dir="${policy_dir}/node_${i}"
            mkdir -p "${node_dir}/db"

            generate_tendisplus_conf "${node_dir}/tendisplus.conf" "${port}" \
                "${node_dir}/db" "${node_dir}/tendisplus.log" \
                "test_password" "${CP_ADDR}" "${shared_fs_uri}"

            start_tendisplus "policy_${policy}_node_${i}" "${node_dir}/tendisplus.conf"
        done

        for port in "${ports[@]}"; do
            wait_for_port "127.0.0.1" "${port}" 30
        done

        # Pre-populate
        for port in "${ports[@]}"; do
            populate_data "127.0.0.1" "${port}" "test_password" 1500000 "${DATA_SIZE}" &
        done
        wait

        # Collect metrics
        local obs_pid
        obs_pid=$(collect_observatory_metrics \
            "http://localhost:${OBSERVATORY_PORT}" \
            "${policy_dir}/observatory_metrics.csv" \
            "$((BENCHMARK_DURATION + 30))" 2)

        # Run concurrent workloads
        for i in "${!ports[@]}"; do
            run_memtier_benchmark "policy_${policy}_node_${i}" \
                "127.0.0.1" "${ports[$i]}" "test_password" \
                "${BENCHMARK_DURATION}" 10 2 5 "1:0" "${DATA_SIZE}" &
        done
        wait

        # Record results
        collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${policy_dir}/prometheus_final.txt"
        curl -s "http://localhost:${OBSERVATORY_PORT}/api/workers" > "${policy_dir}/workers_final.json" 2>/dev/null || true

        wait $obs_pid 2>/dev/null || true

        # Cleanup
        for port in "${ports[@]}"; do
            ${REDIS_CLI} -h 127.0.0.1 -p "${port}" -a test_password shutdown 2>/dev/null || true
        done
        sleep 3
        cleanup_all "${ports[@]}"
        sleep 5
    done
}

# ============================================================================
# Report
# ============================================================================
generate_multi_task_report() {
    log_section "Generating Multi-Task Scheduling Report"

    local report_file="${RESULT_DIR}/multi_task_report.txt"

    {
        echo "============================================================"
        echo "实验4: 多任务并发调度测试报告"
        echo "============================================================"
        echo "日期: $(date)"
        echo "TendisPlus 节点数: ${NUM_NODES}"
        echo "Worker 数: ${NUM_WORKERS}"
        echo ""

        # Multi-node results
        echo "--- 多节点并发 Compaction ---"
        for i in $(seq 0 $((NUM_NODES - 1))); do
            local json_file="${RESULT_DIR}/multi_node/memtier_node_${i}.json"
            if [[ -f "${json_file}" ]]; then
                echo "  [Node ${i}]"
                python3 -c "
import json
with open('${json_file}') as f:
    data = json.load(f)
t = data.get('ALL STATS', {}).get('Totals', {})
print(f\"    Ops/sec: {t.get('Ops/sec', 0):.0f}\")
print(f\"    P99 latency: {t.get('p99 Latency', 0):.3f} ms\")
" 2>/dev/null || echo "    (parse error)"
            fi
        done
        echo ""

        # Policy comparison
        echo "--- 调度策略对比 ---"
        for policy in "${POLICIES[@]}"; do
            local prom_file="${RESULT_DIR}/policy_comparison/${policy}/prometheus_final.txt"
            if [[ -f "${prom_file}" ]]; then
                echo "  [${policy}]"
                grep -E "^caas_lsm_(tasks_completed|queue_time|execution_time)" "${prom_file}" | head -6 | sed 's/^/    /'
            fi
        done

        echo ""
        echo "============================================================"
        echo "预期结论:"
        echo "  1. 多节点并发时，CaaS-LSM 调度器可有效分配任务到各 Worker"
        echo "  2. Compaction 和 Bulk Load 混合场景下，双队列设计保证公平性"
        echo "  3. Priority 策略在高优先级场景下延迟最低"
        echo "  4. LeastLoaded 策略在 Worker 异构时负载最均衡"
        echo "  5. FairShare 策略确保各源节点公平获取调度资源"
        echo "============================================================"

    } | tee "${report_file}"

    log_info "Report: ${report_file}"
}

# ============================================================================
# Main
# ============================================================================
main() {
    init_experiment "multi_task_concurrent"
    trap_cleanup "${TENDISPLUS_PORTS[@]}"

    check_binaries "${TENDISPLUS_BIN}" "${MEMTIER}" "${REDIS_CLI}"

    local start_time=$(date +%s)

    run_multi_node_test
    run_mixed_task_test
    run_policy_comparison_test

    local elapsed=$(( $(date +%s) - start_time ))

    generate_multi_task_report

    log_section "Experiment Complete"
    log_info "Total time: $(seconds_to_human $elapsed)"
    log_info "Results: ${RESULT_DIR}"
}

main "$@"
