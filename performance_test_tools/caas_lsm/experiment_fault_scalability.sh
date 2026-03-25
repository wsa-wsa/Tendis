#!/bin/bash
# ============================================================================
# 实验5: 故障恢复与弹性扩展测试
# 论文: "支持远程后台任务的分布式存储系统设计与实现"
# 开题报告场景: 系统容错性和弹性扩缩容
#
# 目标: 验证 CaaS-LSM 在故障和弹性场景下的可用性与恢复能力
#   A) Worker 故障恢复 (Worker 崩溃/重启后任务重新调度)
#   B) 弹性扩容 (动态添加 Worker, 负载自动均衡)
#   C) 弹性缩容 (Worker 下线, 任务自动迁移)
#   D) 告警系统验证 (AlertManager 告警触发与自愈)
#
# 评估维度:
#   - 故障检测时间 (从 Worker 崩溃到发现)
#   - 任务重调度时间 (从发现到新 Worker 执行)
#   - 扩容后负载收敛时间
#   - 缩容期间任务无损率
#   - 告警触发准确率和响应时间
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ============================================================================
# Experiment Parameters
# ============================================================================
TENDISPLUS_PORT=30040
CP_ADDR="localhost:50055"
OBSERVATORY_PORT=8085

DATA_SIZE=256
NUM_KEYS=3000000
BENCHMARK_DURATION=120

# ============================================================================
# Test 1: Worker Crash and Recovery
# ============================================================================
run_worker_crash_test() {
    log_section "Test 1: Worker Crash and Recovery"

    local test_dir="${RESULT_DIR}/worker_crash"
    mkdir -p "${test_dir}"
    DATA_DIR="${test_dir}"

    local shared_dir="/tmp/caas_lsm_fault_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start infrastructure
    start_control_plane "${CP_ADDR}"
    sleep 2

    # Start 3 workers
    local worker1_pid worker2_pid worker3_pid
    worker1_pid=$(start_csa_worker "1" "8080" "${CP_ADDR}" "${shared_fs_uri}")
    worker2_pid=$(start_csa_worker "2" "8081" "${CP_ADDR}" "${shared_fs_uri}")
    worker3_pid=$(start_csa_worker "3" "8082" "${CP_ADDR}" "${shared_fs_uri}")
    sleep 2

    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    # Start TendisPlus
    local conf="${test_dir}/tendisplus.conf"
    local db_dir="${test_dir}/db"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" \
        "${test_dir}/tendisplus.log" "test_password" "${CP_ADDR}" "${shared_fs_uri}"

    start_tendisplus "fault_test" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30

    # Pre-populate
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Start continuous metrics collection
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${test_dir}/observatory_metrics.csv" \
        300 1)

    # Phase 1: Normal operation (baseline)
    log_info "Phase 1: Normal operation (30s baseline)..."
    run_memtier_benchmark "pre_crash" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        30 10 2 5 "1:0" "${DATA_SIZE}" &
    local bench_pid=$!

    sleep 15

    # Phase 2: Kill Worker 2 (simulate crash)
    log_info "Phase 2: Killing Worker 2 (simulating crash)..."
    local crash_time=$(date +%s%3N)
    kill -9 $worker2_pid 2>/dev/null || true
    echo "crash_time_ms=${crash_time}" > "${test_dir}/crash_timing.txt"

    wait $bench_pid 2>/dev/null || true

    # Phase 3: Continue workload during recovery
    log_info "Phase 3: Workload during recovery..."
    run_memtier_benchmark "during_crash" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        60 10 2 5 "1:0" "${DATA_SIZE}" &
    bench_pid=$!

    # Wait for detection and re-scheduling
    sleep 20

    # Record detection time (check if worker count decreased)
    local detect_time=$(date +%s%3N)
    local workers_online
    workers_online=$(curl -s "http://localhost:${OBSERVATORY_PORT}/api/metrics" 2>/dev/null \
        | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('workers_online',0))" 2>/dev/null || echo "?")
    echo "detect_time_ms=${detect_time}" >> "${test_dir}/crash_timing.txt"
    echo "workers_after_crash=${workers_online}" >> "${test_dir}/crash_timing.txt"

    wait $bench_pid 2>/dev/null || true

    # Phase 4: Restart Worker 2 (recovery)
    log_info "Phase 4: Restarting Worker 2..."
    local restart_time=$(date +%s%3N)
    start_csa_worker "2_restarted" "8081" "${CP_ADDR}" "${shared_fs_uri}"
    echo "restart_time_ms=${restart_time}" >> "${test_dir}/crash_timing.txt"

    sleep 10

    # Phase 5: Post-recovery workload
    log_info "Phase 5: Post-recovery workload..."
    run_memtier_benchmark "post_recovery" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        30 10 2 5 "1:0" "${DATA_SIZE}"

    local recovery_workers
    recovery_workers=$(curl -s "http://localhost:${OBSERVATORY_PORT}/api/metrics" 2>/dev/null \
        | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('workers_online',0))" 2>/dev/null || echo "?")
    echo "workers_after_recovery=${recovery_workers}" >> "${test_dir}/crash_timing.txt"

    # Collect alerts
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/alerts" > "${test_dir}/alerts.json" 2>/dev/null || true

    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${test_dir}/prometheus_final.txt"

    wait $obs_pid 2>/dev/null || true

    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3
    cleanup_all "${TENDISPLUS_PORT}"
    sleep 5

    log_info "Worker crash test complete."
}

# ============================================================================
# Test 2: Elastic Scale-Out
# ============================================================================
run_scale_out_test() {
    log_section "Test 2: Elastic Scale-Out"

    local test_dir="${RESULT_DIR}/scale_out"
    mkdir -p "${test_dir}"
    DATA_DIR="${test_dir}"

    local shared_dir="/tmp/caas_lsm_scaleout_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start with 1 worker only
    start_control_plane "${CP_ADDR}"
    sleep 2
    start_csa_worker "initial" "8090" "${CP_ADDR}" "${shared_fs_uri}"
    sleep 2
    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    # Start TendisPlus
    local conf="${test_dir}/tendisplus.conf"
    local db_dir="${test_dir}/db"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" \
        "${test_dir}/tendisplus.log" "test_password" "${CP_ADDR}" "${shared_fs_uri}"

    start_tendisplus "scale_out" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Metrics collection
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${test_dir}/observatory_metrics.csv" \
        300 1)

    # Continuous workload
    run_memtier_benchmark "scale_out_continuous" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        240 15 4 10 "1:0" "${DATA_SIZE}" &
    local bench_pid=$!

    # Record timing
    echo "start_time=$(date +%s%3N)" > "${test_dir}/scale_timing.txt"

    # Phase 1: 1 worker (60s)
    log_info "Phase 1: Running with 1 worker (60s)..."
    sleep 60

    # Phase 2: Scale to 2 workers
    log_info "Phase 2: Adding Worker 2..."
    echo "scale_to_2_time=$(date +%s%3N)" >> "${test_dir}/scale_timing.txt"
    start_csa_worker "scale2" "8091" "${CP_ADDR}" "${shared_fs_uri}"
    sleep 60

    # Phase 3: Scale to 4 workers
    log_info "Phase 3: Adding Workers 3 and 4..."
    echo "scale_to_4_time=$(date +%s%3N)" >> "${test_dir}/scale_timing.txt"
    start_csa_worker "scale3" "8092" "${CP_ADDR}" "${shared_fs_uri}"
    start_csa_worker "scale4" "8093" "${CP_ADDR}" "${shared_fs_uri}"
    sleep 60

    # Record final state
    echo "end_time=$(date +%s%3N)" >> "${test_dir}/scale_timing.txt"
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/workers" > "${test_dir}/workers_final.json" 2>/dev/null || true

    wait $bench_pid 2>/dev/null || true
    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${test_dir}/prometheus_final.txt"
    wait $obs_pid 2>/dev/null || true

    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3
    cleanup_all "${TENDISPLUS_PORT}"
    sleep 5

    log_info "Scale-out test complete."
}

# ============================================================================
# Test 3: Elastic Scale-In
# ============================================================================
run_scale_in_test() {
    log_section "Test 3: Elastic Scale-In"

    local test_dir="${RESULT_DIR}/scale_in"
    mkdir -p "${test_dir}"
    DATA_DIR="${test_dir}"

    local shared_dir="/tmp/caas_lsm_scalein_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start with 4 workers
    start_control_plane "${CP_ADDR}"
    sleep 2

    declare -a worker_pids=()
    for w in 1 2 3 4; do
        local pid
        pid=$(start_csa_worker "$w" "$((8100 + w))" "${CP_ADDR}" "${shared_fs_uri}")
        worker_pids+=($pid)
    done
    sleep 2

    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    local conf="${test_dir}/tendisplus.conf"
    local db_dir="${test_dir}/db"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" \
        "${test_dir}/tendisplus.log" "test_password" "${CP_ADDR}" "${shared_fs_uri}"

    start_tendisplus "scale_in" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" "${NUM_KEYS}" "${DATA_SIZE}"

    # Metrics
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${test_dir}/observatory_metrics.csv" \
        300 1)

    # Continuous workload
    run_memtier_benchmark "scale_in_continuous" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        240 15 4 10 "1:0" "${DATA_SIZE}" &
    local bench_pid=$!

    echo "start_time=$(date +%s%3N)" > "${test_dir}/scale_timing.txt"

    # Phase 1: 4 workers (60s)
    log_info "Phase 1: Running with 4 workers (60s)..."
    sleep 60

    # Phase 2: Scale down to 2 workers (gracefully stop workers 3 and 4)
    log_info "Phase 2: Removing Workers 3 and 4..."
    echo "scale_to_2_time=$(date +%s%3N)" >> "${test_dir}/scale_timing.txt"
    kill ${worker_pids[2]} 2>/dev/null || true
    kill ${worker_pids[3]} 2>/dev/null || true
    sleep 60

    # Phase 3: Scale down to 1 worker
    log_info "Phase 3: Removing Worker 2..."
    echo "scale_to_1_time=$(date +%s%3N)" >> "${test_dir}/scale_timing.txt"
    kill ${worker_pids[1]} 2>/dev/null || true
    sleep 60

    echo "end_time=$(date +%s%3N)" >> "${test_dir}/scale_timing.txt"

    wait $bench_pid 2>/dev/null || true

    # Collect final state
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/alerts" > "${test_dir}/alerts.json" 2>/dev/null || true
    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${test_dir}/prometheus_final.txt"
    wait $obs_pid 2>/dev/null || true

    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3
    cleanup_all "${TENDISPLUS_PORT}"
    sleep 5

    log_info "Scale-in test complete."
}

# ============================================================================
# Test 4: Alert System Verification
# ============================================================================
run_alert_test() {
    log_section "Test 4: Alert System Verification"

    local test_dir="${RESULT_DIR}/alerts"
    mkdir -p "${test_dir}"
    DATA_DIR="${test_dir}"

    local shared_dir="/tmp/caas_lsm_alert_shared"
    mkdir -p "${shared_dir}"
    local shared_fs_uri="nfs://localhost${shared_dir}"

    # Start with 1 worker only (to trigger alerts)
    start_control_plane "${CP_ADDR}"
    sleep 2
    local worker_pid
    worker_pid=$(start_csa_worker "1" "8110" "${CP_ADDR}" "${shared_fs_uri}")
    sleep 2
    start_observatory "${CP_ADDR}" "${OBSERVATORY_PORT}"
    sleep 2

    local conf="${test_dir}/tendisplus.conf"
    local db_dir="${test_dir}/db"
    mkdir -p "${db_dir}"

    generate_tendisplus_conf "${conf}" "${TENDISPLUS_PORT}" "${db_dir}" \
        "${test_dir}/tendisplus.log" "test_password" "${CP_ADDR}" "${shared_fs_uri}"

    start_tendisplus "alert_test" "${conf}"
    wait_for_port "127.0.0.1" "${TENDISPLUS_PORT}" 30
    populate_data "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" 2000000 "${DATA_SIZE}"

    # Collect alert events
    local obs_pid
    obs_pid=$(collect_observatory_metrics \
        "http://localhost:${OBSERVATORY_PORT}" \
        "${test_dir}/observatory_metrics.csv" \
        180 1)

    # Phase 1: Heavy workload to fill task queue
    log_info "Phase 1: Generating heavy load to trigger queue alerts..."
    run_memtier_benchmark "heavy_load" "127.0.0.1" "${TENDISPLUS_PORT}" "test_password" \
        30 30 4 20 "1:0" "${DATA_SIZE}"

    sleep 10

    # Record alerts
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/alerts" > "${test_dir}/alerts_after_heavy.json" 2>/dev/null || true
    curl -s "http://localhost:${OBSERVATORY_PORT}/api/alert-rules" > "${test_dir}/alert_rules.json" 2>/dev/null || true

    # Phase 2: Kill worker to trigger "no workers" alert
    log_info "Phase 2: Killing worker to trigger no_workers_online alert..."
    local kill_time=$(date +%s%3N)
    kill -9 $worker_pid 2>/dev/null || true
    echo "worker_killed_time=${kill_time}" > "${test_dir}/alert_timing.txt"

    sleep 30

    curl -s "http://localhost:${OBSERVATORY_PORT}/api/alerts" > "${test_dir}/alerts_no_workers.json" 2>/dev/null || true

    # Phase 3: Restart worker (should resolve alerts)
    log_info "Phase 3: Restarting worker (alert should resolve)..."
    local restart_time=$(date +%s%3N)
    start_csa_worker "recovered" "8110" "${CP_ADDR}" "${shared_fs_uri}"
    echo "worker_restarted_time=${restart_time}" >> "${test_dir}/alert_timing.txt"

    sleep 30

    curl -s "http://localhost:${OBSERVATORY_PORT}/api/alerts" > "${test_dir}/alerts_resolved.json" 2>/dev/null || true

    collect_prometheus_metrics "http://localhost:${OBSERVATORY_PORT}" "${test_dir}/prometheus_final.txt"
    wait $obs_pid 2>/dev/null || true

    ${REDIS_CLI} -h 127.0.0.1 -p "${TENDISPLUS_PORT}" -a test_password shutdown 2>/dev/null || true
    sleep 3
    cleanup_all "${TENDISPLUS_PORT}"

    log_info "Alert test complete."
}

# ============================================================================
# Report
# ============================================================================
generate_fault_report() {
    log_section "Generating Fault & Scalability Report"

    local report_file="${RESULT_DIR}/fault_scalability_report.txt"

    {
        echo "============================================================"
        echo "实验5: 故障恢复与弹性扩展测试报告"
        echo "============================================================"
        echo "日期: $(date)"
        echo ""

        # Worker crash results
        echo "--- Worker 故障恢复 ---"
        if [[ -f "${RESULT_DIR}/worker_crash/crash_timing.txt" ]]; then
            cat "${RESULT_DIR}/worker_crash/crash_timing.txt" | sed 's/^/  /'
        fi
        for phase in "pre_crash" "during_crash" "post_recovery"; do
            local json_file="${RESULT_DIR}/worker_crash/memtier_${phase}.json"
            if [[ -f "${json_file}" ]]; then
                echo "  [${phase}]"
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

        # Scale-out results
        echo "--- 弹性扩容 ---"
        if [[ -f "${RESULT_DIR}/scale_out/scale_timing.txt" ]]; then
            cat "${RESULT_DIR}/scale_out/scale_timing.txt" | sed 's/^/  /'
        fi
        echo ""

        # Scale-in results
        echo "--- 弹性缩容 ---"
        if [[ -f "${RESULT_DIR}/scale_in/scale_timing.txt" ]]; then
            cat "${RESULT_DIR}/scale_in/scale_timing.txt" | sed 's/^/  /'
        fi
        echo ""

        # Alert results
        echo "--- 告警系统验证 ---"
        if [[ -f "${RESULT_DIR}/alerts/alert_timing.txt" ]]; then
            cat "${RESULT_DIR}/alerts/alert_timing.txt" | sed 's/^/  /'
        fi
        for phase in "after_heavy" "no_workers" "resolved"; do
            local json_file="${RESULT_DIR}/alerts/alerts_${phase}.json"
            if [[ -f "${json_file}" ]]; then
                local count
                count=$(python3 -c "
import json
with open('${json_file}') as f:
    data = json.load(f)
alerts = data if isinstance(data, list) else data.get('alerts', [])
print(len(alerts))
" 2>/dev/null || echo "?")
                echo "  [${phase}] Active alerts: ${count}"
            fi
        done

        echo ""
        echo "============================================================"
        echo "预期结论:"
        echo "  1. Worker 崩溃后 < 30s 检测到故障，任务自动重新调度"
        echo "  2. 扩容后 < 60s 新 Worker 开始承载任务"
        echo "  3. 缩容期间在途任务成功完成或重新调度，无任务丢失"
        echo "  4. AlertManager 准确触发 pending_tasks_high 和 no_workers_online 告警"
        echo "  5. Worker 恢复后告警自动解除（Resolved 状态）"
        echo "============================================================"

    } | tee "${report_file}"

    log_info "Report: ${report_file}"
}

# ============================================================================
# Main
# ============================================================================
main() {
    init_experiment "fault_scalability"
    trap_cleanup "${TENDISPLUS_PORT}"

    check_binaries "${TENDISPLUS_BIN}" "${REDIS_CLI}" "${MEMTIER}"

    local start_time=$(date +%s)

    run_worker_crash_test
    run_scale_out_test
    run_scale_in_test
    run_alert_test

    local elapsed=$(( $(date +%s) - start_time ))

    generate_fault_report

    log_section "Experiment Complete"
    log_info "Total time: $(seconds_to_human $elapsed)"
    log_info "Results: ${RESULT_DIR}"
}

main "$@"
