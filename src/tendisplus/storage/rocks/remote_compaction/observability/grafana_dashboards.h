// Copyright (c) 2024-present, Tencent Inc. All rights reserved.
// Grafana 仪表盘配置生成

#pragma once

#include <string>
#include <vector>

namespace tendisplus {
namespace observability {

// ============================================================================
// Grafana 面板类型
// ============================================================================
enum class PanelType {
  kGraph = 0,
  kStat = 1,
  kGauge = 2,
  kTable = 3,
  kHeatmap = 4,
  kPieChart = 5,
  kTimeSeries = 6,
  kBarGauge = 7,
  kText = 8,
  kAlertList = 9
};

// ============================================================================
// Grafana 仪表盘生成器
// ============================================================================
class GrafanaDashboards {
 public:
  GrafanaDashboards() = default;
  ~GrafanaDashboards() = default;
  
  // =========================================================================
  // 主仪表盘 - 系统概览
  // =========================================================================
  static std::string GenerateOverviewDashboard() {
    return R"({
  "dashboard": {
    "title": "TendisPlus Remote Compaction - Overview",
    "uid": "tendis-rc-overview",
    "tags": ["tendisplus", "remote-compaction"],
    "timezone": "browser",
    "refresh": "10s",
    "panels": [
      {
        "title": "Active Tasks",
        "type": "stat",
        "gridPos": {"x": 0, "y": 0, "w": 4, "h": 4},
        "targets": [{"expr": "sum(tendis_tasks_active)"}],
        "fieldConfig": {"defaults": {"thresholds": {"steps": [
          {"color": "green", "value": null},
          {"color": "yellow", "value": 50},
          {"color": "red", "value": 100}
        ]}}}
      },
      {
        "title": "Queue Length",
        "type": "stat",
        "gridPos": {"x": 4, "y": 0, "w": 4, "h": 4},
        "targets": [{"expr": "sum(tendis_queue_length)"}],
        "fieldConfig": {"defaults": {"thresholds": {"steps": [
          {"color": "green", "value": null},
          {"color": "yellow", "value": 100},
          {"color": "red", "value": 500}
        ]}}}
      },
      {
        "title": "Online Workers",
        "type": "stat",
        "gridPos": {"x": 8, "y": 0, "w": 4, "h": 4},
        "targets": [{"expr": "sum(tendis_workers_online)"}]
      },
      {
        "title": "Success Rate",
        "type": "gauge",
        "gridPos": {"x": 12, "y": 0, "w": 4, "h": 4},
        "targets": [{"expr": "sum(rate(tendis_tasks_completed_total[5m])) / sum(rate(tendis_tasks_submitted_total[5m])) * 100"}],
        "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100}}
      },
      {
        "title": "Task Throughput",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 4, "w": 12, "h": 8},
        "targets": [
          {"expr": "sum(rate(tendis_tasks_completed_total[5m]))", "legendFormat": "Completed"},
          {"expr": "sum(rate(tendis_tasks_failed_total[5m]))", "legendFormat": "Failed"},
          {"expr": "sum(rate(tendis_tasks_submitted_total[5m]))", "legendFormat": "Submitted"}
        ]
      },
      {
        "title": "Task Duration Distribution",
        "type": "heatmap",
        "gridPos": {"x": 12, "y": 4, "w": 12, "h": 8},
        "targets": [{"expr": "sum(rate(tendis_task_duration_seconds_bucket[5m])) by (le)"}]
      },
      {
        "title": "Worker CPU Usage",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_worker_cpu_usage", "legendFormat": "{{worker_id}}"}]
      },
      {
        "title": "Worker Memory Usage",
        "type": "timeseries",
        "gridPos": {"x": 8, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_worker_memory_usage", "legendFormat": "{{worker_id}}"}]
      },
      {
        "title": "Data Transfer Rate",
        "type": "timeseries",
        "gridPos": {"x": 16, "y": 12, "w": 8, "h": 6},
        "targets": [
          {"expr": "sum(rate(tendis_file_transfer_bytes_total{direction=\"upload\"}[5m]))", "legendFormat": "Upload"},
          {"expr": "sum(rate(tendis_file_transfer_bytes_total{direction=\"download\"}[5m]))", "legendFormat": "Download"}
        ]
      }
    ]
  }
})";
  }
  
  // =========================================================================
  // 任务详情仪表盘
  // =========================================================================
  static std::string GenerateTaskDashboard() {
    return R"({
  "dashboard": {
    "title": "TendisPlus Remote Compaction - Tasks",
    "uid": "tendis-rc-tasks",
    "tags": ["tendisplus", "remote-compaction", "tasks"],
    "panels": [
      {
        "title": "Task Status Distribution",
        "type": "piechart",
        "gridPos": {"x": 0, "y": 0, "w": 6, "h": 6},
        "targets": [
          {"expr": "sum(tendis_tasks_active{status=\"running\"})", "legendFormat": "Running"},
          {"expr": "sum(tendis_tasks_active{status=\"queued\"})", "legendFormat": "Queued"},
          {"expr": "sum(tendis_tasks_active{status=\"scheduled\"})", "legendFormat": "Scheduled"}
        ]
      },
      {
        "title": "Task Type Distribution",
        "type": "piechart",
        "gridPos": {"x": 6, "y": 0, "w": 6, "h": 6},
        "targets": [
          {"expr": "sum(tendis_tasks_submitted_total{type=\"compaction\"})", "legendFormat": "Compaction"},
          {"expr": "sum(tendis_tasks_submitted_total{type=\"bulk_load\"})", "legendFormat": "Bulk Load"}
        ]
      },
      {
        "title": "Execution Mode Distribution",
        "type": "piechart",
        "gridPos": {"x": 12, "y": 0, "w": 6, "h": 6},
        "targets": [
          {"expr": "sum(tendis_executions_total{mode=\"local\"})", "legendFormat": "Local"},
          {"expr": "sum(tendis_executions_total{mode=\"remote_single\"})", "legendFormat": "Remote Single"},
          {"expr": "sum(tendis_executions_total{mode=\"remote_parallel\"})", "legendFormat": "Remote Parallel"}
        ]
      },
      {
        "title": "Queue Wait Time",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 6, "w": 12, "h": 6},
        "targets": [
          {"expr": "histogram_quantile(0.50, sum(rate(tendis_queue_wait_seconds_bucket[5m])) by (le))", "legendFormat": "p50"},
          {"expr": "histogram_quantile(0.95, sum(rate(tendis_queue_wait_seconds_bucket[5m])) by (le))", "legendFormat": "p95"},
          {"expr": "histogram_quantile(0.99, sum(rate(tendis_queue_wait_seconds_bucket[5m])) by (le))", "legendFormat": "p99"}
        ]
      },
      {
        "title": "Task Execution Time",
        "type": "timeseries",
        "gridPos": {"x": 12, "y": 6, "w": 12, "h": 6},
        "targets": [
          {"expr": "histogram_quantile(0.50, sum(rate(tendis_task_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p50"},
          {"expr": "histogram_quantile(0.95, sum(rate(tendis_task_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p95"},
          {"expr": "histogram_quantile(0.99, sum(rate(tendis_task_duration_seconds_bucket[5m])) by (le))", "legendFormat": "p99"}
        ]
      },
      {
        "title": "Task Retry Rate",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "sum(rate(tendis_tasks_retried_total[5m])) by (type)"}]
      },
      {
        "title": "Task Failure Rate",
        "type": "timeseries",
        "gridPos": {"x": 8, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "sum(rate(tendis_tasks_failed_total[5m])) / sum(rate(tendis_tasks_completed_total[5m])) * 100"}],
        "fieldConfig": {"defaults": {"unit": "percent"}}
      },
      {
        "title": "State Transitions",
        "type": "timeseries",
        "gridPos": {"x": 16, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "sum(rate(tendis_state_transitions_total[5m])) by (from, to)"}]
      }
    ]
  }
})";
  }
  
  // =========================================================================
  // Worker 仪表盘
  // =========================================================================
  static std::string GenerateWorkerDashboard() {
    return R"({
  "dashboard": {
    "title": "TendisPlus Remote Compaction - Workers",
    "uid": "tendis-rc-workers",
    "tags": ["tendisplus", "remote-compaction", "workers"],
    "panels": [
      {
        "title": "Worker Status",
        "type": "table",
        "gridPos": {"x": 0, "y": 0, "w": 24, "h": 6},
        "targets": [
          {"expr": "tendis_worker_info", "format": "table", "instant": true}
        ],
        "transformations": [{"id": "organize", "options": {"excludeByName": {"__name__": true}}}]
      },
      {
        "title": "CPU Usage by Worker",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 6, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_worker_cpu_usage", "legendFormat": "{{worker_id}}"}]
      },
      {
        "title": "Memory Usage by Worker",
        "type": "timeseries",
        "gridPos": {"x": 8, "y": 6, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_worker_memory_usage", "legendFormat": "{{worker_id}}"}]
      },
      {
        "title": "Disk IO by Worker",
        "type": "timeseries",
        "gridPos": {"x": 16, "y": 6, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_worker_disk_usage", "legendFormat": "{{worker_id}}"}]
      },
      {
        "title": "Tasks per Worker",
        "type": "bargauge",
        "gridPos": {"x": 0, "y": 12, "w": 12, "h": 6},
        "targets": [{"expr": "sum(tendis_tasks_active) by (worker_id)"}]
      },
      {
        "title": "Worker Load Balance",
        "type": "gauge",
        "gridPos": {"x": 12, "y": 12, "w": 6, "h": 6},
        "targets": [{"expr": "tendis_load_balance_score"}],
        "fieldConfig": {"defaults": {"min": 0, "max": 1, "thresholds": {"steps": [
          {"color": "red", "value": null},
          {"color": "yellow", "value": 0.5},
          {"color": "green", "value": 0.8}
        ]}}}
      },
      {
        "title": "Heartbeat Status",
        "type": "stat",
        "gridPos": {"x": 18, "y": 12, "w": 6, "h": 6},
        "targets": [{"expr": "sum(rate(tendis_worker_heartbeat_timeouts_total[5m]))"}],
        "fieldConfig": {"defaults": {"thresholds": {"steps": [
          {"color": "green", "value": null},
          {"color": "yellow", "value": 1},
          {"color": "red", "value": 5}
        ]}}}
      }
    ]
  }
})";
  }
  
  // =========================================================================
  // 性能仪表盘
  // =========================================================================
  static std::string GeneratePerformanceDashboard() {
    return R"({
  "dashboard": {
    "title": "TendisPlus Remote Compaction - Performance",
    "uid": "tendis-rc-performance",
    "tags": ["tendisplus", "remote-compaction", "performance"],
    "panels": [
      {
        "title": "End-to-End Latency",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 0, "w": 12, "h": 6},
        "targets": [
          {"expr": "histogram_quantile(0.50, sum(rate(tendis_end_to_end_latency_seconds_bucket[5m])) by (le))", "legendFormat": "p50"},
          {"expr": "histogram_quantile(0.95, sum(rate(tendis_end_to_end_latency_seconds_bucket[5m])) by (le))", "legendFormat": "p95"},
          {"expr": "histogram_quantile(0.99, sum(rate(tendis_end_to_end_latency_seconds_bucket[5m])) by (le))", "legendFormat": "p99"}
        ]
      },
      {
        "title": "Data Throughput",
        "type": "timeseries",
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 6},
        "targets": [
          {"expr": "sum(rate(tendis_bytes_processed_total[5m]))", "legendFormat": "Bytes/sec"}
        ],
        "fieldConfig": {"defaults": {"unit": "Bps"}}
      },
      {
        "title": "Write Amplification",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 6, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_write_amplification", "legendFormat": "Write Amp"}]
      },
      {
        "title": "Read Amplification",
        "type": "timeseries",
        "gridPos": {"x": 8, "y": 6, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_read_amplification", "legendFormat": "Read Amp"}]
      },
      {
        "title": "Space Amplification",
        "type": "timeseries",
        "gridPos": {"x": 16, "y": 6, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_space_amplification", "legendFormat": "Space Amp"}]
      },
      {
        "title": "Compaction Rate",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_compaction_rate_bytes_per_sec"}],
        "fieldConfig": {"defaults": {"unit": "Bps"}}
      },
      {
        "title": "Ingest Rate",
        "type": "timeseries",
        "gridPos": {"x": 8, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_ingest_rate_bytes_per_sec"}],
        "fieldConfig": {"defaults": {"unit": "Bps"}}
      },
      {
        "title": "Cache Hit Rate",
        "type": "gauge",
        "gridPos": {"x": 16, "y": 12, "w": 8, "h": 6},
        "targets": [{"expr": "tendis_cache_hit_rate * 100"}],
        "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100}}
      }
    ]
  }
})";
  }
  
  // =========================================================================
  // 业务影响仪表盘
  // =========================================================================
  static std::string GenerateBusinessImpactDashboard() {
    return R"({
  "dashboard": {
    "title": "TendisPlus Remote Compaction - Business Impact",
    "uid": "tendis-rc-business",
    "tags": ["tendisplus", "remote-compaction", "business"],
    "panels": [
      {
        "title": "QPS Impact",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 0, "w": 12, "h": 6},
        "targets": [
          {"expr": "tendis_qps_baseline", "legendFormat": "Baseline"},
          {"expr": "tendis_qps_current", "legendFormat": "Current"}
        ]
      },
      {
        "title": "Latency Impact",
        "type": "timeseries",
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 6},
        "targets": [
          {"expr": "tendis_latency_p99_baseline_ms", "legendFormat": "Baseline P99"},
          {"expr": "tendis_latency_p99_current_ms", "legendFormat": "Current P99"}
        ],
        "fieldConfig": {"defaults": {"unit": "ms"}}
      },
      {
        "title": "SLA Compliance Rate",
        "type": "gauge",
        "gridPos": {"x": 0, "y": 6, "w": 6, "h": 6},
        "targets": [{"expr": "tendis_sla_compliance_rate * 100"}],
        "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100, "thresholds": {"steps": [
          {"color": "red", "value": null},
          {"color": "yellow", "value": 95},
          {"color": "green", "value": 99}
        ]}}}
      },
      {
        "title": "Remote Task Ratio",
        "type": "gauge",
        "gridPos": {"x": 6, "y": 6, "w": 6, "h": 6},
        "targets": [{"expr": "tendis_remote_task_ratio * 100"}],
        "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100}}
      },
      {
        "title": "CPU Offload Efficiency",
        "type": "gauge",
        "gridPos": {"x": 12, "y": 6, "w": 6, "h": 6},
        "targets": [{"expr": "tendis_offload_cpu_saved_percent"}],
        "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100}}
      },
      {
        "title": "IO Offload Efficiency",
        "type": "gauge",
        "gridPos": {"x": 18, "y": 6, "w": 6, "h": 6},
        "targets": [{"expr": "tendis_offload_io_saved_percent"}],
        "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100}}
      },
      {
        "title": "Space Saved",
        "type": "stat",
        "gridPos": {"x": 0, "y": 12, "w": 8, "h": 4},
        "targets": [{"expr": "sum(tendis_space_saved_bytes_total)"}],
        "fieldConfig": {"defaults": {"unit": "bytes"}}
      },
      {
        "title": "Data Processed",
        "type": "stat",
        "gridPos": {"x": 8, "y": 12, "w": 8, "h": 4},
        "targets": [{"expr": "sum(tendis_data_bytes_processed_total)"}],
        "fieldConfig": {"defaults": {"unit": "bytes"}}
      },
      {
        "title": "Availability",
        "type": "stat",
        "gridPos": {"x": 16, "y": 12, "w": 8, "h": 4},
        "targets": [{"expr": "tendis_availability_rate * 100"}],
        "fieldConfig": {"defaults": {"unit": "percent"}}
      }
    ]
  }
})";
  }
  
  // =========================================================================
  // 告警仪表盘
  // =========================================================================
  static std::string GenerateAlertDashboard() {
    return R"({
  "dashboard": {
    "title": "TendisPlus Remote Compaction - Alerts",
    "uid": "tendis-rc-alerts",
    "tags": ["tendisplus", "remote-compaction", "alerts"],
    "panels": [
      {
        "title": "Active Alerts",
        "type": "alertlist",
        "gridPos": {"x": 0, "y": 0, "w": 24, "h": 8},
        "options": {"showOptions": "current", "sortOrder": 1, "stateFilter": {"firing": true, "pending": true}}
      },
      {
        "title": "Alert History",
        "type": "table",
        "gridPos": {"x": 0, "y": 8, "w": 24, "h": 8},
        "targets": [{"expr": "ALERTS", "format": "table"}]
      },
      {
        "title": "Critical Alerts Count",
        "type": "stat",
        "gridPos": {"x": 0, "y": 16, "w": 6, "h": 4},
        "targets": [{"expr": "sum(tendis_alerts_active{severity=\"critical\"})"}],
        "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "red"}}}
      },
      {
        "title": "Warning Alerts Count",
        "type": "stat",
        "gridPos": {"x": 6, "y": 16, "w": 6, "h": 4},
        "targets": [{"expr": "sum(tendis_alerts_active{severity=\"warning\"})"}],
        "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "yellow"}}}
      },
      {
        "title": "Task Failure Rate Alert",
        "type": "timeseries",
        "gridPos": {"x": 12, "y": 16, "w": 12, "h": 4},
        "targets": [
          {"expr": "sum(rate(tendis_tasks_failed_total[5m])) / sum(rate(tendis_tasks_submitted_total[5m])) * 100", "legendFormat": "Failure Rate"},
          {"expr": "5", "legendFormat": "Threshold"}
        ],
        "fieldConfig": {"defaults": {"unit": "percent"}}
      }
    ]
  }
})";
  }
  
  // 获取所有仪表盘
  static std::vector<std::string> GetAllDashboards() {
    return {
      GenerateOverviewDashboard(),
      GenerateTaskDashboard(),
      GenerateWorkerDashboard(),
      GeneratePerformanceDashboard(),
      GenerateBusinessImpactDashboard(),
      GenerateAlertDashboard()
    };
  }
};

}  // namespace observability
}  // namespace tendisplus
