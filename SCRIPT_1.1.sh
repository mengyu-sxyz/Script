#!/bin/bash

CONTEXT="sentio-sea"
COMMAND=$1
POD_NAME=$2
MAX_RETRIES=3
RETRY_DELAY=2
LOG_FILE="Result.log"  # 输出文件名

# 显示帮助信息
show_help() {
    cat << EOF
Usage: $0 <command> <pod_name|all>

Manage database pods (ClickHouse/PostgreSQL) in Kubernetes

Commands:
  install-tools   Install essential tools (vim, less) in the container
  sql             Connect to database interactive shell
  crashlog        View critical error logs (Fatal/ERROR level)
  log             View full application logs
  schemacheck     List database schemas (ClickHouse only)
  datacheck       Show table sizes (ClickHouse only)
  grantcheck      View user permissions (ClickHouse only)
  healthcheck     Verify database connectivity (ClickHouse only)

Pod specification:
  all             Apply command to all matching pods
  <pod_name>      Target specific pod (auto-detects namespace)
  <namespace>/<pod> Explicit namespace specification

Examples:
  $0 sql clickhouse-server-0
  $0 install-tools all
  $0 schemacheck analytics/clickhouse-analytics-pod
EOF
}

# 初始化日志文件
init_log() {
    echo "=== Script started at $(date) ===" > "$LOG_FILE"
    echo "Command: $0 $COMMAND $POD_NAME" >> "$LOG_FILE"
    echo "--------------------------------" >> "$LOG_FILE"
}

# 记录日志函数
log() {
    echo "$@" | tee -a "$LOG_FILE"  # 同时输出到屏幕和日志文件
}

# 检查帮助请求
if [[ "$1" == "-h" || "$1" == "--help" || "$1" == "help" ]]; then
    show_help | tee -a "$LOG_FILE"  # 帮助信息也记录日志
    exit 0
fi

# 检查参数数量
if [ $# -lt 2 ]; then
    log "Error: Missing required arguments"
    show_help
    exit 1
fi

# 获取相关POD列表
get_relevant_pods() {
    kubectl --context=$CONTEXT get pods -A -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.namespace}{"\n"}{end}' | 
        grep -E 'clickhouse|postgres|pg'
}

# 在POD中安装工具
install_tools() {
    local namespace=$1
    local pod=$2
    log "Installing tools in $pod..."
    kubectl --context=$CONTEXT -n $namespace exec $pod -- sh -c \
        'if command -v apt-get &>/dev/null; then 
           apt-get update && apt-get install -y vim less; 
         elif command -v yum &>/dev/null; then 
           yum install -y vim less; 
         else 
           echo "Unsupported package manager"; 
         fi' 2>&1 | tee -a "$LOG_FILE"  # 捕获所有输出
    log "Tool installation completed for $pod"
}

# 检查ClickHouse连接
check_ch_connection() {
    local namespace=$1
    local pod=$2
    local retries=0
    local status=1
    
    while [ $retries -lt $MAX_RETRIES ]; do
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            sh -c 'clickhouse-client -u default_viewer --password="Ddod9yDdno" -q "SELECT 1" > /dev/null 2>&1'
        status=$?
        
        if [ $status -eq 0 ]; then
            return 0
        fi
        
        log "Connection check failed (attempt $((retries+1))/$MAX_RETRIES), retrying in $RETRY_DELAY seconds..."
        sleep $RETRY_DELAY
        ((retries++))
    done
    
    log "Error: Unable to connect to ClickHouse after $MAX_RETRIES attempts"
    return 1
}

# 执行SQL
run_sql() {
    local namespace=$1
    local pod=$2
    if [[ $pod == *"clickhouse"* ]]; then
        log "Connecting to ClickHouse in $pod..."
        kubectl --context=$CONTEXT -n $namespace exec -it $pod -- \
            clickhouse-client -u default_viewer --password="Ddod9yDdno" --multiline 2>&1 | tee -a "$LOG_FILE"
    elif [[ $pod == *"postgres"* || $pod == *"pg"* ]]; then
        log "Connecting to PostgreSQL in $pod..."
        kubectl --context=$CONTEXT -n $namespace exec -it $pod -- \
            psql -U default_viewer -d postgres 2>&1 | tee -a "$LOG_FILE"
    else
        log "Unsupported database type for pod: $pod"
        return 1
    fi
}

# 查看崩溃日志
view_crashlog() {
    local namespace=$1
    local pod=$2
    log "Viewing crash logs for $pod..."
    if [[ $pod == *"clickhouse"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            grep -A 50 -B 20 'Fatal\|Critical\|ERROR' /var/log/clickhouse-server/clickhouse-server.log 2>&1 | tee -a "$LOG_FILE" | less -R
    elif [[ $pod == *"postgres"* || $pod == *"pg"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            grep -A 20 -B 10 'FATAL\|ERROR' /var/log/postgresql/* 2>&1 | tee -a "$LOG_FILE" | less -R
    else
        log "Crash logs not available for pod type: $pod"
        return 1
    fi
}

# 查看应用日志
view_log() {
    local namespace=$1
    local pod=$2
    log "Viewing logs for $pod..."
    if [[ $pod == *"clickhouse"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            cat /var/log/clickhouse-server/clickhouse-server.log 2>&1 | tee -a "$LOG_FILE" | less -R
    elif [[ $pod == *"postgres"* || $pod == *"pg"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            cat "$(ls -t /var/log/postgresql/* | head -1)" 2>&1 | tee -a "$LOG_FILE" | less -R
    else
        log "Logs not available for pod type: $pod"
        return 1
    fi
}

# 模式检查
schema_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        log "Schema check only available for ClickHouse pods"
        return 1
    fi
    
    if ! check_ch_connection "$namespace" "$pod"; then
        return 1
    fi
    
    log "Running schema check on $pod..."
    local query="SELECT 
        database AS Database,
        name AS Table,
        engine AS Engine,
        formatReadableSize(total_bytes) AS Size,
        total_rows AS Rows,
        partition_key AS PartitionKey,
        sorting_key AS SortingKey
    FROM system.tables
    WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    ORDER BY database, name"
    
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c "echo \"$query\" | clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" --format PrettyCompact" 2>&1 | tee -a "$LOG_FILE"
    
    # 验证结果
    local result_lines
    result_lines=$(kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c "echo \"SELECT count() FROM system.tables WHERE database NOT IN ('system')\" | \
        clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" --query=- 2>/dev/null")
    
    if [ -z "$result_lines" ] || [ "$result_lines" -eq 0 ]; then
        log "WARNING: Schema check returned no results. Possible issues:"
        log "1. Database connection problem"
        log "2. No user tables exist"
        log "3. System tables not accessible"
        return 2
    fi
}

# 数据检查
data_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        log "Data check only available for ClickHouse pods"
        return 1
    fi
    
    if ! check_ch_connection "$namespace" "$pod"; then
        return 1
    fi
    
    log "Running data check on $pod..."
    local query="SELECT
        database AS Database,
        table AS Table,
        formatReadableSize(sum(bytes)) AS Size,
        sum(rows) AS Rows,
        count() AS Parts,
        max(modification_time) AS LastModified
    FROM system.parts
    WHERE active
    GROUP BY database, table
    ORDER BY database, table"
    
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c "echo \"$query\" | clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" --format PrettyCompact" 2>&1 | tee -a "$LOG_FILE"
    
    # 验证结果
    local result_lines
    result_lines=$(kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c "echo \"SELECT count() FROM system.parts WHERE active\" | \
        clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" --query=- 2>/dev/null")
    
    if [ -z "$result_lines" ] || [ "$result_lines" -eq 0 ]; then
        log "WARNING: Data check returned no results. Possible issues:"
        log "1. No active data parts found"
        log "2. Table is empty or not loaded"
        log "3. System.parts table not accessible"
        return 2
    fi
}

# 权限检查
grant_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        log "Grant check only available for ClickHouse pods"
        return 1
    fi
    
    if ! check_ch_connection "$namespace" "$pod"; then
        return 1
    fi
    
    log "Running grant check on $pod..."
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c 'echo "SHOW GRANTS FOR default_viewer;" | clickhouse-client -u default_viewer --password="Ddod9yDdno" --format PrettyCompact' 2>&1 | tee -a "$LOG_FILE"
    
    # 验证结果
    local result_lines
    result_lines=$(kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c "echo \"SELECT count() FROM system.grants WHERE user_name = 'default_viewer'\" | \
        clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" --query=- 2>/dev/null")
    
    if [ -z "$result_lines" ] || [ "$result_lines" -eq 0 ]; then
        log "WARNING: Grant check returned no results. Possible issues:"
        log "1. User 'default_viewer' has no grants"
        log "2. System.grants table not accessible"
        return 2
    fi
}

# 健康检查
health_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        log "Health check only available for ClickHouse pods"
        return 1
    fi
    
    log "Running health check on $pod..."
    
    # 1. 检查服务状态
    local service_status
    service_status=$(kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c 'curl -s localhost:8123/ping')
    
    if [ "$service_status" = "Ok." ]; then
        log "HTTP Service: OK"
    else
        log "HTTP Service: FAILED (response: $service_status)"
        return 1
    fi
    
    # 2. 检查TCP连接
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c 'clickhouse-client -u default_viewer --password="Ddod9yDdno" -q "SELECT 1" > /dev/null'
    
    if [ $? -eq 0 ]; then
        log "TCP Connection: OK"
    else
        log "TCP Connection: FAILED"
        return 1
    fi
    
    # 3. 检查关键系统表
    local system_tables=("tables" "databases" "processes")
    local missing_tables=0
    
    for table in "${system_tables[@]}"; do
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            sh -c "clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" -q \"EXISTS system.$table\" > /dev/null"
        
        if [ $? -ne 0 ]; then
            log "System table check: system.$table MISSING"
            ((missing_tables++))
        fi
    done
    
    if [ $missing_tables -eq 0 ]; then
        log "System Tables: OK"
    else
        log "System Tables: $missing_tables critical tables missing"
        return 1
    fi
    
    # 4. 检查副本状态
    local replica_query="SELECT 
        database,
        table,
        is_leader,
        is_readonly,
        replica_is_active
    FROM system.replicas
    WHERE replica_is_active = 0 OR is_readonly = 1"
    
    local replica_status
    replica_status=$(kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c "echo \"$replica_query\" | clickhouse-client -u default_viewer --password=\"Ddod9yDdno\" --format TSV")
    
    if [ -n "$replica_status" ]; then
        log "Replica Status: ISSUES FOUND"
        echo "$replica_status" | while read -r line; do
            log "  $line"
        done
        return 1
    else
        log "Replica Status: OK"
    fi
    
    log "All health checks passed"
}

# 执行命令
execute_command() {
    local namespace=$1
    local pod=$2
    case $COMMAND in
        install-tools)
            install_tools "$namespace" "$pod"
            ;;
        sql)
            run_sql "$namespace" "$pod"
            ;;
        crashlog)
            view_crashlog "$namespace" "$pod"
            ;;
        log)
            view_log "$namespace" "$pod"
            ;;
        schemacheck)
            schema_check "$namespace" "$pod"
            ;;
        datacheck)
            data_check "$namespace" "$pod"
            ;;
        grantcheck)
            grant_check "$namespace" "$pod"
            ;;
        healthcheck)
            health_check "$namespace" "$pod"
            ;;
        *)
            log "Unknown command: $COMMAND"
            show_help
            return 1
            ;;
    esac
}

# === 主逻辑开始 ===
init_log  # 初始化日志文件

if [ "$POD_NAME" == "all" ]; then
    log "Processing all relevant pods..."
    get_relevant_pods | while read -r pod_line; do
        pod=$(echo "$pod_line" | awk '{print $1}')
        namespace=$(echo "$pod_line" | awk '{print $2}')
        log "=== Processing pod: $pod in namespace: $namespace ==="
        execute_command "$namespace" "$pod"
        log ""
    done
else
    # 解析命名空间和POD名称
    if [[ $POD_NAME == */* ]]; then
        namespace=${POD_NAME%%/*}
        pod=${POD_NAME##*/}
    else
        # 自动查找命名空间
        pod_info=$(get_relevant_pods | grep -w "$POD_NAME")
        if [ -z "$pod_info" ]; then
            log "Error: Pod '$POD_NAME' not found in any namespace"
            log "Available pods:"
            get_relevant_pods | awk '{print "- " $1 " (namespace: " $2 ")"}' | tee -a "$LOG_FILE"
            exit 1
        fi
        pod=$(echo "$pod_info" | awk '{print $1}')
        namespace=$(echo "$pod_info" | awk '{print $2}')
    fi
    
    execute_command "$namespace" "$pod"
fi

log "=== Script completed at $(date) ==="