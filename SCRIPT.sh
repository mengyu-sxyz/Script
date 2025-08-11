#!/bin/bash

CONTEXT="sentio-sea"
COMMAND=$1
POD_NAME=$2

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

# 检查帮助请求
if [[ "$1" == "-h" || "$1" == "--help" || "$1" == "help" ]]; then
    show_help
    exit 0
fi

# 检查参数数量
if [ $# -lt 2 ]; then
    echo "Error: Missing required arguments"
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
    echo "Installing tools in $pod..."
    kubectl --context=$CONTEXT -n $namespace exec $pod -- sh -c \
        'if command -v apt-get &>/dev/null; then 
           apt-get update && apt-get install -y vim less; 
         elif command -v yum &>/dev/null; then 
           yum install -y vim less; 
         else 
           echo "Unsupported package manager"; 
         fi'
    echo "Tool installation completed for $pod"
}

# 执行SQL
run_sql() {
    local namespace=$1
    local pod=$2
    if [[ $pod == *"clickhouse"* ]]; then
        echo "Connecting to ClickHouse in $pod..."
        kubectl --context=$CONTEXT -n $namespace exec -it $pod -- \
            clickhouse-client -u default_viewer --password="Ddod9yDdno" --multiline
    elif [[ $pod == *"postgres"* || $pod == *"pg"* ]]; then
        echo "Connecting to PostgreSQL in $pod..."
        kubectl --context=$CONTEXT -n $namespace exec -it $pod -- \
            psql -U default_viewer -d postgres
    else
        echo "Unsupported database type for pod: $pod"
        return 1
    fi
}

# 查看崩溃日志
view_crashlog() {
    local namespace=$1
    local pod=$2
    echo "Viewing crash logs for $pod..."
    if [[ $pod == *"clickhouse"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            grep -A 50 -B 20 'Fatal\|Critical\|ERROR' /var/log/clickhouse-server/clickhouse-server.log | less
    elif [[ $pod == *"postgres"* || $pod == *"pg"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec $pod -- \
            grep -A 20 -B 10 'FATAL\|ERROR' /var/log/postgresql/* | less
    else
        echo "Crash logs not available for pod type: $pod"
        return 1
    fi
}

# 查看应用日志
view_log() {
    local namespace=$1
    local pod=$2
    echo "Viewing logs for $pod..."
    if [[ $pod == *"clickhouse"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec -it $pod -- \
            less /var/log/clickhouse-server/clickhouse-server.log
    elif [[ $pod == *"postgres"* || $pod == *"pg"* ]]; then
        kubectl --context=$CONTEXT -n $namespace exec -it $pod -- \
            less "$(ls -t /var/log/postgresql/* | head -1)"
    else
        echo "Logs not available for pod type: $pod"
        return 1
    fi
}

# 模式检查
schema_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        echo "Schema check only available for ClickHouse pods"
        return 1
    fi
    echo "Running schema check on $pod..."
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c 'echo "SELECT database, name, create_table_query FROM system.tables ORDER BY database, name;" | clickhouse-client -u default_viewer --password="Ddod9yDdno"'
}

# 数据检查
data_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        echo "Data check only available for ClickHouse pods"
        return 1
    fi
    echo "Running data check on $pod..."
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c 'echo "SELECT database, table, formatReadableSize(sum(bytes)) as size FROM system.parts WHERE active GROUP BY database, table ORDER BY database, table;" | clickhouse-client -u default_viewer --password="Ddod9yDdno"'
}

# 权限检查
grant_check() {
    local namespace=$1
    local pod=$2
    if [[ $pod != *"clickhouse"* ]]; then
        echo "Grant check only available for ClickHouse pods"
        return 1
    fi
    echo "Running grant check on $pod..."
    kubectl --context=$CONTEXT -n $namespace exec $pod -- \
        sh -c 'echo "SHOW GRANTS FOR default_viewer;" | clickhouse-client -u default_viewer --password="Ddod9yDdno"'
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
        *)
            echo "Unknown command: $COMMAND"
            show_help
            return 1
            ;;
    esac
}

# 主逻辑
if [ "$POD_NAME" == "all" ]; then
    echo "Processing all relevant pods..."
    get_relevant_pods | while read -r pod_line; do
        pod=$(echo "$pod_line" | awk '{print $1}')
        namespace=$(echo "$pod_line" | awk '{print $2}')
        echo "=== Processing pod: $pod in namespace: $namespace ==="
        execute_command "$namespace" "$pod"
        echo ""
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
            echo "Error: Pod '$POD_NAME' not found in any namespace"
            echo "Available pods:"
            get_relevant_pods | awk '{print "- " $1 " (namespace: " $2 ")"}'
            exit 1
        fi
        pod=$(echo "$pod_info" | awk '{print $1}')
        namespace=$(echo "$pod_info" | awk '{print $2}')
    fi
    
    execute_command "$namespace" "$pod"
fi