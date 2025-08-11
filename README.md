# Script
Jack‘s Script
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
