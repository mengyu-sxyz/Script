# Script
Jack‘s Script
# Kubernetes Database Pod Management (ClickHouse/PostgreSQL)

This tool helps manage database pods (ClickHouse/PostgreSQL) in a Kubernetes environment, providing various commands for easier operations.

## Commands

- **`install-tools`**: Install essential tools (vim, less) in the container.
- **`sql`**: Connect to the database interactive shell.
- **`crashlog`**: View critical error logs (Fatal/ERROR level).
- **`log`**: View full application logs.
- **`schemacheck`**: List database schemas (ClickHouse only).
- **`datacheck`**: Show table sizes (ClickHouse only).
- **`grantcheck`**: View user permissions (ClickHouse only).

## Pod Specification

You can specify the pod in the following ways:

- **`all`**: Apply command to all matching pods.
- **`<pod_name>`**: Target a specific pod (auto-detects namespace).
- **`<namespace>/<pod>`**: Explicit namespace specification.

## Examples

- Connect to the ClickHouse server interactive shell:
  ```bash
  $0 sql clickhouse-server-0

