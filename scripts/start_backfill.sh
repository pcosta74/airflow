#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
FROM_DATE="${1:-2026-09-27}"
TO_DATE="${2:-2026-10-10}"

if (( $# > 2 )); then
    echo "Usage: $0 [FROM_DATE [TO_DATE]]" >&2
    exit 2
fi

exec docker compose \
    --project-directory "$PROJECT_ROOT" \
    -f "$PROJECT_ROOT/deploy/docker-compose.yml" \
    -f "$PROJECT_ROOT/docker-compose.override.yml" \
    exec -T airflow-scheduler airflow backfill create \
    --dag-id ecommerce_sales_etl_pipeline \
    --from-date "$FROM_DATE" \
    --to-date "$TO_DATE"
