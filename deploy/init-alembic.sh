#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
SCRIPT_NAME="$(basename -- "$0")"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${AIRFLOW__LOGGING__BASE_LOG_FOLDER:-$PROJECT_ROOT/logs}"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_DIR/${SCRIPT_NAME%.sh}.log") 2>&1

if [ ! -x "$(command -v alembic)" ]; then
    echo "ERROR: Alembic command binary is not installed in this environment path."
    exit 1
fi

if [ ! -x "$(command -v psql)" ]; then
    echo "ERROR: psql client binary is not installed. Unable to verify database connectivity."
    exit 1
fi

RAW_CONN="${SHOP_DATABASE_URL:?SHOP_DATABASE_URL must be set and non-empty}"
SHOP_CONN_STRING=$(echo "$RAW_CONN" | sed 's/+psycopg2//')

echo "Verifying shop database connectivity..."

attempt=1
delay=1
while true; do
    if psql "$SHOP_CONN_STRING" -v ON_ERROR_STOP=1 -c '\q'; then
        break
    fi

    if [ "$attempt" -eq 11 ]; then
        echo "ERROR: Could not connect to the shop database after 10 retries."
        exit 1
    fi

    echo "Shop database connection attempt $attempt failed; retrying in ${delay}s..."
    sleep "$delay"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
done

echo "Shop database connectivity verified successfully."

if [ ! -d "config/alembic" ] || [ ! -f "config/alembic/alembic.ini" ]; then
    echo "Alembic configuration tracking directory not found. Initializing skeleton layout..."
    alembic init config/alembic
    echo "SUCCESS: Skeleton created. Remember to update config/alembic/env.py target_metadata!"
    exit 0
fi

HAS_VERSIONS=false
if find config/alembic/versions -maxdepth 1 -type f -name '*.py' -print -quit 2>/dev/null | grep -q .; then
    HAS_VERSIONS=true
fi

if [ "$HAS_VERSIONS" = false ]; then
    if [ "$ALLOW_AUTOGENERATE" = "true" ]; then
        echo "No migration files found. [DEV MODE] Generating the initial schema revision..."
        alembic -c config/alembic/alembic.ini revision --autogenerate -m "baseline_schema"
    else
        echo "CRITICAL ERROR: No migration scripts found under config/alembic/versions/!"
        echo "For production stability, you must run autogenerate locally, review the file, and commit it to git."
        exit 1
    fi
fi

echo "Applying outstanding shop database migrations..."
alembic -c config/alembic/alembic.ini upgrade head

echo "App database migrations completed successfully!"
