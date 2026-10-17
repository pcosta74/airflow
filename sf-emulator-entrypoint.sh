#!/usr/bin/env bash
set -e

export DB_FILE="/var/lib/localstack/snowflake.db"
export INIT_SCRIPT="/docker-entrypoint-initaws.d/init-datawarehouse.sf.sql"
export PORT=$1

datetime() {
    date +"%Y/%m/%d %H:%M:%S"
}

shutdown() {
    exit_code=${1:-0}
    echo "[$(datetime)] Catching shutdown signal! Stopping emulator..."
    trap - SIGTERM SIGINT
    for pid in "${NGINX_PID:-}" "${PROXY_PID:-}" "${EMULATOR_PID:-}"; do
        if [ -n "$pid" ]; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    for pid in "${NGINX_PID:-}" "${PROXY_PID:-}" "${EMULATOR_PID:-}"; do
        if [ -n "$pid" ]; then
            wait "$pid" 2>/dev/null || true
        fi
    done

    echo "[$(datetime)] Emulator services stopped. Exiting."
    exit "$exit_code"
}

trace() {
    echo -e "\e[2m$1\e[0m"
}

info() {
    echo -e "\e[34m$(datetime) INFO: $1\e[0m"
}

error() {
    echo -e "\e[31m$(datetime) ERROR: $1\e[0m"
    shutdown 1
}

trap 'shutdown' SIGTERM SIGINT

info "starting Snowflake Emulator on port $PORT..."
/app/snowflake-emulator -dbpath "$DB_FILE" 2>&1 >/dev/null &
EMULATOR_PID=$!

curl --retry 10 --retry-delay 5 --retry-connrefused -s http://localhost:${PORT}/ > /dev/null
if [ $? -eq 0 ]; then
    info "Snowflake emulator running on port $PORT."
else
    error "Snowflake emulator failed to start."
    exit 1
fi

info "Starting the Snowflake gzip adapter on port 4567..."
python3 /app/snowflake_gzip_proxy.py &
PROXY_PID=$!
curl --retry 10 --retry-delay 1 --retry-connrefused -fsS http://127.0.0.1:4567/healthz > /dev/null
if [ $? -ne 0 ]; then
    error "Snowflake gzip adapter failed to start."
    exit 1
fi

nginx -t
info "Starting NGINX reverse proxy on port 443..."
nginx -g "daemon off;" &
NGINX_PID=$!

if [ -f "$INIT_SCRIPT" ]; then
    info "Translating and executing Snowflake SQL..."
    SQL_CONTENT=$(cat "$INIT_SCRIPT" | \
                envsubst | \
                sed '/^[[:space:]]*--/d' | \
                sed 's/--.*//g' | \
                python3 -c \
                    'import sys; print(sys.stdin.read().replace(sys.argv[1] + ".", ""))' \
                    "$SNOWFLAKE_DATABASE")
    SQL_CONTENT="SET GLOBAL order_by_non_integer_literal = true;${SQL_CONTENT}"
    trace "$SQL_CONTENT"
    echo

    while IFS= read -r -d ';' statement; do
        if [[ -z "${statement//[[:space:]]/}" ]]; then
            continue
        fi

        REQUEST_BODY=$(printf '%s' "$statement" | \
            python3 -c 'import json, sys; print(json.dumps({"statement": sys.stdin.read()}))')
        RESPONSE=$(curl -sS -X POST http://localhost:4566/api/v2/statements \
                        -H "Content-Type: application/json" \
                        -d "$REQUEST_BODY")

        if ! printf '%s' "$RESPONSE" | \
            python3 -c 'import json, sys; sys.exit(json.load(sys.stdin).get("sqlState") != "00000")'; then
            error "SQL execution failed for statement: $statement. Details: $RESPONSE"
        fi
    done < <(printf '%s;' "$SQL_CONTENT")

    info "Database initialization complete."
fi

if wait -n "$EMULATOR_PID" "$PROXY_PID" "$NGINX_PID"; then
    exit_code=0
else
    exit_code=$?
fi
shutdown "$exit_code"
