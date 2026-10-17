#!/usr/bin/env bash
set -euo pipefail

: "${ADMIN_EMAIL:?ADMIN_EMAIL must be set and non-empty}"
: "${ADMIN_FIRSTNAME:?ADMIN_FIRSTNAME must be set and non-empty}"
: "${ADMIN_LASTNAME:?ADMIN_LASTNAME must be set and non-empty}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD must be set and non-empty}"
: "${ADMIN_USERNAME:?ADMIN_USERNAME must be set and non-empty}"
: "${SNOWFLAKE_USER:?SNOWFLAKE_USER must be set and non-empty}"
: "${SNOWFLAKE_PASSWORD:?SNOWFLAKE_PASSWORD must be set and non-empty}"
: "${SNOWFLAKE_ACCOUNT:?SNOWFLAKE_ACCOUNT must be set and non-empty}"
: "${SNOWFLAKE_DATABASE:?SNOWFLAKE_DATABASE must be set and non-empty}"
: "${SNOWFLAKE_SCHEMA:?SNOWFLAKE_SCHEMA must be set and non-empty}"
: "${SNOWFLAKE_WAREHOUSE:?SNOWFLAKE_WAREHOUSE must be set and non-empty}"

urlencode() {
    local LC_ALL=C
    local input=$1 output= char code

    for ((i = 0; i < ${#input}; i++)); do
        char=${input:i:1}
        case "$char" in
            [a-zA-Z0-9._~-]) output+=$char ;;
            *)
                printf -v code '%d' "'$char"
                printf -v char '%%%02X' "$code"
                output+=$char
                ;;
        esac
    done

    printf '%s' "$output"
}

# Run migrations and setup
superset db upgrade
superset fab create-admin             \
    --email "$ADMIN_EMAIL"            \
    --firstname "$ADMIN_FIRSTNAME"    \
    --lastname "$ADMIN_LASTNAME"      \
    --password "$ADMIN_PASSWORD"      \
    --username "$ADMIN_USERNAME"      \
    || true
superset init

python /app/init-superset.py

snowflake_uri="snowflake://$(urlencode "$SNOWFLAKE_USER"):$(urlencode "$SNOWFLAKE_PASSWORD")@$(urlencode "$SNOWFLAKE_ACCOUNT")/$(urlencode "$SNOWFLAKE_DATABASE")?schema=$(urlencode "$SNOWFLAKE_SCHEMA")&warehouse=$(urlencode "$SNOWFLAKE_WAREHOUSE")"
superset set-database-uri --database_name DWH_SNOWFLAKE --uri "$snowflake_uri"

exec /usr/bin/run-server.sh