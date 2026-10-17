#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
START_DATE="${1:-2026-09-27}"
END_DATE="${2:-2026-10-10}"

if (( $# > 2 )); then
    echo "Usage: $0 [START_DATE [END_DATE]]" >&2
    exit 2
fi

exec docker compose \
    --project-directory "$PROJECT_ROOT" \
    -f "$PROJECT_ROOT/deploy/docker-compose.yml" \
    -f "$PROJECT_ROOT/docker-compose.override.yml" \
    exec -T airflow-api-server python - "$START_DATE" "$END_DATE" <<'PY'
import json
import os
import sys
from datetime import date, datetime, time
from pathlib import Path
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen

dag_id = "ecommerce_sales_etl_pipeline"
start_date = date.fromisoformat(sys.argv[1])
end_date = date.fromisoformat(sys.argv[2])
if end_date < start_date:
    raise SystemExit("END_DATE must be on or after START_DATE.")

users = os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"]
username = users.split(",", 1)[0].split(":", 1)[0]
passwords = json.loads(
    Path("/opt/airflow/simple_auth_manager_passwords.json.generated").read_text()
)
base_url = "http://localhost:8080"

def request(path, method="GET", body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(f"{base_url}{path}", data=data, headers=headers, method=method)
    with urlopen(req) as response:
        return json.load(response)

token = request("/auth/token", "POST", {
    "username": username,
    "password": passwords[username],
})["access_token"]

backfills = request(
    f"/api/v2/backfills?{urlencode({'dag_id': dag_id, 'limit': 100, 'offset': 0})}",
    token=token,
)["backfills"]
for backfill in backfills:
    if backfill["completed_at"] is not None:
        continue
    backfill_start = date.fromisoformat(backfill["from_date"][:10])
    backfill_end = date.fromisoformat(backfill["to_date"][:10])
    if backfill_start <= end_date and backfill_end >= start_date:
        request(f"/api/v2/backfills/{backfill['id']}/cancel", "PUT", token=token)
        print(f"Cancelled backfill {backfill['id']} ({backfill['from_date']} .. {backfill['to_date']}).")

start = datetime.combine(start_date, time.min).isoformat() + "Z"
end = datetime.combine(end_date, time.max).isoformat() + "Z"
offset = 0
active_runs = []
while True:
    params = urlencode({
        "logical_date_gte": start,
        "logical_date_lte": end,
        "limit": 100,
        "offset": offset,
    })
    result = request(
        f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns?{params}", token=token
    )
    runs = result["dag_runs"]
    active_runs.extend(
        run for run in runs if run["state"] in {"queued", "running"}
    )
    if offset + len(runs) >= result["total_entries"]:
        break
    offset += len(runs)

for run in active_runs:
    request(
        f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run['dag_run_id'], safe='')}",
        "PATCH",
        {
            "state": "failed",
            "note": f"Stopped by stop_backfill.sh for {start_date} through {end_date}.",
        },
        token,
    )
    print(f"Stopped {run['state']} run {run['dag_run_id']}.")

if not active_runs:
    print(f"No queued or running DAG runs found from {start_date} through {end_date}.")
PY
