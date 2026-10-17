# E-commerce Data Platform

## Executive summary

An Airflow 3 and Celery project for extracting e-commerce order and refund data,
validating product identities, and producing analytics tables. The daily ELT
uses PostgreSQL, Snappy-compressed Parquet extracts, S3-compatible object
storage, and Snowflake-compatible warehouse interfaces. The platform also
includes anomaly detection, semantic product matching, quarantine controls,
and Superset dashboard definitions.

The repository includes local/demo substitutes for the commerce source, PIM,
object storage, and warehouse. These are identified in
[Surrounding systems](#surrounding-systems); they are not production vendor
integrations.

## Table of contents

- [ELT](#elt)
- [Machine learning](#machine-learning)
- [Surrounding systems](#surrounding-systems)
- [Installation and operation](#installation-and-operation)
- [Configuration](#configuration)

## ELT

### Daily pipeline

The `ecommerce_sales_etl_pipeline` DAG runs daily with `catchup=False` and one
active run at a time. Its stages are:

1. Extract changed orders and refund events from PostgreSQL as Snappy Parquet,
   and read the product catalog from the PIM endpoint and export a Parquet
   snapshot.
2. Upload those files to the S3-compatible object store.
3. Airflow workers download and read the Parquet files, then insert order and
   refund JSON payloads into Bronze `JSON` tables and catalog records into the
   Silver dimension. Local Snowflake emulator loads use row inserts; no
   Snowflake-side S3 stage or Parquet `COPY` is used.
4. Maintain the Silver Type 2 product dimension and infer, score, and validate
   orders.
5. Write valid order facts and quarantined records to separate Silver tables;
   transform refunds into Silver rows.
6. Check the quarantine rate before running Gold loads.
7. Refresh Gold product-sales, customer-RFM, estimated-tax, and fulfillment
   tables.

Order extraction uses the half-open UTC interval for `created_at`; refund
extraction uses the same interval for refund `created_at`. A late-arriving
order update remains in its original creation-date partition.
`catchup=False` does not schedule historical intervals.

### Warehouse layers and data contracts

The warehouse schema is created by
[`deploy/init-datawarehouse.sf.sql`](deploy/init-datawarehouse.sf.sql).
The bootstrap uses `CREATE TABLE IF NOT EXISTS`; it does not migrate tables in
an existing persistent warehouse. An existing warehouse with older Silver
tables needs a separate migration before running the current ETL.

| Layer | Tables and purpose |
|---|---|
| Bronze | Raw order and refund payloads, retained in `JSON` with ingestion time. |
| Silver | Order facts, quarantined orders, refunds, and the Type 2 product dimension. |
| Gold | `product_sales_summary`, `customer_rfm_metrics`, `daily_tax_ledger`, and `order_fulfillment_kpis`. |

Silver `orders` stores transaction facts and a `product_key` for the Type 2
product version valid at order creation time. Canonical product attributes are
read from the dimension rather than copied into each order row.
`orders_quarantine` retains the reported SKU and description for investigation,
along with a nullable product key and review status. Bronze remains the record
of the source payload.

The order price is the transaction price. The product dimension's price is the
catalog reference price; the pipeline does not substitute it for transaction
price. Unknown products and orders without a matching historical dimension
version do not receive a product key.

Gold loads select the latest order revision and aggregate completed,
non-anomalous sales. Customer RFM is grouped by customer and currency; missing
customers are represented as unattributed and missing currencies as `UNKNOWN`.
The tax table uses `ESTIMATED_VAT_RATE` and is an estimate, not statutory tax
calculation. Fulfillment metrics count completions in the reporting interval;
shipment/backlog events are not modeled.

### Dashboard assets

[`deploy/superset_dashboards.zip`](deploy/superset_dashboards.zip) is a
versioned Superset dashboard export containing the dashboard, four charts,
four datasets, and their database definition. Superset imports the bundle
during startup, then configures the `DWH_SNOWFLAKE` connection from the
`SNOWFLAKE_*` environment variables. Its password is encrypted in Superset's
metadata database and is not stored in the dashboard bundle. The export's
connection options target the local Snowflake-compatible emulator. Tests
check the export format, Gold table columns, and chart references.

## Machine learning

### Order anomaly detection

The `ml_weekly_train_anomaly_gatekeeper` DAG trains an Isolation Forest from a
sample of completed Shop orders using `price` and `qty`. It writes an ONNX
model to `ML_MODEL_DIR`; the training parameters include the sample percentage
and the Median Absolute Deviation multiplier used for the score threshold.

The daily pipeline loads the ONNX model and scores orders. Numeric anomalies
are quarantined. The quarantine audit also counts unresolved identities and
records requiring product review; the DAG fails when the combined quarantine
rate exceeds `MAX_ALLOWED_ANOMALY_RATE` (default `5.0` percent).

Train the anomaly model before the first daily ELT run. The weekly schedule
handles subsequent training runs. The training and inference tasks use the
`ml_workers` Celery queue.

### Product identity and description validation

The adapter validates the PIM catalog and maps its `sku`, `description`,
`category`, and `price` fields into the product dimension contract. SKU is the
catalog identity. Fixture-only seller wording is generated separately from the
PIM catalog.

For a known SKU, a SentenceTransformer compares the order's reported
description with that SKU's canonical description. Scores below
`SEMANTIC_MIN_SIMILARITY` (default `0.55`) require review and quarantine.
Unknown SKUs may receive a nearest-description suggestion for review; the
pipeline does not automatically change the reported SKU. Source descriptions
remain available in Bronze and quarantine.

### Fraud model status

`ml_weekly_train_fraud_gatekeeper` is a separate training prototype. It trains
an ONNX Random Forest using historical order features and accepts a configurable
lookback period and risk threshold. The daily ELT does not invoke this model or
quarantine orders based on its scores. The fixture data does not supply useful
fraud-positive labels for validating it.

## Surrounding systems

| System | Repository implementation | Real-world counterpart or example |
|---|---|---|
| Commerce source | Local PostgreSQL Shop schema with fixture seeding. | An operational commerce/order-management database or commerce platform API. Fixture data is synthetic and must not be treated as production orders. |
| Product information | Read-only Nginx service serving `static/product_catalog.json` at `/products`. | A governed PIM or ERP integration, such as an Akeneo or SAP product API. Authentication, catalog editing, and vendor-specific API behavior are not implemented here. |
| Object storage | Silo service exposes an S3-compatible landing bucket; the DAG exchanges Snappy-compressed Parquet files through this interface. | Amazon S3 or another S3-compatible object store. Validate credentials, access policies, lifecycle, encryption, and network configuration for deployment. |
| Analytical warehouse | Local Snowflake emulator, initialized from the Snowflake SQL bootstrap. The emulator does not support external stages or Parquet `COPY` loading, so the production-style staged ingestion path is not supported locally. | A Snowflake account. Use this for production deployment; the emulator is for local development and does not establish parity for all Snowflake features or operational behavior. |
| Orchestration | Apache Airflow 3 with CeleryExecutor, Redis broker, and queue-specific workers. | Airflow and Celery are used directly; local configuration provides a deployable example, not a managed Airflow service. |
| BI | Apache Superset with committed dataset, chart, and dashboard definitions. | Superset is used directly. Configure its production metadata database, secret key, authentication, and warehouse connection for deployment. |

Local deployment also includes separate PostgreSQL services for Shop and
Airflow metadata. The optional root Compose override combines those roles for
local development and adds host ports for local access.

## Installation and operation

### Requirements

- Docker Engine or Docker Desktop with Docker Compose.
- Python 3.12 for local development and tests.
- A project-root `.env` file configured for the selected Compose deployment.

Create the configuration file:

```bash
cp .env.example .env
```

Set the placeholders to valid local values. Keep credentials and secrets in
`.env`; do not commit them. Compose requires Superset administrator values and
`SUPERSET_SECRET_KEY` in addition to database, Airflow, object-storage, and
warehouse settings.

### Start the local stack

The root Compose override configures the local development setup. From the
repository root:

```bash
docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  -f docker-compose.override.yml \
  up -d --build
```

The `airflow-init` service migrates Airflow metadata and applies the Shop
Alembic migrations. Local interfaces:

- Airflow: <http://localhost:8080>
- Superset: <http://localhost:8088>
- PIM: <http://localhost:9999/products>
- Snowflake emulator API: <http://localhost:4566>
- S3-compatible object-store console: <http://localhost:9001>

### Seed and run demo data

The seeder replaces existing Shop demo orders and refunds. Do not run it against
data that must be preserved. Run from the API-server container:

```bash
docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  -f docker-compose.override.yml \
  exec airflow-api-server \
  python tests/fixtures/ecom_seeder.py --freeze

docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  -f docker-compose.override.yml \
  exec airflow-api-server \
  python tests/fixtures/ecom_seeder.py
```

Before the first daily ELT run, trigger `ml_weekly_train_anomaly_gatekeeper`
from the Airflow UI and wait for it to finish. Then trigger
`ecommerce_sales_etl_pipeline`. The DAG has `catchup=False`; to process a
historical fixture date range, create a backfill:

```bash
./scripts/start_backfill.sh YYYY-MM-DD YYYY-MM-DD
```

The date arguments are inclusive. The fraud training DAG is optional and is
not a dependency of the daily ELT.

### Observe and stop the local stack

```bash
docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  -f docker-compose.override.yml \
  ps

docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  -f docker-compose.override.yml \
  logs -f airflow-scheduler worker-api
```

Stop containers while retaining persistent volumes:

```bash
docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  -f docker-compose.override.yml \
  down
```

Use `down -v` only when intentionally deleting persistent database and Superset
volumes.

### Base Compose deployment

The base file runs separate PostgreSQL services for Airflow metadata and Shop
data. Configure `.env` on the Docker host, then start without the local override:

```bash
docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  up -d --build
```

The data-lake, log, and project directories are host bind mounts. Provide
sufficient disk space and suitable permissions. Restrict exposed service ports
with host firewall rules; do not expose database ports publicly. Stop the base
stack without deleting persistent volumes:

```bash
docker compose --project-directory . \
  -f deploy/docker-compose.yml \
  down
```

### Backfill controls

Create a backfill for an inclusive date range:

```bash
./scripts/start_backfill.sh YYYY-MM-DD YYYY-MM-DD
```

Stop queued or running DAG runs in a selected range:

```bash
./scripts/stop_backfill.sh YYYY-MM-DD YYYY-MM-DD
```

The stop script cancels overlapping active backfills and marks queued or running
DAG runs in the range as failed. Run records are retained.

## Configuration

| Variable | Purpose |
|---|---|
| `DATALAKE_ROOT_PATH` | Container path for the shared data-lake mount. |
| `EXTRACT_CHUNK_SIZE` | Extraction chunk size. |
| `MAX_ALLOWED_ANOMALY_RATE` | Maximum allowed anomaly/review quarantine percentage; default `5.0`. |
| `ESTIMATED_VAT_RATE` | Demonstration VAT fraction; default `0.23`. |
| `ML_MODEL_DIR` | Directory used for ONNX model artifacts. |
| `ANOMALY_ISOLATION_MODEL` | Anomaly model filename. |
| `SEMANTIC_MODEL` | SentenceTransformer model ID; default `all-MiniLM-L6-v2`. |
| `SEMANTIC_MIN_SIMILARITY` | Minimum semantic similarity score; default `0.55`. |
| `FRAUD_DETECTION_MODEL` | Fraud prototype model filename. |
| `FRAUD_RISK_THRESHOLD` | Default fraud prototype threshold; default `0.75`. |
| `PIM_API_URL` | PIM service base URL; default `http://pim` inside Compose. |

The Airflow tasks also require Airflow connections named `DWH_SNOWFLAKE` and
`SILO_S3` (and `shop_postgres` for model training). Configure these in Airflow
or provide their `AIRFLOW_CONN_*` environment variables. For the local stack,
`AIRFLOW_CONN_DWH_SNOWFLAKE` must target the `snowflake` Compose service on
port 443, with `public` as the URI path/schema and `database=memory` as a query
parameter. Do not put both database and schema in the URI path: Airflow parses
the whole path as the schema. The local S3 connection must use the `silo-s3`
service on port 9000 with path-style access. The credentials must match the
local service configuration.
