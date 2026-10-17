import io
import json
import os
import tempfile
from datetime import datetime, timedelta
from hashlib import sha256
from logging import getLogger
from pathlib import Path
from textwrap import dedent
from urllib.request import urlopen

import duckdb
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task

from data_infra.integrations.pim.catalog import product_catalog_to_dimension_records
from data_infra.integrations.snowflake import create_snowflake_hook, table_name

logger = getLogger(__name__)

LANDING_BUCKET = os.getenv("MINIO_BUCKET") or ValueError("set MINIO_BUCKET is .env")
SHOP_DB_URL = os.getenv("SHOP_DATABASE_URL") or ValueError(
    "set SHOP_DATABASE_URL is .env"
)
PIM_API_URL = os.getenv("PIM_API_URL") or ValueError("set PIM_API_URL is .env")

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE") or ValueError(
    "set SNOWFLAKE_DATABASE is .env"
)
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") or ValueError(
    "set SNOWFLAKE_SCHEMA is .env"
)


def run_id_partition(run_id: str | None) -> str:
    """Return a filesystem- and SQL-safe partition prefix for an Airflow run."""
    if not run_id:
        return ""
    run_id_hash = sha256(run_id.encode("utf-8")).hexdigest()
    return f"run_{run_id_hash}_"


def _get_extraction_interval(ds: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(ds, "%Y-%m-%d")
    return start, start + timedelta(days=1)


def _ingest_from_s3(
    s3_key: str, target_table: str, snowflake_hook, ingestion_date: str
) -> int:
    """Read a Parquet object from S3 and insert its records as JSON."""
    s3_hook = S3Hook(aws_conn_id="SILO_S3")
    s3_object = s3_hook.get_key(key=s3_key, bucket_name=LANDING_BUCKET)
    if s3_object is None:
        raise FileNotFoundError(f"S3 object not found: s3://{LANDING_BUCKET}/{s3_key}")

    response = s3_object.get()
    with response["Body"] as body:
        parquet_bytes = body.read()

    parquet_df = pd.read_parquet(io.BytesIO(parquet_bytes))
    if len(parquet_df) == 0:
        return 0

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            insert_query = (
                f"INSERT INTO {target_table} (raw_data, ingested_at) "
                "VALUES (%s::JSON, %s::TIMESTAMP)"
            )
            for row in parquet_df.to_json(
                orient="records", lines=True, date_format="iso"
            ).splitlines():
                cursor.execute(insert_query, (row, ingestion_date))

    return len(parquet_df)


@task(queue="api_workers")
def extract_orders(
    ds: str,
    run_id: str | None = None,
) -> str:
    """Extract orders changed on the logical date using DuckDB and upload to S3."""
    start, end = _get_extraction_interval(ds)
    load_date = start.date().isoformat()
    partition_prefix = run_id_partition(run_id)
    s3_key = f"ecom_shop/orders/load_date={load_date}/{partition_prefix}data.parquet"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_parquet_path = Path(tmp_dir) / "orders.parquet"

        logger.info(
            f"Streaming data from Postgres into local tmp file: {tmp_parquet_path}\n"
            f"Extracting orders updated between {start} and {end}..."
        )

        with duckdb.connect(":memory:") as conn:
            conn.execute("INSTALL postgres; LOAD postgres;")

            query = dedent(f"""
                COPY (
                    SELECT 
                        order_id, customer_id, sku, product_description, 
                        price::DOUBLE as price, currency_code, qty, status, 
                        created_at, completed_at, updated_at, revision
                    FROM postgres_scan('{SHOP_DB_URL}', 'public', 'orders')
                    WHERE created_at >= '{start}' AND created_at < '{end}'
                    ORDER BY order_id
                ) TO '{tmp_parquet_path}' (
                    FORMAT PARQUET,
                    COMPRESSION SNAPPY
                );
            """).strip()
            conn.execute(query)

        tmp_parquet_size = tmp_parquet_path.stat().st_size
        logger.info(
            f"Uploading {tmp_parquet_path} ({tmp_parquet_size} bytes) "
            f"to s3://{LANDING_BUCKET}/{s3_key}..."
        )

        s3_hook = S3Hook(aws_conn_id="SILO_S3")
        if not s3_hook.check_for_bucket(LANDING_BUCKET):
            s3_hook.create_bucket(LANDING_BUCKET)

        s3_hook.load_file(
            filename=str(tmp_parquet_path),
            key=s3_key,
            bucket_name=LANDING_BUCKET,
            replace=True,
        )

    logger.info("Upload complete!")
    return s3_key


@task(queue="api_workers")
def extract_refunds(
    ds: str,
    run_id: str | None = None,
) -> str:
    """Extract refunds changed on the logical date using DuckDB and upload to S3."""
    start, end = _get_extraction_interval(ds)
    load_date = start.date().isoformat()

    partition_prefix = run_id_partition(run_id)
    s3_key = f"ecom_shop/refunds/load_date={load_date}/{partition_prefix}data.parquet"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_parquet_path = Path(tmp_dir) / "refunds.parquet"

        with duckdb.connect(":memory:") as conn:
            conn.execute("INSTALL postgres; LOAD postgres;")

            export_query = dedent(f"""
                COPY (
                    SELECT 
                        refund_id, order_id, refund_amount::DOUBLE as refund_amount, 
                        currency_code, created_at
                    FROM postgres_scan('{SHOP_DB_URL}', 'public', 'refunds')
                    WHERE created_at >= '{start}' AND created_at < '{end}'
                    ORDER BY refund_id
                ) TO '{tmp_parquet_path}' (
                    FORMAT PARQUET,
                    COMPRESSION SNAPPY
                );
            """).strip()
            conn.execute(export_query)

            tmp_parquet_size = tmp_parquet_path.stat().st_size
            logger.info(
                f"Uploading {tmp_parquet_path} ({tmp_parquet_size} bytes) "
                f"to s3://{LANDING_BUCKET}/{s3_key}..."
            )

            s3_hook = S3Hook(aws_conn_id="SILO_S3")
            if not s3_hook.check_for_bucket(LANDING_BUCKET):
                s3_hook.create_bucket(LANDING_BUCKET)

            s3_hook.load_file(
                filename=str(tmp_parquet_path),
                key=s3_key,
                bucket_name=LANDING_BUCKET,
                replace=True,
            )

    return s3_key


@task(queue="api_workers")
def extract_catalog_dimension(ds: str) -> str:
    """Read the PIM API catalog, export Snappy Parquet, and upload it to S3."""

    s3_key = f"ecom_shop/load_date={ds}/catalog_snapshot.parquet"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_parquet_path = Path(tmp_dir) / "catalog.parquet"

        with urlopen(f"{PIM_API_URL.rstrip('/')}/products", timeout=30) as response:
            catalog_products = json.load(response)
        catalog_df = pd.DataFrame(product_catalog_to_dimension_records(catalog_products))
        with duckdb.connect(":memory:") as conn:
            conn.register("catalog_df", catalog_df)
            query = dedent(f"""
                COPY catalog_df TO '{tmp_parquet_path}'
                (FORMAT PARQUET, COMPRESSION SNAPPY);
            """).strip()
            conn.execute(query)

        s3_hook = S3Hook(aws_conn_id="SILO_S3")
        if not s3_hook.check_for_bucket(LANDING_BUCKET):
            s3_hook.create_bucket(LANDING_BUCKET)
        s3_hook.load_file(
            filename=str(tmp_parquet_path),
            key=s3_key,
            bucket_name=LANDING_BUCKET,
            replace=True,
        )

    return s3_key


@task(queue="utility_workers")
def ingest_orders_to_bronze(s3_key: str, ds: str) -> dict:
    """Load Parquet from S3 into the emulator's Bronze orders table."""
    logger.info(f"Executing Snowflake bulk copy for file: s3://{LANDING_BUCKET}/{s3_key}")

    snowflake_hook = create_snowflake_hook(
        snowflake_conn_id="DWH_SNOWFLAKE",
        insecure_mode=True,
    )
    target_schema = f"{SNOWFLAKE_SCHEMA}_bronze"
    target_table = table_name(SNOWFLAKE_DATABASE, target_schema, "orders")

    # The emulator does not support Parquet COPY or external S3 stages.
    # Keep the Snowflake production path commented; local ingestion inserts JSON rows.
    # query = dedent(f"""
    #     COPY INTO {target_table} (raw_data)
    #     FROM (
    #         SELECT OBJECT_CONSTRUCT(*)
    #         FROM @{SNOWFLAKE_DATABASE}.{target_schema}.s3_landing/{s3_key}
    #     )
    #     FILE_FORMAT = (
    #         FORMAT_NAME = '{SNOWFLAKE_DATABASE}.{target_schema}.snappy_parquet_format'
    #     )
    #     ON_ERROR = 'CONTINUE';
    # """).strip()
    # copy_results = snowflake_hook.get_first(query)
    # rows_ingested = int(copy_results[3]) if copy_results and len(copy_results) > 3 else 0
    rows_ingested = _ingest_from_s3(s3_key, target_table, snowflake_hook, ds)

    logger.info(
        f"Successfully ingested {rows_ingested} rows from {s3_key} into Snowflake Bronze!"
    )

    return {
        "target_table": target_table,
        "partition_date": ds,
        "rows_ingested": rows_ingested,
        "status": "SUCCESS" if rows_ingested > 0 else "EMPTY",
    }


@task(queue="utility_workers")
def ingest_refunds_to_bronze(s3_key: str, ds: str) -> dict:
    """Load Parquet from S3 into the emulator's Bronze refunds table."""
    logger.info(f"Executing Snowflake bulk copy for file: s3://{LANDING_BUCKET}/{s3_key}")

    snowflake_hook = create_snowflake_hook(
        snowflake_conn_id="DWH_SNOWFLAKE",
        insecure_mode=True,
    )
    target_schema = f"{SNOWFLAKE_SCHEMA}_bronze"
    target_table = table_name(SNOWFLAKE_DATABASE, target_schema, "refunds")

    # The emulator does not support Parquet COPY or external S3 stages.
    # Keep the Snowflake production path commented; local ingestion inserts JSON rows.
    # query = dedent(f"""
    #     COPY INTO {target_table} (raw_data)
    #     FROM (
    #         SELECT OBJECT_CONSTRUCT(*)
    #         FROM @{SNOWFLAKE_DATABASE}.{target_schema}.s3_landing/{s3_key}
    #     )
    #     FILE_FORMAT = (
    #         FORMAT_NAME = '{SNOWFLAKE_DATABASE}.{target_schema}.snappy_parquet_format'
    #     )
    #     ON_ERROR = 'CONTINUE';
    # """).strip()
    # copy_results = snowflake_hook.get_first(query)
    # rows_ingested = int(copy_results[3]) if copy_results and len(copy_results) > 3 else 0
    rows_ingested = _ingest_from_s3(s3_key, target_table, snowflake_hook, ds)

    logger.info(
        f"Successfully ingested {rows_ingested} refund records from {s3_key} "
        "into Snowflake Bronze!"
    )

    return {
        "target_table": target_table,
        "partition_date": ds,
        "rows_ingested": rows_ingested,
        "status": "SUCCESS" if rows_ingested > 0 else "EMPTY",
    }
