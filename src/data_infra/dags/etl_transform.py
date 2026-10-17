import hashlib
import io
import json
import logging
import os
from pathlib import Path
from textwrap import dedent

import onnxruntime as ort
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task
from airflow.sdk.exceptions import AirflowFailException

from data_infra.integrations.snowflake import create_snowflake_hook, table_name

logger = logging.getLogger(__name__)

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE") or ValueError(
    "set SNOWFLAKE_DATABASE is .env"
)
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") or ValueError(
    "set SNOWFLAKE_SCHEMA is .env"
)
LANDING_BUCKET = os.getenv("MINIO_BUCKET") or ValueError("set MINIO_BUCKET is .env")

ML_MODEL_DIR = os.getenv("ML_MODEL_DIR", "/opt/airflow/models")
ML_MODEL_NAME = os.getenv("ANOMALY_ISOLATION_MODEL") or ValueError(
    "set ANOMALY_ISOLATION_MODEL is .env"
)

CLEAN_TARGET_FIELDS = (
    "order_id",
    "customer_id",
    "product_key",
    "price",
    "currency_code",
    "quantity",
    "status",
    "created_at",
    "completed_at",
    "updated_at",
    "revision",
    "total_value",
    "is_anomaly",
    "match_status",
    "description_status",
    "suggestion_similarity",
)

QUARANTINE_TARGET_FIELDS = (
    "order_id",
    "customer_id",
    "sku",
    "product_description",
    "product_key",
    "price",
    "currency_code",
    "quantity",
    "status",
    "created_at",
    "completed_at",
    "updated_at",
    "revision",
    "total_value",
    "is_anomaly",
    "needs_review",
    "match_status",
    "description_status",
    "suggestion_similarity",
    "suggested_sku",
)


def _delete_reprocessed_order_versions_sql(
    source_table: str, silver_schema: str, ds: str
) -> str:
    order_id = _raw_json_field("order_id", "BIGINT")
    clean_orders_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "orders"
    )
    quarantine_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "orders_quarantine"
    )
    affected_order_ids = f"""
        SELECT DISTINCT {order_id}
        FROM {source_table}
        WHERE DATE(ingested_at) = '{ds}'::DATE
    """
    return dedent(f"""
        DELETE FROM {clean_orders_table}
        WHERE order_id IN ({affected_order_ids});
        DELETE FROM {quarantine_table}
        WHERE order_id IN ({affected_order_ids});
    """).strip()


def _is_snowflake_emulator() -> bool:
    return os.getenv("SNOWFLAKE_EMULATOR", "").lower() in {"1", "true", "yes"}


def _insert_dataframe_rows(
    snowflake_hook, target_table: str, dataframe: pd.DataFrame
) -> None:
    columns = tuple(dataframe.columns)
    placeholders = ", ".join("%s" for _ in columns)
    insert_query = (
        f"INSERT INTO {target_table} ({', '.join(columns)}) "
        f"VALUES ({placeholders})"
    )

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for row in dataframe.itertuples(index=False, name=None):
                values = []
                for value in row:
                    if pd.isna(value):
                        values.append(None)
                    elif isinstance(value, pd.Timestamp):
                        values.append(value.to_pydatetime())
                    elif hasattr(value, "item"):
                        values.append(value.item())
                    else:
                        values.append(value)
                cursor.execute(insert_query, tuple(values))


def _raw_json_field(field: str, sql_type: str) -> str:
    if _is_snowflake_emulator():
        path = f"'$.{field}'"
        if sql_type == "STRING":
            return f"json_extract_string(raw_data, {path})"
        if sql_type == "TIMESTAMP":
            return f"json_extract_string(raw_data, {path})::TIMESTAMP"
        return f"json_extract(raw_data, {path})::{sql_type}"

    return f"TRY_PARSE_JSON(raw_data):{field}::{sql_type}"


def _normalize_emulator_order_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    result = dataframe.rename(columns=str.lower).copy()
    for column in ("order_id", "customer_id", "price", "qty", "revision"):
        result[column] = pd.to_numeric(result[column], errors="raise")

    for column in ("created_at", "completed_at", "updated_at"):
        values = result[column]
        epoch_milliseconds = pd.to_numeric(values, errors="coerce")
        is_epoch = epoch_milliseconds.notna()
        timestamps = pd.Series(
            pd.NaT, index=values.index, dtype="datetime64[ns, UTC]"
        )
        timestamps.loc[is_epoch] = pd.to_datetime(
            epoch_milliseconds.loc[is_epoch], unit="ms", utc=True
        )
        timestamps.loc[~is_epoch] = pd.to_datetime(
            values.loc[~is_epoch], errors="coerce", utc=True
        )
        result[column] = timestamps

    return result


def _current_product_mask(values: pd.Series) -> pd.Series:
    normalized = values.astype("string").str.strip().str.lower()
    invalid_values = normalized.notna() & ~normalized.isin({"true", "false"})
    if invalid_values.any():
        raise ValueError(
            "Product dimension contains invalid is_current values: "
            f"{normalized.loc[invalid_values].unique().tolist()}"
        )
    return normalized.eq("true").fillna(False)


def _bronze_orders_query(source_table: str, ds: str) -> str:
    if _is_snowflake_emulator():
        order_fields = (
            "order_id",
            "customer_id",
            "sku",
            "product_description",
            "price",
            "currency_code",
            "qty",
            "status",
            "created_at",
            "completed_at",
            "updated_at",
            "revision",
        )
        selected_fields = ",\n".join(
            f"{_raw_json_field(field, 'STRING')} AS {field}"
            for field in order_fields
        )
        return dedent(f"""
            SELECT {selected_fields}
            FROM {source_table}
            WHERE DATE(ingested_at) = '{ds}'::DATE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY json_extract_string(raw_data, '$.order_id'),
                    json_extract_string(raw_data, '$.revision')
                ORDER BY ingested_at DESC
            ) = 1;
        """).strip()

    return dedent(f"""
        SELECT raw_data
        FROM {source_table}
        WHERE DATE(ingested_at) = '{ds}'::DATE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_PARSE_JSON(raw_data):order_id::BIGINT,
                TRY_PARSE_JSON(raw_data):revision::INTEGER
            ORDER BY ingested_at DESC
        ) = 1;
    """).strip()


@task(queue="utility_workers")
def audit_quarantine_metrics(
    ml_output: dict, max_allowed_anomaly_rate: float = 5.0
) -> str:
    """
    Audits the newly loaded partition inside Snowflake using analytical pushdown
    across both clean and quarantined silver tables.
    """
    ds = ml_output["partition_date"]

    if ml_output["status"] == "empty":
        logger.info(
            f"Upstream status reported empty partition for {ds}. "
            "Skipping gatekeeper validation."
        )
        return ds

    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")
    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    clean_orders_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "orders"
    )
    quarantine_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "orders_quarantine"
    )

    audit_query = dedent(f"""
        WITH unified_records AS (
            SELECT is_anomaly, FALSE AS needs_review, created_at
            FROM {clean_orders_table}
            WHERE DATE(created_at) = '{ds}'::DATE
            
            UNION ALL
            
            SELECT is_anomaly, COALESCE(needs_review, TRUE) AS needs_review, created_at
            FROM {quarantine_table}
            WHERE DATE(created_at) = '{ds}'::DATE
        ),
        stats AS (
            SELECT 
                COUNT_IF(is_anomaly = TRUE) AS total_anomalies,
                COUNT_IF(is_anomaly = FALSE) AS total_valid,
                COUNT_IF(needs_review = TRUE) AS total_needs_review,
                COUNT_IF(
                    COALESCE(is_anomaly, FALSE) OR needs_review
                ) AS total_quarantined,
                COUNT_IF(is_anomaly IS NOT NULL) AS total_scored,
                COUNT(*) AS total_processed
            FROM unified_records
        )
        SELECT 
            total_anomalies, 
            total_valid, 
            total_needs_review, 
            total_quarantined,
            total_scored,
            total_processed,
            ROUND(
                COALESCE(
                    total_anomalies / NULLIF(total_scored, 0) * 100,
                    0.0
                ),
                2
            ) AS anomaly_rate
        FROM stats;
    """).strip()

    if _is_snowflake_emulator():
        clean_stats = snowflake_hook.get_first(dedent(f"""
            SELECT
                COUNT_IF(is_anomaly = TRUE),
                COUNT_IF(is_anomaly = FALSE),
                COUNT_IF(is_anomaly IS NOT NULL),
                COUNT(*)
            FROM {clean_orders_table}
            WHERE DATE(created_at) = '{ds}'::DATE
        """).strip())
        quarantine_stats = snowflake_hook.get_first(dedent(f"""
            SELECT
                COUNT_IF(is_anomaly = TRUE),
                COUNT_IF(COALESCE(needs_review, TRUE) = TRUE),
                COUNT_IF(COALESCE(is_anomaly, FALSE) OR needs_review),
                COUNT_IF(is_anomaly IS NOT NULL),
                COUNT(*)
            FROM {quarantine_table}
            WHERE DATE(created_at) = '{ds}'::DATE
        """).strip())
        clean_anomalies, total_valid, clean_scored, clean_total = (
            clean_stats or (0, 0, 0, 0)
        )
        (
            quarantined_anomalies,
            needs_review,
            quarantined_total,
            quarantine_scored,
            quarantine_count,
        ) = quarantine_stats or (0, 0, 0, 0, 0)
        clean_anomalies = int(clean_anomalies or 0)
        total_valid = int(total_valid or 0)
        clean_scored = int(clean_scored or 0)
        clean_total = int(clean_total or 0)
        quarantined_anomalies = int(quarantined_anomalies or 0)
        needs_review = int(needs_review or 0)
        quarantined_total = int(quarantined_total or 0)
        quarantine_scored = int(quarantine_scored or 0)
        quarantine_count = int(quarantine_count or 0)
        result = (
            clean_anomalies + quarantined_anomalies,
            total_valid,
            needs_review,
            clean_anomalies + quarantined_total,
            clean_scored + quarantine_scored,
            clean_total + quarantine_count,
            None,
        )
    else:
        result = snowflake_hook.get_first(audit_query)
    (
        total_anomalies,
        total_valid,
        needs_review,
        total_quarantined,
        total_scored,
        total_processed,
        anomaly_rate,
    ) = result or (0, 0, 0, 0, 0, 0, 0.0)
    total_anomalies = int(total_anomalies or 0)
    total_valid = int(total_valid or 0)
    needs_review = int(needs_review or 0)
    total_quarantined = int(total_quarantined or 0)
    total_scored = int(total_scored or 0)
    total_processed = int(total_processed or 0)
    anomaly_rate = (
        round(total_anomalies / total_scored * 100, 2)
        if total_scored
        else 0.0
    )
    quarantine_rate = (
        round(total_quarantined / total_processed * 100, 2)
        if total_processed
        else 0.0
    )

    msg = dedent(f"""
        =====================================================
        === QUARANTINE AUDIT SUMMARY (SNOWFLAKE PUSHDOWN) ===
        =====================================================
        Total Processed: {total_processed:,} rows
        Numerically Scored: {total_scored:,} completed orders
        Total Valid:     {total_valid:,} rows
        Total Quarantined: {total_quarantined:,} rows
        Needs Review:    {needs_review:,} rows
        Isolated Anomalies: {total_anomalies:,} rows
        Numerical Anomaly Rate: {anomaly_rate:.2f}%
        Total Quarantine Rate: {quarantine_rate:.2f}%
        =====================================================
    """).strip()
    logger.info(msg)

    if anomaly_rate > max_allowed_anomaly_rate:
        msg = (
            f"CRITICAL PIPELINE FAILURE: Anomaly rate ({anomaly_rate:.2f}%) "
            f"breaks safety boundary threshold of {max_allowed_anomaly_rate}%!"
        )
        raise AirflowFailException(msg)

    return ds


@task(queue="ml_workers")
def ml_clean_orders(bronze_orders_metadata: dict, run_id: str | None = None) -> dict:
    """
    Streams raw variant records directly from Snowflake Bronze based on upstream ingestion
    metadata, executes ML inference in worker RAM, and bulk uploads to Snowflake
    Silver schemas.
    """

    from data_infra.ml.cleansing import (
        assign_product_dimension_keys,
        heal_chunk_metadata,
        score_chunk_anomalies,
    )

    ds = bronze_orders_metadata["partition_date"]
    source_table = bronze_orders_metadata["target_table"]

    if bronze_orders_metadata["status"] == "EMPTY":
        logger.warning(
            f"Upstream batch reported 0 records ingested for {ds}. "
            "Short-circuiting ML pipeline."
        )
        return {"status": "empty", "anomaly_rate": 0.0, "partition_date": ds}

    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")
    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    dimension_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "dim_products"
    )
    clean_orders_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "orders"
    )
    quarantine_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "orders_quarantine"
    )

    query = dedent(f"""
        SELECT
            sku,
            product_key,
            canonical_description,
            CAST(is_current AS VARCHAR) AS is_current,
            CAST(valid_from AS VARCHAR) AS valid_from,
            CAST(valid_to AS VARCHAR) AS valid_to
        FROM {dimension_table}
    """).strip()
    dimensions_df = snowflake_hook.get_pandas_df(query)
    dimensions_df = dimensions_df.rename(columns=str.lower)
    catalog_df = dimensions_df.loc[
        _current_product_mask(dimensions_df["is_current"])
    ].copy()

    query = _bronze_orders_query(source_table, ds)
    df_chunks = snowflake_hook.get_pandas_df(query)
    df_chunks = df_chunks.rename(columns=str.lower)

    if df_chunks.empty:
        logger.warning(
            f"No records found in Snowflake Bronze table for date: {ds}. "
            "Skipping ML pipeline."
        )
        return {"status": "empty", "anomaly_rate": 0.0, "partition_date": ds}

    if _is_snowflake_emulator():
        raw_df = _normalize_emulator_order_frame(df_chunks)
    else:
        raw_records = []
        for raw_data in df_chunks["raw_data"]:
            if isinstance(raw_data, dict):
                raw_records.append(raw_data)
                continue
            if not isinstance(raw_data, str):
                raise TypeError(
                    f"Expected JSON text for Bronze raw_data, got {type(raw_data)}"
                )
            raw_records.append(json.loads(raw_data))
        raw_df = pd.json_normalize(raw_records)

    model_path = Path(ML_MODEL_DIR) / ML_MODEL_NAME
    if not model_path.exists():
        raise FileNotFoundError(
            f"Active Gatekeeper ONNX model binary missing at: {model_path}"
        )

    session = ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])
    dynamic_threshold = float(
        session.get_modelmeta().custom_metadata_map["dynamic_threshold"]
    )

    healed_chunk = heal_chunk_metadata(raw_df, catalog_df)
    healed_chunk = assign_product_dimension_keys(healed_chunk, dimensions_df)
    clean_chunk, quar_chunk = score_chunk_anomalies(
        healed_chunk, session, dynamic_threshold
    )

    total_records = len(clean_chunk) + len(quar_chunk)
    anomaly_rate = (len(quar_chunk) / total_records) * 100.0 if total_records > 0 else 0.0

    snowflake_hook.run(
        _delete_reprocessed_order_versions_sql(source_table, silver_schema, ds)
    )

    if not clean_chunk.empty:
        clean_chunk["quantity"] = clean_chunk["qty"].astype(int)
        clean_chunk["total_value"] = (
            clean_chunk["price"].astype(float) * clean_chunk["quantity"]
        ).round(2)
        clean_target = clean_chunk[list(CLEAN_TARGET_FIELDS)]

        if _is_snowflake_emulator():
            _insert_dataframe_rows(
                snowflake_hook,
                clean_orders_table,
                clean_target,
            )
        else:
            from snowflake.connector.pandas_tools import write_pandas

            write_pandas(
                conn=snowflake_hook.get_conn(),
                df=clean_target,
                table_name="ORDERS",
                database=SNOWFLAKE_DATABASE,
                schema=silver_schema,
                auto_create_table=False,
            )

    if not quar_chunk.empty:
        quar_chunk["quantity"] = quar_chunk["qty"].astype(int)
        quar_chunk["total_value"] = (
            quar_chunk["price"].astype(float) * quar_chunk["quantity"]
        ).round(2)

        quar_target = quar_chunk[list(QUARANTINE_TARGET_FIELDS)]

        if _is_snowflake_emulator():
            _insert_dataframe_rows(
                snowflake_hook,
                quarantine_table,
                quar_target,
            )
        else:
            from snowflake.connector.pandas_tools import write_pandas

            write_pandas(
                conn=snowflake_hook.get_conn(),
                df=quar_target,
                table_name="ORDERS_QUARANTINE",
                database=SNOWFLAKE_DATABASE,
                schema=silver_schema,
                auto_create_table=False,
            )

    logger.info("Successfully populated Snowflake Silver Orders Layer.")

    return {
        "status": "processed",
        "partition_date": ds,
        "clean_table": clean_orders_table,
        "quarantine_table": quarantine_table,
        "anomaly_rate": round(anomaly_rate, 2),
    }


@task(queue="utility_workers")
def transform_silver_refunds(ds: str) -> str:
    """
    Pushes data flattening down to Snowflake for refunds. Parses the raw JSON
    from Bronze directly into relational Silver schemas via high-performance SQL query.
    """
    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")
    bronze_schema = f"{SNOWFLAKE_SCHEMA}_bronze"
    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    target_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "refunds")

    source_table = table_name(SNOWFLAKE_DATABASE, bronze_schema, "refunds")
    refund_id = _raw_json_field("refund_id", "BIGINT")
    order_id = _raw_json_field("order_id", "BIGINT")
    refund_amount = _raw_json_field("refund_amount", "DOUBLE")
    currency_code = _raw_json_field("currency_code", "STRING")
    created_at = _raw_json_field("created_at", "TIMESTAMP")
    source_query = dedent(f"""
        SELECT
            {refund_id} AS refund_id,
            {order_id} AS order_id,
            {refund_amount} AS refund_amount,
            {currency_code} AS currency_code,
            {created_at} AS created_at
        FROM {source_table}
        WHERE DATE(ingested_at) = '{ds}'::DATE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {refund_id}
            ORDER BY ingested_at DESC
        ) = 1
    """).strip()

    if _is_snowflake_emulator():
        snowflake_hook.run(dedent(f"""
            DELETE FROM {target_table}
            WHERE refund_id IN (
                SELECT refund_id FROM ({source_query}) AS source
            )
        """).strip())
        snowflake_hook.run(dedent(f"""
            INSERT INTO {target_table} (
                refund_id, order_id, refund_amount, currency_code, created_at
            )
            {source_query}
        """).strip())
    else:
        query = dedent(f"""
            MERGE INTO {target_table} AS target
            USING ({source_query}) AS source
            ON target.refund_id = source.refund_id
            WHEN MATCHED THEN UPDATE SET
                target.order_id = source.order_id,
                target.refund_amount = source.refund_amount,
                target.currency_code = source.currency_code,
                target.created_at = source.created_at
            WHEN NOT MATCHED THEN INSERT (
                refund_id, order_id, refund_amount, currency_code, created_at
            ) VALUES (
                source.refund_id, source.order_id, source.refund_amount,
                source.currency_code, source.created_at
            );
        """).strip()
        snowflake_hook.run(query)
    logger.info(
        "Successfully transformed and bulk-loaded Snowflake Silver Refunds "
        f"for date partition {ds}!"
    )

    return ds


@task(queue="utility_workers")
def load_dim_products(s3_key: str, ds: str) -> None:
    """Load catalog Parquet from S3 and apply SCD Type 2 changes in Snowflake."""
    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")
    s3_hook = S3Hook(aws_conn_id="SILO_S3")
    s3_object = s3_hook.get_key(key=s3_key, bucket_name=LANDING_BUCKET)
    if s3_object is None:
        raise FileNotFoundError(f"Catalog object not found: {s3_key}")

    response = s3_object.get()
    with response["Body"] as body:
        parquet_bytes = body.read()

    catalog_df = pd.read_parquet(io.BytesIO(parquet_bytes))
    if catalog_df.empty:
        raise ValueError(f"Catalog Parquet object contains no products: {s3_key}")

    silver = f"{SNOWFLAKE_SCHEMA}_silver"
    dimension_table = table_name(SNOWFLAKE_DATABASE, silver, "dim_products")
    insert_query = f"""
        INSERT INTO {dimension_table} (
            product_key, product_hash, sku, source_description,
            canonical_description, category, price, is_current, valid_from, valid_to
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::TIMESTAMP, NULL)
    """

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT sku, product_hash FROM {dimension_table} WHERE is_current = TRUE"
            )
            current_products = dict(cursor.fetchall())
            inserts = []

            for product in catalog_df.to_dict(orient="records"):
                sku = product["sku"]
                product_hash = product["product_hash"]
                existing_hash = current_products.get(sku)
                if existing_hash == product_hash:
                    continue

                if existing_hash is not None:
                    cursor.execute(
                        f"""
                        UPDATE {dimension_table}
                        SET valid_to = %s::TIMESTAMP, is_current = FALSE
                        WHERE sku = %s AND is_current = TRUE
                        """,
                        (ds, sku),
                    )

                product_key = hashlib.sha256(
                    f"{sku}|{ds}|{product_hash}".encode("utf-8")
                ).hexdigest()
                inserts.append(
                    (
                        product_key,
                        product_hash,
                        sku,
                        product["source_description"],
                        product["canonical_description"],
                        product["category"],
                        product["price"],
                        True,
                        ds,
                    )
                )

            if inserts:
                for insert in inserts:
                    cursor.execute(insert_query, insert)

    logger.info(
        "Product dimension processed: "
        f"{len(inserts)} new or changed records from {s3_key}."
    )
