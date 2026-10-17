import hashlib
import io
import re
from pathlib import Path

import pandas as pd
from airflow.sdk.exceptions import AirflowFailException

import data_infra.dags.etl_transform as transform_module

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _FakeSnowflakeHook:
    def __init__(self, *, result=None):
        self.result = result
        self.queries = []

    def get_first(self, query):
        self.queries.append(query)
        return self.result

    def run(self, query):
        self.queries.append(query)


class _FakeDimensionCursor:
    def __init__(self, current_products):
        self.current_products = current_products
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, parameters=None):
        self.executed.append((query, parameters))

    def fetchall(self):
        return self.current_products

class _FakeDimensionConnection:
    def __init__(self, current_products):
        self.fake_cursor = _FakeDimensionCursor(current_products)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.committed = exc_type is None
        return False

    def cursor(self):
        return self.fake_cursor


class _FakeDimensionSnowflakeHook:
    def __init__(self, connection):
        self.connection = connection

    def get_conn(self):
        return self.connection


def test_emulator_silver_insert_writes_rows_with_native_values():
    connection = _FakeDimensionConnection([])
    snowflake_hook = _FakeDimensionSnowflakeHook(connection)
    dataframe = pd.DataFrame({"order_id": [41, 42], "product_key": ["a", None]})

    transform_module._insert_dataframe_rows(
        snowflake_hook, "memory.public_silver.orders", dataframe
    )

    assert connection.fake_cursor.executed == [
        (
            "INSERT INTO memory.public_silver.orders (order_id, product_key) "
            "VALUES (%s, %s)",
            (41, "a"),
        ),
        (
            "INSERT INTO memory.public_silver.orders (order_id, product_key) "
            "VALUES (%s, %s)",
            (42, None),
        ),
    ]
    assert connection.committed


class _FakeCatalogS3Object:
    def __init__(self, parquet_bytes):
        self.parquet_bytes = parquet_bytes

    def get(self):
        return {"Body": io.BytesIO(self.parquet_bytes)}


def test_load_dim_products_reads_parquet_and_applies_scd_changes(monkeypatch):
    products = [
        {
            "sku": "CHANGED",
            "source_description": "new source",
            "canonical_description": "new canonical",
            "category": "cables",
            "price": 12.5,
            "product_hash": "new-hash",
        },
        {
            "sku": "UNCHANGED",
            "source_description": "same source",
            "canonical_description": "same canonical",
            "category": "keyboards",
            "price": 20.0,
            "product_hash": "same-hash",
        },
        {
            "sku": "NEW",
            "source_description": "new product",
            "canonical_description": "new product",
            "category": "mice",
            "price": 30.0,
            "product_hash": "new-product-hash",
        },
    ]
    parquet_buffer = io.BytesIO()
    pd.DataFrame(products).to_parquet(parquet_buffer, index=False)
    s3_object = _FakeCatalogS3Object(parquet_buffer.getvalue())
    connection = _FakeDimensionConnection(
        [("CHANGED", "old-hash"), ("UNCHANGED", "same-hash")]
    )

    def make_s3_hook(*, aws_conn_id):
        assert aws_conn_id == "SILO_S3"

        class FakeS3Hook:
            def get_key(self, *, key, bucket_name):
                assert key == "catalog.parquet"
                assert bucket_name == transform_module.LANDING_BUCKET
                return s3_object

        return FakeS3Hook()

    monkeypatch.setattr(transform_module, "S3Hook", make_s3_hook)
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: _FakeDimensionSnowflakeHook(connection),
    )

    transform_module.load_dim_products.function("catalog.parquet", "2026-10-05")

    cursor = connection.fake_cursor
    assert "SELECT sku, product_hash" in cursor.executed[0][0]
    assert cursor.executed[1][1] == ("2026-10-05", "CHANGED")
    insert_calls = cursor.executed[2:]
    assert len(insert_calls) == 2
    assert all("INSERT INTO" in query for query, _ in insert_calls)
    assert insert_calls[0][1] == (
        hashlib.sha256(b"CHANGED|2026-10-05|new-hash").hexdigest(),
        "new-hash",
        "CHANGED",
        "new source",
        "new canonical",
        "cables",
        12.5,
        True,
        "2026-10-05",
    )
    assert insert_calls[1][1][2] == "NEW"
    assert connection.committed


def test_quarantine_gate_counts_review_rows(monkeypatch):
    snowflake_hook = _FakeSnowflakeHook(result=(3, 7, 4, 5, 10, 10, 50.0))
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    try:
        transform_module.audit_quarantine_metrics.function(
            {"status": "processed", "partition_date": "2026-10-03"},
            max_allowed_anomaly_rate=20.0,
        )
    except AirflowFailException as exc:
        assert "30.00%" in str(exc)
    else:
        raise AssertionError("Excess quarantined rows must fail the quality gate")

    query = snowflake_hook.queries[0]
    assert "COALESCE(is_anomaly, FALSE) OR needs_review" in query
    assert "total_anomalies / NULLIF(total_scored, 0)" in query


def test_emulator_semantic_review_rate_does_not_trip_anomaly_gate(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")

    class FakeSnowflakeHook:
        def __init__(self):
            self.results = [(0, 5, 5, 5), (1, 4, 5, 5, 5)]
            self.queries = []

        def get_first(self, query):
            self.queries.append(query)
            return self.results[len(self.queries) - 1]

    snowflake_hook = FakeSnowflakeHook()
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    result = transform_module.audit_quarantine_metrics.function(
        {"status": "processed", "partition_date": "2026-10-06"},
        max_allowed_anomaly_rate=20.0,
    )

    assert result == "2026-10-06"
    assert len(snowflake_hook.queries) == 2
    assert "FROM public_silver.orders" in snowflake_hook.queries[0]
    assert (
        "FROM public_silver.orders_quarantine" in snowflake_hook.queries[1]
    )


def test_emulator_audit_still_fails_when_numerical_anomaly_rate_is_too_high(
    monkeypatch,
):
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")

    class FakeSnowflakeHook:
        def __init__(self):
            self.results = [(0, 5, 5, 5), (2, 4, 5, 5, 5)]

        def get_first(self, query):
            return self.results.pop(0)

    snowflake_hook = FakeSnowflakeHook()
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    try:
        transform_module.audit_quarantine_metrics.function(
            {"status": "processed", "partition_date": "2026-10-06"},
            max_allowed_anomaly_rate=10.0,
        )
    except AirflowFailException as exc:
        assert "20.00%" in str(exc)
    else:
        raise AssertionError("Numerical anomalies above the limit must fail the gate")


def test_reprocessed_order_deletes_previous_clean_and_quarantined_versions(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_EMULATOR", raising=False)
    query = transform_module._delete_reprocessed_order_versions_sql(
        "DWH.public_bronze.orders",
        "public_silver",
        "2026-10-03",
    )

    assert query.count(
        "SELECT DISTINCT TRY_PARSE_JSON(raw_data):order_id::BIGINT"
    ) == 2
    assert "FROM DWH.public_bronze.orders" in query
    assert query.count("WHERE order_id IN") == 2
    assert "DATE(created_at)" not in query


def test_empty_order_batch_preserves_partition_date():
    result = transform_module.ml_clean_orders.function(
        {"partition_date": "2026-10-17", "status": "EMPTY", "target_table": ""}
    )

    assert result == {
        "status": "empty",
        "anomaly_rate": 0.0,
        "partition_date": "2026-10-17",
    }


def test_emulator_order_query_extracts_struct_fields_and_normalizes_epoch_dates(
    monkeypatch,
):
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")
    query = transform_module._bronze_orders_query(
        "memory.public_bronze.orders", "2026-10-06"
    )
    assert "json_extract_string(raw_data, '$.order_id') AS order_id" in query
    assert "json_extract_string(raw_data, '$.created_at') AS created_at" in query
    assert "SELECT raw_data" not in query
    assert "WHERE DATE(ingested_at) = '2026-10-06'::DATE" in query
    assert "PARTITION BY json_extract_string(raw_data, '$.order_id')" in query

    normalized = transform_module._normalize_emulator_order_frame(
        pd.DataFrame(
            {
                "ORDER_ID": ["10001"],
                "CUSTOMER_ID": ["2"],
                "SKU": ["MOUSE-WRLSS"],
                "PRODUCT_DESCRIPTION": ["Wireless Mouse Black"],
                "PRICE": ["45.5"],
                "CURRENCY_CODE": ["USD"],
                "QTY": ["2"],
                "STATUS": ["COMPLETED"],
                "CREATED_AT": ["1791329979872"],
                "COMPLETED_AT": [None],
                "UPDATED_AT": ["1791329979872"],
                "REVISION": ["1"],
            }
        )
    )

    assert normalized.loc[0, "order_id"] == 10001
    assert normalized.loc[0, "price"] == 45.5
    assert normalized.loc[0, "qty"] == 2
    assert normalized.loc[0, "created_at"] == pd.Timestamp(
        "2026-10-06T23:39:39.872Z"
    )
    assert pd.isna(normalized.loc[0, "completed_at"])


def test_current_product_mask_handles_emulator_boolean_strings():
    values = pd.Series(["true", "FALSE", True, False, None])

    assert transform_module._current_product_mask(values).tolist() == [
        True,
        False,
        True,
        False,
        False,
    ]


def test_missing_bronze_rows_preserves_partition_date_and_casts_dimension_dates(
    monkeypatch,
):
    class FakeSnowflakeHook:
        def __init__(self):
            self.queries = []

        def get_pandas_df(self, query):
            self.queries.append(query)
            if "dim_products" in query:
                return pd.DataFrame(
                    columns=[
                        "sku",
                        "product_key",
                        "canonical_description",
                        "is_current",
                        "valid_from",
                        "valid_to",
                    ]
                )
            return pd.DataFrame()

    snowflake_hook = FakeSnowflakeHook()
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    result = transform_module.ml_clean_orders.function(
        {
            "partition_date": "2026-10-06",
            "status": "SUCCESS",
            "target_table": "memory.public_bronze.orders",
        }
    )

    assert result == {
        "status": "empty",
        "anomaly_rate": 0.0,
        "partition_date": "2026-10-06",
    }
    assert (
        "CAST(is_current AS VARCHAR) AS is_current" in snowflake_hook.queries[0]
    )
    assert "CAST(valid_from AS VARCHAR) AS valid_from" in snowflake_hook.queries[0]
    assert "CAST(valid_to AS VARCHAR) AS valid_to" in snowflake_hook.queries[0]


def test_refunds_are_merged_without_overwriting_other_dates(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_EMULATOR", raising=False)
    snowflake_hook = _FakeSnowflakeHook()
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    transform_module.transform_silver_refunds.function("2026-10-03")

    query = snowflake_hook.queries[0]
    assert (
        f"MERGE INTO {transform_module.SNOWFLAKE_DATABASE}."
        f"{transform_module.SNOWFLAKE_SCHEMA}_silver.refunds AS target"
    ) in query
    assert "ON target.refund_id = source.refund_id" in query
    assert "WHEN MATCHED THEN UPDATE SET" in query
    assert "WHEN NOT MATCHED THEN INSERT" in query
    assert "QUALIFY ROW_NUMBER() OVER" in query
    assert "INSERT OVERWRITE" not in query


def test_local_raw_json_fields_use_json_extraction(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")
    snowflake_hook = _FakeSnowflakeHook()
    monkeypatch.setattr(
        transform_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    transform_module.transform_silver_refunds.function("2026-10-03")

    query = snowflake_hook.queries[0]
    assert "json_extract(" in query
    assert "json_extract(raw_data," in query
    assert "PARSE_JSON(raw_data)" not in query
    assert len(snowflake_hook.queries) == 2
    assert snowflake_hook.queries[0].startswith("DELETE FROM")
    assert snowflake_hook.queries[1].startswith("INSERT INTO")


def test_silver_order_schema_keeps_product_attributes_in_dimension():
    ddl = (PROJECT_ROOT / "deploy" / "init-datawarehouse.sf.sql").read_text()

    def columns_for(table_name):
        match = re.search(
            rf"CREATE TABLE IF NOT EXISTS .*_silver\.{table_name}\s*\((.*?)\);",
            ddl,
            flags=re.DOTALL,
        )
        assert match is not None
        return set(re.findall(r"^\s+(\w+)\s+\w+", match.group(1), re.MULTILINE))

    orders_columns = columns_for("orders")
    quarantine_columns = columns_for("orders_quarantine")

    assert "product_key" in orders_columns
    duplicated_product_attributes = {
        "sku",
        "product_description",
        "product_sku",
        "canonical_description",
    }
    assert not duplicated_product_attributes & orders_columns
    assert {"sku", "product_description", "product_key"} <= quarantine_columns
    assert not {
        "product_sku",
        "canonical_description",
        "suggested_description",
    } & quarantine_columns
    assert "ALTER TABLE" not in ddl.upper()
    assert not re.search(r"^\s*UPDATE\b", ddl, flags=re.MULTILINE | re.IGNORECASE)
