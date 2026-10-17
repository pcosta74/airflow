import io
import json
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pandas as pd
import pytest

import data_infra.dags.etl_extract as extract_module
import data_infra.integrations.snowflake as snowflake_integration
from data_infra.dags.etl_extract import (
    _get_extraction_interval,
    extract_catalog_dimension,
    extract_orders,
    extract_refunds,
    run_id_partition,
)
from tests.fixtures.data_generator import PRODUCT_CATALOG


def test_snowflake_hook_enables_autocommit_for_emulator(monkeypatch):
    class FakeSnowflakeHook:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(snowflake_integration, "SnowflakeHook", FakeSnowflakeHook)
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")

    hook = snowflake_integration.create_snowflake_hook(
        "DWH_SNOWFLAKE",
        session_parameters={"QUERY_TAG": "unit-test"},
    )

    assert hook.kwargs["session_parameters"] == {
        "AUTOCOMMIT": True,
        "QUERY_TAG": "unit-test",
    }


class _FakeDuckDB:
    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, statement):
        self.statements.append(statement)
        if " TO '" in statement:
            output_path = statement.split(" TO '", 1)[1].split("'", 1)[0]
            Path(output_path).touch()


class _FakeS3Hook:
    def __init__(self, *, bucket_exists=True):
        self.bucket_exists = bucket_exists
        self.created_buckets = []
        self.uploads = []

    def check_for_bucket(self, bucket_name):
        return self.bucket_exists

    def create_bucket(self, bucket_name):
        self.created_buckets.append(bucket_name)

    def load_file(self, **kwargs):
        self.uploads.append(kwargs)


class _CatalogS3Hook(_FakeS3Hook):
    def load_file(self, **kwargs):
        kwargs["parquet_bytes"] = Path(kwargs["filename"]).read_bytes()
        self.uploads.append(kwargs)


def _mock_extract_hooks(monkeypatch, *, bucket_exists=True):
    duckdb_connection = _FakeDuckDB()
    s3_hook = _FakeS3Hook(bucket_exists=bucket_exists)

    def make_s3_hook(aws_conn_id):
        assert aws_conn_id == "SILO_S3"
        return s3_hook

    monkeypatch.setattr(
        extract_module.duckdb, "connect", lambda _database: duckdb_connection
    )
    monkeypatch.setattr(extract_module, "S3Hook", make_s3_hook)
    return duckdb_connection, s3_hook


def test_extraction_uses_logical_date_for_one_utc_day():
    start, end = _get_extraction_interval("2026-10-03")

    assert start == datetime(2026, 10, 3)
    assert end - start == timedelta(days=1)


def test_extraction_rejects_invalid_logical_date():
    with pytest.raises(ValueError):
        _get_extraction_interval("2026-10-03T00:00:00")


def test_run_id_partition_does_not_include_untrusted_run_id():
    run_id = "manual__x'; DROP TABLE orders;--"

    partition = run_id_partition(run_id)

    assert partition == f"run_{sha256(run_id.encode()).hexdigest()}_"
    assert partition.isascii()
    assert all(character.isalnum() or character == "_" for character in partition)


def test_parquet_ingestion_converts_records_to_variant_json(monkeypatch):
    created_at = datetime(2026, 10, 3, 12, 34, 56)
    records = [
        {"order_id": 1, "created_at": created_at},
        {"order_id": 2, "created_at": created_at + timedelta(days=1)},
    ]
    parquet_buffer = io.BytesIO()
    pd.DataFrame(records).to_parquet(parquet_buffer, index=False)
    request_body = io.BytesIO(parquet_buffer.getvalue())

    class FakeS3Object:
        def get(self):
            return {"Body": request_body}

    class FakeS3Hook:
        def __init__(self, aws_conn_id):
            assert aws_conn_id == "SILO_S3"

        def get_key(self, *, key, bucket_name):
            assert key == "orders.parquet"
            assert bucket_name == extract_module.LANDING_BUCKET
            return FakeS3Object()

    class FakeCursor:
        def __init__(self):
            self.execute_calls = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.closed = True

        def execute(self, query, parameters):
            self.execute_calls.append((query, parameters))

    class FakeConnection:
        def __init__(self):
            self.fake_cursor = FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def cursor(self):
            return self.fake_cursor

    class FakeSnowflakeHook:
        def __init__(self):
            self.connection = FakeConnection()

        def get_conn(self):
            return self.connection

    monkeypatch.setattr(extract_module, "S3Hook", FakeS3Hook)
    snowflake_hook = FakeSnowflakeHook()

    rows_inserted = extract_module._ingest_from_s3(
        "orders.parquet",
        "DWH.public_bronze.orders",
        snowflake_hook,
        "2026-10-03",
    )

    assert rows_inserted == 2
    execute_calls = snowflake_hook.connection.fake_cursor.execute_calls
    assert len(execute_calls) == 2
    assert all(
        "VALUES (%s::JSON, %s::TIMESTAMP)" in query for query, _ in execute_calls
    )
    ingested_records = [
        (json.loads(parameters[0]), parameters[1])
        for _, parameters in execute_calls
    ]
    assert [record["order_id"] for record, _ in ingested_records] == [1, 2]
    assert [record["created_at"] for record, _ in ingested_records] == [
        "2026-10-03T12:34:56.000",
        "2026-10-04T12:34:56.000",
    ]
    assert all(ingestion_date == "2026-10-03" for _, ingestion_date in ingested_records)
    assert snowflake_hook.connection.fake_cursor.closed
    assert request_body.closed


@pytest.mark.parametrize(
    ("task_name", "table"),
    [
        ("ingest_orders_to_bronze", "orders"),
        ("ingest_refunds_to_bronze", "refunds"),
    ],
)
@pytest.mark.parametrize("emulator", [False, True])
def test_bronze_ingestion_uses_backend_table_qualification(
    monkeypatch, task_name, table, emulator
):
    if emulator:
        monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")
    else:
        monkeypatch.delenv("SNOWFLAKE_EMULATOR", raising=False)

    captured = {}
    monkeypatch.setattr(
        extract_module,
        "create_snowflake_hook",
        lambda **kwargs: object(),
    )

    def ingest_from_s3(s3_key, target_table, snowflake_hook, ingestion_date):
        captured["target_table"] = target_table
        captured["ingestion_date"] = ingestion_date
        return 1

    monkeypatch.setattr(extract_module, "_ingest_from_s3", ingest_from_s3)

    result = getattr(extract_module, task_name).function("data.parquet", "2026-10-03")

    expected_table = snowflake_integration.table_name(
        extract_module.SNOWFLAKE_DATABASE,
        f"{extract_module.SNOWFLAKE_SCHEMA}_bronze",
        table,
    )
    assert result["target_table"] == expected_table
    assert captured["target_table"] == expected_table
    assert captured["ingestion_date"] == "2026-10-03"


def test_order_extraction_uploads_under_safe_run_partition(monkeypatch):
    duckdb_connection, s3_hook = _mock_extract_hooks(monkeypatch)
    run_id = "manual__x'; DROP TABLE orders;--"

    s3_key = extract_orders.function(ds="2026-10-03", run_id=run_id)

    assert s3_key == (
        f"ecom_shop/orders/load_date=2026-10-03/run_{sha256(run_id.encode()).hexdigest()}_data.parquet"
    )
    assert run_id not in s3_key
    assert "created_at >= '2026-10-03 00:00:00'" in duckdb_connection.statements[1]
    assert "created_at < '2026-10-04 00:00:00'" in duckdb_connection.statements[1]
    assert "FORMAT PARQUET" in duckdb_connection.statements[1]
    assert "COMPRESSION SNAPPY" in duckdb_connection.statements[1]
    assert s3_hook.uploads[0]["key"] == s3_key
    assert s3_hook.uploads[0]["replace"] is True


def test_refund_extraction_uploads_expected_interval(monkeypatch):
    duckdb_connection, s3_hook = _mock_extract_hooks(monkeypatch, bucket_exists=False)

    s3_key = extract_refunds.function(ds="2026-10-03", run_id="manual__refund_test")

    assert s3_key.startswith("ecom_shop/refunds/load_date=2026-10-03/run_")
    assert s3_key.endswith(".parquet")
    assert "created_at >= '2026-10-03 00:00:00'" in duckdb_connection.statements[1]
    assert "created_at < '2026-10-04 00:00:00'" in duckdb_connection.statements[1]
    assert s3_hook.created_buckets == [extract_module.LANDING_BUCKET]
    assert s3_hook.uploads[0]["key"] == s3_key


def test_catalog_extraction_reads_pim_and_uploads_parquet(monkeypatch):
    products = PRODUCT_CATALOG
    catalog_hook = _CatalogS3Hook()

    def read_catalog(url, timeout):
        assert url == "http://pim/products"
        assert timeout == 30
        return io.BytesIO(json.dumps(products).encode())

    monkeypatch.setattr(
        extract_module,
        "urlopen",
        read_catalog,
    )
    monkeypatch.setattr(
        extract_module,
        "PIM_API_URL",
        "http://pim",
    )
    monkeypatch.setattr(
        extract_module,
        "S3Hook",
        lambda aws_conn_id: catalog_hook,
    )

    s3_key = extract_catalog_dimension.function("2026-10-03")

    assert s3_key == "ecom_shop/load_date=2026-10-03/catalog_snapshot.parquet"
    assert catalog_hook.uploads[0]["key"] == s3_key
    exported_catalog = pd.read_parquet(
        io.BytesIO(catalog_hook.uploads[0]["parquet_bytes"])
    )
    assert set(exported_catalog["sku"]) == {product["sku"] for product in products}
    assert "source_description" in exported_catalog
    assert "canonical_description" in exported_catalog
