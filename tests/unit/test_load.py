import pytest

import data_infra.dags.etl_load as load_module
from data_infra.dags.etl_load import (
    load_customer_rfm_metrics,
    load_daily_tax_ledger,
    load_order_fulfillment_kpis,
    load_product_sales_summary,
)

SNAPSHOT_DATE = "2026-10-03"


class _FakeSnowflakeHook:
    def __init__(self):
        self.queries = []
        self.connection = _FakeConnection(self.queries)

    def run(self, query):
        self.queries.append(query)

    def get_conn(self):
        return self.connection


class _FakeCursor:
    def __init__(self, queries):
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query):
        self.queries.append(query)


class _FakeConnection:
    def __init__(self, queries):
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def cursor(self):
        return _FakeCursor(self.queries)


def test_gold_load_runs_transaction_statements_separately_for_emulator(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")
    snowflake_hook = _FakeSnowflakeHook()

    load_module._run_gold_load(
        snowflake_hook,
        "BEGIN TRANSACTION; DELETE FROM target; INSERT INTO target SELECT 1; COMMIT;",
    )

    assert snowflake_hook.queries == [
        "DELETE FROM target",
        "INSERT INTO target SELECT 1",
    ]


def test_emulator_rfm_uses_duckdb_date_part_literal(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_EMULATOR", "true")
    snowflake_hook = _FakeSnowflakeHook()
    monkeypatch.setattr(
        load_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    load_customer_rfm_metrics.function(SNAPSHOT_DATE)

    assert any(
        "DATEDIFF('day'," in statement for statement in snowflake_hook.queries
    )


@pytest.mark.parametrize(
    ("task", "table_name", "date_column"),
    [
        (load_product_sales_summary, "product_sales_summary", "snapshot_date"),
        (load_customer_rfm_metrics, "customer_rfm_metrics", "snapshot_date"),
        (load_daily_tax_ledger, "daily_tax_ledger", "accounting_date"),
        (
            load_order_fulfillment_kpis,
            "order_fulfillment_kpis",
            "fulfillment_snapshot_date",
        ),
    ],
)
def test_gold_load_replaces_only_the_current_snapshot(
    monkeypatch, task, table_name, date_column
):
    monkeypatch.delenv("SNOWFLAKE_EMULATOR", raising=False)
    snowflake_hook = _FakeSnowflakeHook()
    monkeypatch.setattr(
        load_module,
        "create_snowflake_hook",
        lambda snowflake_conn_id, **kwargs: snowflake_hook,
    )

    task.function(SNAPSHOT_DATE)

    query = snowflake_hook.queries[0]
    target_table = load_module.table_name(
        load_module.SNOWFLAKE_DATABASE,
        f"{load_module.SNOWFLAKE_SCHEMA}_gold",
        table_name,
    )
    assert query.index("BEGIN TRANSACTION;") < query.index("DELETE FROM")
    assert f"DELETE FROM {target_table}" in query
    assert f"WHERE {date_column} = '{SNAPSHOT_DATE}'::DATE" in query
    assert query.index("DELETE FROM") < query.index(f"INSERT INTO {target_table}")
    assert query.index(f"INSERT INTO {target_table}") < query.index("COMMIT;")
    assert "INSERT OVERWRITE" not in query
    _, _, snapshot_cutoff = load_module._get_extraction_intervals(SNAPSHOT_DATE)
    assert f"created_at < '{snapshot_cutoff}'::TIMESTAMP" in query
    assert "created_at <= '" not in query
