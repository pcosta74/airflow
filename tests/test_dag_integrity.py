from pathlib import Path

import pytest  # noqa: F401
from airflow.models import DagBag


def test_dag_loading_integrity():
    """Ensures the orchestrator DAG file parses cleanly with zero structural errors."""
    test_dir = Path(__file__).resolve().parent
    dags_path = test_dir.parent / "src" / "data_infra" / "dags"

    dag_bag = DagBag(dag_folder=str(dags_path))

    assert len(dag_bag.import_errors) == 0, (
        f"DAG Compiler Failures Found: {dag_bag.import_errors}"
    )
    assert "ecommerce_sales_etl_pipeline" in dag_bag.dags

    etl_dag = dag_bag.dags["ecommerce_sales_etl_pipeline"]
    clean = etl_dag.get_task("ml_clean_orders")
    audit = etl_dag.get_task("audit_quarantine_metrics")
    products = etl_dag.get_task("load_dim_products")
    orders_ingest = etl_dag.get_task("ingest_orders_to_bronze")
    extract_refunds = etl_dag.get_task("extract_refunds")
    refunds_ingest = etl_dag.get_task("ingest_refunds_to_bronze")
    silver_refunds = etl_dag.get_task("transform_silver_refunds")
    assert orders_ingest in clean.upstream_list
    assert clean in audit.upstream_list
    assert products in clean.upstream_list
    assert audit in etl_dag.get_task("load_product_sales_summary").upstream_list
    assert etl_dag.max_active_runs == 1
    assert etl_dag.schedule == "@daily"
    assert etl_dag.timezone.name == "Europe/London"
    business_date = (
        "{{ logical_date.in_timezone('Europe/London').to_date_string() }}"
    )
    assert etl_dag.get_task("extract_orders").op_kwargs["ds"] == business_date
    assert etl_dag.get_task("extract_refunds").op_kwargs["ds"] == business_date
    assert (
        etl_dag.get_task("extract_catalog_dimension").op_kwargs["ds"] == business_date
    )
    assert extract_refunds in refunds_ingest.upstream_list
    assert refunds_ingest in silver_refunds.upstream_list
    assert silver_refunds.op_kwargs["ds"] == business_date
    assert silver_refunds.queue == "utility_workers"
    for task_id in (
        "load_product_sales_summary",
        "load_customer_rfm_metrics",
        "load_daily_tax_ledger",
        "load_order_fulfillment_kpis",
    ):
        task = etl_dag.get_task(task_id)
        assert audit in task.upstream_list
        assert silver_refunds in task.upstream_list
        assert products in task.upstream_list

    training_dag = dag_bag.dags["ml_weekly_train_anomaly_gatekeeper"]
    train = training_dag.get_task("train_and_publish_gatekeeper")
    assert train.queue == "ml_workers"
    assert training_dag.params["target_anomaly_rate"] == 5.0

    fraud_training_dag = dag_bag.dags["ml_weekly_train_fraud_gatekeeper"]
    assert fraud_training_dag.params["risk_threshold"] == 0.75
