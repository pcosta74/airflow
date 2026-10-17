import logging
import os

import pendulum
from airflow.sdk import dag

from data_infra.dags.etl_extract import (
    extract_catalog_dimension,
    extract_orders,
    extract_refunds,
    ingest_orders_to_bronze,
    ingest_refunds_to_bronze,
)
from data_infra.dags.etl_load import (
    load_customer_rfm_metrics,
    load_daily_tax_ledger,
    load_order_fulfillment_kpis,
    load_product_sales_summary,
)
from data_infra.dags.etl_transform import (
    audit_quarantine_metrics,
    load_dim_products,
    ml_clean_orders,
    transform_silver_refunds,
)

logger = logging.getLogger(__name__)

MAX_ALLOWED_ANOMALY_RATE = float(os.getenv("MAX_ALLOWED_ANOMALY_RATE", "5.0"))
if not 0 <= MAX_ALLOWED_ANOMALY_RATE <= 100:
    raise ValueError("MAX_ALLOWED_ANOMALY_RATE must be between 0 and 100.")

BUSINESS_TIMEZONE = "Europe/London"
BUSINESS_DATE = (
    f"{{{{ logical_date.in_timezone('{BUSINESS_TIMEZONE}').to_date_string() }}}}"
)


@dag(
    dag_id="ecommerce_sales_etl_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz=BUSINESS_TIMEZONE),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["production", "celery", "postgres", "ecommerce"],
)
def run_etl():
    extraction_context = {"ds": BUSINESS_DATE, "run_id": "{{ run_id }}"}

    orders_s3_key = extract_orders(**extraction_context)
    refunds_s3_key = extract_refunds(**extraction_context)
    catalog_s3_key = extract_catalog_dimension(ds=BUSINESS_DATE)

    bronze_orders_metadata = ingest_orders_to_bronze(
        s3_key=orders_s3_key, ds=BUSINESS_DATE
    )
    bronze_refunds_metadata = ingest_refunds_to_bronze(
        s3_key=refunds_s3_key, ds=BUSINESS_DATE
    )
    dim_products_date = load_dim_products(s3_key=catalog_s3_key, ds=BUSINESS_DATE)

    ml_output = ml_clean_orders(
        bronze_orders_metadata=bronze_orders_metadata, run_id="{{ run_id }}"
    )
    dim_products_date >> ml_output
    gatekeeper_date = audit_quarantine_metrics(
        ml_output=ml_output, max_allowed_anomaly_rate=MAX_ALLOWED_ANOMALY_RATE
    )

    silver_refunds_date = transform_silver_refunds(ds=BUSINESS_DATE)
    bronze_refunds_metadata >> silver_refunds_date

    load_tasks = [
        load_product_sales_summary(ds=gatekeeper_date),
        load_customer_rfm_metrics(ds=gatekeeper_date),
        load_daily_tax_ledger(ds=gatekeeper_date),
        load_order_fulfillment_kpis(ds=gatekeeper_date),
    ]

    silver_refunds_date >> load_tasks
    dim_products_date >> load_tasks

    logger.info("ecom-shop ETL pipeline initialization completed successfully.")


run_etl()
