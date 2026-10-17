import logging
import os
from datetime import datetime, timedelta
from textwrap import dedent

from airflow.sdk import task

from data_infra.integrations.snowflake import create_snowflake_hook, table_name

logger = logging.getLogger(__name__)

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE") or ValueError(
    "set SNOWFLAKE_DATABASE is .env"
)
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") or ValueError(
    "set SNOWFLAKE_SCHEMA is .env"
)
ESTIMATED_VAT_RATE = float(os.getenv("ESTIMATED_VAT_RATE", "0.23"))
if not 0 <= ESTIMATED_VAT_RATE <= 1:
    raise ValueError("ESTIMATED_VAT_RATE must be between 0 and 1.")


def _run_gold_load(snowflake_hook, query: str) -> None:
    if os.getenv("SNOWFLAKE_EMULATOR", "").lower() in {"1", "true", "yes"}:
        statements = [
            statement.strip()
            for statement in query.split(";")
            if statement.strip().upper() not in {"", "BEGIN TRANSACTION", "COMMIT"}
        ]
        with snowflake_hook.get_conn() as connection:
            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
        return

    snowflake_hook.run(query)


def _get_extraction_intervals(ds: str) -> tuple[str, str, str]:
    """Calculate date partition boundaries using safe Snowflake ISO string formatting."""
    snapshot_date = datetime.strptime(ds, "%Y-%m-%d")
    snapshot_start = snapshot_date.strftime("%Y-%m-%d 00:00:00.000")
    snapshot_cutoff = (snapshot_date + timedelta(days=1)).strftime(
        "%Y-%m-%d 00:00:00.000"
    )
    return ds, snapshot_start, snapshot_cutoff


@task()
def load_product_sales_summary(ds: str) -> None:
    """
    Write rolling daily and calendar-day product sales summaries using an optimized
    30-day lookback window, enriched with point-in-time SCD Type 2 dimension values.
    """
    snapshot_date, _, snapshot_cutoff = _get_extraction_intervals(ds)
    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")

    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    gold_schema = f"{SNOWFLAKE_SCHEMA}_gold"
    target_table = table_name(
        SNOWFLAKE_DATABASE, gold_schema, "product_sales_summary"
    )
    orders_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "orders")
    dimension_table = table_name(
        SNOWFLAKE_DATABASE, silver_schema, "dim_products"
    )

    query = dedent(f"""
        BEGIN TRANSACTION;
        DELETE FROM {target_table}
        WHERE snapshot_date = '{snapshot_date}'::DATE;
        INSERT INTO {target_table}
        WITH ranked_orders AS (
            SELECT 
                order_id, product_key, currency_code, quantity, total_value, 
                status, is_anomaly, completed_at, created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY COALESCE(updated_at, created_at) DESC, COALESCE(revision, 0) DESC
                ) AS version_rank
            FROM {orders_table}
            -- Micro-partition pruning prevents scanning table history
            WHERE created_at >= '{snapshot_date}'::DATE - INTERVAL '30 DAYS'
              AND created_at < '{snapshot_cutoff}'::TIMESTAMP
              AND COALESCE(updated_at, created_at) < '{snapshot_cutoff}'::TIMESTAMP
        ),
        current_orders AS (
            SELECT 
                ro.order_id,
                dim.sku AS product_sku,
                ro.quantity,
                ro.total_value,
                ro.completed_at,
                ro.created_at,
                UPPER(COALESCE(NULLIF(TRIM(ro.currency_code), ''), 'UNKNOWN')) AS reporting_currency,
                DATE(ro.completed_at) AS sale_date,
                -- Enrich with historical master dimensions at point-of-sale
                COALESCE(dim.canonical_description, 'UNKNOWN_PRODUCT') AS product_name,
                COALESCE(dim.category, 'UNCATEGORIZED') AS product_category
            FROM ranked_orders ro
            LEFT JOIN {dimension_table} dim
              ON ro.product_key = dim.product_key
            WHERE ro.version_rank = 1
              AND NOT COALESCE(ro.is_anomaly, false)
              AND COALESCE(ro.status, 'COMPLETED') = 'COMPLETED'
              AND ro.completed_at IS NOT NULL
        )
        SELECT
            '{snapshot_date}'::DATE AS snapshot_date,
            product_sku,
            reporting_currency AS currency_code,
            SUM(CASE WHEN sale_date = '{snapshot_date}'::DATE THEN quantity ELSE 0 END) AS daily_units_sold,
            ROUND(SUM(CASE WHEN sale_date = '{snapshot_date}'::DATE THEN total_value ELSE 0 END), 2) AS daily_revenue,
            SUM(CASE WHEN sale_date BETWEEN '{snapshot_date}'::DATE - INTERVAL '6 DAYS' AND '{snapshot_date}'::DATE THEN quantity ELSE 0 END) AS trailing_7d_units_sold,
            ROUND(SUM(CASE WHEN sale_date BETWEEN '{snapshot_date}'::DATE - INTERVAL '6 DAYS' AND '{snapshot_date}'::DATE THEN total_value ELSE 0 END) / 7.0, 2) AS trailing_7d_avg_daily_revenue,
            SUM(CASE WHEN sale_date BETWEEN '{snapshot_date}'::DATE - INTERVAL '29 DAYS' AND '{snapshot_date}'::DATE THEN quantity ELSE 0 END) AS trailing_30d_units_sold,
            COUNT(DISTINCT CASE WHEN sale_date BETWEEN '{snapshot_date}'::DATE - INTERVAL '29 DAYS' AND '{snapshot_date}'::DATE THEN sale_date END) AS active_sales_days_30d
        FROM current_orders
        GROUP BY product_sku, reporting_currency;
        COMMIT;
    """).strip()

    _run_gold_load(snowflake_hook, query)
    logger.info(
        f"Product sales summary data mart updated successfully for partition date: {ds}"
    )


@task()
def load_customer_rfm_metrics(ds: str) -> None:
    """
    Compute customer lifetime RFM metrics from each order's latest known warehouse state,
    utilizing subquery partition pushdown to prevent full table scanning costs.
    """
    snapshot_date, _, snapshot_cutoff = _get_extraction_intervals(ds)
    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")

    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    gold_schema = f"{SNOWFLAKE_SCHEMA}_gold"
    target_table = table_name(
        SNOWFLAKE_DATABASE, gold_schema, "customer_rfm_metrics"
    )
    orders_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "orders")
    refunds_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "refunds")
    day_date_part = (
        "'day'"
        if os.getenv("SNOWFLAKE_EMULATOR", "").lower() in {"1", "true", "yes"}
        else "day"
    )

    query = dedent(f"""
        BEGIN TRANSACTION;
        DELETE FROM {target_table}
        WHERE snapshot_date = '{snapshot_date}'::DATE;
        INSERT INTO {target_table}
        WITH ranked_orders AS (
            SELECT 
                order_id, customer_id, currency_code, total_value, status, 
                is_anomaly, completed_at, created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY COALESCE(updated_at, created_at) DESC, COALESCE(revision, 0) DESC
                ) AS version_rank
            FROM {orders_table}
            WHERE created_at < '{snapshot_cutoff}'::TIMESTAMP
              AND COALESCE(updated_at, created_at) < '{snapshot_cutoff}'::TIMESTAMP
        ),
        order_lifetime AS (
            SELECT
                customer_id, 
                order_id,
                UPPER(COALESCE(NULLIF(TRIM(currency_code), ''), 'UNKNOWN')) AS reporting_currency,
                DATE(completed_at) AS order_date,
                SUM(total_value) AS order_value
            FROM ranked_orders
            WHERE version_rank = 1
              AND NOT COALESCE(is_anomaly, false)
              AND COALESCE(status, 'COMPLETED') = 'COMPLETED'
              AND completed_at IS NOT NULL
            GROUP BY customer_id, order_id, reporting_currency, order_date
        ),
        refunds_by_order AS (
            SELECT 
                order_id,
                UPPER(COALESCE(NULLIF(TRIM(currency_code), ''), 'UNKNOWN')) AS reporting_currency,
                SUM(refund_amount) AS refunded_amount
            FROM {refunds_table}
            -- Core Optimization: Prunes refund partitions dynamically by filtering out keys 
            -- that don't belong to the orders being calculated for this snapshot period
            WHERE order_id IN (SELECT order_id FROM order_lifetime)
              AND created_at < '{snapshot_cutoff}'::TIMESTAMP
            GROUP BY order_id, reporting_currency
        ),
        customer_lifetime AS (
            SELECT
                customer_id, 
                reporting_currency,
                MAX(order_date) AS last_purchase_date,
                COUNT(DISTINCT order_id) AS total_lifetime_orders,
                SUM(order_value - COALESCE(refunded_amount, 0.0)) AS customer_lifetime_value,
                AVG(order_value - COALESCE(refunded_amount, 0.0)) AS average_order_value
            FROM order_lifetime
            LEFT JOIN refunds_by_order USING (order_id, reporting_currency)
            GROUP BY customer_id, reporting_currency
        )
        SELECT
            '{snapshot_date}'::DATE AS snapshot_date,
            customer_id,
            reporting_currency AS currency_code,
            DATEDIFF({day_date_part}, last_purchase_date, '{snapshot_date}'::DATE) AS rfm_recency_days,
            total_lifetime_orders AS rfm_frequency_score,
            ROUND(customer_lifetime_value, 2) AS rfm_monetary_value,
            ROUND(average_order_value, 2) AS average_order_value,
            CASE
                WHEN customer_id IS NULL THEN 'UNATTRIBUTED'
                WHEN DATEDIFF({day_date_part}, last_purchase_date, '{snapshot_date}'::DATE) > 90 THEN 'CHURNED'
                WHEN customer_lifetime_value > 1000 AND DATEDIFF({day_date_part}, last_purchase_date, '{snapshot_date}'::DATE) <= 30 THEN 'VIP_ACTIVE'
                WHEN total_lifetime_orders = 1 THEN 'SINGLE_BUYER'
                ELSE 'REGULAR_RETAINED'
            END AS customer_segment_cohort
        FROM customer_lifetime;
        COMMIT;
    """).strip()
    _run_gold_load(snowflake_hook, query)
    logger.info(
        f"Customer RFM metric summary mart successfully refreshed inside Snowflake for snapshot: {ds}"
    )


@task()
def load_daily_tax_ledger(ds: str) -> None:
    """Estimate daily net sales and accounting VAT using structured transaction streams."""
    accounting_date, _, snapshot_cutoff = _get_extraction_intervals(ds)
    snowflake_hook = create_snowflake_hook(snowflake_conn_id="DWH_SNOWFLAKE")

    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    gold_schema = f"{SNOWFLAKE_SCHEMA}_gold"
    target_table = table_name(SNOWFLAKE_DATABASE, gold_schema, "daily_tax_ledger")
    orders_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "orders")
    refunds_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "refunds")

    query = dedent(f"""
        BEGIN TRANSACTION;
        DELETE FROM {target_table}
        WHERE accounting_date = '{accounting_date}'::DATE;
        INSERT INTO {target_table}
        WITH ranked_orders AS (
            SELECT 
                order_id, currency_code, total_value, status, is_anomaly, completed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY COALESCE(updated_at, created_at) DESC, COALESCE(revision, 0) DESC
                ) AS version_rank
            FROM {orders_table}
            -- Core Optimization: 30-day window limits partition scanning while capturing late updates
            WHERE created_at >= '{accounting_date}'::DATE - INTERVAL '30 DAYS'
              AND created_at < '{snapshot_cutoff}'::TIMESTAMP
              AND COALESCE(updated_at, created_at) < '{snapshot_cutoff}'::TIMESTAMP
        ),
        sales AS (
            SELECT
                UPPER(COALESCE(NULLIF(TRIM(currency_code), ''), 'UNKNOWN')) AS currency_code,
                SUM(total_value) AS gross_sales
            FROM ranked_orders
            WHERE version_rank = 1
              AND NOT COALESCE(is_anomaly, false)
              AND COALESCE(status, 'COMPLETED') = 'COMPLETED'
              AND DATE(completed_at) = '{accounting_date}'::DATE
            GROUP BY currency_code
        ),
        refunds AS (
            SELECT
                UPPER(COALESCE(NULLIF(TRIM(currency_code), ''), 'UNKNOWN')) AS currency_code,
                SUM(refund_amount) AS refunded_amount
            FROM {refunds_table}
            -- Core Optimization: Direct partition filter on refunds
            WHERE created_at >= '{accounting_date}'::DATE
              AND created_at < '{snapshot_cutoff}'::TIMESTAMP
              AND DATE(created_at) = '{accounting_date}'::DATE
            GROUP BY currency_code
        ),
        reporting_currencies AS (
            SELECT currency_code FROM sales
            UNION
            SELECT currency_code FROM refunds
        )
        SELECT
            '{accounting_date}'::DATE AS accounting_date,
            COALESCE(rc.currency_code, 'UNKNOWN') AS currency_code,
            ROUND(COALESCE(s.gross_sales, 0.0), 2) AS gross_sales_amount,
            ROUND(COALESCE(r.refunded_amount, 0.0), 2) AS gross_refunds_amount,
            ROUND(COALESCE(s.gross_sales, 0.0) - COALESCE(r.refunded_amount, 0.0), 2) AS net_sales_amount,
            {ESTIMATED_VAT_RATE}::DOUBLE AS estimated_vat_rate,
            ROUND((COALESCE(s.gross_sales, 0.0) - COALESCE(r.refunded_amount, 0.0)) * {ESTIMATED_VAT_RATE}, 2) AS estimated_net_vat_amount
        FROM reporting_currencies rc
        LEFT JOIN sales s ON rc.currency_code = s.currency_code
        LEFT JOIN refunds r ON rc.currency_code = r.currency_code;
        COMMIT;
    """).strip()
    _run_gold_load(snowflake_hook, query)
    logger.info(f"Daily accounting tax ledger updated successfully for date {ds}.")


@task()
def load_order_fulfillment_kpis(ds: str) -> None:
    """Summarize warehouse operational completions within the specific execution partition data interval."""
    snapshot_date, snapshot_start, snapshot_cutoff = _get_extraction_intervals(ds)
    snowflake_hook = create_snowflake_hook(
        snowflake_conn_id="DWH_SNOWFLAKE",
        insecure_mode=True,
    )

    silver_schema = f"{SNOWFLAKE_SCHEMA}_silver"
    gold_schema = f"{SNOWFLAKE_SCHEMA}_gold"
    target_table = table_name(
        SNOWFLAKE_DATABASE, gold_schema, "order_fulfillment_kpis"
    )
    orders_table = table_name(SNOWFLAKE_DATABASE, silver_schema, "orders")

    query = dedent(f"""
        BEGIN TRANSACTION;
        DELETE FROM {target_table}
        WHERE fulfillment_snapshot_date = '{snapshot_date}'::DATE;
        INSERT INTO {target_table}
        WITH ranked_orders AS (
            SELECT 
                order_id, currency_code, quantity, price, status, is_anomaly, completed_at, created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY COALESCE(updated_at, created_at) DESC, COALESCE(revision, 0) DESC
                ) AS version_rank
            FROM {orders_table}
            -- Core Optimization: 30-day lookback limits micro-partition scanning while catching updates
            WHERE created_at >= '{snapshot_date}'::DATE - INTERVAL '30 DAYS'
              AND created_at < '{snapshot_cutoff}'::TIMESTAMP
              AND COALESCE(updated_at, created_at) < '{snapshot_cutoff}'::TIMESTAMP
        ),
        completed_orders AS (
            SELECT
                order_id,
                UPPER(COALESCE(NULLIF(TRIM(currency_code), ''), 'UNKNOWN')) AS currency_code,
                SUM(quantity) AS completed_items,
                SUM(quantity * price) AS basket_value
            FROM ranked_orders
            WHERE version_rank = 1
              AND NOT COALESCE(is_anomaly, false)
              AND COALESCE(status, 'COMPLETED') = 'COMPLETED'
              AND completed_at >= '{snapshot_start}'::TIMESTAMP
              AND completed_at < '{snapshot_cutoff}'::TIMESTAMP
            GROUP BY order_id, currency_code
        )
        SELECT
            '{snapshot_date}'::DATE AS fulfillment_snapshot_date,
            currency_code,
            COUNT(DISTINCT order_id) AS orders_completed_in_interval,
            COALESCE(SUM(completed_items), 0) AS total_completed_items,
            COALESCE(MAX(completed_items), 0) AS largest_completed_order_items,
            ROUND(COALESCE(AVG(completed_items), 0.0), 2) AS average_items_per_completed_order,
            COUNT(CASE WHEN completed_items > 10 THEN 1 END) AS bulk_completed_order_count,
            COUNT(CASE WHEN currency_code = 'USD' AND basket_value > 500 THEN 1 END) AS high_value_usd_completed_order_count
        FROM completed_orders
        GROUP BY currency_code;
        COMMIT;
    """).strip()

    _run_gold_load(snowflake_hook, query)
    logger.info(f"Order fulfillment metrics summarized to Snowflake Gold for date {ds}.")
