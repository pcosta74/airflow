
-- The emulator does not support creating databases, so this line is commented out.
-- You can uncomment it if you are running against a real Snowflake instance.
-- CREATE DATABASE IF NOT EXISTS ${SNOWFLAKE_DATABASE};

CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_bronze;
CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_silver;
CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_gold;


CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_bronze.orders (
    -- VARIANT NOT SUPPORTED BY LOCAL SNOWFLAKE EMULATOR; USE JSON INSTEAD
    -- You can uncomment it if you are running against a real Snowflake instance.
    -- raw_data    VARIANT,
    raw_data    JSON,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_bronze.refunds (
    -- VARIANT NOT SUPPORTED BY LOCAL SNOWFLAKE EMULATOR; USE JSON INSTEAD
    -- You can uncomment it if you are running against a real Snowflake instance.
    -- raw_data    VARIANT,
    raw_data    JSON,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_silver.dim_products (
    product_key             STRING,
    product_hash            STRING,
    sku                     STRING,
    source_description      STRING,
    canonical_description   STRING,
    category                STRING,
    price                   DOUBLE,
    is_current              BOOLEAN,
    valid_from              TIMESTAMP,
    valid_to                TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_silver.orders (
    order_id              BIGINT,
    customer_id           BIGINT,
    product_key           STRING,
    price                 DOUBLE,
    currency_code         STRING,
    quantity              INT,
    status                STRING,
    created_at            TIMESTAMP,
    completed_at          TIMESTAMP,
    updated_at            TIMESTAMP,
    revision              INT,
    total_value           DOUBLE,
    is_anomaly            BOOLEAN,
    match_status          STRING,
    description_status    STRING,
    suggestion_similarity DOUBLE
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_silver.orders_quarantine (
    order_id              BIGINT,
    customer_id           BIGINT,
    sku                   STRING,
    product_description   STRING,
    product_key           STRING,
    price                 DOUBLE,
    currency_code         STRING,
    quantity              INT,
    status                STRING,
    created_at            TIMESTAMP,
    completed_at          TIMESTAMP,
    updated_at            TIMESTAMP,
    revision              INT,
    total_value           DOUBLE,
    is_anomaly            BOOLEAN,
    needs_review          BOOLEAN,
    match_status          STRING,
    description_status    STRING,
    suggestion_similarity DOUBLE,
    suggested_sku         STRING
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_silver.refunds (
    refund_id     BIGINT,
    order_id      BIGINT,
    refund_amount DOUBLE,
    currency_code STRING,
    created_at    TIMESTAMP
);


CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_gold.product_sales_summary (
    snapshot_date                 DATE, 
    product_sku                   STRING, 
    currency_code                 STRING, 
    daily_units_sold              INT,
    daily_revenue                 DOUBLE, 
    trailing_7d_units_sold        INT, 
    trailing_7d_avg_daily_revenue DOUBLE,
    trailing_30d_units_sold       INT, 
    active_sales_days_30d         INT
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_gold.customer_rfm_metrics (
    snapshot_date           DATE,
    customer_id             BIGINT,
    currency_code           STRING,
    rfm_recency_days        INT,
    rfm_frequency_score     INT,
    rfm_monetary_value      DOUBLE,
    average_order_value     DOUBLE,
    customer_segment_cohort STRING
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_gold.daily_tax_ledger (
    accounting_date          DATE, 
    currency_code            STRING, 
    gross_sales_amount       DOUBLE,
    gross_refunds_amount     DOUBLE, 
    net_sales_amount         DOUBLE, 
    estimated_vat_rate       DOUBLE, 
    estimated_net_vat_amount DOUBLE
);

CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_gold.order_fulfillment_kpis (
    fulfillment_snapshot_date            DATE, 
    currency_code                        STRING, 
    orders_completed_in_interval         INT,
    total_completed_items                INT, 
    largest_completed_order_items        INT, 
    average_items_per_completed_order    DOUBLE,
    bulk_completed_order_count           INT, 
    high_value_usd_completed_order_count INT
);


-- Parquet file format is not supported by the local Snowflake emulator, so this line is commented out.
-- You can uncomment it if you are running against a real Snowflake instance.
-- CREATE OR REPLACE FILE FORMAT ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}_bronze.snappy_parquet_format
--     TYPE = 'PARQUET'
--     COMPRESSION = 'SNAPPY';
