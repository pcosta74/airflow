import argparse
import os
import sys
from logging import INFO, basicConfig, getLogger
from pathlib import Path

import pandas as pd
from data_generator import (
    ensure_daily_refund_coverage,
    generate_order_records,
    generate_refund_records,
)
from dotenv import load_dotenv
from sqlalchemy import create_engine, make_url, text

from data_infra.integrations.shop.models import Orders, Refunds

basicConfig(level=INFO)
logger = getLogger(__name__)

load_dotenv()

DEV_DB_URL = os.getenv("SHOP_DATABASE_URL")
if not DEV_DB_URL:
    logger.error(
        "Environment variable 'SHOP_DATABASE_URL' is missing.\n"
        "Please define it in your '.env' file or your active virtual environment.\n"
        "Example:\n"
        "  export SHOP_DATABASE_URL=postgresql+psycopg2://user:password@pg-shop:5432/shop_db\n",
    )
    sys.exit(1)

RUNNING_IN_DOCKER = os.path.exists("/.dockerenv")
if not RUNNING_IN_DOCKER:
    url_object = make_url(DEV_DB_URL)
    url_object = url_object.set(host="127.0.0.1")
    DEV_DB_URL = url_object.render_as_string(hide_password=False)

FIXTURE_DIR = Path(__file__).resolve().parent
ORDERS_FIXTURE_PATH = FIXTURE_DIR / "persistent_orders.parquet"
REFUND_FIXTURE_PATH = FIXTURE_DIR / "persistent_refunds.parquet"


def create_persistent_fixture(days: int = 7):
    """Generates the mock dataset once and freezes it to a local binary file."""
    logger.info("Generating data points for the 1-week timeline...")
    raw_records = generate_order_records(days=days)
    refund_records = generate_refund_records(raw_records)

    df = pd.DataFrame(raw_records)
    df["price"] = df["price"].astype(float)
    df.to_parquet(ORDERS_FIXTURE_PATH, compression="snappy", index=False)
    refunds_df = pd.DataFrame(refund_records)
    refunds_df["refund_amount"] = refunds_df["refund_amount"].astype(float)
    refunds_df.to_parquet(REFUND_FIXTURE_PATH, compression="snappy", index=False)

    logger.info(
        f"Persistent fixtures created: {len(df):,} orders and "
        f"{len(refunds_df):,} refunds."
    )


def load_fixture_to_dev():
    """Read the frozen file and populate the database without breaking Alembic."""
    if not ORDERS_FIXTURE_PATH.exists():
        logger.warning(
            "Fixture file not found! Generating a fresh baseline dataset first..."
        )
        create_persistent_fixture()

    logger.info(f"Reading frozen dataset from {ORDERS_FIXTURE_PATH.name}...")
    df = pd.read_parquet(ORDERS_FIXTURE_PATH)
    if "customer_id" not in df:
        df["customer_id"] = None
    if "updated_at" not in df:
        df["updated_at"] = df["created_at"]
    if "completed_at" not in df:
        df["completed_at"] = None
    completed_at_fallback = df["created_at"].where(df["status"] == "COMPLETED")
    df["completed_at"] = df["completed_at"].fillna(completed_at_fallback)
    if "currency_code" not in df:
        df["currency_code"] = None

    if REFUND_FIXTURE_PATH.exists():
        refunds_df = pd.read_parquet(REFUND_FIXTURE_PATH)
    else:
        refunds_df = pd.DataFrame(generate_refund_records(df.to_dict("records")))
    refunds_df = pd.DataFrame(
        ensure_daily_refund_coverage(
            df.to_dict("records"), refunds_df.to_dict("records")
        )
    )

    logger.info(f"Logging into {make_url(DEV_DB_URL)}")
    engine = create_engine(DEV_DB_URL)
    with engine.begin() as connection:
        logger.info("Safely truncating order and refund fixture data...")
        connection.execute(
            text("TRUNCATE TABLE refunds, orders RESTART IDENTITY CASCADE;")
        )

        logger.info(f"Streaming {len(df):,} records into the persistent schema...")
        try:
            df.to_sql(
                name=Orders.__tablename__,
                con=connection,
                if_exists="append",
                index=False,
                chunksize=5000,
            )
            if not refunds_df.empty:
                refunds_df.to_sql(
                    name=Refunds.__tablename__,
                    con=connection,
                    if_exists="append",
                    index=False,
                    chunksize=5000,
                )
        except Exception as e:
            logger.error(f"Failed to populate database:\n{e}")
            sys.exit(1)

    logger.info("Development database populated cleanly and Alembic history preserved!")


parser = argparse.ArgumentParser(prog="DevSeeder", description="ECom-Shop Order seeder")
parser.add_argument("--freeze", action="store_true")
parser.add_argument("-d", "--days", type=int, default=7)

if __name__ == "__main__":
    args = parser.parse_args()

    if args.freeze:
        create_persistent_fixture(days=args.days)
    else:
        load_fixture_to_dev()
