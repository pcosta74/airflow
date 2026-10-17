import os
import sys
from pathlib import Path

import pytest  # noqa: F401
from dotenv import load_dotenv
from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_infra.integrations.shop.models import Base, Orders
from tests.fixtures.data_generator import generate_order_records

load_dotenv()

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_DIR.parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

TEST_USER = os.getenv("SHOP_USER", "shop")
TEST_PASSWORD = os.getenv("SHOP_PASSWORD", "shop")
TEST_DB_URL = f"postgresql+psycopg2://{TEST_USER}:{TEST_PASSWORD}@pg-shop:5432/test_db"


@fixture(scope="session", autouse=True)
def configure_test_airflow_env():
    """Mocks backend environment settings so Airflow tests run fast and isolated."""
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    os.environ["AIRFLOW_HOME"] = str(TESTS_DIR / ".airflow_test_home")


@fixture(scope="module")
def test_db_engine():
    """Creates a clean database schema for pipeline verification tests."""
    engine = create_engine(TEST_DB_URL)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@fixture(scope="function")
def seed_orders_dataset(test_db_engine):
    """Fixture to seed ~95,000 records into Postgres for extraction verification."""
    SessionLocal = sessionmaker(bind=test_db_engine)

    with SessionLocal() as session:
        records = generate_order_records(days=7)

        chunk_size = 10000
        for i in range(0, len(records), chunk_size):
            session.bulk_insert_mappings(Orders, records[i : i + chunk_size])

        session.commit()
        yield

        session.query(Orders).delete()
        session.commit()
