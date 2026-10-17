import os
from datetime import datetime
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.definitions.param import Param

from data_infra.ml.cleansing import train_and_freeze_anomaly_model

DEFAULT_ARGS = {
    "owner": "data-ops",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

ML_MODEL_DIR = os.getenv("ML_MODEL_DIR", "/opt/airflow/models")
ML_MODEL_NAME = os.getenv("ANOMALY_ISOLATION_MODEL") or ValueError(
    "set ANOMALY_ISOLATION_MODEL is .env"
)


@task(queue="ml_workers")
def train_and_publish_gatekeeper(target_anomaly_rate: float, sample_size: int):
    """Periodic loop: connects via DuckDB, trains the model, saves ONNX, and exits."""

    model_output_path = Path(ML_MODEL_DIR) / ML_MODEL_NAME

    hook = PostgresHook(postgres_conn_id="shop_postgres")
    pg_conn_str = hook.get_uri()
    train_and_freeze_anomaly_model(
        model_output_path=model_output_path,
        pg_conn_str=pg_conn_str,
        target_anomaly_rate=target_anomaly_rate,
        sample_size=sample_size,
    )


@dag(
    dag_id="ml_weekly_train_anomaly_gatekeeper",
    default_args=DEFAULT_ARGS,
    schedule="@weekly",
    catchup=False,
    tags=["mlops", "training"],
    params={
        "target_anomaly_rate": Param(
            default=5.0,
            type="number",
            minimum=0.1,
            maximum=20.0,
            description="Maximum percent of completed calibration orders to flag",
        ),
        "sample_size": Param(
            default=10,
            type="integer",
            minimum=1,
            maximum=100,
            description="Percentage of data to sample from Postgres (1-100)",
        ),
    },
)
def anomaly_training_pipeline() -> bool:
    try:
        train_and_publish_gatekeeper(
            target_anomaly_rate="{{params.target_anomaly_rate}}",
            sample_size="{{params.sample_size}}",
        )
        return True
    except Exception:
        return False


anomaly_training_pipeline()
