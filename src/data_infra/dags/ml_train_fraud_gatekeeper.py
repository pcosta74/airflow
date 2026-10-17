import os
from datetime import datetime
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.definitions.param import Param

from data_infra.ml.fraud import train_and_freeze_fd_model

DEFAULT_ARGS = {
    "owner": "data-ops",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

ML_MODEL_DIR = os.getenv("ML_MODEL_DIR", "/opt/airflow/models")
ML_MODEL_NAME = os.getenv("FRAUD_DETECTION_MODEL") or ValueError(
    "set FRAUD_DETECTION_MODEL is .env"
)
DEFAULT_FRAUD_RISK_THRESHOLD = float(os.getenv("FRAUD_RISK_THRESHOLD", "0.75"))
if not 0 <= DEFAULT_FRAUD_RISK_THRESHOLD <= 1:
    raise ValueError("FRAUD_RISK_THRESHOLD must be between 0 and 1.")


@task(queue="ml_workers")
def train_and_publish_gatekeeper(lookback_days: int, risk_threshold: float):
    """Periodic loop: connects via DuckDB, trains the model, saves ONNX, and exits."""

    model_output_path = Path(ML_MODEL_DIR) / ML_MODEL_NAME

    hook = PostgresHook(postgres_conn_id="shop_postgres")
    conn_obj = hook.get_connection("shop_postgres")
    pg_conn_str = (
        f"dbname={conn_obj.schema} user={conn_obj.login} "
        f"password={conn_obj.password} host={conn_obj.host} port={conn_obj.port}"
    )

    train_and_freeze_fd_model(
        model_output_path=model_output_path,
        pg_conn_str=pg_conn_str,
        lookback_days=lookback_days,
        risk_threshold=risk_threshold,
    )


@dag(
    dag_id="ml_weekly_train_fraud_gatekeeper",
    default_args=DEFAULT_ARGS,
    schedule="@weekly",
    catchup=False,
    tags=["mlops", "training"],
    params={
        "lookback_days": Param(
            default=180,
            type="integer",
            description="The number of historical days to pull from the database for "
            "model training.",
        ),
        "risk_threshold": Param(
            default=DEFAULT_FRAUD_RISK_THRESHOLD,
            type="number",
            minimum=0.0,
            maximum=1.0,
            description="The minimum probability score (0.0 to 1.0) required to flag "
            "an order as fraudulent and quarantine it.",
        ),
    },
)
def fraud_training_pipeline() -> bool:
    try:
        train_and_publish_gatekeeper(
            lookback_days="{{params.lookback_days}}",
            risk_threshold="{{params.risk_threshold}}",
        )
        return True
    except Exception:
        return False


fraud_training_pipeline()
