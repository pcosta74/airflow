import logging
from pathlib import Path
from textwrap import dedent

import duckdb
import numpy as np
import onnxruntime as ort
import pandas as pd
from skl2onnx import to_onnx
from skl2onnx.common.data_types import FloatTensorType
from sklearn.ensemble import RandomForestClassifier

from data_infra.utils import safe_tofloat, safe_toint

FRAUD_FEATURES = (
    "item_velocity",
    "order_value",
    "price",
    "price_to_sku_avg_ratio",
    "qty",
)

logger = logging.getLogger(__name__)


def _engineer_behavioral_features(dq_chunk_df: pd.DataFrame) -> pd.DataFrame:
    """
    Engineers behavioral risk vectors from the raw dataframe using DuckDB in-memory.
    Captures velocity checks and structural transactional properties.
    """
    if dq_chunk_df.empty:
        return dq_chunk_df

    with duckdb.connect(":memory:") as con:
        featured_df = con.execute("""
            SELECT 
                *,
                (price * qty) AS order_value,
                CAST(qty AS DOUBLE) AS item_velocity,
                -- Risk Feature: High unit price deviations relative to typical 
                -- item baseline
                CASE 
                    WHEN AVG(price) OVER (PARTITION BY sku) > 0 
                    THEN price / AVG(price) OVER (PARTITION BY sku)
                    ELSE 1.0 
                END AS price_to_sku_avg_ratio
            FROM dq_chunk_df
        """).df()

    return featured_df


def score_fraud_probabilities(
    featured_df: pd.DataFrame, session: ort.InferenceSession, risk_threshold: float = 0.75
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Executes the trained supervised ONNX model graph against chunked matrices.
    Splits rows into clean and quarantine streams based on the risk threshold score.
    """
    if featured_df.empty:
        return featured_df, featured_df
    featured_df = _engineer_behavioral_features(featured_df)

    features = list(FRAUD_FEATURES)
    X = featured_df[features].to_numpy().astype(np.float32)

    input_name = session.get_inputs()[0].name
    raw_predictions = session.run(None, {input_name: X})

    prob_dicts = raw_predictions[1]
    fraud_scores = [float(d[1]) for d in prob_dicts]

    featured_df["fraud_score"] = fraud_scores
    featured_df["is_fraud_suspect"] = featured_df["fraud_score"] >= risk_threshold

    clean_mask = ~featured_df["is_fraud_suspect"]
    quar_mask = featured_df["is_fraud_suspect"]

    clean_chunk = featured_df[clean_mask].copy()
    quar_chunk = featured_df[quar_mask].copy()

    return clean_chunk, quar_chunk


def train_and_freeze_fd_model(
    model_output_path: str,
    pg_conn_str: str,
    lookback_days: int = 180,
    risk_threshold: float = 0.75,
):
    """
    TRAINING PHASE: Uses DuckDB to pull historical orders directly from Postgres,
    fits a supervised classifier against returns, and exports it to ONNX format.
    """
    if not model_output_path:
        raise ValueError(f"Invalid model_output_path: {model_output_path}")
    if not pg_conn_str:
        raise ValueError(f"Undefined pg_conn_str: {pg_conn_str}")

    lookback_days = safe_toint(lookback_days, 180)
    risk_threshold = safe_tofloat(risk_threshold, 0.75)

    with duckdb.connect(":memory:") as conn:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(f"ATTACH '{pg_conn_str}' AS pg_db (TYPE POSTGRES);")

        query = dedent(f"""
            SELECT price, qty, status, sku
            FROM pg_db.orders 
            WHERE status IN ('COMPLETED', 'returned')
            AND created_at >= NOW() - INTERVAL '{lookback_days} DAYS';
        """).strip()
        historical_orders_df = conn.execute(query).df()

    if historical_orders_df.empty:
        raise ValueError(
            "No historical records found in Postgres to fit the fraud model!"
        )

    historical_orders_df["label"] = (historical_orders_df["status"] == "returned").astype(
        int
    )

    historical_orders_df["order_value"] = (
        historical_orders_df["price"] * historical_orders_df["qty"]
    )
    historical_orders_df["item_velocity"] = historical_orders_df["qty"].astype(float)
    historical_orders_df["price_to_sku_avg_ratio"] = historical_orders_df[
        "price"
    ] / historical_orders_df.groupby("sku")["price"].transform("mean")

    features = list(FRAUD_FEATURES)
    X = historical_orders_df[features].to_numpy().astype(np.float32)
    y = historical_orders_df["label"].to_numpy()

    clf = RandomForestClassifier(n_estimators=50, random_state=42, max_depth=8)
    clf.fit(X, y)

    initial_type = [("float_input", FloatTensorType([None, len(features)]))]
    onnx_model = to_onnx(
        clf,
        initial_types=initial_type,
        target_opset={
            "": 15,
            "ai.onnx.ml": 3,
        },
    )

    meta = onnx_model.metadata_props.add()
    meta.key = "risk_threshold"
    meta.value = str(float(risk_threshold))

    model_output_path = Path(model_output_path).resolve()
    model_output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_model_path = model_output_path.with_suffix(".tmp")
    try:
        with open(temp_model_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
        temp_model_path.rename(model_output_path)
        logger.info(f"Fraud model atomically deployed to: {model_output_path}")
    except Exception as e:
        if temp_model_path.exists():
            temp_model_path.unlink()
        raise e

    return {"status": "trained", "risk_threshold": float(risk_threshold)}
