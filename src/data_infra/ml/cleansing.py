import logging
import os
from pathlib import Path
from textwrap import dedent

import duckdb
import numpy as np
import onnxruntime as ort
import pandas as pd
from cachetools import TTLCache, cached
from skl2onnx import to_onnx
from skl2onnx.common.data_types import FloatTensorType
from sklearn.ensemble import IsolationForest
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split

from data_infra.integrations.shop.product_aliases import PRODUCT_DESCRIPTION_ALIASES
from data_infra.utils import safe_toint

ML_MODEL_DIR = os.getenv("ML_MODEL_DIR", "/opt/airflow/models")

ANOMALY_FEATURES = ("price", "qty")
ANOMALY_MODEL_NAME = os.getenv("ANOMALY_ISOLATION_MODEL") or ValueError(
    "set ANOMALY_ISOLATION_MODEL is .env"
)
SEMANTIC_MODEL_NAME = os.getenv("SEMANTIC_MODEL") or ValueError(
    "set SEMANTIC_MODEL is .env"
)
SEMANTIC_MIN_SIMILARITY = float(os.getenv("SEMANTIC_MIN_SIMILARITY", "0.55"))
if not 0.0 <= SEMANTIC_MIN_SIMILARITY <= 1.0:
    raise ValueError("SEMANTIC_MIN_SIMILARITY must be between 0 and 1.")


logger = logging.getLogger(__name__)


@cached(cache=TTLCache(maxsize=1, ttl=3600))
def get_semantic_model():
    """
    Lazy-loading singleton. Ensures the model is ONLY loaded into worker RAM
    when data execution actually begins, avoiding Airflow Scheduler bloat.
    """
    logger.info("Initializing SentenceTransformer on Celery Worker CPU...")
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(
        SEMANTIC_MODEL_NAME,
        cache_folder=str(Path(ML_MODEL_DIR)),
        token=os.getenv("HF_TOKEN"),
    )


def heal_chunk_metadata(chunk_df: pd.DataFrame, catalog_df: pd.DataFrame) -> pd.DataFrame:
    """Validate SKU descriptions semantically and add review-only catalog suggestions."""
    if chunk_df.empty:
        result = chunk_df.copy()
        result["canonical_sku"] = pd.Series(dtype="string")
        result["canonical_description"] = pd.Series(dtype="string")
        result["product_key"] = pd.Series(dtype="string")
        result["suggested_sku"] = pd.Series(dtype="string")
        result["suggested_description"] = pd.Series(dtype="string")
        result["suggestion_similarity"] = pd.Series(dtype="float64")
        result["needs_review"] = pd.Series(dtype="bool")
        result["is_anomaly"] = pd.Series(dtype="bool")
        result["match_status"] = pd.Series(dtype="string")
        result["description_status"] = pd.Series(dtype="string")
        return result

    catalog_df = catalog_df.rename(columns=str.lower).copy()
    required_columns = {"sku", "canonical_description"}
    missing_columns = required_columns - set(catalog_df.columns)
    if missing_columns:
        raise ValueError(
            f"Product catalog is missing required columns: {sorted(missing_columns)}"
        )

    if catalog_df.empty:
        raise ValueError("Product catalog is empty.")

    for column in ("sku", "canonical_description"):
        invalid_values = catalog_df[column].isna() | catalog_df[column].astype(
            "string"
        ).str.strip().eq("")
        if invalid_values.any():
            raise ValueError(f"Product catalog has missing or blank {column} values.")

    catalog_df["sku_key"] = catalog_df["sku"].astype("string").str.strip().str.upper()
    if catalog_df["sku_key"].duplicated().any():
        raise ValueError("Product catalog contains duplicate SKUs.")

    result = chunk_df.copy()
    result["sku_key"] = result["sku"].astype("string").str.strip().str.upper()
    catalog_by_sku = catalog_df.set_index("sku_key")
    result["canonical_sku"] = result["sku_key"].map(catalog_by_sku["sku"])
    result["canonical_description"] = result["sku_key"].map(
        catalog_by_sku["canonical_description"]
    )
    if "product_key" in catalog_by_sku:
        result["product_key"] = result["sku_key"].map(catalog_by_sku["product_key"])
    result["suggested_sku"] = None
    result["suggested_description"] = None
    result["suggestion_similarity"] = np.nan

    has_known_sku = result["canonical_sku"].notna()
    source_descriptions = result["product_description"].astype("string")
    has_description = source_descriptions.notna() & source_descriptions.str.strip().ne(
        ""
    ).fillna(False)
    result["match_status"] = "unresolved"
    result.loc[has_known_sku, "match_status"] = "sku_matched"
    result["needs_review"] = ~has_known_sku | ~has_description
    result["is_anomaly"] = False

    source_normalized = source_descriptions.str.strip().str.casefold()
    result["description_status"] = "not_assessed"
    result.loc[has_known_sku & ~has_description, "description_status"] = (
        "missing_source_description"
    )
    description_lookup = {
        sku: description.strip().casefold()
        for sku, description in zip(
            catalog_df["sku"], catalog_df["canonical_description"], strict=True
        )
    }
    exact_match = pd.Series(False, index=result.index)
    approved_alias_match = pd.Series(False, index=result.index)
    normalized_aliases = {
        sku.strip().upper(): {
            alias.strip().casefold()
            for alias in aliases
            if alias.strip()
        }
        for sku, aliases in PRODUCT_DESCRIPTION_ALIASES.items()
    }
    for index in result.index[has_known_sku & has_description]:
        sku = result.at[index, "canonical_sku"]
        source_text = source_normalized.at[index]
        if source_text == description_lookup[sku]:
            exact_match.at[index] = True
            result.at[index, "description_status"] = "same_text"
            result.at[index, "suggestion_similarity"] = 1.0
        elif source_text in normalized_aliases.get(sku.strip().upper(), set()):
            approved_alias_match.at[index] = True
            result.at[index, "description_status"] = "approved_alias"
            result.at[index, "suggestion_similarity"] = 1.0

    assessed_rows = result.loc[
        has_description & ~exact_match & ~approved_alias_match
    ]
    if not assessed_rows.empty:
        model = get_semantic_model()
        descriptions = assessed_rows["product_description"].astype(str).tolist()
        vendor_embeddings = model.encode(descriptions, convert_to_numpy=True)
        flattened_catalog = [
            (row.sku, row.canonical_description)
            for row in catalog_df.itertuples(index=False)
        ]
        catalog_embeddings = model.encode(
            [description for _, description in flattened_catalog],
            convert_to_numpy=True,
        )

        similarity_matrix = cosine_similarity(vendor_embeddings, catalog_embeddings)
        catalog_skus = np.asarray([sku for sku, _ in flattened_catalog])
        catalog_canonical_descriptions = np.asarray(
            [description for _, description in flattened_catalog]
        )
        for row_position, index in enumerate(assessed_rows.index):
            global_best = int(np.argmax(similarity_matrix[row_position]))
            best_sku = catalog_skus[global_best]
            best_score = float(similarity_matrix[row_position, global_best])
            if has_known_sku.at[index]:
                same_sku_mask = catalog_skus == result.at[index, "canonical_sku"]
                same_sku_scores = similarity_matrix[row_position, same_sku_mask]
                same_sku_best = float(np.max(same_sku_scores))
                result.at[index, "suggestion_similarity"] = same_sku_best
                if same_sku_best >= SEMANTIC_MIN_SIMILARITY:
                    result.at[index, "description_status"] = "semantic_match"
                else:
                    result.at[index, "description_status"] = "semantic_mismatch"
                    result.at[index, "needs_review"] = True
                    result.at[index, "match_status"] = "sku_description_mismatch"
                    if best_sku != result.at[index, "canonical_sku"]:
                        result.at[index, "suggested_sku"] = best_sku
                        result.at[index, "suggested_description"] = (
                            catalog_canonical_descriptions[global_best]
                        )
            else:
                result.at[index, "suggested_sku"] = best_sku
                result.at[index, "suggested_description"] = (
                    catalog_canonical_descriptions[global_best]
                )
                result.at[index, "suggestion_similarity"] = best_score
                result.at[index, "match_status"] = "candidate_review"

    return result.drop(columns=["sku_key"])


def assign_product_dimension_keys(
    orders_df: pd.DataFrame, dimensions_df: pd.DataFrame
) -> pd.DataFrame:
    """Resolve the product dimension version valid at each order's event time."""
    result = orders_df.copy()
    result["product_key"] = pd.Series(pd.NA, index=result.index, dtype="string")

    required_columns = {"sku", "product_key", "valid_from", "valid_to"}
    missing_columns = required_columns - set(dimensions_df.columns)
    if missing_columns:
        raise ValueError(
            f"Product dimensions are missing required columns: {sorted(missing_columns)}"
        )

    dimensions = dimensions_df.copy()
    dimensions["sku_key"] = dimensions["sku"].astype("string").str.strip().str.upper()
    dimensions["valid_from"] = pd.to_datetime(
        dimensions["valid_from"], errors="coerce", utc=True
    )
    dimensions["valid_to"] = pd.to_datetime(
        dimensions["valid_to"], errors="coerce", utc=True
    )
    event_times = pd.to_datetime(result["created_at"], errors="coerce", utc=True)
    matched_skus = result["canonical_sku"].astype("string").str.strip().str.upper()

    for dimension in dimensions.itertuples(index=False):
        if pd.isna(dimension.valid_from) or pd.isna(dimension.product_key):
            raise ValueError("Product dimension has an invalid key or valid_from value.")
        matches = (
            matched_skus.eq(dimension.sku_key)
            & event_times.ge(dimension.valid_from)
            & (event_times.lt(dimension.valid_to) | pd.isna(dimension.valid_to))
        )
        already_assigned = result["product_key"].notna()
        if (matches & already_assigned).any():
            raise ValueError(
                "Overlapping product dimension validity periods for "
                f"SKU {dimension.sku!r}."
            )
        result.loc[matches, "product_key"] = str(dimension.product_key)

    unresolved = result["canonical_sku"].notna() & result["product_key"].isna()
    result.loc[unresolved, "needs_review"] = True
    result.loc[unresolved, "match_status"] = "dimension_version_unavailable"
    return result


def score_chunk_anomalies(
    healed_chunk_df: pd.DataFrame, session: ort.InferenceSession, dynamic_threshold: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Combines numerical anomalies with unresolved product identities requiring review.
    """
    if healed_chunk_df.empty:
        return healed_chunk_df, healed_chunk_df

    if "status" not in healed_chunk_df.columns:
        raise ValueError("Order data must include status before anomaly scoring.")

    completed_orders = (
        healed_chunk_df["status"].astype("string").str.strip().str.upper().eq("COMPLETED")
    )
    numerical_anomaly = pd.Series(
        pd.NA, index=healed_chunk_df.index, dtype="boolean"
    )
    if completed_orders.any():
        features = list(ANOMALY_FEATURES)
        input_data = (
            healed_chunk_df.loc[completed_orders, features]
            .to_numpy()
            .astype(np.float32)
        )
        input_name = session.get_inputs()[0].name
        _, scores = session.run(None, {input_name: input_data})
        numerical_anomaly.loc[completed_orders] = (
            np.asarray(scores).reshape(-1) < dynamic_threshold
        )

    healed_chunk_df = healed_chunk_df.copy()
    healed_chunk_df["is_anomaly"] = numerical_anomaly
    needs_review = healed_chunk_df["needs_review"].to_numpy()

    is_any_anomaly = numerical_anomaly.fillna(False).to_numpy() | needs_review
    final_df = healed_chunk_df.copy()

    clean_chunk = final_df[~is_any_anomaly]
    quarantine_chunk = final_df[is_any_anomaly]

    return clean_chunk, quarantine_chunk


def calibrate_anomaly_threshold(scores: np.ndarray, target_anomaly_rate: float) -> float:
    """Choose a cutoff that flags no more than the target share of calibration rows."""
    if not 0.0 < target_anomaly_rate < 100.0:
        raise ValueError("target_anomaly_rate must be greater than 0 and less than 100.")

    values = np.asarray(scores, dtype=np.float64).reshape(-1)
    if values.size == 0 or not np.isfinite(values).all():
        raise ValueError("Calibration scores must be a non-empty finite array.")

    cutoff_index = int(np.floor(target_anomaly_rate / 100.0 * values.size))
    cutoff_index = min(cutoff_index, values.size - 1)
    return float(np.partition(values, cutoff_index)[cutoff_index])


def train_and_freeze_anomaly_model(
    model_output_path: str,
    pg_conn_str: str,
    target_anomaly_rate: float = 5.0,
    sample_size: int = 10,
) -> dict:
    """
    TRAINING PHASE: Uses DuckDB to connect directly to Postgres, samples raw
    completed records, trains an Isolation Forest, and exports it to ONNX format.
    """
    if not model_output_path:
        raise ValueError(f"Invalid model_output_path: {model_output_path}")
    if not pg_conn_str:
        raise ValueError(f"Undefined pg_conn_str: {pg_conn_str}")

    target_anomaly_rate = float(target_anomaly_rate)
    if not 0.0 < target_anomaly_rate < 100.0:
        raise ValueError("target_anomaly_rate must be greater than 0 and less than 100.")
    sample_size = safe_toint(sample_size, 10)

    features = list(ANOMALY_FEATURES)
    with duckdb.connect(":memory:") as conn:
        conn.execute("INSTALL postgres; LOAD postgres;")
        connection_string = pg_conn_str.replace("'", "''")
        conn.execute(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")

        query = dedent(f"""
            SELECT {", ".join(features)} 
            FROM pg_db.orders 
            WHERE status = 'COMPLETED'
            USING SAMPLE {sample_size} PERCENT (bernoulli);
        """).strip()
        sample_df = conn.execute(query).df()

    if sample_df.empty:
        raise ValueError(
            "No completed historical records found in Postgres to train the ML model!"
        )

    X = sample_df[features].astype(np.float32)
    if len(X) < 2:
        raise ValueError("At least two completed records are required for training.")
    X_train, X_calibration = train_test_split(
        X, test_size=0.2, random_state=42
    )

    iso_forest = IsolationForest(
        n_estimators=200,
        max_samples="auto",
        bootstrap=True,
        contamination="auto",
        random_state=42,
        n_jobs=-1,
    )
    iso_forest.fit(X_train)

    calibration_scores = iso_forest.decision_function(X_calibration)
    dynamic_threshold = calibrate_anomaly_threshold(
        calibration_scores, target_anomaly_rate
    )

    initial_type = [("float_input", FloatTensorType([None, len(features)]))]
    onnx_model = to_onnx(
        iso_forest,
        initial_types=initial_type,
        target_opset={
            "": 15,
            "ai.onnx.ml": 3,
        },
    )

    meta = onnx_model.metadata_props.add()
    meta.key = "dynamic_threshold"
    meta.value = str(float(dynamic_threshold))

    model_output_path = Path(model_output_path).resolve()
    model_output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_model_path = model_output_path.with_suffix(".tmp")
    try:
        with open(temp_model_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
        temp_model_path.rename(model_output_path)
    except Exception as e:
        if temp_model_path.exists():
            temp_model_path.unlink()
        raise e

    return {
        "status": "trained",
        "dynamic_threshold_locked": float(dynamic_threshold),
        "target_anomaly_rate": target_anomaly_rate,
    }
