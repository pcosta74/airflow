import numpy as np
import pandas as pd

import data_infra.ml.cleansing as cleansing
from data_infra.integrations.shop.product_aliases import PRODUCT_DESCRIPTION_ALIASES


class FakeInferenceSession:
    def __init__(self, scores=None):
        self.scores = scores
        self.inputs_seen = None

    def get_inputs(self):
        return [type("Input", (), {"name": "input"})()]

    def run(self, outputs, input_feed):
        self.inputs_seen = input_feed["input"]
        scores = self.scores
        if scores is None:
            scores = np.zeros(len(self.inputs_seen))
        return [None, np.asarray(scores).reshape(-1, 1)]


class FakeSemanticModel:
    def encode(self, texts, convert_to_numpy=True):
        vectors = {
            "vendor description": [1.0, 0.0],
            "vendor wording for A": [0.0, 1.0],
            "unrelated description": [0.0, 0.0],
            "Master Product A": [0.0, 1.0],
            "Master Product B": [1.0, 0.0],
        }
        return np.asarray([vectors[text] for text in texts])


def _make_catalog():
    return pd.DataFrame(
        [
            {
                "sku": "SKU-A",
                "canonical_description": "Master Product A",
            },
            {
                "sku": "SKU-B",
                "canonical_description": "Master Product B",
            },
        ]
    )


def test_known_sku_semantically_matches_approved_vendor_wording(monkeypatch):
    monkeypatch.setattr(cleansing, "get_semantic_model", FakeSemanticModel)
    source = pd.DataFrame(
        [
            {
                "order_id": 1,
                "sku": "SKU-A",
                "product_description": "vendor wording for A",
                "price": 12.0,
                "qty": 1,
                "status": "COMPLETED",
            }
        ]
    )

    result = cleansing.heal_chunk_metadata(source, _make_catalog())

    assert result.loc[0, "sku"] == "SKU-A"
    assert result.loc[0, "product_description"] == "vendor wording for A"
    assert result.loc[0, "canonical_sku"] == "SKU-A"
    assert result.loc[0, "canonical_description"] == "Master Product A"
    assert result.loc[0, "match_status"] == "sku_matched"
    assert result.loc[0, "description_status"] == "semantic_match"
    assert result.loc[0, "suggestion_similarity"] == 1.0
    assert not result.loc[0, "needs_review"]


def test_registered_product_aliases_are_accepted_without_rewriting_source(monkeypatch):
    monkeypatch.setattr(
        cleansing,
        "get_semantic_model",
        lambda: (_ for _ in ()).throw(AssertionError("aliases should bypass embeddings")),
    )
    catalog = pd.DataFrame(
        [
            {"sku": sku, "canonical_description": f"Canonical {sku}"}
            for sku in PRODUCT_DESCRIPTION_ALIASES
        ]
    )
    source = pd.DataFrame(
        [
            {
                "sku": sku,
                "product_description": alias,
            }
            for sku, aliases in PRODUCT_DESCRIPTION_ALIASES.items()
            for alias in aliases
        ]
    )

    result = cleansing.heal_chunk_metadata(source, catalog)

    assert len(result) == sum(map(len, PRODUCT_DESCRIPTION_ALIASES.values()))
    assert result["description_status"].eq("approved_alias").all()
    assert result["suggestion_similarity"].eq(1.0).all()
    assert not result["needs_review"].any()
    assert (
        result["product_description"].tolist()
        == source["product_description"].tolist()
    )


def test_registered_alias_for_wrong_sku_remains_under_semantic_review(monkeypatch):
    class AliasSemanticModel:
        def encode(self, texts, convert_to_numpy=True):
            vectors = {
                "HDMI Cable, 6 ft, 4K": [1.0, 0.0],
                "Canonical cable": [1.0, 0.0],
                "Canonical mouse": [0.0, 1.0],
            }
            return np.asarray([vectors[text] for text in texts])

    monkeypatch.setattr(cleansing, "get_semantic_model", AliasSemanticModel)
    catalog = pd.DataFrame(
        [
            {"sku": "CABLE-HDMI", "canonical_description": "Canonical cable"},
            {"sku": "MOUSE-WRLSS", "canonical_description": "Canonical mouse"},
        ]
    )
    source = pd.DataFrame(
        [
            {
                "sku": "MOUSE-WRLSS",
                "product_description": "HDMI Cable, 6 ft, 4K",
            }
        ]
    )

    result = cleansing.heal_chunk_metadata(source, catalog)

    assert result.loc[0, "description_status"] == "semantic_mismatch"
    assert result.loc[0, "match_status"] == "sku_description_mismatch"
    assert result.loc[0, "suggested_sku"] == "CABLE-HDMI"
    assert result.loc[0, "needs_review"]


def test_known_sku_with_low_similarity_is_quarantined(monkeypatch):
    monkeypatch.setattr(cleansing, "get_semantic_model", FakeSemanticModel)
    source = pd.DataFrame(
        [
            {
                "order_id": 3,
                "sku": "SKU-A",
                "product_description": "unrelated description",
                "price": 12.0,
                "qty": 1,
                "status": "COMPLETED",
            }
        ]
    )

    healed = cleansing.heal_chunk_metadata(source, _make_catalog())
    clean, quarantine = cleansing.score_chunk_anomalies(
        healed, FakeInferenceSession(), dynamic_threshold=-0.1
    )

    assert clean.empty
    assert quarantine["order_id"].tolist() == [3]
    assert quarantine.iloc[0]["description_status"] == "semantic_mismatch"
    assert quarantine.iloc[0]["match_status"] == "sku_description_mismatch"
    assert quarantine.iloc[0]["suggestion_similarity"] == 0.0
    assert quarantine.iloc[0]["needs_review"]


def test_unknown_sku_gets_review_candidate_without_source_rewrite(monkeypatch):
    monkeypatch.setattr(cleansing, "get_semantic_model", FakeSemanticModel)
    source = pd.DataFrame(
        [
            {
                "order_id": 2,
                "sku": "UNKNOWN",
                "product_description": "vendor description",
                "price": 12.0,
                "qty": 1,
                "status": "COMPLETED",
            }
        ]
    )

    result = cleansing.heal_chunk_metadata(source, _make_catalog())

    assert result.loc[0, "sku"] == "UNKNOWN"
    assert result.loc[0, "product_description"] == "vendor description"
    assert result.loc[0, "suggested_sku"] == "SKU-B"
    assert result.loc[0, "suggested_description"] == "Master Product B"
    assert result.loc[0, "suggestion_similarity"] == 1.0
    assert result.loc[0, "match_status"] == "candidate_review"
    assert result.loc[0, "description_status"] == "not_assessed"
    assert result.loc[0, "needs_review"]


def test_candidate_is_quarantined_with_source_values_and_suggestion_intact(monkeypatch):
    monkeypatch.setattr(cleansing, "get_semantic_model", FakeSemanticModel)
    source = pd.DataFrame(
        [
            {
                "order_id": 1,
                "sku": "SKU-A",
                "product_description": "Master Product A",
                "price": 12.0,
                "qty": 1,
                "status": "COMPLETED",
            },
            {
                "order_id": 2,
                "sku": "UNKNOWN",
                "product_description": "vendor description",
                "price": 12.0,
                "qty": 1,
                "status": "COMPLETED",
            },
        ]
    )
    healed = cleansing.heal_chunk_metadata(source, _make_catalog())

    clean, quarantine = cleansing.score_chunk_anomalies(
        healed, FakeInferenceSession(), dynamic_threshold=-0.1
    )

    assert clean["order_id"].tolist() == [1]
    assert quarantine["order_id"].tolist() == [2]
    assert quarantine.iloc[0]["sku"] == "UNKNOWN"
    assert quarantine.iloc[0]["product_description"] == "vendor description"
    assert quarantine.iloc[0]["suggested_sku"] == "SKU-B"
    assert quarantine.iloc[0]["needs_review"]


def test_only_completed_orders_are_scored_and_other_statuses_are_not_anomalies():
    source = pd.DataFrame(
        [
            {
                "order_id": 1,
                "status": "CANCELLED",
                "price": 450.0,
                "qty": 0,
                "needs_review": False,
            },
            {
                "order_id": 2,
                "status": "PENDING",
                "price": 450.0,
                "qty": 1,
                "needs_review": False,
            },
            {
                "order_id": 3,
                "status": "COMPLETED",
                "price": 450.0,
                "qty": 1,
                "needs_review": False,
            },
        ]
    )
    session = FakeInferenceSession(scores=[-0.2])

    clean, quarantine = cleansing.score_chunk_anomalies(
        source, session, dynamic_threshold=-0.1
    )

    assert len(session.inputs_seen) == 1
    assert clean["order_id"].tolist() == [1, 2]
    assert clean["is_anomaly"].isna().all()
    assert quarantine["order_id"].tolist() == [3]
    assert quarantine["is_anomaly"].tolist() == [True]


def test_anomaly_threshold_calibration_caps_training_false_positive_rate():
    scores = np.asarray([-0.9, -0.8, -0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0.0])

    threshold = cleansing.calibrate_anomaly_threshold(scores, target_anomaly_rate=20)

    assert threshold == -0.7
    assert int((scores < threshold).sum()) <= 2


def test_anomaly_threshold_calibration_rejects_invalid_scores_and_rate():
    for scores, rate in (([], 5), ([0.1, np.nan], 5), ([0.1], 0), ([0.1], 100)):
        try:
            cleansing.calibrate_anomaly_threshold(
                np.asarray(scores), target_anomaly_rate=rate
            )
        except ValueError:
            continue
        raise AssertionError("Invalid calibration inputs must be rejected")


def test_product_key_uses_dimension_version_valid_at_order_time():
    orders = pd.DataFrame(
        [
            {
                "canonical_sku": "SKU-A",
                "created_at": "2024-06-01T12:00:00",
                "needs_review": False,
                "match_status": "sku_matched",
            },
            {
                "canonical_sku": "SKU-A",
                "created_at": "2025-06-01T12:00:00",
                "needs_review": False,
                "match_status": "sku_matched",
            },
            {
                "canonical_sku": "SKU-A",
                "created_at": "2023-06-01T12:00:00",
                "needs_review": False,
                "match_status": "sku_matched",
            },
        ]
    )
    dimensions = pd.DataFrame(
        [
            {
                "sku": "SKU-A",
                "product_key": "SKU-A-v1",
                "valid_from": "2024-01-01",
                "valid_to": "2025-01-01",
            },
            {
                "sku": "SKU-A",
                "product_key": "SKU-A-v2",
                "valid_from": "2025-01-01",
                "valid_to": None,
            },
        ]
    )

    result = cleansing.assign_product_dimension_keys(orders, dimensions)

    assert result["product_key"].tolist()[:2] == ["SKU-A-v1", "SKU-A-v2"]
    assert pd.isna(result.loc[2, "product_key"])
    assert result.loc[2, "needs_review"]
    assert result.loc[2, "match_status"] == "dimension_version_unavailable"


def test_product_key_assignment_rejects_overlapping_dimension_versions():
    orders = pd.DataFrame(
        [
            {
                "canonical_sku": "SKU-A",
                "created_at": "2024-06-01",
                "needs_review": False,
                "match_status": "sku_matched",
            }
        ]
    )
    dimensions = pd.DataFrame(
        [
            {
                "sku": "SKU-A",
                "product_key": "SKU-A-v1",
                "valid_from": "2024-01-01",
                "valid_to": "2025-01-01",
            },
            {
                "sku": "SKU-A",
                "product_key": "SKU-A-v2",
                "valid_from": "2024-03-01",
                "valid_to": None,
            },
        ]
    )

    try:
        cleansing.assign_product_dimension_keys(orders, dimensions)
    except ValueError as exc:
        assert "Overlapping product dimension validity periods" in str(exc)
    else:
        raise AssertionError("Overlapping product versions must be rejected")
