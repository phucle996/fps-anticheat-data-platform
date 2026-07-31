import os

import numpy as np
import pandas as pd
import pytest

from src.config import Config
from src.onnx_exporter import ONNXExporter
from src.storage import StorageClient
from src.trainer import FEATURE_CONTRACT, ModelTrainer


@pytest.fixture
def config_env(monkeypatch):
    values = {
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_TOPIC_GOLD": "pubg.v1.dataset.gold.ready",
        "KAFKA_TOPIC_MODEL": "pubg.v1.ml.model.ready",
        "KAFKA_TOPIC_ML_DLQ": "pubg.v1.ml.dlq",
        "KAFKA_ML_GROUP_ID": "ml-worker-test",
        "MINIO_ENDPOINT": "http://localhost:9000",
        "MINIO_BUCKET_DATA": "fps-anticheat-datalake",
        "MINIO_BUCKET_MODEL": "pubg-models",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "MODEL_ROOT": "/tmp/fps-anticheat-test-models",
        "ML_MAX_RETRIES": "3",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def sample_gold(rows=80):
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "match_id": [f"match-{index // 8}" for index in range(rows)],
            "player_id": [f"player-{index}" for index in range(rows)],
            "kills": rng.integers(0, 12, rows),
            "minimum_kill_interval_seconds": rng.uniform(0, 999, rows),
            "median_kill_distance_coordinate_units": rng.uniform(0, 500, rows),
            "short_kill_interval_count": rng.integers(0, 6, rows),
            "unique_weapons_used": rng.integers(0, 8, rows),
        }
    )


def test_config_fail_close(config_env, monkeypatch):
    monkeypatch.delenv("KAFKA_BROKERS")
    with pytest.raises(ValueError, match="FAIL-CLOSE TRIGGERED"):
        Config.from_env()


def test_trainer_pipeline_is_deterministic_and_contract_complete(config_env):
    model, metrics = ModelTrainer().train_pipeline(sample_gold())
    assert "XGBClassifier" in metrics["model_name"] or "IsolationForest" in metrics["model_name"]
    assert metrics["features_used"] == FEATURE_CONTRACT
    assert metrics["pseudo_anomaly_count"] > 0


def test_onnx_export_bundle_contains_valid_model(config_env):
    model, metrics = ModelTrainer().train_pipeline(sample_gold())
    bundle = ONNXExporter.export_bundle(model, metrics, version="v-test")
    assert {
        "model.onnx",
        "feature_schema.json",
        "threshold_policy.json",
        "training_manifest.json",
        "metrics.json",
        "checksums.sha256",
    } <= set(bundle)

    import onnx
    import onnxruntime as ort

    onnx_model = onnx.load_from_string(bundle["model.onnx"])
    onnx.checker.check_model(onnx_model)
    session = ort.InferenceSession(bundle["model.onnx"], providers=["CPUExecutionProvider"])
    output = session.run(
        None, {session.get_inputs()[0].name: sample_gold()[FEATURE_CONTRACT].iloc[:2].to_numpy(dtype=np.float32)}
    )
    assert output[0].shape == (2,)
    assert output[1].shape == (2, 2)


def test_gold_uri_security_boundary(config_env):
    storage = StorageClient(Config.from_env())
    assert storage._resolve_gold_object(
        "s3://fps-anticheat-datalake/gold/player-match-features/features.parquet"
    )[1].startswith("gold/player-match-features/")
    with pytest.raises(ValueError):
        storage._resolve_gold_object("s3://other-bucket/private/model.parquet")
    with pytest.raises(ValueError):
        storage._resolve_gold_object("s3://fps-anticheat-datalake/bronze/raw.parquet")
