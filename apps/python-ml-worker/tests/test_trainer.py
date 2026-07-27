import os
import pytest
import pandas as pd
from src.config import Config
from src.storage import StorageClient
from src.trainer import ModelTrainer, FEATURE_CONTRACT
from src.onnx_exporter import ONNXExporter

@pytest.fixture(autouse=True)
def setup_env():
    """Thiết lập các biến môi trường bắt buộc phục vụ kiểm thử (Fail-Close Enforced)"""
    os.environ["KAFKA_BROKERS"] = "localhost:9092"
    os.environ["KAFKA_TOPIC_GOLD"] = "pubg.v1.dataset.gold.ready"
    os.environ["KAFKA_TOPIC_MODEL"] = "pubg.v1.ml.model.ready"
    os.environ["MINIO_ENDPOINT"] = "http://localhost:9000"
    os.environ["MINIO_BUCKET_DATA"] = "fps-anticheat-datalake"
    os.environ["MINIO_BUCKET_MODEL"] = "pubg-models"
    os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = "minioadmin"

def test_trainer_pipeline():
    """Kiểm thử pipeline huấn luyện ML và đánh giá các chỉ số"""
    config = Config.from_env()
    storage = StorageClient(config)
    
    # Nạp dữ liệu Gold DataFrame
    df = storage.load_gold_dataset()
    assert len(df) > 0
    assert all(feat in df.columns for feat in FEATURE_CONTRACT)

    # Kích hoạt huấn luyện
    trainer = ModelTrainer()
    model, metrics = trainer.train_pipeline(df)

    # Đảm bảo các chỉ số đánh giá hợp lệ
    assert metrics["model_name"] == "RandomForestClassifier"
    assert "pr_auc" in metrics
    assert "f1_score" in metrics
    assert metrics["pr_auc"] >= 0.0
    assert metrics["f1_score"] >= 0.0

def test_onnx_export_bundle():
    """Kiểm thử đóng gói ONNX Model Bundle và các manifest metadata"""
    config = Config.from_env()
    storage = StorageClient(config)
    
    df = storage.load_gold_dataset()
    trainer = ModelTrainer()
    model, metrics = trainer.train_pipeline(df)

    # Đóng gói ONNX bundle
    bundle = ONNXExporter.export_bundle(model, metrics, version="v1_test")

    # Đảm bảo có đầy đủ 6 file thành phần trong bundle
    assert "model.onnx" in bundle
    assert "feature_schema.json" in bundle
    assert "threshold_policy.json" in bundle
    assert "metrics.json" in bundle
    assert "training_manifest.json" in bundle
    assert "checksums.sha256" in bundle

    # Kiểm tra kích thước file không rỗng
    assert len(bundle["model.onnx"]) > 0
    assert len(bundle["feature_schema.json"]) > 0

def test_config_fail_close():
    """Kiểm thử cơ chế Fail-Close: Ném ra ValueError khi thiếu biến môi trường"""
    del os.environ["KAFKA_BROKERS"]
    with pytest.raises(ValueError, match="FAIL-CLOSE TRIGGERED"):
        Config.from_env()
