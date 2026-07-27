import pytest
import pandas as pd
from src.config import Config
from src.storage import StorageClient
from src.trainer import ModelTrainer, FEATURE_CONTRACT
from src.onnx_exporter import ONNXExporter

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
