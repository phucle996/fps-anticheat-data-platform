import sys
import json
import time
from datetime import datetime, timezone
from src.config import Config
from src.storage import StorageClient
from src.trainer import ModelTrainer
from src.onnx_exporter import ONNXExporter

def run_pipeline(config: Config, storage: StorageClient, gold_uri: str = ""):
    """Thực thi toàn bộ luồng pipeline: Load Gold -> Train -> Export ONNX -> Upload S3 -> Publish Event"""
    print(f"\n=================================================================")
    print(f"[ML WORKER PIPELINE] Kích hoạt lúc {datetime.now(timezone.utc).isoformat()}")
    print(f"=================================================================")

    # 1. Nạp dữ liệu Gold Feature Parquet
    df_gold = storage.load_gold_dataset(gold_uri)
    print(f"[DATA INFRA] Nạp thành công Gold Feature Dataset ({len(df_gold)} rows)")

    # 2. Huấn luyện các mô hình Machine Learning scikit-learn
    trainer = ModelTrainer()
    model, metrics = trainer.train_pipeline(df_gold)

    # 3. Đóng gói ONNX Model Bundle
    version = "v1"
    bundle_files = ONNXExporter.export_bundle(model, metrics, version=version)

    # 4. Upload toàn bộ artifacts lên MinIO S3 Model Registry
    storage.upload_model_bundle(version, bundle_files)

    # 5. Thông báo kết quả hoàn tất
    print(f"[ML WORKER SUCCESS] Đã phát tín hiệu pubg.v1.ml.model.ready cho Model Version '{version}'!")
    return version, metrics

def main():
    """Hàm main lắng nghe Kafka Events và khởi chạy Event-driven ML Pipeline"""
    config = Config.from_env()
    storage = StorageClient(config)

    print(f"[STARTUP] Python ML Worker Daemon khởi chạy (Kafka: {config.kafka_brokers})")

    # Thử kết nối Kafka Consumer
    try:
        from kafka import KafkaConsumer, KafkaProducer
        consumer = KafkaConsumer(
            config.kafka_topic_gold,
            bootstrap_servers=config.kafka_brokers.split(","),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="python-ml-worker-group"
        )
        producer = KafkaProducer(
            bootstrap_servers=config.kafka_brokers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(f"[KAFKA CONNECTED] Lắng nghe topic '{config.kafka_topic_gold}'...")

        for msg in consumer:
            payload = msg.value
            print(f"[KAFKA EVENT] Nhận tín hiệu dataset.gold.ready: {payload}")
            gold_uri = payload.get("object_uri", "")
            version, metrics = run_pipeline(config, storage, gold_uri)

            # Publish event ml.model.ready
            model_event = {
                "op": "ml.model.ready",
                "model_name": "pubg-risk",
                "model_version": version,
                "feature_version": "player-match-feature-v1",
                "bundle_uri": f"s3://{config.minio_bucket_model}/pubg-risk/versions/{version}/",
                "metrics": metrics,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            producer.send(config.kafka_topic_model, model_event)
            print(f"[KAFKA EVENT PUBLISHED] Đã gửi thông điệp ml.model.ready sang topic '{config.kafka_topic_model}'")
    except Exception as err:
        print(f"[WARN] Kafka connection timeout/error ({err}). Chạy trực tiếp 1 lần cho Dev Engine Mode...")
        run_pipeline(config, storage)

if __name__ == "__main__":
    main()
