import os
from dataclasses import dataclass

@dataclass
class Config:
    """Config lưu trữ các biến môi trường cấu hình cho Python ML Worker với nguyên tắc Fail-Close 100%"""
    kafka_brokers: str      # Danh sách Kafka Brokers (vd: "localhost:9092")
    kafka_topic_gold: str   # Topic Kafka phát tín hiệu Gold ready (vd: "pubg.v1.dataset.gold.ready")
    kafka_topic_model: str  # Topic Kafka phát tín hiệu Model ready (vd: "pubg.v1.ml.model.ready")
    minio_endpoint: str     # Endpoint S3 MinIO (vd: "http://localhost:9000")
    minio_bucket_data: str  # Bucket Data Lake (vd: "fps-anticheat-datalake")
    minio_bucket_model: str # Bucket Model Registry (vd: "pubg-models")
    minio_access_key: str   # Access Key MinIO S3
    minio_secret_key: str   # Secret Key MinIO S3

    @classmethod
    def from_env(cls) -> "Config":
        """Nạp biến môi trường với cơ chế Fail-Close bắt buộc các biến quan trọng phải tồn tại"""
        kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
        kafka_topic_gold = os.getenv("KAFKA_TOPIC_GOLD", "pubg.v1.dataset.gold.ready")
        kafka_topic_model = os.getenv("KAFKA_TOPIC_MODEL", "pubg.v1.ml.model.ready")
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        minio_bucket_data = os.getenv("MINIO_BUCKET_DATA", "fps-anticheat-datalake")
        minio_bucket_model = os.getenv("MINIO_BUCKET_MODEL", "pubg-models")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        return cls(
            kafka_brokers=kafka_brokers,
            kafka_topic_gold=kafka_topic_gold,
            kafka_topic_model=kafka_topic_model,
            minio_endpoint=minio_endpoint,
            minio_bucket_data=minio_bucket_data,
            minio_bucket_model=minio_bucket_model,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
        )
