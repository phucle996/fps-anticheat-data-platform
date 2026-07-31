import os
from dataclasses import dataclass

def _get_required_env(key: str) -> str:
    """Helper bắt buộc biến môi trường phải tồn tại (Fail-Close 100%, Zero Fallback)"""
    val = os.getenv(key)
    if not val:
        raise ValueError(f"[FAIL-CLOSE TRIGGERED] Thiếu biến môi trường bắt buộc '{key}'!")
    return val

@dataclass
class Config:
    """Config lưu trữ các biến môi trường cấu hình cho Python ML Worker với nguyên tắc Fail-Close 100%"""
    kafka_brokers: str      # Danh sách Kafka Brokers (vd: "localhost:9092")
    kafka_topic_gold: str   # Topic Kafka phát tín hiệu Gold ready (vd: "pubg.v1.dataset.gold.ready")
    kafka_topic_model: str  # Topic Kafka phát tín hiệu Model ready (vd: "pubg.v1.ml.model.ready")
    kafka_topic_ml_dlq: str # Durable DLQ cho training event đã hết retry
    kafka_group_id: str
    minio_endpoint: str     # Endpoint S3 MinIO (vd: "http://localhost:9000")
    minio_bucket_data: str  # Bucket Data Lake (vd: "fps-anticheat-datalake")
    minio_bucket_model: str # Bucket Model Registry (vd: "pubg-models")
    minio_access_key: str   # Access Key MinIO S3
    minio_secret_key: str   # Secret Key MinIO S3
    model_dir: str          # Shared volume root dùng để activate model atomic
    ml_max_retries: int

    @classmethod
    def from_env(cls) -> "Config":
        """Nạp biến môi trường với cơ chế Fail-Close 100% (Không chấp nhận fallback giá trị mặc định)"""
        config = cls(
            kafka_brokers=_get_required_env("KAFKA_BROKERS"),
            kafka_topic_gold=_get_required_env("KAFKA_TOPIC_GOLD"),
            kafka_topic_model=_get_required_env("KAFKA_TOPIC_MODEL"),
            kafka_topic_ml_dlq=_get_required_env("KAFKA_TOPIC_ML_DLQ"),
            kafka_group_id=_get_required_env("KAFKA_ML_GROUP_ID"),
            minio_endpoint=_get_required_env("MINIO_ENDPOINT"),
            minio_bucket_data=_get_required_env("MINIO_BUCKET_DATA"),
            minio_bucket_model=_get_required_env("MINIO_BUCKET_MODEL"),
            minio_access_key=_get_required_env("MINIO_ACCESS_KEY"),
            minio_secret_key=_get_required_env("MINIO_SECRET_KEY"),
            model_dir=_get_required_env("MODEL_ROOT"),
            ml_max_retries=int(_get_required_env("ML_MAX_RETRIES")),
        )
        if config.ml_max_retries < 1 or config.ml_max_retries > 20:
            raise ValueError("[FAIL-CLOSE TRIGGERED] ML_MAX_RETRIES phải trong [1,20]")
        return config
