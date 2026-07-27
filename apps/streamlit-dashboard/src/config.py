import os
from dataclasses import dataclass

def _get_required_env(key: str) -> str:
    """Helper bắt buộc biến môi trường phải tồn tại (Fail-Close 100%, Zero Fallback)"""
    val = os.getenv(key)
    if not val:
        raise ValueError(f"[FAIL-CLOSE TRIGGERED] Thiếu biến môi trường bắt buộc '{key}' trong Streamlit Dashboard!")
    return val

@dataclass
class DashboardConfig:
    """DashboardConfig lưu trữ cấu hình môi trường cho Streamlit Dashboard với nguyên tắc Fail-Close 100%"""
    go_api_url: str        # URL REST API Gateway (vd: "http://localhost:8081")
    minio_endpoint: str    # Endpoint S3 MinIO (vd: "http://localhost:9000")
    minio_bucket_data: str # Bucket Data Lake chứa Parquet files (vd: "fps-anticheat-datalake")
    minio_access_key: str  # Access Key MinIO S3
    minio_secret_key: str  # Secret Key MinIO S3

    @classmethod
    def from_env(cls) -> "DashboardConfig":
        """Nạp biến môi trường với cơ chế Fail-Close 100% (Không chấp nhận fallback giá trị mặc định)"""
        return cls(
            go_api_url=_get_required_env("GO_API_URL"),
            minio_endpoint=_get_required_env("MINIO_ENDPOINT"),
            minio_bucket_data=_get_required_env("MINIO_BUCKET_DATA"),
            minio_access_key=_get_required_env("MINIO_ACCESS_KEY"),
            minio_secret_key=_get_required_env("MINIO_SECRET_KEY"),
        )
