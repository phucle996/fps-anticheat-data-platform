import os
import pytest
from src.config import DashboardConfig
from src.services.api_client import APIClient
from src.services.s3_client import S3DataClient

@pytest.fixture(autouse=True)
def setup_env():
    """Thiết lập các biến môi trường kiểm thử Fail-Close"""
    os.environ["GO_API_URL"] = "http://localhost:8081"
    os.environ["MINIO_ENDPOINT"] = "http://localhost:9000"
    os.environ["MINIO_BUCKET_DATA"] = "fps-anticheat-datalake"
    os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = "minioadmin"

def test_s3_client_initialization():
    """Kiểm thử S3DataClient khởi tạo thành công"""
    cfg = DashboardConfig.from_env()
    client = S3DataClient(cfg)
    assert client.config.minio_bucket_data == "fps-anticheat-datalake"
