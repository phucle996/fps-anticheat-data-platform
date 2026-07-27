import os
import pytest
from src.config import DashboardConfig
from src.services.api_client import APIClient

@pytest.fixture(autouse=True)
def setup_env():
    """Thiết lập các biến môi trường kiểm thử Fail-Close"""
    os.environ["GO_API_URL"] = "http://localhost:8081"
    os.environ["MINIO_ENDPOINT"] = "http://localhost:9000"
    os.environ["MINIO_BUCKET_DATA"] = "fps-anticheat-datalake"
    os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = "minioadmin"

def test_api_client_predict_realtime_structure():
    """Kiểm thử APIClient predict_realtime cấu trúc dữ liệu"""
    cfg = DashboardConfig.from_env()
    client = APIClient(cfg)
    resp = client.predict_realtime("match_100", "player_test", [1.5, 140.0, 0.95, 120.0, 250.0, 800.0])
    assert isinstance(resp, dict)
