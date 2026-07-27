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

def test_dashboard_config_success():
    """Kiểm thử nạp biến môi trường thành công"""
    cfg = DashboardConfig.from_env()
    assert cfg.go_api_url == "http://localhost:8081"
    assert cfg.minio_bucket_data == "fps-anticheat-datalake"

def test_dashboard_config_fail_close():
    """Kiểm thử cơ chế Fail-Close: Ném ra ValueError khi thiếu biến môi trường bắt buộc"""
    del os.environ["GO_API_URL"]
    with pytest.raises(ValueError, match="FAIL-CLOSE TRIGGERED"):
        DashboardConfig.from_env()

def test_api_client_health():
    """Kiểm thử APIClient khởi tạo và xử lý khi API Gateway offline"""
    cfg = DashboardConfig.from_env()
    client = APIClient(cfg)
    health = client.check_health()
    assert "status" in health
