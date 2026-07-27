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

def test_risk_analysis_page_import():
    """Kiểm thử nạp module Risk Analysis view thành công"""
    from src.views.risk_analysis import render_risk_analysis_page
    assert callable(render_risk_analysis_page)
