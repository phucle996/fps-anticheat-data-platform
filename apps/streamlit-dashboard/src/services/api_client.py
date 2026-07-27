import requests
from typing import Dict, Any
from src.config import DashboardConfig

class APIClient:
    """APIClient quản lý gọi HTTP REST API tới Go API Gateway"""
    def __init__(self, config: DashboardConfig):
        self.base_url = config.go_api_url.rstrip("/")

    def check_health(self) -> Dict[str, Any]:
        """Gọi GET /api/v1/health kiểm tra trạng thái Go API và UDS socket"""
        try:
            resp = requests.get(f"{self.base_url}/api/v1/health", timeout=3)
            if resp.status_code == 200:
                return resp.json()
            return {"status": "DOWN", "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"status": "DOWN", "error": str(e)}

    def get_dataset_summary(self) -> Dict[str, Any]:
        """Gọi GET /api/v1/dataset/summary lấy thống kê dữ liệu Trước/Sau tiền xử lý"""
        try:
            resp = requests.get(f"{self.base_url}/api/v1/dataset/summary", timeout=3)
            if resp.status_code == 200:
                return resp.json()
            return {"status": "error", "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def predict_realtime(self, match_id: str, player_id: str, features: list) -> Dict[str, Any]:
        """Gọi POST /api/v1/predict để thực thi dự báo real-time qua UDS IPC"""
        try:
            payload = {
                "op": "predict",
                "match_id": match_id,
                "player_id": player_id,
                "features": features
            }
            resp = requests.post(f"{self.base_url}/api/v1/predict", json=payload, timeout=3)
            if resp.status_code == 200:
                return resp.json()
            return {"status": "error", "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"status": "error", "error": str(e)}
