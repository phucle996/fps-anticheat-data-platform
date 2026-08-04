import json
from datetime import datetime, timezone

from src.config import Config
from src.storage.s3_client import StorageClient

class CheckpointManager:
    """Quản lý đọc/ghi Checkpoint State cho tiến trình huấn luyện ML trên S3 (manifests/ml-training-checkpoint.json)"""

    def __init__(self, config: Config, storage_client: StorageClient):
        self.config = config
        self.storage = storage_client
        self.checkpoint_key = "manifests/ml-training-checkpoint.json"

    def get_ml_checkpoint(self) -> dict:
        """Tải file checkpoint trạng thái huấn luyện ML từ MinIO S3"""
        try:
            obj = self.storage.s3.get_object(
                Bucket=self.config.minio_bucket_data,
                Key=self.checkpoint_key
            )
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            # Trả về checkpoint rỗng mặc định nếu chưa từng tạo checkpoint
            return {"processed_gold_files": [], "last_version": "", "total_samples": 0}

    def save_ml_checkpoint(self, version: str, processed_files: list[str], total_samples: int) -> None:
        """Lưu trạng thái checkpoint huấn luyện mới lên MinIO S3 (Atomicity & Persistence)"""
        checkpoint_data = {
            "last_trained_at": datetime.now(timezone.utc).isoformat(),
            "last_version": version,
            "processed_gold_files": sorted(list(set(processed_files))),
            "total_samples": total_samples,
        }

        self.storage.s3.put_object(
            Bucket=self.config.minio_bucket_data,
            Key=self.checkpoint_key,
            Body=json.dumps(checkpoint_data, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[CHECKPOINT] Đã lưu ML Training Checkpoint mới lên s3://{self.config.minio_bucket_data}/{self.checkpoint_key}", flush=True)

    def check_unprocessed_gold_files(self) -> tuple[bool, list[str], list[str]]:
        """So sánh danh sách file Gold hiện có trên S3 với Checkpoint để phát hiện các file mới chưa từng huấn luyện"""
        response = self.storage.s3.list_objects_v2(
            Bucket=self.config.minio_bucket_data,
            Prefix="gold/player-match-features/",
        )
        all_candidates = sorted([
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith((".parquet", ".csv"))
        ])

        if not all_candidates:
            return False, [], []

        checkpoint = self.get_ml_checkpoint()
        processed_set = set(checkpoint.get("processed_gold_files", []))

        # Tìm các file Gold Parquet mới chưa từng đưa vào phiên train trước
        new_files = [f for f in all_candidates if f not in processed_set]
        has_new = len(new_files) > 0

        return has_new, all_candidates, new_files
