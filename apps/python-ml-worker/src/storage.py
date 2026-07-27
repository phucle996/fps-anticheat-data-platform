import io
import os
import boto3
import pandas as pd
from src.config import Config

class StorageClient:
    """StorageClient đóng gói thao tác MinIO S3 SDK nạp Parquet và lưu trữ ONNX Model Bundle"""
    
    def __init__(self, config: Config):
        self.config = config
        # Khởi tạo boto3 S3 client với custom endpoint MinIO
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint,
            aws_access_key_id=config.minio_access_key,
            aws_secret_access_key=config.minio_secret_key,
        )

    def load_gold_dataset(self, gold_object_key: str = "") -> pd.DataFrame:
        """Nạp tập dữ liệu Gold Feature Parquet từ MinIO S3"""
        try:
            if not gold_object_key:
                # Nếu không chỉ định key, quét file Parquet mới nhất từ bucket data lake
                response = self.s3.list_objects_v2(
                    Bucket=self.config.minio_bucket_data,
                    Prefix="gold/player-match-features/"
                )
                if "Contents" in response and len(response["Contents"]) > 0:
                    gold_object_key = response["Contents"][-1]["Key"]
            
            if gold_object_key:
                obj = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=gold_object_key)
                buffer = io.BytesIO(obj["Body"].read())
                return pd.read_parquet(buffer)
        except Exception as err:
            print(f"[WARN] Không nạp được từ S3 ({err}), khởi tạo Mock Gold DataFrame cho local dev")
        
        # Mock Gold DataFrame nếu chưa có file S3 thực tế
        return pd.DataFrame({
            "match_id": ["m1", "m1", "m2", "m2", "m3", "m3", "m4", "m4", "m5", "m5"],
            "player_id": [f"p{i}" for i in range(10)],
            "kills": [12, 1, 5, 0, 25, 2, 8, 0, 15, 1],
            "damage_dealt": [1400.0, 110.0, 520.0, 0.0, 2800.0, 180.0, 900.0, 50.0, 1600.0, 120.0],
            "headshot_kills": [10, 0, 2, 0, 23, 0, 6, 0, 12, 0],
            "kills_per_minute": [0.60, 0.10, 0.30, 0.0, 1.25, 0.15, 0.40, 0.0, 0.75, 0.10],
            "damage_per_minute": [70.0, 11.0, 31.2, 0.0, 140.0, 15.0, 45.0, 5.0, 80.0, 12.0],
            "headshot_ratio": [0.833, 0.0, 0.400, 0.0, 0.920, 0.0, 0.750, 0.0, 0.800, 0.0],
            "damage_per_kill": [116.6, 110.0, 104.0, 0.0, 112.0, 90.0, 112.5, 0.0, 106.6, 120.0],
            "movement_per_minute": [175.0, 40.0, 108.0, 5.0, 250.0, 60.0, 140.0, 10.0, 190.0, 50.0],
            "performance_versus_lobby": [645.0, -645.0, 260.0, -260.0, 1310.0, -1310.0, 425.0, -425.0, 740.0, -740.0],
            "is_suspicious": [1, 0, 0, 0, 1, 0, 1, 0, 1, 0] # Label phục vụ supervised validation
        })

    def upload_model_bundle(self, version: str, bundle_files: dict):
        """Upload toàn bộ ONNX Model Bundle lên MinIO S3 Model Registry"""
        base_prefix = f"pubg-risk/versions/{version}/"
        
        # Đảm bảo bucket model registry tồn tại
        try:
            self.s3.create_bucket(Bucket=self.config.minio_bucket_model)
        except Exception:
            pass

        for filename, content in bundle_files.items():
            key = base_prefix + filename
            if isinstance(content, str):
                body = content.encode("utf-8")
            elif isinstance(content, bytes):
                body = content
            else:
                body = str(content).encode("utf-8")

            self.s3.put_object(
                Bucket=self.config.minio_bucket_model,
                Key=key,
                Body=body
            )
            print(f"[STORAGE SUCCESS] Uploaded artifact -> s3://{self.config.minio_bucket_model}/{key}")
