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
        if not gold_object_key:
            # Quét file Parquet mới nhất từ bucket data lake
            response = self.s3.list_objects_v2(
                Bucket=self.config.minio_bucket_data,
                Prefix="gold/player-match-features/"
            )
            if "Contents" in response and len(response["Contents"]) > 0:
                # Lấy file mới nhất
                gold_object_key = response["Contents"][-1]["Key"]

        if not gold_object_key:
            raise FileNotFoundError(
                f"[FAIL-CLOSE] Không tìm thấy bất kỳ Gold Feature Parquet file nào trong s3://{self.config.minio_bucket_data}/gold/player-match-features/! Pipeline FAIL đỏ."
            )

        print(f"[STORAGE] Đang nạp Gold Feature Parquet từ s3://{self.config.minio_bucket_data}/{gold_object_key}")
        try:
            obj = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=gold_object_key)
            buffer = io.BytesIO(obj["Body"].read())
            return pd.read_parquet(buffer)
        except Exception as err:
            raise FileNotFoundError(
                f"[FAIL-CLOSE] Không nạp được Gold Feature Parquet từ s3://{self.config.minio_bucket_data}/{gold_object_key}: {err}"
            )

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
