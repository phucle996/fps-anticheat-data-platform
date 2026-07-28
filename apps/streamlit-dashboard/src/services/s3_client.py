import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
from typing import Optional
from src.config import DashboardConfig

class S3DataClient:
    """S3DataClient quản lý đọc trực tiếp các file dữ liệu Parquet từ MinIO S3 Data Lake"""
    def __init__(self, config: DashboardConfig):
        self.config = config
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint,
            aws_access_key_id=config.minio_access_key,
            aws_secret_access_key=config.minio_secret_key,
        )

    def load_parquet_dataframe(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Tải và chuyển đổi file Parquet từ MinIO S3 thành Pandas DataFrame"""
        try:
            response = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=s3_key)
            parquet_bytes = response["Body"].read()
            table = pq.read_table(io.BytesIO(parquet_bytes))
            return table.to_pandas()
        except Exception:
            return None

    def list_manifests(self) -> list:
        """Đọc toàn bộ danh sách BatchManifest JSON thực tế từ s3://fps-anticheat-datalake/manifests/"""
        manifests = []
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.config.minio_bucket_data, Prefix="manifests/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".json"):
                        import json
                        res = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=key)
                        content = res["Body"].read().decode("utf-8")
                        manifests.append(json.loads(content))
        except Exception:
            pass
        return manifests

    def load_invalid_records(self) -> list:
        """Đọc tất cả bản ghi không hợp lệ thực tế từ s3://fps-anticheat-datalake/bronze/invalid/"""
        invalid_list = []
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.config.minio_bucket_data, Prefix="bronze/invalid/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".json"):
                        import json
                        res = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=key)
                        content = res["Body"].read().decode("utf-8")
                        data = json.loads(content)
                        if isinstance(data, list):
                            invalid_list.extend(data)
                        elif isinstance(data, dict):
                            invalid_list.append(data)
        except Exception:
            pass
        return invalid_list

    def load_gold_features_dataframe(self) -> Optional[pd.DataFrame]:
        """Tải toàn bộ dữ liệu Gold Features từ s3://fps-anticheat-datalake/gold/player-match-features/"""
        dfs = []
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.config.minio_bucket_data, Prefix="gold/player-match-features/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".parquet") or key.endswith(".csv"):
                        df = self.load_parquet_dataframe(key)
                        if df is not None:
                            dfs.append(df)
        except Exception:
            pass
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return None

    def load_kill_events_dataframe(self) -> Optional[pd.DataFrame]:
        """Tải toàn bộ nhật ký Telemetry Kill Events từ s3://fps-anticheat-datalake/silver/kill-events/"""
        dfs = []
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.config.minio_bucket_data, Prefix="silver/kill-events/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".parquet") or key.endswith(".csv"):
                        df = self.load_parquet_dataframe(key)
                        if df is not None:
                            dfs.append(df)
        except Exception:
            pass
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return None
