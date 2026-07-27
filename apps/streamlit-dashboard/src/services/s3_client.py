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
