import io
import hashlib
import os
import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pandas as pd

from src.config import Config

class StorageClient:
    """Quản lý kết nối MinIO S3 Object Storage và kích hoạt Atomic Local Symlink cho ONNX Model Bundle"""

    def __init__(self, config: Config):
        self.config = config
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint,
            aws_access_key_id=config.minio_access_key,
            aws_secret_access_key=config.minio_secret_key,
        )

    def load_gold_dataset(self, gold_uri: str = "") -> pd.DataFrame:
        """Nạp tập tin Gold Parquet duy nhất từ S3 URI"""
        bucket, object_key = self._resolve_gold_object(gold_uri)
        print(f"[STORAGE] Nạp Gold Parquet từ s3://{bucket}/{object_key}", flush=True)
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=object_key)
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))
        except Exception as err:
            raise FileNotFoundError(
                f"[FAIL-CLOSE] Không nạp được s3://{bucket}/{object_key}: {err}"
            ) from err

    def load_all_gold_datasets(self) -> pd.DataFrame:
        """Nạp và hợp nhất (concat) tất cả các tập tin Gold Parquet có trong S3 Data Lake"""
        response = self.s3.list_objects_v2(
            Bucket=self.config.minio_bucket_data,
            Prefix="gold/player-match-features/",
        )
        candidates = [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith((".parquet", ".csv"))
        ]
        if not candidates:
            raise FileNotFoundError("[FAIL-CLOSE] Không tìm thấy bất kỳ tập tin Gold dataset nào trên MinIO S3!")

        dfs = []
        for key in candidates:
            print(f"[STORAGE] Nạp Gold Parquet tập hợp từ s3://{self.config.minio_bucket_data}/{key}", flush=True)
            obj = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=key)
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    def upload_model_bundle(self, version: str, bundle_files: dict) -> str:
        """Upload toàn bộ các file trong Model Bundle (model.onnx, manifests) lên S3 Model Registry Bucket"""
        base_prefix = f"pubg-risk/versions/{version}/"
        self.s3.head_bucket(Bucket=self.config.minio_bucket_model)
        for filename, content in bundle_files.items():
            body = content.encode("utf-8") if isinstance(content, str) else bytes(content)
            key = base_prefix + filename
            self.s3.put_object(
                Bucket=self.config.minio_bucket_model,
                Key=key,
                Body=body,
            )
            print(f"[STORAGE] Uploaded s3://{self.config.minio_bucket_model}/{key}", flush=True)
        return f"s3://{self.config.minio_bucket_model}/{base_prefix}"

    def activate_local_bundle(self, version: str, bundle_files: dict) -> Path:
        """Kích hoạt Model Bundle cục bộ bằng Symlink Atomic để Rust Inference Engine không bị đọc dở dang file"""
        model_root = Path(self.config.model_dir)
        versions_dir = model_root / "versions"
        versions_dir.mkdir(parents=True, exist_ok=True)
        final_dir = versions_dir / version

        # Xây dựng trong thư mục staging tạm thời cùng filesystem rồi thực hiện rename Atomic
        staging = Path(tempfile.mkdtemp(prefix=f".{version}-", dir=versions_dir))
        try:
            for filename, content in bundle_files.items():
                destination = staging / filename
                body = content.encode("utf-8") if isinstance(content, str) else bytes(content)
                with destination.open("wb") as output:
                    output.write(body)
                    output.flush()
                    os.fsync(output.fileno())

            if final_dir.exists():
                existing_model = final_dir / "model.onnx"
                expected_model = bytes(bundle_files["model.onnx"])
                existing_checksum = (
                    hashlib.sha256(existing_model.read_bytes()).hexdigest()
                    if existing_model.is_file()
                    else ""
                )
                expected_checksum = hashlib.sha256(expected_model).hexdigest()
                if existing_checksum != expected_checksum:
                    raise RuntimeError(
                        f"[FAIL-CLOSE] Phát hiện va chạm Model Version Collision cho '{version}'!"
                    )
                shutil.rmtree(staging)
            else:
                os.replace(staging, final_dir)

            # Cập nhật Symlink `.current-next` -> `current` Atomic
            next_link = model_root / ".current-next"
            current_link = model_root / "current"
            if next_link.exists() or next_link.is_symlink():
                next_link.unlink()
            next_link.symlink_to(Path("versions") / version, target_is_directory=True)
            os.replace(next_link, current_link)
            return current_link
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    def _resolve_gold_object(self, gold_uri: str) -> tuple[str, str]:
        """Giải mã và kiểm tra tính an toàn bảo mật của S3 URI (Trust Boundary Guard)"""
        if gold_uri:
            parsed = urlparse(gold_uri)
            if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
                raise ValueError("[FAIL-CLOSE] object_uri phải có dạng s3://bucket/key!")
            if parsed.netloc != self.config.minio_bucket_data:
                raise ValueError(
                    f"[FAIL-CLOSE] Bucket '{parsed.netloc}' không nằm trong danh sách được phép đọc Gold input!"
                )
            key = parsed.path.lstrip("/")
            if not key.startswith("gold/player-match-features/"):
                raise ValueError("[FAIL-CLOSE] Gold object nằm ngoài prefix cho phép!")
            return parsed.netloc, key

        response = self.s3.list_objects_v2(
            Bucket=self.config.minio_bucket_data,
            Prefix="gold/player-match-features/",
        )
        candidates = [
            item
            for item in response.get("Contents", [])
            if item["Key"].endswith((".parquet", ".csv"))
        ]
        if not candidates:
            raise FileNotFoundError("[FAIL-CLOSE] Không tìm thấy Gold dataset trên S3!")
        newest = max(candidates, key=lambda item: item["LastModified"])
        return self.config.minio_bucket_data, newest["Key"]
