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
    """MinIO registry + local atomic activation cho model bundle."""

    def __init__(self, config: Config):
        self.config = config
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint,
            aws_access_key_id=config.minio_access_key,
            aws_secret_access_key=config.minio_secret_key,
        )

    def load_gold_dataset(self, gold_uri: str = "") -> pd.DataFrame:
        bucket, object_key = self._resolve_gold_object(gold_uri)
        print(f"[STORAGE] Nạp Gold Parquet từ s3://{bucket}/{object_key}")
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=object_key)
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))
        except Exception as err:
            raise FileNotFoundError(
                f"[FAIL-CLOSE] Không nạp được s3://{bucket}/{object_key}: {err}"
            ) from err

    def load_all_gold_datasets(self) -> pd.DataFrame:
        """Nạp và tổng hợp (concat) tất cả các tập tin Gold Parquet có sẵn trong MinIO S3."""
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
            raise FileNotFoundError("[FAIL-CLOSE] Không tìm thấy bất kỳ tập tin Gold dataset nào trên MinIO S3")
        
        dfs = []
        for key in candidates:
            print(f"[STORAGE] Nạp Gold Parquet tập hợp từ s3://{self.config.minio_bucket_data}/{key}")
            obj = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=key)
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            dfs.append(df)
            
        # Concat toàn bộ DataFrames từ tất cả các file Gold trong S3 Data Lake
        return pd.concat(dfs, ignore_index=True)

    def get_ml_checkpoint(self) -> dict:
        """Tải file checkpoint trạng thái huấn luyện ML từ MinIO S3 (manifests/ml-training-checkpoint.json)."""
        checkpoint_key = "manifests/ml-training-checkpoint.json"
        try:
            obj = self.s3.get_object(Bucket=self.config.minio_bucket_data, Key=checkpoint_key)
            import json
            return json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            # Trả về checkpoint rỗng mặc định nếu chưa từng tạo checkpoint
            return {"processed_gold_files": [], "last_version": "", "total_samples": 0}

    def save_ml_checkpoint(self, version: str, processed_files: list[str], total_samples: int) -> None:
        """Lưu trạng thái checkpoint huấn luyện mới lên MinIO S3 (Atomicity & Persistence)."""
        checkpoint_key = "manifests/ml-training-checkpoint.json"
        import json
        from datetime import datetime, timezone

        checkpoint_data = {
            "last_trained_at": datetime.now(timezone.utc).isoformat(),
            "last_version": version,
            "processed_gold_files": sorted(list(set(processed_files))),
            "total_samples": total_samples,
        }

        self.s3.put_object(
            Bucket=self.config.minio_bucket_data,
            Key=checkpoint_key,
            Body=json.dumps(checkpoint_data, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[STORAGE] Đã lưu ML Training Checkpoint mới lên s3://{self.config.minio_bucket_data}/{checkpoint_key}")

    def check_unprocessed_gold_files(self) -> tuple[bool, list[str], list[str]]:
        """So sánh danh sách file Gold hiện có trên S3 với Checkpoint để phát hiện các file mới chưa được train."""
        response = self.s3.list_objects_v2(
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

    def upload_model_bundle(self, version: str, bundle_files: dict) -> str:
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
            print(f"[STORAGE] Uploaded s3://{self.config.minio_bucket_model}/{key}")
        return f"s3://{self.config.minio_bucket_model}/{base_prefix}"

    def activate_local_bundle(self, version: str, bundle_files: dict) -> Path:
        model_root = Path(self.config.model_dir)
        versions_dir = model_root / "versions"
        versions_dir.mkdir(parents=True, exist_ok=True)
        final_dir = versions_dir / version

        # Build trong staging cùng filesystem rồi rename; inference không bao giờ
        # quan sát bundle dở dang. Duplicate cùng version chỉ thay desired state.
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
                        f"[FAIL-CLOSE] Model version collision cho '{version}'"
                    )
                shutil.rmtree(staging)
            else:
                os.replace(staging, final_dir)

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
        if gold_uri:
            parsed = urlparse(gold_uri)
            if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
                raise ValueError("[FAIL-CLOSE] object_uri phải có dạng s3://bucket/key")
            if parsed.netloc != self.config.minio_bucket_data:
                # Event Kafka là trust boundary; không cho phép đọc bucket tùy ý
                # bằng credential của ML worker.
                raise ValueError(
                    f"[FAIL-CLOSE] Bucket '{parsed.netloc}' không được phép cho Gold input"
                )
            key = parsed.path.lstrip("/")
            if not key.startswith("gold/player-match-features/"):
                raise ValueError("[FAIL-CLOSE] Gold object nằm ngoài prefix cho phép")
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
            raise FileNotFoundError("[FAIL-CLOSE] Không tìm thấy Gold dataset")
        newest = max(candidates, key=lambda item: item["LastModified"])
        return self.config.minio_bucket_data, newest["Key"]
