import json
import hashlib
import boto3
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from src.trainer import FEATURE_CONTRACT
from src.config import Config

class ONNXExporter:
    """ONNXExporter đóng gói chuyển đổi mô hình ML sang định dạng ONNX, xuất Bundle Manifest và upload MinIO S3 Model Bucket"""

    @staticmethod
    def export_bundle(model: Any, metrics: Dict[str, Any], version: str = "v1") -> Dict[str, bytes]:
        """Xuất toàn bộ ONNX Model Bundle hoàn chỉnh"""
        print(f"[ONNX EXPORTER] Đóng gói ONNX Model Bundle phiên bản {version}...")

        # Lấy danh sách đặc trưng thực tế đã dùng khi huấn luyện
        features_used = metrics.get("features_used", FEATURE_CONTRACT)

        # 1. Chuyển đổi mô hình scikit-learn sang ONNX format
        onnx_bytes = ONNXExporter._convert_to_onnx(model, feature_cols=features_used)

        # 2. Tạo feature_schema.json
        feature_schema = {
            "model_name": "pubg-risk",
            "model_version": version,
            "feature_version": "kill-event-player-match-v1",
            "input_dtype": "float32",
            "input_shape": ["batch_size", len(features_used)],
            "features": features_used
        }

        # 3. Tạo threshold_policy.json (Quy định phân vùng rủi ro Risk Level)
        threshold_policy = {
            "model_version": version,
            "thresholds": {
                "LOW": 0.30,
                "MEDIUM": 0.60,
                "HIGH": 0.80,
                "CRITICAL": 0.95
            },
            "default_action": "flag_for_review"
        }

        # 4. Tạo training_manifest.json sử dụng UTC timezone chuẩn hóa
        training_manifest = {
            "model_name": "pubg-risk",
            "model_version": version,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics
        }

        # 5. Tính checksum SHA-256 cho từng file trong bundle
        onnx_sha256 = hashlib.sha256(onnx_bytes).hexdigest()
        schema_bytes = json.dumps(feature_schema, indent=2).encode("utf-8")
        policy_bytes = json.dumps(threshold_policy, indent=2).encode("utf-8")
        metrics_bytes = json.dumps(metrics, indent=2).encode("utf-8")
        manifest_bytes = json.dumps(training_manifest, indent=2).encode("utf-8")

        checksums = {
            "model.onnx": onnx_sha256,
            "feature_schema.json": hashlib.sha256(schema_bytes).hexdigest(),
            "threshold_policy.json": hashlib.sha256(policy_bytes).hexdigest(),
            "metrics.json": hashlib.sha256(metrics_bytes).hexdigest(),
            "training_manifest.json": hashlib.sha256(manifest_bytes).hexdigest(),
        }
        checksum_bytes = json.dumps(checksums, indent=2).encode("utf-8")

        print(f"[ONNX EXPORTER SUCCESS] SHA-256 model.onnx: {onnx_sha256}")

        return {
            "model.onnx": onnx_bytes,
            "feature_schema.json": schema_bytes,
            "threshold_policy.json": policy_bytes,
            "metrics.json": metrics_bytes,
            "training_manifest.json": manifest_bytes,
            "checksums.sha256": checksum_bytes,
        }

    @staticmethod
    def upload_bundle_to_minio(bundle: Dict[str, bytes], config: Config, version: str = "v1") -> bool:
        """Upload toàn bộ Model Bundle (model.onnx, manifests) lên MinIO S3 bucket pubg-models đảm bảo tính bền vững khi restart"""
        try:
            print(f"[MINIO UPLOAD] Đang đẩy ONNX Model Bundle '{version}' lên S3 Bucket '{config.minio_bucket_model}'...")
            s3 = boto3.client(
                "s3",
                endpoint_url=config.minio_endpoint,
                aws_access_key_id=config.minio_access_key,
                aws_secret_access_key=config.minio_secret_key,
            )
            for file_name, content_bytes in bundle.items():
                s3_key = f"{version}/{file_name}"
                s3.put_object(
                    Bucket=config.minio_bucket_model,
                    Key=s3_key,
                    Body=content_bytes
                )
                print(f"  └─ Uploaded s3://{config.minio_bucket_model}/{s3_key}")
            print(f"[MINIO UPLOAD SUCCESS] Đã lưu trữ model.onnx thành công lên MinIO S3!")
            return True
        except Exception as err:
            print(f"[WARN] Upload MinIO S3 thất bại ({err}) - Đã bỏ qua cho môi trường dev local")
            return False

    @staticmethod
    def _convert_to_onnx(model: Any, feature_cols: list = None) -> bytes:
        """Chuyển đổi mô hình scikit-learn sang ONNX bytes bằng skl2onnx và validate bằng onnx.checker"""
        if feature_cols is None:
            feature_cols = FEATURE_CONTRACT

        try:
            from skl2onnx import convert_sklearn
            from skl2onnx.common.data_types import FloatTensorType
            import onnx

            initial_type = [("float_input", FloatTensorType([None, len(feature_cols)]))]
            # ZipMap tạo map output khó tối ưu ở Rust; tensor [batch, classes]
            # giữ contract byte/shape ổn định giữa Python và ONNX Runtime.
            onnx_model = convert_sklearn(
                model,
                initial_types=initial_type,
                options={id(model): {"zipmap": False}},
            )

            # Validate ONNX model hợp lệ
            onnx.checker.check_model(onnx_model)

            onnx_bytes = onnx_model.SerializeToString()
            print(f"[ONNX EXPORTER] Conversion & Validation ONNX model thành công! ({len(onnx_bytes)} bytes)")
            return onnx_bytes

        except Exception as err:
            raise RuntimeError(
                f"[FAIL-CLOSE] Chuyển đổi mô hình sang ONNX thất bại ({err})! Pipeline FAIL đỏ, không tạo mock model."
            )
