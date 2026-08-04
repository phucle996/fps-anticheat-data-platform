import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any

from src.pipeline.trainer import FEATURE_CONTRACT

class ONNXExporter:
    """Đóng gói chuyển đổi mô hình ML sang định dạng ONNX, tính checksum SHA-256 và xuất Model Bundle hoàn chỉnh"""

    @staticmethod
    def export_bundle(model: Any, metrics: Dict[str, Any], version: str = "v1") -> Dict[str, bytes]:
        """Xuất toàn bộ ONNX Model Bundle hoàn chỉnh"""
        print(f"[ONNX EXPORTER] Đóng gói ONNX Model Bundle phiên bản {version}...", flush=True)

        # Lấy danh sách đặc trưng thực tế đã dùng khi huấn luyện
        features_used = metrics.get("features_used", FEATURE_CONTRACT)

        # 1. Chuyển đổi mô hình ML sang định dạng ONNX
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

        print(f"[ONNX EXPORTER SUCCESS] SHA-256 model.onnx: {onnx_sha256}", flush=True)

        return {
            "model.onnx": onnx_bytes,
            "feature_schema.json": schema_bytes,
            "threshold_policy.json": policy_bytes,
            "metrics.json": metrics_bytes,
            "training_manifest.json": manifest_bytes,
            "checksums.sha256": checksum_bytes,
        }

    @staticmethod
    def _convert_to_onnx(model: Any, feature_cols: list = None) -> bytes:
        """Chuyển đổi mô hình ML (XGBoost GPU hoặc scikit-learn CPU) sang ONNX bytes và kiểm tra tính hợp lệ bằng onnx.checker"""
        if feature_cols is None:
            feature_cols = FEATURE_CONTRACT

        try:
            import onnx
            model_type_name = type(model).__name__

            # Nếu mô hình là XGBoost (GPU / CPU accelerated), sử dụng onnxmltools
            if "XGB" in model_type_name or hasattr(model, "get_booster"):
                from onnxmltools import convert_xgboost
                from onnxmltools.convert.common.data_types import FloatTensorType

                print(f"[ONNX EXPORTER] Đang chuyển đổi mô hình XGBoost ({model_type_name}) sang ONNX...", flush=True)
                if hasattr(model, "get_booster"):
                    model.get_booster().feature_names = [f"f{i}" for i in range(len(feature_cols))]

                initial_type = [("float_input", FloatTensorType([None, len(feature_cols)]))]
                onnx_model = convert_xgboost(
                    model,
                    initial_types=initial_type,
                )
            else:
                # Ngược lại, mô hình là scikit-learn (RandomForest/IsolationForest), sử dụng skl2onnx
                from skl2onnx import convert_sklearn
                from skl2onnx.common.data_types import FloatTensorType

                print(f"[ONNX EXPORTER] Đang chuyển đổi mô hình Scikit-Learn ({model_type_name}) sang ONNX...", flush=True)
                initial_type = [("float_input", FloatTensorType([None, len(feature_cols)]))]
                onnx_model = convert_sklearn(
                    model,
                    initial_types=initial_type,
                    options={id(model): {"zipmap": False}},
                )

            # Validate cấu trúc ONNX Graph (Fail-Close)
            onnx.checker.check_model(onnx_model)

            onnx_bytes = onnx_model.SerializeToString()
            print(f"[ONNX EXPORTER] Conversion & Validation ONNX model ({model_type_name}) thành công! ({len(onnx_bytes)} bytes)", flush=True)
            return onnx_bytes

        except Exception as err:
            raise RuntimeError(
                f"[FAIL-CLOSE] Chuyển đổi mô hình sang ONNX thất bại ({err})! Pipeline FAIL đỏ, không tạo mock model."
            )
