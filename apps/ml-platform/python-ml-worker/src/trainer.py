import numpy as np
import pandas as pd
from typing import Tuple, Dict, Any
from sklearn.model_selection import GroupShuffleSplit
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score, average_precision_score

# Feature Contract khớp 100% với R Gold Feature Engine
FEATURE_CONTRACT = [
    "kills",
    "minimum_kill_interval_seconds",
    "median_kill_distance_coordinate_units",
    "short_kill_interval_count",
    "unique_weapons_used",
]

SUPERVISED_FEATURES = FEATURE_CONTRACT
UNSUPERVISED_FEATURES = FEATURE_CONTRACT


class ModelTrainer:
    """ModelTrainer huấn luyện mô hình ML theo 2 chế độ:
    - Supervised (RandomForest): khi có cột `is_suspicious` trong data
    - Unsupervised (IsolationForest): khi không có ground truth
    """

    def __init__(self, features: list = None):
        self.features = features or FEATURE_CONTRACT

    def train_pipeline(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        total_records = len(df)
        print(f"\n[TRAINER PROGRESS 0%] Bắt đầu tiến trình huấn luyện ML trên {total_records:,} dòng dữ liệu...", flush=True)

        if total_records < 20:
            raise ValueError(
                f"[FAIL-CLOSE] Cần tối thiểu 20 records để train, chỉ nhận {total_records}"
            )
        has_ground_truth = "is_suspicious" in df.columns and df["is_suspicious"].notna().any()

        if has_ground_truth:
            return self._train_supervised(df)
        else    def _build_classifier(self) -> Tuple[Any, str]:
        """Tự động khởi tạo mô hình phân loại: ưu tiên XGBoost GPU (CUDA), fallback về RandomForest CPU nếu không có GPU"""
        try:
            import xgboost as xgb
            # Thử khởi tạo XGBoost Classifier chạy trên NVIDIA GPU với CUDA acceleration
            model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                tree_method="hist",
                device="cuda",
                eval_metric="logloss"
            )
            print("[GPU ACCELERATION] Đã khởi tạo thành công mô hình XGBoost Classifier trên NVIDIA GPU (CUDA)...", flush=True)
            return model, "XGBClassifier (CUDA GPU)"
        except Exception as err:
            # Fallback an toàn về CPU nếu node Cloud không có GPU hoặc lỗi driver CUDA
            print(f"[CPU FALLBACK] Không thể bật GPU CUDA ({err}), tự động fallback về RandomForest CPU...", flush=True)
            model = RandomForestClassifier(
                n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
            )
            return model, "RandomForestClassifier (CPU)"

    def _train_supervised(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        print("[TRAINER PROGRESS 25%] Phát hiện Ground Truth (is_suspicious) -> Khởi tạo Supervised Classifier...", flush=True)

        features = self.features
        missing = [f for f in features if f not in df.columns]
        if missing:
            raise KeyError(f"Dữ liệu thiếu các cột feature contract: {missing}")

        clean_df = df.dropna(subset=features + ["is_suspicious", "match_id"]).copy()
        if clean_df["match_id"].nunique() < 2:
            raise ValueError("[FAIL-CLOSE] Supervised split cần ít nhất 2 match_id độc lập")
        if clean_df["is_suspicious"].nunique() != 2:
            raise ValueError("[FAIL-CLOSE] Ground truth phải chứa đủ hai lớp 0 và 1")

        print(f"[TRAINER PROGRESS 50%] Thực hiện GroupShuffleSplit phân chia Train/Test trên {len(clean_df):,} bản ghi sạch...", flush=True)
        gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        train_idx, test_idx = next(gss.split(clean_df, groups=clean_df["match_id"]))

        X_train = clean_df.iloc[train_idx][features]
        y_train = clean_df.iloc[train_idx]["is_suspicious"]
        X_test = clean_df.iloc[test_idx][features]
        y_test = clean_df.iloc[test_idx]["is_suspicious"]

        model, model_name = self._build_classifier()
        print(f"[TRAINER PROGRESS 75%] Đang fit mô hình {model_name} trên {len(X_train):,} mẫu train...", flush=True)
        model.fit(X_train, y_train)

        print("[TRAINER PROGRESS 90%] Đánh giá chỉ số chất lượng mô hình trên tập test...", flush=True)
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else y_pred

        metrics = {
            "mode": "supervised",
            "model_name": model_name,
            "features_used": features,
            "precision": float(precision_score(y_test, y_pred, zero_division=0)),
            "recall": float(recall_score(y_test, y_pred, zero_division=0)),
            "f1_score": float(f1_score(y_test, y_pred, zero_division=0)),
            "pr_auc": float(average_precision_score(y_test, y_prob)),
            "test_sample_count": int(len(y_test)),
        }

        print(f"[TRAINER PROGRESS 100%] Supervised Metrics OK: Precision={metrics['precision']:.3f}, Recall={metrics['recall']:.3f}, F1={metrics['f1_score']:.3f}", flush=True)
        return model, metrics

    def _train_unsupervised(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        print("[TRAINER PROGRESS 25%] Không có Ground Truth -> Chạy Unsupervised IsolationForest Anomaly Detection...", flush=True)

        features = self.features
        missing = [feature for feature in features if feature not in df.columns]
        if missing:
            raise KeyError(f"Dữ liệu thiếu các cột feature contract: {missing}")

        print(f"[TRAINER PROGRESS 50%] Chuẩn hóa dữ liệu đầu vào trên {len(df):,} mẫu cho IsolationForest...", flush=True)
        clean_df = df[features].replace([np.inf, -np.inf], np.nan).dropna().copy()
        if len(clean_df) < 20:
            raise ValueError(
                f"[FAIL-CLOSE] Chỉ còn {len(clean_df)} records finite sau data-quality filter"
            )

        print(f"[TRAINER PROGRESS 75%] Đang fit mô hình IsolationForest Anomaly Detector (n_estimators=100)...", flush=True)
        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        model.fit(clean_df)

        print("[TRAINER PROGRESS 90%] Tính toán điểm bất thường (Anomaly Decision Scores)...", flush=True)
        scores = model.decision_function(clean_df)

        # Export contract yêu cầu probability [0,1]. IsolationForest xuất raw
        # decision score, nên dùng nó tạo pseudo-label rồi fit classifier GPU/CPU;
        # Rust chỉ phải hiểu một output semantics duy nhất.
        anomaly_count = max(1, int(round(len(clean_df) * 0.05)))
        anomaly_indices = np.argsort(scores)[:anomaly_count]
        pseudo_labels = np.zeros(len(clean_df), dtype=np.int64)
        pseudo_labels[anomaly_indices] = 1

        probability_model, model_name = self._build_classifier()
        probability_model.fit(clean_df, pseudo_labels)
        pseudo_predictions = probability_model.predict(clean_df)

        metrics = {
            "mode": "unsupervised",
            "model_name": model_name,
            "teacher_model": "IsolationForest",
            "features_used": features,
            "total_samples": int(len(clean_df)),
            "pseudo_anomaly_count": int(anomaly_count),
            "surrogate_f1": float(f1_score(pseudo_labels, pseudo_predictions, zero_division=0)),
            "anomaly_threshold_5pct": float(np.percentile(scores, 5)),
            "mean_anomaly_score": float(np.mean(scores)),
        }

        print(f"[TRAINER PROGRESS 100%] Unsupervised Training OK. Total Samples={metrics['total_samples']:,}, Mean Anomaly Score={metrics['mean_anomaly_score']:.4f}", flush=True)
        return probability_model, metricsSS 100%] Unsupervised Training OK. Total Samples={metrics['total_samples']:,}, Mean Anomaly Score={metrics['mean_anomaly_score']:.4f}", flush=True)
        return probability_model, metrics
