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
        print(f"[TRAINER] Bắt đầu tiến trình huấn luyện ML trên {len(df)} dòng dữ liệu...")

        has_ground_truth = "is_suspicious" in df.columns and df["is_suspicious"].notna().any()

        if has_ground_truth:
            return self._train_supervised(df)
        else:
            return self._train_unsupervised(df)

    def _train_supervised(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        print("[TRAINER] Phát hiện Ground Truth (is_suspicious) -> Chạy Supervised RandomForest...")

        features = self.features
        missing = [f for f in features if f not in df.columns]
        if missing:
            raise KeyError(f"Dữ liệu thiếu các cột feature contract: {missing}")

        clean_df = df.dropna(subset=features + ["is_suspicious", "match_id"]).copy()

        gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        train_idx, test_idx = next(gss.split(clean_df, groups=clean_df["match_id"]))

        X_train = clean_df.iloc[train_idx][features]
        y_train = clean_df.iloc[train_idx]["is_suspicious"]
        X_test = clean_df.iloc[test_idx][features]
        y_test = clean_df.iloc[test_idx]["is_suspicious"]

        model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else y_pred

        metrics = {
            "mode": "supervised",
            "precision": float(precision_score(y_test, y_pred, zero_division=0)),
            "recall": float(recall_score(y_test, y_pred, zero_division=0)),
            "f1_score": float(f1_score(y_test, y_pred, zero_division=0)),
            "pr_auc": float(average_precision_score(y_test, y_prob)),
            "test_sample_count": int(len(y_test)),
        }

        print(f"[TRAINER] Supervised Metrics: Precision={metrics['precision']:.3f}, Recall={metrics['recall']:.3f}, F1={metrics['f1_score']:.3f}")
        return model, metrics

    def _train_unsupervised(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        print("[TRAINER] Không có Ground Truth -> Chạy Unsupervised IsolationForest Anomaly Detection...")

        features = self.features
        for f in features:
            if f not in df.columns:
                df[f] = 0.0

        clean_df = df[features].fillna(0.0).copy()

        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        model.fit(clean_df)

        scores = model.decision_function(clean_df)

        metrics = {
            "mode": "unsupervised",
            "model_type": "IsolationForest",
            "total_samples": int(len(clean_df)),
            "anomaly_threshold_5pct": float(np.percentile(scores, 5)),
            "mean_anomaly_score": float(np.mean(scores)),
        }

        print(f"[TRAINER] Unsupervised Training OK. Total Samples={metrics['total_samples']}, Mean Score={metrics['mean_anomaly_score']:.4f}")
        return model, metrics
