import numpy as np
import pandas as pd
from typing import Tuple, Dict, Any
from sklearn.model_selection import GroupShuffleSplit
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score, average_precision_score

# Feature Contract cho Supervised mode (khi có is_suspicious ground truth)
# QUAN TRỌNG: headshot_ratio ĐƯỢC PHÉP dùng trong supervised vì label KHÔNG phụ thuộc vào nó
SUPERVISED_FEATURES = [
    "kills_per_minute",
    "damage_per_minute",
    "headshot_ratio",
    "damage_per_kill",
    "movement_per_minute",
    "performance_versus_lobby"
]

# Feature Contract cho Unsupervised mode (Isolation Forest anomaly detection)
# headshot_ratio bị LOẠI vì không có ground truth để kiểm chứng signal này độc lập
# Dùng các chỉ số tốc độ và sát thương thuần túy — phát hiện pattern bất thường đa chiều
UNSUPERVISED_FEATURES = [
    "kills_per_minute",
    "damage_per_minute",
    "damage_per_kill",
    "movement_per_minute",
    "performance_versus_lobby"
]

# Alias để backward compatibility với code cũ tham chiếu FEATURE_CONTRACT
FEATURE_CONTRACT = SUPERVISED_FEATURES


class ModelTrainer:
    """ModelTrainer huấn luyện mô hình ML theo 2 chế độ:
    - Supervised (RandomForest): khi có cột `is_suspicious` trong data (ground truth thật sự)
    - Unsupervised (IsolationForest): khi không có ground truth — trả về anomaly score thay vì metrics

    KHÔNG dùng pseudo-label từ headshot_ratio > threshold vì đó là target leakage:
    - Label: headshot_ratio > 0.8
    - Feature: headshot_ratio
    → Model chỉ học lại rule if/else, metrics không có ý nghĩa thật sự.
    """

    def __init__(self, features: list = None):
        self.features = features  # None = tự chọn theo chế độ supervised/unsupervised

    def train_pipeline(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        """Vòng lặp huấn luyện full pipeline.

        Nếu `is_suspicious` tồn tại trong df → chạy Supervised RandomForest.
        Nếu không → chạy Unsupervised IsolationForest, KHÔNG tạo pseudo-label.
        """
        print(f"[TRAINER] Bắt đầu tiến trình huấn luyện ML trên {len(df)} dòng dữ liệu...")

        has_ground_truth = "is_suspicious" in df.columns and df["is_suspicious"].notna().any()

        if has_ground_truth:
            return self._train_supervised(df)
        else:
            return self._train_unsupervised(df)

    def _train_supervised(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        """Supervised RandomForest — CHỈ chạy khi có ground truth `is_suspicious` thật sự.
        Không tạo label từ feature (tránh target leakage).
        """
        print("[TRAINER] Chế độ: SUPERVISED (ground truth `is_suspicious` có sẵn)")

        feature_cols = self.features if self.features else SUPERVISED_FEATURES

        # Kiểm tra đủ cột đặc trưng
        X = df[feature_cols].astype(np.float32)
        y = df["is_suspicious"].astype(int)
        groups = df["match_id"] if "match_id" in df.columns else np.arange(len(df))

        # Group-Split Train / Test chống Data Leakage giữa các trận đấu (Match-level split)
        # Đảm bảo cùng 1 trận không xuất hiện ở cả train và test
        gss = GroupShuffleSplit(n_splits=1, test_size=0.3, random_state=42)
        train_idx, test_idx = next(gss.split(X, y, groups=groups))

        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        # Train Random Forest Classifier
        rf = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
        rf.fit(X_train, y_train)

        # Đánh giá mô hình trên test set
        preds = rf.predict(X_test)
        probs = rf.predict_proba(X_test)[:, 1] if hasattr(rf, "predict_proba") else preds.astype(float)

        precision = float(precision_score(y_test, preds, zero_division=0))
        recall = float(recall_score(y_test, preds, zero_division=0))
        f1 = float(f1_score(y_test, preds, zero_division=0))
        pr_auc = float(average_precision_score(y_test, probs))

        metrics = {
            "mode": "supervised",
            "model_name": "RandomForestClassifier",
            "feature_count": len(feature_cols),
            "train_samples": len(X_train),
            "test_samples": len(X_test),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4),
            "pr_auc": round(pr_auc, 4),
        }

        print(f"[TRAINER SUPERVISED] PR-AUC: {pr_auc:.4f} | F1: {f1:.4f} | P: {precision:.4f} | R: {recall:.4f}")
        return rf, metrics

    def _train_unsupervised(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        """Unsupervised IsolationForest — dùng khi không có ground truth `is_suspicious`.

        Không tạo pseudo-label từ headshot_ratio hay bất kỳ feature nào (tránh target leakage).
        Output là anomaly score per record, không phải precision/recall/F1.

        Kết quả được dùng để:
        1. Rank top suspicious records để manual annotation
        2. Xây dựng label ledger cho supervised training sau này
        """
        print("[TRAINER] Chế độ: UNSUPERVISED (không có ground truth — dùng IsolationForest anomaly detection)")
        print("[TRAINER] headshot_ratio KHÔNG được dùng làm signal chính để tránh circular reasoning")

        feature_cols = self.features if self.features else UNSUPERVISED_FEATURES

        # Kiểm tra đủ cột đặc trưng
        X = df[feature_cols].astype(np.float32)

        # IsolationForest: phát hiện outlier đa chiều (contamination=0.1 = ước tính 10% bất thường)
        # Đây là ước tính conservative — cần điều chỉnh sau khi có domain knowledge thực tế
        iso = IsolationForest(n_estimators=100, contamination=0.1, random_state=42)
        iso.fit(X)

        # Anomaly score: âm = bất thường, dương = bình thường (sklearn convention)
        # Chuyển về [0, 1]: 1.0 = suspicious nhất, 0.0 = bình thường nhất
        raw_scores = iso.decision_function(X)  # Khoảng [-0.5, 0.5] thường gặp
        anomaly_scores = 1.0 - (raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min() + 1e-9)

        n_anomalies = int((iso.predict(X) == -1).sum())
        contamination_rate = round(n_anomalies / len(X), 4)

        metrics = {
            "mode": "unsupervised",
            "model_name": "IsolationForest",
            "feature_count": len(feature_cols),
            "features_used": feature_cols,
            "train_samples": len(X),
            "n_anomalies_detected": n_anomalies,
            "contamination_rate": contamination_rate,
            "anomaly_score_mean": round(float(anomaly_scores.mean()), 4),
            "anomaly_score_p95": round(float(np.percentile(anomaly_scores, 95)), 4),
            # Không có precision/recall/F1 vì không có ground truth — đây là thiết kế có chủ đích
            "note": "No supervised metrics: no ground truth available. Use anomaly scores for manual annotation."
        }

        print(f"[TRAINER UNSUPERVISED] Anomalies detected: {n_anomalies}/{len(X)} ({contamination_rate:.1%}) | Score P95: {metrics['anomaly_score_p95']:.4f}")
        return iso, metrics
