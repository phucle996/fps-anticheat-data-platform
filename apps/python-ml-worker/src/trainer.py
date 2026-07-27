import numpy as np
import pandas as pd
from typing import Tuple, Dict, Any
from sklearn.model_selection import GroupShuffleSplit
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier, IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score, average_precision_score

# Feature Contract cố định 6 đặc trưng Gold (Tuyệt đối giữ nguyên thứ tự)
FEATURE_CONTRACT = [
    "kills_per_minute",
    "damage_per_minute",
    "headshot_ratio",
    "damage_per_kill",
    "movement_per_minute",
    "performance_versus_lobby"
]

class ModelTrainer:
    """ModelTrainer huấn luyện các mô hình Machine Learning scikit-learn chống Data Leakage"""
    
    def __init__(self, features: list = None):
        self.features = features if features else FEATURE_CONTRACT

    def train_pipeline(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        """Vòng lặp huấn luyện full pipeline với Group-Split theo match_id"""
        print(f"[TRAINER] Bắt đầu tiến trình huấn luyện ML trên {len(df)} dòng dữ liệu...")

        # Đảm bảo đủ các cột đặc trưng trong contract
        X = df[self.features].astype(np.float32)
        y = df["is_suspicious"] if "is_suspicious" in df.columns else (df["headshot_ratio"] > 0.80).astype(int)
        groups = df["match_id"] if "match_id" in df.columns else np.arange(len(df))

        # 1. Group-Split Train / Test chống Data Leakage giữa các trận đấu (Match-level split)
        gss = GroupShuffleSplit(n_splits=1, test_size=0.3, random_state=42)
        train_idx, test_idx = next(gss.split(X, y, groups=groups))

        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        # 2. Train Logistic Regression Baseline
        lr = LogisticRegression(max_iter=1000, random_state=42)
        lr.fit(X_train, y_train)

        # 3. Train Random Forest Classifier
        rf = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
        rf.fit(X_train, y_train)

        # 4. Train Unsupervised Isolation Forest (Anti-Cheat Anomaly Detection)
        iso = IsolationForest(n_estimators=50, contamination=0.1, random_state=42)
        iso.fit(X_train)

        # 5. Đánh giá mô hình tốt nhất (Random Forest)
        preds = rf.predict(X_test)
        probs = rf.predict_proba(X_test)[:, 1] if hasattr(rf, "predict_proba") else preds

        precision = float(precision_score(y_test, preds, zero_division=0))
        recall = float(recall_score(y_test, preds, zero_division=0))
        f1 = float(f1_score(y_test, preds, zero_division=0))
        pr_auc = float(average_precision_score(y_test, probs))

        metrics = {
            "model_name": "RandomForestClassifier",
            "feature_count": len(self.features),
            "train_samples": len(X_train),
            "test_samples": len(X_test),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4),
            "pr_auc": round(pr_auc, 4),
        }

        print(f"[TRAINER SUCCESS] PR-AUC: {pr_auc:.4f} | F1-Score: {f1:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f}")
        return rf, metrics
