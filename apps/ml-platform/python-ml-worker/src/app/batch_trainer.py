import time
from src.config import Config
from src.pipeline.exporter import ONNXExporter
from src.pipeline.trainer import ModelTrainer
from src.storage.s3_client import StorageClient
from src.storage.checkpoint import CheckpointManager
from src.kafka.producer import EventProducer

class OnDemandBatchTrainer:
    """Orchestrator huấn luyện On-Demand mô hình XGBoost GPU trên toàn bộ dữ liệu Gold Parquet trong S3 Data Lake (Có Checkpoint State Guard)"""

    def __init__(self, config: Config, storage: StorageClient):
        self.config = config
        self.storage = storage
        self.checkpoint_mgr = CheckpointManager(config, storage)
        self.trainer = ModelTrainer()

    def run_train_all(self):
        """Kiểm tra Checkpoint State và huấn luyện trên tổng hợp toàn bộ các file Gold Parquet nếu phát hiện dữ liệu mới"""
        # 1. So sánh danh sách file Gold trên S3 với Checkpoint State
        has_new_files, all_files, new_files = self.checkpoint_mgr.check_unprocessed_gold_files()

        if not all_files:
            print("[ML PIPELINE] Không tìm thấy bất kỳ file Gold dataset nào trên MinIO S3. Bỏ qua huấn luyện.", flush=True)
            return None, None, None

        if not has_new_files:
            print(
                f"[ML PIPELINE] Tất cả {len(all_files)} file Gold Parquet đã được huấn luyện tại Checkpoint trước đó (No new data). "
                f"Bỏ qua tiến trình huấn luyện (Skipped) để bảo vệ tài nguyên GPU!",
                flush=True,
            )
            return None, None, None

        print(
            f"[ML PIPELINE] Kích hoạt On-Demand Training từ {len(all_files)} file Gold Parquet "
            f"(Phát hiện {len(new_files)} file Gold mới chưa từng train)...",
            flush=True,
        )

        df_gold = self.storage.load_all_gold_datasets()
        if len(df_gold) == 0:
            raise ValueError("[FAIL-CLOSE] Tập dữ liệu Gold tổng hợp bị rỗng!")

        version = f"v-all-{int(time.time())}"
        model, metrics = self.trainer.train_pipeline(df_gold)
        bundle_files = ONNXExporter.export_bundle(model, metrics, version=version)
        bundle_uri = self.storage.upload_model_bundle(version, bundle_files)
        self.storage.activate_local_bundle(version, bundle_files)

        # 2. Cập nhật ML Training Checkpoint State mới lên MinIO S3
        self.checkpoint_mgr.save_ml_checkpoint(version, all_files, len(df_gold))

        # 3. Bắn tín hiệu Kafka model.ready thông báo cho Rust Inference Engine Hot-Swap ngay lập tức
        try:
            producer = EventProducer(self.config)
            producer.publish_model_ready(
                version=version,
                operation_id=f"on-demand-train-{version}",
                bundle_uri=bundle_uri,
                metrics=metrics,
            )
            producer.close()
            print(f"[ML PIPELINE] Đã phát tín hiệu Kafka model.ready cho version {version} thành công!", flush=True)
        except Exception as err:
            print(f"[WARNING] Bắn tín hiệu model.ready qua Kafka gặp sự cố (Model local đã được activate): {err}", flush=True)

        return version, metrics, bundle_uri
