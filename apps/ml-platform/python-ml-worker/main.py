import argparse
import signal
import warnings
from threading import Event

# Ẩn các cảnh báo DeprecationWarning lặt vặt từ thư viện kafka-python
warnings.filterwarnings("ignore", category=DeprecationWarning)

from src.config import Config
from src.storage.s3_client import StorageClient
from src.app.streaming_worker import StreamingWorkerDaemon
from src.app.batch_trainer import OnDemandBatchTrainer


def main():
    parser = argparse.ArgumentParser(description="Python ML Training & Worker Service")
    parser.add_argument(
        "--mode",
        type=str,
        default="worker",
        choices=["worker", "train-all"],
        help="Chế độ thực thi ứng dụng (worker/train-all)"
    )
    args = parser.parse_args()

    # Nạp cấu hình ứng dụng từ biến môi trường (Fail-Close 100%)
    config = Config.from_env()
    storage = StorageClient(config)

    # Nếu chạy chế độ On-Demand Batch Training (--mode=train-all)
    if args.mode == "train-all":
        trainer_app = OnDemandBatchTrainer(config, storage)
        trainer_app.run_train_all()
        print("[ML WORKER] Hoàn tất tiến trình On-Demand Training!", flush=True)
        return

    # Nếu chạy chế độ Real-time Streaming Worker Daemon (--mode=worker)
    stop_event = Event()

    def _shutdown(_signum, _frame):
        print("\n🛑 [SHUTDOWN] Bắt đầu dừng tiến trình ML Worker Daemon an toàn...", flush=True)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    worker_app = StreamingWorkerDaemon(config, storage)
    worker_app.start(stop_event)


if __name__ == "__main__":
    main()
