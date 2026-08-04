import time
from datetime import datetime, timezone
from threading import Event

from src.config import Config
from src.models.event_schemas import validate_gold_event, compute_model_version
from src.pipeline.exporter import ONNXExporter
from src.pipeline.trainer import ModelTrainer
from src.storage.s3_client import StorageClient
from src.kafka.consumer import EventConsumer
from src.kafka.producer import EventProducer

class StreamingWorkerDaemon:
    """Daemon lắng nghe liên tục sự kiện Kafka `gold.ready`, thực thi huấn luyện mô hình ML real-time, xuất ONNX và phát tín hiệu `model.ready`"""

    def __init__(self, config: Config, storage: StorageClient):
        self.config = config
        self.storage = storage
        self.trainer = ModelTrainer()

    def run_single_pipeline(self, gold_uri: str, version: str):
        """Thực thi pipeline huấn luyện trên một file Gold Parquet duy nhất nhận được từ Kafka event"""
        print(f"[STREAMING PIPELINE] Bắt đầu huấn luyện phiên bản {version} lúc {datetime.now(timezone.utc).isoformat()}", flush=True)

        df_gold = self.storage.load_gold_dataset(gold_uri)
        if len(df_gold) == 0:
            raise ValueError("[FAIL-CLOSE] Tập dữ liệu Gold Parquet bị rỗng!")

        model, metrics = self.trainer.train_pipeline(df_gold)
        bundle_files = ONNXExporter.export_bundle(model, metrics, version=version)
        bundle_uri = self.storage.upload_model_bundle(version, bundle_files)
        self.storage.activate_local_bundle(version, bundle_files)

        return version, metrics, bundle_uri

    def start(self, stop_event: Event):
        """Khởi chạy vòng lặp Event Loop lắng nghe Kafka tin nhắn với Retry & Exponential Backoff & Durable DLQ"""
        consumer = EventConsumer(self.config)
        producer = EventProducer(self.config)

        print(f"[STARTUP] ML Worker Daemon đang lắng nghe trên topic '{self.config.kafka_topic_gold}'...", flush=True)
        try:
            while not stop_event.is_set():
                messages = consumer.poll(timeout_ms=1000)
                if not messages:
                    continue

                for _, batch_messages in messages.items():
                    for msg in batch_messages:
                        if stop_event.is_set():
                            break

                        last_error = None
                        for attempt in range(self.config.ml_max_retries):
                            try:
                                payload = msg.value
                                validate_gold_event(payload)
                                version = compute_model_version(payload)

                                version, metrics, bundle_uri = self.run_single_pipeline(
                                    payload["object_uri"], version
                                )

                                producer.publish_model_ready(
                                    version=version,
                                    operation_id=payload["event_id"],
                                    bundle_uri=bundle_uri,
                                    metrics=metrics,
                                )

                                # Commit offset chỉ sau khi đã nạp S3, kích hoạt local symlink và phát thành công model.ready ACK
                                consumer.commit()
                                last_error = None
                                break
                            except Exception as error:
                                last_error = error
                                print(
                                    f"[RETRY] partition={msg.partition} offset={msg.offset} "
                                    f"attempt={attempt + 1}/{self.config.ml_max_retries}: {error}",
                                    flush=True,
                                )
                                if attempt + 1 < self.config.ml_max_retries:
                                    time.sleep(min(60, 2 ** attempt))

                        if last_error is not None:
                            # Đẩy tin nhắn hỏng (Poison Message) vào Durable DLQ và commit offset
                            producer.publish_dlq(msg, last_error)
                            consumer.commit()
                            print(
                                f"[DLQ] Đã ghi durable event cho partition={msg.partition} offset={msg.offset}",
                                flush=True,
                            )
        finally:
            producer.close()
            consumer.close()
