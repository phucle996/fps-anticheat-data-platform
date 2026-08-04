import json
import hashlib
from datetime import datetime, timezone
from kafka import KafkaProducer
from src.config import Config

class EventProducer:
    """Quản lý việc phát tin nhắn Kafka (Model Ready Events & Durable Dead Letter Queue - DLQ)"""

    def __init__(self, config: Config):
        self.config = config
        brokers = config.kafka_brokers.split(",")
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def publish_event(self, topic: str, event: dict) -> None:
        """Phát tin nhắn Kafka đồng bộ kèm theo timeout ACK 30s"""
        future = self.producer.send(topic, event)
        future.get(timeout=30)

    def publish_model_ready(self, version: str, operation_id: str, bundle_uri: str, metrics: dict) -> None:
        """Phát tín hiệu Kafka model.ready thông báo cho Rust Inference Engine biết để Hot-Swap mô hình"""
        event = {
            "schema_version": "1.0",
            "event_id": hashlib.sha256(f"model.ready|{operation_id}|{version}".encode()).hexdigest(),
            "op": "ml.model.ready",
            "operation_id": operation_id,
            "model_name": "pubg-risk",
            "model_version": version,
            "feature_version": "kill-event-player-match-v1",
            "bundle_uri": bundle_uri,
            "metrics": metrics,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.publish_event(self.config.kafka_topic_model, event)
        self.flush()
        print(f"[KAFKA] Đã phát thành công tín hiệu model.ready (Version: {version})", flush=True)

    def publish_dlq(self, msg, error: Exception) -> None:
        """Ghi tin nhắn hỏng (Poison Message) đã retry thất bại vào Durable Dead Letter Queue (DLQ)"""
        event = {
            "schema_version": "1.0",
            "op": "ml.dataset.gold.ready.dlq",
            "event_id": hashlib.sha256(
                f"{msg.topic}|{msg.partition}|{msg.offset}".encode()
            ).hexdigest(),
            "source_topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "payload": msg.value,
            "error": str(error)[:2000],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.publish_event(self.config.kafka_topic_ml_dlq, event)
        self.flush()
        print(f"[DLQ] Đã ghi durable DLQ event thành công cho partition={msg.partition} offset={msg.offset}", flush=True)

    def flush(self) -> None:
        """Flush bộ nhớ đệm Kafka Producer"""
        self.producer.flush(timeout=30)

    def close(self) -> None:
        """Đóng kết nối Kafka Producer an toàn"""
        try:
            self.flush()
            self.producer.close()
        except Exception:
            pass
