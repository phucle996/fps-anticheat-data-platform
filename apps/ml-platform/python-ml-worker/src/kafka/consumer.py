import json
from kafka import KafkaConsumer
from src.config import Config

class EventConsumer:
    """Quản lý Kafka Consumer kết nối tới topic gold.ready với Manual Commit"""

    def __init__(self, config: Config):
        self.config = config
        brokers = config.kafka_brokers.split(",")
        self.consumer = KafkaConsumer(
            config.kafka_topic_gold,
            bootstrap_servers=brokers,
            group_id=config.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def poll(self, timeout_ms: int = 1000) -> dict:
        """Poll danh sách tin nhắn từ Kafka Broker"""
        return self.consumer.poll(timeout_ms=timeout_ms)

    def commit(self) -> None:
        """Thực hiện Manual Commit Offset sau khi xử lý thành công hoặc đã đẩy DLQ ACK"""
        self.consumer.commit()

    def close(self) -> None:
        """Đóng kết nối Kafka Consumer an toàn"""
        try:
            self.consumer.close()
        except Exception:
            pass
