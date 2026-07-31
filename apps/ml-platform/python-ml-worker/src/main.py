import hashlib
import json
import signal
import time
from datetime import datetime, timezone
from threading import Event

from src.config import Config
from src.onnx_exporter import ONNXExporter
from src.storage import StorageClient
from src.trainer import ModelTrainer


def _model_version(payload: dict) -> str:
    event_id = payload["event_id"]
    checksum = payload["checksum_sha256"]
    # Stable version makes Kafka redelivery idempotent at the model registry.
    return f"v-{hashlib.sha256(f'{event_id}|{checksum}'.encode()).hexdigest()[:16]}"


def _validate_gold_event(payload: dict) -> None:
    if payload.get("schema_version") != "1.0":
        raise ValueError("[FAIL-CLOSE] Gold event schema_version không được hỗ trợ")
    if payload.get("op") != "data.dataset.gold.ready":
        raise ValueError("[FAIL-CLOSE] Gold event op không hợp lệ")
    event_id = payload.get("event_id", "")
    checksum = payload.get("checksum_sha256", "")
    object_uri = payload.get("object_uri", "")
    if len(event_id) != 64 or any(char not in "0123456789abcdef" for char in event_id):
        raise ValueError("[FAIL-CLOSE] Gold event_id phải là SHA-256 hex")
    if len(checksum) != 64 or any(char not in "0123456789abcdef" for char in checksum):
        raise ValueError("[FAIL-CLOSE] Gold checksum_sha256 phải là SHA-256 hex")
    if not object_uri.startswith("s3://"):
        raise ValueError("[FAIL-CLOSE] Gold object_uri phải là s3:// URI")


def run_pipeline(config: Config, storage: StorageClient, gold_uri: str, version: str):
    print(
        f"[ML PIPELINE] Bắt đầu {version} lúc "
        f"{datetime.now(timezone.utc).isoformat()}",
        flush=True,
    )
    df_gold = storage.load_gold_dataset(gold_uri)
    if len(df_gold) == 0:
        raise ValueError("[FAIL-CLOSE] Gold dataset rỗng")
    model, metrics = ModelTrainer().train_pipeline(df_gold)
    bundle_files = ONNXExporter.export_bundle(model, metrics, version=version)
    bundle_uri = storage.upload_model_bundle(version, bundle_files)
    storage.activate_local_bundle(version, bundle_files)
    return version, metrics, bundle_uri


def _publish(producer, topic: str, event: dict) -> None:
    future = producer.send(topic, event)
    future.get(timeout=30)


def _publish_dlq(producer, topic: str, msg, error: Exception) -> None:
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
    _publish(producer, topic, event)


def main():
    config = Config.from_env()
    storage = StorageClient(config)
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    from kafka import KafkaConsumer, KafkaProducer

    brokers = config.kafka_brokers.split(",")
    consumer = KafkaConsumer(
        config.kafka_topic_gold,
        bootstrap_servers=brokers,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=config.kafka_group_id,
        consumer_timeout_ms=1000,
        max_poll_records=1,
        max_partition_fetch_bytes=1024 * 1024,
    )
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda value: json.dumps(
            value, separators=(",", ":"), sort_keys=True
        ).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
    )

    print(f"[STARTUP] ML worker lắng nghe {config.kafka_topic_gold}", flush=True)
    try:
        while not stop.is_set():
            records = consumer.poll(timeout_ms=1000, max_records=1)
            if not records:
                continue
            for messages in records.values():
                for msg in messages:
                    last_error = None
                    for attempt in range(config.ml_max_retries):
                        try:
                            payload = msg.value
                            _validate_gold_event(payload)
                            version = _model_version(payload)
                            version, metrics, bundle_uri = run_pipeline(
                                config, storage, payload["object_uri"], version
                            )
                            _publish(
                                producer,
                                config.kafka_topic_model,
                                {
                                    "schema_version": "1.0",
                                    "event_id": hashlib.sha256(
                                        f"model.ready|{payload['event_id']}|{version}".encode()
                                    ).hexdigest(),
                                    "op": "ml.model.ready",
                                    "operation_id": payload["event_id"],
                                    "model_name": "pubg-risk",
                                    "model_version": version,
                                    "feature_version": "kill-event-player-match-v1",
                                    "bundle_uri": bundle_uri,
                                    "metrics": metrics,
                                    "created_at": datetime.now(timezone.utc).isoformat(),
                                },
                            )
                            # Commit only after S3, local activation and model.ready ACK.
                            consumer.commit()
                            last_error = None
                            break
                        except Exception as error:
                            last_error = error
                            print(
                                f"[RETRY] partition={msg.partition} offset={msg.offset} "
                                f"attempt={attempt + 1}/{config.ml_max_retries}: {error}",
                                flush=True,
                            )
                            if attempt + 1 < config.ml_max_retries:
                                time.sleep(min(60, 2 ** attempt))

                    if last_error is not None:
                        # A poison event is committed only after durable DLQ ACK.
                        _publish_dlq(producer, config.kafka_topic_ml_dlq, msg, last_error)
                        consumer.commit()
                        print(
                            f"[DLQ] Đã ghi durable event partition={msg.partition} "
                            f"offset={msg.offset}",
                            flush=True,
                        )
    finally:
        producer.flush(timeout=30)
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
