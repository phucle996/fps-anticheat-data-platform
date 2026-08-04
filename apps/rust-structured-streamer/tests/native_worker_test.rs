// Import modules từ crate rust_structured_streamer
use rust_structured_streamer::config::Config;
use rust_structured_streamer::storage::MinioWriter;
use rust_structured_streamer::worker::NativeWorkerSpawner;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_native_worker_spawner_initialization() {
    let cfg = Config {
        kafka_brokers: "localhost:9092".to_string(),
        kafka_raw_topic: "pubg.v1.player-stat.raw".to_string(),
        kafka_gold_ready_topic: "pubg.v1.dataset.gold.ready".to_string(),
        kafka_group_id: "test-group".to_string(),
        minio_endpoint: "http://localhost:9000".to_string(),
        minio_bucket: "fps-anticheat-datalake".to_string(),
        minio_access_key: "minioadmin".to_string(),
        minio_secret_key: "minioadmin".to_string(),
        batch_size: 1000,
        flush_interval_ms: 1000,
        r_max_workers: 4,
        r_worker_timeout_seconds: 30,
        max_message_bytes: 5 * 1024 * 1024,
    };

    let writer = Arc::new(MinioWriter::new(&cfg).unwrap());
    let spawner = NativeWorkerSpawner::new(4, writer, Duration::from_secs(30));
    assert!(spawner
        .process_manifest("non_existent_manifest.json")
        .await
        .is_err());
}
