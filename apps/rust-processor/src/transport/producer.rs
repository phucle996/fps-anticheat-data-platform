use crate::config::Config;
use crate::error::{AppError, Result};
use crate::worker::RWorkerResult;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tracing::info;

#[derive(Debug, Serialize)]
struct GoldReadyEvent<'a> {
    schema_version: &'static str,
    event_id: String,
    op: &'static str,
    batch_id: &'a str,
    object_uri: String,
    checksum_sha256: &'a str,
    source_manifest: &'a str,
    created_at: String,
}

#[derive(Clone)]
pub struct KafkaEventProducer {
    producer: FutureProducer,
    gold_ready_topic: String,
    bucket: String,
}

impl KafkaEventProducer {
    pub fn new(config: &Config) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "zstd")
            .set("message.timeout.ms", "30000")
            .create()
            .map_err(|err| {
                AppError::Kafka(format!("Tạo Kafka event producer thất bại: {}", err))
            })?;
        Ok(Self {
            producer,
            gold_ready_topic: config.kafka_gold_ready_topic.clone(),
            bucket: config.minio_bucket.clone(),
        })
    }

    /// Publish chỉ sau khi Gold artifact durable. Retry tạo duplicate cùng event_id, consumer phải idempotent.
    pub async fn publish_gold_ready(
        &self,
        batch_id: &str,
        result: &RWorkerResult,
    ) -> Result<usize> {
        let gold_artifacts: Vec<_> = result
            .artifacts
            .iter()
            .filter(|artifact| artifact.layer == "gold/player-match-features")
            .collect();

        for artifact in &gold_artifacts {
            let stable_material = format!(
                "{}|{}|{}",
                batch_id, artifact.object_key, artifact.checksum_sha256
            );
            let event_id = format!("{:x}", Sha256::digest(stable_material.as_bytes()));
            let event = GoldReadyEvent {
                schema_version: "1.0",
                event_id: event_id.clone(),
                op: "data.dataset.gold.ready",
                batch_id,
                object_uri: format!("s3://{}/{}", self.bucket, artifact.object_key),
                checksum_sha256: &artifact.checksum_sha256,
                source_manifest: &result.manifest_key,
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            let payload = serde_json::to_vec(&event).map_err(|err| {
                AppError::Kafka(format!("Serialize dataset.gold.ready thất bại: {}", err))
            })?;

            self.producer
                .send(
                    FutureRecord::to(&self.gold_ready_topic)
                        .key(batch_id)
                        .payload(&payload),
                    Timeout::After(Duration::from_secs(30)),
                )
                .await
                .map_err(|(err, _)| {
                    AppError::Kafka(format!(
                        "Publish dataset.gold.ready event_id={} thất bại: {}",
                        event_id, err
                    ))
                })?;
            info!(
                event_id = %event_id,
                batch_id = %batch_id,
                object_key = %artifact.object_key,
                "Published durable dataset.gold.ready"
            );
        }
        Ok(gold_artifacts.len())
    }
}
