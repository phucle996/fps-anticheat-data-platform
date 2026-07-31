pub mod app;
pub mod config;
pub mod domain;
pub mod error;
pub mod ingest;
pub mod storage;
pub mod transform;
pub mod transport;
pub mod worker;

pub use app::StreamProcessorApp;
pub use config::Config;
pub use domain::*;
pub use error::{AppError, Result};
pub use ingest::{
    BatchAccumulator, BatchAccumulatorConfig, CompletedBatch, ConsumedMessage, KafkaConsumer,
};
pub use storage::{BatchManifest, MinioWriter, PartitionOffsetMetadata};
pub use transform::{
    ArrowConverter, DeduplicateOutcome, EventDeduplicator, EventValidator, InvalidEnvelopeRecord,
    NativeGoldFeatureGenerator, NativeSilverPreprocessor, ParquetSerializer, ValidationOutcome,
};
pub use worker::{
    CircuitBreakerState, DynamicWorkerPool, NativeWorkerResult, NativeWorkerSpawner,
    ResourceCircuitBreaker,
};
