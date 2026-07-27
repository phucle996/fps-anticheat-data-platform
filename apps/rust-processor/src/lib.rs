pub mod config;
pub mod domain;
pub mod error;
pub mod ingest;
pub mod transform;

pub use config::Config;
pub use domain::*;
pub use error::{AppError, Result};
pub use ingest::{BatchAccumulator, BatchAccumulatorConfig, CompletedBatch, ConsumedMessage, KafkaConsumer};
pub use transform::{
    ArrowConverter, DeduplicateOutcome, EventDeduplicator, EventValidator, InvalidEnvelopeRecord, ParquetSerializer,
    ValidationOutcome,
};
