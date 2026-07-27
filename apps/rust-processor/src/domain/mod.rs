pub mod event;

// Re-export các struct domain chính cho dự án
pub use event::{BatchMetadata, EventEnvelope, PlayerStatPayload, SourceMetadata};
