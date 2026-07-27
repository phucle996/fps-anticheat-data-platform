pub mod event;

// Re-export các struct domain chính
pub use event::{BatchMetadata, EventEnvelope, PlayerStatPayload, SourceMetadata};
