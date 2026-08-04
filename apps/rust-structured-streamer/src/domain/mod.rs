pub mod event;

// Re-export các struct domain chính cho dự án
pub use event::{
    AnyEnvelope, BatchMetadata, EventEnvelope, KillEventEnvelope, KillEventPayload,
    PlayerStatPayload, SourceMetadata,
};
